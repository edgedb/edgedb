use crate::{
    algo::{
        AcquireOp, PoolAlgoTargetData, PoolAlgorithmDataBlock, PoolAlgorithmDataMetrics,
        PoolConstraints, RebalanceOp, ReleaseOp, ReleaseType, VisitPoolAlgoData,
    },
    block::{Blocks, Name},
    conn::{ConnError, ConnHandle, ConnResult, Connector},
    metrics::{MetricVariant, PoolMetrics},
};
use consume_on_drop::{Consume, ConsumeOnDrop};
use derive_more::Debug;
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    rc::Rc,
    time::Duration,
};
use tracing::trace;

#[derive(Debug)]
pub struct PoolConfig {
    pub constraints: PoolConstraints,
    pub adjustment_interval: Duration,
}

impl PoolConfig {
    pub fn assert_valid(&self) {
        assert!(self.constraints.max > 0);
    }

    /// Generate suggested default configurations for the expected number of connections with an
    /// unknown number of databases.
    pub fn suggested_default_for(connections: usize) -> Self {
        Self::suggested_default_for_databases(connections, usize::MAX)
    }

    /// Generate suggested default configurations for the expected number of connections and databases.
    pub fn suggested_default_for_databases(connections: usize, databases: usize) -> Self {
        assert!(connections > 0);
        assert!(databases > 0);
        Self {
            adjustment_interval: Duration::from_millis(10),
            constraints: PoolConstraints { max: connections },
        }
    }
}

struct HandleAndPool<C: Connector>(ConnHandle<C>, Rc<Pool<C>>, Cell<bool>);

/// An opaque handle representing a RAII lock on a connection in the pool. The
/// underlying connection object may be
pub struct PoolHandle<C: Connector> {
    conn: ConsumeOnDrop<HandleAndPool<C>>,
}

impl<C: Connector> Debug for PoolHandle<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.conn.0.fmt(f)
    }
}

impl<C: Connector> Consume for HandleAndPool<C> {
    fn consume(self) {
        self.1.release(self.0, self.2.get())
    }
}

impl<C: Connector> PoolHandle<C> {
    /// Marks this handle as poisoned, which will not allow it to be reused in the pool. The
    /// most likely case for this is that the underlying connection's stream has closed, or
    /// the remote end is no longer valid for some reason.
    pub fn poison(&self) {
        self.conn.2.set(true)
    }

    /// Use this pool's handle temporarily.
    #[inline(always)]
    pub fn with_handle<T>(&self, f: impl Fn(&C::Conn) -> T) -> T {
        self.conn.0.conn.with_handle(f).unwrap()
    }

    fn new(conn: ConnHandle<C>, pool: Rc<Pool<C>>) -> Self {
        Self {
            conn: ConsumeOnDrop::new(HandleAndPool(conn, pool, Cell::default())),
        }
    }
}

impl<C: Connector> PoolHandle<C>
where
    C::Conn: Copy,
{
    /// If the handle is `Copy`, copies this handle.
    #[inline(always)]
    pub fn handle(&self) -> C::Conn {
        self.conn.0.conn.with_handle(|c| *c).unwrap()
    }
}

impl<C: Connector> PoolHandle<C>
where
    C::Conn: Clone,
{
    /// If the handle is `Clone`, clones this handle.
    #[inline(always)]
    pub fn handle_clone(&self) -> C::Conn {
        self.conn.0.conn.with_handle(|c| c.clone()).unwrap()
    }
}

#[derive(derive_more::Debug)]
/// A connection pool consists of a number of blocks, each with a target
/// connection count (aka a quota). Generally, a block may take up to its quota,
/// but no more, though the pool operating conditions may allow for this to vary
/// for optimal allocation of the limited connections. If a block is over quota,
/// one of its connections may be stolen to satisfy another block's needs.
pub struct Pool<C: Connector> {
    connector: C,
    config: PoolConfig,
    blocks: Blocks<C, PoolAlgoTargetData>,
    drain: Drain,
    /// If the pool has been dirtied by acquiring or releasing a connection
    dirty: Rc<Cell<bool>>,
}

impl<C: Connector> Pool<C> {
    pub fn new(config: PoolConfig, connector: C) -> Rc<Self> {
        config.assert_valid();
        Rc::new(Self {
            config,
            blocks: Default::default(),
            connector,
            dirty: Default::default(),
            drain: Default::default(),
        })
    }
}

impl<C: Connector> Pool<C> {
    /// Runs the required async task that takes care of quota management, garbage collection,
    /// and other important async tasks. This should happen only if something has changed in
    /// the pool.
    pub async fn run(&self) {
        loop {
            tokio::time::sleep(self.config.adjustment_interval).await;
            self.run_once();
        }
    }

    /// Runs the required async task that takes care of quota management, garbage collection,
    /// and other important async tasks. This should happen only if we have live blocks.
    pub fn run_once(&self) {
        if self.blocks.is_empty() {
            return;
        }

        self.config.constraints.adjust(&self.blocks);
        let mut s = String::new();
        self.blocks.with_all(|name, block| {
            s += &format!("{name}={} ", block.target());
        });
        trace!("Targets: {s}");
        for op in self.config.constraints.plan_rebalance(&self.blocks) {
            trace!("Rebalance: {op:?}");
            match op {
                RebalanceOp::Transfer { from, to } => {
                    tokio::task::spawn_local(self.blocks.task_steal(&self.connector, &to, &from));
                }
                RebalanceOp::Create(name) => {
                    tokio::task::spawn_local(self.blocks.task_create_one(&self.connector, &name));
                }
                RebalanceOp::Close(name) => {
                    tokio::task::spawn_local(self.blocks.task_close_one(&self.connector, &name));
                }
            }
        }
    }

    /// Acquire a handle from this connection pool. The returned [`PoolHandle`]
    /// controls the lock for the connection and may be dropped to release it
    /// back into the pool.
    pub async fn acquire(self: &Rc<Self>, db: &str) -> ConnResult<PoolHandle<C>> {
        if self.drain.shutdown.get() {
            return Err(ConnError::Shutdown);
        }
        self.dirty.set(true);
        let plan = self.config.constraints.plan_acquire(db, &self.blocks);
        trace!("Acquire {db}: {plan:?}");
        match plan {
            AcquireOp::Create => {
                tokio::task::spawn_local(self.blocks.task_create_one(&self.connector, db));
            }
            AcquireOp::Steal(from) => {
                tokio::task::spawn_local(self.blocks.task_steal(&self.connector, db, &from));
            }
            AcquireOp::Wait => {}
        };
        let conn = self.blocks.queue(db).await?;

        Ok(PoolHandle::new(conn, self.clone()))
    }

    /// Internal release method
    fn release(self: Rc<Self>, conn: ConnHandle<C>, poison: bool) {
        let db = &conn.state.db_name;
        self.dirty.set(true);
        let release_type = if self.drain.is_draining(db) {
            ReleaseType::Drain
        } else if poison {
            ReleaseType::Poison
        } else {
            ReleaseType::Normal
        };
        let plan = self
            .config
            .constraints
            .plan_release(db, release_type, &self.blocks);
        trace!("Release: {conn:?} {plan:?}");
        match plan {
            ReleaseOp::Release => {}
            ReleaseOp::Discard => {
                tokio::task::spawn_local(self.blocks.task_discard(&self.connector, conn));
            }
            ReleaseOp::ReleaseTo(db) => {
                tokio::task::spawn_local(self.blocks.task_move_to(&self.connector, conn, &db));
            }
            ReleaseOp::Reopen => {
                tokio::task::spawn_local(self.blocks.task_reopen(&self.connector, conn));
            }
        }
    }

    /// Retrieve the current pool metrics snapshot.
    pub fn metrics(&self) -> PoolMetrics {
        self.blocks.summary()
    }

    /// Is this pool idle?
    pub fn idle(&self) -> bool {
        self.blocks.is_empty()
    }

    /// Drain all connections to the given database. All connections will be
    /// poisoned on return and this method will return when the given database
    /// is idle. Multiple calls to this method with the same database are valid,
    /// and the drain operation will be kept alive as long as one future has not
    /// been dropped.
    ///
    /// It is valid, though unadvisable, to request a connection during this
    /// period. The connection will be poisoned on return as well.
    ///
    /// Dropping this future cancels the drain operation.
    pub async fn drain(self: Rc<Self>, db: &str) {
        // If the block doesn't exist, we can return
        let Some(name) = self.blocks.name(db) else {
            return;
        };

        let lock = Drain::lock(self.clone(), name);
        while self.blocks.metrics(db).total() > 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        drop(lock);
    }

    /// Drain all connections in the pool, returning when the pool is completely
    /// empty. Multiple calls to this method with the same database are valid,
    /// and the drain operation will be kept alive as long as one future has not
    /// been dropped.
    ///
    /// It is valid, though unadvisable, to request a connection during this
    /// period. The connection will be poisoned on return as well.
    ///
    /// Dropping this future cancels the drain operation.
    pub async fn drain_all(self: Rc<Self>) {
        let lock = Drain::lock_all(self.clone());
        while self.blocks.total() > 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        drop(lock);
    }

    /// Shuts this pool down safely. Dropping this future does not cancel
    /// the shutdown operation.
    pub async fn shutdown(mut self: Rc<Self>) {
        self.drain.shutdown();
        let pool = loop {
            match Rc::try_unwrap(self) {
                Ok(pool) => break pool,
                Err(pool) => self = pool,
            };
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        while !pool.idle() {
            pool.run_once();
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        if cfg!(debug_assertions) {
            let all_time = &pool.metrics().all_time;
            assert_eq!(
                all_time[MetricVariant::Connecting],
                all_time[MetricVariant::Disconnecting],
                "Connecting != Disconnecting"
            );
            assert_eq!(
                all_time[MetricVariant::Disconnecting],
                all_time[MetricVariant::Closed],
                "Disconnecting != Closed"
            );
        }
    }
}

impl<C: Connector> AsRef<Drain> for Rc<Pool<C>> {
    fn as_ref(&self) -> &Drain {
        &self.drain
    }
}

/// Holds the current drainage and shutdown state for the `Pool`.
#[derive(Default, Debug)]
struct Drain {
    drain_all: Cell<usize>,
    drain: RefCell<HashMap<Name, usize>>,
    shutdown: Cell<bool>,
}

impl Drain {
    pub fn shutdown(&self) {
        self.shutdown.set(true)
    }

    /// Lock all connections for draining.
    pub fn lock_all<T: AsRef<Drain>>(this: T) -> DrainLock<T> {
        let drain = this.as_ref();
        drain.drain_all.set(drain.drain_all.get() + 1);
        DrainLock {
            db: None,
            has_drain: this,
        }
    }

    // Lock a specific connection for draining.
    pub fn lock<T: AsRef<Drain>>(this: T, db: Name) -> DrainLock<T> {
        {
            let mut drain = this.as_ref().drain.borrow_mut();
            drain.entry(db.clone()).and_modify(|v| *v += 1).or_default();
        }
        DrainLock {
            db: Some(db),
            has_drain: this,
        }
    }

    /// Is this connection draining?
    fn is_draining(&self, db: &str) -> bool {
        self.drain_all.get() > 0 || self.drain.borrow().contains_key(db) || self.shutdown.get()
    }
}

/// Provides a RAII lock for a db- or whole-pool drain operation.
struct DrainLock<T: AsRef<Drain>> {
    db: Option<Name>,
    has_drain: T,
}

impl<T: AsRef<Drain>> Drop for DrainLock<T> {
    fn drop(&mut self) {
        if let Some(name) = self.db.take() {
            let mut drain = self.has_drain.as_ref().drain.borrow_mut();
            if let Some(count) = drain.get_mut(&name) {
                if *count > 1 {
                    *count -= 1;
                } else {
                    drain.remove(&name);
                }
            } else {
                unreachable!()
            }
        } else {
            let this = self.has_drain.as_ref();
            this.drain_all.set(this.drain_all.get() - 1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use crate::time::Instant;
    use anyhow::{Ok, Result};
    use itertools::Itertools;
    use rstest::rstest;

    use test_log::test;
    use tokio::task::LocalSet;
    use tracing::{error, info, trace};

    #[test(tokio::test(flavor = "current_thread", start_paused = true))]
    async fn test_pool_basic() -> Result<()> {
        LocalSet::new()
            .run_until(async {
                let config = PoolConfig::suggested_default_for(10);

                let pool = Pool::new(config, BasicConnector::no_delay());
                let conn1 = pool.acquire("1").await?;
                let conn2 = pool.acquire("1").await?;

                drop(conn1);
                conn2.poison();
                drop(conn2);

                pool.shutdown().await;

                Ok(())
            })
            .await
    }

    #[test(tokio::test(flavor = "current_thread", start_paused = true))]
    async fn test_pool_eventually_idles() -> Result<()> {
        let future = async {
            let config = PoolConfig::suggested_default_for(10);

            let pool = Pool::new(config, BasicConnector::no_delay());
            let conn = pool.acquire("1").await?;
            tokio::time::sleep(Duration::from_millis(10)).await;
            drop(conn);

            while !pool.idle() {
                tokio::time::sleep(Duration::from_millis(10)).await;
                pool.run_once();
            }
            trace!("Pool idle, shutting down");

            pool.shutdown().await;
            Ok(())
        };
        tokio::time::timeout(Duration::from_secs(120), LocalSet::new().run_until(future)).await?
    }

    #[test(tokio::test(flavor = "current_thread", start_paused = true))]
    async fn test_pool_drains() -> Result<()> {
        let future = async {
            let config = PoolConfig::suggested_default_for(10);

            let pool = Pool::new(config, BasicConnector::no_delay());
            let conn = pool.acquire("1").await?;
            tokio::task::spawn_local(pool.clone().drain_all());
            tokio::task::spawn_local(async {
                tokio::time::sleep(Duration::from_millis(10)).await;
                drop(conn);
            });

            while !pool.idle() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            trace!("Pool idle, shutting down");

            pool.shutdown().await;
            Ok(())
        };
        tokio::time::timeout(Duration::from_secs(120), LocalSet::new().run_until(future)).await?
    }

    #[test(tokio::test(flavor = "current_thread", start_paused = true))]
    #[rstest]
    #[case::one(1)]
    #[case::small(10)]
    #[case::medium(12)]
    #[case::large(20)]
    async fn test_pool(#[case] databases: usize) -> Result<()> {
        let spec = Spec {
            name: format!("test_pool_{databases}").into(),
            desc: "",
            capacity: 10,
            conn_cost: Triangle(0.05, 0.0),
            score: vec![
                Score::new(
                    0.8,
                    [2.0, 0.5, 0.25, 0.0],
                    LatencyDistribution {
                        group: 0..databases,
                    },
                ),
                Score::new(0.2, [0.5, 0.2, 0.1, 0.0], ConnectionOverhead {}),
            ],
            dbs: (0..databases)
                .map(|db| DBSpec {
                    db,
                    start_at: 0.0,
                    end_at: 1.0,
                    qps: 1200,
                    query_cost: Triangle(0.001, 0.0),
                })
                .collect_vec(),
            ..Default::default()
        };

        run(spec).await.map(drop)
    }

    async fn run(spec: Spec) -> Result<QoS> {
        let local = LocalSet::new();
        let res = local.run_until(run_local(spec)).await?;
        local.await;
        Ok(res)
    }

    async fn run_local(spec: Spec) -> std::result::Result<QoS, anyhow::Error> {
        let start = Instant::now();
        let real_time = std::time::Instant::now();
        let config = PoolConfig::suggested_default_for(spec.capacity);
        let disconnect_cost = spec.disconn_cost;
        let connect_cost = spec.disconn_cost;
        let pool = Pool::new(
            config,
            BasicConnector::delay(move |disconnect| {
                if disconnect {
                    disconnect_cost.random_duration()
                } else {
                    connect_cost.random_duration()
                }
            }),
        );
        let mut tasks = vec![];
        let latencies = Latencies::default();

        // Boot a task for each DBSpec in the Spec
        for (i, db_spec) in spec.dbs.into_iter().enumerate() {
            let interval = 1.0 / (db_spec.qps as f64);
            info!("[{i:-2}] db {db_spec:?}");
            let db = format!("t{}", db_spec.db);
            let pool = pool.clone();
            let latencies = latencies.clone();
            let local = async move {
                let now = Instant::now();
                let count = ((db_spec.end_at - db_spec.start_at) * (db_spec.qps as f64)) as usize;
                tokio::time::sleep(Duration::from_secs_f64(db_spec.start_at)).await;
                info!(
                    "+[{i:-2}] Starting db {db} at {}qps (approx {}q·s/s from {}..{})...",
                    db_spec.qps,
                    db_spec.qps as f64 * db_spec.query_cost.0,
                    db_spec.start_at,
                    db_spec.end_at,
                );
                let start_time = now.elapsed().as_secs_f64();
                // Boot one task for each expected query in a localset, with a
                // sleep that schedules it for the appropriate time.
                let local = LocalSet::new();
                for i in 0..count {
                    let pool = pool.clone();
                    let latencies = latencies.clone();
                    let duration = db_spec.query_cost.random_duration();
                    let db = db.clone();
                    local.spawn_local(async move {
                        tokio::time::sleep(Duration::from_secs_f64(i as f64 * interval)).await;
                        let now = Instant::now();
                        let conn = pool.acquire(&db).await?;
                        let latency = now.elapsed();
                        latencies.mark(&db, latency.as_secs_f64());
                        tokio::time::sleep(duration).await;
                        drop(conn);
                        Ok(())
                    });
                }
                tokio::time::timeout(Duration::from_secs(120), local)
                    .await
                    .unwrap_or_else(move |_| error!("*[{i:-2}] DBSpec {i} for {db} timed out"));
                let end_time = now.elapsed().as_secs_f64();
                info!("-[{i:-2}] Finished db t{} at {}qps. Load generated from {}..{}, processed from {}..{}",
                        db_spec.db, db_spec.qps, db_spec.start_at, db_spec.end_at, start_time, end_time);
            };
            tasks.push(tokio::task::spawn_local(local));
        }

        // Boot the monitor the runs the pool algorithm and prints the current
        // block connection stats.
        let monitor = {
            let pool = pool.clone();
            tokio::task::spawn_local(async move {
                let mut orig = "".to_owned();
                loop {
                    pool.run_once();
                    let mut s = "".to_owned();
                    for (name, block) in pool.metrics().blocks {
                        s += &format!("{name}={} ", block.total);
                    }
                    if !s.is_empty() && s != orig {
                        trace!(
                            "Blocks: {}/{} {s}",
                            pool.metrics().pool.total,
                            pool.config.constraints.max
                        );
                        orig = s;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
        };

        info!("Starting...");
        tokio::time::sleep(Duration::from_secs_f64(spec.duration)).await;

        for task in tasks {
            _ = task.await;
        }

        info!(
            "Took {:?} of virtual time ({:?} real time)",
            start.elapsed(),
            real_time.elapsed()
        );

        monitor.abort();
        _ = monitor.await;
        let metrics = pool.metrics();
        info!("{metrics:#?}");
        info!("{latencies:#?}");

        let metrics = pool.metrics();
        let mut qos = 0.0;
        let mut scores = vec![];
        for score in spec.score {
            let scored = score.method.score(&latencies, &metrics, &pool.config);

            let score_component = score.calculate(scored.raw_value);
            info!(
                "[QoS: {}] {} = {:.2} -> {:.2} (weight {:.2})",
                spec.name, scored.description, scored.raw_value, score_component, score.weight
            );
            trace!(
                "[QoS: {}] {} [detail]: {} = {:.3}",
                spec.name,
                scored.description,
                (scored.detailed_calculation)(3),
                scored.raw_value
            );
            scores.push(WeightedScored {
                scored,
                weight: score.weight,
                score: score_component,
            });
            qos += score_component * score.weight;
        }
        info!("[QoS: {}] Score = {qos:0.02}", spec.name);

        info!("Shutting down...");
        pool.shutdown().await;

        Ok(QoS { scores, qos })
    }

    fn test_connpool_1() -> Spec {
        let mut dbs = vec![];
        for i in 0..6 {
            dbs.push(DBSpec {
                db: i,
                start_at: 0.0,
                end_at: 0.5,
                qps: 50,
                query_cost: Triangle(0.03, 0.005),
            })
        }
        for i in 6..12 {
            dbs.push(DBSpec {
                db: i,
                start_at: 0.3,
                end_at: 0.7,
                qps: 50,
                query_cost: Triangle(0.03, 0.005),
            })
        }
        for i in 0..6 {
            dbs.push(DBSpec {
                db: i,
                start_at: 0.6,
                end_at: 0.8,
                qps: 50,
                query_cost: Triangle(0.03, 0.005),
            })
        }

        Spec {
            name: "test_connpool_1".into(),
            desc: r#"
                This is a test for Mode D, where 2 groups of blocks race for connections
                in the pool with max capacity set to 6. The first group (0-5) has more
                dedicated time with the pool, so it should have relatively lower latency
                than the second group (6-11). But the QoS is focusing on the latency
                distribution similarity, as we don't want to starve only a few blocks
                because of the lack of capacity. Therefore, reconnection is a necessary
                cost for QoS.
            "#,
            capacity: 6,
            conn_cost: Triangle(0.05, 0.01),
            score: vec![
                Score::new(
                    0.18,
                    [2.0, 0.5, 0.25, 0.0],
                    LatencyDistribution { group: 0..6 },
                ),
                Score::new(
                    0.28,
                    [2.0, 0.3, 0.1, 0.0],
                    LatencyDistribution { group: 6..12 },
                ),
                Score::new(
                    0.48,
                    [2.0, 0.7, 0.45, 0.2],
                    LatencyDistribution { group: 0..12 },
                ),
                Score::new(0.06, [0.5, 0.2, 0.1, 0.0], ConnectionOverhead {}),
            ],
            dbs,
            ..Default::default()
        }
    }

    fn test_connpool_2() -> Spec {
        let mut dbs = vec![];
        for i in 0..6 {
            dbs.push(DBSpec {
                db: i,
                start_at: 0.0,
                end_at: 0.5,
                qps: 1500,
                query_cost: Triangle(0.001, 0.005),
            })
        }
        for i in 6..12 {
            dbs.push(DBSpec {
                db: i,
                start_at: 0.3,
                end_at: 0.7,
                qps: 700,
                query_cost: Triangle(0.03, 0.001),
            })
        }
        for i in 0..6 {
            dbs.push(DBSpec {
                db: i,
                start_at: 0.6,
                end_at: 0.8,
                qps: 700,
                query_cost: Triangle(0.06, 0.01),
            })
        }

        Spec {
            name: "test_connpool_2".into(),
            desc: r#"
                In this test, we have 6x1500qps connections that simulate fast
                queries (0.001..0.006s), and 6x700qps connections that simulate
                slow queries (~0.03s). The algorithm allocates connections
                fairly to both groups, essentially using the
                "demand = avg_query_time * avg_num_of_connection_waiters"
                formula. The QoS is at the same level for all DBs. (Mode B / C)
            "#,
            capacity: 100,
            conn_cost: Triangle(0.04, 0.011),
            score: vec![
                Score::new(
                    0.18,
                    [2.0, 0.5, 0.25, 0.0],
                    LatencyDistribution { group: 0..6 },
                ),
                Score::new(
                    0.28,
                    [2.0, 0.3, 0.1, 0.0],
                    LatencyDistribution { group: 6..12 },
                ),
                Score::new(
                    0.48,
                    [2.0, 0.7, 0.45, 0.2],
                    LatencyDistribution { group: 0..12 },
                ),
                Score::new(0.06, [0.5, 0.2, 0.1, 0.0], ConnectionOverhead {}),
            ],
            dbs,
            ..Default::default()
        }
    }

    fn test_connpool_3() -> Spec {
        let mut dbs = vec![];
        for i in 0..6 {
            dbs.push(DBSpec {
                db: i,
                start_at: 0.0,
                end_at: 0.8,
                qps: 5000,
                query_cost: Triangle(0.01, 0.005),
            })
        }

        Spec {
            name: "test_connpool_3".into(),
            desc: r#"
                This test simply starts 6 same crazy requesters for 6 databases to
                test the pool fairness in Mode C with max capacity of 100.
            "#,
            capacity: 100,
            conn_cost: Triangle(0.04, 0.011),
            score: vec![
                Score::new(
                    0.85,
                    [1.0, 0.2, 0.1, 0.0],
                    LatencyDistribution { group: 0..6 },
                ),
                Score::new(0.15, [0.5, 0.2, 0.1, 0.0], ConnectionOverhead {}),
            ],
            dbs,
            ..Default::default()
        }
    }

    fn test_connpool_4() -> Spec {
        let mut dbs = vec![];
        for i in 0..6 {
            dbs.push(DBSpec {
                db: i,
                start_at: 0.0,
                end_at: 0.8,
                qps: 1000,
                query_cost: Triangle(0.01 * (i as f64 + 1.0), 0.005 * (i as f64 + 1.0)),
            })
        }

        Spec {
            name: "test_connpool_4".into(),
            desc: r#"
                Similar to test 3, this test also has 6 requesters for 6 databases,
                they have the same Q/s but with different query cost. In Mode C,
                we should observe equal connection acquisition latency, fair and
                stable connection distribution and reasonable reconnection cost.
            "#,
            capacity: 50,
            conn_cost: Triangle(0.04, 0.011),
            score: vec![
                Score::new(
                    0.9,
                    [1.0, 0.2, 0.1, 0.0],
                    LatencyDistribution { group: 0..6 },
                ),
                Score::new(0.1, [0.5, 0.2, 0.1, 0.0], ConnectionOverhead {}),
            ],
            dbs,
            ..Default::default()
        }
    }

    fn test_connpool_5() -> Spec {
        let mut dbs = vec![];

        for i in 0..6 {
            dbs.push(DBSpec {
                db: i,
                start_at: 0.0 + i as f64 / 10.0,
                end_at: 0.5 + i as f64 / 10.0,
                qps: 150,
                query_cost: Triangle(0.020, 0.005),
            });
        }
        for i in 6..12 {
            dbs.push(DBSpec {
                db: i,
                start_at: 0.3,
                end_at: 0.7,
                qps: 50,
                query_cost: Triangle(0.008, 0.003),
            });
        }
        for i in 0..6 {
            dbs.push(DBSpec {
                db: i,
                start_at: 0.6,
                end_at: 0.8,
                qps: 50,
                query_cost: Triangle(0.003, 0.002),
            });
        }

        Spec {
            name: "test_connpool_5".into(),
            desc: r#"
                This is a mixed test with pool max capacity set to 6. Requests in
                the first group (0-5) come and go alternatively as time goes on,
                even with different query cost, so its latency similarity doesn't
                matter much, as far as the latency distribution is not too crazy
                and unstable. However the second group (6-11) has a stable
                environment - pressure from the first group is quite even at the
                time the second group works. So we should observe a high similarity
                in the second group. Also due to a low query cost, the second group
                should have a higher priority in connection acquisition, therefore
                a much lower latency distribution comparing to the first group.
                Pool Mode wise, we should observe a transition from Mode A to C,
                then D and eventually back to C. One regression to be aware of is
                that, the last D->C transition should keep the pool running at
                a full capacity.
            "#,
            capacity: 6,
            conn_cost: Triangle(0.15, 0.05),
            score: vec![
                Score::new(
                    0.05,
                    [2.0, 0.8, 0.4, 0.0],
                    LatencyDistribution { group: 0..6 },
                ),
                Score::new(
                    0.25,
                    [2.0, 0.8, 0.4, 0.0],
                    LatencyDistribution { group: 6..12 },
                ),
                Score::new(
                    0.45,
                    [1.0, 2.0, 5.0, 30.0],
                    LatencyRatio {
                        percentile: 75,
                        dividend: 0..6,
                        divisor: 6..12,
                    },
                ),
                Score::new(0.15, [0.5, 0.2, 0.1, 0.0], ConnectionOverhead {}),
                Score::new(0.10, [3.0, 4.0, 5.0, 6.0], EndingCapacity {}),
            ],
            dbs,
            ..Default::default()
        }
    }

    fn test_connpool_6() -> Spec {
        let mut dbs = vec![];

        for i in 0..6 {
            dbs.push(DBSpec {
                db: 0,
                start_at: 0.0 + i as f64 / 10.0,
                end_at: 0.5 + i as f64 / 10.0,
                qps: 150,
                query_cost: Triangle(0.020, 0.005),
            });
        }

        Spec {
            name: "test_connpool_6".into(),
            desc: r#"
                This is a simple test for Mode A. In this case, we don't want to
                have lots of reconnection overhead.
            "#,
            capacity: 6,
            conn_cost: Triangle(0.15, 0.05),
            score: vec![Score::new(1.0, [0.5, 0.2, 0.1, 0.0], ConnectionOverhead {})],
            dbs,
            ..Default::default()
        }
    }

    fn test_connpool_7() -> Spec {
        Spec {
            name: "test_connpool_7".into(),
            desc: r#"
                The point of this test is to have one connection "t1" that
                just has crazy demand for connections.  Then the "t2" connections
                are infrequent -- so they have a miniscule quota.

                Our goal is to make sure that "t2" has good QoS and gets
                its queries processed as soon as they're submitted. Therefore,
                "t2" should have way lower connection acquisition cost than "t1".
            "#,
            capacity: 6,
            conn_cost: Triangle(0.15, 0.05),
            score: vec![
                Score::new(
                    0.2,
                    [1.0, 10.0, 50.0, 100.0],
                    LatencyRatio {
                        percentile: 99,
                        dividend: 1..2,
                        divisor: 2..3,
                    },
                ),
                Score::new(
                    0.4,
                    [1.0, 20.0, 100.0, 200.0],
                    LatencyRatio {
                        percentile: 75,
                        dividend: 1..2,
                        divisor: 2..3,
                    },
                ),
                Score::new(0.4, [0.5, 0.2, 0.1, 0.0], ConnectionOverhead {}),
            ],
            dbs: vec![
                DBSpec {
                    db: 1,
                    start_at: 0.0,
                    end_at: 1.0,
                    qps: 500,
                    query_cost: Triangle(0.040, 0.005),
                },
                DBSpec {
                    db: 2,
                    start_at: 0.1,
                    end_at: 0.3,
                    qps: 30,
                    query_cost: Triangle(0.030, 0.005),
                },
                DBSpec {
                    db: 2,
                    start_at: 0.6,
                    end_at: 0.9,
                    qps: 30,
                    query_cost: Triangle(0.010, 0.005),
                },
            ],
            ..Default::default()
        }
    }

    fn test_connpool_8() -> Spec {
        let base_load = 200;

        Spec {
            name: "test_connpool_8".into(),
            desc: r#"
                This test spec is to check the pool connection reusability with a
                single block before the pool reaches its full capacity in Mode A.
                We should observe just enough number of connects to serve the load,
                while there can be very few disconnects because of GC.
            "#,
            capacity: 100,
            conn_cost: Triangle(0.0, 0.0),
            score: vec![Score::new(1.0, [0.5, 0.2, 0.1, 0.0], ConnectionOverhead {})],
            dbs: vec![
                DBSpec {
                    db: 1,
                    start_at: 0.0,
                    end_at: 0.1,
                    qps: base_load / 4,
                    query_cost: Triangle(0.01, 0.0),
                },
                DBSpec {
                    db: 1,
                    start_at: 0.1,
                    end_at: 0.2,
                    qps: base_load / 2,
                    query_cost: Triangle(0.01, 0.0),
                },
                DBSpec {
                    db: 1,
                    start_at: 0.2,
                    end_at: 0.6,
                    qps: base_load,
                    query_cost: Triangle(0.01, 0.0),
                },
            ],
            ..Default::default()
        }
    }

    fn test_connpool_9() -> Spec {
        let full_qps = 20000;

        Spec {
            name: "test_connpool_9".into(),
            desc: r#"
                This test spec is to check the pool performance with low traffic
                between 3 pre-heated blocks in Mode B. t1 is a reference block,
                t2 has the same qps as t1, but t3 with doubled qps came in while t2
                is active. As the total throughput is low enough, we shouldn't have
                a lot of connects and disconnects, nor a high acquire waiting time.
            "#,
            capacity: 100,
            conn_cost: Triangle(0.01, 0.005),
            score: vec![
                Score::new(
                    0.1,
                    [2.0, 1.0, 0.5, 0.2],
                    LatencyDistribution { group: 1..4 },
                ),
                Score::new(
                    0.1,
                    [0.05, 0.004, 0.002, 0.001],
                    AbsoluteLatency {
                        group: 1..4,
                        percentile: 99,
                    },
                ),
                Score::new(
                    0.2,
                    [0.005, 0.0004, 0.0002, 0.0001],
                    AbsoluteLatency {
                        group: 1..4,
                        percentile: 75,
                    },
                ),
                Score::new(0.6, [0.5, 0.2, 0.1, 0.0], ConnectionOverhead {}),
            ],
            dbs: vec![
                DBSpec {
                    db: 1,
                    start_at: 0.0,
                    end_at: 0.1,
                    qps: (full_qps / 32),
                    query_cost: Triangle(0.01, 0.005),
                },
                DBSpec {
                    db: 1,
                    start_at: 0.1,
                    end_at: 0.4,
                    qps: (full_qps / 16),
                    query_cost: Triangle(0.01, 0.005),
                },
                DBSpec {
                    db: 2,
                    start_at: 0.5,
                    end_at: 0.6,
                    qps: (full_qps / 32),
                    query_cost: Triangle(0.01, 0.005),
                },
                DBSpec {
                    db: 2,
                    start_at: 0.6,
                    end_at: 1.0,
                    qps: (full_qps / 16),
                    query_cost: Triangle(0.01, 0.005),
                },
                DBSpec {
                    db: 3,
                    start_at: 0.7,
                    end_at: 0.8,
                    qps: (full_qps / 16),
                    query_cost: Triangle(0.01, 0.005),
                },
                DBSpec {
                    db: 3,
                    start_at: 0.8,
                    end_at: 0.9,
                    qps: (full_qps / 8),
                    query_cost: Triangle(0.01, 0.005),
                },
            ],
            ..Default::default()
        }
    }

    fn test_connpool_10() -> Spec {
        let full_qps = 2000;

        Spec {
            name: "test_connpool_10".into(),
            desc: r#"
                This test spec is to check the pool garbage collection feature.
                t1 is a constantly-running reference block, t2 starts in the middle
                with a full qps and ends early to leave enough time for the pool to
                execute garbage collection.
            "#,
            timeout: 10,
            duration: 2.0,
            capacity: 100,
            conn_cost: Triangle(0.01, 0.005),
            score: vec![Score::new(
                1.0,
                [100.0, 40.0, 20.0, 10.0],
                EndingCapacity {},
            )],
            dbs: vec![
                DBSpec {
                    db: 1,
                    start_at: 0.0,
                    end_at: 1.0,
                    qps: (full_qps / 32),
                    query_cost: Triangle(0.01, 0.005),
                },
                DBSpec {
                    db: 2,
                    start_at: 0.4,
                    end_at: 0.6,
                    qps: ((full_qps / 32) * 31),
                    query_cost: Triangle(0.01, 0.005),
                },
            ],
            ..Default::default()
        }
    }

    #[test(tokio::test(flavor = "current_thread", start_paused = true))]
    async fn run_spec_tests() -> Result<()> {
        spec_tests(None).await?;
        Ok(())
    }

    async fn spec_tests(scale: Option<f64>) -> Result<SuiteQoS> {
        let mut results = SuiteQoS::default();
        for spec in SPEC_FUNCTIONS {
            let mut spec = spec();
            if let Some(scale) = scale {
                spec.scale(scale);
            }
            let name = spec.name.clone();
            let res = run(spec).await?;
            results.insert(name, res);
        }
        for (name, QoS { qos, .. }) in &results {
            info!("QoS[{name}] = [{qos:.02}]");
        }
        info!(
            "QoS = [{:.02}] (rms={:.02})",
            results.qos(),
            results.qos_rms_error()
        );
        Ok(results)
    }

    /// Runs the specs `count` times, returning the median run.
    #[allow(unused)]
    fn run_specs_tests_in_runtime(count: usize, scale: Option<f64>) -> Result<SuiteQoS> {
        let mut runs = vec![];
        for _ in 0..count {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap();
            let _guard = runtime.enter();
            tokio::time::pause();
            let qos = runtime.block_on(spec_tests(scale))?;
            runs.push(qos);
        }
        runs.sort_by_cached_key(|run| (run.qos_rms_error() * 1_000_000.0) as usize);
        let ret = runs.drain(count / 2..).next().unwrap();
        Ok(ret)
    }

    #[test]
    #[cfg(feature = "optimizer")]
    fn optimizer() {
        use crate::knobs::*;
        use std::sync::atomic::AtomicIsize;

        use genetic_algorithm::strategy::evolve::prelude::*;
        use lru::LruCache;
        use rand::Rng;

        // the search goal to optimize towards (maximize or minimize)
        #[derive(Clone, std::fmt::Debug, smart_default::SmartDefault)]
        pub struct Optimizer {
            #[default(std::sync::Arc::new(AtomicIsize::new(isize::MIN)))]
            best: std::sync::Arc<AtomicIsize>,
            #[default(LruCache::new(100_000_000.try_into().unwrap()))]
            lru: LruCache<[usize; ALL_KNOB_COUNT], isize>,
            #[default(std::time::Instant::now())]
            now: std::time::Instant,
        }

        impl Fitness for Optimizer {
            type Genotype = ContinuousGenotype;
            fn calculate_for_chromosome(
                &mut self,
                chromosome: &Chromosome<Self::Genotype>,
            ) -> Option<FitnessValue> {
                let mut knobs: [usize; ALL_KNOB_COUNT] = Default::default();
                for (knob, gene) in knobs.iter_mut().zip(&chromosome.genes) {
                    *knob = *gene as _;
                }
                if let Some(res) = self.lru.get(&knobs) {
                    return Some(*res);
                }

                for (i, knob) in crate::knobs::ALL_KNOBS.iter().enumerate() {
                    if knob.set(knobs[i]).is_err() {
                        return None;
                    };
                }

                let real = rand::thread_rng().gen_range(0..1000) < 200;
                let weights = if real {
                    [(1.0, 5, None), (0.5, 1, Some(10.0))]
                } else {
                    [(1.0, 5, None), (0.5, 1, None)]
                };
                let outputs =
                    weights.map(|(_, count, scale)| run_specs_tests_in_runtime(count, scale));
                let mut score = 0.0;
                for ((weight, ..), output) in weights.iter().zip(&outputs) {
                    score += weight * output.as_ref().ok()?.qos_rms_error();
                }
                let qos_i = (score * 1_000_000.0) as isize;
                if real && qos_i > self.best.load(std::sync::atomic::Ordering::SeqCst) {
                    eprintln!("{:?} New best: {score:.02} {knobs:?}", self.now.elapsed());
                    eprintln!("{:?}", crate::knobs::ALL_KNOBS);
                    for (weight, output) in weights.iter().zip(outputs) {
                        eprintln!("{weight:?}: {:?}", output.ok()?);
                    }
                    eprintln!("*****************************");
                    self.best.store(qos_i, std::sync::atomic::Ordering::SeqCst);
                }
                self.lru.push(knobs, qos_i);

                Some(qos_i)
            }
        }

        let mut seeds: Vec<Vec<isize>> = vec![];

        // The current state
        seeds.push(
            crate::knobs::ALL_KNOBS
                .iter()
                .map(|k| k.get() as _)
                .collect(),
        );

        // A constant value for all knobs
        for i in 0..100 {
            seeds.push([i].repeat(crate::knobs::ALL_KNOBS.len()));
        }

        // Some randomness
        for _ in 0..100 {
            seeds.push(
                (0..crate::knobs::ALL_KNOBS.len())
                    .map(|_| rand::thread_rng().gen_range(0..1000))
                    .collect(),
            );
        }

        let mut f32_seeds = vec![];
        for mut seed in seeds {
            for (i, knob) in crate::knobs::ALL_KNOBS.iter().enumerate() {
                let mut value = seed[i] as _;
                if knob.set(value).is_err() {
                    knob.clamp(&mut value);
                    seed[i] = value as _;
                };
            }
            f32_seeds.push(seed.into_iter().map(|n| n as _).collect());
        }

        let genotype = ContinuousGenotype::builder()
            .with_genes_size(crate::knobs::ALL_KNOBS.len())
            .with_allele_range(0.0..1000.0)
            .with_allele_neighbour_ranges(vec![-50.0..50.0, -5.0..5.0])
            .with_seed_genes_list(f32_seeds)
            .build()
            .unwrap();

        let mut rng = rand::thread_rng(); // a randomness provider implementing Trait rand::Rng
        let evolve = Evolve::builder()
            .with_multithreading(true)
            .with_genotype(genotype)
            .with_target_population_size(1000)
            .with_target_fitness_score(100 * 1_000_000)
            .with_max_stale_generations(1000)
            .with_fitness(Optimizer::default())
            .with_crossover(CrossoverUniform::new(true))
            .with_mutate(MutateOnce::new(0.5))
            .with_compete(CompeteTournament::new(200))
            .with_extension(ExtensionMassInvasion::new(0.6, 0.6))
            .call(&mut rng)
            .unwrap();
        println!("{}", evolve);
    }

    macro_rules! run_spec {
        ($($spec:ident),* $(,)?) => {
            const SPEC_FUNCTIONS: [fn() -> Spec; [$( $spec ),*].len()] = [
                $(
                    $spec,
                )*
            ];

            mod spec {
                use super::*;
                $(
                    #[super::test(tokio::test(flavor = "current_thread", start_paused = true))]
                    async fn $spec() -> Result<()> {
                        run(super::$spec()).await.map(drop)
                    }
                )*
            }
        };
    }

    run_spec!(
        test_connpool_1,
        test_connpool_2,
        test_connpool_3,
        test_connpool_4,
        test_connpool_5,
        test_connpool_6,
        test_connpool_7,
        test_connpool_8,
        test_connpool_9,
        test_connpool_10,
    );
}
