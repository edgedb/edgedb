use crate::{
    algo::{
        AcquireOp, AlgoState, PoolAlgoTargetData, PoolAlgorithmDataMetrics, PoolConstraints,
        RebalanceOp, ReleaseOp, ReleaseType,
    },
    block::Blocks,
    conn::{ConnError, ConnHandle, ConnResult, Connector},
    drain::Drain,
    metrics::{MetricVariant, PoolMetrics},
    time::Instant,
};
use consume_on_drop::{Consume, ConsumeOnDrop};
use derive_more::Debug;
use std::{cell::Cell, rc::Rc, time::Duration};
use tracing::trace;

#[derive(Debug)]
pub struct PoolConfig {
    pub constraints: PoolConstraints,
    pub adjustment_interval: Duration,
    pub gc_interval: Duration,
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
            gc_interval: Duration::from_secs(1),
            constraints: PoolConstraints {
                max: connections,
                min_idle_time_for_gc: Duration::from_secs(120),
            },
        }
    }

    pub fn with_min_idle_time_for_gc(mut self, min_idle_time_for_gc: Duration) -> Self {
        self.constraints.min_idle_time_for_gc = min_idle_time_for_gc;
        self.gc_interval = (min_idle_time_for_gc / 120).max(Duration::from_secs_f64(0.5));
        self
    }
}

struct HandleAndPool<C: Connector>(ConnHandle<C>, Rc<Pool<C>>, Cell<bool>);

/// An opaque handle representing a RAII lock on a connection in the pool. The
/// underlying connection object may be
pub struct PoolHandle<C: Connector> {
    conn: ConsumeOnDrop<HandleAndPool<C>>,
}

impl<C: Connector> std::fmt::Debug for PoolHandle<C> {
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
    pub(crate) config: PoolConfig,
    blocks: Blocks<C, PoolAlgoTargetData>,
    drain: Drain,
    /// If the pool has been dirtied by acquiring or releasing a connection
    dirty: Rc<Cell<bool>>,
    last_gc: Cell<Instant>,
}

impl<C: Connector> Pool<C> {
    pub fn new(config: PoolConfig, connector: C) -> Rc<Self> {
        config.assert_valid();
        Rc::new(Self {
            config,
            blocks: Default::default(),
            connector,
            dirty: Default::default(),
            drain: Drain::default(),
            last_gc: Instant::now().into(),
        })
    }
}

impl<C: Connector> Pool<C> {
    fn algo(&self) -> AlgoState<'_, Blocks<C, PoolAlgoTargetData>> {
        AlgoState {
            drain: &self.drain,
            blocks: &self.blocks,
            constraints: &self.config.constraints,
        }
    }

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
        // No need to run if we have no blocks
        if self.blocks.is_empty() {
            return;
        }

        self.algo().adjust();

        // Run a garbage collection if we're due
        let since_last_gc = self.last_gc.get().elapsed();
        let gc = if since_last_gc > self.config.gc_interval {
            trace!(
                "GC triggered: time since last GC = {since_last_gc:?} > {:?}",
                self.config.gc_interval
            );
            self.last_gc.set(Instant::now());
            true
        } else {
            false
        };

        for op in self.algo().plan_rebalance(gc) {
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
    pub async fn acquire(self: &Rc<Self>, db: &str) -> ConnResult<PoolHandle<C>, C::Error> {
        self.dirty.set(true);
        let plan = self.algo().plan_acquire(db);
        trace!("Acquire {db}: {plan:?}");
        match plan {
            AcquireOp::Create => {
                tokio::task::spawn_local(self.blocks.task_create_one(&self.connector, db));
            }
            AcquireOp::Steal(from) => {
                tokio::task::spawn_local(self.blocks.task_steal(&self.connector, db, &from));
            }
            AcquireOp::Wait => {}
            AcquireOp::FailInShutdown => {
                return Err(ConnError::Shutdown);
            }
        };
        let conn = self.blocks.queue(db).await?;

        Ok(PoolHandle::new(conn, self.clone()))
    }

    /// Internal release method
    fn release(self: Rc<Self>, conn: ConnHandle<C>, poison: bool) {
        let db = &conn.state.db_name;
        self.dirty.set(true);
        let release_type = if poison {
            ReleaseType::Poison
        } else {
            ReleaseType::Normal
        };
        let plan = self.algo().plan_release(db, release_type);
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

    /// Drain all idle connections to the given database. All connections will be
    /// poisoned on return and this method will return when the given database
    /// is idle. Multiple calls to this method with the same database are valid,
    /// and the drain operation will be kept alive as long as one future has not
    /// been dropped.
    ///
    /// It is valid, though unadvisable, to request a connection during this
    /// period. The connection will be poisoned on return as well.
    ///
    /// Dropping this future cancels the drain operation.
    pub async fn drain_idle(self: Rc<Self>, db: &str) {
        // If the block doesn't exist, we can return
        let Some(name) = self.blocks.name(db) else {
            return;
        };

        let lock = Drain::lock(self.clone(), name);
        while self.blocks.metrics(db).get(MetricVariant::Idle) > 0 {
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
            if all_time[MetricVariant::Failed] == 0 {
                assert_eq!(
                    all_time[MetricVariant::Connecting] + all_time[MetricVariant::Reconnecting],
                    all_time[MetricVariant::Disconnecting],
                    "Connecting + Reconnecting != Disconnecting"
                );
                assert_eq!(
                    all_time[MetricVariant::Disconnecting],
                    all_time[MetricVariant::Closed],
                    "Disconnecting != Closed"
                );
            }
        }
    }
}

impl<C: Connector> AsRef<Drain> for Rc<Pool<C>> {
    fn as_ref(&self) -> &Drain {
        &self.drain
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use anyhow::{Ok, Result};
    use itertools::Itertools;
    use rstest::rstest;

    use test_log::test;
    use tokio::task::LocalSet;
    use tracing::trace;

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
            let config = PoolConfig::suggested_default_for(10)
                .with_min_idle_time_for_gc(Duration::from_secs(1));

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
    #[rstest]
    #[case(1)]
    #[case(3)]
    #[case(10)]
    async fn test_pool_gc_from_max(
        #[case] dbs: usize,
        #[values(10, 100)] pool_size: usize,
    ) -> Result<()> {
        let future = async {
            let config = PoolConfig::suggested_default_for(pool_size)
                .with_min_idle_time_for_gc(Duration::from_secs(1));

            let pool = Pool::new(config, BasicConnector::no_delay());
            let mut conns = vec![];
            for i in 0..10 {
                conns.push(pool.acquire(&format!("{}", i % dbs)).await?);
            }
            drop(conns);

            while !pool.idle() {
                tokio::time::sleep(Duration::from_millis(10)).await;
                pool.run_once();
            }
            trace!("Pool idle, shutting down");

            pool.shutdown().await;
            Ok(())
        };
        tokio::time::timeout(Duration::from_secs(10), LocalSet::new().run_until(future)).await?
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

        crate::test::spec::run(spec).await.map(drop)
    }

    #[test(tokio::test(flavor = "current_thread", start_paused = true))]
    #[rstest]
    #[case::small(1)]
    #[case::medium(10)]
    #[case::large(20)]
    async fn test_pool_failures(#[case] databases: usize) -> Result<()> {
        let spec = Spec {
            name: format!("test_pool_fail50_{databases}").into(),
            desc: "",
            capacity: 10,
            conn_cost: Triangle(0.05, 0.0),
            conn_failure_percentage: 50,
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

        crate::test::spec::run(spec).await.map(drop)
    }
}
