use crate::{
    algo::{AcquireOp, PoolAlgoTargetData, PoolConstraints, RebalanceOp, ReleaseOp},
    block::Blocks,
    conn::{ConnHandle, ConnResult, Connector},
    metrics::PoolMetrics,
};
use std::{cell::Cell, rc::Rc, time::Duration};

use consume_on_drop::{Consume, ConsumeOnDrop};
use derive_more::Debug;
#[cfg(test)]
use mock_instant::thread_local::Instant;
#[cfg(not(test))]
use std::time::Instant;

#[derive(Debug)]
pub struct PoolConfig {
    pub constraints: PoolConstraints,
    pub adjustment_interval: Duration,
}

impl PoolConfig {
    pub fn assert_valid(&self) {
        assert!(
            self.constraints.max > 0 && self.constraints.max >= self.constraints.max_per_target
        );
        assert!(self.constraints.max_per_target > 0);
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
        if databases == 1 {
            Self {
                adjustment_interval: Duration::from_millis(25),
                constraints: PoolConstraints {
                    max: connections,
                    max_per_target: connections,
                },
            }
        } else {
            Self {
                adjustment_interval: Duration::from_millis(25),
                constraints: PoolConstraints {
                    max: connections,
                    max_per_target: connections / 2,
                },
            }
        }
    }
}

struct HandleAndPool<C: Connector>(ConnHandle<C>, Rc<Pool<C>>, Cell<bool>);

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
/// connection count (aka a quota). A block may take up to its quota, but no
/// more. If a block is over quota, one of its connections may be stolen
/// to satisfy another block's needs.
pub struct Pool<C: Connector> {
    connector: C,
    config: PoolConfig,
    blocks: Blocks<C, PoolAlgoTargetData>,
    last_adjust: Cell<Instant>,
    /// If the pool has been dirties by acquiring or releasing a connection
    dirty: Rc<Cell<bool>>,
}

impl<C: Connector> Pool<C> {
    pub fn new(config: PoolConfig, connector: C) -> Rc<Self> {
        config.assert_valid();
        Rc::new(Self {
            config,
            blocks: Default::default(),
            connector,
            last_adjust: Cell::new(Instant::now()),
            dirty: Default::default(),
        })
    }

    /// Runs the required async task that takes care of quota management, garbage collection,
    /// and other important async tasks. This should happen only if something has changed in
    /// the pool.
    pub async fn run(&self) {
        loop {
            tokio::time::sleep(self.config.adjustment_interval).await;
            self.run_once().await;
        }
    }

    /// Runs the required async task that takes care of quota management, garbage collection,
    /// and other important async tasks. This should happen only if something has changed in
    /// the pool.
    pub async fn run_once(&self) {
        if !self.dirty.take() {
            return;
        }

        self.config.constraints.adjust(&self.blocks);
        for op in self.config.constraints.plan_rebalance(&self.blocks) {
            match op {
                RebalanceOp::Transfer(from, to) => {
                    tokio::task::spawn_local(self.blocks.task_steal(&self.connector, &to, &from));
                }
            }
        }
    }

    /// Acquire a handle from this connection pool. The returned [`PoolHandle`]
    /// controls the lock for the connection and may be dropped to release it
    /// back into the pool.
    pub async fn acquire(self: &Rc<Self>, db: &str) -> ConnResult<PoolHandle<C>> {
        // We have to deal with a few cases:
        //
        // 1. If the block is new, this means we immediately need to re-run the allocation algorithm to
        // determine its quota.
        // 2. If the block is not new and the quota algorithm has not been run within the last
        // `config.adjustment_interval` ms, we re-run the quota algorithm to recompute.
        // 3. If the block is not new and the quota algorithm has been run, we just add ourselves to the
        // wait list (which will potentially give us a connection if there are some free).
        self.dirty.set(true);
        let conn = match self.config.constraints.plan_acquire(db, &self.blocks) {
            AcquireOp::Create => self.blocks.create_if_needed(&self.connector, db).await,
            AcquireOp::Steal(from) => {
                tokio::task::spawn_local(self.blocks.task_steal(&self.connector, db, &from));
                self.blocks.queue(db).await
            }
            AcquireOp::Wait => self.blocks.queue(db).await,
        }?;

        Ok(PoolHandle::new(conn, self.clone()))
    }

    /// Internal release method
    fn release(self: Rc<Self>, conn: ConnHandle<C>, poison: bool) {
        let db = &conn.state.db_name;
        self.dirty.set(true);
        match self
            .config
            .constraints
            .plan_release(db, poison, &self.blocks)
        {
            ReleaseOp::Release => {}
            ReleaseOp::ReleaseTo(db) => {
                tokio::task::spawn_local(self.blocks.task_move_to(&self.connector, conn, &db));
            }
            ReleaseOp::Reopen => {
                tokio::task::spawn_local(self.blocks.task_reopen(&self.connector, conn));
            }
        }
    }

    pub fn metrics(&self) -> PoolMetrics {
        self.blocks.summary()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{pool, test::*};
    use anyhow::{Ok, Result};
    use rstest::rstest;
    use scopeguard::defer;
    use std::rc::Rc;
    use test_log::test;
    use tokio::task::LocalSet;
    use tracing::{info, trace};

    #[test(tokio::test)]
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

                Ok(())
            })
            .await
    }

    #[test(tokio::test(flavor = "current_thread", start_paused = true))]
    #[rstest]
    #[case::small(10)]
    #[case::medium(12)]
    #[case::large(20)]
    async fn test_pool(#[case] databases: usize) -> Result<()> {
        let config = PoolConfig::suggested_default_for(10);

        let local = LocalSet::new();
        let start = Instant::now();
        const CONNECTIONS: usize = 10000;
        info!("Starting tasks");
        let real_time = std::time::Instant::now();
        local
            .run_until(async {
                let mut tasks = vec![];
                let pool = Rc::new(Pool::new(config, BasicConnector::delay()));
                for i in 0..CONNECTIONS {
                    let pool = pool.clone();
                    let task = tokio::task::spawn_local(async move {
                        let db = format!("db-{}", i % databases);
                        trace!("In local task for connection {i} (using {db})");
                        let conn = pool.acquire(&db).await?;
                        virtual_sleep(Duration::from_millis(500)).await;
                        drop(conn);
                        Ok(())
                    });
                    tasks.push(task);
                }
                let monitor = tokio::task::spawn_local(async move {
                    loop {
                        let mut s = "".to_owned();
                        for block in pool.metrics().blocks {
                            s += &format!("{} ", block.total);
                        }
                        trace!("Blocks: {s}");
                        virtual_sleep(Duration::from_millis(100)).await;
                    }
                });
                for task in tasks {
                    task.await??;
                }
                monitor.abort();
                // let metrics = pool.metrics();
                // info!("{metrics:?}");
                Ok(())
            })
            .await?;
        info!(
            "Took {:?} of virtual time ({:?} real time) for {CONNECTIONS} connections to {databases} databases",
            start.elapsed(), real_time.elapsed()
        );
        Ok(())
    }

    async fn run(spec: Spec) -> Result<()> {
        let start = Instant::now();
        let real_time = std::time::Instant::now();
        let connections = Cell::new(0_usize);

        let config = PoolConfig::suggested_default_for(spec.capacity);
        let local = LocalSet::new();
        let mut pool = local
        .run_until(async {
            let pool = Pool::new(config, BasicConnector::delay());
            let mut tasks = vec![];
            for db_spec in spec.dbs {
                let pool = pool.clone();
                tasks.push(tokio::task::spawn_local(async move {
                    let interval = 1.0 / (db_spec.qps as f64);
                    let interval = Duration::from_secs_f64(interval);
                    info!("db {} interval={interval:?}", db_spec.db);
                    virtual_sleep(Duration::from_secs_f64(db_spec.start_at)).await;
                    loop {
                        let db = db_spec.db.clone();
                        let pool = pool.clone();
                        tokio::task::spawn_local(async move {
                            let conn = pool.acquire(&db).await?;
                            virtual_sleep(Duration::from_millis(500)).await;
                            drop(conn);
                            Ok(())
                        });
                        virtual_sleep(interval).await;
                    }
                }));
            }
            let monitor = {
                let pool = pool.clone();
                let monitor = tokio::task::spawn_local(async move {
                    loop {
                        let mut s = "".to_owned();
                        for block in pool.metrics().blocks {
                            s += &format!("{} ", block.total);
                        }
                        trace!("Blocks: {s}");
                        virtual_sleep(Duration::from_millis(100)).await;
                    }
                });
                monitor
            };
            
            info!("Starting...");
            virtual_sleep(Duration::from_secs_f64(spec.duration)).await;
                
            info!(
                "Took {:?} of virtual time ({:?} real time)",
                start.elapsed(), real_time.elapsed()
            );

            monitor.abort();
            _ = monitor.await;
            for task in tasks.drain(..) {
                task.abort();
                _ = task.await;
            }
            // let metrics = pool.metrics();
            // info!("{metrics:?}");
            Ok(pool)
        })
        .await?;
        local.await;

        info!("Shutting down");
        while let Err(pool_) = Rc::try_unwrap(pool) {
            pool = pool_;
            virtual_sleep(Duration::from_millis(100)).await;
        }

        Ok(())
    }

    /// This is a test for Mode D, where 2 groups of blocks race for connections
    /// in the pool with max capacity set to 6. The first group (0-5) has more
    /// dedicated time with the pool, so it should have relatively lower latency
    /// than the second group (6-11). But the QoS is focusing on the latency
    /// distribution similarity, as we don't want to starve only a few blocks
    /// because of the lack of capacity. Therefore, reconnection is a necessary
    /// cost for QoS.
    #[test(tokio::test(flavor = "current_thread", start_paused = true))]
    async fn test_connpool_1() -> Result<()> {
        let mut dbs = vec![];
        for i in 0..6 {
            dbs.push(DBSpec {
                db: format!("t{i}"),
                start_at: 0.0,
                end_at: 0.5,
                qps: 50,
                query_cost_base: 0.03,
                query_cost_var: 0.005,
            })
        }
        for i in 6..12 {
            dbs.push(DBSpec {
                db: format!("t{i}"),
                start_at: 0.3,
                end_at: 0.7,
                qps: 50,
                query_cost_base: 0.03,
                query_cost_var: 0.005,
            })
        }
        for i in 0..6 {
            dbs.push(DBSpec {
                db: format!("t{i}"),
                start_at: 0.6,
                end_at: 0.8,
                qps: 50,
                query_cost_base: 0.03,
                query_cost_var: 0.005,
            })
        }

        let spec = Spec {
            timeout: 20,
            capacity: 6,
            conn_cost_base: 0.05,
            conn_cost_var: 0.01,
            score: vec![
                LatencyDistribution {
                    score: Score {
                        v100: 0.0,
                        v90: 0.25,
                        v60: 0.5,
                        v0: 2.0,
                        weight: 0.18,
                    },
                    group: 0..6,
                }.into(),
                LatencyDistribution {
                    score: Score {
                        v100: 0.0,
                        v90: 0.1,
                        v60: 0.3,
                        v0: 2.0,
                        weight: 0.28,
                    },
                    group: 6..12,
                }.into(),
                LatencyDistribution {
                    score: Score {
                        v100: 0.2,
                        v90: 0.45,
                        v60: 0.7,
                        v0: 2.0,
                        weight: 0.48,
                    },
                    group: 0..12,
                }.into(),
                ConnectionOverhead {
                    score: Score {
                        v100: 40.0,
                        v90: 160.0,
                        v60: 200.0,
                        v0: 300.0,
                        weight: 0.06,
                    },
                }.into(),
            ],
            dbs,
            ..Default::default()
        };

        run(spec).await
    }

}
