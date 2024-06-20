use crate::{
    algo::{PoolAlgoTargetData, PoolConstraints, VisitPoolAlgoData},
    block::Blocks,
    conn::{ConnHandle, ConnResult, Connector},
};
use std::{cell::Cell, time::Duration};

#[cfg(test)]
use mock_instant::thread_local::Instant;
#[cfg(not(test))]
use std::time::Instant;

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

#[derive(Debug)]
pub struct PoolHandle<C: Connector> {
    conn: ConnHandle<C>,
}

impl<C: Connector> PoolHandle<C> {
    /// Marks this handle as poisoned, which will not allow it to be reused in the pool. The
    /// most likely case for this is that the underlying connection's stream has closed, or
    /// the remote end is no longer valid for some reason.
    pub fn poison(&self) {
        self.conn.poison()
    }

    #[inline(always)]
    pub fn with_handle<T>(&self, f: impl Fn(&C::Conn) -> T) -> T {
        self.conn.conn.with_handle(f).unwrap()
    }
}

impl<C: Connector> PoolHandle<C>
where
    C::Conn: Copy,
{
    /// If the handle is `Copy`, copies this handle.
    #[inline(always)]
    pub fn handle(&self) -> C::Conn {
        self.conn.conn.with_handle(|c| *c).unwrap()
    }
}

impl<C: Connector> PoolHandle<C>
where
    C::Conn: Clone,
{
    /// If the handle is `Clone`, clones this handle.
    #[inline(always)]
    pub fn handle_clone(&self) -> C::Conn {
        self.conn.conn.with_handle(|c| c.clone()).unwrap()
    }
}

/// A connection pool consists of a number of blocks, each with a target
/// connection count (aka a quota). A block may take up to its quota, but no
/// more. If a block is over quota, one of its connections may be stolen
/// to satisfy another block's needs.
pub struct Pool<C: Connector> {
    connector: C,
    config: PoolConfig,
    blocks: Blocks<C, PoolAlgoTargetData>,
    last_adjust: Cell<Instant>,
}

impl<C: Connector> Pool<C> {
    pub fn new(config: PoolConfig, connector: C) -> Self {
        config.assert_valid();
        Self {
            config,
            blocks: Default::default(),
            connector,
            last_adjust: Cell::new(Instant::now()),
        }
    }

    /// Runs the required async task that takes care of quota management, garbage collection,
    /// and other important async tasks. This should happen only if something has changed in
    /// the pool.
    pub async fn run(&self) {
        loop {
            tokio::time::sleep(self.config.adjustment_interval).await;
            self.config.constraints.adjust(&self.blocks);
        }
    }

    /// Acquire a handle from this connection pool. The returned [`PoolHandle`]
    /// controls the lock for the connection and may be dropped to release it
    /// back into the pool.
    pub async fn acquire(&self, db: &str) -> ConnResult<PoolHandle<C>> {
        // We have to deal with a few cases:
        //
        // 1. If the block is new, this means we immediately need to re-run the allocation algorithm to
        // determine its quota.
        // 2. If the block is not new and the quota algorithm has not been run within the last
        // `config.adjustment_interval` ms, we re-run the quota algorithm to recompute.
        // 3. If the block is not new and the quota algorithm has been run, we just add ourselves to the
        // wait list (which will potentially give us a connection if there are some free).

        if !self.blocks.contains(db) {
            self.blocks.prepare(db);
            self.config.constraints.adjust(&self.blocks);
        } else if self.last_adjust.get().elapsed() > self.config.adjustment_interval {
            self.config.constraints.adjust(&self.blocks);
        }

        let target_block_size = self.blocks.target(db);
        let current_block_size = self.blocks.block_conn_count(db);
        let current_pool_size = self.blocks.conn_count();
        let max_pool_size = self.config.constraints.max;

        let pool_is_full = current_pool_size >= max_pool_size;
        let block_has_room = current_block_size < target_block_size;

        let conn = if pool_is_full && block_has_room {
            // We need to try to steal a connection
            if let Some(from) = self.config.constraints.identify_victim(&self.blocks) {
                self.blocks.steal(&self.connector, db, &from).await
            } else {
                self.blocks.queue(db).await
            }
        } else if block_has_room {
            self.blocks.create_if_needed(&self.connector, db).await
        } else {
            self.blocks.queue(db).await
        }?;

        Ok(PoolHandle { conn })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::BasicConnector;
    use anyhow::{Ok, Result};
    use std::rc::Rc;
    use test_log::test;
    use tokio::task::LocalSet;
    use tracing::trace;

    #[test(tokio::test)]
    async fn test_pool_basic() -> Result<()> {
        let config = PoolConfig::suggested_default_for(10);

        let pool = Pool::new(config, BasicConnector::no_delay());
        let conn1 = pool.acquire("1").await?;
        let conn2 = pool.acquire("1").await?;

        drop(conn1);
        drop(conn2);
        Ok(())
    }

    #[test(tokio::test)]
    async fn test_pool_large() -> Result<()> {
        let config = PoolConfig::suggested_default_for(10);

        let local = LocalSet::new();
        local
            .run_until(async {
                let mut tasks = vec![];
                let pool = Rc::new(Pool::new(config, BasicConnector::no_delay()));
                for i in 0..100 {
                    let pool = pool.clone();
                    let task = tokio::task::spawn_local(async move {
                        trace!("In local task");
                        let db = format!("db-{}", i % 10);
                        let conn = pool.acquire(&db).await?;
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        drop(conn);
                        Ok(())
                    });
                    tasks.push(task);
                }
                for task in tasks {
                    task.await??;
                }
                Ok(())
            })
            .await
    }
}
