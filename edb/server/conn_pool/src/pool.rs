use crate::{
    algo::{PoolAlgoTargetData, PoolConstraints},
    block::Blocks,
    conn::{self, ConnHandle, ConnResult, Connector},
};
use std::{cell::Cell, cmp::Ordering, time::Duration};
use tracing::trace;

#[cfg(test)]
use mock_instant::thread_local::Instant;
#[cfg(not(test))]
use std::time::Instant;

pub struct PoolConfig {
    pub constraints: PoolConstraints,
    pub adjustment_interval: Duration,
}

impl PoolConfig {
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
        Self {
            config,
            blocks: Default::default(),
            connector,
            last_adjust: Cell::new(Instant::now()),
        }
    }

    /// Runs the required async task that takes care of quota management, garbage collection,
    /// and other important async tasks.
    pub async fn run(&self) {
        loop {
            tokio::time::sleep(Duration::from_millis(25)).await;
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

        let target = self.blocks.target(db);
        let current = self.blocks.block_size(db);
        trace!("Target pool size={target} Current size={current}");
        let conn = if target.cmp(&current) == Ordering::Greater {
            // If we've got room in the quota for this block, we can acquire a new connection
            self.blocks.create_if_needed(&self.connector, db).await
        } else {
            self.blocks.queue(db).await
        }?;
        Ok(PoolHandle { conn })
    }
}

#[cfg(test)]
mod tests {
    use crate::test::BasicConnector;

    use super::*;

    #[tokio::test]
    async fn test_pool() {
        let config = PoolConfig::suggested_default_for(10);

        let pool = Pool::new(config, BasicConnector::no_delay());
        let conn1 = pool.acquire("1").await.unwrap();
        let conn2 = pool.acquire("1").await.unwrap();

        drop(conn1);
        drop(conn2);
    }
}
