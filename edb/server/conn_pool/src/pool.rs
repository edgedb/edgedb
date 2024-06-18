use std::{marker::PhantomData, time::Duration};

use tracing::trace;

use crate::{
    algo::{PoolAlgoTargetData, PoolConstraints},
    block::{Block, Blocks},
    conn::{ConnHandle, ConnResult, Connector},
};

#[derive(Clone, Debug, PartialEq, Eq)]
enum PoolResult {
    RequiresPromotion,
    Uninitialized,
    Initialized,
}

pub struct PoolConfig {
    pub constraints: PoolConstraints,
}

pub struct PoolHandle<C: Connector> {
    conn: ConnHandle<C>,
}

impl<C: Connector> PoolHandle<C>
where
    C::Conn: Copy,
{
    pub fn handle(&self) -> C::Conn {
        self.conn.conn.with_handle(|c| *c).unwrap()
    }
}

impl<C: Connector> PoolHandle<C>
where
    C::Conn: Clone,
{
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
}

impl<C: Connector> Pool<C> {
    pub fn new(config: PoolConfig, connector: C) -> Self {
        Self {
            config,
            blocks: Default::default(),
            connector,
        }
    }

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
        // TODO: don't add a block?
        self.blocks.prepare(db);
        self.config.constraints.adjust(&self.blocks);
        let target = self.blocks.target(db);
        let current = self.blocks.block_size(db);
        trace!("Target pool size={target} Current size={current}");
        let conn = if target > current {
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
        let config = PoolConfig {
            constraints: PoolConstraints {
                max: 10,
                max_per_target: 10,
            },
        };

        let pool = Pool::new(config, BasicConnector::no_delay());
        let conn1 = pool.acquire("1").await.unwrap();
        let conn2 = pool.acquire("1").await.unwrap();

        drop(conn1);
        drop(conn2);
    }
}
