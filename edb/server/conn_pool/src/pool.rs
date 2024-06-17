use std::marker::PhantomData;

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

struct PoolHandle<C: Connector> {
    conn: ConnHandle<C>,
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

trait PoolInnerImpl<C: Connector> {
    type Promotion: PoolInnerImpl<C> + Into<PoolInner<C>>;
    fn try_acquire(&mut self, config: &PoolConfig, db: &str) -> PoolResult;
    fn promote(self, db: &str) -> Self::Promotion;
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
        loop {}
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
        eprintln!("{target} {current}");
        let conn = if target > current {
            // If we've got room in the quota for this block, we can acquire a new connection
            self.blocks.create_if_needed(&self.connector, db).await
        } else {
            self.blocks.queue(db).await
        }?;
        Ok(PoolHandle { conn })
    }
}

#[derive(derive_more::From)]
enum PoolInner<C: Connector> {
    /// No connections
    EmptyDB(EmptyDBPool<C>),
    /// One DB
    SingleDB(SingleDBPool<C>),
    /// Multiple DBs, spare capacity
    MultiDB(MultiDBPool<C>),
    /// Multiple DBs, max connections
    MultiDBFull(MultiDBFullPool<C>),
    /// Multiple DBs, more DBs than connections
    MultiDBOverFull(MultiDBOverFullPool<C>),
}

impl<C: Connector> Default for PoolInner<C> {
    fn default() -> Self {
        Self::EmptyDB(EmptyDBPool {
            _phantom: Default::default(),
        })
    }
}

struct EmptyDBPool<C: Connector> {
    _phantom: PhantomData<C>,
}

impl<C: Connector> PoolInnerImpl<C> for EmptyDBPool<C> {
    type Promotion = SingleDBPool<C>;
    fn try_acquire(&mut self, _config: &PoolConfig, db: &str) -> PoolResult {
        PoolResult::RequiresPromotion
    }

    fn promote(self, db: &str) -> Self::Promotion {
        SingleDBPool {
            block: Block::new(db),
        }
    }
}

/// One DB only, no sharing required.
struct SingleDBPool<C: Connector> {
    block: Block<C>,
}

impl<C: Connector> PoolInnerImpl<C> for SingleDBPool<C> {
    type Promotion = MultiDBPool<C>;
    fn try_acquire(&mut self, config: &PoolConfig, db: &str) -> PoolResult {
        // if &self.block.db_name == db {
        //     self.block.try_acquire(&config)
        // } else {
        //     PoolResult::RequiresPromotion
        // }
        todo!()
    }

    fn promote(self, db: &str) -> Self::Promotion {
        MultiDBPool {
            blocks: Blocks::new(self.block),
        }
    }
}

/// More than one DB.
struct MultiDBPool<C: Connector> {
    blocks: Blocks<C>,
}

impl<C: Connector> PoolInnerImpl<C> for MultiDBPool<C> {
    type Promotion = MultiDBFullPool<C>;
    fn try_acquire(&mut self, config: &PoolConfig, db: &str) -> PoolResult {
        // if self.blocks.conn_count() < config.max_connections {
        //     self.blocks.try_acquire(&config, &db)
        // } else {
        //     PoolResult::RequiresPromotion
        // }
        todo!()
    }
    fn promote(self, db: &str) -> Self::Promotion {
        MultiDBFullPool {
            blocks: self.blocks,
        }
    }
}

/// More than one DB.
struct MultiDBFullPool<C: Connector> {
    blocks: Blocks<C>,
}

impl<C: Connector> PoolInnerImpl<C> for MultiDBFullPool<C> {
    type Promotion = MultiDBOverFullPool<C>;
    fn try_acquire(&mut self, config: &PoolConfig, db: &str) -> PoolResult {
        // if self.blocks.conn_count() < config.max_connections {
        //     self.blocks.try_acquire(&config, &db)
        // } else {
        //     PoolResult::RequiresPromotion
        // }
        todo!()
    }

    fn promote(self, db: &str) -> Self::Promotion {
        MultiDBOverFullPool {
            blocks: self.blocks,
        }
    }
}

/// More DBs than available connections.
struct MultiDBOverFullPool<C: Connector> {
    blocks: Blocks<C>,
}

impl<C: Connector> PoolInnerImpl<C> for MultiDBOverFullPool<C> {
    type Promotion = EmptyDBPool<C>;

    fn try_acquire(&mut self, config: &PoolConfig, db: &str) -> PoolResult {
        // if self.blocks.conn_count() < config.max_connections {
        //     self.blocks.try_acquire(&config, &db)
        // } else {
        //     PoolResult::RequiresPromotion
        // }
        todo!()
    }

    fn promote(self, db: &str) -> Self::Promotion {
        unreachable!()
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
