use std::marker::PhantomData;

use crate::{
    block::{Block, Blocks},
    conn::Connector,
};

#[derive(Clone, Debug, PartialEq, Eq)]
enum PoolResult {
    RequiresPromotion,
    Uninitialized,
    Initialized,
}

pub struct PoolConfig {
    pub max_connections: usize,
    pub max_connections_per_db: usize,
}

pub struct Pool<C: Connector>(PoolConfig, PoolInner<C>);

trait PoolInnerImpl<C: Connector> {
    type Promotion: PoolInnerImpl<C> + Into<PoolInner<C>>;
    fn try_acquire(&mut self, config: &PoolConfig, db: &str) -> PoolResult;
    fn promote(self, db: &str) -> Self::Promotion;
}

impl<C: Connector> Pool<C> {
    pub fn new(config: PoolConfig) -> Self {
        Self(config, Default::default())
    }

    // pub async fn acquire(&self, db: &str) -> Result<C: Connector> {

    // }

    // pub async fn release(&self, db: &str, t: T) -> Result<(), E> {

    // }
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
    use super::*;

    #[tokio::test]
    async fn test_pool() {
        let config = PoolConfig {
            max_connections: 10,
            max_connections_per_db: 10,
        };
        // let pool = Pool::new(config);
    }
}
