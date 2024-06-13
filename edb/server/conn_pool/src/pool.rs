use std::default;
use std::intrinsics::unreachable;
use std::ops::Mul;
use std::{collections::HashMap, future::Future, pin::Pin};
use std::marker::PhantomData;
use futures::FutureExt;

#[derive(Clone, Debug, PartialEq, Eq)]
enum PoolResult {
    RequiresPromotion,
    Uninitialized,
    Initialized,
}

pub struct PoolConfig<T, E> {
    max_connections: usize,
    max_connections_per_db: usize,
    connector: Box<dyn Fn(&str) -> Pin<Box<dyn Future<Output = Result<T, E>>>>>,
    disconnector: Box<dyn Fn(&str, T) -> Pin<Box<dyn Future<Output = Result<(), E>>>>>,
}

impl <T, E> PoolConfig<T, E> {
    pub fn new<F1: Future<Output = Result<T, E>> + 'static, F2: Future<Output = Result<(), E>> + 'static>(max_connections: usize, max_connections_per_db: usize, connector: impl Fn(&str) -> F1 + 'static, disconnector: impl Fn(&str, T) -> F2 + 'static) -> Self {
        Self {
            max_connections,
            max_connections_per_db,
            connector: Box::new(move |db| connector(db).boxed_local()),
            disconnector: Box::new(move |db, t| disconnector(db, t).boxed_local())
        }
    }
}

pub struct Pool<T, E>(PoolConfig<T, E>, PoolInner<T, E>);

trait PoolInnerImpl<T, E> {
    type Promotion: PoolInnerImpl<T, E> + Into<PoolInner<T, E>>;
    fn try_acquire(&mut self, config: &PoolConfig<T, E>, db: &str) -> PoolResult;
    fn promote(self, db: &str) -> Self::Promotion;
}

impl <T, E> Pool <T, E> {
    pub fn new(config: PoolConfig<T, E>) -> Self {
        Self(config, Default::default())
    }

    pub async fn acquire(&self, db: &str) -> Result<T, E> {

    }

    pub async fn release(&self, db: &str, t: T) -> Result<(), E> {

    }
}

struct Conn<T, E> {
    _phantom: PhantomData<(T, E)>
}

enum ConnInner<T, E> {
    Uninitialized,
    Connecting(Pin<Box<dyn Future<Output = Result<T, E>>>>),
    Connected(T),
}

struct Block<T, E> {
    db_name: String,
    conns: Vec<Conn<T, E>>
}

impl <T, E> Block<T, E> {
    pub fn new(db: &str) -> Self {
        Self {
            db_name: db.to_owned(),
            conns: Vec::new(),
        }
    }

    fn try_acquire(&self, config: &PoolConfig<T, E>) -> PoolResult {
        todo!()
    }
}

struct Blocks<T, E>(HashMap<String, Block<T, E>>);

impl <T, E> Blocks<T, E> {
    fn new<T, E>(block: Block<T, E>) -> Self {
        Self(todo!())
    }

    fn conn_count(&self) -> usize {
        todo!()
    }

    fn try_acquire(&self, config: &PoolConfig<T, E>, db: &str) -> PoolResult {
        todo!()
    }
}

#[derive(derive_more::From)]
enum PoolInner<T, E> {
    /// No connections
    EmptyDB(EmptyDBPool<T, E>),
    /// One DB
    SingleDB(SingleDBPool<T, E>),
    /// Multiple DBs, spare capacity
    MultiDB(MultiDBPool<T, E>),
    /// Multiple DBs, max connections
    MultiDBFull(MultiDBFullPool<T, E>),
    /// Multiple DBs, more DBs than connections
    MultiDBOverFull(MultiDBOverFullPool<T, E>),
}

impl <T, E> Default for PoolInner<T, E> {
    fn default() -> Self {
        Self::EmptyDB(EmptyDBPool { _phantom: Default::default() })
    }
}

struct EmptyDBPool<T, E> {
    _phantom: PhantomData<(T, E)>,
}

impl <T, E> PoolInnerImpl<T, E> for EmptyDBPool<T, E> {
    type Promotion = SingleDBPool<T, E>;
    fn try_acquire(&mut self, _config: &PoolConfig<T, E>, db: &str) -> PoolResult {
        PoolResult::RequiresPromotion
    }

    fn promote(self, db: &str) -> Self::Promotion {
        SingleDBPool {
            block: Block::new(db)
        }
    }
}

/// One DB only, no sharing required.
struct SingleDBPool<T, E> {
    block: Block<T, E>,
}

impl <T, E> PoolInnerImpl<T, E> for SingleDBPool<T, E> {
    type Promotion = MultiDBPool<T, E>;
    fn try_acquire(&mut self, config: &PoolConfig<T, E>, db: &str) -> PoolResult {
        if &self.block.db_name == db {
            self.block.try_acquire(&config)
        } else {
            PoolResult::RequiresPromotion
        }
    }

    fn promote(self, db: &str) -> Self::Promotion {
        MultiDBPool {
            blocks: Blocks::new(self.block)
        }
    }
}

/// More than one DB.
struct MultiDBPool<T, E> {
    blocks: Blocks<T, E>,
}

impl <T, E> PoolInnerImpl<T, E> for MultiDBPool<T, E> {
    type Promotion = MultiDBFullPool<T, E>;
    fn try_acquire(&mut self, config: &PoolConfig<T, E>, db: &str) -> PoolResult {
        if self.blocks.conn_count() < config.max_connections {
            self.blocks.try_acquire(&config, &db)
        } else {
            PoolResult::RequiresPromotion
        }
    }
    fn promote(self, db: &str) -> Self::Promotion {
        MultiDBFullPool { blocks: self.blocks }
    }
}

/// More than one DB.
struct MultiDBFullPool<T, E> {
    blocks: Blocks<T, E>,
}

impl <T, E> PoolInnerImpl<T, E> for MultiDBFullPool<T, E> {
    type Promotion = MultiDBOverFullPool<T, E>;
    fn try_acquire(&mut self, config: &PoolConfig<T, E>, db: &str) -> PoolResult {
        if self.blocks.conn_count() < config.max_connections {
            self.blocks.try_acquire(&config, &db)
        } else {
            PoolResult::RequiresPromotion
        }
    }

    fn promote(self, db: &str) -> Self::Promotion {
        MultiDBOverFullPool { blocks: self.blocks }
    }
}

/// More DBs than available connections.
struct MultiDBOverFullPool<T, E> {
    blocks: Blocks<T, E>,
}

impl <T, E> PoolInnerImpl<T, E> for MultiDBOverFullPool<T, E> {
    type Promotion = EmptyDBPool<T, E>;

    fn try_acquire(&mut self, config: &PoolConfig<T, E>, db: &str) -> PoolResult {
        if self.blocks.conn_count() < config.max_connections {
            self.blocks.try_acquire(&config, &db)
        } else {
            PoolResult::RequiresPromotion
        }
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
        let config = PoolConfig::new(10, 10, |db| {
            let db = db.to_owned();
            async {
                tokio::task::yield_now().await;
                Ok::<_, ()>(db)
            }
        }, |db, conn| {
            async {
                Ok::<_, ()>(())
            }
        });
        let pool = Pool::new(config);
    }
}
