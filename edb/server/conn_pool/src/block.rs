use crate::{conn::*, waitqueue::WaitQueue};
use futures::FutureExt;
use std::future::Future;
use std::{cell::RefCell, collections::HashMap, future::poll_fn, rc::Rc};

pub struct Block<C: Connector> {
    pub db_name: String,
    conns: RefCell<Vec<Conn<C>>>,
    waiters: Rc<WaitQueue>,
}

impl<C: Connector> Block<C> {
    pub fn new(db: &str) -> Self {
        Self {
            db_name: db.to_owned(),
            conns: Vec::new().into(),
            waiters: Default::default(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.conns.borrow().is_empty()
    }

    pub fn stats(&self) -> ConnStats {
        let mut stats = ConnStats::default();
        for conn in &*self.conns.borrow() {
            stats.count(conn);
        }
        stats
    }

    fn try_acquire_used(&self) -> Option<Conn<C>> {
        for conn in &*self.conns.borrow() {
            if conn.try_lock() {
                return Some(conn.clone());
            }
        }
        None
    }

    fn try_take_used(&self) -> Option<Conn<C>> {
        let mut lock = self.conns.borrow_mut();
        let pos = lock.iter().position(|conn| conn.try_lock());
        if let Some(index) = pos {
            let conn = lock.remove(index);
            return Some(conn);
        }
        None
    }

    async fn reconnect(&self, connector: &C, conn: Conn<C>) -> ConnResult<ConnHandle<C>> {
        self.conns.borrow_mut().push(conn.clone());
        conn.reopen(connector, &self.db_name);
        poll_fn(|cx| conn.poll_ready(cx)).await?;
        Ok(ConnHandle {
            conn,
            waiters: self.waiters.clone(),
        })
    }

    /// Creates or awaits a connection from this block.
    async fn create(&self, connector: &C) -> ConnResult<ConnHandle<C>> {
        let conn = Conn::new(connector.connect(&self.db_name));
        self.conns.borrow_mut().push(conn.clone());
        poll_fn(|cx| conn.poll_ready(cx)).await?;
        Ok(ConnHandle {
            conn,
            waiters: self.waiters.clone(),
        })
    }

    /// Creates or awaits a connection from this block.
    async fn queue(&self, connector: &C) -> ConnResult<ConnHandle<C>> {
        loop {
            eprintln!("loop");
            if let Some(conn) = self.try_acquire_used() {
                return Ok(ConnHandle {
                    conn,
                    waiters: self.waiters.clone(),
                });
            }
            self.waiters.queue().await;
        }
    }

    async fn close_one(&self, connector: &C) -> ConnResult<()> {
        let conn = self
            .try_acquire_used()
            .expect("Could not acquire a connection");
        conn.close(connector);
        poll_fn(|cx| conn.poll_ready(cx)).await?;
        self.conns.borrow_mut().retain(|other| other != &conn);
        Ok(())
    }
}

pub struct Blocks<C: Connector>(RefCell<HashMap<String, Rc<Block<C>>>>);

impl<C: Connector> Default for Blocks<C> {
    fn default() -> Self {
        Self(RefCell::new(HashMap::default()))
    }
}

impl<C: Connector> Blocks<C> {
    pub fn new(block: Block<C>) -> Self {
        let mut map = HashMap::new();
        map.insert(block.db_name.clone(), Rc::new(block));
        Self(RefCell::new(map))
    }

    pub fn block_count(&self) -> usize {
        self.0.borrow().len()
    }

    fn stats(&self, db: &str) -> ConnStats {
        self.0
            .borrow_mut()
            .get(db)
            .map(|b| b.stats())
            .unwrap_or_default()
    }

    fn block(&self, db: &str) -> Rc<Block<C>> {
        self.0
            .borrow_mut()
            .entry(db.to_owned())
            .or_insert_with(|| Rc::new(Block::new(db)))
            .clone()
    }

    async fn create(&self, connector: &C, db: &str) -> ConnResult<ConnHandle<C>> {
        let block = self.block(db);
        block.create(connector).await
    }

    async fn queue(&self, connector: &C, db: &str) -> ConnResult<ConnHandle<C>> {
        let block = self.block(db);
        block.queue(connector).await
    }

    async fn close_one(&self, connector: &C, db: &str) -> ConnResult<()> {
        let block = self.block(db);
        block.close_one(connector).await?;
        if block.is_empty() {
            self.0.borrow_mut().remove(db);
        }
        Ok(())
    }

    async fn steal(&self, connector: &C, db: &str, from: &str) -> ConnResult<ConnHandle<C>> {
        let block = self.block(from);
        let conn = block
            .try_take_used()
            .expect("Could not acquire a connection");
        if block.is_empty() {
            self.0.borrow_mut().remove(from);
        }
        let block = self.block(db);
        block.reconnect(connector, conn).await
    }
}

#[cfg(test)]
mod tests {
    use tokio::task::LocalSet;

    use super::*;

    #[derive(Debug)]
    struct BasicConnector {
        delay: bool,
    }

    impl BasicConnector {
        pub fn no_delay() -> Self {
            BasicConnector { delay: false }
        }
    }

    impl Connector for BasicConnector {
        type Conn = ();
        fn connect(&self, db: &str) -> impl Future<Output = ConnResult<Self::Conn>> + 'static {
            let delay = self.delay;
            async move {
                if delay {
                    tokio::task::yield_now().await
                }
                Ok(())
            }
        }
        fn reconnect(
            &self,
            conn: Self::Conn,
            db: &str,
        ) -> impl Future<Output = ConnResult<Self::Conn>> + 'static {
            let delay = self.delay;
            async move {
                if delay {
                    tokio::task::yield_now().await
                }
                Ok(conn)
            }
        }
        fn disconnect(&self, conn: Self::Conn) -> impl Future<Output = ConnResult<()>> + 'static {
            let delay = self.delay;
            async move {
                if delay {
                    tokio::task::yield_now().await
                }
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn test_block() {
        let connector = BasicConnector::no_delay();
        let block = Rc::new(Block::<BasicConnector>::new("db"));
        let conn = block
            .create(&connector)
            .await
            .expect("Expected a connection");
        assert_eq!(block.stats(), ConnStats::connected(1));
        let local = LocalSet::new();
        let block2 = block.clone();
        local.spawn_local(async move {
            let connector = BasicConnector::no_delay();
            assert_eq!(block2.stats(), ConnStats::connected(1));
            block2
                .queue(&connector)
                .await
                .expect("Expected a connection");
            assert_eq!(block2.stats(), ConnStats::connected(1));
        });
        local.spawn_local(async move {
            tokio::task::yield_now().await;
            drop(conn);
        });
        local.await;
        assert_eq!(block.stats(), ConnStats::connected(1));
    }

    #[tokio::test]
    async fn test_block_parallel_acquire() {
        let connector = BasicConnector::no_delay();
        let block = Rc::new(Block::<BasicConnector>::new("db"));
        block
            .create(&connector)
            .await
            .expect("Expected a connection");
        block
            .create(&connector)
            .await
            .expect("Expected a connection");
        block
            .create(&connector)
            .await
            .expect("Expected a connection");
        assert_eq!(block.stats(), ConnStats::connected(3));

        let local = LocalSet::new();
        for i in 0..100 {
            let block2 = block.clone();
            local.spawn_local(async move {
                let connector = BasicConnector::no_delay();
                for j in 0..i % 10 {
                    tokio::task::yield_now().await;
                }
                block2
                    .queue(&connector)
                    .await
                    .expect("Expected a connection");
            });
        }
        local.await;
        assert_eq!(block.stats(), ConnStats::connected(3));
    }

    #[tokio::test]
    async fn test_steal() {
        let connector = BasicConnector::no_delay();
        let blocks = Blocks::default();
        assert_eq!(0, blocks.block_count());
        blocks
            .create(&connector, "db")
            .await
            .expect("Expected a connection");
        blocks
            .create(&connector, "db")
            .await
            .expect("Expected a connection");
        blocks
            .create(&connector, "db")
            .await
            .expect("Expected a connection");
        assert_eq!(1, blocks.block_count());
        assert_eq!(blocks.stats("db"), ConnStats::connected(3));
        assert_eq!(blocks.stats("db2"), ConnStats::connected(0));
        blocks
            .steal(&connector, "db2", "db")
            .await
            .expect("Expected a connection");
        blocks
            .steal(&connector, "db2", "db")
            .await
            .expect("Expected a connection");
        blocks
            .steal(&connector, "db2", "db")
            .await
            .expect("Expected a connection");
        assert_eq!(1, blocks.block_count());
        assert_eq!(blocks.stats("db"), ConnStats::connected(0));
        assert_eq!(blocks.stats("db2"), ConnStats::connected(3));
    }

    #[tokio::test]
    async fn test_close() {
        let connector = BasicConnector::no_delay();
        let blocks = Blocks::default();
        assert_eq!(0, blocks.block_count());
        blocks
            .create(&connector, "db")
            .await
            .expect("Expected a connection");
        blocks
            .create(&connector, "db")
            .await
            .expect("Expected a connection");
        assert_eq!(1, blocks.block_count());
        assert_eq!(blocks.stats("db"), ConnStats::connected(2));
        blocks.close_one(&connector, "db").await.unwrap();
        blocks.close_one(&connector, "db").await.unwrap();
        assert_eq!(blocks.stats("db"), ConnStats::connected(0));
        assert_eq!(0, blocks.block_count());
    }
}
