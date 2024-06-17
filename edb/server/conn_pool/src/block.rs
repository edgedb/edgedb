use crate::{
    algo::{HasPoolAlgorithmData, PoolAlgoTargetData, PoolAlgorithmData, VisitPoolAlgoData},
    conn::{self, *},
};
use std::{cell::RefCell, collections::HashMap, future::poll_fn, iter::Map, rc::Rc};

/// Manages the connection state for a single backend database. This is only a
/// set of connections, and does not understand policy, balancing or anything
/// outside of a request to make a new connection, or a request to disconnect
/// and existing connection. It also manages connection statistics for higher
/// layers of code to make decisions.
///
/// If the block does not contain enough connections to satisfy a request, a
/// request may queue itself for the next available connection.
///
/// The block has an associated data generic parameter that may be provided where
/// additional metadata for this block can live.
pub struct Block<C: Connector, D: Default = ()> {
    pub db_name: String,
    conns: RefCell<Vec<Conn<C>>>,
    state: Rc<ConnState>,
    /// Associated data for this block useful for statistics, quotas or other
    /// information.
    data: RefCell<D>,
}

impl<C: Connector, D: Default> Block<C, D> {
    pub fn new(db: &str) -> Self {
        Self {
            db_name: db.to_owned(),
            conns: Vec::new().into(),
            state: Default::default(),
            data: Default::default(),
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
        Ok(ConnHandle::new(conn, self.state.clone()))
    }

    /// Creates a connection from this block.
    async fn create(&self, connector: &C) -> ConnResult<ConnHandle<C>> {
        let conn = Conn::new(connector.connect(&self.db_name));
        self.conns.borrow_mut().push(conn.clone());
        poll_fn(|cx| conn.poll_ready(cx)).await?;
        Ok(ConnHandle::new(conn, self.state.clone()))
    }

    /// Awaits a connection from this block.
    async fn queue(&self) -> ConnResult<ConnHandle<C>> {
        loop {
            eprintln!("loop");
            if let Some(conn) = self.try_acquire_used() {
                return Ok(ConnHandle::new(conn, self.state.clone()));
            }
            self.state.waiters.queue().await;
        }
    }

    /// Awaits a connection from this block.
    async fn create_if_needed(&self, connector: &C) -> ConnResult<ConnHandle<C>> {
        if let Some(conn) = self.try_acquire_used() {
            return Ok(ConnHandle::new(conn, self.state.clone()));
        }
        self.create(connector).await
    }

    /// Close one of idle connections in this block
    async fn close_one(&self, connector: &C) -> ConnResult<()> {
        let conn = self
            .try_acquire_used()
            .expect("Could not acquire a connection");
        conn.close(connector);
        poll_fn(|cx| conn.poll_ready(cx)).await?;
        self.conns.borrow_mut().retain(|other| other != &conn);
        Ok(())
    }

    #[inline(always)]
    fn with_data<T>(&self, mut f: impl FnOnce(&mut D) -> T) -> T {
        f(&mut *self.data.borrow_mut())
    }
}

/// Manages the connection state for a number of backend databases. See
/// the notes on [`Block`] for the scope of responsibility of this struct.
pub struct Blocks<C: Connector, D: Default = ()>(RefCell<HashMap<String, Rc<Block<C, D>>>>);

impl<C: Connector, D: Default> Default for Blocks<C, D> {
    fn default() -> Self {
        Self(RefCell::new(HashMap::default()))
    }
}

impl<C: Connector, D: HasPoolAlgorithmData + Default> VisitPoolAlgoData<D> for &Blocks<C, D> {
    fn with_algo_data_all(&self, mut f: impl FnMut(&D)) {
        for it in self.0.borrow().values() {
            it.with_data(|data| f(data))
        }
    }
}

/// Expose the pool algorithm data if the data supports it.
impl<C: Connector, D: HasPoolAlgorithmData + Default> Blocks<C, D> {
    #[inline(always)]
    pub fn with_algo_data<T>(&self, db: &str, f: impl FnOnce(&PoolAlgorithmData) -> T) -> T {
        self.block(db).with_data(move |data| data.with_algo_data(f))
    }

    #[inline(always)]
    pub fn target(&self, db: &str) -> usize {
        self.block(db).with_data(|data| data.target())
    }

    #[inline(always)]
    pub fn stealability(&self, db: &str) -> usize {
        self.block(db).with_data(|data| data.stealability())
    }
}

impl<C: Connector, D: Default> Blocks<C, D> {
    pub fn new(block: Block<C, D>) -> Self {
        let mut map = HashMap::new();
        map.insert(block.db_name.clone(), Rc::new(block));
        Self(RefCell::new(map))
    }

    pub fn prepare(&self, db: &str) {
        _ = self.block(db)
    }

    pub fn block_count(&self) -> usize {
        self.0.borrow().len()
    }

    pub fn block_size(&self, db: &str) -> usize {
        self.block(db).conns.borrow().len()
    }

    fn stats(&self, db: &str) -> ConnStats {
        self.0
            .borrow_mut()
            .get(db)
            .map(|b| b.stats())
            .unwrap_or_default()
    }

    fn block(&self, db: &str) -> Rc<Block<C, D>> {
        self.0
            .borrow_mut()
            .entry(db.to_owned())
            .or_insert_with(|| Rc::new(Block::new(db)))
            .clone()
    }

    pub async fn create(&self, connector: &C, db: &str) -> ConnResult<ConnHandle<C>> {
        let block = self.block(db);
        block.create(connector).await
    }

    pub async fn queue(&self, db: &str) -> ConnResult<ConnHandle<C>> {
        let block = self.block(db);
        block.queue().await
    }

    pub async fn create_if_needed(&self, connector: &C, db: &str) -> ConnResult<ConnHandle<C>> {
        let block = self.block(db);
        block.create_if_needed(connector).await
    }

    pub async fn close_one(&self, connector: &C, db: &str) -> ConnResult<()> {
        let block = self.block(db);
        block.close_one(connector).await?;
        if block.is_empty() {
            self.0.borrow_mut().remove(db);
        }
        Ok(())
    }

    pub async fn steal(&self, connector: &C, db: &str, from: &str) -> ConnResult<ConnHandle<C>> {
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
    use crate::test::*;
    use tokio::task::LocalSet;

    use super::*;

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
            block2.queue().await.expect("Expected a connection");
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
                block2.queue().await.expect("Expected a connection");
            });
        }
        local.await;
        assert_eq!(block.stats(), ConnStats::connected(3));
    }

    #[tokio::test]
    async fn test_steal() {
        let connector = BasicConnector::no_delay();
        let blocks = Blocks::<_, ()>::default();
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
        let blocks = Blocks::<_, ()>::default();
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
