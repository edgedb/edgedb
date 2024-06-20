use crate::{
    algo::{HasPoolAlgorithmData, VisitPoolAlgoData},
    conn::*,
};
use scopeguard::defer;
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    future::poll_fn,
    rc::Rc,
};
use tracing::trace;

/// Perform a consistency check on entry and exit for this function.
macro_rules! consistency_check {
    ($self:ident) => {
        $self.check_consistency();
        scopeguard::defer!($self.check_consistency());
    };
}

/// Helper trait for [`Cell<usize>`].
trait Counter {
    fn inc(&self);
    fn dec(&self);
}

impl Counter for Cell<usize> {
    fn inc(&self) {
        #[cfg(debug_assertions)]
        self.set(self.get().checked_add(1).unwrap());
        #[cfg(not(debug_assertions))]
        self.set(self.get() + 1);
    }
    fn dec(&self) {
        #[cfg(debug_assertions)]
        self.set(self.get().checked_sub(1).unwrap());
        #[cfg(not(debug_assertions))]
        self.set(self.get() - 1);
    }
}

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
    count: Cell<usize>,
    state: Rc<ConnState>,
    /// Associated data for this block useful for statistics, quotas or other
    /// information.
    data: D,
}

impl<C: Connector, D: Default> Block<C, D> {
    pub fn new(db: &str) -> Self {
        Self {
            db_name: db.to_owned(),
            conns: Vec::new().into(),
            state: Default::default(),
            data: Default::default(),
            count: Default::default(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.conn_count() == 0
    }

    pub fn conn_count(&self) -> usize {
        self.count.get()
    }

    fn conn(&self, conn: Conn<C>) -> ConnHandle<C> {
        ConnHandle::new(conn, self.state.clone())
    }

    #[track_caller]
    pub fn check_consistency(&self) {
        if cfg!(debug_assertions) {
            assert_eq!(
                self.conn_count(),
                self.conns.borrow().len(),
                "Blocks failed consistency check. Total connection count was wrong."
            );
            let conn_metrics = ConnMetrics::default();
            for conn in &*self.conns.borrow() {
                conn_metrics.set(conn.variant())
            }
            assert_eq!(self.metrics(), conn_metrics.into());
        }
    }

    pub fn metrics(&self) -> Rc<ConnMetrics> {
        self.state.metrics.clone()
    }

    fn try_acquire_used(&self) -> Option<Conn<C>> {
        for conn in &*self.conns.borrow() {
            if conn.try_lock(&self.state.metrics) {
                return Some(conn.clone());
            }
        }
        None
    }

    fn try_take_used(&self) -> Option<Conn<C>> {
        consistency_check!(self);
        let mut lock = self.conns.borrow_mut();
        let pos = lock
            .iter()
            .position(|conn| conn.try_lock(&self.state.metrics));
        if let Some(index) = pos {
            let conn = lock.remove(index);
            conn.untrack(&self.state.metrics);
            self.count.dec();
            return Some(conn);
        }
        None
    }

    /// Creates a connection from this block.
    async fn create(&self, connector: &C) -> ConnResult<ConnHandle<C>> {
        consistency_check!(self);
        let conn = Conn::new(connector.connect(&self.db_name), &self.state.metrics);
        self.conns.borrow_mut().push(conn.clone());
        self.count.inc();
        poll_fn(|cx| conn.poll_ready(cx, &self.state.metrics)).await?;
        Ok(self.conn(conn))
    }

    /// Awaits a connection from this block.
    async fn queue(&self) -> ConnResult<ConnHandle<C>> {
        consistency_check!(self);
        loop {
            if let Some(conn) = self.try_acquire_used() {
                trace!("Got a connection");
                return Ok(self.conn(conn));
            }
            trace!("Queueing for a connection");
            self.state.waiters.queue().await;
        }
    }

    /// Awaits a connection from this block.
    async fn create_if_needed(&self, connector: &C) -> ConnResult<(bool, ConnHandle<C>)> {
        consistency_check!(self);
        if let Some(conn) = self.try_acquire_used() {
            return Ok((false, self.conn(conn)));
        }
        Ok((true, self.create(connector).await?))
    }

    /// Close one of idle connections in this block
    async fn close_one(&self, connector: &C) -> ConnResult<()> {
        consistency_check!(self);
        let conn = self
            .try_acquire_used()
            .expect("Could not acquire a connection");
        conn.close(connector, &self.state.metrics);
        poll_fn(|cx| conn.poll_ready(cx, &self.state.metrics)).await?;
        self.conns.borrow_mut().retain(|other| other != &conn);
        conn.untrack(&self.metrics());
        self.count.dec();
        Ok(())
    }

    async fn reconnect(
        from: &Block<C, D>,
        to: &Block<C, D>,
        connector: &C,
    ) -> ConnResult<ConnHandle<C>> {
        consistency_check!(from);
        consistency_check!(to);

        let conn = from
            .try_take_used()
            .expect("Could not acquire a connection");
        to.conns.borrow_mut().push(conn.clone());
        to.count.inc();
        conn.reopen(connector, &to.state.metrics, &to.db_name);
        poll_fn(|cx| conn.poll_ready(cx, &to.state.metrics)).await?;
        Ok(to.conn(conn))
    }
}

/// Manages the connection state for a number of backend databases. See
/// the notes on [`Block`] for the scope of responsibility of this struct.
pub struct Blocks<C: Connector, D: Default = ()> {
    map: RefCell<HashMap<String, Rc<Block<C, D>>>>,
    /// A cached count
    count: Cell<usize>,
}

impl<C: Connector, D: Default> Default for Blocks<C, D> {
    fn default() -> Self {
        Self {
            map: RefCell::new(HashMap::default()),
            count: Cell::default(),
        }
    }
}

impl<C: Connector, D: HasPoolAlgorithmData + Default> VisitPoolAlgoData<D> for Blocks<C, D> {
    #[inline]
    fn with_algo_data_all(&self, mut f: impl FnMut(&str, &D)) {
        for it in self.map.borrow().values() {
            f(&it.db_name, &it.data)
        }
    }
    #[inline]
    fn with_algo_data<T>(&self, db: &str, f: impl Fn(&D) -> T) -> Option<T> {
        self.map.borrow().get(db).map(|d| f(&d.data))
    }
}

impl<C: Connector, D: Default> Blocks<C, D> {
    #[track_caller]
    pub fn check_consistency(&self) {
        if cfg!(debug_assertions) {
            let mut total = 0;
            for block in self.map.borrow().values() {
                block.check_consistency();
                total += block.conn_count();
            }
            if total != self.count.get() {
                if tracing::enabled!(tracing::Level::TRACE) {
                    for block in self.map.borrow().values() {
                        trace!("{}: {}", block.db_name, block.conn_count());
                    }
                }
            }
            assert_eq!(
                total,
                self.count.get(),
                "Blocks failed consistency check. Total connection count ({total}) was wrong."
            );
        }
    }

    pub fn prepare(&self, db: &str) {
        _ = self.block(db)
    }

    pub fn contains(&self, db: &str) -> bool {
        self.map.borrow().contains_key(db)
    }

    pub fn block_count(&self) -> usize {
        self.map.borrow().len()
    }

    pub fn conn_count(&self) -> usize {
        self.count.get()
    }

    pub fn block_conn_count(&self, db: &str) -> usize {
        self.block(db).conn_count()
    }

    fn metrics(&self, db: &str) -> Rc<ConnMetrics> {
        self.map
            .borrow_mut()
            .get(db)
            .map(|b| b.metrics())
            .unwrap_or_default()
    }

    fn block(&self, db: &str) -> Rc<Block<C, D>> {
        self.map
            .borrow_mut()
            .entry(db.to_owned())
            .or_insert_with(|| Rc::new(Block::new(db)))
            .clone()
    }

    pub async fn create(&self, connector: &C, db: &str) -> ConnResult<ConnHandle<C>> {
        consistency_check!(self);
        defer!(self.count.inc());
        let block = self.block(db);
        block.create(connector).await
    }

    pub async fn queue(&self, db: &str) -> ConnResult<ConnHandle<C>> {
        consistency_check!(self);
        let block = self.block(db);
        block.queue().await
    }

    pub async fn create_if_needed(&self, connector: &C, db: &str) -> ConnResult<ConnHandle<C>> {
        consistency_check!(self);
        let block = self.block(db);
        let (new, c) = block.create_if_needed(connector).await?;
        if new {
            self.count.inc()
        }
        Ok(c)
    }

    pub async fn close_one(&self, connector: &C, db: &str) -> ConnResult<()> {
        consistency_check!(self);
        defer!(self.count.dec());
        let block = self.block(db);
        block.close_one(connector).await?;
        if block.is_empty() {
            self.map.borrow_mut().remove(db);
        }
        Ok(())
    }

    pub async fn steal(&self, connector: &C, db: &str, from: &str) -> ConnResult<ConnHandle<C>> {
        let from_block = self.block(from);
        let to_block = self.block(db);
        Block::reconnect(&from_block, &to_block, connector).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use anyhow::{Ok, Result};
    use pretty_assertions::assert_eq;
    use test_log::test;
    use tokio::task::LocalSet;

    #[test(tokio::test)]
    async fn test_block() -> Result<()> {
        let connector = BasicConnector::no_delay();
        let block = Rc::new(Block::<BasicConnector>::new("db"));
        let conn = block
            .create(&connector)
            .await
            .expect("Expected a connection");
        assert_eq!(
            block.metrics(),
            ConnMetrics::with(ConnStateVariant::Active, 1).into()
        );
        let local = LocalSet::new();
        let block2 = block.clone();
        local.spawn_local(async move {
            let connector = BasicConnector::no_delay();
            assert_eq!(
                block2.metrics(),
                ConnMetrics::with(ConnStateVariant::Active, 1).into()
            );
            block2.queue().await?;
            assert_eq!(
                block2.metrics(),
                ConnMetrics::with(ConnStateVariant::Active, 1).into()
            );
            anyhow::Ok(())
        });
        local.spawn_local(async move {
            tokio::task::yield_now().await;
            drop(conn);
        });
        local.await;
        assert_eq!(
            block.metrics(),
            ConnMetrics::with(ConnStateVariant::Idle, 1).into()
        );
        Ok(())
    }

    #[test(tokio::test)]
    async fn test_block_parallel_acquire() -> Result<()> {
        let connector = BasicConnector::no_delay();
        let block = Rc::new(Block::<BasicConnector>::new("db"));
        block.create(&connector).await?;
        block.create(&connector).await?;
        block.create(&connector).await?;
        assert_eq!(
            block.metrics(),
            ConnMetrics::with(ConnStateVariant::Idle, 3).into()
        );

        let local = LocalSet::new();
        for i in 0..100 {
            let block2 = block.clone();
            local.spawn_local(async move {
                let connector = BasicConnector::no_delay();
                for j in 0..i % 10 {
                    tokio::task::yield_now().await;
                }
                block2.queue().await
            });
        }
        local.await;
        assert_eq!(
            block.metrics(),
            ConnMetrics::with(ConnStateVariant::Idle, 3).into()
        );
        Ok(())
    }

    #[test(tokio::test)]
    async fn test_steal() -> Result<()> {
        let connector = BasicConnector::no_delay();
        let blocks = Blocks::<_, ()>::default();
        assert_eq!(0, blocks.block_count());
        blocks.create(&connector, "db").await?;
        blocks.create(&connector, "db").await?;
        blocks.create(&connector, "db").await?;
        assert_eq!(1, blocks.block_count());
        assert_eq!(
            blocks.metrics("db"),
            ConnMetrics::with(ConnStateVariant::Idle, 3).into()
        );
        assert_eq!(blocks.metrics("db2"), ConnMetrics::default().into());
        blocks.steal(&connector, "db2", "db").await?;
        blocks.steal(&connector, "db2", "db").await?;
        blocks.steal(&connector, "db2", "db").await?;
        // Block hasn't been GC'd yet
        assert_eq!(2, blocks.block_count());
        assert_eq!(blocks.metrics("db"), ConnMetrics::default().into());
        assert_eq!(
            blocks.metrics("db2"),
            ConnMetrics::with(ConnStateVariant::Idle, 3).into()
        );
        Ok(())
    }

    #[test(tokio::test)]
    async fn test_close() -> Result<()> {
        let connector = BasicConnector::no_delay();
        let blocks = Blocks::<_, ()>::default();
        assert_eq!(0, blocks.block_count());
        blocks.create(&connector, "db").await?;
        blocks.create(&connector, "db").await?;
        assert_eq!(1, blocks.block_count());
        assert_eq!(
            blocks.metrics("db"),
            ConnMetrics::with(ConnStateVariant::Idle, 2).into()
        );
        blocks.close_one(&connector, "db").await?;
        blocks.close_one(&connector, "db").await?;
        assert_eq!(blocks.metrics("db"), ConnMetrics::default().into());
        assert_eq!(0, blocks.block_count());
        Ok(())
    }
}
