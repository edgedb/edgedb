use crate::{
    algo::{PoolAlgoTargetData, VisitPoolAlgoData},
    conn::*,
    metrics::{MetricVariant, MetricsAccum, PoolMetrics},
    waitqueue::WaitQueue,
};
use futures::future::Either;
use std::{
    cell::RefCell,
    collections::{BTreeSet, HashMap},
    future::{poll_fn, ready, Future},
    rc::Rc,
};
use tracing::trace;

#[cfg(test)]
use mock_instant::thread_local::Instant;
#[cfg(not(test))]
use std::time::Instant;

/// Perform a consistency check on entry and exit for this function.
macro_rules! consistency_check {
    ($self:ident) => {
        // On entry
        $self.check_consistency();
        // On exit
        scopeguard::defer!($self.check_consistency());
    };
}

/// A cheaply cloneable name string.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, derive_more::Debug, derive_more::Display)]
pub struct Name(Rc<String>);

impl From<&str> for Name {
    fn from(value: &str) -> Self {
        Name(Rc::new(String::from(value)))
    }
}

impl AsRef<str> for Name {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl std::ops::Deref for Name {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        self.0.as_str()
    }
}

impl std::borrow::Borrow<str> for Name {
    fn borrow(&self) -> &str {
        self.0.as_str()
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
    pub db_name: Name,
    conns: RefCell<Vec<Conn<C>>>,
    state: Rc<ConnState>,
    /// Associated data for this block useful for statistics, quotas or other
    /// information.
    data: D,
}

impl<C: Connector, D: Default> Block<C, D> {
    pub fn new(db_name: Name, parent_metrics: Option<Rc<MetricsAccum>>) -> Self {
        let metrics = Rc::new(MetricsAccum::new(parent_metrics));
        let state = ConnState {
            db_name: db_name.clone(),
            waiters: WaitQueue::new(),
            metrics,
        }
        .into();
        Self {
            db_name,
            conns: Vec::new().into(),
            state,
            data: Default::default(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.state.metrics.total() == 0
    }

    pub fn conn_count(&self) -> usize {
        self.state.metrics.total()
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
            let conn_metrics = MetricsAccum::default();
            for conn in &*self.conns.borrow() {
                conn_metrics.insert(conn.variant())
            }
            conn_metrics.set_value(MetricVariant::Waiting, self.state.waiters.lock.get());
            assert_eq!(self.metrics().summary().value, conn_metrics.summary().value);
        }
    }

    #[inline]
    pub fn metrics(&self) -> Rc<MetricsAccum> {
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
            return Some(conn);
        }
        None
    }

    /// Creates a connection from this block.
    fn create(self: Rc<Self>, connector: &C) -> impl Future<Output = ConnResult<ConnHandle<C>>> {
        let conn = {
            consistency_check!(self);
            let conn = Conn::new(connector.connect(&self.db_name), &self.state.metrics);
            self.conns.borrow_mut().push(conn.clone());
            conn
        };
        async move {
            consistency_check!(self);
            poll_fn(|cx| conn.poll_ready(cx, &self.state.metrics)).await?;
            Ok(self.conn(conn))
        }
    }

    /// Awaits a connection from this block.
    fn queue(self: Rc<Self>) -> impl Future<Output = ConnResult<ConnHandle<C>>> {
        if let Some(conn) = self.try_acquire_used() {
            trace!("Got a connection");
            return Either::Left(ready(Ok(self.conn(conn))));
        }
        // Update the metrics now before we actually queue
        self.state.waiters.lock();
        self.state.metrics.insert(MetricVariant::Waiting);
        let state = self.state.clone();
        let now = Instant::now();
        let guard = scopeguard::guard((), move |_| {
            state.waiters.unlock();
            state
                .metrics
                .remove_time(MetricVariant::Waiting, now.elapsed());
        });
        Either::Right(async move {
            consistency_check!(self);
            loop {
                if let Some(conn) = self.try_acquire_used() {
                    trace!("Got a connection");
                    drop(guard);
                    return Ok(self.conn(conn));
                }
                trace!("Queueing for a connection");
                self.state.waiters.queue().await;
            }
        })
    }

    /// Awaits a connection from this block.
    fn create_if_needed(
        self: Rc<Self>,
        connector: &C,
    ) -> impl Future<Output = ConnResult<ConnHandle<C>>> {
        if let Some(conn) = self.try_acquire_used() {
            return Either::Left(ready(Ok(self.conn(conn))));
        }
        Either::Right(self.create(connector))
    }

    /// Close one of idle connections in this block
    fn task_close_one(self: Rc<Self>, connector: &C) -> impl Future<Output = ConnResult<()>> {
        let conn = {
            consistency_check!(self);
            let conn = self
                .try_acquire_used()
                .expect("Could not acquire a connection");
            conn.close(connector, &self.state.metrics);
            conn
        };
        async move {
            consistency_check!(self);
            poll_fn(|cx| conn.poll_ready(cx, &self.state.metrics)).await?;
            // TODO: this can be replaced by moving the final item of the list into the
            // empty spot to avoid reshuffling
            self.conns.borrow_mut().retain(|other| other != &conn);
            conn.untrack(&self.state.metrics);
            Ok(())
        }
    }

    fn task_reconnect(
        from: Rc<Block<C, D>>,
        to: Rc<Block<C, D>>,
        connector: &C,
    ) -> impl Future<Output = ConnResult<()>> {
        let conn = {
            consistency_check!(from);
            consistency_check!(to);

            let conn = from
                .try_take_used()
                .expect("Could not acquire a connection");
            to.conns.borrow_mut().push(conn.clone());
            conn.reopen(connector, &to.state.metrics, &to.db_name);
            conn
        };
        async move {
            consistency_check!(from);
            consistency_check!(to);
            poll_fn(|cx| conn.poll_ready(cx, &to.state.metrics)).await?;
            _ = to.conn(conn);
            Ok(())
        }
    }

    fn task_reconnect_conn(
        from: Rc<Block<C, D>>,
        to: Rc<Block<C, D>>,
        mut conn: ConnHandle<C>,
        connector: &C,
    ) -> impl Future<Output = ConnResult<()>> {
        let conn = {
            consistency_check!(from);
            consistency_check!(to);

            // TODO: this can be replaced by moving the final item of the list into the
            // empty spot to avoid reshuffling
            from.conns.borrow_mut().retain(|other| other != &conn.conn);
            conn.conn.untrack(&from.state.metrics);
            conn.state = to.state.clone();
            to.conns.borrow_mut().push(conn.conn.clone());
            conn.conn.reopen(connector, &to.state.metrics, &to.db_name);
            conn
        };
        async move {
            consistency_check!(from);
            consistency_check!(to);
            poll_fn(|cx| conn.conn.poll_ready(cx, &to.state.metrics)).await?;
            Ok(())
        }
    }
}

/// Manages the connection state for a number of backend databases. See
/// the notes on [`Block`] for the scope of responsibility of this struct.
pub struct Blocks<C: Connector, D: Default = ()> {
    map: RefCell<HashMap<Name, Rc<Block<C, D>>>>,
    metrics: Rc<MetricsAccum>,
}

impl<C: Connector, D: Default> Default for Blocks<C, D> {
    fn default() -> Self {
        Self {
            map: RefCell::new(HashMap::default()),
            metrics: Rc::new(MetricsAccum::default()),
        }
    }
}

impl<C: Connector> VisitPoolAlgoData<PoolAlgoTargetData> for Blocks<C, PoolAlgoTargetData> {
    fn update_algo_data(&self) {
        for it in self.map.borrow().values() {
            // trace!("{}: {:?}", it.db_name, it.metrics().summary());
            *it.data.data.borrow_mut() = (&*it.metrics()).into();
        }
    }
    #[inline]
    fn with_algo_data_all(&self, mut f: impl FnMut(&Name, &PoolAlgoTargetData)) {
        for it in self.map.borrow().values() {
            f(&it.db_name, &it.data)
        }
    }
    #[inline]
    fn with_algo_data<T>(&self, db: &str, f: impl Fn(&PoolAlgoTargetData) -> T) -> Option<T> {
        self.map.borrow().get(db).map(|d| f(&d.data))
    }
    fn total(&self) -> usize {
        self.metrics.total()
    }
}

impl<C: Connector, D: Default> Blocks<C, D> {
    /// To ensure that we can trust our summary statistics, we run a consistency check in
    /// debug mode on most operations. This is cheap enough to run all the time, but we
    /// assume confidence in this code and disable the checks in release mode.
    ///
    /// See [`consistency_check!`] for the macro that calls this on entry and exit.
    #[track_caller]
    pub fn check_consistency(&self) {
        if cfg!(debug_assertions) {
            let mut total = 0;
            for block in self.map.borrow().values() {
                block.check_consistency();
                total += block.conn_count();
            }
            if total != self.metrics.total() && tracing::enabled!(tracing::Level::TRACE) {
                for block in self.map.borrow().values() {
                    trace!(
                        "{}: {} {:?}",
                        block.db_name,
                        block.conn_count(),
                        block.metrics().summary()
                    );
                }
            }
            assert_eq!(
                total,
                self.metrics.total(),
                "Blocks failed consistency check. Total connection count was wrong."
            );
        }
    }

    pub fn prepare(&self, db: &str) -> Name {
        self.block(db).db_name.clone()
    }

    pub fn contains(&self, db: &str) -> bool {
        self.map.borrow().contains_key(db)
    }

    pub fn block_count(&self) -> usize {
        self.map.borrow().len()
    }

    pub fn conn_count(&self) -> usize {
        self.metrics.total()
    }

    pub fn block_conn_count(&self, db: &str) -> usize {
        self.block(db).conn_count()
    }

    pub fn metrics(&self, db: &str) -> Rc<MetricsAccum> {
        self.map
            .borrow_mut()
            .get(db)
            .map(|b| b.metrics())
            .unwrap_or_default()
    }

    pub fn summary(&self) -> PoolMetrics {
        let mut metrics = PoolMetrics::default();
        metrics.pool = self.metrics.summary();
        for block in self.map.borrow().values() {
            metrics.blocks.push(block.metrics().summary());
        }
        metrics
    }

    fn block(&self, db: &str) -> Rc<Block<C, D>> {
        let mut lock = self.map.borrow_mut();
        if let Some(block) = lock.get(db) {
            block.clone()
        } else {
            let db = Name(Rc::new(db.to_owned()));
            let block = Rc::new(Block::new(db.clone(), Some(self.metrics.clone())));
            lock.insert(db, block.clone());
            block
        }
    }

    pub fn create(
        &self,
        connector: &C,
        db: &str,
    ) -> impl Future<Output = ConnResult<ConnHandle<C>>> {
        consistency_check!(self);
        let block = self.block(db);
        block.create(connector)
    }

    pub fn queue(&self, db: &str) -> impl Future<Output = ConnResult<ConnHandle<C>>> {
        consistency_check!(self);
        let block = self.block(db);
        block.queue()
    }

    pub fn create_if_needed(
        &self,
        connector: &C,
        db: &str,
    ) -> impl Future<Output = ConnResult<ConnHandle<C>>> {
        consistency_check!(self);
        let block = self.block(db);
        block.create_if_needed(connector)
    }

    /// Closes one connection in a block.
    pub fn task_close_one(&self, connector: &C, db: &str) -> impl Future<Output = ConnResult<()>> {
        consistency_check!(self);
        let block = self.block(db);
        block.task_close_one(connector)
    }

    /// Steals a connection from one block to another.
    pub fn task_steal(
        &self,
        connector: &C,
        db: &str,
        from: &str,
    ) -> impl Future<Output = ConnResult<()>> {
        let from_block = self.block(from);
        let to_block = self.block(db);
        Block::task_reconnect(from_block, to_block, connector)
    }

    /// Moves a connection to a different block than it was acquired from
    /// without giving any wakers on the old block a chance to get it.
    pub fn task_move_to(
        &self,
        connector: &C,
        conn: ConnHandle<C>,
        db: &str,
    ) -> impl Future<Output = ConnResult<()>> {
        let from_block = self.block(&conn.state.db_name);
        let to_block = self.block(db);
        Block::task_reconnect_conn(from_block, to_block, conn, connector)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{ConnMetrics, MetricVariant, VariantArray};
    use crate::test::*;
    use anyhow::{Ok, Result};
    use pretty_assertions::assert_eq;
    use test_log::test;
    use tokio::task::LocalSet;

    /// Tiny DSL to make the tests more readable
    macro_rules! assert_block {
        ($block:ident has $count:literal $type:ident) => {
            assert_eq!(
                $block.metrics().summary().value,
                VariantArray::with(MetricVariant::$type, $count),
                stringify!(Expected block has $count $type)
            );
        };
        ($block:ident $db:literal is empty) => {
            assert_eq!($block.metrics($db).summary().value, VariantArray::default(), stringify!(Expected block is empty));
        };
        ($block:ident $db:literal has $count:literal $type:ident) => {
            assert_eq!(
                $block.metrics($db).summary().value,
                VariantArray::with(MetricVariant::$type, $count),
                stringify!(Expected block has $count $type)
            );
        };
    }

    #[test(tokio::test)]
    async fn test_block() -> Result<()> {
        let connector = BasicConnector::no_delay();
        let block = Rc::new(Block::<BasicConnector>::new(Name::from("db"), None));
        let conn = block.clone().create(&connector).await?;
        assert_block!(block has 1 Active);
        let local = LocalSet::new();
        let block2 = block.clone();
        local.spawn_local(async move {
            assert_block!(block2 has 1 Active);
            let conn = block2.clone().queue().await?;
            assert_block!(block2 has 1 Active);
            drop(conn);
            Ok(())
        });
        local.spawn_local(async move {
            tokio::task::yield_now().await;
            drop(conn);
        });
        local.await;
        Ok(())
    }

    #[test(tokio::test)]
    async fn test_block_parallel_acquire() -> Result<()> {
        let connector = BasicConnector::no_delay();
        let block = Rc::new(Block::<BasicConnector>::new(Name::from("db"), None));
        block.clone().create(&connector).await?;
        block.clone().create(&connector).await?;
        block.clone().create(&connector).await?;
        assert_block!(block has 3 Idle);

        let local = LocalSet::new();
        for i in 0..100 {
            let block2 = block.clone();
            local.spawn_local(async move {
                for _ in 0..i % 10 {
                    tokio::task::yield_now().await;
                }
                block2.clone().queue().await
            });
        }
        local.await;
        assert_block!(block has 3 Idle);
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
        assert_block!(blocks "db" has 3 Idle);
        assert_block!(blocks "db2" is empty);
        blocks.task_steal(&connector, "db2", "db").await?;
        blocks.task_steal(&connector, "db2", "db").await?;
        blocks.task_steal(&connector, "db2", "db").await?;
        // Block hasn't been GC'd yet
        assert_eq!(2, blocks.block_count());
        assert_block!(blocks "db" is empty);
        assert_block!(blocks "db2" has 3 Idle);
        Ok(())
    }

    #[test(tokio::test)]
    async fn test_move() -> Result<()> {
        let connector = BasicConnector::no_delay();
        let blocks = Blocks::<_, ()>::default();
        assert_eq!(0, blocks.block_count());
        blocks.create(&connector, "db").await?;
        blocks.create(&connector, "db").await?;
        blocks.create(&connector, "db").await?;
        assert_eq!(1, blocks.block_count());
        assert_block!(blocks "db" has 3 Idle);
        assert_block!(blocks "db2" is empty);
        let conn = blocks.queue("db").await?;
        blocks.task_move_to(&connector, conn, "db2").await?;
        assert_eq!(2, blocks.block_count());
        assert_block!(blocks "db" has 2 Idle);
        assert_block!(blocks "db2" has 1 Idle);
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
        assert_block!(blocks "db" has 2 Idle);
        blocks.task_close_one(&connector, "db").await?;
        blocks.task_close_one(&connector, "db").await?;
        assert_block!(blocks "db" is empty);
        // Hasn't GC'd yet
        assert_eq!(1, blocks.block_count());
        Ok(())
    }
}
