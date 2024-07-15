use crate::{
    algo::{
        PoolAlgoTargetData, PoolAlgorithmDataBlock, PoolAlgorithmDataMetrics,
        PoolAlgorithmDataPool, VisitPoolAlgoData,
    },
    conn::*,
    metrics::{MetricVariant, MetricsAccum, PoolMetrics},
    time::Instant,
    waitqueue::WaitQueue,
};
use futures::future::Either;
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    future::{poll_fn, ready, Future},
    rc::Rc,
};
use tracing::trace;

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
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Name(Rc<String>);

impl std::fmt::Display for Name {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::fmt::Debug for Name {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl PartialEq<str> for Name {
    fn eq(&self, other: &str) -> bool {
        self.0.as_str() == other
    }
}

impl From<&str> for Name {
    fn from(value: &str) -> Self {
        Name(Rc::new(String::from(value)))
    }
}

impl From<String> for Name {
    fn from(value: String) -> Self {
        Name(Rc::new(value))
    }
}

#[cfg(test)]
impl From<usize> for Name {
    fn from(value: usize) -> Self {
        Name::from(format!("db-{value}"))
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

/// Manages the connection state for a single backend database.
///
/// This is only a set of connections, and does not understand policy, capacity,
/// balancing, or anything outside of a request to make, transfer or discard a
/// connection. It also manages connection statistics for higher layers of code
/// to make decisions.
///
/// The block provides a number of tasks related to connections. The task
/// methods provide futures, but run the accounting "up-front" to ensure that we
/// keep a handle on quotas, even if running the task async.
///
/// The block has an associated data generic parameter that may be provided,
/// where additional metadata for this block can live.
pub struct Block<C: Connector, D: Default = ()> {
    pub db_name: Name,
    conns: RefCell<Vec<Conn<C>>>,
    state: Rc<ConnState>,
    youngest: Cell<Instant>,
    /// Associated data for this block useful for statistics, quotas or other
    /// information. This is provided by the algorithm in this crate.
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
            youngest: Cell::new(Instant::now()),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.state.metrics.total()
    }

    fn conn(&self, conn: Conn<C>) -> ConnHandle<C> {
        ConnHandle::new(conn, self.state.clone())
    }

    #[track_caller]
    pub fn check_consistency(&self) {
        if cfg!(debug_assertions) {
            assert_eq!(
                self.len(),
                self.conns.borrow().len(),
                "Block {} failed consistency check. Total connection count was wrong.",
                self.db_name
            );
            let conn_metrics = MetricsAccum::default();
            for conn in &*self.conns.borrow() {
                conn_metrics.insert(conn.variant())
            }
            conn_metrics.set_value(MetricVariant::Waiting, self.state.waiters.lock.get());
            assert_eq!(
                self.metrics().summary().value,
                conn_metrics.summary().value,
                "Connection metrics are incorrect. Left: actual, right: expected"
            );
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

    fn try_get_used(&self) -> Option<Conn<C>> {
        for conn in &*self.conns.borrow() {
            if conn.variant() == MetricVariant::Idle {
                return Some(conn.clone());
            }
        }
        None
    }

    fn try_take_used(&self) -> Option<Conn<C>> {
        let mut lock = self.conns.borrow_mut();
        let pos = lock
            .iter()
            .position(|conn| conn.variant() == MetricVariant::Idle);
        if let Some(index) = pos {
            let conn = lock.remove(index);
            return Some(conn);
        }
        None
    }

    /// Creates a connection from this block.
    #[cfg(test)]
    fn create(self: Rc<Self>, connector: &C) -> impl Future<Output = ConnResult<ConnHandle<C>>> {
        let conn = {
            consistency_check!(self);
            let conn = Conn::new(connector.connect(&self.db_name), &self.state.metrics);
            self.youngest.set(Instant::now());
            self.conns.borrow_mut().push(conn.clone());
            conn
        };
        async move {
            consistency_check!(self);
            poll_fn(|cx| conn.poll_ready(cx, &self.state.metrics, MetricVariant::Active)).await?;
            Ok(self.conn(conn))
        }
    }

    /// Awaits a connection from this block.
    #[cfg(test)]
    fn create_if_needed(
        self: Rc<Self>,
        connector: &C,
    ) -> impl Future<Output = ConnResult<ConnHandle<C>>> {
        if let Some(conn) = self.try_acquire_used() {
            return Either::Left(ready(Ok(self.conn(conn))));
        }
        Either::Right(self.create(connector))
    }

    /// Awaits a connection from this block.
    fn queue(self: Rc<Self>) -> impl Future<Output = ConnResult<ConnHandle<C>>> {
        // If someone else is waiting, we have to queue, even if there's a connection
        if self.state.waiters.len() == 0 {
            if let Some(conn) = self.try_acquire_used() {
                trace!("Got a connection");
                return Either::Left(ready(Ok(self.conn(conn))));
            }
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

    /// Creates a connection from this block.
    fn task_create(self: Rc<Self>, connector: &C) -> impl Future<Output = ConnResult<()>> {
        let conn = {
            consistency_check!(self);
            let conn = Conn::new(connector.connect(&self.db_name), &self.state.metrics);
            self.youngest.set(Instant::now());
            self.conns.borrow_mut().push(conn.clone());
            conn
        };
        async move {
            consistency_check!(self);
            poll_fn(|cx| conn.poll_ready(cx, &self.state.metrics, MetricVariant::Idle)).await?;
            self.state.waiters.trigger();
            Ok(())
        }
    }

    /// Close one of idle connections in this block
    fn task_close_one(self: Rc<Self>, connector: &C) -> impl Future<Output = ConnResult<()>> {
        let conn = {
            consistency_check!(self);
            let conn = self.try_get_used().expect("Could not acquire a connection");
            conn.close(connector, &self.state.metrics);
            conn
        };
        async move {
            consistency_check!(self);
            poll_fn(|cx| conn.poll_ready(cx, &self.state.metrics, MetricVariant::Closed)).await?;
            // TODO: this can be replaced by moving the final item of the list into the
            // empty spot to avoid reshuffling
            self.conns.borrow_mut().retain(|other| other != &conn);
            conn.untrack(&self.state.metrics);
            Ok(())
        }
    }

    fn task_reconnect(
        from: Rc<Self>,
        to: Rc<Self>,
        connector: &C,
    ) -> impl Future<Output = ConnResult<()>> {
        let conn = {
            consistency_check!(from);
            consistency_check!(to);

            let conn = from
                .try_take_used()
                .expect("Could not acquire a connection");
            to.youngest.set(Instant::now());
            to.conns.borrow_mut().push(conn.clone());
            conn.transfer(
                connector,
                &from.state.metrics,
                &to.state.metrics,
                &to.db_name,
            );
            conn
        };
        async move {
            consistency_check!(from);
            consistency_check!(to);
            poll_fn(|cx| conn.poll_ready(cx, &to.state.metrics, MetricVariant::Idle)).await?;
            to.state.waiters.trigger();
            Ok(())
        }
    }

    fn task_reconnect_conn(
        from: Rc<Self>,
        to: Rc<Self>,
        conn: ConnHandle<C>,
        connector: &C,
    ) -> impl Future<Output = ConnResult<()>> {
        let conn = {
            consistency_check!(from);
            consistency_check!(to);

            // TODO: this can be replaced by moving the final item of the list into the
            // empty spot to avoid reshuffling
            let conn = conn.into_inner();
            from.conns.borrow_mut().retain(|other| other != &conn);
            to.youngest.set(Instant::now());
            to.conns.borrow_mut().push(conn.clone());
            conn.transfer(
                connector,
                &from.state.metrics,
                &to.state.metrics,
                &to.db_name,
            );
            conn
        };
        async move {
            consistency_check!(from);
            consistency_check!(to);
            poll_fn(|cx| conn.poll_ready(cx, &to.state.metrics, MetricVariant::Idle)).await?;
            to.state.waiters.trigger();
            Ok(())
        }
    }

    fn task_reopen(
        self: Rc<Self>,
        conn: ConnHandle<C>,
        connector: &C,
    ) -> impl Future<Output = ConnResult<()>> {
        let conn = {
            consistency_check!(self);
            let conn = conn.into_inner();
            conn.reopen(connector, &self.state.metrics, &self.db_name);
            conn
        };
        async move {
            consistency_check!(self);
            poll_fn(|cx| conn.poll_ready(cx, &self.state.metrics, MetricVariant::Idle)).await?;
            self.state.waiters.trigger();
            Ok(())
        }
    }

    fn task_discard(
        self: Rc<Self>,
        conn: ConnHandle<C>,
        connector: &C,
    ) -> impl Future<Output = ConnResult<()>> {
        let conn = {
            consistency_check!(self);
            let conn = conn.into_inner();
            conn.discard(connector, &self.state.metrics);
            conn
        };
        async move {
            consistency_check!(self);
            poll_fn(|cx| conn.poll_ready(cx, &self.state.metrics, MetricVariant::Closed)).await?;
            // TODO: this can be replaced by moving the final item of the list into the
            // empty spot to avoid reshuffling
            self.conns.borrow_mut().retain(|other| other != &conn);
            conn.untrack(&self.state.metrics);
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

impl<C: Connector> PoolAlgorithmDataMetrics for Block<C, PoolAlgoTargetData> {
    #[inline(always)]
    fn avg_ms(&self, variant: MetricVariant) -> usize {
        self.state.metrics.avg_ms(variant)
    }
    #[inline(always)]
    fn count(&self, variant: MetricVariant) -> usize {
        self.state.metrics.count(variant)
    }
    #[inline(always)]
    fn max(&self, variant: MetricVariant) -> usize {
        self.state.metrics.max(variant)
    }
    #[inline(always)]
    fn total(&self) -> usize {
        self.state.metrics.total()
    }
    #[inline(always)]
    fn total_max(&self) -> usize {
        self.state.metrics.total_max()
    }
}

impl<C: Connector> PoolAlgorithmDataBlock for Block<C, PoolAlgoTargetData> {
    #[inline(always)]
    fn target(&self) -> usize {
        self.data.target()
    }
    #[inline(always)]
    fn set_target(&self, target: usize) {
        self.data.set_target(target);
    }
    #[inline(always)]
    fn insert_demand(&self, demand: u32) {
        self.data.insert_demand(demand)
    }
    #[inline(always)]
    fn demand(&self) -> u32 {
        self.data.demand()
    }
    #[inline(always)]
    fn oldest_ms(&self, variant: MetricVariant) -> usize {
        assert_eq!(variant, MetricVariant::Waiting);
        self.state.waiters.oldest().as_millis() as _
    }
    #[inline(always)]
    fn youngest_ms(&self) -> usize {
        self.youngest.get().elapsed().as_millis() as _
    }
}

impl<C: Connector> PoolAlgorithmDataMetrics for Blocks<C, PoolAlgoTargetData> {
    #[inline(always)]
    fn avg_ms(&self, variant: MetricVariant) -> usize {
        self.metrics.avg_ms(variant)
    }
    #[inline(always)]
    fn count(&self, variant: MetricVariant) -> usize {
        self.metrics.count(variant)
    }
    #[inline(always)]
    fn max(&self, variant: MetricVariant) -> usize {
        self.metrics.max(variant)
    }
    #[inline(always)]
    fn total(&self) -> usize {
        self.metrics.total()
    }
    #[inline(always)]
    fn total_max(&self) -> usize {
        self.metrics.total_max()
    }
}

impl<C: Connector> PoolAlgorithmDataPool for Blocks<C, PoolAlgoTargetData> {
    #[inline(always)]
    fn reset_max(&self) {
        self.metrics.reset_max();
        for block in self.map.borrow().values() {
            block.metrics().reset_max()
        }
    }
}

impl<C: Connector> VisitPoolAlgoData for Blocks<C, PoolAlgoTargetData> {
    type Block = Block<C, PoolAlgoTargetData>;

    fn ensure_block(&self, db: &str, default_demand: usize) -> bool {
        if self.map.borrow().contains_key(db) {
            false
        } else {
            let block: Rc<Block<C, PoolAlgoTargetData>> =
                Rc::new(Block::new(db.into(), Some(self.metrics.clone())));
            block.data.insert_demand(default_demand as _);
            self.map.borrow_mut().insert(db.into(), block);
            true
        }
    }

    fn with<T>(&self, db: &str, f: impl Fn(&Block<C, PoolAlgoTargetData>) -> T) -> Option<T> {
        self.map.borrow().get(db).map(|block| f(block))
    }

    fn with_all(&self, mut f: impl FnMut(&Name, &Block<C, PoolAlgoTargetData>)) {
        self.map.borrow_mut().retain(|name, block| {
            if block.is_empty() && block.data.demand() == 0 {
                false
            } else {
                f(name, block);
                true
            }
        });
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
                total += block.len();
            }
            if total != self.metrics.total() && tracing::enabled!(tracing::Level::TRACE) {
                for block in self.map.borrow().values() {
                    trace!(
                        "{}: {} {:?}",
                        block.db_name,
                        block.len(),
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

    pub fn name(&self, db: &str) -> Option<Name> {
        if let Some((name, _)) = self.map.borrow().get_key_value(db) {
            Some(name.clone())
        } else {
            None
        }
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
        metrics.all_time = self.metrics.all_time();
        for (name, block) in self.map.borrow().iter() {
            metrics
                .blocks
                .insert(name.clone(), block.metrics().summary());
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

    /// Create and acquire a connection. Only used for tests.
    #[cfg(test)]
    pub fn create(
        &self,
        connector: &C,
        db: &str,
    ) -> impl Future<Output = ConnResult<ConnHandle<C>>> {
        consistency_check!(self);
        let block = self.block(db);
        block.create(connector)
    }

    /// Create and acquire a connection. If a connection is free, skips
    /// creation. Only used for tests.
    #[cfg(test)]
    pub fn create_if_needed(
        &self,
        connector: &C,
        db: &str,
    ) -> impl Future<Output = ConnResult<ConnHandle<C>>> {
        consistency_check!(self);
        let block = self.block(db);
        block.create_if_needed(connector)
    }

    /// Queue for a connection.
    pub fn queue(&self, db: &str) -> impl Future<Output = ConnResult<ConnHandle<C>>> {
        consistency_check!(self);
        let block = self.block(db);
        block.queue()
    }

    /// Creates one connection in a block.
    pub fn task_create_one(&self, connector: &C, db: &str) -> impl Future<Output = ConnResult<()>> {
        consistency_check!(self);
        let block = self.block(db);
        block.task_create(connector)
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

    /// Marks a connection as requiring a discard.
    pub fn task_discard(
        &self,
        connector: &C,
        conn: ConnHandle<C>,
    ) -> impl Future<Output = ConnResult<()>> {
        let block = self.block(&conn.state.db_name);
        block.task_discard(conn, connector)
    }

    /// Marks a connection as requiring re-open.
    pub fn task_reopen(
        &self,
        connector: &C,
        conn: ConnHandle<C>,
    ) -> impl Future<Output = ConnResult<()>> {
        let block = self.block(&conn.state.db_name);
        block.task_reopen(conn, connector)
    }

    /// Do we have any live blocks?
    pub fn is_empty(&self) -> bool {
        self.conn_count() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{MetricVariant, VariantArray};
    use crate::test::*;
    use anyhow::{Ok, Result};
    use pretty_assertions::assert_eq;
    use test_log::test;
    use tokio::task::LocalSet;

    /// Tiny DSL to make the tests more readable.
    macro_rules! assert_block {
        ($block:ident has $($count:literal $type:ident),+) => {
            assert_eq!(
                $block.metrics().summary().value,
                [$(VariantArray::with(MetricVariant::$type, $count)),+].into_iter().sum(),
                stringify!(Expected block has $($count $type),+)
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
    async fn test_counts_updated() -> Result<()> {
        let connector = BasicConnector::no_delay();
        let block = Rc::new(Block::<BasicConnector>::new(Name::from("db"), None));
        let f = block.clone().create(&connector);
        assert_block!(block has 1 Connecting);
        let conn = f.await?;
        assert_block!(block has 1 Active);
        let f = block.clone().queue();
        assert_block!(block has 1 Waiting, 1 Active);
        drop(conn);
        assert_block!(block has 1 Waiting, 1 Idle);
        let conn = f.await?;
        assert_block!(block has 1 Active);
        drop(conn);
        assert_block!(block has 1 Idle);

        Ok(())
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
        blocks.metrics("db").reset_max();
        blocks.metrics("db2").reset_max();
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
        // Should not activate a connection to steal it
        assert_eq!(0, blocks.metrics("db").max(MetricVariant::Active));
        assert_eq!(0, blocks.metrics("db2").max(MetricVariant::Active));
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
        blocks.metrics("db").reset_max();
        blocks.metrics("db2").reset_max();
        assert_eq!(1, blocks.block_count());
        assert_block!(blocks "db" has 3 Idle);
        assert_block!(blocks "db2" is empty);
        let conn = blocks.queue("db").await?;
        blocks.task_move_to(&connector, conn, "db2").await?;
        assert_eq!(2, blocks.block_count());
        assert_block!(blocks "db" has 2 Idle);
        assert_block!(blocks "db2" has 1 Idle);
        // Should not activate a connection to move it
        assert_eq!(1, blocks.metrics("db").max(MetricVariant::Active));
        assert_eq!(0, blocks.metrics("db2").max(MetricVariant::Active));
        Ok(())
    }

    #[test(tokio::test)]
    async fn test_close() -> Result<()> {
        let connector = BasicConnector::no_delay();
        let blocks = Blocks::<_, ()>::default();
        assert_eq!(0, blocks.block_count());
        blocks.create(&connector, "db").await?;
        blocks.create(&connector, "db").await?;
        blocks.metrics("db").reset_max();
        assert_eq!(1, blocks.block_count());
        assert_block!(blocks "db" has 2 Idle);
        blocks.task_close_one(&connector, "db").await?;
        blocks.task_close_one(&connector, "db").await?;
        assert_block!(blocks "db" is empty);
        // Hasn't GC'd yet
        assert_eq!(1, blocks.block_count());
        // Should not activate a connection to close it
        assert_eq!(0, blocks.metrics("db").max(MetricVariant::Active));
        Ok(())
    }

    #[test(tokio::test)]
    async fn test_reopen() -> Result<()> {
        let connector = BasicConnector::no_delay();
        let blocks = Blocks::<_, ()>::default();
        assert_eq!(0, blocks.block_count());
        let conn = blocks.create(&connector, "db").await?;
        blocks.task_reopen(&connector, conn).await?;
        assert_block!(blocks "db" has 1 Idle);
        assert_eq!(
            blocks.metrics("db").all_time()[MetricVariant::Connecting],
            2
        );
        assert_eq!(
            blocks.metrics("db").all_time()[MetricVariant::Disconnecting],
            1
        );
        Ok(())
    }

    #[test(tokio::test)]
    async fn test_discard() -> Result<()> {
        let connector = BasicConnector::no_delay();
        let blocks = Blocks::<_, ()>::default();
        assert_eq!(0, blocks.block_count());
        let conn = blocks.create(&connector, "db").await?;
        blocks.task_discard(&connector, conn).await?;
        assert_block!(blocks "db" is empty);
        assert_eq!(
            blocks.metrics("db").all_time()[MetricVariant::Connecting],
            1
        );
        assert_eq!(
            blocks.metrics("db").all_time()[MetricVariant::Disconnecting],
            1
        );
        Ok(())
    }
}
