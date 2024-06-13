use std::{borrow::Cow, cell::{Cell, RefCell, UnsafeCell}, collections::HashMap, future::{poll_fn, Future}, marker::PhantomData, pin::Pin, process::Output, rc::Rc, task::{ready, Poll}};
use futures::{lock, FutureExt};
use scopeguard::defer;

#[derive(Debug)]
enum BlockError {
    ConnectionIdentityIncorrect,
    Other(Cow<'static, str>),
}

type BlockResult<T> = Result<T, BlockError>;

trait Connector: std::fmt::Debug {
    type Conn;
    fn connect(&self, db: &str) -> impl Future<Output = BlockResult<Self::Conn>> + 'static;
    fn reconnect(&self, conn: Self::Conn, db: &str) -> impl Future<Output = BlockResult<Self::Conn>> + 'static;
    fn disconnect(&self, conn: Self::Conn) -> impl Future<Output = BlockResult<()>> + 'static;
}

#[derive(Debug)]
struct Conn<C: Connector> {
    inner: Rc<RefCell<ConnInner<C>>>
}

impl <C: Connector> Clone for Conn<C> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone()
        }
    }
}

impl <C: Connector> Conn<C> {
    pub fn new(f: impl Future<Output = BlockResult<C::Conn>> + 'static, waiters: Rc<WaitQueue>) -> Self {
        Self {
            inner: Rc::new(RefCell::new(ConnInner::Connecting(f.boxed_local(), waiters)))
        }
    }

    pub fn close(&self, connector: &C) {
        let mut lock = self.inner.borrow_mut();
        match std::mem::replace(&mut *lock, ConnInner::Closed) {
            ConnInner::Connected(conn, ..) => {
                let f = connector.disconnect(conn).boxed_local();
                *lock = ConnInner::Disconnecting(f);
            }
            _ => unreachable!()
        }
    }

    pub fn reopen(&self, connector: &C, db: &str, waiters: Rc<WaitQueue>) {
        let mut lock = self.inner.borrow_mut();
        match std::mem::replace(&mut *lock, ConnInner::Closed) {
            ConnInner::Connected(conn, ..) => {
                let f = connector.reconnect(conn, db).boxed_local();
                *lock = ConnInner::Connecting(f, waiters);
            }
            _ => unreachable!()
        }
    }

    pub fn poll_ready(&self, cx: &mut std::task::Context) -> Poll<BlockResult<()>> {
        let mut lock = self.inner.borrow_mut();
        match &mut *lock {
            ConnInner::Connected(c, ..) => Poll::Ready(Ok(())),
            ConnInner::Connecting(f, waiters) => {
                Poll::Ready(match ready!(f.poll_unpin(cx)) {
                    Ok(c) => {
                        *lock = ConnInner::Connected(c, Cell::new(true), waiters.clone());
                        Ok(())
                    }
                    Err(err) => {
                        *lock = ConnInner::Failed;
                        Err(err)
                    }
                })
            },
            ConnInner::Disconnecting(f) => {
                Poll::Ready(match ready!(f.poll_unpin(cx)) {
                    Ok(c) => {
                        *lock = ConnInner::Closed;
                        Ok(())
                    }
                    Err(err) => {
                        *lock = ConnInner::Failed;
                        Err(err)
                    }
                })
            },
            ConnInner::Failed => Poll::Ready(Err(BlockError::Other("Failed".into()))),
            ConnInner::Closed => unreachable!()
        }
    }

    fn try_lock(&self) -> bool {
        match &*self.inner.borrow() {
            ConnInner::Connected(_, locked, _) => {
                if !locked.get() {
                    eprintln!("try_lock success");
                    locked.set(true);
                    true
                } else {
                    eprintln!("try_lock fail");
                    false
                }
            }
            _ => false
        }
    }
}

enum ConnInner<C: Connector> {
    Connecting(Pin<Box<dyn Future<Output = BlockResult<C::Conn>>>>, Rc<WaitQueue>),
    Disconnecting(Pin<Box<dyn Future<Output = BlockResult<()>>>>),
    Connected(C::Conn, Cell<bool>, Rc<WaitQueue>),
    Failed,
    Closed,
}

impl <C: Connector> std::fmt::Debug for ConnInner<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ConnInner")
    }
}

#[derive(Debug)]
struct ConnHandle<C: Connector> {
    conn: Conn<C>
}

impl <C: Connector> ConnHandle<C> {
}

impl <C: Connector> Drop for ConnHandle<C> {
    fn drop(&mut self) {
        match &*self.conn.inner.borrow() {
            ConnInner::Connected(c, locked, waiters) => {
                debug_assert!(locked.get());
                locked.set(false);
                waiters.trigger();
            },
            ConnInner::Closed => {}
            _ => {
                unreachable!()
            }
        }
    }
}

#[derive(Default)]
struct WaitQueue {
    id: Cell<usize>,
    waiters: RefCell<Vec<(usize, std::task::Waker)>>,
}

impl WaitQueue {
    pub fn trigger(&self) {
        // TODO: messy and inefficient -- we just wake everything
        eprintln!("trigger");
        for (_, waker) in &*self.waiters.borrow() {
            eprintln!("triggered");
            waker.wake_by_ref()
        }
    }

    pub async fn queue(&self) {
        // TODO: messy
        eprintln!("queue");
        let id = self.id.get() + 1;
        self.id.set(id);
        let waker = poll_fn(|cx| Poll::Ready(cx.waker().clone())).await;
        self.waiters.borrow_mut().push((id, waker));
        defer! {
            // Remove ourselves
            self.waiters.borrow_mut().retain(|(id_, _)| *id_ != id);
        }

        let mut defer = true;
        poll_fn(|cx| if defer {
            defer = false;
            Poll::Pending
        } else {
            Poll::Ready(())
        }).await;

        // Wait for us to be first in line
        poll_fn(|_| if self.waiters.borrow().get(0).unwrap().0 == id {
            Poll::Ready(())
        } else {
            Poll::Pending
        }).await;
    }
}

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq, derive_more::Add)]
struct BlockStats {
    connected: usize,
    connecting: usize,
    disconnecting: usize,
    failed: usize,
}

impl BlockStats {
    fn count<C: Connector>(&mut self, conn: &Conn<C>) {
        match &*conn.inner.borrow() {
            ConnInner::Closed => unreachable!(),
            ConnInner::Connected(..) => self.connected += 1,
            ConnInner::Connecting(..) => self.connecting += 1,
            ConnInner::Disconnecting(..) => self.disconnecting += 1,
            ConnInner::Failed => self.failed += 1,
        }
    }

    fn connected(connected: usize) -> Self { Self { connected, ..Default::default() } }
    fn connecting(connecting: usize) -> Self { Self { connecting, ..Default::default() } }
    fn disconnecting(disconnecting: usize) -> Self { Self { disconnecting, ..Default::default() } }
    fn failed(failed: usize) -> Self { Self { failed, ..Default::default() } }
}

struct Block<C: Connector> {
    db_name: String,
    conns: RefCell<Vec<Conn<C>>>,
    waiters: Rc<WaitQueue>,
}

impl <C: Connector> Block<C> {
    pub fn new(db: &str) -> Self {
        Self {
            db_name: db.to_owned(),
            conns: Vec::new().into(),
            waiters: Default::default()
        }
    }

    pub fn stats(&self) -> BlockStats {
        let mut stats = BlockStats::default();
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

    async fn reconnect(&self, connector: &C, conn: Conn<C>) -> BlockResult<ConnHandle<C>> {
        self.conns.borrow_mut().push(conn.clone());
        conn.reopen(connector, &self.db_name, self.waiters.clone());
        poll_fn(|cx| conn.poll_ready(cx)).await?;
        Ok(ConnHandle {
            conn
        })
    }

    /// Creates or awaits a connection from this block.
    async fn acquire(&self, connector: &C, new: bool) -> BlockResult<ConnHandle<C>> {
        eprintln!("acquire {new}");
        if new {
            let conn = Conn::new(connector.connect(&self.db_name), self.waiters.clone());
            self.conns.borrow_mut().push(conn.clone());
            poll_fn(|cx| conn.poll_ready(cx)).await?;
            Ok(ConnHandle {
                conn
            })
        } else {
            loop {
                eprintln!("loop");
                if let Some(conn) = self.try_acquire_used() {
                    return Ok(ConnHandle { conn });
                }
                self.waiters.queue().await;
            }
        }
    }

    async fn close_one(&self, connector: &C) -> BlockResult<()> {
        let conn = self.try_acquire_used().expect("Could not acquire a connection");
        conn.close(connector);
        poll_fn(|cx| conn.poll_ready(cx)).await?;
        Ok(())
    }
}

struct Blocks<C: Connector>(RefCell<HashMap<String, Rc<Block<C>>>>);

impl <C: Connector> Default for Blocks<C> {
    fn default() -> Self {
        Self(RefCell::new(HashMap::default()))
    }
}

impl <C: Connector> Blocks<C> {
    fn new(block: Block<C>) -> Self {
        let mut map = HashMap::new();
        map.insert(block.db_name.clone(), Rc::new(block));
        Self(RefCell::new(map))
    }

    fn stats(&self, db: &str) -> BlockStats {
        self.0.borrow_mut().get(db).map(|b| b.stats()).unwrap_or(BlockStats::default())
    }

    async fn acquire(&self, connector: &C, db: &str, new: bool) -> BlockResult<ConnHandle<C>> {
        let block = self.0.borrow_mut().entry(db.to_owned()).or_insert_with(|| Rc::new(Block::new(db))).clone();
        block.acquire(connector, new).await
    }

    async fn steal(&self, connector: &C, db: &str, from: &str) -> BlockResult<ConnHandle<C>> {
        let block = self.0.borrow_mut().entry(from.to_owned()).or_insert_with(|| Rc::new(Block::new(from))).clone();
        let conn = block.try_take_used().expect("Could not acquire a connection");
        let block = self.0.borrow_mut().entry(db.to_owned()).or_insert_with(|| Rc::new(Block::new(db))).clone();
        block.reconnect(connector, conn).await
    }
}

#[cfg(test)]
mod tests {
    use tokio::task::LocalSet;

    use super::*;

    #[derive(Debug)]
    struct BasicConnector {
        delay: bool
    }

    impl BasicConnector {
        pub fn no_delay() -> Self {
            BasicConnector {
                delay: false
            }
        }
    }

    impl Connector for BasicConnector {
        type Conn = ();
        fn connect(&self, db: &str) -> impl Future<Output = BlockResult<Self::Conn>> + 'static {
            let delay = self.delay;
            async move {
                if delay {
                    tokio::task::yield_now().await
                }
                Ok(())
            }
        }
        fn reconnect(&self, conn: Self::Conn, db: &str) -> impl Future<Output = BlockResult<Self::Conn>> + 'static {
            let delay = self.delay;
            async move {
                if delay {
                    tokio::task::yield_now().await
                }
                Ok(conn)
            }
        }
        fn disconnect(&self, conn: Self::Conn) -> impl Future<Output = BlockResult<()>> + 'static {
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
        let conn = block.acquire(&connector, true).await.expect("Expected a connection");
        assert_eq!(block.stats(), BlockStats::connected(1));
        let local = LocalSet::new();
        let block2 = block.clone();
        local.spawn_local(async move {
            let connector = BasicConnector::no_delay();
            assert_eq!(block2.stats(), BlockStats::connected(1));
            block2.acquire(&connector, false).await.expect("Expected a connection");
            assert_eq!(block2.stats(), BlockStats::connected(1));
        });
        local.spawn_local(async move {
            tokio::task::yield_now().await;
            drop(conn);
        });
        local.await;
        assert_eq!(block.stats(), BlockStats::connected(1));
    }

    #[tokio::test]
    async fn test_block_parallel_acquire() {
        let connector = BasicConnector::no_delay();
        let block = Rc::new(Block::<BasicConnector>::new("db"));
        block.acquire(&connector, true).await.expect("Expected a connection");
        block.acquire(&connector, true).await.expect("Expected a connection");
        block.acquire(&connector, true).await.expect("Expected a connection");
        assert_eq!(block.stats(), BlockStats::connected(3));

        let local = LocalSet::new();
        for i in 0..100 {
            let block2 = block.clone();
            local.spawn_local(async move {
                let connector = BasicConnector::no_delay();
                for j in 0..i % 10 {
                    tokio::task::yield_now().await;
                }
                block2.acquire(&connector, false).await.expect("Expected a connection");
            });
        }
        local.await;
        assert_eq!(block.stats(), BlockStats::connected(3));
    }

    #[tokio::test]
    async fn test_steal() {
        let connector = BasicConnector::no_delay();
        let blocks = Blocks::default();
        blocks.acquire(&connector, "db", true).await.expect("Expected a connection");
        blocks.acquire(&connector, "db", true).await.expect("Expected a connection");
        blocks.acquire(&connector, "db", true).await.expect("Expected a connection");
        assert_eq!(blocks.stats("db"), BlockStats::connected(3));
        assert_eq!(blocks.stats("db2"), BlockStats::connected(0));
        blocks.steal(&connector, "db2", "db").await.expect("Expected a connection");
        blocks.steal(&connector, "db2", "db").await.expect("Expected a connection");
        blocks.steal(&connector, "db2", "db").await.expect("Expected a connection");
        assert_eq!(blocks.stats("db"), BlockStats::connected(0));
        assert_eq!(blocks.stats("db2"), BlockStats::connected(3));
    }
}
