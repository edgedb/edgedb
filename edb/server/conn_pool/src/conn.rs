use std::{borrow::Cow, cell::{Cell, RefCell, UnsafeCell}, collections::HashMap, future::{poll_fn, Future}, marker::PhantomData, pin::Pin, process::Output, rc::Rc, task::{ready, Poll}};
use futures::{lock, FutureExt};
use scopeguard::defer;
use crate::waitqueue::WaitQueue;

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq, derive_more::Add)]
pub struct BlockStats {
    connected: usize,
    connecting: usize,
    disconnecting: usize,
    failed: usize,
}

impl BlockStats {
    pub fn count<C: Connector>(&mut self, conn: &Conn<C>) {
        match &*conn.inner.borrow() {
            ConnInner::Closed => unreachable!(),
            ConnInner::Connected(..) => self.connected += 1,
            ConnInner::Connecting(..) => self.connecting += 1,
            ConnInner::Disconnecting(..) => self.disconnecting += 1,
            ConnInner::Failed => self.failed += 1,
        }
    }

    pub fn connected(connected: usize) -> Self { Self { connected, ..Default::default() } }
    pub fn connecting(connecting: usize) -> Self { Self { connecting, ..Default::default() } }
    pub fn disconnecting(disconnecting: usize) -> Self { Self { disconnecting, ..Default::default() } }
    pub fn failed(failed: usize) -> Self { Self { failed, ..Default::default() } }
}

#[derive(Debug)]
pub enum BlockError {
    ConnectionIdentityIncorrect,
    Other(Cow<'static, str>),
}

pub type BlockResult<T> = Result<T, BlockError>;

pub trait Connector: std::fmt::Debug {
    type Conn;
    fn connect(&self, db: &str) -> impl Future<Output = BlockResult<Self::Conn>> + 'static;
    fn reconnect(&self, conn: Self::Conn, db: &str) -> impl Future<Output = BlockResult<Self::Conn>> + 'static;
    fn disconnect(&self, conn: Self::Conn) -> impl Future<Output = BlockResult<()>> + 'static;
}

#[derive(Debug)]
pub struct Conn<C: Connector> {
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

    pub fn try_lock(&self) -> bool {
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
pub struct ConnHandle<C: Connector> {
    pub(crate) conn: Conn<C>
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
