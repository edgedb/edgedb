use crate::waitqueue::WaitQueue;
use futures::FutureExt;
use std::{
    borrow::Cow,
    cell::{Cell, RefCell},
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{ready, Poll},
};
use tracing::trace;

#[derive(Default)]
pub struct ConnState {
    pub waiters: WaitQueue,
    active: Cell<usize>,
}

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq, derive_more::Add)]
pub struct ConnStats {
    connected: usize,
    connecting: usize,
    disconnecting: usize,
    failed: usize,
}

impl ConnStats {
    pub fn count<C: Connector>(&mut self, conn: &Conn<C>) {
        match &*conn.inner.borrow() {
            ConnInner::Closed => unreachable!(),
            ConnInner::Connected(..) => self.connected += 1,
            ConnInner::Connecting(..) => self.connecting += 1,
            ConnInner::Disconnecting(..) => self.disconnecting += 1,
            ConnInner::Failed => self.failed += 1,
        }
    }

    pub fn connected(connected: usize) -> Self {
        Self {
            connected,
            ..Default::default()
        }
    }
    pub fn connecting(connecting: usize) -> Self {
        Self {
            connecting,
            ..Default::default()
        }
    }
    pub fn disconnecting(disconnecting: usize) -> Self {
        Self {
            disconnecting,
            ..Default::default()
        }
    }
    pub fn failed(failed: usize) -> Self {
        Self {
            failed,
            ..Default::default()
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConnError {
    #[error("ConnectionIdentityIncorrect")]
    ConnectionIdentityIncorrect,
    #[error("Shutdown")]
    Shutdown,
    #[error("{0}")]
    Other(Cow<'static, str>),
}

pub type ConnResult<T> = Result<T, ConnError>;

pub trait Connector: std::fmt::Debug {
    type Conn;
    fn connect(&self, db: &str) -> impl Future<Output = ConnResult<Self::Conn>> + 'static;
    fn reconnect(
        &self,
        conn: Self::Conn,
        db: &str,
    ) -> impl Future<Output = ConnResult<Self::Conn>> + 'static;
    fn disconnect(&self, conn: Self::Conn) -> impl Future<Output = ConnResult<()>> + 'static;
}

#[derive(Debug)]
pub struct Conn<C: Connector> {
    inner: Rc<RefCell<ConnInner<C>>>,
}

impl<C: Connector> PartialEq for Conn<C> {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.inner, &other.inner)
    }
}

impl<C: Connector> Eq for Conn<C> {}

impl<C: Connector> Clone for Conn<C> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<C: Connector> Conn<C> {
    pub fn new(f: impl Future<Output = ConnResult<C::Conn>> + 'static) -> Self {
        Self {
            inner: Rc::new(RefCell::new(ConnInner::Connecting(f.boxed_local()))),
        }
    }

    #[inline(always)]
    pub fn with_handle<T>(&self, f: impl Fn(&C::Conn) -> T) -> Option<T> {
        match &*self.inner.borrow() {
            ConnInner::Connected(conn, ..) => Some(f(conn)),
            _ => None,
        }
    }

    pub fn close(&self, connector: &C) {
        let mut lock = self.inner.borrow_mut();
        match std::mem::replace(&mut *lock, ConnInner::Closed) {
            ConnInner::Connected(conn, ..) => {
                let f = connector.disconnect(conn).boxed_local();
                *lock = ConnInner::Disconnecting(f);
            }
            _ => unreachable!(),
        }
    }

    pub fn reopen(&self, connector: &C, db: &str) {
        let mut lock = self.inner.borrow_mut();
        match std::mem::replace(&mut *lock, ConnInner::Closed) {
            ConnInner::Connected(conn, ..) => {
                let f = connector.reconnect(conn, db).boxed_local();
                *lock = ConnInner::Connecting(f);
            }
            _ => unreachable!(),
        }
    }

    pub fn poll_ready(&self, cx: &mut std::task::Context) -> Poll<ConnResult<()>> {
        let mut lock = self.inner.borrow_mut();
        match &mut *lock {
            ConnInner::Connected(c, ..) => Poll::Ready(Ok(())),
            ConnInner::Connecting(f) => Poll::Ready(match ready!(f.poll_unpin(cx)) {
                Ok(c) => {
                    *lock = ConnInner::Connected(c, Cell::new(LockState::Locked));
                    Ok(())
                }
                Err(err) => {
                    *lock = ConnInner::Failed;
                    Err(err)
                }
            }),
            ConnInner::Disconnecting(f) => Poll::Ready(match ready!(f.poll_unpin(cx)) {
                Ok(c) => {
                    *lock = ConnInner::Closed;
                    Ok(())
                }
                Err(err) => {
                    *lock = ConnInner::Failed;
                    Err(err)
                }
            }),
            ConnInner::Failed => Poll::Ready(Err(ConnError::Other("Failed".into()))),
            ConnInner::Closed => unreachable!(),
        }
    }

    pub fn try_lock(&self) -> bool {
        match &*self.inner.borrow() {
            ConnInner::Connected(_, locked) => match locked.get() {
                LockState::Locked | LockState::Poisoned => {
                    trace!("try_lock fail");
                    false
                }
                LockState::Unlocked => {
                    trace!("try_lock success");
                    locked.set(LockState::Locked);
                    true
                }
            },
            _ => false,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum LockState {
    Locked,
    Unlocked,
    Poisoned,
}

enum ConnInner<C: Connector> {
    /// Connecting connections hold a spot in the pool as they count towards quotas
    Connecting(Pin<Box<dyn Future<Output = ConnResult<C::Conn>>>>),
    /// Disconnecting connections hold a spot in the pool as they count towards quotas
    Disconnecting(Pin<Box<dyn Future<Output = ConnResult<()>>>>),
    Connected(C::Conn, Cell<LockState>),
    Failed,
    Closed,
}

impl<C: Connector> std::fmt::Debug for ConnInner<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ConnInner")
    }
}

#[derive(derive_more::Debug)]
pub struct ConnHandle<C: Connector> {
    #[debug("{conn:?}")]
    pub(crate) conn: Conn<C>,
    #[debug(skip)]
    pub(crate) state: Rc<ConnState>,
    poison: Cell<bool>,
}

impl<C: Connector> ConnHandle<C> {
    pub fn new(conn: Conn<C>, state: Rc<ConnState>) -> Self {
        state.active.set(state.active.get() + 1);
        Self {
            conn,
            state,
            poison: Cell::new(false),
        }
    }

    pub fn poison(&self) {
        self.poison.set(true)
    }
}

impl<C: Connector> Drop for ConnHandle<C> {
    fn drop(&mut self) {
        match &*self.conn.inner.borrow() {
            ConnInner::Connected(c, locked) => {
                debug_assert_eq!(locked.get(), LockState::Locked);
                locked.set(if self.poison.get() {
                    LockState::Poisoned
                } else {
                    LockState::Unlocked
                });
                self.state.active.set(self.state.active.get() - 1);
                self.state.waiters.trigger();
            }
            _ => {
                unreachable!()
            }
        }
    }
}
