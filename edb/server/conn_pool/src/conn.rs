use crate::waitqueue::WaitQueue;
use futures::FutureExt;
use std::{
    borrow::Cow,
    cell::{Cell, RefCell},
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{ready, Poll},
    time::Duration,
};
use tracing::trace;

#[cfg(test)]
use mock_instant::thread_local::Instant;
#[cfg(not(test))]
use std::time::Instant;

#[derive(Debug, Default, PartialEq, Eq)]
struct RollingAverage {
    values: [u32; 30],
    ptr: u8,
}

impl RollingAverage {
    fn accum(&mut self, as_millis: u32) {
        self.values[self.ptr as usize] = as_millis;
        self.ptr = (self.ptr + 1) % 30;
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ConnMetricsSummary {
    summary: [usize; 8],
}

impl ConnMetricsSummary {
    pub fn with(variant: ConnStateVariant, count: usize) -> Self {
        let mut summary = [0; 8];
        summary[variant as usize] = count;
        Self { summary }
    }
}

#[derive(Debug, Default)]
pub struct ConnMetrics {
    counts: RefCell<[usize; 8]>,
    times: RefCell<[RollingAverage; 8]>,
}

impl ConnMetrics {
    pub fn summary(&self) -> ConnMetricsSummary {
        ConnMetricsSummary {
            summary: *self.counts.borrow(),
        }
    }

    pub fn set(&self, to: ConnStateVariant) {
        let mut lock = self.counts.borrow_mut();
        lock[to as usize] += 1;
        // trace!("None->{to:?} ({})", lock[to as usize]);
    }

    fn transition(&self, from: ConnStateVariant, to: ConnStateVariant, time: Duration) {
        trace!("{from:?}->{to:?}: {time:?}");
        let mut lock = self.counts.borrow_mut();
        lock[from as usize] -= 1;
        self.times.borrow_mut()[from as usize].accum(time.as_millis() as _);
        lock[to as usize] += 1;
    }

    fn remove(&self, from: ConnStateVariant, time: Duration) {
        let mut lock = self.counts.borrow_mut();
        lock[from as usize] -= 1;
        self.times.borrow_mut()[from as usize].accum(time.as_millis() as _);
        // trace!("{from:?}->None ({time:?})");
    }

    fn remove_final(&self, from: ConnStateVariant) {
        let mut lock = self.counts.borrow_mut();
        lock[from as usize] -= 1;
        // trace!("{from:?}->None");
    }
}

#[derive(Default)]
pub struct ConnState {
    pub waiters: WaitQueue,
    pub metrics: Rc<ConnMetrics>,
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
    pub fn new(
        f: impl Future<Output = ConnResult<C::Conn>> + 'static,
        metrics: &ConnMetrics,
    ) -> Self {
        metrics.set(ConnStateVariant::Connecting);
        Self {
            inner: Rc::new(RefCell::new(ConnInner::Connecting(
                Instant::now(),
                f.boxed_local(),
            ))),
        }
    }

    #[inline(always)]
    pub fn with_handle<T>(&self, f: impl Fn(&C::Conn) -> T) -> Option<T> {
        match &*self.inner.borrow() {
            ConnInner::Active(_, conn, ..) => Some(f(conn)),
            _ => None,
        }
    }

    #[inline]
    fn transition(&self, f: impl FnOnce(ConnInner<C>) -> ConnInner<C>) {
        let mut lock = self.inner.borrow_mut();
        let inner = std::mem::replace(&mut *lock, ConnInner::Transition);
        *lock = f(inner);
    }

    pub fn close(&self, connector: &C, metrics: &ConnMetrics) {
        self.transition(|inner| match inner {
            ConnInner::Active(t, conn, ..) => {
                metrics.transition(
                    ConnStateVariant::Active,
                    ConnStateVariant::Disconnecting,
                    t.elapsed(),
                );
                let f = connector.disconnect(conn).boxed_local();
                ConnInner::Disconnecting(Instant::now(), f)
            }
            _ => unreachable!(),
        });
    }

    pub fn reopen(&self, connector: &C, to: &ConnMetrics, db: &str) {
        self.transition(|inner| match inner {
            ConnInner::Active(_, conn, ..) => {
                to.set(ConnStateVariant::Connecting);
                let f = connector.reconnect(conn, db).boxed_local();
                ConnInner::Connecting(Instant::now(), f)
            }
            _ => unreachable!(),
        });
    }

    pub fn poll_ready(
        &self,
        cx: &mut std::task::Context,
        metrics: &ConnMetrics,
    ) -> Poll<ConnResult<()>> {
        let mut lock = self.inner.borrow_mut();
        match &mut *lock {
            ConnInner::Idle(_, ..) => Poll::Ready(Ok(())),
            ConnInner::Connecting(t, f) => Poll::Ready(match ready!(f.poll_unpin(cx)) {
                Ok(c) => {
                    metrics.transition(
                        ConnStateVariant::Connecting,
                        ConnStateVariant::Active,
                        t.elapsed(),
                    );
                    *lock = ConnInner::Active(Instant::now(), c);
                    Ok(())
                }
                Err(err) => {
                    metrics.transition(
                        ConnStateVariant::Connecting,
                        ConnStateVariant::Failed,
                        t.elapsed(),
                    );
                    *lock = ConnInner::Failed;
                    Err(err)
                }
            }),
            ConnInner::Disconnecting(t, f) => Poll::Ready(match ready!(f.poll_unpin(cx)) {
                Ok(c) => {
                    metrics.transition(
                        ConnStateVariant::Disconnecting,
                        ConnStateVariant::Closed,
                        t.elapsed(),
                    );
                    *lock = ConnInner::Closed;
                    Ok(())
                }
                Err(err) => {
                    metrics.transition(
                        ConnStateVariant::Disconnecting,
                        ConnStateVariant::Failed,
                        t.elapsed(),
                    );
                    *lock = ConnInner::Failed;
                    Err(err)
                }
            }),
            ConnInner::Failed => Poll::Ready(Err(ConnError::Other("Failed".into()))),
            _ => unreachable!(),
        }
    }

    pub fn try_lock(&self, metrics: &ConnMetrics) -> bool {
        let mut lock = self.inner.borrow_mut();

        let res: bool;
        let old = std::mem::replace(&mut *lock, ConnInner::Transition);
        (*lock, res) = match old {
            ConnInner::Idle(t, conn) => {
                metrics.transition(
                    ConnStateVariant::Idle,
                    ConnStateVariant::Active,
                    t.elapsed(),
                );
                (ConnInner::Active(Instant::now(), conn), true)
            }
            other => (other, false),
        };
        res
    }

    pub fn variant(&self) -> ConnStateVariant {
        (&*self.inner.borrow()).into()
    }

    pub fn untrack(&self, metrics: &ConnMetrics) {
        match &*self.inner.borrow() {
            ConnInner::Active(t, _)
            | ConnInner::Idle(t, _)
            | ConnInner::Poisoned(t, _)
            | ConnInner::Connecting(t, _)
            | ConnInner::Disconnecting(t, _) => metrics.remove(self.variant(), t.elapsed()),
            other => metrics.remove_final(other.into()),
        }
    }
}

#[derive(strum::EnumDiscriminants)]
#[strum_discriminants(vis(pub))]
#[strum_discriminants(name(ConnStateVariant))]
enum ConnInner<C: Connector> {
    /// Connecting connections hold a spot in the pool as they count towards quotas
    Connecting(Instant, Pin<Box<dyn Future<Output = ConnResult<C::Conn>>>>),
    /// Disconnecting connections hold a spot in the pool as they count towards quotas
    Disconnecting(Instant, Pin<Box<dyn Future<Output = ConnResult<()>>>>),
    Idle(Instant, C::Conn),
    Active(Instant, C::Conn),
    Poisoned(Instant, C::Conn),
    Failed,
    Closed,
    /// Transitioning
    Transition,
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
        self.conn.transition(|inner| match inner {
            ConnInner::Active(t, c) => {
                let next = if self.poison.get() {
                    self.state.metrics.transition(
                        ConnStateVariant::Active,
                        ConnStateVariant::Poisoned,
                        t.elapsed(),
                    );
                    ConnInner::Poisoned(Instant::now(), c)
                } else {
                    self.state.metrics.transition(
                        ConnStateVariant::Active,
                        ConnStateVariant::Idle,
                        t.elapsed(),
                    );
                    ConnInner::Idle(Instant::now(), c)
                };
                self.state.waiters.trigger();
                next
            }
            _ => {
                unreachable!()
            }
        });
    }
}
