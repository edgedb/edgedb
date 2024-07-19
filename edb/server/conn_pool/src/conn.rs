use crate::{
    block::Name,
    metrics::{MetricVariant, MetricsAccum},
    time::Instant,
    waitqueue::WaitQueue,
};
use futures::FutureExt;
use std::{
    borrow::Cow,
    cell::{Cell, RefCell},
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{ready, Poll},
};
use tracing::error;

pub struct ConnState {
    pub db_name: Name,
    pub waiters: WaitQueue,
    pub metrics: Rc<MetricsAccum>,
}

#[derive(Debug, thiserror::Error)]
pub enum ConnError {
    #[error("Shutdown")]
    Shutdown,
    #[error("{0}")]
    Other(Cow<'static, str>),
}

pub type ConnResult<T> = Result<T, ConnError>;

pub trait Connector: std::fmt::Debug + 'static {
    /// The type of connection associated with this [`Connector`].
    type Conn;

    /// Perform a connect operation to the given database.
    fn connect(&self, db: &str) -> impl Future<Output = ConnResult<Self::Conn>> + 'static;

    /// Perform a graceful reconnect operation from the existing connection to a new database.
    fn reconnect(
        &self,
        conn: Self::Conn,
        db: &str,
    ) -> impl Future<Output = ConnResult<Self::Conn>> + 'static;

    /// Perform a graceful disconnect operation on the given connection.
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
        metrics: &MetricsAccum,
    ) -> Self {
        metrics.insert(MetricVariant::Connecting);
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

    pub fn close(&self, connector: &C, metrics: &MetricsAccum) {
        self.transition(|inner| match inner {
            ConnInner::Idle(t, conn, ..) => {
                metrics.transition(
                    MetricVariant::Idle,
                    MetricVariant::Disconnecting,
                    t.elapsed(),
                );
                let f = connector.disconnect(conn).boxed_local();
                ConnInner::Disconnecting(Instant::now(), f)
            }
            _ => unreachable!(),
        });
    }

    pub fn discard(&self, connector: &C, metrics: &MetricsAccum) {
        self.transition(|inner| match inner {
            ConnInner::Active(t, conn, ..) => {
                metrics.transition(
                    MetricVariant::Active,
                    MetricVariant::Disconnecting,
                    t.elapsed(),
                );
                let f = connector.disconnect(conn).boxed_local();
                ConnInner::Disconnecting(Instant::now(), f)
            }
            _ => unreachable!(),
        });
    }

    pub fn transfer(&self, connector: &C, from: &MetricsAccum, to: &MetricsAccum, db: &str) {
        self.untrack(from);
        self.transition(|inner| match inner {
            ConnInner::Idle(_t, conn, ..) | ConnInner::Active(_t, conn, ..) => {
                from.inc_all_time(MetricVariant::Disconnecting);
                from.inc_all_time(MetricVariant::Closed);
                to.insert(MetricVariant::Reconnecting);
                let f = connector.reconnect(conn, db).boxed_local();
                ConnInner::Reconnecting(Instant::now(), f)
            }
            _ => unreachable!(),
        });
    }

    pub fn reopen(&self, connector: &C, metrics: &MetricsAccum, db: &str) {
        self.transition(|inner| match inner {
            ConnInner::Active(t, conn) => {
                metrics.inc_all_time(MetricVariant::Disconnecting);
                metrics.inc_all_time(MetricVariant::Closed);
                metrics.transition(
                    MetricVariant::Active,
                    MetricVariant::Connecting,
                    t.elapsed(),
                );
                let f = connector.reconnect(conn, db).boxed_local();
                ConnInner::Connecting(Instant::now(), f)
            }
            _ => unreachable!(),
        });
    }

    pub fn poll_ready(
        &self,
        cx: &mut std::task::Context,
        metrics: &MetricsAccum,
        to: MetricVariant,
    ) -> Poll<ConnResult<()>> {
        let mut lock = self.inner.borrow_mut();
        debug_assert!(
            to == MetricVariant::Active || to == MetricVariant::Idle || to == MetricVariant::Closed
        );

        let res = match &mut *lock {
            ConnInner::Idle(..) => Ok(()),
            ConnInner::Connecting(t, f) | ConnInner::Reconnecting(t, f) => {
                match ready!(f.poll_unpin(cx)) {
                    Ok(c) => {
                        let elapsed = t.elapsed();
                        let from = (&std::mem::replace(&mut *lock, ConnInner::Transition)).into();
                        metrics.transition(from, to, elapsed);
                        if to == MetricVariant::Active {
                            *lock = ConnInner::Active(Instant::now(), c);
                        } else if to == MetricVariant::Idle {
                            *lock = ConnInner::Idle(Instant::now(), c);
                        } else {
                            unreachable!()
                        }
                        Ok(())
                    }
                    Err(err) => {
                        metrics.transition(
                            MetricVariant::Connecting,
                            MetricVariant::Failed,
                            t.elapsed(),
                        );
                        *lock = ConnInner::Failed;
                        Err(err)
                    }
                }
            }
            ConnInner::Disconnecting(t, f) => match ready!(f.poll_unpin(cx)) {
                Ok(_) => {
                    debug_assert_eq!(to, MetricVariant::Closed);
                    metrics.transition(MetricVariant::Disconnecting, to, t.elapsed());
                    *lock = ConnInner::Closed;
                    Ok(())
                }
                Err(err) => {
                    metrics.transition(
                        MetricVariant::Disconnecting,
                        MetricVariant::Failed,
                        t.elapsed(),
                    );
                    *lock = ConnInner::Failed;
                    Err(err)
                }
            },
            ConnInner::Failed => Err(ConnError::Other("Failed".into())),
            _ => unreachable!(),
        };
        Poll::Ready(res)
    }

    pub fn try_lock(&self, metrics: &MetricsAccum) -> bool {
        let mut lock = self.inner.borrow_mut();

        let res: bool;
        let old = std::mem::replace(&mut *lock, ConnInner::Transition);
        (*lock, res) = match old {
            ConnInner::Idle(t, conn) => {
                metrics.transition(MetricVariant::Idle, MetricVariant::Active, t.elapsed());
                (ConnInner::Active(Instant::now(), conn), true)
            }
            other => (other, false),
        };
        res
    }

    pub fn variant(&self) -> MetricVariant {
        (&*self.inner.borrow()).into()
    }

    pub fn untrack(&self, metrics: &MetricsAccum) {
        match &*self.inner.borrow() {
            ConnInner::Active(t, _)
            | ConnInner::Idle(t, _)
            | ConnInner::Connecting(t, _)
            | ConnInner::Disconnecting(t, _)
            | ConnInner::Reconnecting(t, _) => metrics.remove_time(self.variant(), t.elapsed()),
            other => metrics.remove(other.into()),
        }
    }
}

/// Connection state diagram:
///
/// ```text
///                      v-------------+
/// S -> Connecting -> Idle -> Active -+
///                 -> Failed          +-> Disconnecting -> Closed
/// ```
enum ConnInner<C: Connector> {
    /// Connecting connections hold a spot in the pool as they count towards quotas
    Connecting(Instant, Pin<Box<dyn Future<Output = ConnResult<C::Conn>>>>),
    /// Disconnecting connections hold a spot in the pool as they count towards quotas
    Disconnecting(Instant, Pin<Box<dyn Future<Output = ConnResult<()>>>>),
    /// Reconnecting hold a spot in the pool as they count towards quotas
    Reconnecting(Instant, Pin<Box<dyn Future<Output = ConnResult<C::Conn>>>>),
    /// The connection is alive, but it is not being held.
    Idle(Instant, C::Conn),
    /// The connection is alive, and is being held.
    Active(Instant, C::Conn),
    /// The connection is in a failed state.
    Failed,
    /// The connection is in a closed state.
    Closed,
    /// Transitioning between states. Used internally, never escapes an internal
    /// function.
    Transition,
}

impl<C: Connector> From<&ConnInner<C>> for MetricVariant {
    fn from(val: &ConnInner<C>) -> Self {
        match val {
            ConnInner::Connecting(..) => MetricVariant::Connecting,
            ConnInner::Disconnecting(..) => MetricVariant::Disconnecting,
            ConnInner::Reconnecting(..) => MetricVariant::Reconnecting,
            ConnInner::Idle(..) => MetricVariant::Idle,
            ConnInner::Active(..) => MetricVariant::Active,
            ConnInner::Failed => MetricVariant::Failed,
            ConnInner::Closed => MetricVariant::Closed,
            ConnInner::Transition => unreachable!(),
        }
    }
}

impl<C: Connector> std::fmt::Debug for ConnInner<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("ConnInner({:?})", MetricVariant::from(self)))
    }
}

pub struct ConnHandle<C: Connector> {
    pub(crate) conn: Conn<C>,
    pub(crate) state: Rc<ConnState>,
    pub(crate) dropped: Cell<bool>,
}

impl<C: Connector> std::fmt::Debug for ConnHandle<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "Connection({:?}, {:?})",
            self.state.db_name,
            self.conn.variant()
        ))
    }
}

impl<C: Connector> ConnHandle<C> {
    pub fn new(conn: Conn<C>, state: Rc<ConnState>) -> Self {
        Self {
            conn,
            state,
            dropped: Cell::default(),
        }
    }

    pub(crate) fn into_inner(mut self) -> Conn<C> {
        self.dropped.set(true);
        std::mem::replace(
            &mut self.conn,
            Conn {
                inner: Rc::new(RefCell::new(ConnInner::Closed)),
            },
        )
    }
}

impl<C: Connector> Drop for ConnHandle<C> {
    fn drop(&mut self) {
        if self.dropped.get() {
            return;
        }
        self.conn.transition(|inner| match inner {
            ConnInner::Active(t, c) => {
                self.state.metrics.transition(
                    MetricVariant::Active,
                    MetricVariant::Idle,
                    t.elapsed(),
                );
                self.state.waiters.trigger();
                ConnInner::Idle(Instant::now(), c)
            }
            _ => {
                unreachable!("Impossible state: {:?}", MetricVariant::from(&inner));
            }
        });
    }
}
