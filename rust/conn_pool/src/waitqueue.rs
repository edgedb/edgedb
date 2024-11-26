use crate::time::Instant;
use scopeguard::defer;
use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    future::poll_fn,
    rc::Rc,
    task::{Poll, Waker},
    time::Duration,
};
use tracing::trace;

struct WaitObject {
    waker: Waker,
    woke: Cell<bool>,
    gc: Cell<bool>,
    when: Instant,
}

/// Maintains a list of waiters for a given object. Similar to tokio's `Notify`
/// but explicitly not thread-safe.
pub struct WaitQueue {
    waiters: RefCell<VecDeque<Rc<WaitObject>>>,
    pub(crate) lock: Cell<usize>,
}

impl Default for WaitQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl WaitQueue {
    pub fn new() -> Self {
        Self {
            waiters: RefCell::default(),
            lock: Cell::default(),
        }
    }

    pub fn trigger(&self) {
        loop {
            if let Some(front) = self.waiters.borrow_mut().pop_front() {
                if front.gc.get() {
                    trace!("Tossing away a GC'd entry");
                    continue;
                }
                trace!("Triggered a waiter");
                front.woke.set(true);
                front.waker.wake_by_ref();
            } else {
                trace!("No waiters to trigger");
            }
            break;
        }
    }

    pub async fn queue(&self) {
        trace!("Queueing");
        let waker = poll_fn(|cx| Poll::Ready(cx.waker().clone())).await;

        let entry = Rc::new(WaitObject {
            waker,
            gc: Cell::default(),
            woke: Cell::default(),
            when: Instant::now(),
        });

        self.waiters.borrow_mut().push_back(entry.clone());

        defer! {
            entry.gc.set(true);
        }

        poll_fn(|_cx| {
            if entry.woke.get() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.lock.get()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn lock(&self) {
        self.lock.set(self.lock.get() + 1);
    }

    pub(crate) fn unlock(&self) {
        self.lock.set(self.lock.get() - 1);
    }

    pub(crate) fn oldest(&self) -> Duration {
        if let Some(entry) = self.waiters.borrow().front() {
            entry.when.elapsed()
        } else {
            Duration::default()
        }
    }
}
