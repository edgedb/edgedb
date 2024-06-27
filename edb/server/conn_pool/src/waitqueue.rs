use scopeguard::defer;
use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    future::poll_fn,
    task::{Poll, Waker},
};
use tracing::trace;

#[cfg(test)]
use mock_instant::thread_local::Instant;
#[cfg(not(test))]
use std::time::Instant;

pub struct WaitQueue {
    id: Cell<usize>,
    waiters: RefCell<VecDeque<(usize, Instant, Waker)>>,
    pub(crate) lock: Cell<usize>,
}

impl WaitQueue {
    pub fn new() -> Self {
        Self {
            id: Cell::default(),
            waiters: RefCell::default(),
            lock: Cell::default(),
        }
    }

    pub fn trigger(&self) {
        if let Some((_, _, waker)) = self.waiters.borrow().front() {
            trace!("Triggered a waiter");
            waker.wake_by_ref()
        } else {
            trace!("No waiters to trigger");
        }
    }

    pub async fn queue(&self) {
        // TODO: messy
        trace!("queue");
        let id = self.id.get() + 1;
        self.id.set(id);
        let waker = poll_fn(|cx| Poll::Ready(cx.waker().clone())).await;
        self.waiters
            .borrow_mut()
            .push_back((id, Instant::now(), waker));
        let normal_exit = Cell::new(false);

        defer! {
            // Remove ourselves
            if !normal_exit.get() {
                self.waiters.borrow_mut().retain(|(id_, _, _)| *id_ != id);
            }
        }

        // Wait for a waker
        let mut defer = true;
        poll_fn(|cx| {
            if defer {
                defer = false;
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .await;

        // Wait for us to be first in line
        poll_fn(|_| {
            if self.waiters.borrow().front().unwrap().0 == id {
                Poll::Ready(())
            } else {
                // Re-trigger whoever should be first
                self.trigger();
                Poll::Pending
            }
        })
        .await;

        let (_, t, _) = self.waiters.borrow_mut().pop_front().unwrap();

        // Prevent defer block
        normal_exit.set(true);
    }

    pub fn len(&self) -> usize {
        self.waiters.borrow().len()
    }

    pub(crate) fn lock(&self) {
        self.lock.set(self.lock.get() + 1);
    }
    pub(crate) fn unlock(&self) {
        self.lock.set(self.lock.get() - 1);
    }
}
