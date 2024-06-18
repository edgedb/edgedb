use scopeguard::defer;
use std::{
    cell::{Cell, RefCell},
    future::poll_fn,
    marker::PhantomData,
    task::Poll,
};
use tracing::trace;

trait Time {
    type Instant;
    fn now() -> Self::Instant;
    fn elapsed_ms(instant: &Self::Instant) -> u128;
}

pub struct DefaultTime {}

impl Time for DefaultTime {
    type Instant = std::time::Instant;
    fn now() -> Self::Instant {
        std::time::Instant::now()
    }
    fn elapsed_ms(instant: &Self::Instant) -> u128 {
        instant.elapsed().as_millis()
    }
}

pub struct WaitQueue<T: Time = DefaultTime> {
    id: Cell<usize>,
    waiters: RefCell<Vec<(usize, T::Instant, std::task::Waker)>>,
    _time: PhantomData<T>,
}

impl Default for WaitQueue {
    fn default() -> Self {
        Self {
            id: Default::default(),
            waiters: RefCell::new(vec![]),
            _time: PhantomData,
        }
    }
}

impl<T: Time> WaitQueue<T> {
    pub fn trigger(&self) {
        if let Some((_, _, waker)) = self.waiters.borrow().first() {
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
        self.waiters.borrow_mut().push((id, T::now(), waker));
        defer! {
            // Remove ourselves
            self.waiters.borrow_mut().retain(|(id_, _, _)| *id_ != id);
        }

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
            if self.waiters.borrow().first().unwrap().0 == id {
                Poll::Ready(())
            } else {
                // Re-trigger whoever should be first
                self.trigger();
                Poll::Pending
            }
        })
        .await;
    }
}
