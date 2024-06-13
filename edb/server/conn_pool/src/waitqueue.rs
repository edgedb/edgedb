use scopeguard::defer;
use std::{
    cell::{Cell, RefCell},
    future::poll_fn,
    task::Poll,
};

#[derive(Default)]
pub struct WaitQueue {
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
                Poll::Pending
            }
        })
        .await;
    }
}
