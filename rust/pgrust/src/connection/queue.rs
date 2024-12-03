use std::future::Future;
use std::ops::DerefMut;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A queue of futures that can be polled in order. This will trigger the waker
/// for `poll_next` when new futures are submitted to the queue.
pub struct FutureQueue<T> {
    queue: tokio::sync::mpsc::UnboundedReceiver<Pin<Box<dyn Future<Output = T>>>>,
    sender: tokio::sync::mpsc::UnboundedSender<Pin<Box<dyn Future<Output = T>>>>,
    current: Option<Pin<Box<dyn Future<Output = T>>>>,
}

impl<T> FutureQueue<T> {
    pub fn submit(&self, future: impl Future<Output = T> + 'static) {
        // This will never fail because we hold both ends of the channel.
        self.sender.send(Box::pin(future)).unwrap();
    }

    pub fn poll_next_unpin(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        // Poll until the current future is complete, or until the queue is pending.
        loop {
            if let Some(future) = self.current.as_mut() {
                match future.as_mut().poll(cx) {
                    Poll::Ready(output) => {
                        self.current = None;
                        return Poll::Ready(Some(output));
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            // If there is no current future, try to receive the next one from the queue.
            let next = match self.queue.poll_recv(cx) {
                Poll::Ready(Some(next)) => next,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            };

            self.current = Some(next);
        }
    }
}

impl<T> Default for FutureQueue<T> {
    fn default() -> Self {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        Self {
            queue: receiver,
            sender,
            current: None,
        }
    }
}

impl<T> futures::Stream for FutureQueue<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // We're Unpin
        let this = self.deref_mut();
        this.poll_next_unpin(cx)
    }
}
