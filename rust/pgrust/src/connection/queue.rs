use std::future::Future;
use std::ops::DerefMut;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A queue of futures that can be polled in order.
///
/// Only one future will be active at a time. If no futures are active, the
/// waker will be triggered when the next future is submitted to the queue.
pub struct FutureQueue<T> {
    queue: tokio::sync::mpsc::UnboundedReceiver<Pin<Box<dyn Future<Output = T>>>>,
    sender: tokio::sync::mpsc::UnboundedSender<Pin<Box<dyn Future<Output = T>>>>,
    current: Option<Pin<Box<dyn Future<Output = T>>>>,
}

#[cfg(test)]
#[derive(Clone)]
pub struct FutureQueueSender<T> {
    sender: tokio::sync::mpsc::UnboundedSender<Pin<Box<dyn Future<Output = T>>>>,
}

#[cfg(test)]
impl<T> FutureQueueSender<T> {
    pub fn submit(&self, future: impl Future<Output = T> + 'static) {
        // This will never fail because the receiver still exists
        self.sender.send(Box::pin(future)).unwrap();
    }
}

impl<T> FutureQueue<T> {
    #[cfg(test)]
    pub fn sender(&self) -> FutureQueueSender<T> {
        FutureQueueSender {
            sender: self.sender.clone(),
        }
    }

    pub fn submit(&self, future: impl Future<Output = T> + 'static) {
        // This will never fail because we hold both ends of the channel.
        self.sender.send(Box::pin(future)).unwrap();
    }

    /// Poll the current future, or no current future, poll for the next item
    /// from the queue (and then poll that future).
    pub fn poll_next_unpin(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
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

            // Note that we loop around to poll this future until we get a Pending
            // result.
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use tokio::{
        task::LocalSet,
        time::{sleep, Duration},
    };

    #[tokio::test]
    async fn test_basic_queue() {
        LocalSet::new()
            .run_until(async {
                let mut queue = FutureQueue::default();
                let sender = queue.sender();

                // Spawn a task that sends some futures
                tokio::task::spawn_local(async move {
                    sleep(Duration::from_millis(10)).await;
                    sender.submit(async { 1 });
                    sleep(Duration::from_millis(10)).await;
                    sender.submit(async { 2 });
                    sleep(Duration::from_millis(10)).await;
                    sender.submit(async { 3 });
                });

                // Collect results
                let mut results = Vec::new();
                while let Some(value) = queue.next().await {
                    results.push(value);
                    if results.len() == 3 {
                        break;
                    }
                }

                assert_eq!(results, vec![1, 2, 3]);
            })
            .await;
    }

    #[tokio::test]
    async fn test_delayed_futures() {
        LocalSet::new()
            .run_until(async {
                let mut queue = FutureQueue::default();
                let sender = queue.sender();

                // Spawn task with delayed futures
                tokio::task::spawn_local(async move {
                    sleep(Duration::from_millis(10)).await;
                    sender.submit(async {
                        sleep(Duration::from_millis(50)).await;
                        1
                    });
                    sleep(Duration::from_millis(10)).await;
                    sender.submit(async {
                        sleep(Duration::from_millis(10)).await;
                        2
                    });
                });

                // Even though second future completes first, results should be in order of sending
                let mut results = Vec::new();
                while let Some(value) = queue.next().await {
                    results.push(value);
                    if results.len() == 2 {
                        break;
                    }
                }

                assert_eq!(results, vec![1, 2]);
            })
            .await;
    }
}
