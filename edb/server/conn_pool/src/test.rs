//! Test utilities.
use std::{future::Future, time::Duration};

use mock_instant::{thread_local::Instant, thread_local::MockClock};
use tracing::{info, trace};

use crate::conn::{ConnResult, Connector};

#[derive(Debug)]
pub struct BasicConnector {
    delay: bool,
}

impl BasicConnector {
    pub fn no_delay() -> Self {
        BasicConnector { delay: false }
    }
    pub fn delay() -> Self {
        BasicConnector { delay: true }
    }
}

/// Perform a virtual async sleep that advances all of the virtual clocks (`mock_instant` and `tokio`).
pub async fn virtual_sleep(duration: Duration) {
    // Old mock instant so we can detect simultaneous clock advances
    let now = Instant::now();
    // Perform the mock sleep, assumes that the tokio time is paused which will
    // auto-advance the paused clock.
    tokio::time::sleep(duration).await;
    // Ensure the mock clock is advanced to the correct state when this tokio sleep completes. Note
    // that other virtual sleeps may have occurred.
    MockClock::advance(duration - now.elapsed());
}

impl Connector for BasicConnector {
    type Conn = ();
    fn connect(&self, db: &str) -> impl Future<Output = ConnResult<Self::Conn>> + 'static {
        let delay = self.delay;
        async move {
            if delay {
                virtual_sleep(Duration::from_millis(100)).await;
            }
            Ok(())
        }
    }
    fn reconnect(
        &self,
        conn: Self::Conn,
        db: &str,
    ) -> impl Future<Output = ConnResult<Self::Conn>> + 'static {
        let delay = self.delay;
        async move {
            if delay {
                virtual_sleep(Duration::from_millis(100)).await;
            }
            Ok(conn)
        }
    }
    fn disconnect(&self, conn: Self::Conn) -> impl Future<Output = ConnResult<()>> + 'static {
        let delay = self.delay;
        async move {
            if delay {
                virtual_sleep(Duration::from_millis(100)).await;
            }
            Ok(())
        }
    }
}
