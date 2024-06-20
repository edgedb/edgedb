//! Test utilities.
use std::{future::Future, time::Duration};

use mock_instant::thread_local::MockClock;

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

impl Connector for BasicConnector {
    type Conn = ();
    fn connect(&self, db: &str) -> impl Future<Output = ConnResult<Self::Conn>> + 'static {
        let delay = self.delay;
        async move {
            if delay {
                tokio::task::yield_now().await;
                MockClock::advance(Duration::from_millis(100));
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
                tokio::task::yield_now().await
            }
            Ok(conn)
        }
    }
    fn disconnect(&self, conn: Self::Conn) -> impl Future<Output = ConnResult<()>> + 'static {
        let delay = self.delay;
        async move {
            if delay {
                tokio::task::yield_now().await
            }
            Ok(())
        }
    }
}
