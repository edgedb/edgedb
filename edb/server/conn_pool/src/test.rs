//! Test utilities.
use std::{future::Future, ops::{Range, RangeBounds}, time::Duration};

use mock_instant::{thread_local::Instant, thread_local::MockClock};

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

#[derive(smart_default::SmartDefault)]
pub struct Spec {
     #[default = 30]
    pub timeout: usize,
     #[default = 1.1]
    pub duration: f64,
    pub capacity: usize,
    pub conn_cost_base: f64,
    pub conn_cost_var: f64,
    pub dbs: Vec<DBSpec>,
     #[default = 0.006]
    pub disconn_cost_base: f64,
     #[default = 0.015]
    pub disconn_cost_var: f64,
    pub score: Vec<Box<dyn ScoringMethod>>
}

pub trait ScoringMethod {
}

pub struct Score {
   pub v100: f64,
   pub v90: f64,
   pub v60: f64,
   pub v0: f64,
   pub weight: f64,
}

pub struct LatencyDistribution {
    pub score: Score,
    pub group: Range<usize>
}

impl ScoringMethod for LatencyDistribution {
}

impl <T: ScoringMethod + 'static> From<T> for Box<dyn ScoringMethod> {
    fn from(value: T) -> Self {
        Box::new(value)
    }
}

pub struct ConnectionOverhead {
    pub score: Score,
}

impl ScoringMethod for ConnectionOverhead {
}

pub struct DBSpec {
    pub db: String,
    pub start_at: f64,
    pub end_at: f64,
    pub qps: usize,  
    pub query_cost_base: f64,
    pub query_cost_var: f64,  
}
