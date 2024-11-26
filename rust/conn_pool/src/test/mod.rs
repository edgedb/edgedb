//! Test utilities.
use itertools::Itertools;
use rand::random;
use statrs::statistics::{Data, Distribution, OrderStatistics, Statistics};
use std::{
    borrow::Cow,
    cell::{Cell, RefCell},
    collections::{BTreeMap, HashMap},
    future::Future,
    ops::Range,
    rc::Rc,
    time::Duration,
};

use crate::{
    conn::{ConnError, ConnResult, Connector},
    metrics::{MetricVariant, PoolMetrics},
    PoolConfig,
};

pub mod spec;

#[derive(derive_more::Debug)]
pub struct BasicConnector {
    #[allow(clippy::type_complexity)]
    #[debug(skip)]
    delay: Option<Rc<dyn Fn(bool) -> Result<Duration, ()>>>,
    fail_next_connect: Cell<bool>,
    fail_next_disconnect: Cell<bool>,
}

impl BasicConnector {
    pub fn no_delay() -> Self {
        BasicConnector {
            delay: None,
            fail_next_connect: Default::default(),
            fail_next_disconnect: Default::default(),
        }
    }

    pub fn delay(f: impl Fn(bool) -> Result<Duration, ()> + 'static) -> Self {
        BasicConnector {
            delay: Some(Rc::new(f)),
            fail_next_connect: Default::default(),
            fail_next_disconnect: Default::default(),
        }
    }

    pub fn fail_next_connect(&self) {
        self.fail_next_connect.set(true);
    }

    pub fn fail_next_disconnect(&self) {
        self.fail_next_disconnect.set(true);
    }

    fn duration(&self, disconnect: bool) -> ConnResult<Option<Duration>, String> {
        if disconnect && self.fail_next_disconnect.replace(false) {
            return Err(ConnError::Underlying("failed".to_string()));
        }
        if !disconnect && self.fail_next_connect.replace(false) {
            return Err(ConnError::Underlying("failed".to_string()));
        }
        if let Some(f) = &self.delay {
            Ok(Some(f(disconnect).map_err(|_| {
                ConnError::Underlying("failed".to_string())
            })?))
        } else {
            Ok(None)
        }
    }
}

impl Connector for BasicConnector {
    type Conn = ();
    type Error = String;
    fn connect(
        &self,
        _db: &str,
    ) -> impl Future<Output = ConnResult<Self::Conn, Self::Error>> + 'static {
        let connect = self.duration(false);
        async move {
            if let Some(f) = connect? {
                tokio::time::sleep(f).await;
            }
            Ok(())
        }
    }
    fn reconnect(
        &self,
        conn: Self::Conn,
        _db: &str,
    ) -> impl Future<Output = ConnResult<Self::Conn, Self::Error>> + 'static {
        let connect = self.duration(false);
        let disconnect = self.duration(true);
        async move {
            if let Some(f) = disconnect? {
                tokio::time::sleep(f).await;
            }
            if let Some(f) = connect? {
                tokio::time::sleep(f).await;
            }
            Ok(conn)
        }
    }
    fn disconnect(
        &self,
        _conn: Self::Conn,
    ) -> impl Future<Output = ConnResult<(), Self::Error>> + 'static {
        let disconnect = self.duration(true);
        async move {
            if let Some(f) = disconnect? {
                tokio::time::sleep(f).await;
            }
            Ok(())
        }
    }
}

#[derive(Clone, Default)]
pub struct Latencies {
    data: Rc<RefCell<HashMap<String, Vec<f64>>>>,
}

/// Helper function for [`Stats`] [`Debug`] impl.
#[allow(unused)]
fn m(v: &f64) -> Duration {
    if *v <= 0.000_001 {
        Duration::ZERO
    } else {
        Duration::from_secs_f64(*v)
    }
}

#[derive(derive_more::Debug)]
#[allow(unused)]
#[debug(
    "#{count} %{{1,25,50,75,99}}: {:?}/{:?}/{:?}/{:?}/{:?}, x̄: {:?} Πx: {:?}",
    m(p01),
    m(p25),
    m(p50),
    m(p75),
    m(p99),
    m(mean),
    m(geometric_mean)
)]
struct Stats {
    p01: f64,
    p25: f64,
    p50: f64,
    p75: f64,
    p99: f64,
    geometric_mean: f64,
    mean: f64,
    count: usize,
}

impl Latencies {
    pub fn mark(&self, db: &str, latency: f64) {
        self.data
            .borrow_mut()
            .entry(db.to_owned())
            .or_default()
            .push(latency.max(0.000_001));
    }

    fn len(&self) -> usize {
        let mut len = 0;
        for values in self.data.borrow().values() {
            len += values.len()
        }
        len
    }
}

impl std::fmt::Debug for Latencies {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("Latencies");
        let mut data = self.data.borrow_mut();
        let mut all = vec![];
        for key in data.keys().cloned().sorted() {
            let data = data.get_mut(&key).unwrap();
            all.extend_from_slice(data);
            let stats = stats(data);
            s.field(&key, &stats);
        }
        let stats = stats(&mut all);
        s.field("all", &stats);
        s.finish()
    }
}

fn stats(data: &mut [f64]) -> Stats {
    let geometric_mean = data.geometric_mean();
    let mut data = Data::new(data);
    let mean = data.mean().unwrap();

    Stats {
        p01: data.percentile(1),
        p25: data.percentile(25),
        p50: data.percentile(50),
        p75: data.percentile(75),
        p99: data.percentile(99),
        geometric_mean,
        mean,
        count: data.len(),
    }
}

#[derive(smart_default::SmartDefault)]
pub struct Spec {
    pub name: Cow<'static, str>,
    pub desc: &'static str,
    #[default = 30]
    pub timeout: usize,
    #[default = 1.1]
    pub duration: f64,
    pub capacity: usize,
    pub conn_cost: Triangle,
    #[default = 0]
    pub conn_failure_percentage: u8,
    pub dbs: Vec<DBSpec>,
    #[default(Triangle(0.006, 0.0015))]
    pub disconn_cost: Triangle,
    pub score: Vec<Score>,
}

impl Spec {
    pub fn scale(&mut self, time_scale: f64) {
        self.duration *= time_scale;
        for db in &mut self.dbs {
            db.scale(time_scale);
        }
    }
}

#[derive(derive_more::Debug)]
pub struct Scored {
    pub description: String,
    #[debug(skip)]
    pub detailed_calculation: Box<dyn Fn(usize) -> String + Send + Sync>,
    pub raw_value: f64,
}

#[derive(Debug)]
pub struct WeightedScored {
    pub weight: f64,
    pub score: f64,
    pub scored: Scored,
}

#[derive(Debug)]
pub struct QoS {
    pub scores: Vec<WeightedScored>,
    pub qos: f64,
}

#[derive(Default, derive_more::Deref, derive_more::DerefMut, derive_more::IntoIterator)]
pub struct SuiteQoS(#[into_iterator(owned, ref, ref_mut)] BTreeMap<Cow<'static, str>, QoS>);

impl std::fmt::Debug for SuiteQoS {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("SuiteQos");
        for (name, qos) in self {
            s.field(name, &format!("QoS = {:.02}", qos.qos));
        }
        s.field("qos", &self.qos());
        s.field("qos_rms", &self.qos_rms_error());
        s.field("qos_min", &self.qos_min());
        s.finish()
    }
}

impl SuiteQoS {
    pub fn qos(&self) -> f64 {
        let mut total = 0.0;
        for qos in self.values() {
            total += qos.qos;
        }
        total /= self.len() as f64;
        if !total.is_normal() || total < 0.0 {
            0.0
        } else {
            total
        }
    }

    /// Return the root-mean-square error QoS. The error between the QoS and 100
    /// is squared, averaged, and we subtract that from 100 for a final score.
    pub fn qos_rms_error(&self) -> f64 {
        let mut total = 0.0;
        for qos in self.values() {
            total += (100.0 - qos.qos).powf(2.0);
        }
        total /= self.len() as f64;
        total = 100.0 - total.sqrt();
        if !total.is_normal() || total < 0.0 {
            0.0
        } else {
            total
        }
    }

    /// Return the root-mean-square error QoS. The error between the QoS and 100
    /// is squared, averaged, and we subtract that from 100 for a final score.
    pub fn qos_min(&self) -> f64 {
        let mut min: f64 = 100.0;
        for qos in self.values() {
            min = min.min(qos.qos);
        }
        if !min.is_normal() || min < 0.0 {
            0.0
        } else {
            min
        }
    }
}

pub trait ScoringMethod {
    fn score(&self, latencies: &Latencies, metrics: &PoolMetrics, config: &PoolConfig) -> Scored;
}

pub struct Score {
    pub v100: f64,
    pub v90: f64,
    pub v60: f64,
    pub v0: f64,
    pub weight: f64,
    pub method: Box<dyn ScoringMethod + Send + Sync + 'static>,
}

impl Score {
    pub fn new(
        weight: f64,
        scores: [f64; 4],
        method: impl ScoringMethod + Send + Sync + 'static,
    ) -> Self {
        Self {
            weight,
            v0: scores[0],
            v60: scores[1],
            v90: scores[2],
            v100: scores[3],
            method: Box::new(method),
        }
    }

    pub fn calculate(&self, value: f64) -> f64 {
        if value.is_nan() || value.is_infinite() {
            return 0.0;
        }

        let intervals = [
            (self.v100, self.v90, 90.0, 10.0),
            (self.v90, self.v60, 60.0, 30.0),
            (self.v60, self.v0, 0.0, 60.0),
        ];

        for &(v1, v2, base, diff) in &intervals {
            let v_min = v1.min(v2);
            let v_max = v1.max(v2);
            if v_min <= value && value < v_max {
                return base + (value - v2).abs() / (v_max - v_min) * diff;
            }
        }

        if self.v0 > self.v100 {
            if value < self.v100 {
                100.0
            } else {
                0.0
            }
        } else if value < self.v0 {
            0.0
        } else {
            100.0
        }
    }
}

pub struct LatencyDistribution {
    pub group: Range<usize>,
}

impl ScoringMethod for LatencyDistribution {
    fn score(&self, latencies: &Latencies, _metrics: &PoolMetrics, _config: &PoolConfig) -> Scored {
        let dbs = self.group.clone().map(|t| format!("t{t}")).collect_vec();
        let mut data = latencies.data.borrow_mut();
        let fail = Cell::new(false);

        // Calculates the average CV (coefficient of variation) of the given
        // distributions. The result is a float ranging from zero indicating how
        // different the given distributions are, where zero means no
        // difference. Known defect: when the mean value is close to zero, the
        // coefficient of variation will approach infinity and is therefore
        // sensitive to small changes.
        let values = (1..=9)
            .map(move |n| {
                let decile = Data::new(
                    dbs.iter()
                        .map(|db| {
                            let Some(data) = data.get_mut(db) else {
                                fail.set(true);
                                return 0.0;
                            };
                            let mut data = Data::new(data.as_mut_slice());
                            // This is equivalent to Python's statistics.quartile(n=10)
                            data.percentile(n * 10)
                        })
                        .collect_vec(),
                );
                let cv = decile.std_dev().unwrap_or_default() / decile.mean().unwrap_or_default();
                if cv.is_normal() {
                    cv
                } else {
                    0.0
                }
            })
            .collect_vec();
        let mean = values.iter().geometric_mean();
        Scored {
            description: format!("Average CV for range {:?}", self.group),
            detailed_calculation: Box::new(move |precision| format!("{values:.precision$?}")),
            raw_value: mean,
        }
    }
}

impl<T: ScoringMethod + 'static> From<T> for Box<dyn ScoringMethod> {
    fn from(value: T) -> Self {
        Box::new(value)
    }
}

pub struct ConnectionOverhead {}

impl ScoringMethod for ConnectionOverhead {
    fn score(&self, latencies: &Latencies, metrics: &PoolMetrics, _config: &PoolConfig) -> Scored {
        let reconnects = metrics.all_time[MetricVariant::Reconnecting];
        let count = latencies.len();
        let raw_value = reconnects as f64 / count as f64;
        Scored {
            description: "Num of re-connects/query".to_owned(),
            detailed_calculation: Box::new(move |_precision| format!("{reconnects}/{count}")),
            raw_value,
        }
    }
}

pub struct LatencyRatio {
    pub percentile: u8,
    pub dividend: Range<usize>,
    pub divisor: Range<usize>,
}

impl ScoringMethod for LatencyRatio {
    fn score(&self, latencies: &Latencies, _metrics: &PoolMetrics, _config: &PoolConfig) -> Scored {
        let mut data = latencies.data.borrow_mut();
        let dbs = self.divisor.clone().map(|t| format!("t{t}")).collect_vec();
        let divisor = dbs
            .iter()
            .map(|db| {
                let Some(data) = data.get_mut(db) else {
                    return f64::NAN;
                };
                let mut data = Data::new(data.as_mut_slice());
                data.percentile(self.percentile as _)
            })
            .mean();
        let dbs = self.dividend.clone().map(|t| format!("t{t}")).collect_vec();
        let dividend = dbs
            .iter()
            .map(|db| {
                let Some(data) = data.get_mut(db) else {
                    return f64::NAN;
                };
                let mut data = Data::new(data.as_mut_slice());
                data.percentile(self.percentile as _)
            })
            .mean();
        let raw_value = dividend / divisor;
        Scored {
            description: format!(
                "P{} ratio {:?}/{:?}",
                self.percentile, self.dividend, self.divisor
            ),
            detailed_calculation: Box::new(move |precision| {
                format!("{dividend:.precision$}/{divisor:.precision$}")
            }),
            raw_value,
        }
    }
}

pub struct EndingCapacity {}

impl ScoringMethod for EndingCapacity {
    fn score(&self, _latencies: &Latencies, metrics: &PoolMetrics, _config: &PoolConfig) -> Scored {
        let total = metrics.pool.total;
        let raw_value = total as _;
        Scored {
            description: "Ending capacity".to_string(),
            detailed_calculation: Box::new(move |_| format!("{total}")),
            raw_value,
        }
    }
}

pub struct AbsoluteLatency {
    pub percentile: u8,
    pub group: Range<usize>,
}

impl ScoringMethod for AbsoluteLatency {
    fn score(&self, latencies: &Latencies, _metrics: &PoolMetrics, _config: &PoolConfig) -> Scored {
        let mut data = latencies.data.borrow_mut();
        let dbs = self.group.clone().map(|t| format!("t{t}")).collect_vec();
        let raw_value = dbs
            .iter()
            .map(|db| {
                let Some(data) = data.get_mut(db) else {
                    return f64::NAN;
                };
                let mut data = Data::new(data.as_mut_slice());
                data.percentile(self.percentile as _)
            })
            .mean();

        Scored {
            description: format!("Absolute P{} value {:?}", self.percentile, self.group),
            detailed_calculation: Box::new(move |precision| format!("{raw_value:.precision$}")),
            raw_value,
        }
    }
}

#[derive(Debug)]
pub struct DBSpec {
    pub db: usize,
    pub start_at: f64,
    pub end_at: f64,
    pub qps: usize,
    pub query_cost: Triangle,
}

impl DBSpec {
    pub fn scale(&mut self, time_scale: f64) {
        self.start_at *= time_scale;
        self.end_at *= time_scale;
    }
}

#[derive(Default, derive_more::Debug, Clone, Copy)]
#[debug("{0:?}±{1:?}", Duration::from_secs_f64(self.0), Duration::from_secs_f64(self.1))]
pub struct Triangle(pub f64, pub f64);

impl Triangle {
    pub fn random(&self) -> f64 {
        self.0 + (random::<f64>() * 2.0 - 1.0) * self.1
    }

    pub fn random_duration(&self) -> Duration {
        let r = self.random();
        if r <= 0.001 {
            Duration::from_millis(1)
        } else {
            Duration::from_secs_f64(r)
        }
    }
}
