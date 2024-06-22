use derive_more::AddAssign;
use std::{cell::RefCell, rc::Rc, time::Duration};
use strum::EnumCount;

use crate::algo::PoolAlgorithmData;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, strum::EnumCount)]
pub enum MetricVariant {
    Connecting,
    Disconnecting,
    Idle,
    Active,
    Poisoned,
    Failed,
    Closed,
    Waiting,
}

/// Maintains a rolling average of `u32` values.
#[derive(Debug, PartialEq, Eq)]
struct RollingAverageU32<const SIZE: usize> {
    values: [u32; SIZE],
    cumulative: u64,
    ptr: u8,
    /// If we've never rolled over, we cannot divide the entire array by `SIZE`
    /// and have to use `ptr` instead.
    rollover: bool,
}

impl<const SIZE: usize> Default for RollingAverageU32<SIZE> {
    fn default() -> Self {
        assert!(SIZE <= u8::MAX as _);
        Self {
            values: [0; SIZE],
            ptr: 0,
            cumulative: 0,
            rollover: false,
        }
    }
}

impl<const SIZE: usize> RollingAverageU32<SIZE> {
    pub fn reset(&mut self) {
        *self = Default::default()
    }

    fn accum(&mut self, new: u32) {
        let size = SIZE as u8;
        let old = std::mem::replace(&mut self.values[self.ptr as usize], new);
        self.cumulative -= old as u64;
        self.cumulative += new as u64;
        self.ptr = (self.ptr + 1) % size;
        if self.ptr == 0 {
            self.rollover = true;
        }
    }

    #[inline]
    pub fn avg(&self) -> u32 {
        if self.rollover {
            (self.cumulative / SIZE as u64) as u32
        } else if self.ptr == 0 {
            0
        } else {
            (self.cumulative / self.ptr as u64) as u32
        }
    }
}

#[derive(Debug, Default)]
pub struct PoolMetrics {
    pub pool: ConnMetrics,
    pub blocks: Vec<ConnMetrics>,
}

#[derive(Debug, Default)]
pub struct ConnMetrics {
    value: [usize; MetricVariant::COUNT],
    max: [usize; MetricVariant::COUNT],
    avg_time: [u32; MetricVariant::COUNT],
}

impl PartialEq for ConnMetrics {
    fn eq(&self, other: &Self) -> bool {
        // HACK: Intentionally ignoring max/avg_time for unit tests
        self.value == other.value
    }
}

impl ConnMetrics {
    pub fn with(variant: MetricVariant, count: usize) -> Self {
        let mut summary = [0; MetricVariant::COUNT];
        summary[variant as usize] = count;
        Self {
            value: summary,
            max: Default::default(),
            avg_time: Default::default(),
        }
    }
}

impl AddAssign for ConnMetrics {
    fn add_assign(&mut self, rhs: Self) {
        for i in 0..self.value.len() {
            self.value[i] += rhs.value[i];
            self.max[i] += rhs.max[i];
        }
    }
}

#[derive(Debug, Default)]
struct RawMetrics {
    /// The total number of non-waiting connections.
    total: usize,
    /// The max total number of non-waiting connections.
    total_max: usize,
    /// The number of connections per state.
    counts: [usize; MetricVariant::COUNT],
    /// The max number of connections per state.
    max: [usize; MetricVariant::COUNT],
    /// The time spent in each state.
    times: [RollingAverageU32<32>; MetricVariant::COUNT],
}

impl RawMetrics {
    #[inline(always)]
    fn inc(&mut self, to: MetricVariant) {
        self.counts[to as usize] += 1;
        self.max[to as usize] = self.max[to as usize].max(self.counts[to as usize]);
    }

    #[inline(always)]
    fn inc_total(&mut self, to: MetricVariant) {
        if to != MetricVariant::Waiting {
            self.total += 1;
            self.total_max = self.total_max.max(self.total);
        }
    }

    #[inline(always)]
    fn dec(&mut self, from: MetricVariant) {
        self.counts[from as usize] -= 1;
    }

    #[inline(always)]
    fn time(&mut self, from: MetricVariant, time: Duration) {
        self.times[from as usize].accum(time.as_millis() as _);
    }

    #[inline(always)]
    fn dec_total(&mut self, from: MetricVariant) {
        if from != MetricVariant::Waiting {
            self.total -= 1;
        }
    }
}

/// Metrics accumulator. Designed to be updated without a lock.
#[derive(Debug, Default)]
pub struct MetricsAccum {
    raw: RefCell<RawMetrics>,
    parent: Option<Rc<MetricsAccum>>,
}

impl MetricsAccum {
    pub fn new(parent: Option<Rc<MetricsAccum>>) -> Self {
        Self {
            parent,
            ..Default::default()
        }
    }

    #[inline]
    pub fn total(&self) -> usize {
        self.raw.borrow().total
    }

    pub fn summary(&self) -> ConnMetrics {
        let lock = self.raw.borrow_mut();
        let mut avg_time: [u32; MetricVariant::COUNT] = Default::default();
        for i in 0..MetricVariant::COUNT {
            avg_time[i] = lock.times[i].avg();
        }
        ConnMetrics {
            value: lock.counts,
            max: lock.max,
            avg_time,
        }
    }

    #[inline]
    pub fn insert(&self, to: MetricVariant) {
        let mut lock = self.raw.borrow_mut();
        lock.inc(to);
        lock.inc_total(to);
        // trace!("None->{to:?} ({})", lock[to as usize]);
        if let Some(parent) = &self.parent {
            parent.insert(to);
        }
    }

    #[inline]
    pub fn set_value(&self, to: MetricVariant, len: usize) {
        let mut lock = self.raw.borrow_mut();
        debug_assert_eq!(lock.counts[to as usize], 0);
        lock.counts[to as usize] = len;
        lock.total += len;
        lock.max[to as usize] = lock.max[to as usize].max(lock.counts[to as usize]);
    }

    #[inline]
    pub fn transition(&self, from: MetricVariant, to: MetricVariant, time: Duration) {
        // trace!("{from:?}->{to:?}: {time:?}");
        let mut lock = self.raw.borrow_mut();
        lock.dec(from);
        lock.time(from, time);
        lock.inc(to);
        if let Some(parent) = &self.parent {
            parent.transition(from, to, time);
        }
    }

    #[inline]
    pub fn remove_time(&self, from: MetricVariant, time: Duration) {
        let mut lock = self.raw.borrow_mut();
        lock.dec(from);
        lock.time(from, time);
        lock.dec_total(from);
        // trace!("{from:?}->None ({time:?})");
        if let Some(parent) = &self.parent {
            parent.remove_time(from, time);
        }
    }

    #[inline]
    pub fn remove(&self, from: MetricVariant) {
        let mut lock = self.raw.borrow_mut();
        lock.dec(from);
        lock.dec_total(from);
        // trace!("{from:?}->None");
        if let Some(parent) = &self.parent {
            parent.remove(from);
        }
    }
}

impl From<&RawMetrics> for PoolAlgorithmData {
    fn from(val: &RawMetrics) -> Self {
        PoolAlgorithmData {
            active: val.counts[MetricVariant::Active as usize] as _,
            idle: val.counts[MetricVariant::Idle as usize],
            waiters: val.counts[MetricVariant::Waiting as usize],
            avg_connect_time: val.times[MetricVariant::Connecting as usize].avg() as _,
            avg_disconnect_time: val.times[MetricVariant::Disconnecting as usize].avg() as _,
            max_concurrent: val.max[MetricVariant::Active as usize],
            max_waiters: val.max[MetricVariant::Waiting as usize],
            // TODO
            oldest_waiter_ms: 0,
        }
    }
}

impl From<&MetricsAccum> for PoolAlgorithmData {
    fn from(val: &MetricsAccum) -> Self {
        (&*val.raw.borrow()).into()
    }
}
