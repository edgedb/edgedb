use derive_more::AddAssign;
use std::{cell::RefCell, time::Duration};
use strum::EnumCount;

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
    avg: u64,
    ptr: u8,
}

impl<const SIZE: usize> Default for RollingAverageU32<SIZE> {
    fn default() -> Self {
        assert!(SIZE <= u8::MAX as _);
        Self {
            values: [0; SIZE],
            ptr: 0,
            avg: 0,
        }
    }
}

impl<const SIZE: usize> RollingAverageU32<SIZE> {
    pub fn reset(&mut self) {
        // Note that we don't reset the ptr here, doesn't actually matter
        self.values = [0; SIZE];
        self.avg = 0;
    }

    fn accum(&mut self, new: u32) {
        let size = SIZE as u8;
        let old = std::mem::replace(&mut self.values[self.ptr as usize], new);
        self.avg -= old as u64;
        self.avg += new as u64;
        self.ptr = (self.ptr + 1) % size;
    }

    #[inline]
    pub fn avg(&self) -> u32 {
        (self.avg / SIZE as u64) as u32
    }
}

#[derive(Debug, Default)]
pub struct PoolMetrics {
    value: [usize; MetricVariant::COUNT],
    max: [usize; MetricVariant::COUNT],
    avg_time: [u32; MetricVariant::COUNT],

    blocks: Vec<ConnMetrics>,
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

/// Metrics accumulator. Designed to be updated without a lock.
#[derive(Debug, Default)]
pub struct MetricsAccum {
    counts: RefCell<[usize; 8]>,
    max: RefCell<[usize; 8]>,
    times: RefCell<[RollingAverageU32<32>; 8]>,
}

impl MetricsAccum {
    pub fn summary(&self) -> ConnMetrics {
        ConnMetrics {
            value: *self.counts.borrow(),
            max: *self.max.borrow(),
            avg_time: Default::default(),
        }
    }

    pub fn insert(&self, to: MetricVariant) {
        let mut lock = self.counts.borrow_mut();
        lock[to as usize] += 1;
        let mut max = self.max.borrow_mut();
        max[to as usize] = max[to as usize].max(lock[to as usize]);
        // trace!("None->{to:?} ({})", lock[to as usize]);
    }

    pub fn transition(&self, from: MetricVariant, to: MetricVariant, time: Duration) {
        // trace!("{from:?}->{to:?}: {time:?}");
        let mut lock = self.counts.borrow_mut();
        lock[from as usize] -= 1;
        self.times.borrow_mut()[from as usize].accum(time.as_millis() as _);
        lock[to as usize] += 1;
        let mut max = self.max.borrow_mut();
        max[to as usize] = max[to as usize].max(lock[to as usize]);
    }

    pub fn remove_time(&self, from: MetricVariant, time: Duration) {
        let mut lock = self.counts.borrow_mut();
        lock[from as usize] -= 1;
        self.times.borrow_mut()[from as usize].accum(time.as_millis() as _);
        // trace!("{from:?}->None ({time:?})");
    }

    pub fn remove(&self, from: MetricVariant) {
        let mut lock = self.counts.borrow_mut();
        lock[from as usize] -= 1;
        // trace!("{from:?}->None");
    }
}
