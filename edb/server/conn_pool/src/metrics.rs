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
        } else {
            if self.ptr == 0 {
                0
            } else {
                (self.cumulative / self.ptr as u64) as u32
            }
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
    counts: [usize; MetricVariant::COUNT],
    max: [usize; MetricVariant::COUNT],
    times: [RollingAverageU32<32>; MetricVariant::COUNT],
}

impl Into<PoolAlgorithmData> for &RawMetrics {
    fn into(self) -> PoolAlgorithmData {
        PoolAlgorithmData {
            active: self.counts[MetricVariant::Active as usize] as _,
            idle: self.counts[MetricVariant::Idle as usize],
            avg_connect_time: self.times[MetricVariant::Connecting as usize].avg() as _,
            avg_disconnect_time: self.times[MetricVariant::Disconnecting as usize].avg() as _,
            max_concurrent: self.max[MetricVariant::Active as usize],
            // TODO
            max_waiters: 0,
            waiters: 0,
            oldest_waiter_ms: 0,
        }
    }
}

impl Into<PoolAlgorithmData> for &MetricsAccum {
    fn into(self) -> PoolAlgorithmData {
        (&*self.raw.borrow()).into()
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

    pub fn insert(&self, to: MetricVariant) {
        let mut lock = self.raw.borrow_mut();
        lock.counts[to as usize] += 1;
        lock.max[to as usize] = lock.max[to as usize].max(lock.counts[to as usize]);
        // trace!("None->{to:?} ({})", lock[to as usize]);
        if let Some(parent) = &self.parent {
            parent.insert(to);
        }
    }

    pub fn transition(&self, from: MetricVariant, to: MetricVariant, time: Duration) {
        // trace!("{from:?}->{to:?}: {time:?}");
        let mut lock = self.raw.borrow_mut();
        lock.counts[from as usize] -= 1;
        lock.times[from as usize].accum(time.as_millis() as _);
        lock.counts[to as usize] += 1;
        lock.max[to as usize] = lock.max[to as usize].max(lock.counts[to as usize]);
        if let Some(parent) = &self.parent {
            parent.transition(from, to, time);
        }
    }

    pub fn remove_time(&self, from: MetricVariant, time: Duration) {
        let mut lock = self.raw.borrow_mut();
        lock.counts[from as usize] -= 1;
        lock.times[from as usize].accum(time.as_millis() as _);
        // trace!("{from:?}->None ({time:?})");
        if let Some(parent) = &self.parent {
            parent.remove_time(from, time);
        }
    }

    pub fn remove(&self, from: MetricVariant) {
        let mut lock = self.raw.borrow_mut();
        lock.counts[from as usize] -= 1;
        // trace!("{from:?}->None");
        if let Some(parent) = &self.parent {
            parent.remove(from);
        }
    }
}
