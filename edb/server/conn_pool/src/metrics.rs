use derive_more::AddAssign;
use std::{cell::RefCell, rc::Rc, time::Duration};
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
            avg: 0,
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
        self.avg -= old as u64;
        self.avg += new as u64;
        self.ptr = (self.ptr + 1) % size;
        if self.ptr == 0 {
            self.rollover = true;
        }
    }

    #[inline]
    pub fn avg(&self) -> u32 {
        if self.rollover {
            (self.avg / SIZE as u64) as u32
        } else {
            if self.ptr == 0 {
                0
            } else {
                (self.avg / self.ptr as u64) as u32
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

/// Metrics accumulator. Designed to be updated without a lock.
#[derive(Debug, Default)]
pub struct MetricsAccum {
    counts: RefCell<[usize; MetricVariant::COUNT]>,
    max: RefCell<[usize; MetricVariant::COUNT]>,
    times: RefCell<[RollingAverageU32<32>; MetricVariant::COUNT]>,

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
        let mut avg_time: [u32; MetricVariant::COUNT] = Default::default();
        let lock = self.times.borrow();
        for i in 0..MetricVariant::COUNT {
            avg_time[i] = lock[i].avg();
        }
        ConnMetrics {
            value: *self.counts.borrow(),
            max: *self.max.borrow(),
            avg_time,
        }
    }

    pub fn insert(&self, to: MetricVariant) {
        let mut lock = self.counts.borrow_mut();
        lock[to as usize] += 1;
        let mut max = self.max.borrow_mut();
        max[to as usize] = max[to as usize].max(lock[to as usize]);
        // trace!("None->{to:?} ({})", lock[to as usize]);
        if let Some(parent) = &self.parent {
            parent.insert(to);
        }
    }

    pub fn transition(&self, from: MetricVariant, to: MetricVariant, time: Duration) {
        // trace!("{from:?}->{to:?}: {time:?}");
        let mut lock = self.counts.borrow_mut();
        lock[from as usize] -= 1;
        self.times.borrow_mut()[from as usize].accum(time.as_millis() as _);
        lock[to as usize] += 1;
        let mut max = self.max.borrow_mut();
        max[to as usize] = max[to as usize].max(lock[to as usize]);
        if let Some(parent) = &self.parent {
            parent.transition(from, to, time);
        }
    }

    pub fn remove_time(&self, from: MetricVariant, time: Duration) {
        let mut lock = self.counts.borrow_mut();
        lock[from as usize] -= 1;
        self.times.borrow_mut()[from as usize].accum(time.as_millis() as _);
        // trace!("{from:?}->None ({time:?})");
        if let Some(parent) = &self.parent {
            parent.remove_time(from, time);
        }
    }

    pub fn remove(&self, from: MetricVariant) {
        let mut lock = self.counts.borrow_mut();
        lock[from as usize] -= 1;
        // trace!("{from:?}->None");
        if let Some(parent) = &self.parent {
            parent.remove(from);
        }
    }
}
