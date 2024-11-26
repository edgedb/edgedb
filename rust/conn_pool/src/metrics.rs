use serde::{Serialize, Serializer};
use std::collections::BTreeMap;
use std::{cell::RefCell, rc::Rc, time::Duration};
use strum::EnumCount;
use strum::IntoEnumIterator;

use crate::algo::PoolAlgorithmDataMetrics;
use crate::block::Name;

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, strum::EnumCount, strum::EnumIter, strum::AsRefStr,
)]
pub enum MetricVariant {
    Connecting,
    Disconnecting,
    Reconnecting,
    Idle,
    Active,
    Failed,
    Closed,
    Waiting,
}

/// Maintains a rolling average of `u32` values. Note that this struct attempts
/// to optimize `SIZE == 1`.
#[derive(Debug, PartialEq, Eq)]
pub struct RollingAverageU32<const SIZE: usize> {
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
    pub fn accum(&mut self, new: u32) {
        if SIZE == 1 {
            self.values[0] = new;
        } else {
            let size = SIZE as u8;
            let old = std::mem::replace(&mut self.values[self.ptr as usize], new);
            self.cumulative -= old as u64;
            self.cumulative += new as u64;
            self.ptr = (self.ptr + 1) % size;
            if self.ptr == 0 {
                self.rollover = true;
            }
        }
    }

    #[inline]
    pub fn avg(&self) -> u32 {
        if SIZE == 1 {
            self.values[0]
        } else if self.rollover {
            (self.cumulative / SIZE as u64) as u32
        } else if self.ptr == 0 {
            0
        } else {
            (self.cumulative / self.ptr as u64) as u32
        }
    }
}

#[derive(Debug, Default, Serialize)]
pub struct PoolMetrics {
    pub pool: ConnMetrics,
    pub all_time: VariantArray<usize>,
    pub blocks: BTreeMap<Name, ConnMetrics>,
}

/// An array indexed by [`MetricVariant`].
#[derive(Default, Clone, Copy, PartialEq, Eq)]
pub struct VariantArray<T>([T; MetricVariant::COUNT]);

impl<T> Serialize for VariantArray<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<T> std::ops::Index<MetricVariant> for VariantArray<T> {
    type Output = T;
    fn index(&self, index: MetricVariant) -> &Self::Output {
        &self.0[index as usize]
    }
}

impl<T> std::ops::IndexMut<MetricVariant> for VariantArray<T> {
    fn index_mut(&mut self, index: MetricVariant) -> &mut Self::Output {
        &mut self.0[index as usize]
    }
}

impl<T: Copy + std::ops::AddAssign> std::ops::Add for VariantArray<T> {
    type Output = VariantArray<T>;
    fn add(self, rhs: Self) -> Self::Output {
        let mut out = self;
        for i in MetricVariant::iter() {
            out[i] += rhs[i];
        }
        out
    }
}

impl<T: Copy + std::ops::AddAssign> std::ops::AddAssign for VariantArray<T> {
    fn add_assign(&mut self, rhs: Self) {
        for i in MetricVariant::iter() {
            self[i] += rhs[i];
        }
    }
}

impl<T: Default + Copy + std::ops::AddAssign> std::iter::Sum for VariantArray<T> {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        let mut sum = Default::default();
        for i in iter {
            sum += i;
        }
        sum
    }
}

impl<T: std::fmt::Debug + std::cmp::Eq + Default> std::fmt::Debug for VariantArray<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("");
        for variant in MetricVariant::iter() {
            if self[variant] != T::default() {
                d.field(variant.as_ref(), &self[variant]);
            }
        }
        d.finish()
    }
}

impl<T: Copy + Default> VariantArray<T> {
    #[cfg(test)]
    pub fn with(variant: MetricVariant, count: T) -> Self {
        let mut summary = Self::default();
        summary[variant] = count;
        summary
    }
}

#[derive(Default, Serialize)]
#[allow(unused)]
pub struct ConnMetrics {
    pub(crate) value: VariantArray<usize>,
    pub(crate) all_time: VariantArray<usize>,
    pub(crate) max: VariantArray<usize>,
    pub(crate) avg_time: VariantArray<u32>,
    pub(crate) total: usize,
    pub(crate) total_max: usize,
    pub(crate) target: usize,
}

impl std::fmt::Debug for ConnMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ConnMetrics {\n")?;
        for variant in MetricVariant::iter() {
            f.write_fmt(format_args!(
                "    {variant:?}: {} (max={}, avg={}ms)\n",
                self.value[variant], self.max[variant], self.avg_time[variant]
            ))?;
        }
        f.write_str("}")?;
        Ok(())
    }
}

#[derive(Debug, Default)]
struct RawMetrics {
    /// The total number of non-waiting connections.
    total: usize,
    /// The max total number of non-waiting connections.
    total_max: usize,
    /// The number of connections per state.
    counts: VariantArray<usize>,
    /// The total number of transitions into this state for all time.
    all_time: VariantArray<usize>,
    /// The max number of connections per state.
    max: VariantArray<usize>,
    /// The time spent in each state.
    times: VariantArray<RollingAverageU32<32>>,
}

impl RawMetrics {
    #[inline(always)]
    fn reset_max(&mut self) {
        self.max = self.counts;
        self.total_max = self.total;
    }

    #[inline(always)]
    fn inc_all_time(&mut self, to: MetricVariant) {
        self.all_time[to] += 1;
    }

    #[inline(always)]
    fn inc(&mut self, to: MetricVariant) {
        self.counts[to] += 1;
        self.max[to] = self.max[to].max(self.counts[to]);
        self.inc_all_time(to)
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
        self.counts[from] -= 1;
    }

    #[inline(always)]
    fn time(&mut self, from: MetricVariant, time: Duration) {
        self.times[from].accum(time.as_millis() as _);
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

impl PoolAlgorithmDataMetrics for MetricsAccum {
    #[inline(always)]
    fn avg_ms(&self, variant: MetricVariant) -> usize {
        self.raw.borrow().times[variant].avg() as _
    }
    #[inline(always)]
    fn count(&self, variant: MetricVariant) -> usize {
        self.raw.borrow().counts[variant]
    }
    #[inline(always)]
    fn max(&self, variant: MetricVariant) -> usize {
        self.raw.borrow().max[variant]
    }
    #[inline(always)]
    fn total(&self) -> usize {
        self.raw.borrow().total
    }
    #[inline(always)]
    fn total_max(&self) -> usize {
        self.raw.borrow().total_max
    }
}

impl MetricsAccum {
    pub fn new(parent: Option<Rc<MetricsAccum>>) -> Self {
        Self {
            parent,
            ..Default::default()
        }
    }

    /// Get the current total
    #[inline(always)]
    pub fn total(&self) -> usize {
        self.raw.borrow().total
    }

    /// Get the current value of a variant
    #[inline(always)]
    pub fn get(&self, variant: MetricVariant) -> usize {
        self.raw.borrow().counts[variant]
    }

    /// Sums the values of all the given variants.
    #[inline(always)]
    pub fn sum_all(&self, variants: &[MetricVariant]) -> usize {
        let mut sum = 0;
        let lock = self.raw.borrow();
        for variant in variants {
            sum += lock.counts[*variant];
        }
        sum
    }

    /// Returns true if there is a non-zero count for any of the variants.
    #[inline(always)]
    pub fn has_any(&self, variants: &[MetricVariant]) -> bool {
        let lock = self.raw.borrow();
        for variant in variants {
            if lock.counts[*variant] > 0 {
                return true;
            }
        }
        false
    }

    #[inline(always)]
    pub fn reset_max(&self) {
        self.raw.borrow_mut().reset_max();
    }

    pub fn summary(&self) -> ConnMetrics {
        let lock = self.raw.borrow_mut();
        let mut avg_time = VariantArray::default();
        for i in MetricVariant::iter() {
            avg_time[i] = lock.times[i].avg();
        }
        ConnMetrics {
            value: lock.counts,
            all_time: lock.all_time,
            max: lock.max,
            avg_time,
            total: lock.total,
            total_max: lock.total_max,
            target: 0,
        }
    }

    pub fn counts(&self) -> VariantArray<usize> {
        self.raw.borrow().counts
    }

    pub fn all_time(&self) -> VariantArray<usize> {
        self.raw.borrow().all_time
    }

    #[inline]
    pub fn inc_all_time(&self, to: MetricVariant) {
        let mut lock = self.raw.borrow_mut();
        lock.inc_all_time(to);
        if let Some(parent) = &self.parent {
            parent.inc_all_time(to);
        }
    }

    #[inline]
    pub fn insert(&self, to: MetricVariant) {
        let mut lock = self.raw.borrow_mut();
        lock.inc(to);
        lock.inc_total(to);
        // trace!("None->{to:?} ({})", lock[to ]);
        if let Some(parent) = &self.parent {
            parent.insert(to);
        }
    }

    #[inline]
    pub fn set_value(&self, to: MetricVariant, len: usize) {
        let mut lock = self.raw.borrow_mut();
        debug_assert_eq!(lock.counts[to], 0);
        lock.counts[to] = len;
        lock.total += len;
        lock.max[to] = lock.max[to].max(lock.counts[to]);
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
