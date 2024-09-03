use std::{
    cell::{Cell, RefCell},
    time::Duration,
};
use tracing::trace;

use crate::{
    block::Name,
    drain::Drain,
    metrics::{MetricVariant, RollingAverageU32},
};

/// The historical length of data we'll maintain for demand.
const DEMAND_HISTORY_LENGTH: usize = 16;

#[cfg(not(feature = "optimizer"))]
#[derive(Clone, Copy, derive_more::From)]
pub struct Knob<T: Copy>(&'static str, T);

#[cfg(not(feature = "optimizer"))]
impl<T: Copy> Knob<T> {
    pub const fn new(name: &'static str, value: T) -> Self {
        Self(name, value)
    }

    pub fn get(&self) -> T {
        self.1
    }
}

#[cfg(feature = "optimizer")]
pub struct Knob<T: Copy + 'static>(
    &'static str,
    &'static std::thread::LocalKey<std::cell::RefCell<T>>,
    Option<std::ops::RangeInclusive<T>>,
);

impl<T: Copy + PartialOrd<T> + std::fmt::Display + std::fmt::Debug> std::fmt::Debug for Knob<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}={:?}", self.0, self.get()))
    }
}

#[cfg(feature = "optimizer")]
impl<T: Copy + PartialOrd<T> + std::fmt::Display + std::fmt::Debug> Knob<T> {
    pub const fn new(
        name: &'static str,
        value: &'static std::thread::LocalKey<std::cell::RefCell<T>>,
        bounds: &[std::ops::RangeInclusive<T>],
    ) -> Self {
        let copy = if !bounds.is_empty() {
            Some(*bounds[0].start()..=*bounds[0].end())
        } else {
            None
        };
        Self(name, value, copy)
    }

    pub fn name(&self) -> &'static str {
        self.0
    }

    pub fn get(&self) -> T {
        self.1.with_borrow(|t| *t)
    }

    pub fn set(&self, value: T) -> Result<(), String> {
        if let Some(range) = &self.2 {
            if range.contains(&value) {
                self.1.with_borrow_mut(|t| *t = value);
                Ok(())
            } else {
                Err(format!("{value} is out of range of {range:?}"))
            }
        } else {
            self.1.with_borrow_mut(|t| *t = value);
            Ok(())
        }
    }

    pub fn clamp(&self, value: &mut T) {
        if let Some(range) = &self.2 {
            if !range.contains(value) {
                if *value < *range.start() {
                    *value = *range.start()
                } else {
                    *value = *range.end()
                }
            }
        }
    }
}

#[allow(unused)]
macro_rules! range {
    () => {
        &[]
    };
    (($n1:literal..)) => {
        &[$n1..=isize::MAX]
    };
    (($n1:literal..=$n2:literal)) => {
        &[$n1..=$n2]
    };
}

macro_rules! constants {
    ($(
        $( #[doc=$doc:literal] )*
        $( #[range $range:tt] )?
        const $name:ident: $type:ty = $value:literal;
    )*) => {
        #[cfg(feature="optimizer")]
        pub mod knobs {
            pub use super::Knob;
            mod locals {
                $(
                    thread_local! {
                        pub static $name: std::cell::RefCell<$type> = std::cell::RefCell::new($value);
                    }
                )*
            }

            $(
                $( #[doc=$doc] )*
                pub static $name: Knob<$type> = Knob::new(stringify!($name), &locals::$name, range!($($range)?));
            )*

            pub const ALL_KNOB_COUNT: usize = [$(stringify!($name)),*].len();
            pub static ALL_KNOBS: [&Knob<isize>; ALL_KNOB_COUNT] = [
                $(&$name),*
            ];
        }
        #[cfg(not(feature="optimizer"))]
        pub mod knobs {
            pub use super::Knob;
            $(
                $( #[doc=$doc] )*
                pub const $name: Knob<$type> = Knob::new(stringify!($name), $value);
            )*
        }
        pub use knobs::*;
    };
}

// Note: these constants are tuned via the generic algorithm optimizer.
constants! {
    /// The maximum number of connections to create or destroy during a rebalance.
    #[range(1..=10)]
    const MAX_REBALANCE_OPS: isize = 5;
    /// The maximum % of total connections to create or destroy during a
    /// rebalance when full.
    #[range(1..=10)]
    const MAX_REBALANCE_OPS_PERCENT_WHEN_FULL: isize = 5;
    /// The minimum headroom in a block between its current total and its target
    /// for us to pre-create connections for it.
    #[range(0..=10)]
    const MIN_REBALANCE_HEADROOM_TO_CREATE: isize = 0;

    /// The minimum amount of time we'll consider for an active connection.
    #[range(1..=100)]
    const MIN_TIME: isize = 1;

    /// The weight we apply to waiting connections.
    #[range(0..)]
    const DEMAND_WEIGHT_WAITING: isize = 61;
    /// The weight we apply to active connections.
    #[range(0..)]
    const DEMAND_WEIGHT_ACTIVE: isize = 31;
    /// The minimum non-zero demand. This makes the demand calculations less noisy
    /// when we are competing at lower levels of demand, allowing for more
    /// reproducable results.
    #[range(1..=256)]
    const DEMAND_MINIMUM: isize = 1;

    /// The maximum-minimum connection count we'll allocate to connections if there
    /// is more capacity than backends.
    #[range(1..)]
    const MAXIMUM_SHARED_TARGET: isize = 1;

    /// The boost we apply to our own apparent hunger when releasing a connection.
    /// This prevents excessive swapping when hunger is similar across various
    /// backends.
    #[range(0..)]
    const SELF_HUNGER_BOOST_FOR_RELEASE: isize = 46;
    /// The weight we apply to the difference between the target and required
    /// connections when determining overfullness.
    const HUNGER_DIFF_WEIGHT: isize = -3;
    /// The weight we apply to waiters when determining hunger.
    const HUNGER_WAITER_WEIGHT: isize = 2;
    const HUNGER_WAITER_ACTIVE_WEIGHT: isize = 0;
    const HUNGER_ACTIVE_WEIGHT_DIVIDEND_ADD: isize = -611;
    const HUNGER_ACTIVE_WEIGHT_DIVIDEND_SUB: isize = 0;
    /// The weight we apply to the oldest waiter's age in milliseconds (as a divisor).
    const HUNGER_AGE_DIVISOR_WEIGHT: isize = -35;
    /// Penalize switching to a backend which has changed recently.
    const HUNGER_CHANGE_WEIGHT_DIVIDEND: isize = -39;

    /// The weight we apply to the difference between the target and required
    /// connections when determining overfullness.
    const OVERFULL_DIFF_WEIGHT: isize = -3;
    /// The weight we apply to idle connections when determining overfullness.
    const OVERFULL_IDLE_WEIGHT: isize = 423;
    /// This is divided by the youngest connection metric to penalize switching from
    /// a backend which has changed recently.
    const OVERFULL_CHANGE_WEIGHT_DIVIDEND: isize = -59;
    /// The weight we apply to waiters when determining overfullness.
    const OVERFULL_WAITER_WEIGHT: isize = 194;
    const OVERFULL_WAITER_ACTIVE_WEIGHT: isize = -98;

    const OVERFULL_ACTIVE_WEIGHT_DIVIDEND_ADD: isize = -696;
    const OVERFULL_ACTIVE_WEIGHT_DIVIDEND_SUB: isize = 60;
}

/// Determines the rebalance plan based on the current pool state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebalanceOp {
    /// Transfer from one block to another
    Transfer { to: Name, from: Name },
    /// Create a block
    Create(Name),
    /// Garbage collect a block.
    Close(Name),
}

/// Determines the acquire plan based on the current pool state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcquireOp {
    /// Create a new connection.
    Create,
    /// Steal a connection from another block.
    Steal(Name),
    /// Wait for a connection.
    Wait,
    /// A connection cannot be established due to shutdown.
    FailInShutdown,
}

/// Determines the release plan based on the current pool state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReleaseOp {
    /// Release this connection back to the same database.
    Release,
    /// Reopen this connection.
    Reopen,
    /// Discard this connection.
    Discard,
    /// Release this connection to a different database.
    ReleaseTo(Name),
}

/// The type of release to perform.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub enum ReleaseType {
    /// A normal release
    #[default]
    Normal,
    /// A release of a poisoned connection.
    Poison,
}

#[derive(Default, Clone, Copy)]
#[repr(transparent)]
struct Score(isize);

#[allow(clippy::comparison_chain)]
impl Score {
    #[inline(always)]
    pub fn add(&mut self, knob: &Knob<isize>, value: impl TryInto<isize>) {
        let knob = knob.get();
        let value = value.try_into().unwrap_or_default();
        if knob < 0 {
            self.0 += value / -knob
        } else if knob > 0 {
            self.0 += value * knob
        }
    }

    #[inline(always)]
    pub fn add_fraction(
        &mut self,
        knob: &Knob<isize>,
        numerator: impl TryInto<isize>,
        denominator: impl TryInto<isize>,
    ) {
        let knob = knob.get();
        let numerator = numerator.try_into().unwrap_or_default();
        let denominator = denominator.try_into().unwrap_or_default();
        if knob < 0 {
            self.0 += numerator / (denominator * -knob)
        } else if knob > 0 {
            self.0 += numerator * knob / denominator
        }
    }

    #[inline(always)]
    pub fn sub(&mut self, knob: &Knob<isize>, value: impl TryInto<isize>) {
        let knob = knob.get();
        if knob < 0 {
            self.0 -= value.try_into().unwrap_or_default() / -knob
        } else if knob > 0 {
            self.0 -= value.try_into().unwrap_or_default() * knob
        }
    }

    #[inline(always)]
    pub fn sub_fraction(
        &mut self,
        knob: &Knob<isize>,
        numerator: impl TryInto<isize>,
        denominator: impl TryInto<isize>,
    ) {
        let knob = knob.get();
        let numerator = numerator.try_into().unwrap_or_default();
        let denominator = denominator.try_into().unwrap_or_default();
        if knob < 0 {
            self.0 -= numerator / (denominator * -knob)
        } else if knob > 0 {
            self.0 -= numerator * knob / denominator
        }
    }

    #[inline(always)]
    pub fn into(self) -> isize {
        self.0
    }
}

/// Generic trait to decouple the algorithm from the underlying pool blocks.
/// This minimizes the interface between the algorithm and the blocks to keep
/// coupling between the two at the right level.
pub trait VisitPoolAlgoData: PoolAlgorithmDataPool {
    type Block: PoolAlgorithmDataBlock;

    /// Ensure that the given block is available, inserting it with the default
    /// demand if necessary.
    fn ensure_block(&self, db: &str, default_demand: usize) -> bool;
    /// Iterates all the blocks, garbage collecting any idle blocks with no demand.
    fn with_all(&self, f: impl FnMut(&Name, &Self::Block));
    /// Retreives a single block, returning `None` if the block doesn't exist.
    fn with<T>(&self, db: &str, f: impl Fn(&Self::Block) -> T) -> Option<T>;

    #[inline]
    fn target(&self, db: &str) -> usize {
        self.with(db, |data| data.target()).unwrap_or_default()
    }
}

pub trait PoolAlgorithmDataMetrics {
    fn total(&self) -> usize;
    fn count(&self, variant: MetricVariant) -> usize;
    fn total_max(&self) -> usize;
    fn max(&self, variant: MetricVariant) -> usize;
    fn avg_ms(&self, variant: MetricVariant) -> usize;
}

pub trait PoolAlgorithmDataBlock: PoolAlgorithmDataMetrics {
    fn target(&self) -> usize;
    fn set_target(&self, target: usize);
    fn insert_demand(&self, demand: u32);
    fn demand(&self) -> u32;

    fn count_older(&self, variant: MetricVariant, age: Duration) -> usize;
    fn oldest_ms(&self, variant: MetricVariant) -> usize;
    fn youngest_ms(&self) -> usize;

    /// Calculates the hunger score for the current state.
    ///
    /// The score is determined based on the difference between the target and current metrics,
    /// and the number of waiting elements. It uses weights for each component to compute the final score.
    /// If the current state exceeds the target, the function returns `None`.
    ///
    /// # Parameters
    ///
    /// - `will_release`: A boolean indicating whether an element will be released.
    ///
    /// # Returns
    ///
    /// Returns an `Option<NonZeroUsize>` containing the hunger score if the current state is below the target
    /// and there are waiting elements; otherwise, returns `None`.
    fn hunger_score(&self, will_release: bool) -> Option<isize> {
        let waiting = self.count(MetricVariant::Waiting);
        let connecting =
            self.count(MetricVariant::Connecting) + self.count(MetricVariant::Reconnecting);
        let waiters = waiting.saturating_sub(connecting);
        let current = self.total() - if will_release { 1 } else { 0 };
        let target = self.target();

        // If we have more connections than our target, we are not hungry. We
        // may still be hungry if current <= target if we have waiters, however.
        if current > target || (target == current && waiters < 1) {
            return None;
        }

        let active_ms = self.avg_ms(MetricVariant::Active).max(MIN_TIME.get() as _);
        let reconnecting_ms = self
            .avg_ms(MetricVariant::Reconnecting)
            .max(self.avg_ms(MetricVariant::Connecting) + self.avg_ms(MetricVariant::Disconnecting))
            .max(MIN_TIME.get() as _);
        let youngest_ms = self.youngest_ms();

        let mut score = Score::default();

        // Waiters become more hungry as they age
        score.add(
            &HUNGER_AGE_DIVISOR_WEIGHT,
            self.oldest_ms(MetricVariant::Waiting),
        );
        // The number of waiters increases hunger
        score.add(&HUNGER_WAITER_WEIGHT, waiters);
        // The expected number of waiters we could handle within a reconnection period increases hunger
        score.add_fraction(
            &HUNGER_WAITER_ACTIVE_WEIGHT,
            waiters * active_ms,
            reconnecting_ms,
        );
        // The amount of connections we owe this block increases hunger
        score.add(&HUNGER_DIFF_WEIGHT, target - current);

        // Allow for some non-linearity in scoring w/active time
        score.add_fraction(
            &HUNGER_ACTIVE_WEIGHT_DIVIDEND_ADD,
            active_ms,
            reconnecting_ms,
        );
        score.sub_fraction(
            &HUNGER_ACTIVE_WEIGHT_DIVIDEND_SUB,
            active_ms,
            reconnecting_ms,
        );

        // We give an hunger "negative" penalty to blocks that have newly
        // acquired a connection.
        score.sub_fraction(&HUNGER_CHANGE_WEIGHT_DIVIDEND, youngest_ms, reconnecting_ms);

        Some(score.into())
    }

    /// Calculates the overfull score for the current state.
    ///
    /// The score is determined based on the difference between the current and target metrics,
    /// the idle count, and the age of the youngest element. It uses weights for each component to
    /// compute the final score. If the current state is not overfull or there are no idle elements,
    /// the function returns `None`.
    ///
    /// # Parameters
    ///
    /// - `will_release`: A boolean indicating whether an element will be released.
    ///
    /// # Returns
    ///
    /// Returns an `Option<NonZeroUsize>` containing the overfull score if the current state is overfull
    /// and there are idle elements; otherwise, returns `None`.
    fn overfull_score(&self, will_release: bool) -> Option<isize> {
        let idle = self.count(MetricVariant::Idle) + if will_release { 1 } else { 0 };
        let current = self.total();
        let target = self.target();

        // If we have no idle connections, or we don't have enough connections we're not overfull.
        if target >= current || idle == 0 {
            return None;
        }

        let connecting =
            self.count(MetricVariant::Connecting) + self.count(MetricVariant::Reconnecting);
        let waiting = self.count(MetricVariant::Waiting);
        let waiters = waiting.saturating_sub(connecting);

        let active_ms = self.avg_ms(MetricVariant::Active).max(MIN_TIME.get() as _);
        let reconnecting_ms = self
            .avg_ms(MetricVariant::Reconnecting)
            .max(self.avg_ms(MetricVariant::Connecting) + self.avg_ms(MetricVariant::Disconnecting))
            .max(MIN_TIME.get() as _);
        let youngest_ms = self.youngest_ms();

        let mut score = Score::default();

        // The more idle connections we have, the more overfull this block is.
        score.add(&OVERFULL_IDLE_WEIGHT, idle);
        // If we've got more connections than we were allocated, we're overfull.
        score.add(&OVERFULL_DIFF_WEIGHT, current - target);

        // We take the ratio of youngest/connecting ato give an overfullness
        // "negative" penalty to blocks that have newly acquired a connection.
        score.sub_fraction(
            &OVERFULL_CHANGE_WEIGHT_DIVIDEND,
            youngest_ms,
            reconnecting_ms,
        );

        // The number of waiters and the amount of time we expect to spend
        // active on these waiters also acts as a "negative" penalty.
        score.sub(&OVERFULL_WAITER_WEIGHT, waiters);
        score.sub_fraction(
            &OVERFULL_WAITER_ACTIVE_WEIGHT,
            waiters * active_ms,
            reconnecting_ms,
        );

        // Allow for some non-linearity in scoring w/active time
        score.add_fraction(
            &OVERFULL_ACTIVE_WEIGHT_DIVIDEND_ADD,
            active_ms,
            reconnecting_ms,
        );
        score.sub_fraction(
            &OVERFULL_ACTIVE_WEIGHT_DIVIDEND_SUB,
            active_ms,
            reconnecting_ms,
        );

        Some(score.into())
    }

    /// We calculate demand based on the estimated connection active time
    /// multiplied by the active + waiting counts. This gives us an
    /// estimated database time statistic we can use for relative
    /// weighting.
    fn demand_score(&self) -> usize {
        // This gives us an approximate count of incoming connection load during the last period
        let active = self.max(MetricVariant::Active);
        let active_ms = self.avg_ms(MetricVariant::Active).max(MIN_TIME.get() as _);
        let waiting = self.max(MetricVariant::Waiting);
        let idle = active == 0 && waiting == 0;

        if idle {
            0
        } else {
            let mut score = Score::default();
            score.add(&DEMAND_WEIGHT_WAITING, waiting * active_ms);
            score.add(&DEMAND_WEIGHT_ACTIVE, active * active_ms);
            // Note that we clamp to DEMAND_MINIMUM to ensure the average is non-zero
            score
                .into()
                .max(DEMAND_MINIMUM.get() * DEMAND_HISTORY_LENGTH as isize) as _
        }
    }
}

pub trait PoolAlgorithmDataPool: PoolAlgorithmDataMetrics {
    fn reset_max(&self);
}

#[derive(Default, Debug)]
pub struct PoolAlgoTargetData {
    /// A numeric score representing hunger or overfullness.
    target_size: Cell<usize>,
    avg_demand: RefCell<RollingAverageU32<DEMAND_HISTORY_LENGTH>>,
}

impl PoolAlgoTargetData {
    pub fn set_target(&self, target: usize) {
        self.target_size.set(target);
    }
    pub fn target(&self) -> usize {
        self.target_size.get()
    }
    pub fn insert_demand(&self, demand: u32) {
        self.avg_demand.borrow_mut().accum(demand)
    }
    pub fn demand(&self) -> u32 {
        self.avg_demand.borrow().avg()
    }
}

/// The pool algorithm constraints.
#[derive(Debug)]
pub struct PoolConstraints {
    /// Maximum pool size.
    pub max: usize,
    /// The minimum idle time before a connection can be GC'd.
    pub min_idle_time_for_gc: Duration,
}

/// The algorithm runs against these data structures.
pub struct AlgoState<'a, V: VisitPoolAlgoData> {
    pub drain: &'a Drain,
    pub blocks: &'a V,
    pub constraints: &'a PoolConstraints,
}

impl<'a, V: VisitPoolAlgoData> AlgoState<'a, V> {
    /// Recalculate the quota targets for each block within the pool/
    fn recalculate_shares(&self, update_demand: bool) {
        // First, compute the overall request load and number of backend targets
        let mut total_demand = 0;
        let mut total_target = 0;
        let mut s = "".to_owned();

        self.blocks.with_all(|name, data| {
            // Draining targets act like they have demand zero
            if self.drain.is_draining(name) {
                if tracing::enabled!(tracing::Level::TRACE) {
                    s += &format!("{name}=drain ");
                }
                return;
            }

            if update_demand {
                let demand = data.demand_score();
                data.insert_demand(demand as _);
            }
            let demand = data.demand();

            if tracing::enabled!(tracing::Level::TRACE) {
                s += &format!("{name}={demand} ");
            }

            total_demand += demand as usize;
            if demand > 0 {
                total_target += 1;
            } else {
                data.set_target(0);
            }
        });

        if tracing::enabled!(tracing::Level::TRACE) {
            trace!("Demand: {total_target} {}", s);
        }

        self.allocate_demand(total_target, total_demand);
    }

    /// Adjust the quota targets for each block within the pool.
    pub fn adjust(&self) {
        self.recalculate_shares(true);

        // Once we've adjusted the constraints, reset the max settings
        self.blocks.reset_max();
    }

    /// Allocate the calculated demand to target quotas.
    fn allocate_demand(&self, total_target: usize, total_demand: usize) {
        // Empty pool, no math
        if total_target == 0 || total_demand == 0 {
            self.blocks.with_all(|_name, data| {
                data.set_target(0);
            });
            return;
        }

        let mut allocated = 0;
        let max = self.constraints.max;
        // This is the minimum number of connections we'll allocate to any particular
        // backend regardless of demand if there are less backends than the capacity.
        let min = (max / total_target).min(MAXIMUM_SHARED_TARGET.get() as usize);
        // The remaining capacity after we allocated the `min` value above.
        let capacity = max - min * total_target;

        if min == 0 {
            self.blocks.with_all(|_name, data| {
                data.set_target(0);
            });
        } else {
            self.blocks.with_all(|name, data| {
                let demand = data.demand();
                if demand == 0 || self.drain.is_draining(name) {
                    data.set_target(0);
                    return;
                }

                // Give everyone what they requested, plus a share of the spare
                // capacity. If there is leftover spare capacity, that capacity
                // may float between whoever needs it the most.
                let target =
                    (demand as f32 * capacity as f32 / total_demand as f32).floor() as usize + min;

                data.set_target(target);
                allocated += target;
            });
        }

        if tracing::enabled!(tracing::Level::TRACE) {
            let mut s = String::new();
            self.blocks.with_all(|name, block| {
                s += &format!("{name}={}/{} ", block.target(), block.total());
            });
            trace!("Targets: {s}");
        }

        debug_assert!(
            allocated <= max,
            "Attempted to allocate more than we were allowed: {allocated} > {} \
                (req={total_demand}, target={total_target})",
            max
        );
    }

    /// Plan a shutdown.
    fn plan_shutdown(&self) -> Vec<RebalanceOp> {
        let mut ops = vec![];
        self.blocks.with_all(|name, block| {
            let idle = block.count(MetricVariant::Idle);
            let failed = block.count(MetricVariant::Failed);

            for _ in 0..(idle + failed) {
                ops.push(RebalanceOp::Close(name.clone()));
            }
        });
        ops
    }

    /// Plan a rebalance to better match the target quotas of the blocks in the
    /// pool.
    pub fn plan_rebalance(&self, garbage_collect: bool) -> Vec<RebalanceOp> {
        // In shutdown, we just want to close all idle connections where possible.
        if self.drain.in_shutdown() {
            return self.plan_shutdown();
        }

        let mut current_pool_size = self.blocks.total();
        let max_pool_size = self.constraints.max;
        let mut tasks = vec![];

        // TODO: These could potentially be transferred instead, but
        // drain/shutdown are pretty unlikely.
        if self.drain.are_any_draining() {
            self.blocks.with_all(|name, data| {
                if self.drain.is_draining(name) {
                    for _ in 0..data.count(MetricVariant::Idle) + data.count(MetricVariant::Failed)
                    {
                        tasks.push(RebalanceOp::Close(name.clone()))
                    }
                }
            })
        }

        // If we're garbage collecting, we want to close any idle connections
        // older than `min_idle_time_for_gc`. If we don't have any, just run a
        // normal rebalance.
        if garbage_collect {
            self.blocks.with_all(|name, block| {
                // Drains are handled at the start of this function
                if self.drain.is_draining(name) {
                    return;
                }

                let gc_able =
                    block.count_older(MetricVariant::Idle, self.constraints.min_idle_time_for_gc);
                if gc_able > 0 {
                    let idle = block.count(MetricVariant::Idle);
                    trace!("GC for {name}: {gc_able}/{idle} connection(s) to close");
                }
                for _ in 0..gc_able {
                    tasks.push(RebalanceOp::Close(name.clone()));
                }
            });
            if !tasks.is_empty() {
                return tasks;
            }
        }

        // If there's room in the pool, we can be more aggressive in
        // how we allocate.
        if current_pool_size < max_pool_size {
            let mut made_changes = false;
            for i in 0..MAX_REBALANCE_OPS.get() as usize {
                self.blocks.with_all(|name, block| {
                    // Drains are handled at the start of this function
                    if self.drain.is_draining(name) {
                        return;
                    }

                    // If there's room in the block, and room in the pool, and
                    // the block is bumping up against its current headroom, we'll grab
                    // another one.
                    if block.target() > block.total()
                        && current_pool_size < max_pool_size
                        && (block.max(MetricVariant::Active) + block.max(MetricVariant::Waiting))
                            > (block.total() + i)
                                .saturating_sub(MIN_REBALANCE_HEADROOM_TO_CREATE.get() as usize)
                    {
                        tasks.push(RebalanceOp::Create(name.clone()));
                        current_pool_size += 1;
                        made_changes = true;
                    }
                });
                if !made_changes {
                    break;
                }
            }

            return tasks;
        }

        // For any block with less connections than its quota that has
        // waiters, we want to transfer from the most overloaded block.
        let mut overloaded = vec![];
        let mut hungriest = vec![];
        let mut idle = vec![];

        let mut s1 = "".to_owned();
        let mut s2 = "".to_owned();

        self.blocks.with_all(|name, block| {
            // Drains are handled at the start of this function
            if self.drain.is_draining(name) {
                return;
            }

            if let Some(value) = block.hunger_score(false) {
                if tracing::enabled!(tracing::Level::TRACE) {
                    s1 += &format!("{name}={value} ");
                }
                hungriest.push((value, name.clone()))
            } else if let Some(value) = block.overfull_score(false) {
                if tracing::enabled!(tracing::Level::TRACE) {
                    s2 += &format!("{name}={value} ");
                }
                if block.demand() == 0 {
                    idle.push(name.clone());
                } else {
                    overloaded.push((value, name.clone()))
                }
            }
        });

        if tracing::enabled!(tracing::Level::TRACE) {
            trace!("Hunger: {s1}");
            trace!("Overfullness: {s2}");
        }
        overloaded.sort();
        hungriest.sort();

        let ops_count =
            ((MAX_REBALANCE_OPS_PERCENT_WHEN_FULL.get() as usize * max_pool_size) / 100).max(1);
        for _ in 0..ops_count {
            let Some((_, to)) = hungriest.pop() else {
                break;
            };

            // Prefer rebalancing from idle connections, otherwise take from
            // overloaded ones.
            if let Some(from) = idle.pop() {
                tasks.push(RebalanceOp::Transfer { to, from });
            } else if let Some((_, from)) = overloaded.pop() {
                tasks.push(RebalanceOp::Transfer { to, from });
            } else {
                break;
            }
        }

        tasks
    }

    /// Plan a connection acquisition.
    pub fn plan_acquire(&self, db: &str) -> AcquireOp {
        if self.drain.in_shutdown() {
            return AcquireOp::FailInShutdown;
        }

        // If the block is new, we need to perform an initial adjustment to
        // ensure this block gets some capacity.
        if self
            .blocks
            .ensure_block(db, DEMAND_MINIMUM.get() as usize * DEMAND_HISTORY_LENGTH)
        {
            self.recalculate_shares(false);
        }

        let target_block_size = self.blocks.target(db);
        let current_block_size = self
            .blocks
            .with(db, |data| data.total())
            .unwrap_or_default();
        let current_pool_size = self.blocks.total();
        let max_pool_size = self.constraints.max;

        let pool_is_full = current_pool_size >= max_pool_size;
        if !pool_is_full {
            trace!("Pool has room, acquiring new connection for {db}");
            return AcquireOp::Create;
        }

        let block_has_room = current_block_size < target_block_size || target_block_size == 0;
        trace!("Acquiring {db}: {current_pool_size}/{max_pool_size} {current_block_size}/{target_block_size}");
        if pool_is_full && block_has_room {
            let mut max = isize::MIN;
            let mut which = None;
            self.blocks.with_all(|name, block| {
                if let Some(overfullness) = block.overfull_score(false) {
                    if overfullness > max {
                        which = Some(name.clone());
                        max = overfullness;
                    }
                }
            });
            match which {
                Some(name) => AcquireOp::Steal(name),
                None => AcquireOp::Wait,
            }
        } else if block_has_room {
            AcquireOp::Create
        } else {
            AcquireOp::Wait
        }
    }

    /// Plan a connection release.
    pub fn plan_release(&self, db: &str, release_type: ReleaseType) -> ReleaseOp {
        if self.drain.is_draining(db) {
            return ReleaseOp::Discard;
        }

        if release_type == ReleaseType::Poison {
            return ReleaseOp::Reopen;
        }

        let current_pool_size = self.blocks.total();
        let max_pool_size = self.constraints.max;
        if current_pool_size < max_pool_size {
            trace!("Pool has room, keeping connection");
            return ReleaseOp::Release;
        }

        // We only want to consider a release elsewhere if this block is overfull
        if let Some(Some(overfull)) = self.blocks.with(db, |block| block.overfull_score(true)) {
            trace!("Block {db} is overfull ({overfull}), trying to release");
            let mut max = isize::MIN;
            let mut which = None;
            let mut s = "".to_owned();
            self.blocks.with_all(|name, block| {
                let is_self = &**name == db;
                if let Some(mut hunger) = block.hunger_score(is_self) {
                    // Penalize switching by boosting the current database's relative hunger here
                    if is_self {
                        hunger += SELF_HUNGER_BOOST_FOR_RELEASE.get();
                    }

                    if tracing::enabled!(tracing::Level::TRACE) {
                        s += &format!("{name}={hunger} ");
                    }

                    if hunger > max {
                        which = if is_self { None } else { Some(name.clone()) };
                        max = hunger;
                    }
                }
            });

            if tracing::enabled!(tracing::Level::TRACE) {
                trace!("Hunger: {s}");
            }

            match which {
                Some(name) => {
                    trace!("Releasing to {name:?} with score {max}");
                    ReleaseOp::ReleaseTo(name)
                }
                None => {
                    trace!("Keeping connection");
                    ReleaseOp::Release
                }
            }
        } else {
            trace!("Block {db} is not overfull, keeping");
            ReleaseOp::Release
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{block::Blocks, test::BasicConnector, PoolConfig};
    use anyhow::{Ok, Result};
    use futures::{stream::FuturesUnordered, StreamExt};
    use test_log::test;
    use tokio::task::LocalSet;

    #[test(tokio::test(flavor = "current_thread"))]
    async fn test_pool_normal() -> Result<()> {
        let future = async {
            let connector = BasicConnector::no_delay();
            let config = PoolConfig::suggested_default_for(10);
            let blocks = Blocks::default();
            let drain = Drain::default();
            let algo = AlgoState {
                constraints: &config.constraints,
                drain: &drain,
                blocks: &blocks,
            };

            let futures = FuturesUnordered::new();
            for i in (0..algo.constraints.max).map(Name::from) {
                assert_eq!(algo.plan_acquire(&i), AcquireOp::Create);
                futures.push(blocks.create_if_needed(&connector, &i));
            }
            let conns: Vec<_> = futures.collect().await;
            let futures = FuturesUnordered::new();
            for i in (0..algo.constraints.max).map(Name::from) {
                assert_eq!(algo.plan_acquire(&i), AcquireOp::Wait);
                futures.push(blocks.queue(&i));
            }
            for conn in conns {
                assert_eq!(
                    algo.plan_release(&conn?.state.db_name, ReleaseType::Normal),
                    ReleaseOp::Release
                );
            }
            let conns: Vec<_> = futures.collect().await;
            for conn in conns {
                assert_eq!(
                    algo.plan_release(&conn?.state.db_name, ReleaseType::Normal),
                    ReleaseOp::Release
                );
            }
            Ok(())
        };
        LocalSet::new().run_until(future).await
    }

    /// Ensures that when a pool is starved for connections because there are
    /// more backends than connections, we release connections to other to
    /// ensure fairness.
    #[test(tokio::test(flavor = "current_thread", start_paused = true))]
    async fn test_pool_starved() -> Result<()> {
        let future = async {
            let connector = BasicConnector::no_delay();
            let config = PoolConfig::suggested_default_for(10);
            let blocks = Blocks::default();
            let drain = Drain::default();
            let algo = AlgoState {
                constraints: &config.constraints,
                drain: &drain,
                blocks: &blocks,
            };

            // Room for these
            let futures = FuturesUnordered::new();
            for db in (0..5).map(Name::from) {
                assert_eq!(algo.plan_acquire(&db), AcquireOp::Create);
                futures.push(blocks.create_if_needed(&connector, &db));
            }
            // ... and these
            let futures2 = FuturesUnordered::new();
            for db in (5..10).map(Name::from) {
                assert_eq!(algo.plan_acquire(&db), AcquireOp::Create);
                futures2.push(blocks.create_if_needed(&connector, &db));
            }
            // But not these (yet)
            let futures3 = FuturesUnordered::new();
            for db in (10..15).map(Name::from) {
                assert_eq!(algo.plan_acquire(&db), AcquireOp::Wait);
                futures3.push(blocks.queue(&db));
            }
            let conns: Vec<_> = futures.collect().await;
            let conns2: Vec<_> = futures2.collect().await;
            // These are released to 10..15
            for conn in conns {
                let conn = conn?;
                let res = algo.plan_release(&conn.state.db_name, ReleaseType::Normal);
                let ReleaseOp::ReleaseTo(to) = res else {
                    panic!("Wrong release: {res:?}");
                };
                blocks.task_move_to(&connector, conn, &to).await?;
            }
            // These don't have anywhere to go
            for conn in conns2 {
                let conn = conn?;
                let res = algo.plan_release(&conn.state.db_name, ReleaseType::Normal);
                let ReleaseOp::Release = res else {
                    panic!("Wrong release: {res:?}");
                };
            }
            Ok(())
        };
        LocalSet::new().run_until(future).await
    }
}
