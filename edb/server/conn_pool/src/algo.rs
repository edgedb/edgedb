use scopeguard::defer;
use std::{
    cell::{Cell, RefCell},
    num::NonZeroUsize,
};
use tracing::trace;

use crate::{
    block::Name,
    metrics::{MetricVariant, RollingAverageU32},
};

/// The maximum number of connections to create during a rebalance.
const MAX_REBALANCE_CREATE: usize = 5;
/// The maximum number of excess connections (> target) we'll keep around during
/// a rebalance if there is still some demand.
const MAX_EXCESS_IDLE_CONNECTIONS: usize = 2;
/// The minimum amount of time we'll consider for an active connection.
const MIN_ACTIVE_TIME: usize = 1;

/// The weight we apply to waiting connections.
const DEMAND_WEIGHT_WAITING: usize = 1;
/// The weight we apply to active connections.
const DEMAND_WEIGHT_ACTIVE: usize = 1;
/// The historical length of data we'll maintain for demand.
const DEMAND_HISTORY_LENGTH: usize = 4;
/// The minimum non-zero demand. This makes the demand calculations less noisy
/// when we are competing at lower levels of demand, allowing for more
/// reproducable results.
const DEMAND_MINIMUM: usize = 16;

/// The maximum-minimum connection count we'll allocate to connections if there
/// is more capacity than backends.
const MAXIMUM_SHARED_TARGET: usize = 1;
/// The boost we apply to our own apparent hunger when releasing a connection.
/// This prevents excessive swapping when hunger is similar across various
/// backends.
const SELF_HUNGER_BOOST_FOR_RELEASE: usize = 10;

/// The weight we apply to the difference between the target and required
/// connections when determining overfullness.
const HUNGER_DIFF_WEIGHT: usize = 100;
/// The weight we apply to waiters when determining hunger.
const HUNGER_WAITER_WEIGHT: usize = 1;
/// The weight we apply to the oldest waiter's age in milliseconds (as a divisor).
const HUNGER_AGE_DIVISOR_WEIGHT: usize = 10;

/// The weight we apply to the difference between the target and required
/// connections when determining overfullness.
const OVERFULL_DIFF_WEIGHT: usize = 100;
/// The weight we apply to idle connections when determining overfullness.
const OVERFULL_IDLE_WEIGHT: usize = 1;
/// This is divided by the youngest connection metric to penalize switching from
/// a backend which has changed recently.
const OVERFULL_CHANGE_WEIGHT_DIVIDEND: usize = 1000;

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
    /// A release of a drained connection.
    Drain,
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
    fn hunger_score(&self, will_release: bool) -> Option<NonZeroUsize> {
        let waiting = self.count(MetricVariant::Waiting);
        let connecting = self.count(MetricVariant::Connecting);
        let waiters = waiting.saturating_sub(connecting);
        let current = self.total() - if will_release { 1 } else { 0 };
        let target = self.target();

        // Waiters become more hungry as they age
        let age_score = self.oldest_ms(MetricVariant::Waiting) / HUNGER_AGE_DIVISOR_WEIGHT;
        let base_score = age_score + waiters * HUNGER_WAITER_WEIGHT;

        // If we have more connections than our target, we are not hungry. We
        // may still be hungry if current <= target if we have waiters, however.
        if current > target {
            None
        } else if target > current {
            let diff = target - current;
            (base_score + diff * HUNGER_DIFF_WEIGHT).try_into().ok()
        } else if waiters > 0 {
            base_score.try_into().ok()
        } else {
            None
        }
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
    fn overfull_score(&self, will_release: bool) -> Option<NonZeroUsize> {
        let idle = self.count(MetricVariant::Idle) + if will_release { 1 } else { 0 };
        let current = self.total();
        let target = self.target();
        let youngest_ms = self.youngest_ms().max(1);
        // If we have no idle connections, or we don't have enough connections we're not overfull.
        if target >= current || idle == 0 {
            None
        } else {
            let base_score =
                idle * OVERFULL_IDLE_WEIGHT + OVERFULL_CHANGE_WEIGHT_DIVIDEND / youngest_ms;
            if current > target {
                let diff = current - target;
                (diff * OVERFULL_DIFF_WEIGHT + base_score).try_into().ok()
            } else {
                base_score.try_into().ok()
            }
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
}

impl PoolConstraints {
    /// Recalculate the quota targets for each block within the pool/
    pub fn recalculate_shares(&self, it: &impl VisitPoolAlgoData) {
        // First, compute the overall request load and number of backend targets
        let mut total_demand = 0;
        let mut total_target = 0;
        let mut s = "".to_owned();

        it.with_all(|name, data| {
            let demand_avg = data.demand();

            if tracing::enabled!(tracing::Level::TRACE) {
                s += &format!("{name}={demand_avg} ",);
            }

            total_demand += demand_avg;
            if demand_avg > 0 {
                total_target += 1;
            } else {
                data.set_target(0);
            }
        });

        if tracing::enabled!(tracing::Level::TRACE) {
            trace!("Demand: {total_target} {}", s);
        }

        self.allocate_demand(it, total_target, total_demand);
    }

    /// Adjust the quota targets for each block within the pool.
    pub fn adjust(&self, it: &impl VisitPoolAlgoData) {
        // Once we've adjusted the constraints, reset the max settings
        defer!(it.reset_max());

        // First, compute the overall request load and number of backend targets
        let mut total_demand = 0;
        let mut total_target = 0;
        let mut s = "".to_owned();

        it.with_all(|name, data| {
            // We calculate demand based on the estimated connection active time
            // multiplied by the active + waiting counts. This gives us an
            // estimated database time statistic we can use for relative
            // weighting.
            let active = data.max(MetricVariant::Active);
            let active_ms = data.avg_ms(MetricVariant::Active).max(MIN_ACTIVE_TIME);
            let waiting = data.max(MetricVariant::Waiting);
            let idle = active == 0 && waiting == 0;
            let demand = if idle {
                0
            } else {
                // Note that we clamp to DEMAND_MINIMUM to ensure the average is non-zero
                (active_ms * (waiting * DEMAND_WEIGHT_WAITING + active * DEMAND_WEIGHT_ACTIVE))
                    .max(DEMAND_MINIMUM)
            };
            data.insert_demand(demand as _);
            let demand_avg = data.demand();

            if tracing::enabled!(tracing::Level::TRACE) {
                s += &format!(
                    "{name}={demand_avg}/{demand} (a={},w={},t={}ms) ",
                    active, waiting, active_ms
                );
            }

            total_demand += demand_avg;
            if demand_avg > 0 {
                total_target += 1;
            } else {
                data.set_target(0);
            }
        });

        if tracing::enabled!(tracing::Level::TRACE) {
            trace!("Demand: {total_target} {}", s);
        }

        self.allocate_demand(it, total_target, total_demand);
    }

    /// Allocate the calculated demand to target quotas.
    fn allocate_demand(&self, it: &impl VisitPoolAlgoData, total_target: usize, total_demand: u32) {
        // Empty pool, no math
        if total_target == 0 || total_demand == 0 {
            return;
        }

        let mut allocated = 0;
        // This is the minimum number of connections we'll allocate to any particular
        // backend regardless of demand if there are less backends than the capacity.
        let min = (self.max / total_target).min(MAXIMUM_SHARED_TARGET);
        // The remaining capacity after we allocated the `min` value above.
        let capacity = self.max - min * total_target;

        it.with_all(|_name, data| {
            let demand = data.demand();
            if demand == 0 {
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

        debug_assert!(
            allocated <= self.max,
            "Attempted to allocate more than we were allowed: {allocated} > {} \
                (req={total_demand}, target={total_target})",
            self.max
        );
    }

    /// Plan a rebalance to better match the target quotas of the blocks in the
    /// pool.
    pub fn plan_rebalance(&self, it: &impl VisitPoolAlgoData) -> Vec<RebalanceOp> {
        let mut current_pool_size = it.total();
        let max_pool_size = self.max;

        // If there's room in the pool, we can be more aggressive in
        // how we allocate.
        if current_pool_size < max_pool_size {
            let mut changes = vec![];
            let mut made_changes = false;

            for i in 0..MAX_REBALANCE_CREATE {
                it.with_all(|name, block| {
                    if block.target() > block.total() && current_pool_size < max_pool_size {
                        // If we are allocated more connections than we currently have,
                        // we'll try to grab some more.
                        changes.push(RebalanceOp::Create(name.clone()));
                        current_pool_size += 1;
                        made_changes = true;
                    } else if block.total() > block.target()
                        && block.count(MetricVariant::Idle) > i
                        && (i > MAX_EXCESS_IDLE_CONNECTIONS || block.demand() == 0)
                    {
                        // If we're holding on to too many connections, we'll
                        // release some of them. If there is still some demand
                        // around, we'll try to keep a few excess connections if
                        // nobody else wants them. Otherwise, we'll just try to close
                        // all the idle connections over time.
                        changes.push(RebalanceOp::Close(name.clone()));
                        made_changes = true;
                    }
                });
                if !made_changes {
                    break;
                }
            }

            return changes;
        }

        // For any block with less connections than its quota that has
        // waiters, we want to transfer from the most overloaded block.
        let mut overloaded = vec![];
        let mut hungriest = vec![];

        let mut s1 = "".to_owned();
        let mut s2 = "".to_owned();

        it.with_all(|name, block| {
            if let Some(value) = block.hunger_score(false) {
                if tracing::enabled!(tracing::Level::TRACE) {
                    s1 += &format!("{name}={value} ");
                }
                hungriest.push((value, name.clone()))
            } else if let Some(value) = block.overfull_score(false) {
                if tracing::enabled!(tracing::Level::TRACE) {
                    s2 += &format!("{name}={value} ");
                }
                overloaded.push((value, name.clone()))
            }
        });

        if tracing::enabled!(tracing::Level::TRACE) {
            trace!("Hunger: {s1}");
            trace!("Overfullness: {s2}");
        }
        overloaded.sort();
        hungriest.sort();

        let mut tasks = vec![];

        // TODO: rebalance more than one?
        loop {
            let Some((_, to)) = hungriest.pop() else {
                // TODO: close more than one?
                if let Some((_, from)) = overloaded.pop() {
                    tasks.push(RebalanceOp::Close(from.clone()));
                }
                break;
            };

            let Some((_, from)) = overloaded.pop() else {
                break;
            };

            tasks.push(RebalanceOp::Transfer { to, from });
        }

        tasks
    }

    /// Plan a connection acquisition.
    pub fn plan_acquire(&self, db: &str, it: &impl VisitPoolAlgoData) -> AcquireOp {
        // If the block is new, we need to perform an initial adjustment to
        // ensure this block gets some capacity.
        if it.ensure_block(db, DEMAND_MINIMUM) {
            self.recalculate_shares(it);
        }

        let target_block_size = it.target(db);
        let current_block_size = it.with(db, |data| data.total()).unwrap_or_default();
        let current_pool_size = it.total();
        let max_pool_size = self.max;

        let pool_is_full = current_pool_size >= max_pool_size;
        if !pool_is_full {
            trace!("Pool has room, acquiring new connection for {db}");
            return AcquireOp::Create;
        }

        let block_has_room = current_block_size < target_block_size || target_block_size == 0;
        trace!("Acquiring {db}: {current_pool_size}/{max_pool_size} {current_block_size}/{target_block_size}");
        if pool_is_full && block_has_room {
            let mut max = 0;
            let mut which = None;
            it.with_all(|name, block| {
                if let Some(overfullness) = block.overfull_score(false) {
                    let overfullness: usize = overfullness.into();
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
    pub fn plan_release(
        &self,
        db: &str,
        release_type: ReleaseType,
        it: &impl VisitPoolAlgoData,
    ) -> ReleaseOp {
        if release_type == ReleaseType::Poison {
            return ReleaseOp::Reopen;
        }
        if release_type == ReleaseType::Drain {
            return ReleaseOp::Discard;
        }

        let current_pool_size = it.total();
        let max_pool_size = self.max;
        if current_pool_size < max_pool_size {
            trace!("Pool has room, keeping connection");
            return ReleaseOp::Release;
        }

        // We only want to consider a release elsewhere if this block is overfull
        if let Some(Some(overfull)) = it.with(db, |block| block.overfull_score(true)) {
            trace!("Block {db} is overfull ({overfull}), trying to release");
            let mut max = 0;
            let mut which = None;
            let mut s = "".to_owned();
            it.with_all(|name, block| {
                let is_self = &**name == db;
                if let Some(hunger) = block.hunger_score(is_self) {
                    let mut hunger: usize = hunger.into();
                    // Penalize switching by boosting the current database's relative hunger here
                    if is_self {
                        hunger += SELF_HUNGER_BOOST_FOR_RELEASE;
                    }

                    if tracing::enabled!(tracing::Level::TRACE) {
                        s += &format!("{name}={hunger} ");
                    }
                    // If this current block has equal hunger to the hungriest, it takes priority
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
            let algo = &config.constraints;

            let futures = FuturesUnordered::new();
            for i in (0..algo.max).map(Name::from) {
                assert_eq!(algo.plan_acquire(&i, &blocks), AcquireOp::Create);
                futures.push(blocks.create_if_needed(&connector, &i));
            }
            let conns: Vec<_> = futures.collect().await;
            let futures = FuturesUnordered::new();
            for i in (0..algo.max).map(Name::from) {
                assert_eq!(algo.plan_acquire(&i, &blocks), AcquireOp::Wait);
                futures.push(blocks.queue(&i));
            }
            for conn in conns {
                assert_eq!(
                    algo.plan_release(&conn?.state.db_name, ReleaseType::Normal, &blocks),
                    ReleaseOp::Release
                );
            }
            let conns: Vec<_> = futures.collect().await;
            for conn in conns {
                assert_eq!(
                    algo.plan_release(&conn?.state.db_name, ReleaseType::Normal, &blocks),
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
            let algo = &config.constraints;
            let blocks = Blocks::default();

            // Room for these
            let futures = FuturesUnordered::new();
            for db in (0..5).map(Name::from) {
                assert_eq!(algo.plan_acquire(&db, &blocks), AcquireOp::Create);
                futures.push(blocks.create_if_needed(&connector, &db));
            }
            // ... and these
            let futures2 = FuturesUnordered::new();
            for db in (5..10).map(Name::from) {
                assert_eq!(algo.plan_acquire(&db, &blocks), AcquireOp::Create);
                futures2.push(blocks.create_if_needed(&connector, &db));
            }
            // But not these (yet)
            let futures3 = FuturesUnordered::new();
            for db in (10..15).map(Name::from) {
                assert_eq!(algo.plan_acquire(&db, &blocks), AcquireOp::Wait);
                futures3.push(blocks.queue(&db));
            }
            let conns: Vec<_> = futures.collect().await;
            let conns2: Vec<_> = futures2.collect().await;
            // These are released to 10..15
            for conn in conns {
                let conn = conn?;
                let res = algo.plan_release(&conn.state.db_name, ReleaseType::Normal, &blocks);
                let ReleaseOp::ReleaseTo(to) = res else {
                    panic!("Wrong release: {res:?}");
                };
                blocks.task_move_to(&connector, conn, &to).await?;
            }
            // These don't have anywhere to go
            for conn in conns2 {
                let conn = conn?;
                let res = algo.plan_release(&conn.state.db_name, ReleaseType::Normal, &blocks);
                let ReleaseOp::Release = res else {
                    panic!("Wrong release: {res:?}");
                };
            }
            Ok(())
        };
        LocalSet::new().run_until(future).await
    }
}
