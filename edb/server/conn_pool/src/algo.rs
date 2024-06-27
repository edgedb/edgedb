use scopeguard::defer;
use std::{cell::Cell, num::NonZeroUsize};
use tracing::trace;

use crate::{block::Name, metrics::MetricVariant};

#[derive(Debug, Clone, PartialEq, Eq)]
/// Determines the rebalance plan based on the current pool state.
pub enum RebalanceOp {
    /// Transfer from one block to another
    Transfer(Name, Name),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcquireOp {
    Create,
    Steal(Name),
    Wait,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReleaseOp {
    Release,
    Reopen,
    ReleaseTo(Name),
}

pub trait VisitPoolAlgoData<D: PoolAlgorithmDataBlock>: PoolAlgorithmDataPool {
    /// Materializes the algorithm data in preparation for computation.
    fn with_all(&self, f: impl FnMut(&Name, &D));
    fn with<T>(&self, db: &str, f: impl Fn(&D) -> T) -> Option<T>;

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
    fn oldest_ms(&self, variant: MetricVariant) -> usize;

    fn hunger_score(&self) -> Option<NonZeroUsize> {
        const DIFF_WEIGHT: usize = 100;
        const WAITER_WEIGHT: usize = 1;

        let waiters = self
            .count(MetricVariant::Waiting)
            .saturating_sub(self.count(MetricVariant::Connecting));
        let current = self.total();
        let target = self.target();
        trace!("{} {} {}", current, target, waiters);
        if current > target {
            None
        } else {
            if target > current {
                let diff = target - current;
                let score = diff * DIFF_WEIGHT + waiters * WAITER_WEIGHT;
                score.try_into().ok()
            } else if waiters > 0 {
                (waiters * WAITER_WEIGHT).try_into().ok()
            } else {
                None
            }
        }
    }

    fn overfull_score(&self, will_release: bool) -> Option<NonZeroUsize> {
        const DIFF_WEIGHT: usize = 100;
        const IDLE_WEIGHT: usize = 1;

        let idle = self.count(MetricVariant::Idle) + if will_release { 1 } else { 0 };
        let current = self.total();
        let target = self.target();
        if target >= current || idle == 0 {
            None
        } else {
            if current > target {
                let diff = current - target;
                let score = diff * DIFF_WEIGHT + idle * IDLE_WEIGHT;
                score.try_into().ok()
            } else {
                (idle * IDLE_WEIGHT).try_into().ok()
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
}

impl PoolAlgoTargetData {
    pub fn set_target(&self, target: usize) {
        self.target_size.set(target);
    }
    pub fn target(&self) -> usize {
        self.target_size.get()
    }
}

#[derive(Debug)]
pub struct PoolConstraints {
    pub max: usize,
    pub max_per_target: usize,
}

impl PoolConstraints {
    /// Adjust the quota targets for each block within the pool
    pub fn adjust<'a, 'b, T, U>(&self, it: &'a U)
    where
        U: VisitPoolAlgoData<T>,
        T: 'b,
        T: PoolAlgorithmDataBlock,
    {
        // Once we've adjusted the constraints, reset the max settings
        defer!(it.reset_max());

        // First, compute the overall request load and number of backend targets
        let mut total_requested = 0;
        let mut total_target = 0;
        let mut total_demand = 0;

        it.with_all(|name, data| {
            let count = data.max(MetricVariant::Active) + data.max(MetricVariant::Waiting);
            let demand = data.avg_ms(MetricVariant::Active) * data.max(MetricVariant::Waiting);
            total_requested += count;
            total_target += 1;
        });

        // Empty pool, no math
        if total_target == 0 {
            return;
        }

        // If we are unconstrained, it's easy.
        if total_requested <= self.max {
            // The "fair share" and "spare share"

            let spare = self.max - total_requested;
            let mut allocated = 0;

            it.with_all(|name, data| {
                let target_size =
                    data.max(MetricVariant::Active) + data.max(MetricVariant::Waiting);

                // Give everyone what they requested, plus a share of the spare
                // capacity. If there is leftover spare capacity, that capacity
                // may float between whoever needs it the most.
                let spare_for_target = spare as f32 / total_target as f32;
                let spare_for_target = (spare_for_target as usize).min(2);
                data.set_target(target_size + spare_for_target);
                allocated += target_size + spare_for_target;

                trace!(
                    "{name}: Target pool size: {} target={target_size} spare={spare_for_target}",
                    target_size + spare_for_target
                );
            });

            debug_assert!(allocated <= self.max, "Attempted to allocate more than we were allowed: {allocated} > {} (req={total_requested}, target={total_target}, spare={spare})", self.max);
            return;
        }

        // Once we start getting constrained, connections will compete for resources and require
        // us to use the various stats to determine which one is "more important".
        let min = total_target / self.max;
        it.with_all(|name, data| {
            data.set_target(min);
        });
    }

    pub fn plan_rebalance<'a, 'b, T, U>(&self, it: &'a U) -> Vec<RebalanceOp>
    where
        U: VisitPoolAlgoData<T>,
        T: 'b,
        T: PoolAlgorithmDataBlock,
    {
        // If nobody in the pool is waiting for anything, we don't do any work
        // here.

        // For any block with less connections than its quota that has
        // waiters, we want to transfer from the most overloaded block.
        let mut overloaded = vec![];
        let mut hungriest = vec![];

        it.with_all(|name, block| {
            if let Some(value) = block.hunger_score() {
                hungriest.push((value, name.clone()))
            } else if let Some(value) = block.overfull_score(false) {
                overloaded.push((value, name.clone()))
            }
        });

        overloaded.sort();
        hungriest.sort();

        let mut tasks = vec![];

        // TODO: rebalance more than one?
        loop {
            let Some((_, to)) = hungriest.pop() else {
                break;
            };

            let Some((_, from)) = overloaded.pop() else {
                break;
            };

            tasks.push(RebalanceOp::Transfer(to, from.clone()));
        }

        tasks
    }

    pub fn plan_acquire<'a, 'b, T, U>(&self, db: &str, it: &'a U) -> AcquireOp
    where
        U: VisitPoolAlgoData<T>,
        T: 'b,
        T: PoolAlgorithmDataBlock,
    {
        let target_block_size = it.target(db);
        let current_block_size = it.with(db, |data| data.total()).unwrap_or_default();
        let current_pool_size = it.total();
        let max_pool_size = self.max;

        let pool_is_full = current_pool_size >= max_pool_size;
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
                        max = overfullness.into();
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

    pub fn plan_release<'a, 'b, T, U>(&self, db: &str, poison: bool, it: &'a U) -> ReleaseOp
    where
        U: VisitPoolAlgoData<T>,
        T: 'b,
        T: PoolAlgorithmDataBlock,
    {
        if poison {
            return ReleaseOp::Reopen;
        }

        // We only want to consider a release elsewhere if this block is overfull
        if let Some(Some(overfull)) = it.with(db, |block| block.overfull_score(true)) {
            trace!("Block is overfull, trying to release {overfull}");
            let mut max = 0;
            let mut which = None;
            it.with_all(|name, block| {
                if let Some(hunger) = block.hunger_score() {
                    let hunger: usize = hunger.into();
                    if hunger > max {
                        which = Some(name.clone());
                        max = hunger;
                    }
                }
            });

            match which {
                Some(name) => ReleaseOp::ReleaseTo(name),
                None => ReleaseOp::Release,
            }
        } else {
            trace!("Block {db} is not overfull, releasing");
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
        LocalSet::new()
            .run_until(async {
                let connector = BasicConnector::no_delay();
                let config = PoolConfig::suggested_default_for(10);
                let blocks = Blocks::default();
                let futures = FuturesUnordered::new();
                for i in 0..config.constraints.max {
                    let db = format!("{i}");
                    assert_eq!(
                        config.constraints.plan_acquire(&db, &blocks),
                        AcquireOp::Create
                    );
                    futures.push(blocks.create_if_needed(&connector, &db));
                }
                let conns: Vec<_> = futures.collect().await;
                let futures = FuturesUnordered::new();
                for i in 0..config.constraints.max {
                    let db = format!("{i}");
                    assert_eq!(
                        config.constraints.plan_acquire(&format!("{i}"), &blocks),
                        AcquireOp::Wait
                    );
                    futures.push(blocks.queue(&db));
                }
                for conn in conns {
                    assert_eq!(
                        config
                            .constraints
                            .plan_release(&conn?.state.db_name, false, &blocks),
                        ReleaseOp::Release
                    );
                }
                let conns: Vec<_> = futures.collect().await;
                for conn in conns {
                    assert_eq!(
                        config
                            .constraints
                            .plan_release(&conn?.state.db_name, false, &blocks),
                        ReleaseOp::Release
                    );
                }
                Ok(())
            })
            .await
    }

    #[test(tokio::test(flavor = "current_thread"))]
    async fn test_pool_starved() -> Result<()> {
        LocalSet::new()
            .run_until(async {
                let connector = BasicConnector::no_delay();
                let config = PoolConfig::suggested_default_for(10);
                let blocks = Blocks::default();
                let futures = FuturesUnordered::new();
                for i in 0..5 {
                    let db = format!("{i}");
                    assert_eq!(
                        config.constraints.plan_acquire(&db, &blocks),
                        AcquireOp::Create
                    );
                    futures.push(blocks.create_if_needed(&connector, &db));
                }
                let futures2 = FuturesUnordered::new();
                for i in 5..10 {
                    let db = format!("{i}");
                    assert_eq!(
                        config.constraints.plan_acquire(&db, &blocks),
                        AcquireOp::Create
                    );
                    futures2.push(blocks.create_if_needed(&connector, &db));
                }
                let futures3 = FuturesUnordered::new();
                for i in 10..15 {
                    let db = format!("{i}");
                    assert_eq!(
                        config.constraints.plan_acquire(&db, &blocks),
                        AcquireOp::Wait
                    );
                    futures3.push(blocks.queue(&db));
                }
                let conns: Vec<_> = futures.collect().await;
                let conns2: Vec<_> = futures2.collect().await;
                for conn in conns {
                    let conn = conn?;
                    let res = config
                        .constraints
                        .plan_release(&conn.state.db_name, false, &blocks);
                    let ReleaseOp::ReleaseTo(to) = res else {
                        panic!("Wrong release: {res:?}");
                    };
                    blocks.task_move_to(&connector, conn, &to).await?;
                }
                for conn in conns2 {
                    let conn = conn?;
                    let res = config
                        .constraints
                        .plan_release(&conn.state.db_name, false, &blocks);
                    let ReleaseOp::Release = res else {
                        panic!("Wrong release: {res:?}");
                    };
                }
                Ok(())
            })
            .await
    }
}
