use std::{
    cell::{Cell, RefCell},
    num::NonZeroUsize,
};
use tracing::trace;

use crate::block::Name;

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
    ReleaseTo(Name),
}

/// How hungry or overfull a block is.
#[derive(Default, Debug, Clone, Copy)]
pub enum Hunger {
    /// This block has the correct number of connections.
    #[default]
    Satisfied,
    /// This block has an expected deficit of connections. Ideally the pool
    /// should transfer this number of connections here, if possible.
    Hungry(NonZeroUsize),
    /// This block has an expected excess of connections. The pool may transfer
    /// up to this number of connections away from this block if the block has
    /// enough idle capacity.
    Overfull(NonZeroUsize),
}

pub enum LiveState {
    HasWaiters(NonZeroUsize),
    Capacity,
    HasIdle(NonZeroUsize),
}

pub trait HasPoolAlgorithmData: std::fmt::Debug {
    fn with_algo_data<T>(&self, f: impl FnOnce(&PoolAlgorithmData) -> T) -> T;

    fn set_target(&self, target: usize);
    fn target(&self) -> usize;
}

pub trait VisitPoolAlgoData<D: HasPoolAlgorithmData> {
    /// Materializes the algorithm data in preparation for computation.
    fn update_algo_data(&self);
    fn with_algo_data_all(&self, f: impl FnMut(&Name, &PoolAlgoTargetData));
    fn with_algo_data<T>(&self, db: &str, f: impl Fn(&PoolAlgoTargetData) -> T) -> Option<T>;
    fn total(&self) -> usize;

    #[inline]
    fn target(&self, db: &str) -> usize {
        self.with_algo_data(db, |data| data.target())
            .unwrap_or_default()
    }
}

/// Factual information about the current state of the pool.
#[derive(Default, Debug, Clone, Copy)]
pub struct PoolAlgorithmData {
    pub total: usize,
    pub active: usize,
    pub idle: usize,
    pub waiters: usize,
    pub max_waiters: usize,
    pub oldest_waiter_ms: usize,
    pub max_concurrent: usize,
    pub avg_connect_time: usize,
    pub avg_disconnect_time: usize,
    pub avg_hold_time: usize,
}

impl PoolAlgorithmData {
    pub fn hunger(&self, target: usize) -> Hunger {
        let current = self.total;
        if current < target {
            Hunger::Hungry((target - current).try_into().unwrap())
        } else if current > target {
            Hunger::Overfull((current - target).try_into().unwrap())
        } else {
            Hunger::Satisfied
        }
    }

    pub fn live(&self) -> LiveState {
        if self.waiters > 0 {
            LiveState::HasWaiters(self.waiters.try_into().unwrap())
        } else if self.idle > 0 {
            LiveState::HasIdle(self.idle.try_into().unwrap())
        } else {
            LiveState::Capacity
        }
    }

    pub fn hunger_score(&self, target: usize) -> Option<NonZeroUsize> {
        const DIFF_WEIGHT: usize = 100;
        const WAITER_WEIGHT: usize = 1;

        let current = self.total;
        trace!("{} {} {}", current, target, self.waiters);
        if current > target {
            None
        } else {
            if target > current {
                let diff = target - current;
                let score = diff * DIFF_WEIGHT + self.waiters * WAITER_WEIGHT;
                score.try_into().ok()
            } else if self.waiters > 0 {
                (self.waiters * WAITER_WEIGHT).try_into().ok()
            } else {
                None
            }
        }
    }

    pub fn overfull_score(&self, target: usize, will_release: bool) -> Option<NonZeroUsize> {
        const DIFF_WEIGHT: usize = 100;
        const IDLE_WEIGHT: usize = 1;

        let idle = self.idle + if will_release { 1 } else { 0 };
        let current = self.total;
        if target >= current || idle == 0 {
            None
        } else {
            if current > target {
                let diff = current - target;
                let score = diff * DIFF_WEIGHT + idle * IDLE_WEIGHT;
                score.try_into().ok()
            } else {
                (self.idle * IDLE_WEIGHT).try_into().ok()
            }
        }
    }
}

#[derive(Default, Debug)]
pub struct PoolAlgoTargetData {
    /// A numeric score representing hunger or overfullness.
    target_size: Cell<usize>,
    pub data: RefCell<PoolAlgorithmData>,
}

impl HasPoolAlgorithmData for PoolAlgoTargetData {
    fn with_algo_data<T>(&self, f: impl FnOnce(&PoolAlgorithmData) -> T) -> T {
        f(&self.data.borrow())
    }
    fn set_target(&self, target: usize) {
        self.target_size.set(target);
    }
    fn target(&self) -> usize {
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
        T: HasPoolAlgorithmData,
    {
        it.update_algo_data();

        // First, compute the overall request load and number of backend targets
        let mut total_requested = 0;
        let mut total_target = 0;
        let mut total_demand = 0;

        it.with_algo_data_all(|name, data| {
            let count = data.with_algo_data(|data| data.max_concurrent + data.max_waiters);
            let demand = data.with_algo_data(|data| data.avg_hold_time * data.waiters);
            total_requested += count;
            total_target += 1;
            trace!("{name}: {data:?}");
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

            it.with_algo_data_all(|name, data| {
                let target_size =
                    data.with_algo_data(|data| data.max_concurrent + data.max_waiters);

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
        it.with_algo_data_all(|name, data| {
            data.set_target(min);
        });
    }

    pub fn plan_rebalance<'a, 'b, T, U>(&self, it: &'a U) -> Vec<RebalanceOp>
    where
        U: VisitPoolAlgoData<T>,
        T: 'b,
        T: HasPoolAlgorithmData,
    {
        // If nobody in the pool is waiting for anything, we don't do any work
        // here.

        // For any block with less connections than its quota that has
        // waiters, we want to transfer from the most overloaded block.
        let mut overloaded = vec![];
        let mut hungriest = vec![];
        it.update_algo_data();

        it.with_algo_data_all(|name, block| {
            if let Some(value) = block.with_algo_data(|data| data.hunger_score(block.target())) {
                hungriest.push((value, name.clone()))
            } else if let Some(value) =
                block.with_algo_data(|data| data.overfull_score(block.target(), false))
            {
                overloaded.push((value, name.clone()))
            }
        });

        overloaded.sort();
        hungriest.sort();

        let mut tasks = vec![];

        loop {
            let Some((hunger, to)) = hungriest.pop() else {
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
        T: HasPoolAlgorithmData,
    {
        let target_block_size = it.target(db);
        let current_block_size = it
            .with_algo_data(db, |data| data.with_algo_data(|data| data.total))
            .unwrap_or_default();
        let current_pool_size = it.total();
        let max_pool_size = self.max;

        let pool_is_full = current_pool_size >= max_pool_size;
        let block_has_room = current_block_size < target_block_size || target_block_size == 0;
        trace!("Acquiring {db}: {current_pool_size}/{max_pool_size} {current_block_size}/{target_block_size}");
        if pool_is_full && block_has_room {
            let mut max = 0;
            let mut which = None;
            it.with_algo_data_all(|name, block| {
                if let Some(overfullness) =
                    block.with_algo_data(|data| data.overfull_score(block.target(), false))
                {
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

    pub fn plan_release<'a, 'b, T, U>(&self, db: &str, it: &'a U) -> ReleaseOp
    where
        U: VisitPoolAlgoData<T>,
        T: 'b,
        T: HasPoolAlgorithmData,
    {
        it.update_algo_data();
        // We only want to consider a release elsewhere if this block is overfull
        if let Some(Some(overfull)) = it.with_algo_data(db, |block| {
            block.with_algo_data(|data| data.overfull_score(block.target(), true))
        }) {
            trace!("Block is overfull, trying to release {overfull}");
            let mut max = 0;
            let mut which = None;
            it.with_algo_data_all(|name, block| {
                if let Some(hunger) = block.with_algo_data(|data| data.hunger_score(block.target()))
                {
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
                            .plan_release(&conn?.state.db_name, &blocks),
                        ReleaseOp::Release
                    );
                }
                let conns: Vec<_> = futures.collect().await;
                for conn in conns {
                    assert_eq!(
                        config
                            .constraints
                            .plan_release(&conn?.state.db_name, &blocks),
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
                for i in 0..config.constraints.max {
                    let db = format!("{i}");
                    assert_eq!(
                        config.constraints.plan_acquire(&db, &blocks),
                        AcquireOp::Create
                    );
                    futures.push(blocks.create_if_needed(&connector, &db));
                }
                let futures2 = FuturesUnordered::new();
                for i in 0..config.constraints.max {
                    let db = format!("{i}b");
                    assert_eq!(
                        config.constraints.plan_acquire(&db, &blocks),
                        AcquireOp::Wait
                    );
                    futures2.push(blocks.queue(&db));
                }
                let conns: Vec<_> = futures.collect().await;
                for conn in conns {
                    let conn = conn?;
                    let res = config
                        .constraints
                        .plan_release(&conn.state.db_name, &blocks);
                    let ReleaseOp::ReleaseTo(to) = res else {
                        panic!("Wrong release: {res:?}");
                    };
                    blocks.task_move_to(&connector, conn, &to).await?;
                }
                Ok(())
            })
            .await
    }
}
