use std::{
    cell::{Cell, RefCell},
    num::NonZeroUsize,
};
use tracing::trace;

use crate::block::Name;

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

pub trait HasPoolAlgorithmData: std::fmt::Debug {
    fn with_algo_data<T>(&self, f: impl FnOnce(&PoolAlgorithmData) -> T) -> T;

    fn set_target(&self, target: usize);
    fn target(&self) -> usize;
}

pub trait VisitPoolAlgoData<D: HasPoolAlgorithmData> {
    /// Materializes the algorithm data in preparation for computation.
    fn update_algo_data(&self);
    fn with_algo_data_all(&self, f: impl FnMut(&Name, &PoolAlgoTargetData, usize));
    fn with_algo_data<T>(&self, db: &str, f: impl Fn(&PoolAlgoTargetData) -> T) -> Option<T>;

    #[inline]
    fn target(&self, db: &str) -> usize {
        self.with_algo_data(db, |data| data.target())
            .unwrap_or_default()
    }
}

/// Factual information about the current state of the pool.
#[derive(Default, Debug, Clone, Copy)]
pub struct PoolAlgorithmData {
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

#[derive(Default, Debug)]
pub struct PoolAlgoTargetData {
    /// A numeric score representing hunger or overfullness.
    pub hunger: Cell<Hunger>,
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

        it.with_algo_data_all(|name, data, _| {
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

            it.with_algo_data_all(|name, data, _| {
                let target_size =
                    data.with_algo_data(|data| data.max_concurrent + data.max_waiters);

                // Give everyone what they requested, plus a share of the spare capacity
                // TODO: there will be some leftover capacity here
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
        let min = total_target / self.max + 1;
        it.with_algo_data_all(|name, data, _| {
            data.set_target(min);
        });
    }

    /// Identify the most appealing victim for pool theft.
    pub fn identify_victim<'a, 'b, T, U>(&self, it: &'a U) -> Option<Name>
    where
        U: VisitPoolAlgoData<T>,
        T: 'b,
        T: HasPoolAlgorithmData,
    {
        let mut max = 0;
        let mut which = None;
        it.with_algo_data_all(|name, data, free| {
            if let Hunger::Overfull(overfullness) = data.hunger.get() {
                let overfullness: usize = overfullness.into();
                if overfullness > max && free > 0 {
                    which = Some(name.clone());
                    max = overfullness.into();
                }
            }
        });
        which
    }

    /// Identify the most desperate block for hunger.
    pub fn identify_hungriest<'a, 'b, T, U>(&self, it: &'a U) -> Option<Name>
    where
        U: VisitPoolAlgoData<T>,
        T: 'b,
        T: HasPoolAlgorithmData,
    {
        let mut max = 0;
        let mut which = None;
        it.with_algo_data_all(|name, data, free| {
            if let Hunger::Hungry(hunger) = data.hunger.get() {
                let hunger: usize = hunger.into();
                if hunger > max {
                    which = Some(name.clone());
                    max = hunger;
                }
            }
        });
        which
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_one_block() {}
}
