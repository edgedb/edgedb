use std::cell::{Cell, RefCell};
use tracing::trace;

pub trait HasPoolAlgorithmData: std::fmt::Debug {
    fn with_algo_data<T>(&self, f: impl FnOnce(&PoolAlgorithmData) -> T) -> T;

    fn set_target(&self, target: usize);
    fn target(&self) -> usize;

    fn set_stealability(&self, stealability: usize);
    fn stealability(&self) -> usize;
}

pub trait VisitPoolAlgoData<D: HasPoolAlgorithmData> {
    fn with_algo_data_all(&self, f: impl FnMut(&str, &D));
    fn with_algo_data<T>(&self, db: &str, f: impl Fn(&D) -> T) -> Option<T>;

    #[inline]
    fn target(&self, db: &str) -> usize {
        self.with_algo_data(db, |data| data.target())
            .unwrap_or_default()
    }
}

#[derive(Default, Debug, Clone, Copy)]
pub struct PoolAlgorithmData {
    waiters: usize,
    max_waiters: usize,
    oldest_waiter_ms: usize,
    max_concurrent: usize,
    avg_connect_time: usize,
    avg_disconnect_time: usize,
}

#[derive(Default, Debug)]
pub struct PoolAlgoTargetData {
    target_size: Cell<usize>,
    stealability: Cell<usize>,
    data: RefCell<PoolAlgorithmData>,
}

impl HasPoolAlgorithmData for PoolAlgoTargetData {
    fn with_algo_data<T>(&self, f: impl FnOnce(&PoolAlgorithmData) -> T) -> T {
        f(&self.data.borrow())
    }
    fn set_target(&self, target: usize) {
        self.target_size.set(target);
    }
    fn set_stealability(&self, stealability: usize) {
        self.stealability.set(stealability);
    }
    fn target(&self) -> usize {
        self.target_size.get()
    }
    fn stealability(&self) -> usize {
        self.stealability.get()
    }
}

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
        // First, compute the overall request load and number of backend targets
        let mut total_requested = 0;
        let mut total_target = 0;
        it.with_algo_data_all(|name, data| {
            let count = data.with_algo_data(|data| data.max_concurrent + data.max_waiters);
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
        let min = total_target / self.max;
        it.with_algo_data_all(|name, data| {
            data.set_target(min);
        });

        // // No starvation
        // if total_target <= self.max {}

        // // Starvation
    }

    /// Identify the most appealing victim for pool theft.
    pub fn identify_victim<'a, 'b, T, U>(&self, _it: &'a U) -> Option<&str>
    where
        U: VisitPoolAlgoData<T>,
        T: 'b,
        T: HasPoolAlgorithmData,
    {
        // it.with_algo_data_all(|name, data| {
        //     data.stealability()
        // });
        None
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_one_block() {}
}
