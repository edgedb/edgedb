use std::{
    cell::{Cell, RefCell},
};
use tracing::trace;

pub trait HasPoolAlgorithmData: std::fmt::Debug {
    fn with_algo_data<T>(&self, f: impl FnOnce(&PoolAlgorithmData) -> T) -> T;

    fn set_target(&self, target: usize);
    fn target(&self) -> usize;

    fn set_stealability(&self, stealability: usize);
    fn stealability(&self) -> usize;
}

pub trait VisitPoolAlgoData<T: HasPoolAlgorithmData> {
    fn with_algo_data_all(&self, f: impl FnMut(&T));
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
        &'a U: VisitPoolAlgoData<T>,
        T: 'b,
        T: HasPoolAlgorithmData,
    {
        let mut total_requested = 0;
        let mut total_target = 0;
        it.with_algo_data_all(|data| {
            let count = data.with_algo_data(|data| data.max_concurrent + data.max_waiters);
            total_requested += count;
            if count > 0 {
                total_target += 1;
            }
            trace!("{data:?}");
        });

        // If we are unconstrained, it's easy.
        if total_requested <= self.max {
            let spare = self.max - total_requested;
            let mut allocated = 0;

            it.with_algo_data_all(|data| {
                let target_size =
                    data.with_algo_data(|data| data.max_concurrent + data.max_waiters);
                let spare_for_target = (spare as f32
                    * (target_size as f32 / total_requested as f32))
                    .min(2.0) as usize;
                data.set_target(target_size + spare_for_target);
                allocated += target_size + spare_for_target;
                trace!("Target pool size: {}", target_size + spare_for_target);
            });

            debug_assert!(allocated <= self.max);
            return;
        }

        let min = total_target / self.max;

        // No starvation
        if total_target <= self.max {}

        // Starvation
    }

    /// Identify the most appealing victim for pool theft.
    pub fn identify_victim(&self) {

    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_one_block() {}
}
