pub(crate) mod algo;
pub(crate) mod block;
pub(crate) mod conn;
pub(crate) mod metrics;
pub(crate) mod pool;
pub(crate) mod waitqueue;

mod time {
    #[cfg(not(test))]
    pub use std::time::Instant;
    #[cfg(test)]
    pub use tokio::time::Instant;
}

#[cfg(feature = "optimizer")]
pub use algo::knobs;

// Public interface

pub use conn::Connector;
pub use pool::{Pool, PoolConfig, PoolHandle};

#[cfg(test)]
pub mod test;

#[cfg(feature = "python_extension")]
mod python;
