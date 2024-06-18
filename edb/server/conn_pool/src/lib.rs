pub(crate) mod algo;
pub(crate) mod block;
pub(crate) mod conn;
pub(crate) mod pool;
pub(crate) mod waitqueue;

// Public interface

pub use conn::Connector;
pub use pool::{Pool, PoolConfig, PoolHandle};

#[cfg(test)]
pub mod test;

#[cfg(feature = "python_extension")]
mod python;
