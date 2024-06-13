use pyo3::prelude::*;

pub(crate) mod block;
pub(crate) mod conn;
pub(crate) mod waitqueue;
// mod pool;

// #[cfg(not(test))]
// #[pymodule]
// fn _conn_pool(py: Python, m: &PyModule) -> PyResult<()> {
//     Ok(())
// }
