pub(crate) mod block;
pub(crate) mod conn;
pub(crate) mod pool;
pub(crate) mod waitqueue;

// #[cfg(not(test))]
// #[pymodule]
// fn _conn_pool(py: Python, m: &PyModule) -> PyResult<()> {
//     Ok(())
// }
