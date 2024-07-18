use pyo3::{pymodule, types::PyModule, PyResult, Python};

#[pymodule]
fn _conn_pool(py: Python, m: &PyModule) -> PyResult<()> {
    Ok(())
}
