use pyo3::{
    pymodule,
    types::{PyAnyMethods, PyModule, PyModuleMethods},
    Bound, PyResult, Python,
};
use pyo3_util::logging::{get_python_logger_level, initialize_logging_in_thread};

const MODULE_PREFIX: &str = "edb.server._rust_native";

fn add_child_module(
    py: Python,
    parent: &Bound<PyModule>,
    name: &str,
    init_fn: fn(Python, &Bound<PyModule>) -> PyResult<()>,
) -> PyResult<()> {
    let full_name = format!("{}.{}", MODULE_PREFIX, name);
    let child_module = PyModule::new(py, &full_name)?;
    init_fn(py, &child_module)?;
    parent.add(name, &child_module)?;

    // Add the child module to the sys.modules dictionary so it can be imported
    // by name.
    let sys_modules = py.import("sys")?.getattr("modules")?;
    sys_modules.set_item(full_name, child_module)?;
    Ok(())
}

#[pymodule]
fn _rust_native(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    // Initialize any logging in this thread to route to "edb.server"
    let level = get_python_logger_level(py, "edb.server")?;
    initialize_logging_in_thread("edb.server", level);

    add_child_module(py, m, "_conn_pool", conn_pool::python::_conn_pool)?;
    add_child_module(py, m, "_pg_rust", pgrust::python::_pg_rust)?;
    add_child_module(py, m, "_http", http::python::_http)?;

    Ok(())
}
