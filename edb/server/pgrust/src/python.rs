use std::path::Path;

use crate::conn_string::{self, EnvVar};
use pyo3::{
    exceptions::PyException,
    pyfunction, pymodule,
    types::{PyAnyMethods, PyByteArray, PyModule, PyModuleMethods},
    wrap_pyfunction, Bound, PyAny, PyResult, Python,
};
use serde_pickle::SerOptions;

impl EnvVar for (String, Bound<'_, PyAny>) {
    fn read(&self, name: &'static str) -> Option<std::borrow::Cow<str>> {
        // os.environ[name], or the default user if not
        let py_str = self.1.get_item(name).ok();
        if name == "PGUSER" && py_str.is_none() {
            Some((&self.0).into())
        } else {
            py_str.map(|s| s.to_string().into())
        }
    }
}

#[pyfunction]
fn parse_dsn(py: Python, username: String, home_dir: String, s: String) -> PyResult<Bound<PyAny>> {
    let pickle = py.import_bound("pickle")?;
    let loads = pickle.getattr("loads")?;
    let os = py.import_bound("os")?;
    let environ = os.getattr("environ")?;
    match conn_string::parse_postgres_url(&s, (username, environ)) {
        Ok(mut res) => {
            if let Some(warning) =
                res.password
                    .resolve(Path::new(&home_dir), &res.hosts, &res.database, &res.user)?
            {
                let warnings = py.import_bound("warnings")?;
                warnings.call_method1("warn", (warning.to_string(),))?;
            }
            let paths = res.ssl.resolve(Path::new(&home_dir))?;
            // Use serde_pickle to get a python-compatible representation of the result
            let vec = serde_pickle::to_vec(&(res, paths), SerOptions::new()).unwrap();
            loads.call1((PyByteArray::new_bound(py, &vec),))
        }
        Err(err) => Err(PyException::new_err(err.to_string())),
    }
}

#[pymodule]
fn _pg_rust(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_dsn, m)?)?;
    Ok(())
}
