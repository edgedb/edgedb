#![cfg(feature = "python_extension")]
mod py_entry;
mod py_exception;
mod py_token;
mod rewrite;
mod token_vec;

pub use py_token::{PyToken, PyTokenKind};
pub use rewrite::{rewrite, Value, Variable};

use py_exception::{AssertionError, LexingError, NotFoundError, QueryError, SyntaxError};
use pyo3::{prelude::*, types::PyString};

/// Rust optimizer for graphql queries
#[pymodule]
fn _graphql_rewrite(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_rewrite, m)?)?;
    m.add_class::<py_entry::Entry>()?;
    m.add("LexingError", py.get_type::<LexingError>())?;
    m.add("SyntaxError", py.get_type::<SyntaxError>())?;
    m.add("NotFoundError", py.get_type::<NotFoundError>())?;
    m.add("AssertionError", py.get_type::<AssertionError>())?;
    m.add("QueryError", py.get_type::<QueryError>())?;
    Ok(())
}

#[pyo3::pyfunction(name = "rewrite")]
#[pyo3(signature = (operation, text))]
fn py_rewrite(
    py: Python<'_>,
    operation: Option<&Bound<PyString>>,
    text: &Bound<PyString>,
) -> PyResult<py_entry::Entry> {
    // convert args
    let operation = operation.map(|x| x.to_string());
    let text = text.to_string();

    match rewrite::rewrite(operation.as_ref().map(|x| &x[..]), &text) {
        Ok(entry) => py_entry::convert_entry(py, entry),
        Err(e) => Err(py_exception::convert_error(e)),
    }
}
