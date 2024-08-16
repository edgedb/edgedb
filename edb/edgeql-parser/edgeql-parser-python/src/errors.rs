use edgeql_parser::tokenizer::Error;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};
use pyo3::{create_exception, exceptions};

use crate::tokenizer::OpaqueToken;

create_exception!(_edgeql_parser, SyntaxError, exceptions::PyException);

#[pyclass]
pub struct ParserResult {
    #[pyo3(get)]
    pub out: PyObject,

    #[pyo3(get)]
    pub errors: PyObject,
}

#[pymethods]
impl ParserResult {
    fn pack(&self, py: Python) -> PyResult<PyObject> {
        let tokens: &PyList = self.out.downcast(py)?;
        let mut rv = Vec::with_capacity(tokens.len());
        for token in tokens {
            let token: &PyCell<OpaqueToken> = token.downcast()?;
            rv.push(token.borrow().inner.clone());
        }
        let mut buf = vec![0u8]; // type and version
        bincode::serialize_into(&mut buf, &rv)
            .map_err(|e| PyValueError::new_err(format!("Failed to pack: {e}")))?;
        Ok(PyBytes::new(py, buf.as_slice()).into())
    }
}

pub fn parser_error_into_tuple(py: Python, error: Error) -> PyObject {
    (
        error.message,
        (error.span.start, error.span.end),
        error.hint,
        error.details,
    )
        .into_py(py)
}
