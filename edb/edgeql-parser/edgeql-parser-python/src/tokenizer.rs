use edgeql_parser::tokenizer::{Token, Tokenizer};
use once_cell::sync::OnceCell;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyString};

use crate::errors::{parser_error_into_tuple, ParserResult};

#[pyfunction]
pub fn tokenize(py: Python, s: &Bound<PyString>) -> PyResult<ParserResult> {
    let data = s.to_string();

    let token_stream = Tokenizer::new(&data[..]).validated_values().with_eof();

    let mut tokens = vec![];
    let mut errors = vec![];

    for res in token_stream.into_iter() {
        match res {
            Ok(token) => tokens.push(token),
            Err(e) => {
                errors.push(parser_error_into_tuple(&e).into_pyobject(py)?);

                // TODO: fix tokenizer to skip bad tokens and continue
                break;
            }
        }
    }

    let out = tokens_to_py(py, tokens)?.into_pyobject(py)?.into();
    let errors = PyList::new(py, errors)?.into();

    Ok(ParserResult { out, errors })
}

// An opaque wrapper around [edgeql_parser::tokenizer::Token].
// Supports Python pickle serialization.
#[pyclass]
pub struct OpaqueToken {
    pub inner: Token<'static>,
}

#[pymethods]
impl OpaqueToken {
    fn __repr__(&self) -> PyResult<String> {
        Ok(self.inner.to_string())
    }
    fn __reduce__(&self, py: Python) -> PyResult<(PyObject, (PyObject,))> {
        let data = bincode::serialize(&self.inner)
            .map_err(|e| PyValueError::new_err(format!("Failed to reduce: {e}")))?;

        let tok = get_unpickle_token_fn(py);
        Ok((tok, (PyBytes::new(py, &data).into(),)))
    }
}

pub fn tokens_to_py(py: Python<'_>, rust_tokens: Vec<Token>) -> PyResult<Py<PyList>> {
    Ok(PyList::new(
        py,
        rust_tokens.into_iter().map(|tok| OpaqueToken {
            inner: tok.cloned(),
        }),
    )?
    .unbind())
}

/// To support pickle serialization of OpaqueTokens, we need to provide a
/// deserialization function in __reduce__ methods.
/// This function must not be inlined and must be globally accessible.
/// To achieve this, we expose it a part of the module definition
/// (`unpickle_token`) and save reference to is in the `FN_UNPICKLE_TOKEN`.
///
/// A bit hackly, but it works.
static FN_UNPICKLE_TOKEN: OnceCell<PyObject> = OnceCell::new();

pub fn fini_module(m: &Bound<PyModule>) {
    let _unpickle_token = m.getattr("unpickle_token").unwrap();
    FN_UNPICKLE_TOKEN
        .set(_unpickle_token.unbind())
        .expect("module is already initialized");
}

#[pyfunction]
pub fn unpickle_token(bytes: &Bound<PyBytes>) -> PyResult<OpaqueToken> {
    let token = bincode::deserialize(bytes.as_bytes())
        .map_err(|e| PyValueError::new_err(format!("Failed to read token: {e}")))?;
    Ok(OpaqueToken { inner: token })
}

fn get_unpickle_token_fn(py: Python) -> PyObject {
    let py_function = FN_UNPICKLE_TOKEN.get().expect("module uninitialized");
    py_function.clone_ref(py)
}
