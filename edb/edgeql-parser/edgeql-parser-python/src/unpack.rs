use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use edgeql_parser::tokenizer::Token;

use crate::normalize::PackedEntry;
use crate::pynormalize::Entry;
use crate::tokenizer::tokens_to_py;

#[pyfunction]
pub fn unpack(py: Python<'_>, serialized: &PyBytes) -> PyResult<PyObject> {
    let buf = serialized.as_bytes();
    match buf[0] {
        0u8 => {
            let tokens: Vec<Token> = bincode::deserialize(&buf[1..])
                .map_err(|e| PyValueError::new_err(format!("{e}")))?;
            tokens_to_py(py, tokens)
        }
        1u8 => {
            let pack: PackedEntry = bincode::deserialize(&buf[1..])
                .map_err(|e| PyValueError::new_err(format!("Failed to unpack: {e}")))?;
            let entry = Entry::new(py, pack.into())?;
            Ok(entry.into_py(py))
        }
        _ => Err(PyValueError::new_err(format!("Invalid type/version byte: {}", buf[0]))),
    }
}
