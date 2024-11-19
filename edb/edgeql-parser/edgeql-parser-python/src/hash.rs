use std::sync::RwLock;

use edgeql_parser::hash;
use pyo3::{exceptions::PyRuntimeError, prelude::*, types::PyString};

use crate::errors::SyntaxError;

#[pyclass]
pub struct Hasher {
    _hasher: RwLock<Option<hash::Hasher>>,
}

#[pymethods]
impl Hasher {
    #[staticmethod]
    fn start_migration(parent_id: &Bound<PyString>) -> PyResult<Hasher> {
        let hasher = hash::Hasher::start_migration(parent_id.to_str()?);
        Ok(Hasher {
            _hasher: RwLock::new(Some(hasher)),
        })
    }

    fn add_source(&self, py: Python, data: &Bound<PyString>) -> PyResult<PyObject> {
        let text = data.to_str()?;
        let mut cell = self._hasher.write().unwrap();
        let hasher = cell
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err(("cannot add source after finish",)))?;

        hasher.add_source(text).map_err(|e| match e {
            hash::Error::Tokenizer(msg, pos) => {
                SyntaxError::new_err((msg, (pos.offset, py.None()), py.None(), py.None()))
            }
        })?;
        Ok(py.None())
    }

    fn make_migration_id(&self) -> PyResult<String> {
        let mut cell = self._hasher.write().unwrap();
        let hasher = cell
            .take()
            .ok_or_else(|| PyRuntimeError::new_err(("cannot do migration id twice",)))?;
        Ok(hasher.make_migration_id())
    }
}
