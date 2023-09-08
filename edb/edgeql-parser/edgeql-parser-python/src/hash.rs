use std::cell::RefCell;

use cpython::{PyErr, PyString, PyResult, PyObject};
use cpython::exc::RuntimeError;

use edgeql_parser::hash;

use crate::errors::SyntaxError;


py_class!(pub class Hasher |py| {
    data _hasher: RefCell<Option<hash::Hasher>>;
    @staticmethod
    def start_migration(parent_id: &PyString) -> PyResult<Hasher> {
        let hasher = hash::Hasher::start_migration(&parent_id.to_string(py)?);
        Hasher::create_instance(py, RefCell::new(Some(hasher)))
    }
    def add_source(&self, data: &PyString) -> PyResult<PyObject> {
        let text = data.to_string(py)?;
        let mut cell = self._hasher(py).borrow_mut();
        let hasher = cell.as_mut().ok_or_else(|| {
            PyErr::new::<RuntimeError, _>(py,
                ("cannot add source after finish",))
        })?;
        hasher.add_source(&text)
            .map_err(|e| match e {
                hash::Error::Tokenizer(msg, pos) => {
                    SyntaxError::new(py, (msg, (pos.offset, py.None())))
                }
            })?;
        Ok(py.None())
    }
    def make_migration_id(&self) -> PyResult<String> {
        let mut cell = self._hasher(py).borrow_mut();
        let hasher = cell.take().ok_or_else(|| {
            PyErr::new::<RuntimeError, _>(py,
                ("cannot do migration id twice",))
        })?;
        Ok(hasher.make_migration_id())
    }
});
