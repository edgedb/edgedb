use cpython::{Python, PyClone, PyDict, PyList, PyString, PyResult};

py_class!(pub class Entry |py| {
    data _key: PyString;
    data _variables: PyDict;
    data _tokens: PyList;
    def key(&self) -> PyResult<PyString> {
        Ok(self._key(py).clone_ref(py))
    }
    def variables(&self) -> PyResult<PyDict> {
        Ok(self._variables(py).clone_ref(py))
    }
    def tokens(&self) -> PyResult<PyList> {
        Ok(self._tokens(py).clone_ref(py))
    }
});


pub fn rewrite(py: Python<'_>, text: &PyString)
    -> PyResult<Entry>
{
    todo!();
}
