use cpython::{Python, PyClone, PyDict, PyList, PyString, PyResult};
use cpython::{PyTuple, PyInt, ToPyObject, PythonObject};

use edgeql_parser::position::Pos;

use crate::errors::TokenizerError;
use crate::rewrite::{Error, Value, rewrite as _rewrite};
use crate::tokenizer::convert_tokens;


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


pub fn py_pos(py: Python, pos: &Pos) -> PyTuple {
    (pos.line, pos.column, pos.offset).to_py_object(py)
}


pub fn rewrite(py: Python<'_>, text: &PyString)
    -> PyResult<Entry>
{
    let text = text.to_string(py)?;
    match _rewrite(&text) {
        Ok(entry) => {
            let vars = PyDict::new(py);
            for (idx, var) in entry.variables.iter().enumerate() {
                let s = format!("_edb_arg__{}", idx).to_py_object(py);
                vars.set_item(py, s.clone_ref(py),
                    match var.value {
                        Value::Int(ref v) => {
                            py.get_type::<PyInt>()
                            .call(py,
                                PyTuple::new(py, &[
                                    v.to_py_object(py).into_object(),
                                ]),
                                None)?
                        }
                        _ => todo!(),
                    })?;
            }
            Ok(Entry::create_instance(py,
                entry.key.to_py_object(py),
                vars,
                convert_tokens(py, entry.tokens, entry.end_pos)?,
            )?)
        }
        Err(Error::Tokenizer(msg, pos)) => {
            return Err(TokenizerError::new(py, (msg, py_pos(py, &pos))))
        }
    }
}
