use cpython::{Python, PyString, PyResult, PyClone, PyDict, ToPyObject};

use crate::pyerrors::{LexingError, SyntaxError, NotFoundError, AssertionError};
use crate::entry_point::{Variable, Error};
use crate::pytoken::PyToken;
use crate::entry_point;


py_class!(pub class Entry |py| {
    data _key: PyString;
    data _variables: PyDict;
    data _tokens: Vec<PyToken>;
    def key(&self) -> PyResult<PyString> {
        Ok(self._key(py).clone_ref(py))
    }
    def variables(&self) -> PyResult<PyDict> {
        Ok(self._variables(py).clone_ref(py))
    }
});

fn init_module(_py: Python<'_>) {
}

fn rewrite(py: Python<'_>, operation: Option<&PyString>, text: &PyString)
    -> PyResult<Entry>
{
    let oper = operation.map(|x| x.to_string(py)).transpose()?;
    let text = text.to_string(py)?;
    match entry_point::rewrite(oper.as_ref().map(|x| &x[..]), &text) {
        Ok(entry) => {
            let vars = PyDict::new(py);
            for (idx, var) in entry.variables.iter().enumerate() {
                vars.set_item(py,
                    format!("_edb_arg__{}", idx).to_py_object(py),
                    match var {
                        Variable::Str(s) => PyString::new(py, s),
                        _ => todo!(),
                    })?;
            }
            // TODO(tailhook) insert defaults
            Entry::create_instance(py,
                PyString::new(py, &entry.key),
                vars,
                entry.tokens,
            )
        }
        Err(Error::Lexing(e)) => Err(LexingError::new(py, e.to_string())),
        Err(Error::Syntax(e)) => Err(SyntaxError::new(py, e.to_string())),
        Err(Error::NotFound(e)) => Err(NotFoundError::new(py, e.to_string())),
        Err(Error::Assertion(e))
        => Err(AssertionError::new(py, e.to_string())),
    }
}

py_module_initializer!(
    _graphql_rewrite, init_graphql_rewrite, PyInit__graphql_rewrite,
    |py, m| {
        init_module(py);
        m.add(py, "__doc__", "Rust optimizer for graphql queries")?;
        m.add(py, "rewrite",
            py_fn!(py, rewrite(option: Option<&PyString>, data: &PyString)))?;
        m.add(py, "Entry", py.get_type::<Entry>())?;
        m.add(py, "LexingError", py.get_type::<LexingError>())?;
        m.add(py, "SyntaxError", py.get_type::<SyntaxError>())?;
        m.add(py, "NotFoundError", py.get_type::<NotFoundError>())?;
        m.add(py, "AssertionError", py.get_type::<AssertionError>())?;
        Ok(())
    });
