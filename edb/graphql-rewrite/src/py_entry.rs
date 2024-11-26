use pyo3::prelude::*;
use pyo3::types::{PyDict, PyInt, PyList, PyString, PyType};

use edb_graphql_parser::position::Pos;

use crate::py_token::{self, PyToken};
use crate::rewrite::{self, Value};

#[pyclass]
pub struct Entry {
    #[pyo3(get)]
    key: PyObject,
    #[pyo3(get)]
    key_vars: PyObject,
    #[pyo3(get)]
    variables: PyObject,
    #[pyo3(get)]
    substitutions: PyObject,
    _tokens: Vec<PyToken>,
    _end_pos: Pos,
}

#[pymethods]
impl Entry {
    fn tokens<'py>(&self, py: Python<'py>, kinds: PyObject) -> PyResult<impl IntoPyObject<'py>> {
        py_token::convert_tokens(py, &self._tokens, &self._end_pos, kinds)
    }
}

pub fn convert_entry(py: Python<'_>, entry: rewrite::Entry) -> PyResult<Entry> {
    // import decimal
    let decimal_cls = PyModule::import(py, "decimal")?.getattr("Decimal")?;

    let vars = PyDict::new(py);
    let substitutions = PyDict::new(py);
    for (idx, var) in entry.variables.iter().enumerate() {
        let s = format!("_edb_arg__{}", idx).into_pyobject(py)?;

        vars.set_item(&s, value_to_py(py, &var.value, &decimal_cls)?)?;
        substitutions.set_item(
            s,
            (
                &var.token.value,
                var.token.position.map(|x| x.line),
                var.token.position.map(|x| x.column),
            ),
        )?;
    }
    for (name, var) in &entry.defaults {
        vars.set_item(name, value_to_py(py, &var.value, &decimal_cls)?)?
    }
    let key_vars = PyList::new(py, entry.key_vars)?;
    Ok(Entry {
        key: PyString::new(py, &entry.key).into(),
        key_vars: key_vars.into(),
        variables: vars.into_pyobject(py)?.into(),
        substitutions: substitutions.into(),
        _tokens: entry.tokens,
        _end_pos: entry.end_pos,
    })
}

fn value_to_py(py: Python, value: &Value, decimal_cls: &Bound<PyAny>) -> PyResult<PyObject> {
    let v = match value {
        Value::Str(ref v) => PyString::new(py, v).into_any(),
        Value::Int32(v) => v.into_pyobject(py)?.into_any(),
        Value::Int64(v) => v.into_pyobject(py)?.into_any(),
        Value::Decimal(v) => decimal_cls.call((v.as_str(),), None)?.into_any(),
        Value::BigInt(ref v) => PyType::new::<PyInt>(py)
            .call((v.as_str(),), None)?
            .into_any(),
        Value::Boolean(b) => b.into_pyobject(py)?.to_owned().into_any(),
    };
    Ok(v.into())
}
