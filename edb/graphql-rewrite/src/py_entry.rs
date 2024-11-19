use pyo3::prelude::*;
use pyo3::types::{PyDict, PyInt, PyList, PyString, PyTuple, PyType};

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
    fn tokens(&self, py: Python, kinds: PyObject) -> PyResult<PyObject> {
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
        vars.set_item(
            name.into_pyobject(py)?,
            value_to_py(py, &var.value, &decimal_cls)?,
        )?
    }
    let key_vars = PyList::new(
        py,
        entry
            .key_vars
            .iter()
            .map(|v| v.into_pyobject(py))
            .collect::<Result<Vec<_>, _>>(),
    )?;
    Ok(Entry {
        key: PyString::new(py, &entry.key).into(),
        key_vars: key_vars.into(),
        variables: vars.into_pyobject(py)?.unbind().into_any(),
        substitutions: substitutions.into(),
        _tokens: entry.tokens,
        _end_pos: entry.end_pos,
    })
}

fn value_to_py(py: Python, value: &Value, decimal_cls: &Bound<PyAny>) -> PyResult<PyObject> {
    let v = match value {
        Value::Str(ref v) => PyString::new(py, v).into(),
        Value::Int32(v) => v.into_pyobject(py)?.to_owned().into(),
        Value::Int64(v) => v.into_pyobject(py)?.to_owned().into(),
        Value::Decimal(v) => decimal_cls
            .call(PyTuple::new(py, &[v.into_pyobject(py)?])?, None)?
            .into(),
        Value::BigInt(ref v) => PyType::new::<PyInt>(py)
            .call(PyTuple::new(py, &[v.into_pyobject(py)?])?, None)?
            .into(),
        Value::Boolean(b) => b.into_pyobject(py)?.to_owned().into(),
    };
    Ok(v)
}
