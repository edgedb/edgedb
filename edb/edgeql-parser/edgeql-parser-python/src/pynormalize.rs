use std::convert::TryFrom;

use bigdecimal::Num;

use bytes::{BufMut, Bytes, BytesMut};
use edgedb_protocol::codec;
use edgedb_protocol::model::{BigInt, Decimal};
use edgeql_parser::tokenizer::Value;
use pyo3::exceptions::PyAssertionError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyFloat, PyList, PyLong, PyString};

use crate::errors::SyntaxError;
use crate::normalize::{normalize as _normalize, Error, Variable};
use crate::tokenizer::tokens_to_py;

#[pyfunction]
pub fn normalize(py: Python<'_>, text: &PyString) -> PyResult<Entry> {
    let text = text.to_string();
    match _normalize(&text) {
        Ok(entry) => {
            let blobs =
                serialize_all(py, &entry.variables).map_err(PyAssertionError::new_err)?;
            let counts: Vec<_> = entry
                .variables
                .iter()
                .map(|x| x.len().into_py(py))
                .collect();

            Ok(Entry {
                key: PyBytes::new(py, &entry.hash[..]).into(),
                tokens: tokens_to_py(py, entry.tokens)?,
                extra_blobs: blobs.into(),
                extra_named: entry.named_args,
                first_extra: entry.first_arg,
                extra_counts: PyList::new(py, &counts[..]).into(),
                variables: entry.variables,
            })
        }
        Err(Error::Tokenizer(msg, pos)) => {
            Err(SyntaxError::new_err((
                msg,
                (pos, py.None()),
                py.None(),
                py.None(),
            )))
        }
        Err(Error::Assertion(msg, pos)) => {
            Err(PyAssertionError::new_err(format!("{}: {}", pos, msg)))
        }
    }
}

#[pyclass]
pub struct Entry {
    #[pyo3(get)]
    key: PyObject,

    #[pyo3(get)]
    tokens: PyObject,

    #[pyo3(get)]
    extra_blobs: PyObject,

    extra_named: bool,

    #[pyo3(get)]
    first_extra: Option<usize>,

    #[pyo3(get)]
    extra_counts: PyObject,

    variables: Vec<Vec<Variable>>,
}

#[pymethods]
impl Entry {
    fn get_variables(&self, py: Python) -> PyResult<PyObject> {
        let vars = PyDict::new(py);
        let first = match self.first_extra {
            Some(first) => first,
            None => return Ok(vars.to_object(py)),
        };
        for (idx, var) in self.variables.iter().flatten().enumerate() {
            let s = if self.extra_named {
                format!("__edb_arg_{}", first + idx)
            } else {
                (first + idx).to_string()
            };
            vars.set_item(s.into_py(py), value_to_py_object(py, &var.value)?)?;
        }

        Ok(vars.to_object(py))
    }
}

pub fn serialize_extra(variables: &[Variable]) -> Result<Bytes, String> {
    use edgedb_protocol::codec::Codec;
    use edgedb_protocol::value::Value as P;

    let mut buf = BytesMut::new();
    buf.reserve(4 * variables.len());
    for var in variables {
        buf.reserve(4);
        let pos = buf.len();
        buf.put_u32(0); // replaced after serializing a value
        match var.value {
            Value::Int(v) => {
                codec::Int64
                    .encode(&mut buf, &P::Int64(v))
                    .map_err(|e| format!("int cannot be encoded: {}", e))?;
            }
            Value::String(ref v) => {
                codec::Str
                    .encode(&mut buf, &P::Str(v.clone()))
                    .map_err(|e| format!("str cannot be encoded: {}", e))?;
            }
            Value::Float(ref v) => {
                codec::Float64
                    .encode(&mut buf, &P::Float64(v.clone()))
                    .map_err(|e| format!("float cannot be encoded: {}", e))?;
            }
            Value::BigInt(ref v) => {
                // We have two different versions of BigInt implementations here.
                // We have to use bigdecimal::num_bigint::BigInt because it can parse with radix 16.

                let val = bigdecimal::num_bigint::BigInt::from_str_radix(v, 16)
                    .map_err(|e| format!("bigint cannot be encoded: {}", e))
                    .and_then(|x| {
                        BigInt::try_from(x).map_err(|e| format!("bigint cannot be encoded: {}", e))
                    })?;

                codec::BigInt
                    .encode(&mut buf, &P::BigInt(val))
                    .map_err(|e| format!("bigint cannot be encoded: {}", e))?;
            }
            Value::Decimal(ref v) => {
                let val = Decimal::try_from(v.clone())
                    .map_err(|e| format!("decimal cannot be encoded: {}", e))?;
                codec::Decimal
                    .encode(&mut buf, &P::Decimal(val))
                    .map_err(|e| format!("decimal cannot be encoded: {}", e))?;
            }
            Value::Bytes(_) => {
                // bytes literals should not be extracted during normalization
                unreachable!()
            }
        }
        let len = buf.len() - pos - 4;
        buf[pos..pos + 4].copy_from_slice(
            &u32::try_from(len)
                .map_err(|_| "element isn't too long".to_owned())?
                .to_be_bytes(),
        );
    }
    Ok(buf.freeze())
}

pub fn serialize_all<'a>(
    py: Python<'a>,
    variables: &[Vec<Variable>],
) -> Result<&'a PyList, String> {
    let mut buf = Vec::with_capacity(variables.len());
    for vars in variables {
        let bytes = serialize_extra(vars)?;
        let pybytes = PyBytes::new(py, &bytes).as_ref();
        buf.push(pybytes);
    }
    Ok(PyList::new(py, buf.as_slice()))
}

pub fn value_to_py_object(py: Python, val: &Value) -> PyResult<PyObject> {
    Ok(match val {
        Value::Int(v) => v.into_py(py),
        Value::String(v) => v.into_py(py),
        Value::Float(v) => v.into_py(py),
        Value::BigInt(v) => py
            .get_type::<PyLong>()
            .call((v, 16.into_py(py)), None)?
            .into(),
        Value::Decimal(v) => py
            .get_type::<PyFloat>()
            .call((v.to_string(),), None)?
            .into(),
        Value::Bytes(v) => PyBytes::new(py, v).into(),
    })
}
