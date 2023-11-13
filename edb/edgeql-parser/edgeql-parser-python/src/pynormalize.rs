use std::convert::TryFrom;

use bigdecimal::Num;
use cpython::exc::AssertionError;
use cpython::{PyBytes, PyErr, PyInt, PythonObject, ToPyObject};
use cpython::{PyClone, PyDict, PyList, PyResult, PyString, Python};
use cpython::{PyFloat, PyObject};

use bytes::{BufMut, Bytes, BytesMut};
use edgedb_protocol::codec;
use edgedb_protocol::model::{BigInt, Decimal};
use edgeql_parser::tokenizer::Value;

use crate::errors::SyntaxError;
use crate::normalize::{normalize as _normalize, Error, Variable};
use crate::tokenizer::tokens_to_py;

py_class!(pub class Entry |py| {
    data _key: PyBytes;
    data _processed_source: String;
    data _tokens: PyList;
    data _extra_blobs: PyList;
    data _extra_named: bool;
    data _first_extra: Option<usize>;
    data _extra_counts: PyList;
    data _variables: Vec<Vec<Variable>>;
    def key(&self) -> PyResult<PyBytes> {
        Ok(self._key(py).clone_ref(py))
    }
    def variables(&self) -> PyResult<PyDict> {
        let vars = PyDict::new(py);
        let named = *self._extra_named(py);
        let first = match self._first_extra(py) {
            Some(first) => first,
            None => return Ok(vars),
        };
        for (idx, var) in self._variables(py).iter().flatten().enumerate() {
            let s = if named {
                format!("__edb_arg_{}", first + idx)
            } else {
                (first + idx).to_string()
            };
            vars.set_item(
                py, s.to_py_object(py), value_to_py_object(py, &var.value)?
            )?;
        }
        Ok(vars)
    }
    def tokens(&self) -> PyResult<PyList> {
        Ok(self._tokens(py).clone_ref(py))
    }
    def first_extra(&self) -> PyResult<Option<PyInt>> {
        Ok(self._first_extra(py).map(|x| x.to_py_object(py)))
    }
    def extra_counts(&self) -> PyResult<PyList> {
        Ok(self._extra_counts(py).to_py_object(py))
    }
    def extra_blobs(&self) -> PyResult<PyList> {
        Ok(self._extra_blobs(py).clone_ref(py))
    }
});

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

pub fn serialize_all(py: Python<'_>, variables: &[Vec<Variable>]) -> Result<PyList, String> {
    let mut buf = Vec::with_capacity(variables.len());
    for vars in variables {
        let bytes = serialize_extra(vars)?;
        let pybytes = PyBytes::new(py, &bytes).into_object();
        buf.push(pybytes);
    }
    Ok(PyList::new(py, &buf[..]))
}

pub fn normalize(py: Python<'_>, text: &PyString) -> PyResult<Entry> {
    let text = text.to_string(py)?;
    match _normalize(&text) {
        Ok(entry) => {
            let blobs = serialize_all(py, &entry.variables)
                .map_err(|e| PyErr::new::<AssertionError, _>(py, e))?;
            let counts: Vec<_> = entry
                .variables
                .iter()
                .map(|x| x.len().to_py_object(py).into_object())
                .collect();

            Ok(Entry::create_instance(
                py,
                /* key: */ PyBytes::new(py, &entry.hash[..]),
                /* processed_source: */ entry.processed_source,
                /* tokens: */ tokens_to_py(py, entry.tokens)?,
                /* extra_blobs: */ blobs,
                /* extra_named: */ entry.named_args,
                /* first_extra: */ entry.first_arg,
                /* extra_counts: */ PyList::new(py, &counts[..]),
                /* variables: */ entry.variables,
            )?)
        }
        Err(Error::Tokenizer(msg, pos)) => {
            return Err(SyntaxError::new(
                py,
                (msg, (pos, py.None()), py.None(), py.None()),
            ))
        }
        Err(Error::Assertion(msg, pos)) => {
            return Err(PyErr::new::<AssertionError, _>(
                py,
                format!("{}: {}", pos, msg),
            ));
        }
    }
}

pub fn value_to_py_object(py: Python, val: &Value) -> PyResult<PyObject> {
    Ok(match val {
        Value::Int(v) => v.to_py_object(py).into_object(),
        Value::String(v) => v.to_py_object(py).into_object(),
        Value::Float(v) => v.to_py_object(py).into_object(),
        Value::BigInt(v) => py
            .get_type::<PyInt>()
            .call(py, (v, 16.to_py_object(py)), None)?,
        Value::Decimal(v) => py.get_type::<PyFloat>().call(py, (v.to_string(),), None)?,
        Value::Bytes(v) => PyBytes::new(py, v).into_object(),
    })
}
