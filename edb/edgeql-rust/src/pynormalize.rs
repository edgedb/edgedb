use std::convert::{TryFrom};

use cpython::{Python, PyClone, PyDict, PyList, PyString, PyResult};
use cpython::{PyTuple, PyInt, ToPyObject, PythonObject, PyBytes, PyErr};
use cpython::{PyFloat};
use cpython::exc::AssertionError;

use bytes::{BytesMut, Bytes, BufMut};
use edgeql_parser::position::Pos;
use edgedb_protocol::codec;
use edgedb_protocol::value::{BigInt, Decimal};

use crate::errors::TokenizerError;
use crate::normalize::{Error, Value, Variable, normalize as _normalize};
use crate::tokenizer::convert_tokens;


py_class!(pub class Entry |py| {
    data _key: PyString;
    data _tokens: PyList;
    data _extra_blob: PyBytes;
    data _extra_named: bool;
    data _first_extra: Option<usize>;
    data _extra_count: usize;
    data _variables: Vec<Variable>;
    def key(&self) -> PyResult<PyString> {
        Ok(self._key(py).clone_ref(py))
    }
    def variables(&self) -> PyResult<PyDict> {
        let vars = PyDict::new(py);
        let named = *self._extra_named(py);
        let first = match self._first_extra(py) {
            Some(first) => first,
            None => return Ok(vars),
        };
        for (idx, var) in self._variables(py).iter().enumerate() {
            let s = if named {
                format!("__edb_arg_{}", first + idx)
            } else {
                (first + idx).to_string()
            };
            vars.set_item(py, s.to_py_object(py),
                match var.value {
                    Value::Int(ref v) => v.to_py_object(py).into_object(),
                    Value::Str(ref v) => v.to_py_object(py).into_object(),
                    Value::Float(ref v) => v.to_py_object(py).into_object(),
                    Value::BigInt(ref v) => {
                        py.get_type::<PyInt>()
                        .call(py,
                            PyTuple::new(py, &[
                                v.to_string().to_py_object(py).into_object(),
                            ]),
                            None)?
                    }
                    Value::Decimal(ref v) => {
                        py.get_type::<PyFloat>()
                        .call(py,
                            PyTuple::new(py, &[
                                v.to_string().to_py_object(py).into_object(),
                            ]),
                            None)?
                    }
                })?;
        }
        Ok(vars)
    }
    def tokens(&self) -> PyResult<PyList> {
        Ok(self._tokens(py).clone_ref(py))
    }
    def first_extra(&self) -> PyResult<Option<PyInt>> {
        Ok(self._first_extra(py).map(|x| x.to_py_object(py)))
    }
    def extra_count(&self) -> PyResult<PyInt> {
        Ok(self._extra_count(py).to_py_object(py))
    }
    def extra_blob(&self) -> PyResult<PyBytes> {
        Ok(self._extra_blob(py).clone_ref(py))
    }
});


pub fn py_pos(py: Python, pos: &Pos) -> PyTuple {
    (pos.line, pos.column, pos.offset).to_py_object(py)
}

pub fn serialize_extra(variables: &[Variable]) -> Result<Bytes, String> {
    use edgedb_protocol::value::Value as P;
    use edgedb_protocol::codec::Codec;

    let mut buf = BytesMut::new();
    buf.reserve(4*variables.len());
    for var in variables {
        buf.reserve(4);
        let pos = buf.len();
        buf.put_u32(0);  // replaced after serializing a value
        match var.value {
            Value::Int(v) => {
                codec::Int64.encode(&mut buf, &P::Int64(v))
                    .map_err(|e| format!("int cannot be encoded: {}", e))?;
            }
            Value::Str(ref v) => {
                codec::Str.encode(&mut buf, &P::Str(v.clone()))
                    .map_err(|e| format!("str cannot be encoded: {}", e))?;
            }
            Value::Float(ref v) => {
                codec::Float64.encode(&mut buf, &P::Float64(v.clone()))
                    .map_err(|e| format!("float cannot be encoded: {}", e))?;
            }
            Value::BigInt(ref v) => {
                let val = BigInt::try_from(v.clone())
                    .map_err(|e| format!("bigint cannot be encoded: {}", e))?;
                codec::BigInt.encode(&mut buf, &P::BigInt(val))
                    .map_err(|e| format!("bigint cannot be encoded: {}", e))?;
            }
            Value::Decimal(ref v) => {
                let val = Decimal::try_from(v.clone())
                    .map_err(|e| format!("decimal cannot be encoded: {}", e))?;
                codec::Decimal.encode(&mut buf, &P::Decimal(val))
                    .map_err(|e| format!("decimal cannot be encoded: {}", e))?;
            }
        }
        let len = buf.len()-pos-4;
        buf[pos..pos+4].copy_from_slice(&u32::try_from(len)
                .map_err(|_| "element isn't too long".to_owned())?
                .to_be_bytes());
    }
    Ok(buf.freeze())
}

pub fn normalize(py: Python<'_>, text: &PyString)
    -> PyResult<Entry>
{
    let text = text.to_string(py)?;
    match _normalize(&text) {
        Ok(entry) => {
            let blob = serialize_extra(&entry.variables)
                .map_err(|e| PyErr::new::<AssertionError, _>(py, e))?;

            Ok(Entry::create_instance(py,
                /* key: */ entry.key.to_py_object(py),
                /* tokens: */ convert_tokens(py, entry.tokens, entry.end_pos)?,
                /* extra_blob: */ PyBytes::new(py, &blob),
                /* extra_named: */ entry.named_args,
                /* first_extra: */ entry.first_arg,
                /* extra_count: */ entry.variables.len(),
                /* variables: */ entry.variables,
            )?)
        }
        Err(Error::Tokenizer(msg, pos)) => {
            return Err(TokenizerError::new(py, (msg, py_pos(py, &pos))))
        }
    }
}
