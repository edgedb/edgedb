use cpython::{PyBytes, PyClone, PyResult, PyString, Python, PythonObject};
use cpython::{PyList, PyObject, PyTuple, ToPyObject};

use edgeql_parser::tokenizer::{Token, Tokenizer};
use once_cell::sync::OnceCell;

use crate::errors::{parser_error_into_tuple, ParserResult};

pub fn tokenize(py: Python, s: &PyString) -> PyResult<ParserResult> {
    let data = s.to_string(py)?;

    let mut token_stream = Tokenizer::new(&data[..]).validated_values().with_eof();

    let mut tokens: Vec<_> = Vec::new();
    let mut errors: Vec<_> = Vec::new();

    for res in &mut token_stream {
        match res {
            Ok(token) => tokens.push(token),
            Err(e) => {
                errors.push(parser_error_into_tuple(py, e));

                // TODO: fix tokenizer to skip bad tokens and continue
                break;
            }
        }
    }

    let tokens = tokens_to_py(py, tokens)?;

    let errors = PyList::new(py, errors.as_slice()).to_py_object(py);

    ParserResult::create_instance(py, tokens.into_object(), errors)
}

// An opaque wrapper around [edgeql_parser::tokenizer::Token].
// Supports Python pickle serialization.
py_class!(pub class OpaqueToken |py| {
    data _inner: Token<'static>;

    def __repr__(&self) -> PyResult<PyString> {
        Ok(PyString::new(py, &self._inner(py).to_string()))
    }
    def __reduce__(&self) -> PyResult<PyTuple> {
        let data = bitcode::serialize(self._inner(py)).unwrap();

        return Ok((
            get_fn_unpickle_token(py),
            (
                PyBytes::new(py, &data),
            ),
        ).to_py_object(py))
    }
});

pub fn tokens_to_py(py: Python, rust_tokens: Vec<Token>) -> PyResult<PyList> {
    let mut buf = Vec::with_capacity(rust_tokens.len());
    for tok in rust_tokens {
        let py_tok = OpaqueToken::create_instance(py, tok.cloned())?.into_object();

        buf.push(py_tok);
    }
    Ok(PyList::new(py, &buf[..]))
}

/// To support pickle serialization of OpaqueTokens, we need to provide a
/// deserialization function in __reduce__ methods.
/// This function must not be inlined and must be globally accessible.
/// To achieve this, we expose it a part of the module definition
/// (`_unpickle_token`) and save reference to is in the `FN_UNPICKLE_TOKEN`.
///
/// A bit hackly, but it works.
static FN_UNPICKLE_TOKEN: OnceCell<PyObject> = OnceCell::new();

pub fn init_module(py: Python) {
    FN_UNPICKLE_TOKEN
        .set(py_fn!(py, _unpickle_token(bytes: &PyBytes)))
        .expect("module is already initialized");
}

pub fn _unpickle_token(py: Python, bytes: &PyBytes) -> PyResult<OpaqueToken> {
    let token = bitcode::deserialize(bytes.data(py)).unwrap();
    OpaqueToken::create_instance(py, token)
}

pub fn get_fn_unpickle_token(py: Python) -> PyObject {
    let py_function = FN_UNPICKLE_TOKEN.get().expect("module initialized");
    return py_function.clone_ref(py);
}

impl OpaqueToken {
    pub(super) fn inner(&self, py: Python) -> Token {
        self._inner(py).clone()
    }
}
