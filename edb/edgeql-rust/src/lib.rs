#[macro_use] extern crate cpython;

use cpython::PyString;

mod errors;
mod keywords;
mod tokenizer;
pub mod normalize;
mod pynormalize;

use errors::TokenizerError;
use tokenizer::{Token, tokenize, get_unpickle_fn};
use pynormalize::normalize;


py_module_initializer!(
    _edgeql_rust, init_edgeql_rust, PyInit__edgeql_rust,
    |py, m| {
        tokenizer::init_module(py);
        let keywords = keywords::get_keywords(py)?;
        m.add(py, "__doc__", "Rust enhancements for edgeql language parser")?;

        m.add(py, "tokenize", py_fn!(py, tokenize(data: &PyString)))?;
        m.add(py, "_unpickle_token", get_unpickle_fn(py))?;
        m.add(py, "Token", py.get_type::<Token>())?;
        m.add(py, "TokenizerError", py.get_type::<TokenizerError>())?;
        m.add(py, "Entry", py.get_type::<pynormalize::Entry>())?;
        m.add(py, "normalize", py_fn!(py, normalize(query: &PyString)))?;
        m.add(py, "unreserved_keywords", keywords.unreserved)?;
        m.add(py, "future_reserved_keywords", keywords.future)?;
        m.add(py, "current_reserved_keywords", keywords.current)?;
        Ok(())
    });
