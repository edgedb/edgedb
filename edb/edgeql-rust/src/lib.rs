#[macro_use] extern crate cpython;

use cpython::PyString;

mod tokenizer;
mod errors;
pub mod rewrite;
mod pyrewrite;

use errors::TokenizerError;
use tokenizer::{Token, tokenize, get_unpickle_fn};
use pyrewrite::rewrite;

py_module_initializer!(
    _edgeql_rust, init_edgeql_rust, PyInit__edgeql_rust,
    |py, m| {
        tokenizer::init_module(py);
        m.add(py, "__doc__", "Rust enhancements for edgeql language parser")?;

        m.add(py, "tokenize", py_fn!(py, tokenize(data: &PyString)))?;
        m.add(py, "_unpickle_token", get_unpickle_fn(py))?;
        m.add(py, "Token", py.get_type::<Token>())?;
        m.add(py, "TokenizerError", py.get_type::<TokenizerError>())?;
        m.add(py, "Entry", py.get_type::<pyrewrite::Entry>())?;
        m.add(py, "rewrite", py_fn!(py, rewrite(query: &PyString)))?;
        Ok(())
    });
