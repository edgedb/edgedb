#[macro_use]
extern crate cpython;

use cpython::PyString;

mod errors;
mod float;
mod hash;
mod keywords;
pub mod normalize;
mod position;
mod pynormalize;
mod tokenizer;

use edgeql_parser::{into_python::IntoPython, parser};
use errors::TokenizerError;
use position::{offset_of_line, SourcePoint};
use pynormalize::normalize;
use tokenizer::{get_unpickle_fn, tokenize, Token};

py_module_initializer!(
    _edgeql_rust,
    init_edgeql_rust,
    PyInit__edgeql_rust,
    |py, m| {
        tokenizer::init_module(py);
        let keywords = keywords::get_keywords(py)?;
        m.add(
            py,
            "__doc__",
            "Rust enhancements for edgeql language parser",
        )?;

        m.add(py, "tokenize", py_fn!(py, tokenize(data: &PyString)))?;
        m.add(py, "_unpickle_token", get_unpickle_fn(py))?;
        m.add(py, "Token", py.get_type::<Token>())?;
        m.add(py, "TokenizerError", py.get_type::<TokenizerError>())?;
        m.add(py, "Entry", py.get_type::<pynormalize::Entry>())?;
        m.add(py, "SourcePoint", py.get_type::<SourcePoint>())?;
        m.add(py, "normalize", py_fn!(py, normalize(query: &PyString)))?;
        m.add(
            py,
            "offset_of_line",
            py_fn!(py, offset_of_line(text: &str, line: usize)),
        )?;
        m.add(py, "Hasher", py.get_type::<hash::Hasher>())?;
        m.add(py, "unreserved_keywords", keywords.unreserved)?;
        m.add(py, "partial_reserved_keywords", keywords.partial)?;
        m.add(py, "future_reserved_keywords", keywords.future)?;
        m.add(py, "current_reserved_keywords", keywords.current)?;
        m.add(py, "parse", py_fn!(py, parse(data: &PyString)))?;
        Ok(())
    }
);

fn parse(py: cpython::Python, source: &PyString) -> cpython::PyResult<cpython::PyObject> {
    let source = source.to_string(py)?;

    // parse
    let ast_root = parser::parse(&source);

    // convert into a Python object
    ast_root.into_python(py, None)
}
