#[macro_use]
extern crate cpython;

use cpython::{PyObject, PyString};

mod errors;
mod hash;
mod keywords;
pub mod normalize;
mod parser;
mod position;
mod pynormalize;
mod tokenizer;

use errors::{SyntaxError, ParserResult};
use parser::{parse, CSTNode, Production};
use position::{offset_of_line, SourcePoint};
use pynormalize::normalize;
use tokenizer::{get_fn_unpickle_token, tokenize, OpaqueToken};

py_module_initializer!(
    _edgeql_parser,
    init_edgeql_parser,
    PyInit__edgeql_parser,
    |py, m| {
        tokenizer::init_module(py);
        let keywords = keywords::get_keywords(py)?;
        m.add(
            py,
            "__doc__",
            "Rust enhancements for edgeql language parser",
        )?;

        m.add(py, "tokenize", py_fn!(py, tokenize(data: &PyString)))?;
        m.add(py, "_unpickle_token", get_fn_unpickle_token(py))?;
        m.add(py, "Token", py.get_type::<OpaqueToken>())?;
        m.add(py, "SyntaxError", py.get_type::<SyntaxError>())?;
        m.add(py, "ParserResult", py.get_type::<ParserResult>())?;
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
        m.add(
            py,
            "parse",
            py_fn!(py, parse(parser_name: &PyString, data: PyObject)),
        )?;
        m.add(py, "CSTNode", py.get_type::<CSTNode>())?;
        m.add(py, "Production", py.get_type::<Production>())?;
        Ok(())
    }
);
