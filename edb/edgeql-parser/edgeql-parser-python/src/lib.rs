#![cfg(feature = "python_extension")]
mod errors;
mod hash;
mod keywords;
pub mod normalize;
mod parser;
mod position;
mod pynormalize;
mod tokenizer;
mod unpack;

use pyo3::prelude::*;

/// Rust bindings to the edgeql-parser crate
#[pymodule]
fn _edgeql_parser(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add("SyntaxError", py.get_type::<errors::SyntaxError>())?;
    m.add("ParserResult", py.get_type::<errors::ParserResult>())?;

    m.add_class::<hash::Hasher>()?;

    let keywords = keywords::get_keywords(py)?;
    m.add("unreserved_keywords", keywords.unreserved)?;
    m.add("partial_reserved_keywords", keywords.partial)?;
    m.add("future_reserved_keywords", keywords.future)?;
    m.add("current_reserved_keywords", keywords.current)?;

    m.add_class::<pynormalize::Entry>()?;
    m.add_function(wrap_pyfunction!(pynormalize::normalize, m)?)?;

    m.add_function(wrap_pyfunction!(parser::parse, m)?)?;
    m.add_function(wrap_pyfunction!(parser::preload_spec, m)?)?;
    m.add_function(wrap_pyfunction!(parser::save_spec, m)?)?;
    m.add_class::<parser::CSTNode>()?;
    m.add_class::<parser::Production>()?;
    m.add_class::<parser::Terminal>()?;

    m.add_function(wrap_pyfunction!(position::offset_of_line, m)?)?;
    m.add("SourcePoint", py.get_type::<position::SourcePoint>())?;

    m.add_class::<tokenizer::OpaqueToken>()?;
    m.add_function(wrap_pyfunction!(tokenizer::tokenize, m)?)?;
    m.add_function(wrap_pyfunction!(tokenizer::unpickle_token, m)?)?;

    m.add_function(wrap_pyfunction!(unpack::unpack, m)?)?;

    tokenizer::fini_module(m);

    Ok(())
}
