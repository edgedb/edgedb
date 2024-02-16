use once_cell::sync::OnceCell;

use edgeql_parser::parser;
use pyo3::exceptions::PyAssertionError;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyString, PyTuple};

use crate::errors::{parser_error_into_tuple, ParserResult};
use crate::pynormalize::value_to_py_object;
use crate::tokenizer::OpaqueToken;

#[pyfunction]
pub fn parse(
    py: Python,
    start_token_name: &PyString,
    tokens: PyObject,
) -> PyResult<(ParserResult, PyObject)> {
    let start_token_name = start_token_name.to_string();

    let (spec, productions) = get_spec()?;

    let tokens = downcast_tokens(py, &start_token_name, tokens)?;

    let context = parser::Context::new(spec);
    let (cst, errors) = parser::parse(&tokens, &context);

    let cst = cst.map(|c| to_py_cst(c, py)).transpose()?;

    let errors = errors
        .into_iter()
        .map(|e| parser_error_into_tuple(py, e))
        .collect::<Vec<_>>();
    let errors = PyList::new(py, &errors);

    let res = ParserResult {
        out: cst.into_py(py),
        errors: errors.into(),
    };

    Ok((res, productions.clone()))
}

#[pyclass]
pub struct CSTNode {
    #[pyo3(get)]
    production: PyObject,
    #[pyo3(get)]
    terminal: PyObject,
}

#[pyclass]
pub struct Production {
    #[pyo3(get)]
    id: usize,
    #[pyo3(get)]
    args: PyObject,
}

#[pyclass]
pub struct Terminal {
    #[pyo3(get)]
    text: String,
    #[pyo3(get)]
    value: PyObject,
    #[pyo3(get)]
    start: u64,
    #[pyo3(get)]
    end: u64,
}

static PARSER_SPECS: OnceCell<(parser::Spec, PyObject)> = OnceCell::new();

fn downcast_tokens(
    py: Python,
    start_token_name: &str,
    token_list: PyObject,
) -> PyResult<Vec<parser::Terminal>> {
    let tokens: &PyList = token_list.downcast(py)?;

    let mut buf = Vec::with_capacity(tokens.len() + 1);
    buf.push(parser::Terminal::from_start_name(start_token_name));
    for token in tokens.iter() {
        let token: &PyCell<OpaqueToken> = token.downcast()?;
        let token = token.borrow().inner.clone();

        buf.push(parser::Terminal::from_token(token));
    }

    // adjust the span of the starting token for nicer error message spans
    if buf.len() >= 2 {
        buf[0].span.start = buf[1].span.start;
        buf[0].span.end = buf[1].span.start;
    }

    Ok(buf)
}

fn get_spec() -> PyResult<&'static (parser::Spec, PyObject)> {
    if let Some(x) = PARSER_SPECS.get() {
        Ok(x)
    } else {
        Err(PyAssertionError::new_err(("grammar spec not loaded",)))
    }
}

/// Loads the grammar specification from file and caches it in memory.
#[pyfunction]
pub fn preload_spec(py: Python, spec_filepath: &PyString) -> PyResult<()> {
    if PARSER_SPECS.get().is_some() {
        return Ok(());
    }

    let spec_filepath = spec_filepath.to_string();
    let bytes = std::fs::read(&spec_filepath)
        .unwrap_or_else(|e| panic!("Cannot read grammar spec from {spec_filepath} ({e})"));

    let spec: parser::Spec = bitcode::deserialize::<parser::SpecSerializable>(&bytes)
        .unwrap()
        .into();
    let productions = load_productions(py, &spec)?;

    let _ = PARSER_SPECS.set((spec, productions));
    Ok(())
}

/// Serialize the grammar specification and write it to a file.
///
/// Called from setup.py.
#[pyfunction]
pub fn save_spec(spec_json: &PyString, dst: &PyString) -> PyResult<()> {
    let spec_json = spec_json.to_string();
    let spec: parser::SpecSerializable = serde_json::from_str(&spec_json).unwrap();
    let spec_bitcode = bitcode::serialize(&spec).unwrap();

    let dst = dst.to_string();

    std::fs::write(dst, spec_bitcode).ok().unwrap();
    Ok(())
}

fn load_productions(py: Python<'_>, spec: &parser::Spec) -> PyResult<PyObject> {
    let grammar_name = "edb.edgeql.parser.grammar.start";
    let grammar_mod = py.import(grammar_name)?;
    let load_productions = py
        .import("edb.common.parsing")?
        .getattr("load_spec_productions")?;

    let production_names: Vec<_> = spec
        .production_names
        .iter()
        .map(|(a, b)| PyTuple::new(py, [a, b]))
        .collect();

    let productions = load_productions.call((production_names, grammar_mod), None)?;
    Ok(productions.into())
}

fn to_py_cst<'a>(cst: &'a parser::CSTNode<'a>, py: Python) -> PyResult<CSTNode> {
    Ok(match cst {
        parser::CSTNode::Empty => CSTNode {
            production: py.None(),
            terminal: py.None(),
        },
        parser::CSTNode::Terminal(token) => CSTNode {
            production: py.None(),
            terminal: Terminal {
                text: token.text.clone(),
                value: if let Some(val) = &token.value {
                    value_to_py_object(py, val)?
                } else {
                    py.None()
                },
                start: token.span.start,
                end: token.span.end,
            }
            .into_py(py),
        },
        parser::CSTNode::Production(prod) => CSTNode {
            production: Production {
                id: prod.id,
                args: PyList::new(
                    py,
                    prod.args
                        .iter()
                        .map(|a| to_py_cst(a, py).map(|x| x.into_py(py)))
                        .collect::<PyResult<Vec<_>>>()?
                        .as_slice(),
                )
                .into(),
            }
            .into_py(py),
            terminal: py.None(),
        },
    })
}
