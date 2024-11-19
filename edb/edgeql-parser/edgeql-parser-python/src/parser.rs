use once_cell::sync::OnceCell;

use edgeql_parser::parser;
use pyo3::exceptions::{PyAssertionError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyList, PyString};

use crate::errors::{parser_error_into_tuple, ParserResult};
use crate::pynormalize::TokenizerValue;
use crate::tokenizer::OpaqueToken;

#[pyfunction]
pub fn parse(
    py: Python,
    start_token_name: &Bound<PyString>,
    tokens: PyObject,
) -> PyResult<(ParserResult, &'static Py<PyAny>)> {
    let start_token_name = start_token_name.to_string();

    let (spec, productions) = get_spec()?;

    let tokens = downcast_tokens(py, &start_token_name, tokens)?;

    let context = parser::Context::new(spec);
    let (cst, errors) = parser::parse(&tokens, &context);

    let errors = PyList::new(py, errors.iter().map(|e| parser_error_into_tuple(e)))?;

    let res = ParserResult {
        out: cst.as_ref().map(ParserCSTNode).into_pyobject(py)?.unbind(),
        errors: errors.into(),
    };

    Ok((res, productions))
}

#[pyclass]
pub struct CSTNode {
    #[pyo3(get)]
    production: Option<Py<Production>>,
    #[pyo3(get)]
    terminal: Option<Py<Terminal>>,
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
    let tokens = token_list.downcast_bound::<PyList>(py)?;

    let mut buf = Vec::with_capacity(tokens.len() + 1);
    buf.push(parser::Terminal::from_start_name(start_token_name));
    for token in tokens.iter() {
        let token: &Bound<OpaqueToken> = token.downcast()?;
        let token = token.borrow().inner.clone();

        buf.push(parser::Terminal::from_token(token));
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
pub fn preload_spec(py: Python, spec_filepath: &Bound<PyString>) -> PyResult<()> {
    if PARSER_SPECS.get().is_some() {
        return Ok(());
    }

    let spec_filepath = spec_filepath.to_string();
    let bytes = std::fs::read(&spec_filepath)
        .unwrap_or_else(|e| panic!("Cannot read grammar spec from {spec_filepath} ({e})"));

    let spec: parser::Spec = bincode::deserialize::<parser::SpecSerializable>(&bytes)
        .map_err(|e| PyValueError::new_err(format!("Bad spec: {e}")))?
        .into();
    let productions = load_productions(py, &spec)?;

    let _ = PARSER_SPECS.set((spec, productions));
    Ok(())
}

/// Serialize the grammar specification and write it to a file.
///
/// Called from setup.py.
#[pyfunction]
pub fn save_spec(spec_json: &Bound<PyString>, dst: &Bound<PyString>) -> PyResult<()> {
    let spec_json = spec_json.to_string();
    let spec: parser::SpecSerializable = serde_json::from_str(&spec_json)
        .map_err(|e| PyValueError::new_err(format!("Invalid JSON: {e}")))?;
    let spec_bitcode = bincode::serialize(&spec)
        .map_err(|e| PyValueError::new_err(format!("Failed to pack spec: {e}")))?;

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

    let productions = load_productions.call((&spec.production_names, grammar_mod), None)?;
    Ok(productions.into())
}

/// Newtype required to define a trait for a foreign type.
struct ParserCSTNode<'a>(&'a parser::CSTNode<'a>);

impl<'a, 'py> IntoPyObject<'py> for ParserCSTNode<'a> {
    type Target = CSTNode;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        let res = match self.0 {
            parser::CSTNode::Empty => CSTNode {
                production: None,
                terminal: None,
            },
            parser::CSTNode::Terminal(token) => CSTNode {
                production: None,
                terminal: Some(Py::new(
                    py,
                    Terminal {
                        text: token.text.clone(),
                        value: (token.value.as_ref())
                            .map(TokenizerValue)
                            .into_pyobject(py)?
                            .unbind(),
                        start: token.span.start,
                        end: token.span.end,
                    },
                )?),
            },
            parser::CSTNode::Production(prod) => CSTNode {
                production: Some(Py::new(
                    py,
                    Production {
                        id: prod.id,
                        args: PyList::new(py, prod.args.iter().map(ParserCSTNode))?.into(),
                    },
                )?),
                terminal: None,
            },
        };
        Ok(Py::new(py, res)?.bind(py).clone())
    }
}
