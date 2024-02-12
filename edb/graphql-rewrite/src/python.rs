use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyLong, PyString, PyTuple, PyType};
use pyo3::{create_exception, exceptions};

use edb_graphql_parser::common::{unquote_block_string, unquote_string};
use edb_graphql_parser::position::Pos;

use crate::entry_point::{self, Error, Value};
use crate::pytoken::PyToken;

/// Rust optimizer for graphql queries
#[pymodule]
fn _graphql_rewrite(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(rewrite, m)?)?;
    m.add_class::<Entry>()?;
    m.add("LexingError", py.get_type::<LexingError>())?;
    m.add("SyntaxError", py.get_type::<SyntaxError>())?;
    m.add("NotFoundError", py.get_type::<NotFoundError>())?;
    m.add("AssertionError", py.get_type::<AssertionError>())?;
    m.add("QueryError", py.get_type::<QueryError>())?;
    Ok(())
}

#[pyo3::pyfunction]
#[pyo3(signature = (operation, text))]
fn rewrite(py: Python<'_>, operation: Option<&PyString>, text: &PyString) -> PyResult<Entry> {
    // import decimal
    let decimal_cls = PyModule::import(py, "decimal")?.getattr("Decimal")?;

    // convert args
    let operation = operation.map(|x| x.to_string());
    let text = text.to_string();

    match entry_point::rewrite(operation.as_ref().map(|x| &x[..]), &text) {
        Ok(entry) => {
            let vars = PyDict::new(py);
            let substitutions = PyDict::new(py);
            for (idx, var) in entry.variables.iter().enumerate() {
                let s = format!("_edb_arg__{}", idx).to_object(py);

                vars.set_item(s.clone_ref(py), value_to_py(py, &var.value, &decimal_cls)?)?;

                substitutions.set_item(
                    s.clone_ref(py),
                    (
                        &var.token.value,
                        var.token.position.map(|x| x.line),
                        var.token.position.map(|x| x.column),
                    ),
                )?;
            }
            for (name, var) in &entry.defaults {
                vars.set_item(name.into_py(py), value_to_py(py, &var.value, &decimal_cls)?)?
            }
            let key_vars = PyList::new(
                py,
                &entry
                    .key_vars
                    .iter()
                    .map(|v| v.into_py(py))
                    .collect::<Vec<_>>(),
            );
            dbg!(&operation);
            dbg!(&text);
            Ok(dbg!(Entry {
                key: PyString::new(py, &entry.key).into(),
                key_vars: key_vars.into(),
                variables: vars.into_py(py),
                substitutions: substitutions.into(),
                _tokens: entry.tokens,
                _end_pos: entry.end_pos,
            }))
        }
        Err(Error::Lexing(e)) => Err(LexingError::new_err(e)),
        Err(Error::Syntax(e)) => Err(SyntaxError::new_err(e.to_string())),
        Err(Error::NotFound(e)) => Err(NotFoundError::new_err(e)),
        Err(Error::Query(e)) => Err(QueryError::new_err(e)),
        Err(Error::Assertion(e)) => Err(AssertionError::new_err(e)),
    }
}

create_exception!(_graphql_rewrite, LexingError, exceptions::PyException);

create_exception!(_graphql_rewrite, SyntaxError, exceptions::PyException);

create_exception!(_graphql_rewrite, NotFoundError, exceptions::PyException);

create_exception!(_graphql_rewrite, AssertionError, exceptions::PyException);

create_exception!(_graphql_rewrite, QueryError, exceptions::PyException);

#[pyclass]
#[derive(Debug)]
struct Entry {
    #[pyo3(get)]
    key: PyObject,
    #[pyo3(get)]
    key_vars: PyObject,
    #[pyo3(get)]
    variables: PyObject,
    #[pyo3(get)]
    substitutions: PyObject,
    _tokens: Vec<PyToken>,
    _end_pos: Pos,
}

#[pymethods]
impl Entry {
    fn tokens(&self, py: Python, kinds: PyObject) -> PyResult<PyObject> {
        use crate::pytoken::PyTokenKind as K;

        let sof = kinds.getattr(py, "SOF")?;
        let eof = kinds.getattr(py, "EOF")?;
        let bang = kinds.getattr(py, "BANG")?;
        let bang_v: PyObject = "!".into_py(py);
        let dollar = kinds.getattr(py, "DOLLAR")?;
        let dollar_v: PyObject = "$".into_py(py);
        let paren_l = kinds.getattr(py, "PAREN_L")?;
        let paren_l_v: PyObject = "(".into_py(py);
        let paren_r = kinds.getattr(py, "PAREN_R")?;
        let paren_r_v: PyObject = ")".into_py(py);
        let spread = kinds.getattr(py, "SPREAD")?;
        let spread_v: PyObject = "...".into_py(py);
        let colon = kinds.getattr(py, "COLON")?;
        let colon_v: PyObject = ":".into_py(py);
        let equals = kinds.getattr(py, "EQUALS")?;
        let equals_v: PyObject = "=".into_py(py);
        let at = kinds.getattr(py, "AT")?;
        let at_v: PyObject = "@".into_py(py);
        let bracket_l = kinds.getattr(py, "BRACKET_L")?;
        let bracket_l_v: PyObject = "[".into_py(py);
        let bracket_r = kinds.getattr(py, "BRACKET_R")?;
        let bracket_r_v: PyObject = "]".into_py(py);
        let brace_l = kinds.getattr(py, "BRACE_L")?;
        let brace_l_v: PyObject = "{".into_py(py);
        let pipe = kinds.getattr(py, "PIPE")?;
        let pipe_v: PyObject = "|".into_py(py);
        let brace_r = kinds.getattr(py, "BRACE_R")?;
        let brace_r_v: PyObject = "}".into_py(py);
        let name = kinds.getattr(py, "NAME")?;
        let int = kinds.getattr(py, "INT")?;
        let float = kinds.getattr(py, "FLOAT")?;
        let string = kinds.getattr(py, "STRING")?;
        let block_string = kinds.getattr(py, "BLOCK_STRING")?;

        let tokens = &self._tokens;
        let mut elems: Vec<PyObject> = Vec::with_capacity(tokens.len());
        elems.push(
            PyTuple::new(
                py,
                &[
                    sof.clone_ref(py),
                    0u32.into_py(py),
                    0u32.into_py(py),
                    0u32.into_py(py),
                    0u32.into_py(py),
                    py.None(),
                ],
            )
            .into(),
        );
        for el in tokens {
            let (kind, value) = match el.kind {
                K::Sof => (sof.clone_ref(py), py.None()),
                K::Eof => (eof.clone_ref(py), py.None()),
                K::Bang => (bang.clone_ref(py), bang_v.clone_ref(py)),
                K::Dollar => (dollar.clone_ref(py), dollar_v.clone_ref(py)),
                K::ParenL => (paren_l.clone_ref(py), paren_l_v.clone_ref(py)),
                K::ParenR => (paren_r.clone_ref(py), paren_r_v.clone_ref(py)),
                K::Spread => (spread.clone_ref(py), spread_v.clone_ref(py)),
                K::Colon => (colon.clone_ref(py), colon_v.clone_ref(py)),
                K::Equals => (equals.clone_ref(py), equals_v.clone_ref(py)),
                K::At => (at.clone_ref(py), at_v.clone_ref(py)),
                K::BracketL => (bracket_l.clone_ref(py), bracket_l_v.clone_ref(py)),
                K::BracketR => (bracket_r.clone_ref(py), bracket_r_v.clone_ref(py)),
                K::BraceL => (brace_l.clone_ref(py), brace_l_v.clone_ref(py)),
                K::Pipe => (pipe.clone_ref(py), pipe_v.clone_ref(py)),
                K::BraceR => (brace_r.clone_ref(py), brace_r_v.clone_ref(py)),
                K::Name => (name.clone_ref(py), el.value.to_owned().into_py(py)),
                K::Int => (int.clone_ref(py), el.value.to_owned().into_py(py)),
                K::Float => (float.clone_ref(py), el.value.to_owned().into_py(py)),
                K::String => {
                    // graphql-core 3 receives unescaped strings from the lexer
                    let v = unquote_string(&el.value)
                        .map_err(|e| LexingError::new_err(e.to_string()))?
                        .into_py(py);
                    (string.clone_ref(py), v)
                }
                K::BlockString => {
                    // graphql-core 3 receives unescaped strings from the lexer
                    let v = unquote_block_string(&el.value)
                        .map_err(|e| LexingError::new_err(e.to_string()))?
                        .into_py(py);
                    (block_string.clone_ref(py), v)
                }
            };
            let elem = PyTuple::new(
                py,
                &[
                    kind,
                    el.position.map(|x| x.character).into_py(py),
                    el.position
                        .map(|x| x.character + el.value.chars().count())
                        .into_py(py),
                    el.position.map(|x| x.line).into_py(py),
                    el.position.map(|x| x.column).into_py(py),
                    value,
                ],
            );
            elems.push(elem.into());
        }
        let pos = self._end_pos;
        let end_off = pos.character.into_py(py);
        elems.push(
            PyTuple::new(
                py,
                &[
                    eof.clone_ref(py),
                    end_off.clone_ref(py),
                    pos.line.into_py(py),
                    pos.column.into_py(py),
                    end_off,
                    py.None(),
                ],
            )
            .into(),
        );
        Ok(PyList::new(py, &elems[..]).into())
    }
}

fn value_to_py(py: Python, value: &Value, decimal_cls: &PyAny) -> PyResult<PyObject> {
    let v = match value {
        Value::Str(ref v) => PyString::new(py, v).into(),
        Value::Int32(v) => v.into_py(py),
        Value::Int64(v) => v.into_py(py),
        Value::Decimal(v) => decimal_cls
            .call(PyTuple::new(py, &[v.into_py(py)]), None)?
            .into(),
        Value::BigInt(ref v) => PyType::new::<PyLong>(py)
            .call(PyTuple::new(py, &[v.into_py(py)]), None)?
            .into(),
        Value::Boolean(b) => b.into_py(py),
    };
    Ok(v)
}
