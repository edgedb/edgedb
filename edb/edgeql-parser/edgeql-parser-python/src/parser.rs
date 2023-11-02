use cpython::exc::AssertionError;
use cpython::{
    ObjectProtocol, PyClone, PyInt, PyList, PyNone, PyObject, PyResult, PyString, PyTuple, Python,
    PythonObject, PythonObjectWithCheckedDowncast, ToPyObject,
};
use once_cell::sync::OnceCell;

use edgeql_parser::parser;

use crate::errors::{parser_error_into_tuple, ParserResult};
use crate::pynormalize::value_to_py_object;
use crate::tokenizer::OpaqueToken;

pub fn parse(py: Python, start_token_name: &PyString, tokens: PyObject) -> PyResult<PyTuple> {
    let start_token_name = start_token_name.to_string(py).unwrap();

    let (spec, productions) = get_spec(py)?;

    let tokens = downcast_tokens(py, &start_token_name, tokens)?;

    let context = parser::Context::new(spec);
    let (cst, errors) = parser::parse(&tokens, &context);

    let cst = cst.map(|c| to_py_cst(c, py)).transpose()?;

    let errors = errors
        .into_iter()
        .map(|e| parser_error_into_tuple(py, e))
        .collect::<Vec<_>>();
    let errors = PyList::new(py, &errors);

    let res = ParserResult::create_instance(py, cst.into_py_object(py), errors)?;

    Ok((res, productions).into_py_object(py))
}

py_class!(pub class CSTNode |py| {
    data _production: PyObject;
    data _terminal: PyObject;

    def production(&self) -> PyResult<PyObject> {
        Ok(self._production(py).clone_ref(py))
    }
    def terminal(&self) -> PyResult<PyObject> {
        Ok(self._terminal(py).clone_ref(py))
    }
});

py_class!(pub class Production |py| {
    data _id: PyInt;
    data _args: PyList;

    def id(&self) -> PyResult<PyInt> {
        Ok(self._id(py).clone_ref(py))
    }
    def args(&self) -> PyResult<PyList> {
        Ok(self._args(py).clone_ref(py))
    }
});

py_class!(pub class Terminal |py| {
    data _text: PyString;
    data _value: PyObject;
    data _start: u64;
    data _end: u64;

    def text(&self) -> PyResult<PyString> {
        Ok(self._text(py).clone_ref(py))
    }
    def value(&self) -> PyResult<PyObject> {
        Ok(self._value(py).clone_ref(py))
    }
    def start(&self) -> PyResult<u64> {
        Ok(*self._start(py))
    }
    def end(&self) -> PyResult<u64> {
        Ok(*self._end(py))
    }
});

static PARSER_SPECS: OnceCell<(parser::Spec, PyObject)> = OnceCell::new();

fn downcast_tokens<'a>(
    py: Python,
    start_token_name: &str,
    token_list: PyObject,
) -> PyResult<Vec<parser::Terminal>> {
    let tokens = PyList::downcast_from(py, token_list)?;

    let mut buf = Vec::with_capacity(tokens.len(py) + 1);
    buf.push(parser::Terminal::from_start_name(start_token_name));
    for token in tokens.iter(py) {
        let token = OpaqueToken::downcast_from(py, token)?;
        let token = token.inner(py);

        buf.push(parser::Terminal::from_token(token));
    }

    // adjust the span of the starting token for nicer error message spans
    if buf.len() >= 2 {
        buf[0].span.start = buf[1].span.start;
        buf[0].span.end = buf[1].span.start;
    }

    Ok(buf)
}

fn get_spec(py: Python) -> Result<&'static (parser::Spec, PyObject), cpython::PyErr> {
    if let Some(x) = PARSER_SPECS.get() {
        return Ok(x);
    } else {
        return Err(cpython::PyErr::new::<AssertionError, _>(
            py,
            ("grammar spec not loaded",),
        ));
    }
}

/// Loads the grammar specification from file and caches it in memory.
pub fn preload_spec(py: Python, spec_filepath: &PyString) -> PyResult<PyNone> {
    if PARSER_SPECS.get().is_some() {
        return Ok(PyNone);
    }

    let spec_filepath = spec_filepath.to_string(py)?;
    let bytes = std::fs::read(spec_filepath.as_ref())
        .unwrap_or_else(|e| panic!("Cannot read grammar spec from {spec_filepath} ({e})"));

    let spec: parser::Spec = bitcode::deserialize::<parser::SpecSerializable>(&bytes)
        .unwrap()
        .into();
    let productions = load_productions(py, &spec)?;

    let _ = PARSER_SPECS.set((spec, productions));
    Ok(PyNone)
}

/// Serialize the grammar specification and write it to a file.
///
/// Called from setup.py.
pub fn save_spec(py: Python, spec_json: &PyString, dst: &PyString) -> PyResult<PyNone> {
    let spec_json = spec_json.to_string(py).unwrap();
    let spec: parser::SpecSerializable = serde_json::from_str(&spec_json).unwrap();
    let spec_bitcode = bitcode::serialize(&spec).unwrap();

    let dst = dst.to_string(py)?.to_string();

    std::fs::write(dst, spec_bitcode).ok().unwrap();
    Ok(PyNone)
}

fn load_productions(py: Python<'_>, spec: &parser::Spec) -> PyResult<PyObject> {
    let grammar_name = "edb.edgeql.parser.grammar.start";
    let grammar_mod = py.import(grammar_name)?;
    let load_productions = py
        .import("edb.common.parsing")?
        .get(py, "load_spec_productions")?;

    let productions = load_productions.call(py, (&spec.production_names, grammar_mod), None)?;
    Ok(productions)
}

fn to_py_cst<'a>(cst: &'a parser::CSTNode<'a>, py: Python) -> PyResult<CSTNode> {
    match cst {
        parser::CSTNode::Empty => CSTNode::create_instance(py, py.None(), py.None()),
        parser::CSTNode::Terminal(token) => CSTNode::create_instance(
            py,
            py.None(),
            Terminal::create_instance(
                py,
                token.text.to_py_object(py),
                if let Some(val) = &token.value {
                    value_to_py_object(py, val)?
                } else {
                    py.None()
                },
                token.span.start,
                token.span.end,
            )?
            .into_object(),
        ),
        parser::CSTNode::Production(prod) => CSTNode::create_instance(
            py,
            Production::create_instance(
                py,
                prod.id.into_py_object(py),
                PyList::new(
                    py,
                    prod.args
                        .iter()
                        .map(|a| to_py_cst(a, py).map(|x| x.into_object()))
                        .collect::<PyResult<Vec<_>>>()?
                        .as_slice(),
                ),
            )?
            .into_object(),
            py.None(),
        ),
    }
}
