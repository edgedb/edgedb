use std::collections::HashMap;
use std::sync::Mutex;

use cpython::{
    ObjectProtocol, PyClone, PyInt, PyList, PyObject, PyResult, PyString, PyTuple, Python,
    PythonObject, PythonObjectWithCheckedDowncast, ToPyObject,
};

use edgeql_parser::parser;
use once_cell::sync::Lazy;

use crate::errors::{parser_error_into_tuple, ParserResult};
use crate::pynormalize::value_to_py_object;
use crate::tokenizer::OpaqueToken;

pub fn parse(py: Python, parser_name: &PyString, tokens: PyObject) -> PyResult<PyTuple> {
    let mut parser_specs = PARSER_SPECS.lock().unwrap();

    let (spec, productions) =
        load_spec(py, &mut parser_specs, parser_name.to_string(py)?.as_ref())?;

    let tokens = downcast_tokens(py, tokens)?;

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

type ParserSpecs = HashMap<String, (parser::Spec, PyObject)>;

static PARSER_SPECS: Lazy<Mutex<ParserSpecs>> = Lazy::new(|| Mutex::new(HashMap::new()));

fn downcast_tokens<'a>(py: Python, token_list: PyObject) -> PyResult<Vec<parser::Terminal>> {
    let tokens = PyList::downcast_from(py, token_list)?;

    let mut buf = Vec::with_capacity(tokens.len(py));
    for token in tokens.iter(py) {
        let token = OpaqueToken::downcast_from(py, token)?;
        let token = token.inner(py);

        buf.push(parser::Terminal::from_token(token));
    }
    Ok(buf)
}

fn load_spec<'a>(
    py: Python,
    specs: &'a mut ParserSpecs,
    parser_name: &str,
) -> PyResult<&'a (parser::Spec, PyObject)> {
    if !specs.contains_key(parser_name) {
        let parser_mod = py.import("edb.edgeql.parser.parser")?;

        let process_spec = py.import("edb.edgeql.parser")?.get(py, "process_spec")?;

        let parser_cls = parser_mod.get(py, parser_name)?;
        let parser = parser_cls.call(py, PyTuple::new(py, &[]), None)?;

        let res = process_spec.call(py, (parser,), None)?;
        let res = PyTuple::downcast_from(py, res)?;

        let spec_json = PyString::downcast_from(py, res.get_item(py, 0))?;
        let spec_json = spec_json.to_string(py).unwrap();

        let productions = res.get_item(py, 1);

        let spec = parser::Spec::from_json(&spec_json).unwrap();

        specs.insert(parser_name.to_string(), (spec, productions));
    }

    Ok(specs.get(parser_name).unwrap())
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
