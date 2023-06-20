use std::collections::HashMap;

use cpython::{
    ObjectProtocol, PyClone, PyList, PyObject, PyResult, PyString, PyTuple, Python, PythonObject,
    PythonObjectWithCheckedDowncast, ToPyObject,
};

use edgeql_parser::parser;

use crate::errors::TokenizerError;

use crate::tokenizer::Token;

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
    data _non_term: PyString;
    data _production: PyString;
    data _args: PyList;

    def non_term(&self) -> PyResult<PyString> {
        Ok(self._non_term(py).clone_ref(py))
    }
    def production(&self) -> PyResult<PyString> {
        Ok(self._production(py).clone_ref(py))
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

static mut PARSER_SPECS: Option<HashMap<String, parser::Spec>> = None;

pub fn init_module() {
    unsafe {
        PARSER_SPECS = Some(HashMap::new());
    }
}

pub fn convert_tokens<'a>(py: Python, tokens: PyObject) -> PyResult<Vec<parser::Terminal>> {
    let tokens = PyList::downcast_from(py, tokens)?;

    let mut buf = Vec::with_capacity(tokens.len(py));
    for token in tokens.iter(py) {
        let token = Token::downcast_from(py, token)?;

        let value = token.value(py)?;
        let value = if value.is_none(py) {
            None
        } else {
            Some(value.to_string())
        };

        buf.push(parser::Terminal {
            kind: token.kind(py)?.to_string(py)?.to_string(),
            text: token.text(py)?.to_string(py)?.to_string(),
            value,
            span: token.span(py),
        });
    }
    Ok(buf)
}

pub fn parse(py: Python, parser_name: &PyString, tokens: PyObject) -> PyResult<CSTNode> {
    let spec = load_spec(py, parser_name.to_string(py)?.as_ref())?;

    let cheese = convert_tokens(py, tokens)?;

    let cst = parser::parse(spec, cheese).map_err(|s| TokenizerError::new(py, (s, py.None())))?;

    let cst = to_py_cst(cst, py)?;

    Ok(cst)
}

fn load_spec(py: Python, parser_name: &str) -> PyResult<&'static parser::Spec> {
    let parser_specs = unsafe { PARSER_SPECS.as_mut().unwrap() };
    if !parser_specs.contains_key(parser_name) {
        let parser_mod = py.import("edb.edgeql.parser.parser")?;

        let process_spec = py.import("edb.edgeql.parser")?.get(py, "process_spec")?;

        let parser_cls = parser_mod.get(py, parser_name)?;
        let parser = parser_cls.call(py, PyTuple::new(py, &[]), None)?;
        let spec_json = process_spec.call(py, (parser,), None)?;

        let spec = parser::Spec::from_json(&spec_json.to_string()).unwrap();

        parser_specs.insert(parser_name.to_string(), spec);
    }

    Ok(unsafe { PARSER_SPECS.as_ref().unwrap().get(parser_name).unwrap() })
}

fn to_py_cst<'a>(cst: parser::CSTNode, py: Python) -> PyResult<CSTNode> {
    match cst {
        parser::CSTNode::Empty => CSTNode::create_instance(py, py.None(), py.None()),
        parser::CSTNode::Terminal(token) => CSTNode::create_instance(
            py,
            py.None(),
            Terminal::create_instance(
                py,
                token.text.into_py_object(py),
                token.value.map(|v| v.into_py_object(py)).into_py_object(py),
                token.span.start.offset,
                token.span.end.offset,
            )?
            .into_object(),
        ),
        parser::CSTNode::Production {
            non_term,
            production,
            args,
        } => CSTNode::create_instance(
            py,
            Production::create_instance(
                py,
                non_term.into_py_object(py),
                production.into_py_object(py),
                PyList::new(
                    py,
                    args.into_iter()
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
