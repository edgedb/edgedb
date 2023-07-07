use std::collections::HashMap;

use cpython::{
    ObjectProtocol, PyClone, PyInt, PyList, PyObject, PyResult, PyString, PyTuple, Python,
    PythonObject, PythonObjectWithCheckedDowncast, ToPyObject,
};

use edgeql_parser::parser;

use crate::errors::{parser_error_into_tuple, ParserResult};
use crate::pynormalize::value_to_py_object;
use crate::tokenizer::OpaqueToken;

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

static mut PARSER_SPECS: Option<HashMap<String, (parser::Spec, PyObject)>> = None;

pub fn init_module() {
    unsafe {
        PARSER_SPECS = Some(HashMap::new());
    }
}

pub fn downcast_tokens<'a>(py: Python, token_list: PyObject) -> PyResult<Vec<parser::Terminal>> {
    let tokens = PyList::downcast_from(py, token_list)?;

    let mut buf = Vec::with_capacity(tokens.len(py));
    for token in tokens.iter(py) {
        let token = OpaqueToken::downcast_from(py, token)?.inner(py);

        buf.push(parser::Terminal::from_token(token));
    }
    Ok(buf)
}

pub fn parse(py: Python, parser_name: &PyString, tokens: PyObject) -> PyResult<PyTuple> {
    let (spec, productions) = load_spec(py, parser_name.to_string(py)?.as_ref())?;

    let tokens = downcast_tokens(py, tokens)?;

    let (cst, errors) = parser::parse(spec, tokens);

    // println!("{}", debug_cst_node(py, cst.as_ref().unwrap(), productions));

    let cst = cst.map(|c| to_py_cst(c, py)).transpose()?;

    let errors = errors
        .into_iter()
        .map(|e| parser_error_into_tuple(py, e))
        .collect::<Vec<_>>();
    let errors = PyList::new(py, &errors);

    let res = ParserResult::create_instance(py, cst.into_py_object(py), errors)?;

    Ok((res, productions).into_py_object(py))
}

fn load_spec(py: Python, parser_name: &str) -> PyResult<&'static (parser::Spec, PyObject)> {
    let parser_specs = unsafe { PARSER_SPECS.as_mut().unwrap() };
    if !parser_specs.contains_key(parser_name) {
        let parser_mod = py.import("edb.edgeql.parser.parser")?;

        let process_spec = py.import("edb.edgeql.parser")?.get(py, "process_spec")?;

        let parser_cls = parser_mod.get(py, parser_name)?;
        let parser = parser_cls.call(py, PyTuple::new(py, &[]), None)?;

        let res = process_spec.call(py, (parser,), None)?;
        let res = PyTuple::downcast_from(py, res).expect("process_spec to return a tuple");

        let spec_json =
            PyString::downcast_from(py, res.get_item(py, 0)).expect("json to be a string");
        let spec_json = spec_json.to_string(py).unwrap();
        std::fs::write(parser_name, spec_json.as_bytes()).unwrap();

        let productions = res.get_item(py, 1);

        let spec = parser::Spec::from_json(&spec_json).unwrap();

        parser_specs.insert(parser_name.to_string(), (spec, productions));
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
                if let Some(val) = token.value {
                    value_to_py_object(py, &val)?
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
                        .into_iter()
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

#[cfg(never)]
fn debug_cst_node(py: Python, node: &parser::CSTNode, productions_obj: &PyObject) -> String {
    let productions = PyList::downcast_borrow_from(py, productions_obj).unwrap();

    match node {
        parser::CSTNode::Empty => "<empty>".to_string(),
        parser::CSTNode::Terminal(t) => format!("{}", t.text),
        parser::CSTNode::Production(prod) => {
            let production = productions.get_item(py, prod.id);

            let mut r = production.getattr(py, "qualified").unwrap().to_string();
            if !prod.args.is_empty() {
                r += "[\n";
                r += &prod
                    .args
                    .iter()
                    .map(|a| debug_cst_node(py, a, productions_obj))
                    .collect::<Vec<_>>()
                    .join(",\n");
                r += "]\n";
            }
            r
        }
    }
}
