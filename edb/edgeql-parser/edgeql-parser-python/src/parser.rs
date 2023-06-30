use std::collections::HashMap;

use cpython::{
    ObjectProtocol, PyClone, PyInt, PyList, PyObject, PyResult, PyString, PyTuple, Python,
    PythonObject, PythonObjectWithCheckedDowncast, ToPyObject,
};

use edgeql_parser::keywords;
use edgeql_parser::parser;
use edgeql_parser::tokenizer;
use edgeql_parser::tokenizer::Value;

use crate::errors::{parser_error_into_tuple, ParserResult};
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

        buf.push(parser::Terminal {
            kind: token.kind,
            text: token.text,
            value: match token.value {
                Some(Value::String(s)) => Some(s),
                _ => None,
            },
            span: token.span,
        });
    }
    Ok(buf)
}

pub fn parse(py: Python, parser_name: &PyString, tokens: PyObject) -> PyResult<PyTuple> {
    let (spec, productions) = load_spec(py, parser_name.to_string(py)?.as_ref())?;

    let tokens = downcast_tokens(py, tokens)?;

    let (cst, errors) = parser::parse(spec, tokens);

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
        let productions = res.get_item(py, 1);

        let spec = spec_from_json(&spec_json.to_string(py).unwrap()).unwrap();

        parser_specs.insert(parser_name.to_string(), (spec, productions));
    }

    Ok(unsafe { PARSER_SPECS.as_ref().unwrap().get(parser_name).unwrap() })
}

pub fn spec_from_json(j_spec: &str) -> Result<parser::Spec, String> {
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    pub struct SpecJson {
        pub actions: Vec<Vec<(String, parser::Action)>>,
        pub goto: Vec<Vec<(String, usize)>>,
        pub start: String,
        pub inlines: Vec<(usize, u8)>,
    }

    let v = serde_json::from_str::<SpecJson>(j_spec).map_err(|e| e.to_string())?;

    Ok(parser::Spec {
        actions: v
            .actions
            .into_iter()
            .map(|x| {
                x.into_iter()
                    .map(|(k, a)| (get_token_kind(&k), a))
                    .collect()
            })
            .collect(),
        goto: v.goto.into_iter().map(HashMap::from_iter).collect(),
        start: v.start,
        inlines: HashMap::from_iter(v.inlines),
    })
}

fn get_token_kind(token_name: &str) -> tokenizer::Kind {
    use tokenizer::Kind::*;

    match token_name {
        "+" => Add,
        "&" => Ampersand,
        "@" => At,
        ".<" => BackwardLink,
        "}" => CloseBrace,
        "]" => CloseBracket,
        ")" => CloseParen,
        "??" => Coalesce,
        ":" => Colon,
        "," => Comma,
        "++" => Concat,
        "/" => Div,
        "." => Dot,
        "**" => DoubleSplat,
        "=" => Eq,
        "//" => FloorDiv,
        "%" => Modulo,
        "*" => Mul,
        "::" => Namespace,
        "{" => OpenBrace,
        "[" => OpenBracket,
        "(" => OpenParen,
        "|" => Pipe,
        "^" => Pow,
        ";" => Semicolon,
        "-" => Sub,

        "?!=" => DistinctFrom,
        ">=" => GreaterEq,
        "<=" => LessEq,
        "?=" => NotDistinctFrom,
        "!=" => NotEq,
        "<" => Less,
        ">" => Greater,

        "IDENT" => Ident,
        "EOF" => EOF,
        "<$>" => EOI,
        "<e>" => Epsilon,

        "BCONST" => BinStr,
        "FCONST" => FloatConst,
        "ICONST" => IntConst,
        "NFCONST" => DecimalConst,
        "NICONST" => BigIntConst,
        "SCONST" => Str,

        "ADDASSIGN" => AddAssign,
        "ARROW" => Arrow,
        "ASSIGN" => Assign,
        "REMASSIGN" => SubAssign,

        "ARGUMENT" => Argument,
        "SUBSTITUTION" => Substitution,

        _ => {
            let mut token_name = token_name.to_lowercase();

            if let Some(rem) = token_name.strip_prefix("dunder") {
                token_name = format!("__{rem}__");
            }

            let kw = keywords::lookup_all(&token_name)
                .unwrap_or_else(|| panic!("unknown keyword {token_name}"));
            Keyword(kw)
        }
    }
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
