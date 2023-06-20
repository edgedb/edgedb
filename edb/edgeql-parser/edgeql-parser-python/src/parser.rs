use std::borrow::Cow;
use std::collections::HashMap;

use cpython::{ObjectProtocol, PyList, PyObject, PyTuple, PythonObject, ToPyObject};
use cpython::{PyClone, PyResult, PyString, Python};

use edgeql_parser::keywords::{
    CURRENT_RESERVED_KEYWORDS, FUTURE_RESERVED_KEYWORDS, PARTIAL_RESERVED_KEYWORDS,
    UNRESERVED_KEYWORDS,
};
use edgeql_parser::parser;

use edgeql_parser::tokenizer::{is_keyword, Kind, Token as PToken, Tokenizer, MAX_KEYWORD_LENGTH};

use crate::errors::TokenizerError;
use crate::pynormalize::py_pos;
use crate::tokenizer::Cache;

py_class!(pub class CSTNode |py| {
    data _production: PyObject;
    data _token: PyObject;

    def production(&self) -> PyResult<PyObject> {
        Ok(self._production(py).clone_ref(py))
    }
    def token(&self) -> PyResult<PyObject> {
        Ok(self._token(py).clone_ref(py))
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

py_class!(pub class ParserToken |py| {
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

static mut TOKENS_CHEESE: Option<TokensCheese> = None;

static mut PARSER_SPECS: Option<HashMap<String, parser::Spec>> = None;

pub fn init_module() {
    unsafe {
        TOKENS_CHEESE = Some(TokensCheese::new());
        PARSER_SPECS = Some(HashMap::new());
    }
}

pub fn convert_tokens(rust_tokens: Vec<PToken<'_>>) -> PyResult<Vec<parser::ParserToken>> {
    let tokens = unsafe { TOKENS_CHEESE.as_ref().expect("module initialized") };
    let mut cache = Cache {
        keyword_buf: String::with_capacity(MAX_KEYWORD_LENGTH),
    };
    let mut buf = Vec::with_capacity(rust_tokens.len());
    for token in rust_tokens {
        let (kind, text) =
            get_token_kind_and_name_cheese(tokens, &mut cache, token.kind, token.text);

        buf.push(parser::ParserToken {
            kind,
            text,
            value: token.value.and_then(|v| match v {
                edgeql_parser::tokenizer::Value::String(s) => Some(s),
                edgeql_parser::tokenizer::Value::Bytes(b) => Some(String::from_utf8(b).unwrap()),
                _ => None,
            }),
            span: token.span,
        });
    }
    buf.push(parser::ParserToken {
        kind: tokens.eof,
        ..Default::default()
    });
    Ok(buf)
}

pub fn parse(py: Python, spec: &PyString, s: &PyString) -> PyResult<CSTNode> {
    let spec = load_spec(py, spec.to_string(py)?.as_ref())?;

    let data = s.to_string(py)?;

    let mut token_stream = Tokenizer::new(&data[..]).validated_values();
    let rust_tokens: Vec<_> = py
        .allow_threads(|| (&mut token_stream).collect::<Result<_, _>>())
        .map_err(|e| TokenizerError::new(py, (e.message, py_pos(py, &e.span.start))))?;
    let cheese = convert_tokens(rust_tokens)?;

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

pub struct TokensCheese {
    ident: &'static str,
    argument: &'static str,
    eof: &'static str,
    substitution: &'static str,

    named_only: &'static str,
    named_only_val: &'static str,
    set_annotation: &'static str,
    set_annotation_val: &'static str,
    set_type: &'static str,
    set_type_val: &'static str,
    extension_package: &'static str,
    extension_package_val: &'static str,
    order_by: &'static str,
    order_by_val: &'static str,

    dot: &'static str,
    backward_link: &'static str,
    open_bracket: &'static str,
    close_bracket: &'static str,
    open_paren: &'static str,
    close_paren: &'static str,
    open_brace: &'static str,
    close_brace: &'static str,
    namespace: &'static str,
    double_splat: &'static str,
    coalesce: &'static str,
    colon: &'static str,
    semicolon: &'static str,
    comma: &'static str,
    add: &'static str,
    concat: &'static str,
    sub: &'static str,
    mul: &'static str,
    div: &'static str,
    floor_div: &'static str,
    modulo: &'static str,
    pow: &'static str,
    less: &'static str,
    greater: &'static str,
    eq: &'static str,
    ampersand: &'static str,
    pipe: &'static str,
    at: &'static str,

    iconst: &'static str,
    niconst: &'static str,
    fconst: &'static str,
    nfconst: &'static str,
    bconst: &'static str,
    sconst: &'static str,
    op: &'static str,

    greater_eq: &'static str,
    less_eq: &'static str,
    not_eq: &'static str,
    distinct_from: &'static str,
    not_distinct_from: &'static str,

    assign: &'static str,
    assign_op: &'static str,
    add_assign: &'static str,
    add_assign_op: &'static str,
    sub_assign: &'static str,
    sub_assign_op: &'static str,
    arrow: &'static str,
    arrow_op: &'static str,

    keywords: HashMap<String, TokenInfoCheese>,
}

pub struct TokenInfoCheese {
    pub kind: Kind,
    pub name: String,
    pub value: Option<String>,
}

impl TokensCheese {
    pub fn new() -> TokensCheese {
        let mut res = TokensCheese {
            ident: "IDENT",
            argument: "ARGUMENT",
            eof: "EOF",
            substitution: "SUBSTITUTION",
            named_only: "NAMEDONLY",
            named_only_val: "NAMED ONLY",
            set_annotation: "SETANNOTATION",
            set_annotation_val: "SET ANNOTATION",
            set_type: "SETTYPE",
            set_type_val: "SET TYPE",
            extension_package: "EXTENSIONPACKAGE",
            extension_package_val: "EXTENSION PACKAGE",
            order_by: "ORDERBY",
            order_by_val: "ORDER BY",

            dot: ".",
            backward_link: ".<",
            open_bracket: "[",
            close_bracket: "]",
            open_paren: "(",
            close_paren: ")",
            open_brace: "{",
            close_brace: "}",
            namespace: "::",
            double_splat: "**",
            coalesce: "??",
            colon: ":",
            semicolon: ";",
            comma: ",",
            add: "+",
            concat: "++",
            sub: "-",
            mul: "*",
            div: "/",
            floor_div: "//",
            modulo: "%",
            pow: "^",
            less: "<",
            greater: ">",
            eq: "=",
            ampersand: "&",
            pipe: "|",
            at: "@",

            iconst: "ICONST",
            niconst: "NICONST",
            fconst: "FCONST",
            nfconst: "NFCONST",
            bconst: "BCONST",
            sconst: "SCONST",
            op: "OP",

            // as OP
            greater_eq: ">=",
            less_eq: "<=",
            not_eq: "!=",
            distinct_from: "?!=",
            not_distinct_from: "?=",

            assign: "ASSIGN",
            assign_op: ":=",
            add_assign: "ADDASSIGN",
            add_assign_op: "+=",
            sub_assign: "REMASSIGN",
            sub_assign_op: "-=",
            arrow: "ARROW",
            arrow_op: "->",

            keywords: HashMap::new(),
        };
        // 'EOF'
        for kw in UNRESERVED_KEYWORDS.iter() {
            res.add_kw(kw);
        }
        for kw in PARTIAL_RESERVED_KEYWORDS.iter() {
            res.add_kw(kw);
        }
        for kw in CURRENT_RESERVED_KEYWORDS.iter() {
            res.add_kw(kw);
        }
        for kw in FUTURE_RESERVED_KEYWORDS.iter() {
            res.add_kw(kw);
        }
        return res;
    }
    fn add_kw(&mut self, name: &str) {
        let py_name = name.to_ascii_uppercase();
        let tok_name = if name.starts_with("__") && name.ends_with("__") {
            format!("DUNDER{}", name[2..name.len() - 2].to_ascii_uppercase())
        } else {
            py_name
        };
        self.keywords.insert(
            name.into(),
            TokenInfoCheese {
                kind: if is_keyword(name) {
                    Kind::Keyword
                } else {
                    Kind::Ident
                },
                name: tok_name,
                value: None,
            },
        );
    }
}

fn get_token_kind_and_name_cheese<'a, 'c>(
    tokens: &'a TokensCheese,
    cache: &'c mut Cache,
    kind: Kind,
    text: Cow<'a, str>,
) -> (&'a str, String) {
    use Kind::*;
    match kind {
        Assign => (tokens.assign, tokens.assign_op.to_string()),
        SubAssign => (tokens.sub_assign, tokens.sub_assign_op.to_string()),
        AddAssign => (tokens.add_assign, tokens.add_assign_op.to_string()),
        Arrow => (tokens.arrow, tokens.arrow_op.to_string()),
        Coalesce => (tokens.coalesce, tokens.coalesce.to_string()),
        Namespace => (tokens.namespace, tokens.namespace.to_string()),
        DoubleSplat => (tokens.double_splat, tokens.double_splat.to_string()),
        BackwardLink => (tokens.backward_link, tokens.backward_link.to_string()),
        FloorDiv => (tokens.floor_div, tokens.floor_div.to_string()),
        Concat => (tokens.concat, tokens.concat.to_string()),
        GreaterEq => (tokens.op, tokens.greater_eq.to_string()),
        LessEq => (tokens.op, tokens.less_eq.to_string()),
        NotEq => (tokens.op, tokens.not_eq.to_string()),
        NotDistinctFrom => (tokens.op, tokens.not_distinct_from.to_string()),
        DistinctFrom => (tokens.op, tokens.distinct_from.to_string()),
        Comma => (tokens.comma, tokens.comma.to_string()),
        OpenParen => (tokens.open_paren, tokens.open_paren.to_string()),
        CloseParen => (tokens.close_paren, tokens.close_paren.to_string()),
        OpenBracket => (tokens.open_bracket, tokens.open_bracket.to_string()),
        CloseBracket => (tokens.close_bracket, tokens.close_bracket.to_string()),
        OpenBrace => (tokens.open_brace, tokens.open_brace.to_string()),
        CloseBrace => (tokens.close_brace, tokens.close_brace.to_string()),
        Dot => (tokens.dot, tokens.dot.to_string()),
        Semicolon => (tokens.semicolon, tokens.semicolon.to_string()),
        Colon => (tokens.colon, tokens.colon.to_string()),
        Add => (tokens.add, tokens.add.to_string()),
        Sub => (tokens.sub, tokens.sub.to_string()),
        Mul => (tokens.mul, tokens.mul.to_string()),
        Div => (tokens.div, tokens.div.to_string()),
        Modulo => (tokens.modulo, tokens.modulo.to_string()),
        Pow => (tokens.pow, tokens.pow.to_string()),
        Less => (tokens.less, tokens.less.to_string()),
        Greater => (tokens.greater, tokens.greater.to_string()),
        Eq => (tokens.eq, tokens.eq.to_string()),
        Ampersand => (tokens.ampersand, tokens.ampersand.to_string()),
        Pipe => (tokens.pipe, tokens.pipe.to_string()),
        At => (tokens.at, tokens.at.to_string()),
        Argument => (tokens.argument, text.to_string()),
        DecimalConst => (tokens.nfconst, text.to_string()),
        FloatConst => (tokens.fconst, text.to_string()),
        IntConst => (tokens.iconst, text.to_string()),
        BigIntConst => (tokens.niconst, text.to_string()),
        BinStr => (tokens.bconst, text.to_string()),
        Str => (tokens.sconst, text.to_string()),
        BacktickName => (tokens.ident, text.to_string()),
        Ident | Keyword => match text.as_ref() {
            "named only" => (tokens.named_only, tokens.named_only_val.to_string()),
            "set annotation" => (tokens.set_annotation, tokens.set_annotation_val.to_string()),
            "set type" => (tokens.set_type, tokens.set_type_val.to_string()),
            "extension package" => (
                tokens.extension_package,
                tokens.extension_package_val.to_string(),
            ),
            "order by" => (tokens.order_by, tokens.order_by_val.to_string()),

            _ => {
                if text.len() > MAX_KEYWORD_LENGTH {
                    (tokens.ident, text.to_string())
                } else {
                    cache.keyword_buf.clear();
                    cache.keyword_buf.push_str(&text);
                    cache.keyword_buf.make_ascii_lowercase();

                    let kind = match tokens.keywords.get(&cache.keyword_buf) {
                        Some(keyword) => {
                            debug_assert_eq!(keyword.kind, kind);

                            keyword.name.as_str()
                        }
                        None => {
                            debug_assert_eq!(Kind::Ident, kind);
                            tokens.ident
                        }
                    };
                    (kind, text.to_string())
                }
            }
        },
        Substitution => (tokens.substitution, text.to_string()),
    }
}

fn to_py_cst<'a>(cst: parser::CSTNode<'a>, py: Python) -> PyResult<CSTNode> {
    match cst {
        parser::CSTNode::Empty => CSTNode::create_instance(py, py.None(), py.None()),
        parser::CSTNode::Token(token) => CSTNode::create_instance(
            py,
            py.None(),
            ParserToken::create_instance(
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
