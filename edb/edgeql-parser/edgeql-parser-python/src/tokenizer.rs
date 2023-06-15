use std::collections::HashMap;

use cpython::{PyString, PyResult, Python, PyClone, PythonObject};
use cpython::{PyTuple, PyList, PyObject, ToPyObject, ObjectProtocol};
use cpython::{FromPyObject};

use edgeql_parser::tokenizer::{Kind, is_keyword, Tokenizer, Token as PToken};
use edgeql_parser::tokenizer::{MAX_KEYWORD_LENGTH};
use edgeql_parser::position::Pos;
use edgeql_parser::keywords::{PARTIAL_RESERVED_KEYWORDS, UNRESERVED_KEYWORDS};
use edgeql_parser::keywords::{CURRENT_RESERVED_KEYWORDS};
use edgeql_parser::keywords::{FUTURE_RESERVED_KEYWORDS};

use crate::errors::TokenizerError;
use crate::pynormalize::{py_pos, value_to_py_object};

static mut TOKENS: Option<Tokens> = None;


fn rs_pos(py: Python, value: &PyObject) -> PyResult<Pos> {
    let (line, column, offset) = FromPyObject::extract(py, value)?;
    Ok(Pos { line, column, offset })
}

py_class!(pub class Token |py| {
    data _kind: PyString;
    data _text: PyString;
    data _value: PyObject;
    data _start: Pos;
    data _end: Pos;
    def kind(&self) -> PyResult<PyString> {
        Ok(self._kind(py).clone_ref(py))
    }
    def text(&self) -> PyResult<PyString> {
        Ok(self._text(py).clone_ref(py))
    }
    def value(&self) -> PyResult<PyObject> {
        Ok(self._value(py).clone_ref(py))
    }
    def start(&self) -> PyResult<PyTuple> {
        Ok(py_pos(py, self._start(py)))
    }
    def end(&self) -> PyResult<PyTuple> {
        Ok(py_pos(py, self._end(py)))
    }
    def __repr__(&self) -> PyResult<PyString> {
        let val = self._value(py);
        let s = if *val == py.None() {
            format!("<Token {}>", self._kind(py).to_string(py)?)
        } else {
            format!("<Token {} {}>",
                self._kind(py).to_string(py)?,
                val.repr(py)?.to_string(py)?)
        };
        Ok(PyString::new(py, &s))
    }
    def __reduce__(&self) -> PyResult<PyTuple> {
        return Ok((
            get_unpickle_fn(py),
            (
                self._kind(py),
                self._text(py),
                self._value(py),
                py_pos(py, self._start(py)),
                py_pos(py, self._end(py)),
            ),
        ).to_py_object(py))
    }
});


pub struct Tokens {
    ident: PyString,
    argument: PyString,
    eof: PyString,
    empty: PyString,
    substitution: PyString,

    named_only: PyString,
    named_only_val: PyString,
    set_annotation: PyString,
    set_annotation_val: PyString,
    set_type: PyString,
    set_type_val: PyString,
    extension_package: PyString,
    extension_package_val: PyString,
    order_by: PyString,
    order_by_val: PyString,

    dot: PyString,
    backward_link: PyString,
    open_bracket: PyString,
    close_bracket: PyString,
    open_paren: PyString,
    close_paren: PyString,
    open_brace: PyString,
    close_brace: PyString,
    namespace: PyString,
    double_splat: PyString,
    coalesce: PyString,
    colon: PyString,
    semicolon: PyString,
    comma: PyString,
    add: PyString,
    concat: PyString,
    sub: PyString,
    mul: PyString,
    div: PyString,
    floor_div: PyString,
    modulo: PyString,
    pow: PyString,
    less: PyString,
    greater: PyString,
    eq: PyString,
    ampersand: PyString,
    pipe: PyString,
    at: PyString,

    iconst: PyString,
    niconst: PyString,
    fconst: PyString,
    nfconst: PyString,
    bconst: PyString,
    sconst: PyString,
    op: PyString,

    greater_eq: PyString,
    less_eq: PyString,
    not_eq: PyString,
    distinct_from: PyString,
    not_distinct_from: PyString,

    assign: PyString,
    assign_op: PyString,
    add_assign: PyString,
    add_assign_op: PyString,
    sub_assign: PyString,
    sub_assign_op: PyString,
    arrow: PyString,
    arrow_op: PyString,

    keywords: HashMap<String, TokenInfo>,
    unpickle_token: PyObject,
}

struct Cache {
    keyword_buf: String,
}

pub struct TokenInfo {
    pub kind: Kind,
    pub name: PyString,
    pub value: Option<PyString>,
}

pub fn init_module(py: Python) {
    unsafe {
        TOKENS = Some(Tokens::new(py))
    }
}

pub fn _unpickle_token(py: Python,
        kind: &PyString, text: &PyString, value: &PyObject,
        start: &PyObject, end: &PyObject)
        -> PyResult<Token>
{
    // TODO(tailhook) We might some strings from Tokens structure
    //                (i.e. internning them).
    //                But if we're storing a collection of tokens
    //                they will store the tokens only once, so it
    //                doesn't seem to help that much.
    Token::create_instance(py,
        kind.clone_ref(py),
        text.clone_ref(py),
        value.clone_ref(py),
        rs_pos(py, start)?,
        rs_pos(py, end)?)
}

pub fn tokenize(py: Python, s: &PyString) -> PyResult<PyList> {
    let data = s.to_string(py)?;

    let mut token_stream = Tokenizer::new(&data[..]).validated_values();
    let rust_tokens: Vec<_> = py.allow_threads(|| {
        (&mut token_stream).collect::<Result<_, _>>()
    }).map_err(|e| {
        TokenizerError::new(py, (e.message, py_pos(py, &e.span.start)))
    })?;
    return convert_tokens(py, rust_tokens, token_stream.current_pos());
}

pub fn convert_tokens(py: Python, rust_tokens: Vec<PToken<'_>>,
    end_pos: Pos)
    -> PyResult<PyList>
{
    let tokens = unsafe { TOKENS.as_ref().expect("module initialized") };
    let mut cache = Cache {
        keyword_buf: String::with_capacity(MAX_KEYWORD_LENGTH),
    };
    let mut buf = Vec::with_capacity(rust_tokens.len());
    for tok in rust_tokens {
        let (kind, text) = get_token_kind_and_name(py, tokens, &mut cache, &tok);

        let value = tok.value.as_ref()
            .map(|v| value_to_py_object(py, v)).transpose()?
            .unwrap_or_else(|| py.None());

        let py_tok = Token::create_instance(
            py, kind, text, value, tok.span.start, tok.span.end
        )?;

        buf.push(py_tok.into_object());
    }
    buf.push(Token::create_instance(
            py,
            tokens.eof.clone_ref(py),
            tokens.empty.clone_ref(py),
            py.None(),
            end_pos,
            end_pos
        )?.into_object()
    );
    Ok(PyList::new(py, &buf[..]))
}

impl Tokens {
    pub fn new(py: Python) -> Tokens {
        let mut res = Tokens {
            ident: PyString::new(py, "IDENT"),
            argument: PyString::new(py, "ARGUMENT"),
            eof: PyString::new(py, "EOF"),
            empty: PyString::new(py, ""),
            substitution: PyString::new(py, "SUBSTITUTION"),
            named_only: PyString::new(py, "NAMEDONLY"),
            named_only_val: PyString::new(py, "NAMED ONLY"),
            set_annotation: PyString::new(py, "SETANNOTATION"),
            set_annotation_val: PyString::new(py, "SET ANNOTATION"),
            set_type: PyString::new(py, "SETTYPE"),
            set_type_val: PyString::new(py, "SET TYPE"),
            extension_package: PyString::new(py, "EXTENSIONPACKAGE"),
            extension_package_val: PyString::new(py, "EXTENSION PACKAGE"),
            order_by: PyString::new(py, "ORDERBY"),
            order_by_val: PyString::new(py, "ORDER BY"),

            dot: PyString::new(py, "."),
            backward_link: PyString::new(py, ".<"),
            open_bracket: PyString::new(py, "["),
            close_bracket: PyString::new(py, "]"),
            open_paren: PyString::new(py, "("),
            close_paren: PyString::new(py, ")"),
            open_brace: PyString::new(py, "{"),
            close_brace: PyString::new(py, "}"),
            namespace: PyString::new(py, "::"),
            double_splat: PyString::new(py, "**"),
            coalesce: PyString::new(py, "??"),
            colon: PyString::new(py, ":"),
            semicolon: PyString::new(py, ";"),
            comma: PyString::new(py, ","),
            add: PyString::new(py, "+"),
            concat: PyString::new(py, "++"),
            sub: PyString::new(py, "-"),
            mul: PyString::new(py, "*"),
            div: PyString::new(py, "/"),
            floor_div: PyString::new(py, "//"),
            modulo: PyString::new(py, "%"),
            pow: PyString::new(py, "^"),
            less: PyString::new(py, "<"),
            greater: PyString::new(py, ">"),
            eq: PyString::new(py, "="),
            ampersand: PyString::new(py, "&"),
            pipe: PyString::new(py, "|"),
            at: PyString::new(py, "@"),

            iconst: PyString::new(py, "ICONST"),
            niconst: PyString::new(py, "NICONST"),
            fconst: PyString::new(py, "FCONST"),
            nfconst: PyString::new(py, "NFCONST"),
            bconst: PyString::new(py, "BCONST"),
            sconst: PyString::new(py, "SCONST"),
            op: PyString::new(py, "OP"),

            // as OP
            greater_eq: PyString::new(py, ">="),
            less_eq: PyString::new(py, "<="),
            not_eq: PyString::new(py, "!="),
            distinct_from: PyString::new(py, "?!="),
            not_distinct_from: PyString::new(py, "?="),

            assign: PyString::new(py, "ASSIGN"),
            assign_op: PyString::new(py, ":="),
            add_assign: PyString::new(py, "ADDASSIGN"),
            add_assign_op: PyString::new(py, "+="),
            sub_assign: PyString::new(py, "REMASSIGN"),
            sub_assign_op: PyString::new(py, "-="),
            arrow: PyString::new(py, "ARROW"),
            arrow_op: PyString::new(py, "->"),

            keywords: HashMap::new(),
            unpickle_token: py_fn!(py, _unpickle_token(
                kind: &PyString, text: &PyString, value: &PyObject,
                start: &PyObject, end: &PyObject)),
        };
        // 'EOF'
        for kw in UNRESERVED_KEYWORDS.iter() {
            res.add_kw(py, kw);
        }
        for kw in PARTIAL_RESERVED_KEYWORDS.iter() {
            res.add_kw(py, kw);
        }
        for kw in CURRENT_RESERVED_KEYWORDS.iter() {
            res.add_kw(py, kw);
        }
        for kw in FUTURE_RESERVED_KEYWORDS.iter() {
            res.add_kw(py, kw);
        }
        return res;
    }
    fn add_kw(&mut self, py: Python, name: &str) {
        let py_name = PyString::new(py, &name.to_ascii_uppercase());
        let tok_name = if name.starts_with("__") && name.ends_with("__") {
            format!("DUNDER{}", name[2..name.len()-2].to_ascii_uppercase())
            .to_py_object(py)
        } else {
            py_name.clone_ref(py)
        };
        self.keywords.insert(name.into(), TokenInfo {
            kind: if is_keyword(name) { Kind::Keyword } else { Kind::Ident },
            name: tok_name,
            value: None,
        });
    }
}

fn get_token_kind_and_name(
    py: Python,
    tokens: &Tokens,
    cache: &mut Cache,
    token: &PToken,
) -> (PyString, PyString) {
    use Kind::*;
    let text = &token.text[..];
    match token.kind {
        Assign => (
            tokens.assign.clone_ref(py),
            tokens.assign_op.clone_ref(py),
        ),
        SubAssign => (
            tokens.sub_assign.clone_ref(py),
            tokens.sub_assign_op.clone_ref(py),
        ),
        AddAssign => (
            tokens.add_assign.clone_ref(py),
            tokens.add_assign_op.clone_ref(py),
        ),
        Arrow => (
            tokens.arrow.clone_ref(py),
            tokens.arrow_op.clone_ref(py),
        ),
        Coalesce => (
            tokens.coalesce.clone_ref(py),
            tokens.coalesce.clone_ref(py),
        ),
        Namespace => (
            tokens.namespace.clone_ref(py),
            tokens.namespace.clone_ref(py),
        ),
        DoubleSplat => (
            tokens.double_splat.clone_ref(py),
            tokens.double_splat.clone_ref(py),
        ),
        BackwardLink => (
            tokens.backward_link.clone_ref(py),
            tokens.backward_link.clone_ref(py),
        ),
        FloorDiv => (
            tokens.floor_div.clone_ref(py),
            tokens.floor_div.clone_ref(py),
        ),
        Concat => (
            tokens.concat.clone_ref(py),
            tokens.concat.clone_ref(py),
        ),
        GreaterEq => (
            tokens.op.clone_ref(py),
            tokens.greater_eq.clone_ref(py),
        ),
        LessEq => (
            tokens.op.clone_ref(py),
            tokens.less_eq.clone_ref(py),
        ),
        NotEq => (
            tokens.op.clone_ref(py),
            tokens.not_eq.clone_ref(py),
        ),
        NotDistinctFrom => (
            tokens.op.clone_ref(py),
            tokens.not_distinct_from.clone_ref(py),
        ),
        DistinctFrom => (
            tokens.op.clone_ref(py),
            tokens.distinct_from.clone_ref(py),
        ),
        Comma => (
            tokens.comma.clone_ref(py),
            tokens.comma.clone_ref(py),
        ),
        OpenParen => (
            tokens.open_paren.clone_ref(py),
            tokens.open_paren.clone_ref(py),
        ),
        CloseParen => (
            tokens.close_paren.clone_ref(py),
            tokens.close_paren.clone_ref(py),
        ),
        OpenBracket => (
            tokens.open_bracket.clone_ref(py),
            tokens.open_bracket.clone_ref(py),
        ),
        CloseBracket => (
            tokens.close_bracket.clone_ref(py),
            tokens.close_bracket.clone_ref(py),
        ),
        OpenBrace => (
            tokens.open_brace.clone_ref(py),
            tokens.open_brace.clone_ref(py),
        ),
        CloseBrace => (
            tokens.close_brace.clone_ref(py),
            tokens.close_brace.clone_ref(py),
        ),
        Dot => (
            tokens.dot.clone_ref(py),
            tokens.dot.clone_ref(py),
        ),
        Semicolon => (
            tokens.semicolon.clone_ref(py),
            tokens.semicolon.clone_ref(py),
        ),
        Colon => (
            tokens.colon.clone_ref(py),
            tokens.colon.clone_ref(py),
        ),
        Add => (
            tokens.add.clone_ref(py),
            tokens.add.clone_ref(py),
        ),
        Sub => (
            tokens.sub.clone_ref(py),
            tokens.sub.clone_ref(py),
        ),
        Mul => (
            tokens.mul.clone_ref(py),
            tokens.mul.clone_ref(py),
        ),
        Div => (
            tokens.div.clone_ref(py),
            tokens.div.clone_ref(py),
        ),
        Modulo => (
            tokens.modulo.clone_ref(py),
            tokens.modulo.clone_ref(py),
        ),
        Pow => (
            tokens.pow.clone_ref(py),
            tokens.pow.clone_ref(py),
        ),
        Less => (
            tokens.less.clone_ref(py),
            tokens.less.clone_ref(py),
        ),
        Greater => (
            tokens.greater.clone_ref(py),
            tokens.greater.clone_ref(py),
        ),
        Eq => (
            tokens.eq.clone_ref(py),
            tokens.eq.clone_ref(py),
        ),
        Ampersand => (
            tokens.ampersand.clone_ref(py),
            tokens.ampersand.clone_ref(py),
        ),
        Pipe => (
            tokens.pipe.clone_ref(py),
            tokens.pipe.clone_ref(py),
        ),
        At => (
            tokens.at.clone_ref(py),
            tokens.at.clone_ref(py),
        ),
        Argument => (
            tokens.argument.clone_ref(py),
            PyString::new(py, text),
        ),
        DecimalConst => (
            tokens.nfconst.clone_ref(py),
            PyString::new(py, text),
        ),
        FloatConst => (
            tokens.fconst.clone_ref(py),
            PyString::new(py, text),
        ),
        IntConst => (
            tokens.iconst.clone_ref(py),
            PyString::new(py, text),
        ),
        BigIntConst => (
            tokens.niconst.clone_ref(py),
            PyString::new(py, text),
        ),
        BinStr => (
            tokens.bconst.clone_ref(py),
            PyString::new(py, text),
        ),
        Str => (
            tokens.sconst.clone_ref(py),
            PyString::new(py, text),
        ),
        BacktickName => (
            tokens.ident.clone_ref(py),
            PyString::new(py, text),
        ),
        Ident | Keyword => match text {
            "named only" => (
                tokens.named_only.clone_ref(py),
                tokens.named_only_val.clone_ref(py),
            ),
            "set annotation" => (
                tokens.set_annotation.clone_ref(py),
                tokens.set_annotation_val.clone_ref(py),
            ),
            "set type" => (
                tokens.set_type.clone_ref(py),
                tokens.set_type_val.clone_ref(py),
            ),
            "extension package" => {
                (
                tokens.extension_package.clone_ref(py),
                tokens.extension_package_val.clone_ref(py),
            )},
            "order by" => (
                tokens.order_by.clone_ref(py),
                tokens.order_by_val.clone_ref(py),
            ),

            _ => {
                if text.len() > MAX_KEYWORD_LENGTH {
                    (
                        tokens.ident.clone_ref(py),
                        PyString::new(py, text),
                    )
                } else {
                    cache.keyword_buf.clear();
                    cache.keyword_buf.push_str(text);
                    cache.keyword_buf.make_ascii_lowercase();

                    let kind = match tokens.keywords.get(&cache.keyword_buf) {
                        Some(keyword) => {
                            debug_assert_eq!(keyword.kind, token.kind);

                            keyword.name.clone_ref(py)
                        }
                        None => {
                            debug_assert_eq!(Kind::Ident, token.kind);
                            tokens.ident.clone_ref(py)
                        }
                    };
                    (kind, PyString::new(py, text))
                }
            },
        }
        Substitution => (
            tokens.substitution.clone_ref(py),
            PyString::new(py, text),
        ),
    }
}

pub fn get_unpickle_fn(py: Python) -> PyObject {
    let tokens = unsafe { TOKENS.as_ref().expect("module initialized") };
    return tokens.unpickle_token.clone_ref(py);
}
