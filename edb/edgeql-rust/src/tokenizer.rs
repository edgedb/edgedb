use std::borrow::Cow;
use std::char;
use std::collections::HashMap;
use std::iter::Peekable;
use std::slice::Iter;
use std::str::FromStr;

use cpython::{PyString, PyBytes, PyResult, Python, PyClone, PythonObject};
use cpython::{PyTuple, PyList, PyInt, PyObject, ToPyObject, ObjectProtocol};
use cpython::{FromPyObject};

use edgeql_parser::tokenizer::{TokenStream, Kind, is_keyword, SpannedToken};
use edgeql_parser::tokenizer::{MAX_KEYWORD_LENGTH};
use edgeql_parser::position::Pos;
use edgeql_parser::keywords::{CURRENT_RESERVED_KEYWORDS, UNRESERVED_KEYWORDS};
use edgeql_parser::keywords::{FUTURE_RESERVED_KEYWORDS};
use crate::errors::TokenizerError;
use crate::pynormalize::py_pos;

static mut TOKENS: Option<Tokens> = None;


#[derive(Debug, Clone)]
pub struct CowToken<'a> {
    pub kind: Kind,
    pub value: Cow<'a, str>,
    pub start: Pos,
    pub end: Pos,
}

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

    named_only: PyString,
    named_only_val: PyString,
    set_annotation: PyString,
    set_annotation_val: PyString,
    set_type: PyString,
    set_type_val: PyString,

    dot: PyString,
    forward_link: PyString,
    backward_link: PyString,
    open_bracket: PyString,
    close_bracket: PyString,
    open_paren: PyString,
    close_paren: PyString,
    open_brace: PyString,
    close_brace: PyString,
    namespace: PyString,
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
    decimal: Option<PyObject>,
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

fn peek_keyword(iter: &mut Peekable<Iter<CowToken>>, kw: &str) -> bool {
    iter.peek()
       .map(|t| t.kind == Kind::Ident && t.value.eq_ignore_ascii_case(kw))
       .unwrap_or(false)
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

    let mut token_stream = TokenStream::new(&data[..]);
    let rust_tokens: Vec<_> = py.allow_threads(|| {
        let mut tokens = Vec::new();
        for res in &mut token_stream {
            match res {
                Ok(t) => tokens.push(CowToken::from(t)),
                Err(e) => {
                    return Err((e, token_stream.current_pos()));
                }
            }
        }
        Ok(tokens)
    }).map_err(|(e, pos)| {
        use combine::easy::Error::*;
        let err = match e {
            Unexpected(s) => s.to_string(),
            o => o.to_string(),
        };
        TokenizerError::new(py, (err, py_pos(py, &pos)))
    })?;
    return convert_tokens(py, rust_tokens, token_stream.current_pos());
}

pub fn convert_tokens(py: Python, rust_tokens: Vec<CowToken<'_>>,
    end_pos: Pos)
    -> PyResult<PyList>
{
    let tokens = unsafe { TOKENS.as_ref().expect("module initialized") };
    let mut cache = Cache {
        decimal: None,
        keyword_buf: String::with_capacity(MAX_KEYWORD_LENGTH),
    };
    let mut buf = Vec::with_capacity(rust_tokens.len());
    let mut tok_iter = rust_tokens.iter().peekable();
    while let Some(tok) = tok_iter.next() {
        let (name, text, value) = convert(py, &tokens, &mut cache,
                                          tok, &mut tok_iter)?;
        let py_tok = Token::create_instance(py, name, text, value,
            tok.start, tok.end)?;

        buf.push(py_tok.into_object());
    }
    buf.push(Token::create_instance(py,
        tokens.eof.clone_ref(py),
        tokens.empty.clone_ref(py),
        py.None(),
        end_pos, end_pos)?
        .into_object());
    Ok(PyList::new(py, &buf[..]))
}


impl Tokens {
    pub fn new(py: Python) -> Tokens {
        let mut res = Tokens {
            ident: PyString::new(py, "IDENT"),
            argument: PyString::new(py, "ARGUMENT"),
            eof: PyString::new(py, "EOF"),
            empty: PyString::new(py, ""),
            named_only: PyString::new(py, "NAMEDONLY"),
            named_only_val: PyString::new(py, "NAMED ONLY"),
            set_annotation: PyString::new(py, "SETANNOTATION"),
            set_annotation_val: PyString::new(py, "SET ANNOTATION"),
            set_type: PyString::new(py, "SETTYPE"),
            set_type_val: PyString::new(py, "SET TYPE"),

            dot: PyString::new(py, "."),
            forward_link: PyString::new(py, ".>"),
            backward_link: PyString::new(py, ".<"),
            open_bracket: PyString::new(py, "["),
            close_bracket: PyString::new(py, "]"),
            open_paren: PyString::new(py, "("),
            close_paren: PyString::new(py, ")"),
            open_brace: PyString::new(py, "{"),
            close_brace: PyString::new(py, "}"),
            namespace: PyString::new(py, "::"),
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

impl Cache {
    fn decimal(&mut self, py: Python) -> PyResult<&mut PyObject> {
        if let Some(ref mut d) = self.decimal {
            return Ok(d);
        }
        let module = py.import("decimal")?;
        let typ = module.get(py, "Decimal")?;
        self.decimal = Some(typ);
        Ok(self.decimal.as_mut().unwrap())
    }
}


fn convert(py: Python, tokens: &Tokens, cache: &mut Cache,
    token: &CowToken,
    tok_iter: &mut Peekable<Iter<CowToken>>)
    -> PyResult<(PyString, PyString, PyObject)>
{
    use Kind::*;
    let value = &token.value[..];
    match token.kind {
        Assign => Ok((tokens.assign.clone_ref(py),
                      tokens.assign_op.clone_ref(py),
                      py.None())),
        SubAssign => Ok((tokens.sub_assign.clone_ref(py),
                         tokens.sub_assign_op.clone_ref(py),
                         py.None())),
        AddAssign => Ok((tokens.add_assign.clone_ref(py),
                         tokens.add_assign_op.clone_ref(py),
                         py.None())),
        Arrow  => Ok((tokens.arrow.clone_ref(py),
                      tokens.arrow_op.clone_ref(py),
                      py.None())),
        Coalesce => Ok((tokens.coalesce.clone_ref(py),
                        tokens.coalesce.clone_ref(py),
                        py.None())),
        Namespace => Ok((tokens.namespace.clone_ref(py),
                         tokens.namespace.clone_ref(py),
                         py.None())),
        ForwardLink => Ok((tokens.forward_link.clone_ref(py),
                           tokens.forward_link.clone_ref(py),
                           py.None())),
        BackwardLink => Ok((tokens.backward_link.clone_ref(py),
                            tokens.backward_link.clone_ref(py),
                            py.None())),
        FloorDiv => Ok((tokens.floor_div.clone_ref(py),
                        tokens.floor_div.clone_ref(py),
                        py.None())),
        Concat => Ok((tokens.concat.clone_ref(py),
                      tokens.concat.clone_ref(py),
                      py.None())),
        GreaterEq => Ok((tokens.op.clone_ref(py),
                         tokens.greater_eq.clone_ref(py),
                         py.None())),
        LessEq => Ok((tokens.op.clone_ref(py),
                      tokens.less_eq.clone_ref(py),
                      py.None())),
        NotEq => Ok((tokens.op.clone_ref(py),
                     tokens.not_eq.clone_ref(py),
                     py.None())),
        NotDistinctFrom => Ok((tokens.op.clone_ref(py),
                               tokens.not_distinct_from.clone_ref(py),
                               py.None())),
        DistinctFrom => Ok((tokens.op.clone_ref(py),
                            tokens.distinct_from.clone_ref(py),
                            py.None())),
        Comma => Ok((tokens.comma.clone_ref(py),
                     tokens.comma.clone_ref(py),
                     py.None())),
        OpenParen => Ok((tokens.open_paren.clone_ref(py),
                         tokens.open_paren.clone_ref(py),
                         py.None())),
        CloseParen => Ok((tokens.close_paren.clone_ref(py),
                          tokens.close_paren.clone_ref(py),
                          py.None())),
        OpenBracket => Ok((tokens.open_bracket.clone_ref(py),
                           tokens.open_bracket.clone_ref(py),
                           py.None())),
        CloseBracket => Ok((tokens.close_bracket.clone_ref(py),
                            tokens.close_bracket.clone_ref(py),
                            py.None())),
        OpenBrace => Ok((tokens.open_brace.clone_ref(py),
                         tokens.open_brace.clone_ref(py),
                         py.None())),
        CloseBrace => Ok((tokens.close_brace.clone_ref(py),
                          tokens.close_brace.clone_ref(py),
                          py.None())),
        Dot => Ok((tokens.dot.clone_ref(py),
                   tokens.dot.clone_ref(py),
                   py.None())),
        Semicolon => Ok((tokens.semicolon.clone_ref(py),
                         tokens.semicolon.clone_ref(py),
                         py.None())),
        Colon => Ok((tokens.colon.clone_ref(py),
                     tokens.colon.clone_ref(py),
                     py.None())),
        Add => Ok((tokens.add.clone_ref(py),
                   tokens.add.clone_ref(py),
                   py.None())),
        Sub => Ok((tokens.sub.clone_ref(py),
                   tokens.sub.clone_ref(py),
                   py.None())),
        Mul => Ok((tokens.mul.clone_ref(py),
                   tokens.mul.clone_ref(py),
                   py.None())),
        Div => Ok((tokens.div.clone_ref(py),
                   tokens.div.clone_ref(py),
                   py.None())),
        Modulo => Ok((tokens.modulo.clone_ref(py),
                      tokens.modulo.clone_ref(py),
                      py.None())),
        Pow => Ok((tokens.pow.clone_ref(py),
                   tokens.pow.clone_ref(py),
                   py.None())),
        Less => Ok((tokens.less.clone_ref(py),
                    tokens.less.clone_ref(py),
                    py.None())),
        Greater => Ok((tokens.greater.clone_ref(py),
                       tokens.greater.clone_ref(py),
                       py.None())),
        Eq => Ok((tokens.eq.clone_ref(py),
                  tokens.eq.clone_ref(py),
                  py.None())),
        Ampersand => Ok((tokens.ampersand.clone_ref(py),
                         tokens.ampersand.clone_ref(py),
                         py.None())),
        Pipe => Ok((tokens.pipe.clone_ref(py),
                    tokens.pipe.clone_ref(py),
                    py.None())),
        At => Ok((tokens.at.clone_ref(py),
                  tokens.at.clone_ref(py),
                  py.None())),
        Argument => {
            if value[1..].starts_with('`') {
                Ok((tokens.argument.clone_ref(py),
                    PyString::new(py, value),
                    PyString::new(py, &value[2..value.len()-1]
                                     .replace("``", "`"))
                   .into_object()))
            } else {
                Ok((tokens.argument.clone_ref(py),
                    PyString::new(py, value),
                    PyString::new(py, &value[1..])
                    .into_object()))
            }
        }
        DecimalConst => {
            Ok((tokens.nfconst.clone_ref(py),
                PyString::new(py, value),
                cache.decimal(py)?.call(py,
                    (&value[..value.len()-1],), None)?))
        }
        FloatConst => {
            Ok((tokens.fconst.clone_ref(py),
                PyString::new(py, value),
                f64::from_str(value)
                .map_err(|e| TokenizerError::new(py,
                    (format!("error reading float: {}", e),
                     py_pos(py, &token.start))))?
               .to_py_object(py)
               .into_object()))
        }
        IntConst => {
            Ok((tokens.iconst.clone_ref(py),
                PyString::new(py, value),
                // We read unsigned here, because unary minus will only
                // be identified on the parser stage. And there is a number
                // -9223372036854775808 which can't be represented in
                // i64 as absolute (positive) value.
                // Python has no problem of representing such a positive
                // value, though.
                u64::from_str(value)
                .map_err(|e| TokenizerError::new(py,
                    (format!("error reading int: {}", e),
                     py_pos(py, &token.start))))?
               .to_py_object(py)
               .into_object()))
        }
        BigIntConst => {
            Ok((tokens.niconst.clone_ref(py),
                PyString::new(py, value),
                py.get_type::<PyInt>().call(py,
                    (&value[..value.len()-1],), None)?))
        }
        BinStr => {
            Ok((tokens.bconst.clone_ref(py),
                PyString::new(py, value),
                PyBytes::new(py,
                    &unquote_bytes(&value[2..value.len()-1])
                    .map_err(|s| TokenizerError::new(py,
                        (s, py_pos(py, &token.start))))?)
                   .into_object()))
        }
        Str => {
            let content = decode_string(value)
                .map_err(|s| TokenizerError::new(py,
                    (s, py_pos(py, &token.start))))?;
            Ok((tokens.sconst.clone_ref(py),
                PyString::new(py, value),
                PyString::new(py, &content).into_object()))
        },
        BacktickName => {
            Ok((tokens.ident.clone_ref(py),
                PyString::new(py, value),
                PyString::new(py, &value[1..value.len()-1].replace("``", "`"))
               .into_object()))
        }
        Ident | Keyword => {
            if value.len() > MAX_KEYWORD_LENGTH {
                let val = PyString::new(py, value);
                Ok((tokens.ident.clone_ref(py),
                    val.clone_ref(py),
                    val.into_object()))
            } else {
                cache.keyword_buf.clear();
                cache.keyword_buf.push_str(value);
                cache.keyword_buf.make_ascii_lowercase();
                match &cache.keyword_buf[..] {
                    "named" if peek_keyword(tok_iter, "only") => {
                         tok_iter.next();
                         Ok((tokens.named_only.clone_ref(py),
                             tokens.named_only_val.clone_ref(py),
                             py.None()))
                    }
                    "set" if peek_keyword(tok_iter, "annotation") => {
                         tok_iter.next();
                         Ok((tokens.set_annotation.clone_ref(py),
                             tokens.set_annotation_val.clone_ref(py),
                             py.None()))
                    }
                    "set" if peek_keyword(tok_iter, "type") => {
                         tok_iter.next();
                         Ok((tokens.set_type.clone_ref(py),
                             tokens.set_type_val.clone_ref(py),
                             py.None()))
                    }
                    _ => match tokens.keywords.get(&cache.keyword_buf) {
                        Some(tok_info) => {
                            debug_assert_eq!(tok_info.kind, token.kind);
                            Ok((tok_info.name.clone_ref(py),
                                 PyString::new(py, value),
                                 py.None()))
                        }
                        None => {
                            debug_assert_eq!(token.kind, Kind::Ident);
                            let val = PyString::new(py, value);
                            Ok((tokens.ident.clone_ref(py),
                                val.clone_ref(py),
                                val.into_object()))
                        }
                    },
                }
            }
        }
    }
}

pub fn get_unpickle_fn(py: Python) -> PyObject {
    let tokens = unsafe { TOKENS.as_ref().expect("module initialized") };
    return tokens.unpickle_token.clone_ref(py);
}

impl<'a, 'b: 'a> From<&'a SpannedToken<'b>> for CowToken<'b> {
    fn from(t: &'a SpannedToken<'b>) -> CowToken<'b> {
        CowToken {
            kind: t.token.kind,
            value: t.token.value.into(),
            start: t.start,
            end: t.end,
        }
    }
}

impl<'a> From<SpannedToken<'a>> for CowToken<'a> {
    fn from(t: SpannedToken<'a>) -> CowToken<'a> {
        CowToken::from(&t)
    }
}

pub fn decode_string<'a>(value: &'a str) -> Result<Cow<'a, str>, String> {
    if value.starts_with('r') {
        Ok(value[2..value.len()-1].into())
    } else if value.starts_with('$') {
        let msize = value[1..].find('$').unwrap() + 2;
        Ok(value[msize..value.len()-msize].into())
    } else {
        Ok(unquote_string(&value[1..value.len()-1])?.into())
    }
}

fn unquote_string<'a>(s: &'a str) -> Result<String, String> {
    let mut res = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => {
                match chars.next().expect("slash cant be at the end") {
                    c@'"' | c@'\\' | c@'/' | c@'\'' => res.push(c),
                    'b' => res.push('\u{0010}'),
                    'f' => res.push('\u{000C}'),
                    'n' => res.push('\n'),
                    'r' => res.push('\r'),
                    't' => res.push('\t'),
                    'x' => {
                        let hex = chars.as_str().get(0..2);
                        let code = hex.and_then(|s| {
                            u8::from_str_radix(s, 16).ok()
                        }).ok_or_else(|| {
                            format!("invalid string literal: \
                                invalid escape sequence '\\x{}'",
                                hex.unwrap_or_else(|| chars.as_str())
                                .escape_debug())
                        })?;
                        if code > 0x7f {
                            return Err(format!(
                                "invalid string literal: \
                                 invalid escape sequence '\\x{:x}' \
                                 (only ascii allowed)", code));
                        }
                        res.push(code as char);
                        chars.nth(1);
                    }
                    'u' => {
                        let hex = chars.as_str().get(0..4);
                        let ch = hex.and_then(|s| {
                                u32::from_str_radix(s, 16).ok()
                            })
                            .and_then(|code| char::from_u32(code))
                            .ok_or_else(|| {
                                format!("invalid string literal: \
                                    invalid escape sequence '\\u{}'",
                                    hex.unwrap_or_else(|| chars.as_str())
                                    .escape_debug())
                            })?;
                        res.push(ch);
                        chars.nth(3);
                    }
                    'U' => {
                        let hex = chars.as_str().get(0..8);
                        let ch = hex.and_then(|s| {
                                u32::from_str_radix(s, 16).ok()
                            })
                            .and_then(|code| char::from_u32(code))
                            .ok_or_else(|| {
                                format!("invalid string literal: \
                                    invalid escape sequence '\\U{}'",
                                    hex.unwrap_or_else(|| chars.as_str())
                                    .escape_debug())
                            })?;
                        res.push(ch);
                        chars.nth(7);
                    },
                    '\r' | '\n' => {
                        let nleft = chars.as_str().trim_start().len();
                        let nskip = chars.as_str().len() - nleft;
                        if nskip > 0 {
                            chars.nth(nskip - 1);
                        }
                    }
                    c => {
                        return Err(format!(
                            "invalid string literal: \
                             invalid escape sequence '\\{}'",
                            c.escape_debug()));
                    }
                }
            }
            c => res.push(c),
        }
    }

    Ok(res)
}

fn unquote_bytes<'a>(s: &'a str) -> Result<Vec<u8>, String> {
    let mut res = Vec::with_capacity(s.len());
    let mut bytes = s.as_bytes().iter();
    while let Some(&c) = bytes.next() {
        match c {
            b'\\' => {
                match *bytes.next().expect("slash cant be at the end") {
                    c@b'"' | c@b'\\' | c@b'/' | c@b'\'' => res.push(c),
                    b'b' => res.push(b'\x10'),
                    b'f' => res.push(b'\x0C'),
                    b'n' => res.push(b'\n'),
                    b'r' => res.push(b'\r'),
                    b't' => res.push(b'\t'),
                    b'x' => {
                        let tail = &s[s.len() - bytes.as_slice().len()..];
                        let hex = tail.get(0..2);
                        let code = hex.and_then(|s| {
                            u8::from_str_radix(s, 16).ok()
                        }).ok_or_else(|| {
                            format!("invalid bytes literal: \
                                invalid escape sequence '\\x{}'",
                                hex.unwrap_or_else(|| tail).escape_debug())
                        })?;
                        res.push(code);
                        bytes.nth(1);
                    }
                    b'\r' | b'\n' => {
                        let nskip = bytes.as_slice()
                            .iter()
                            .take_while(|&&x| x.is_ascii_whitespace())
                            .count();
                        if nskip > 0 {
                            bytes.nth(nskip-1);
                        }
                    }
                    c => {
                        let ch = if c < 0x7f {
                            c as char
                        } else {
                            // recover the unicode byte
                            s[s.len()-bytes.as_slice().len()-1..]
                            .chars().next().unwrap()
                        };
                        return Err(format!("invalid bytes literal: \
                            invalid escape sequence '\\{}'",
                           ch.escape_debug()));
                    }
                }
            }
            c => res.push(c),
        }
    }

    Ok(res)
}

#[test]
fn unquote_unicode_string() {
    assert_eq!(unquote_string(r#"\x09"#).unwrap(), "\u{09}");
    assert_eq!(unquote_string(r#"\u000A"#).unwrap(), "\u{000A}");
    assert_eq!(unquote_string(r#"\u000D"#).unwrap(), "\u{000D}");
    assert_eq!(unquote_string(r#"\u0020"#).unwrap(), "\u{0020}");
    assert_eq!(unquote_string(r#"\uFFFF"#).unwrap(), "\u{FFFF}");
}

#[test]
fn newline_escaping_str() {
    assert_eq!(unquote_string(r"hello \
                                world").unwrap(), "hello world");

    assert_eq!(unquote_string(r"bb\
aa \
            bb").unwrap(), "bbaa bb");
    assert_eq!(unquote_string(r"bb\

        aa").unwrap(), "bbaa");
    assert_eq!(unquote_string(r"bb\
        \
        aa").unwrap(), "bbaa");
    assert_eq!(unquote_string("bb\\\r   aa").unwrap(), "bbaa");
    assert_eq!(unquote_string("bb\\\r\n   aa").unwrap(), "bbaa");
}

#[test]
fn complex_strings() {
    assert_eq!(unquote_string(r#"\u0009 hello \u000A there"#).unwrap(),
        "\u{0009} hello \u{000A} there");

    assert_eq!(unquote_string(r#"\x62:\u2665:\U000025C6"#).unwrap(),
        "\u{62}:\u{2665}:\u{25C6}");
}

#[test]
fn simple_bytes() {
    assert_eq!(unquote_bytes(r#"\x09"#).unwrap(), b"\x09");
    assert_eq!(unquote_bytes(r#"\x0A"#).unwrap(), b"\x0A");
    assert_eq!(unquote_bytes(r#"\x0D"#).unwrap(), b"\x0D");
    assert_eq!(unquote_bytes(r#"\x20"#).unwrap(), b"\x20");
}

#[test]
fn newline_escaping_bytes() {
    assert_eq!(unquote_bytes(r"hello \
                                world").unwrap(), b"hello world");

    assert_eq!(unquote_bytes(r"bb\
aa \
            bb").unwrap(), b"bbaa bb");
    assert_eq!(unquote_bytes(r"bb\

        aa").unwrap(), b"bbaa");
    assert_eq!(unquote_bytes(r"bb\
        \
        aa").unwrap(), b"bbaa");
    assert_eq!(unquote_bytes("bb\\\r   aa").unwrap(), b"bbaa");
    assert_eq!(unquote_bytes("bb\\\r\n   aa").unwrap(), b"bbaa");
}

#[test]
fn complex_bytes() {
    assert_eq!(unquote_bytes(r#"\x09 hello \x0A there"#).unwrap(),
        b"\x09 hello \x0A there");
}
