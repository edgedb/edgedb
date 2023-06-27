use cpython::{PyBytes, PyClone, PyResult, PyString, Python, PythonObject};
use cpython::{PyList, PyObject, PyTuple, ToPyObject};

use edgeql_parser::position::{Pos, Span};

use edgeql_parser::tokenizer::{Kind, Token, Tokenizer};

use crate::errors::{parser_error_into_tuple, ParserResult};

// An opaque wrapper around [edgeql_parser::tokenizer::Token].
// Supports Python pickle serialization.
py_class!(pub class OpaqueToken |py| {
    data _inner: Token;

    def __repr__(&self) -> PyResult<PyString> {
        Ok(PyString::new(py, &self._inner(py).to_string()))
    }
    def __reduce__(&self) -> PyResult<PyTuple> {
        let data: Vec<u8> = rmp_serde::to_vec(self._inner(py)).unwrap().to_vec();

        return Ok((
            get_fn_unpickle_token(py),
            (
                PyBytes::new(py, &data),
            ),
        ).to_py_object(py))
    }
});

pub fn tokenize(py: Python, s: &PyString) -> PyResult<ParserResult> {
    let data = s.to_string(py)?;

    let mut token_stream = Tokenizer::new(&data[..]).validated_values();

    let mut tokens: Vec<_> = Vec::new();
    let mut errors: Vec<_> = Vec::new();

    for res in &mut token_stream {
        match res {
            Ok(token) => tokens.push(token),
            Err(e) => {
                errors.push(parser_error_into_tuple(py, e));

                // TODO: fix tokenizer to skip bad tokens and continue
                break;
            }
        }
    }

    let tokens = convert_tokens(py, tokens, token_stream.current_pos())?;

    let errors = PyList::new(py, errors.as_slice()).to_py_object(py);

    ParserResult::create_instance(py, tokens.into_object(), errors)
}

pub fn convert_tokens(py: Python, rust_tokens: Vec<Token>, end_pos: Pos) -> PyResult<PyList> {
    let mut buf = Vec::with_capacity(rust_tokens.len());
    for tok in rust_tokens {
        let py_tok = OpaqueToken::create_instance(py, tok)?.into_object();

        buf.push(py_tok);
    }
    buf.push(
        OpaqueToken::create_instance(
            py,
            Token {
                kind: Kind::EOF,
                text: "".to_string(),
                value: None,
                span: Span {
                    start: end_pos,
                    end: end_pos,
                },
            },
        )?
        .into_object(),
    );
    Ok(PyList::new(py, &buf[..]))
}

static mut FN_UNPICKLE_TOKEN: Option<PyObject> = None;

pub fn init_module(py: Python) {
    unsafe {
        FN_UNPICKLE_TOKEN = Some(py_fn!(py, _unpickle_token(bytes: &PyBytes)));
    }
}

pub fn _unpickle_token(py: Python, bytes: &PyBytes) -> PyResult<OpaqueToken> {
    let token = rmp_serde::from_slice(bytes.data(py)).unwrap();
    OpaqueToken::create_instance(py, token)
}

pub fn get_fn_unpickle_token(py: Python) -> PyObject {
    let py_function = unsafe { FN_UNPICKLE_TOKEN.as_ref().expect("module initialized") };
    return py_function.clone_ref(py);
}

// fn extend_kind(
//     py: Python,
//     tokens: &Tokens,
//     cache: &mut Cache,
//     token: &Token,
// ) -> (PyString, PyString) {
//     use Kind::*;
//     let text = &token.text[..];
//     match token.kind {
//         Ident | Keyword => match text {
//             "named only" => (
//                 tokens.named_only.clone_ref(py),
//             ),
//             "set annotation" => (
//                 tokens.set_annotation.clone_ref(py),

//             ),
//             "set type" => (
//                 tokens.set_type.clone_ref(py),

//             ),
//             "extension package" => (
//                 tokens.extension_package.clone_ref(py),

//             ),
//             "order by" => (
//                 tokens.order_by.clone_ref(py),
//             ),

//             _ => {
//                 if text.len() > MAX_KEYWORD_LENGTH {
//                     (tokens.ident.clone_ref(py), PyString::new(py, text))
//                 } else {
//                     cache.keyword_buf.clear();
//                     cache.keyword_buf.push_str(text);
//                     cache.keyword_buf.make_ascii_lowercase();

//                     let kind = match tokens.keywords.get(&cache.keyword_buf) {
//                         Some(keyword) => {
//                             debug_assert_eq!(keyword.kind, token.kind);

//                             keyword.name.clone_ref(py)
//                         }
//                         None => {
//                             debug_assert_eq!(Kind::Ident, token.kind);
//                             tokens.ident.clone_ref(py)
//                         }
//                     };
//                     (kind, PyString::new(py, text))
//                 }
//             }
//         },
//         Substitution => (tokens.substitution.clone_ref(py), PyString::new(py, text)),
//     }
// }

// fn override_text(token: &Token) -> Option<&'static str> {
//     use Kind::*;
//     Some(match token.kind {
//         Assign => ":=",
//         SubAssign => "-=",
//         AddAssign => "+=",
//         Arrow => "->",
//         Coalesce => "??",
//         Namespace => "::",
//         DoubleSplat => "**",
//         BackwardLink => ".<",
//         FloorDiv => "//",
//         Concat => "++",
//         GreaterEq => ">=",
//         LessEq => "<=",
//         NotEq => "!=",
//         NotDistinctFrom => "?!=",
//         DistinctFrom => "?=",
//         Comma => ",",
//         OpenParen => "(",
//         CloseParen => ")",
//         OpenBracket => "[",
//         CloseBracket => "]",
//         OpenBrace => "{",
//         CloseBrace => "}",
//         Dot => ".",
//         Semicolon => ";",
//         Colon => ":",
//         Add => "+",
//         Sub => "-",
//         Mul => "*",
//         Div => "/",
//         Modulo => "%",
//         Pow => "^",
//         Less => "<",
//         Greater => ">",
//         Eq => "=",
//         Ampersand => "&",
//         Pipe => "|",
//         At => "@",

//         Ident | Keyword(_) => {
//             let text = &token.text[..];
//             match text {
//                 "named only" => "NAMED ONLY",
//                 "set annotation" => "SET ANNOTATION",
//                 "set type" => "SET TYPE",
//                 "extension package" => "EXTENSION PACKAGE",
//                 "order by" => "ORDER BY",

//                 _ => return None,
//             }
//         }
//         _ => return None,
//     })
// }

impl OpaqueToken {
    pub(super) fn inner(&self, py: Python) -> Token {
        self._inner(py).clone()
    }
}
