use edb_graphql_parser::common::{unquote_block_string, unquote_string};
use edb_graphql_parser::position::Pos;
use edb_graphql_parser::tokenizer::Token;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};
use std::borrow::Cow;

use crate::py_exception::LexingError;
use crate::rewrite::Error;

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum PyTokenKind {
    Sof,
    Eof,
    Bang,
    Dollar,
    ParenL,
    ParenR,
    Spread,
    Colon,
    Equals,
    At,
    BracketL,
    BracketR,
    BraceL,
    Pipe,
    BraceR,
    Name,
    Int,
    Float,
    String,
    BlockString,
}

#[derive(Debug, PartialEq, Clone)]
pub struct PyToken {
    pub kind: PyTokenKind,
    pub value: Cow<'static, str>,
    pub position: Option<Pos>,
}

impl PyToken {
    pub fn new((token, position): &(Token<'_>, Pos)) -> Result<PyToken, Error> {
        use edb_graphql_parser::tokenizer::Kind::*;
        use PyTokenKind as T;

        let (kind, value) = match (token.kind, token.value) {
            (IntValue, val) => (T::Int, Cow::Owned(val.into())),
            (FloatValue, val) => (T::Float, Cow::Owned(val.into())),
            (StringValue, val) => (T::String, Cow::Owned(val.into())),
            (BlockString, val) => (T::BlockString, Cow::Owned(val.into())),
            (Name, val) => (T::Name, Cow::Owned(val.into())),
            (Punctuator, "!") => (T::Bang, "!".into()),
            (Punctuator, "$") => (T::Dollar, "$".into()),
            (Punctuator, "(") => (T::ParenL, "(".into()),
            (Punctuator, ")") => (T::ParenR, ")".into()),
            (Punctuator, "...") => (T::Spread, "...".into()),
            (Punctuator, ":") => (T::Colon, ":".into()),
            (Punctuator, "=") => (T::Equals, "=".into()),
            (Punctuator, "@") => (T::At, "@".into()),
            (Punctuator, "[") => (T::BracketL, "[".into()),
            (Punctuator, "]") => (T::BracketR, "]".into()),
            (Punctuator, "{") => (T::BraceL, "{".into()),
            (Punctuator, "}") => (T::BraceR, "}".into()),
            (Punctuator, "|") => (T::Pipe, "|".into()),
            (Punctuator, _) => Err(Error::Assertion("unsupported punctuator".into()))?,
        };
        Ok(PyToken {
            kind,
            value,
            position: Some(*position),
        })
    }
}

pub fn convert_tokens(
    py: Python,
    tokens: &[PyToken],
    end_pos: &Pos,
    kinds: PyObject,
) -> PyResult<PyObject> {
    use PyTokenKind as K;

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

    let mut elems: Vec<PyObject> = Vec::with_capacity(tokens.len());

    let start_of_file = [
        sof.clone_ref(py),
        0u32.into_py(py),
        0u32.into_py(py),
        0u32.into_py(py),
        0u32.into_py(py),
        py.None(),
    ];
    elems.push(PyTuple::new(py, &start_of_file).into());

    for token in tokens {
        let (kind, value) = match token.kind {
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
            K::Name => (name.clone_ref(py), token.value.clone().into_py(py)),
            K::Int => (int.clone_ref(py), token.value.clone().into_py(py)),
            K::Float => (float.clone_ref(py), token.value.clone().into_py(py)),
            K::String => {
                // graphql-core 3 receives unescaped strings from the lexer
                let v = unquote_string(&token.value)
                    .map_err(|e| LexingError::new_err(e.to_string()))?
                    .into_py(py);
                (string.clone_ref(py), v)
            }
            K::BlockString => {
                // graphql-core 3 receives unescaped strings from the lexer
                let v = unquote_block_string(&token.value)
                    .map_err(|e| LexingError::new_err(e.to_string()))?
                    .into_py(py);
                (block_string.clone_ref(py), v)
            }
        };
        let token_tuple = [
            kind,
            token.position.map(|x| x.character).into_py(py),
            token
                .position
                .map(|x| x.character + token.value.chars().count())
                .into_py(py),
            token.position.map(|x| x.line).into_py(py),
            token.position.map(|x| x.column).into_py(py),
            value,
        ];
        elems.push(PyTuple::new(py, &token_tuple).into());
    }
    elems.push(
        PyTuple::new(
            py,
            &[
                eof.clone_ref(py),
                end_pos.character.into_py(py),
                end_pos.line.into_py(py),
                end_pos.column.into_py(py),
                end_pos.character.into_py(py),
                py.None(),
            ],
        )
        .into(),
    );
    Ok(PyList::new(py, &elems[..]).into())
}
