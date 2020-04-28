use std::borrow::Cow;

use edb_graphql_parser::tokenizer::Token;
use edb_graphql_parser::position::Pos;

use crate::entry_point::Error;


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
    pub fn new((token, position): &(Token<'_>, Pos)) -> Result<PyToken, Error>
    {
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
            (Punctuator, _)
            => Err(Error::Assertion("unsupported punctuator".into()))?,
        };
        Ok(PyToken {
            kind, value,
            position: Some(*position),
        })
    }
}
