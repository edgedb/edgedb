use std::borrow::Cow;

use crate::tokenizer::Token;
use crate::entry_point::Error;
use crate::position::Pos;


#[derive(Debug, PartialEq, Copy, Clone)]
pub enum PyTokenKind {
    Eof = 1,
    Bang = 2,
    Dollar = 3,
    ParenL = 4,
    ParenR = 5,
    Spread = 6,
    Colon = 7,
    Equals = 8,
    At = 9,
    BracketL = 10,
    BracketR = 11,
    BraceL = 12,
    Pipe = 13,
    BraceR = 14,
    Name = 15,
    Variable = 16,  // looks unused in graphql-core
    Int = 17,
    Float = 18,
    String = 19,
}


#[derive(Debug, PartialEq, Clone)]
pub struct PyToken {
    pub kind: PyTokenKind,
    pub value: Cow<'static, str>,
    pub position: Option<Pos>,
}

impl PyToken {
    pub fn new((token, position): (Token<'_>, Pos)) -> Result<PyToken, Error> {
        use crate::tokenizer::Kind::*;
        use PyTokenKind as T;

        let (kind, value) = match (token.kind, token.value) {
            (IntValue, val) => (T::Int, Cow::Owned(val.into())),
            (FloatValue, val) => (T::Float, Cow::Owned(val.into())),
            (StringValue, val) => (T::String, Cow::Owned(val.into())),
            (BlockString, val) => (T::String, Cow::Owned(val.into())),
            (Name, val) => (T::Name, Cow::Owned(val.into())),
            (Punctuator, "!") => (T::Bang, "!".into()),
            (Punctuator, "$") => (T::Dollar, "$".into()),
            (Punctuator, "(") => (T::ParenL, "(".into()),
            (Punctuator, ")") => (T::ParenR, ")".into()),
            (Punctuator, "..") => (T::Spread, "..".into()),
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
            position: Some(position),
        })
    }
}
