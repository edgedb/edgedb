use std::collections::BTreeMap;

use combine::stream::{Positioned, StreamOnce};

use crate::query::{Document, parse_query, ParseError};
use crate::query::VariableDefinition;
use crate::query::Operation;
use crate::position::Pos;
use crate::tokenizer::{self, TokenStream, Token};
use crate::tokenizer::Kind::{StringValue, BlockString, Name, Punctuator};

const ARGUMENT_NAMES: [&str; 21] = [
    "_edb_arg__0",
    "_edb_arg__1",
    "_edb_arg__2",
    "_edb_arg__3",
    "_edb_arg__4",
    "_edb_arg__5",
    "_edb_arg__6",
    "_edb_arg__7",
    "_edb_arg__8",
    "_edb_arg__9",
    "_edb_arg__10",
    "_edb_arg__11",
    "_edb_arg__12",
    "_edb_arg__13",
    "_edb_arg__14",
    "_edb_arg__15",
    "_edb_arg__16",
    "_edb_arg__17",
    "_edb_arg__18",
    "_edb_arg__19",
    "_edb_arg__20",
];

const MAX_ARGUMENTS: usize = ARGUMENT_NAMES.len();


#[derive(Debug, PartialEq)]
pub enum Variable {
    Str(String),
    Int(i32),
    Float(f64),
}

#[derive(Debug)]
pub enum Error {
    Lexing(String),
    Syntax(ParseError),
    Assertion(String),
}


#[derive(Debug)]
pub struct Entry {
    pub key: String,
    pub variables: Vec<Variable>,
    pub defaults: BTreeMap<String, Variable>,
}

impl From<ParseError> for Error {
    fn from(v: ParseError) -> Error {
        Error::Syntax(v)
    }
}

impl<'a> From<combine::easy::Error<Token<'a>,Token<'a>>> for Error {
    fn from(v: combine::easy::Error<Token<'a>,Token<'a>>) -> Error {
        Error::Lexing(v.to_string())
    }
}


pub fn rewrite(operation: Option<&str>, s: &str) -> Result<Entry, Error> {
    let document: Document<'_, &str> = parse_query(s).map_err(Error::Syntax)?;
    let mut lexer = TokenStream::new(s);
    let mut src_tokens = Vec::new();
    let mut pos = lexer.position();
    loop {
        match lexer.uncons() {
            Ok(token) => {
                src_tokens.push((token, pos));
                pos = lexer.position();
            }
            Err(ref e) if e == &combine::easy::Error::end_of_input() => break,
            Err(e) => panic!("Parse error at {}: {}", lexer.position(), e),
        }
    }
    todo!();
}

fn join_tokens<'a, I: IntoIterator<Item=&'a Token<'a>>>(tokens: I) -> String {
    let mut buf = String::new();
    let mut needs_whitespace = false;
    for token in tokens {
        match (token.kind, needs_whitespace) {
            (Punctuator, true) if token.value != "$" => {},
            (_, true) => buf.push(' '),
            (_, false) => {},
        }
        buf.push_str(token.value);
        needs_whitespace = token.kind != Punctuator;
    }
    return buf;
}
