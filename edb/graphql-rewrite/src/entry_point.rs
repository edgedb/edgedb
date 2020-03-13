use std::collections::BTreeMap;

use combine::stream::{Positioned, StreamOnce};

use edb_graphql_parser::position::Pos;
use edb_graphql_parser::query::Definition;
use edb_graphql_parser::query::{Operation, InsertVars, InsertVarsKind};
use edb_graphql_parser::query::{Document, parse_query, ParseError};
use edb_graphql_parser::tokenizer::Kind::{StringValue, BlockString};
use edb_graphql_parser::tokenizer::{TokenStream, Token};
use edb_graphql_parser::common::unquote_string;

use crate::pytoken::{PyToken, PyTokenKind};
use crate::token_vec::TokenVec;


#[derive(Debug, PartialEq)]
pub enum Value {
    Str(String),
    Int(i32),
    Float(f64),
}

#[derive(Debug, PartialEq)]
pub struct Variable {
    pub value: Value,
    pub token: PyToken,
}

#[derive(Debug)]
pub enum Error {
    Lexing(String),
    Syntax(ParseError),
    NotFound(String),
    Assertion(String),
}


#[derive(Debug)]
pub struct Entry {
    pub key: String,
    pub variables: Vec<Variable>,
    pub defaults: BTreeMap<String, Variable>,
    pub tokens: Vec<PyToken>,
    pub end_pos: Pos,
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

fn token_array<'a>(s: &'a str)
    -> Result<(Vec<(Token<'a>, Pos)>, Pos), Error> {
    let mut lexer = TokenStream::new(s);
    let mut tokens = Vec::new();
    let mut pos = lexer.position();
    loop {
        match lexer.uncons() {
            Ok(token) => {
                tokens.push((token, pos));
                pos = lexer.position();
            }
            Err(ref e) if e == &combine::easy::Error::end_of_input() => break,
            Err(e) => panic!("Parse error at {}: {}", lexer.position(), e),
        }
    }
    return Ok((tokens, lexer.position()));
}

fn find_operation<'a>(document: &'a Document<'a, &'a str>,
    operation: &str)
    -> Option<&'a Operation<'a, &'a str>>
{
    for def in &document.definitions {
        let res = match def {
            Definition::Operation(ref op) if op.name == Some(operation) => op,
            _ => continue,
        };
        return Some(res);
    }
    return None;
}

fn insert_args(dest: &mut Vec<PyToken>, ins: &InsertVars, args: Vec<PyToken>) {
    use crate::pytoken::PyTokenKind as P;

    if args.is_empty() {
        return;
    }
    if ins.kind == InsertVarsKind::Query {
        dest.push(PyToken {
            kind: P::Name,
            value: "query".into(),
            position: None,
        });
    }
    if ins.kind != InsertVarsKind::Normal {
        dest.push(PyToken {
            kind: P::ParenL,
            value: "(".into(),
            position: None,
        });
    }
    dest.extend(args);
    if ins.kind != InsertVarsKind::Normal {
        dest.push(PyToken {
            kind: P::ParenR,
            value: ")".into(),
            position: None,
        });
    }
}

pub fn rewrite(operation: Option<&str>, s: &str) -> Result<Entry, Error> {
    use Value::*;
    use crate::pytoken::PyTokenKind as P;

    let document: Document<'_, &str> = parse_query(s).map_err(Error::Syntax)?;
    let oper = if let Some(oper_name) = operation {
        find_operation(&document, oper_name)
            .ok_or_else(|| Error::NotFound(
                format!("no operation {:?} found", operation)))?
    } else {
        let mut oper = None;
        for def in &document.definitions {
            match def {
                Definition::Operation(ref op) => {
                    if oper.is_some() {
                        return Err(Error::NotFound("Multiple operations \
                            found. Please specify operation name".into()))?;
                    } else {
                        oper = Some(op);
                    }
                }
                _ => continue,
            };
        }
        oper.ok_or_else(|| Error::NotFound("no operation found".into()))?
    };
    let (tokens, end_pos) = token_array(s)?;
    let mut src_tokens = TokenVec::new(tokens);
    let mut tokens = Vec::with_capacity(src_tokens.len());

    let mut variables = Vec::new();

    for tok in src_tokens.drain_to(oper.insert_variables.position.token) {
        tokens.push(PyToken::new(tok)?);
    }
    let mut args = Vec::new();
    let mut tmp = Vec::with_capacity(
        oper.selection_set.span.1.token - tokens.len());
    for tok in src_tokens.drain_to(oper.selection_set.span.0.token) {
        tmp.push(PyToken::new(tok)?);
    }
    for (token, pos) in src_tokens.drain_to(oper.selection_set.span.1.token) {
        match token.kind {
            StringValue | BlockString => {
                let varname = format!("_edb_arg__{}", variables.len());
                tmp.push(PyToken {
                    kind: P::Dollar,
                    value: "$".into(),
                    position: None,
                });
                tmp.push(PyToken {
                    kind: P::Name,
                    value: varname.clone().into(),
                    position: None,
                });
                variables.push(Variable {
                    token: PyToken::new((token, pos))?,
                    value: Str(unquote_string(token.value)?),
                });
                args.push(PyToken {
                    kind: P::Dollar,
                    value: "$".into(),
                    position: None,
                });
                args.push(PyToken {
                    kind: P::Name,
                    value: varname.into(),
                    position: None,
                });
                args.push(PyToken {
                    kind: P::Colon,
                    value: ":".into(),
                    position: None,
                });
                args.push(PyToken {
                    kind: P::Name,
                    value: "String".into(),
                    position: None,
                });
                args.push(PyToken {
                    kind: P::Bang,
                    value: "!".into(),
                    position: None,
                });
                continue;
            }
            _ => {}
        }
        tmp.push(PyToken::new((token, pos))?);
    }
    insert_args(&mut tokens, &oper.insert_variables, args);
    tokens.extend(tmp);

    for tok in src_tokens.drain(src_tokens.len()) {
        tokens.push(PyToken::new(tok)?);
    }

    return Ok(Entry {
        key: join_tokens(&tokens),
        variables,
        defaults: BTreeMap::new(),
        tokens,
        end_pos,
    })
}

fn join_tokens<'a, I: IntoIterator<Item=&'a PyToken>>(tokens: I) -> String {
    let mut buf = String::new();
    let mut needs_whitespace = false;
    for token in tokens {
        match (token.kind, needs_whitespace) {
            // space before puncutators is optional
            (PyTokenKind::ParenL, true) => {},
            (PyTokenKind::ParenR, true) => {},
            (PyTokenKind::Spread, true) => {},
            (PyTokenKind::Colon, true) => {},
            (PyTokenKind::Equals, true) => {},
            (PyTokenKind::At, true) => {},
            (PyTokenKind::BracketL, true) => {},
            (PyTokenKind::BracketR, true) => {},
            (PyTokenKind::BraceL, true) => {},
            (PyTokenKind::BraceR, true) => {},
            (PyTokenKind::Pipe, true) => {},
            (PyTokenKind::Bang, true) => {},
            (_, true) => buf.push(' '),
            (_, false) => {},
        }
        buf.push_str(&token.value);
        needs_whitespace = match token.kind {
            PyTokenKind::Dollar => false,
            PyTokenKind::Bang => false,
            PyTokenKind::ParenL => false,
            PyTokenKind::ParenR => false,
            PyTokenKind::Spread => false,
            PyTokenKind::Colon => false,
            PyTokenKind::Equals => false,
            PyTokenKind::At => false,
            PyTokenKind::BracketL => false,
            PyTokenKind::BracketR => false,
            PyTokenKind::BraceL => false,
            PyTokenKind::BraceR => false,
            PyTokenKind::Pipe => false,
            PyTokenKind::Int => true,
            PyTokenKind::Float => true,
            PyTokenKind::String => true,
            PyTokenKind::BlockString => true,
            PyTokenKind::Name => true,
            PyTokenKind::Eof => unreachable!(),
            PyTokenKind::Sof => unreachable!(),
        };
    }
    return buf;
}
