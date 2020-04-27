use std::collections::BTreeMap;

use combine::stream::{Positioned, StreamOnce};

use edb_graphql_parser::position::Pos;
use edb_graphql_parser::query::Definition;
use edb_graphql_parser::query::{Operation, InsertVars, InsertVarsKind};
use edb_graphql_parser::query::{Document, parse_query, ParseError};
use edb_graphql_parser::tokenizer::Kind::{StringValue, BlockString};
use edb_graphql_parser::tokenizer::Kind::{IntValue, Punctuator, Name};
use edb_graphql_parser::tokenizer::{TokenStream, Token};
use edb_graphql_parser::common::{unquote_string, Type};

use crate::pytoken::{PyToken, PyTokenKind};
use crate::token_vec::TokenVec;


#[derive(Debug, PartialEq)]
pub enum Value {
    Str(String),
    Int32(i32),
    Int64(i64),
    BigInt(String),
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
    use edb_graphql_parser::query::Value as G;
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
    let (all_src_tokens, end_pos) = token_array(s)?;
    let mut src_tokens = TokenVec::new(&all_src_tokens);
    let mut tokens = Vec::with_capacity(src_tokens.len());

    let mut variables = Vec::new();
    let mut defaults = BTreeMap::new();

    for var in &oper.variable_definitions {
        if let Some(ref dvalue) = var.default_value {
            let value = match (&dvalue.value, &var.var_type) {
                | (G::String(ref s), Type::NamedType(t)) if t == &"String"
                => Str(s.clone()),
                | (G::String(ref s), Type::NonNullType(t))
                if matches!(**t, Type::NamedType(it) if it == "String")
                => Str(s.clone()),
                // other types are unsupported
                _ => continue,
            };
            for tok in src_tokens.drain_to(dvalue.span.0.token) {
                tokens.push(PyToken::new(tok)?);
            }
            if !matches!(var.var_type, Type::NonNullType(..)) {
                tokens.push(PyToken {
                    kind: P::Bang,
                    value: "!".into(),
                    position: None,
                });
            }
            // first token is needed for errors, others are discarded
            let pair = src_tokens.drain_to(dvalue.span.1.token)
                .next().expect("at least one token of default value");
            defaults.insert(var.name.to_owned(), Variable {
                value,
                token: PyToken::new(pair)?,
            });
        }
    }
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
                    token: PyToken::new(&(*token, *pos))?,
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
            IntValue => {
                if token.value == "1" {
                    if pos.token > 2
                       && all_src_tokens[pos.token-1].0.kind == Punctuator
                       && all_src_tokens[pos.token-1].0.value == ":"
                       && all_src_tokens[pos.token-2].0.kind == Name
                       && all_src_tokens[pos.token-2].0.value == "first"
                    {
                        // skip `first: 1` as this is used to fetch singleton
                        // properties from queries where literal `LIMIT 1`
                        // should be present
                        tmp.push(PyToken::new(&(*token, *pos))?);
                        continue;
                    }
                }
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
                let (value, typ) = if let Ok(val) = token.value.parse::<i64>()
                {
                    if val <= i32::max_value() as i64
                        && val >= i32::min_value() as i64
                    {
                        (Value::Int32(val as i32), "Int")
                    } else {
                        (Value::Int64(val), "Int64")
                    }
                } else {
                    (Value::BigInt(token.value.into()), "Bigint")
                };
                variables.push(Variable {
                    token: PyToken::new(&(*token, *pos))?,
                    value,
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
                    value: typ.into(),
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
        tmp.push(PyToken::new(&(*token, *pos))?);
    }
    insert_args(&mut tokens, &oper.insert_variables, args);
    tokens.extend(tmp);

    for tok in src_tokens.drain(src_tokens.len()) {
        tokens.push(PyToken::new(tok)?);
    }

    return Ok(Entry {
        key: join_tokens(&tokens),
        variables,
        defaults,
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
