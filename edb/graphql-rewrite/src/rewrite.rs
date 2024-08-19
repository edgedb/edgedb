use std::collections::{BTreeMap, BTreeSet, HashSet};

use combine::stream::{Positioned, StreamOnce};

use edb_graphql_parser::common::{unquote_string, Type, Value as GqlValue};
use edb_graphql_parser::position::Pos;
use edb_graphql_parser::query::{parse_query, Document, ParseError};
use edb_graphql_parser::query::{Definition, Directive};
use edb_graphql_parser::query::{InsertVars, InsertVarsKind, Operation};
use edb_graphql_parser::tokenizer::Kind::{BlockString, StringValue};
use edb_graphql_parser::tokenizer::Kind::{FloatValue, IntValue};
use edb_graphql_parser::tokenizer::Kind::{Name, Punctuator};
use edb_graphql_parser::tokenizer::{Token, TokenStream};
use edb_graphql_parser::visitor::Visit;

use crate::py_token::{PyToken, PyTokenKind};
use crate::token_vec::TokenVec;

#[derive(Debug, PartialEq)]
pub enum Value {
    Str(String),
    Int32(i32),
    Int64(i64),
    BigInt(String),
    Decimal(String),
    Boolean(bool),
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
    Query(String),
}

#[derive(Debug)]
pub struct Entry {
    pub key: String,
    pub key_vars: BTreeSet<String>,
    pub variables: Vec<Variable>,
    pub defaults: BTreeMap<String, Variable>,
    pub tokens: Vec<PyToken>,
    pub end_pos: Pos,
}

pub fn rewrite(operation: Option<&str>, s: &str) -> Result<Entry, Error> {
    use crate::py_token::PyTokenKind as P;
    use edb_graphql_parser::query::Value as G;
    use Value::*;

    let document: Document<'_, &str> = parse_query(s).map_err(Error::Syntax)?;
    let oper = if let Some(oper_name) = operation {
        find_operation(&document, oper_name)
            .ok_or_else(|| Error::NotFound(format!("no operation {:?} found", operation)))?
    } else {
        let mut oper = None;
        for def in &document.definitions {
            match def {
                Definition::Operation(ref op) => {
                    if oper.is_some() {
                        Err(Error::NotFound(
                            "Multiple operations \
                            found. Please specify operation name"
                                .into(),
                        ))?;
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
    let mut key_vars = BTreeSet::new();
    let mut value_positions = HashSet::new();

    visit_directives(&mut key_vars, &mut value_positions, oper);

    for var in &oper.variable_definitions {
        if var.name.starts_with("_edb_arg__") {
            return Err(Error::Query(
                "Variables starting with '_edb_arg__' are prohibited".into(),
            ));
        }
        if let Some(ref dvalue) = var.default_value {
            let value = match (&dvalue.value, type_name(&var.var_type)) {
                (G::String(ref s), Some("String")) => Str(s.clone()),
                (G::Int(ref s), Some("Int")) | (G::Int(ref s), Some("Int32")) => {
                    let value = match s.as_i64() {
                        Some(v) if v <= i32::MAX as i64 && v >= i32::MIN as i64 => v,
                        // Ignore bad values. Let graphql solver handle that
                        _ => continue,
                    };
                    Int32(value as i32)
                }
                (G::Int(ref s), Some("Int64")) => {
                    let value = match s.as_i64() {
                        Some(v) => v,
                        // Ignore bad values. Let graphql solver handle that
                        _ => continue,
                    };
                    Int64(value)
                }
                (G::Int(ref s), Some("Bigint")) => BigInt(s.as_bigint().to_string()),
                (G::Float(s), Some("Float")) => Decimal(s.clone()),
                (G::Float(s), Some("Decimal")) => Decimal(s.clone()),
                (G::Boolean(s), Some("Boolean")) => Boolean(*s),
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
            let pair = src_tokens
                .drain_to(dvalue.span.1.token)
                .next()
                .expect("at least one token of default value");
            defaults.insert(
                var.name.to_owned(),
                Variable {
                    value,
                    token: PyToken::new(pair)?,
                },
            );
        }
    }
    for tok in src_tokens.drain_to(oper.insert_variables.position.token) {
        tokens.push(PyToken::new(tok)?);
    }
    let mut args = Vec::new();
    let mut tmp = Vec::with_capacity(oper.selection_set.span.1.token - tokens.len());
    for tok in src_tokens.drain_to(oper.selection_set.span.0.token) {
        tmp.push(PyToken::new(tok)?);
    }
    for (token, pos) in src_tokens.drain_to(oper.selection_set.span.1.token) {
        match token.kind {
            StringValue | BlockString => {
                let var_name = format!("_edb_arg__{}", variables.len());
                tmp.push(PyToken {
                    kind: P::Dollar,
                    value: "$".into(),
                    position: None,
                });
                tmp.push(PyToken {
                    kind: P::Name,
                    value: var_name.clone().into(),
                    position: None,
                });
                variables.push(Variable {
                    token: PyToken::new(&(*token, *pos))?,
                    value: Str(unquote_string(token.value)?),
                });
                push_var_definition(&mut args, &var_name, "String");
                continue;
            }
            IntValue => {
                if token.value == "1"
                    && pos.token > 2
                    && all_src_tokens[pos.token - 1].0.kind == Punctuator
                    && all_src_tokens[pos.token - 1].0.value == ":"
                    && all_src_tokens[pos.token - 2].0.kind == Name
                    && all_src_tokens[pos.token - 2].0.value == "first"
                {
                    // skip `first: 1` as this is used to fetch singleton
                    // properties from queries where literal `LIMIT 1`
                    // should be present
                    tmp.push(PyToken::new(&(*token, *pos))?);
                    continue;
                }
                let var_name = format!("_edb_arg__{}", variables.len());
                tmp.push(PyToken {
                    kind: P::Dollar,
                    value: "$".into(),
                    position: None,
                });
                tmp.push(PyToken {
                    kind: P::Name,
                    value: var_name.clone().into(),
                    position: None,
                });
                let (value, typ) = if let Ok(val) = token.value.parse::<i64>() {
                    if val <= i32::MAX as i64 && val >= i32::MIN as i64 {
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
                push_var_definition(&mut args, &var_name, typ);
                continue;
            }
            FloatValue => {
                let var_name = format!("_edb_arg__{}", variables.len());
                tmp.push(PyToken {
                    kind: P::Dollar,
                    value: "$".into(),
                    position: None,
                });
                tmp.push(PyToken {
                    kind: P::Name,
                    value: var_name.clone().into(),
                    position: None,
                });
                variables.push(Variable {
                    token: PyToken::new(&(*token, *pos))?,
                    value: Value::Decimal(token.value.to_string()),
                });
                push_var_definition(&mut args, &var_name, "Decimal");
                continue;
            }
            Name if token.value == "true" || token.value == "false" => {
                let var_name = format!("_edb_arg__{}", variables.len());
                if value_positions.contains(&pos.token) {
                    key_vars.insert(var_name.clone());
                }
                tmp.push(PyToken {
                    kind: P::Dollar,
                    value: "$".into(),
                    position: None,
                });
                tmp.push(PyToken {
                    kind: P::Name,
                    value: var_name.clone().into(),
                    position: None,
                });
                variables.push(Variable {
                    token: PyToken::new(&(*token, *pos))?,
                    value: Value::Boolean(token.value == "true"),
                });
                push_var_definition(&mut args, &var_name, "Boolean");
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

    Ok(Entry {
        key: join_tokens(&tokens),
        key_vars,
        variables,
        defaults,
        tokens,
        end_pos,
    })
}

impl From<ParseError> for Error {
    fn from(v: ParseError) -> Error {
        Error::Syntax(v)
    }
}

impl<'a> From<combine::easy::Error<Token<'a>, Token<'a>>> for Error {
    fn from(v: combine::easy::Error<Token<'a>, Token<'a>>) -> Error {
        Error::Lexing(v.to_string())
    }
}

fn token_array(s: &str) -> Result<(Vec<(Token, Pos)>, Pos), Error> {
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
    Ok((tokens, lexer.position()))
}

fn find_operation<'a>(
    document: &'a Document<'a, &'a str>,
    operation: &str,
) -> Option<&'a Operation<'a, &'a str>> {
    for def in &document.definitions {
        let res = match def {
            Definition::Operation(ref op) if op.name == Some(operation) => op,
            _ => continue,
        };
        return Some(res);
    }
    None
}

fn insert_args(dest: &mut Vec<PyToken>, ins: &InsertVars, args: Vec<PyToken>) {
    use crate::py_token::PyTokenKind as P;

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

fn type_name<'x>(var_type: &'x Type<'x, &'x str>) -> Option<&'x str> {
    match var_type {
        Type::NamedType(t) => Some(t),
        Type::NonNullType(b) => type_name(b),
        _ => None,
    }
}

fn push_var_definition(args: &mut Vec<PyToken>, var_name: &str, var_type: &'static str) {
    use crate::py_token::PyTokenKind as P;

    args.push(PyToken {
        kind: P::Dollar,
        value: "$".into(),
        position: None,
    });
    args.push(PyToken {
        kind: P::Name,
        value: var_name.to_owned().into(),
        position: None,
    });
    args.push(PyToken {
        kind: P::Colon,
        value: ":".into(),
        position: None,
    });
    args.push(PyToken {
        kind: P::Name,
        value: var_type.into(),
        position: None,
    });
    args.push(PyToken {
        kind: P::Bang,
        value: "!".into(),
        position: None,
    });
}

fn visit_directives<'x>(
    key_vars: &mut BTreeSet<String>,
    value_positions: &mut HashSet<usize>,
    oper: &'x Operation<'x, &'x str>,
) {
    for dir in oper.selection_set.visit::<Directive<_>>() {
        if dir.name == "include" || dir.name == "skip" {
            for arg in &dir.arguments {
                match arg.value {
                    GqlValue::Variable(vname) => {
                        key_vars.insert(vname.to_string());
                    }
                    GqlValue::Boolean(_) => {
                        value_positions.insert(arg.value_position.token);
                    }
                    _ => {}
                }
            }
        }
    }
}

fn join_tokens<'a, I: IntoIterator<Item = &'a PyToken>>(tokens: I) -> String {
    let mut buf = String::new();
    let mut needs_whitespace = false;
    for token in tokens {
        match (token.kind, needs_whitespace) {
            // space before puncutators is optional
            (PyTokenKind::ParenL, true) => {}
            (PyTokenKind::ParenR, true) => {}
            (PyTokenKind::Spread, true) => {}
            (PyTokenKind::Colon, true) => {}
            (PyTokenKind::Equals, true) => {}
            (PyTokenKind::At, true) => {}
            (PyTokenKind::BracketL, true) => {}
            (PyTokenKind::BracketR, true) => {}
            (PyTokenKind::BraceL, true) => {}
            (PyTokenKind::BraceR, true) => {}
            (PyTokenKind::Pipe, true) => {}
            (PyTokenKind::Bang, true) => {}
            (_, true) => buf.push(' '),
            (_, false) => {}
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
    buf
}
