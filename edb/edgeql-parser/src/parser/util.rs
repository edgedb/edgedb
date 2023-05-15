use std::collections::hash_map::RandomState;
use std::collections::HashSet;
use std::ops::Range;

use chumsky::prelude::*;

use crate::position::Pos;
use crate::tokenizer::{Kind as Token, Token as TokenData, TokenStream};

use super::Error;

pub fn token<'a>(knd: Token) -> impl Parser<TokenData<'a>, &'a str, Error = Simple<TokenData<'a>>> {
    filter_map(move |span, x| match x {
        TokenData { kind, value } if kind == knd => Ok(value),
        _ => Err(chumsky::Error::expected_input_found(
            span,
            [Some(TokenData {
                kind: knd,
                value: "",
            })]
            .into_iter(),
            Some(x),
        )),
    })
}

pub fn token_choice<'a, const N: usize>(
    kinds: [Token; N],
) -> impl Parser<TokenData<'a>, &'a str, Error = Simple<TokenData<'a>>> {
    let kinds: HashSet<_, RandomState> = HashSet::from_iter(kinds);
    select! {
        TokenData { kind, value } if kinds.contains(&kind) => value
    }
}

pub fn keyword<'a>(
    kw: &'static str,
) -> impl Parser<TokenData<'a>, &'a str, Error = Simple<TokenData<'a>>> {
    filter_map(move |span, x| match x {
        TokenData { kind, value } if kind == Token::Keyword && value.to_lowercase() == kw => {
            Ok(value)
        }
        _ => Err(chumsky::Error::expected_input_found(
            span,
            [Some(TokenData {
                kind: Token::Keyword,
                value: kw,
            })]
            .into_iter(),
            Some(x),
        )),
    })
}

pub fn ident(ident: &'_ str) -> impl Parser<TokenData<'_>, String, Error = Simple<TokenData<'_>>> {
    select! {
        TokenData { kind, value } if kind == Token::Keyword && value == ident => value.to_string()
    }
}

pub fn convert_errors(errors: Vec<Simple<TokenData>>) -> Vec<Error> {
    errors
        .into_iter()
        .map(|e| {
            let span = e.span();
            let span = (span.start as u64)..(span.end as u64);

            let found = e
                .found()
                .map(|x| x.to_string())
                .unwrap_or_else(|| "end of input".to_string());

            let message = match e.expected().len() {
                1 => format!(
                    "found {found} but expected {}",
                    match e.expected().next().unwrap() {
                        Some(x) => format!("{:}", x.to_string()),
                        None => "end of input".to_string(),
                    },
                ),
                2..=6_ => {
                    format!(
                        "found {found} but expected one of {}",
                        e.expected()
                            .map(|expected| match expected {
                                Some(x) => format!("{:}", x.to_string()),
                                None => "end of input".to_string(),
                            })
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                }
                _ => {
                    format!("unexpected {found}")
                }
            };

            Error { message, span }
        })
        .collect()
}

pub fn tokenize(
    text: &str,
) -> Result<(Range<usize>, Vec<(TokenData<'_>, Range<usize>)>), Vec<Error>> {
    let mut token_stream = TokenStream::new(text);

    // TODO: don't store tokens in the string
    let mut tokens = Vec::new();
    for res in &mut token_stream {
        match res {
            Ok(t) => tokens.push((t.token, into_span(t.start, t.end))),
            Err(e) => {
                // TODO: this should bubble up as TokenizerError
                let span_start = token_stream.current_pos().offset;
                let span = span_start..span_start;
                return Err(vec![Error {
                    message: e.to_string(),
                    span,
                }]);
            }
        }
    }
    let end_span = (text.len() as usize)..(text.len() as usize);
    Ok((end_span, tokens))
}

fn into_span(start: Pos, end: Pos) -> Range<usize> {
    (start.offset as usize)..(end.offset as usize)
}

pub fn prepend<Y, V: Clone>(val: V) -> impl Fn(Y) -> (V, Y) {
    move |y| (val.clone(), y)
}

pub fn append<X, V: Clone>(val: V) -> impl Fn(X) -> (X, V) {
    move |x| (x, val.clone())
}

pub fn map_first<A, B, Y, F>(f: F) -> impl Fn((A, Y)) -> (B, Y)
where
    F: Fn(A) -> B,
{
    move |(a, y)| (f(a), y)
}

pub fn map_second<X, A, B, F>(f: F) -> impl Fn((X, A)) -> (X, B)
where
    F: Fn(A) -> B,
{
    move |(x, a)| (x, f(a))
}
