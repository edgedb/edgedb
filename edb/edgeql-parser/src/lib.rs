pub mod ast;
pub mod expr;
pub mod hash;
#[cfg(feature = "python")]
pub mod into_python;
pub mod keywords;
pub mod position;
pub mod preparser;
pub mod schema_file;
pub mod tokenizer;
pub mod utils;
pub mod validation;

use position::{Pos, Span};
use std::{borrow::Cow, iter::Peekable};
use tokenizer::{Kind, SpannedToken, TokenStream, MAX_KEYWORD_LENGTH};

#[derive(Debug, Clone)]
pub struct CowToken<'a> {
    pub kind: Kind,
    pub text: Cow<'a, str>,
    pub value: Option<TokenValue>,
    pub start: Pos,
    pub end: Pos,
}

#[derive(Debug, Clone)]
pub enum TokenValue {
    String(String),
    Int(u64),
    Float(f64),
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct Error {
    pub message: String,
    pub span: Span,
}

pub struct TokenStream2<'a> {
    inner: Peekable<TokenStream<'a>>,
    keyword_buf: String,
}

impl <'a> TokenStream2<'a> {
    pub fn new(source: &'a str) -> Self {
        TokenStream2 {
            inner: TokenStream::new(source).peekable(),
            keyword_buf: String::with_capacity(MAX_KEYWORD_LENGTH),
        }
    }
}

impl<'a> Iterator for TokenStream2<'a> {
    type Item = Result<CowToken<'a>, Error>;

    fn next(&mut self) -> Option<Self::Item> {

        let token = match self.inner.next()? {
            Ok(t) => t,
            Err(e) => return Some(Err(e)),
        };

        let value = match validation::parse_value(&token) {
            Ok(None) => None,
            Ok(Some(x)) => Some(x),
            Err(e) => return Some(Err(e)),
        };

        let text = validation::combine_multi_word_keywords(&token, &mut self.inner, &mut self.keyword_buf).unwrap_or(token.token.text);

        Some(Ok(CowToken {
            kind: token.token.kind,
            text: text.into(),
            value,
            start: token.start,
            end: token.end,
        }))
    }
}


impl<'a, 'b: 'a> From<&'a SpannedToken<'b>> for CowToken<'b> {
    fn from(t: &'a SpannedToken<'b>) -> CowToken<'b> {
        CowToken {
            kind: t.token.kind,
            text: t.token.text.into(),
            value: None,
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
