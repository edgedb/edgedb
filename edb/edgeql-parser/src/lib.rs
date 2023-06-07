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

use bigdecimal::{num_bigint::BigInt, BigDecimal};
use position::{Pos, Span};
use std::borrow::Cow;
use tokenizer::{Kind, TokenStream, MAX_KEYWORD_LENGTH};

#[derive(Debug, Clone)]
pub struct CowToken<'a> {
    pub kind: Kind,
    pub text: Cow<'a, str>,

    /// Parsed during validation.
    pub value: Option<TokenValue>,

    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TokenValue {
    String(String),
    Int(i64),
    Float(f64),
    Bytes(Vec<u8>),
    BigInt(BigInt),
    Decimal(BigDecimal),
}

#[derive(Debug, Clone)]
pub struct Error {
    pub message: String,
    pub span: Span,
}

pub struct TokenStream2<'a> {
    inner: TokenStream<'a>,
    peeked: Option<Option<Result<CowToken<'a>, Error>>>,

    keyword_buf: String,
}

impl<'a> TokenStream2<'a> {
    pub fn new(source: &'a str) -> Self {
        TokenStream2 {
            inner: TokenStream::new(source),
            peeked: None,
            keyword_buf: String::with_capacity(MAX_KEYWORD_LENGTH),
        }
    }

    fn next_inner(&mut self) -> Option<Result<CowToken<'a>, Error>> {
        let res = if let Some(peeked) = self.peeked.take() {
            peeked
        } else {
            self.inner.next()
        };

        self.peeked = Some(self.inner.next());

        res
    }

    pub fn current_pos(&self) -> Pos {
        self.inner.current_pos()
    }
}

impl<'a> Iterator for TokenStream2<'a> {
    type Item = Result<CowToken<'a>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut token = match self.next_inner()? {
            Ok(t) => t,
            Err(e) => return Some(Err(e)),
        };

        token.value = match validation::parse_value(&token) {
            Ok(x) => x,
            Err(e) => return Some(Err(Error::new(e).with_span(token.span))),
        };

        if let Some(text) =
            validation::combine_multi_word_keywords(&token, &self.peeked, &mut self.keyword_buf)
        {
            token.kind = Kind::Keyword;
            token.text = text.into();
            self.peeked = None;
        }

        Some(Ok(token))
    }
}

impl Error {
    pub fn new<S: ToString>(message: S) -> Self {
        let empty = Pos {
            line: 0,
            column: 0,
            offset: 0,
        };
        Error {
            message: message.to_string(),
            span: Span {
                start: empty.clone(),
                end: empty,
            },
        }
    }

    pub fn with_span(mut self, span: Span) -> Self {
        self.span = span;
        self
    }
}
