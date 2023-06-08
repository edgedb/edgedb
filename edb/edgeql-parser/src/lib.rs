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
use tokenizer::{Kind};

#[derive(Debug, Clone)]
pub struct Token<'a> {
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
