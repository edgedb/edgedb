use std::str::FromStr;

use bigdecimal::num_bigint::ToBigInt;
use bigdecimal::BigDecimal;

use crate::helpers::{unquote_bytes, unquote_string};
use crate::position::Pos;
use crate::tokenizer::{Error, Kind, Token, Tokenizer, Value, MAX_KEYWORD_LENGTH};

/// Applies additional validation to the tokens.
/// Combines multi-word keywords into single tokens.
pub struct Validator<'a> {
    pub inner: Tokenizer<'a>,

    pub(super) peeked: Option<Option<Result<Token<'a>, Error>>>,
    pub(super) keyword_buf: String,
}

impl<'a> Iterator for Validator<'a> {
    type Item = Result<Token<'a>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut token = match self.next_inner()? {
            Ok(t) => t,
            Err(e) => return Some(Err(e)),
        };

        token.value = match parse_value(&token) {
            Ok(x) => x,
            Err(e) => return Some(Err(Error::new(e).with_span(token.span))),
        };

        if let Some(text) = self.combine_multi_word_keywords(&token) {
            token.kind = Kind::Keyword;
            token.text = text.into();
            self.peeked = None;
        }

        Some(Ok(token))
    }
}

impl<'a> Validator<'a> {
    pub(super) fn new(inner: Tokenizer<'a>) -> Self {
        Validator {
            inner,
            peeked: None,
            keyword_buf: String::with_capacity(MAX_KEYWORD_LENGTH),
        }
    }

    /// Mimics behavior of [std::iter::Peekable]. We could use that, but it
    /// hides access to underlying iterator.
    fn next_inner(&mut self) -> Option<Result<Token<'a>, Error>> {
        if let Some(peeked) = self.peeked.take() {
            peeked
        } else {
            self.inner.next()
        }
    }

    /// Mimics behavior of [std::iter::Peekable]. We could use that, but it
    /// hides access to underlying iterator.
    fn peek(&mut self) -> &Option<Result<Token<'a>, Error>> {
        if self.peeked.is_none() {
            self.peeked = Some(self.inner.next());
        }

        self.peeked.as_ref().unwrap()
    }

    pub fn current_pos(&self) -> Pos {
        self.inner.current_pos()
    }

    fn combine_multi_word_keywords(&mut self, token: &Token) -> Option<&'static str> {
        if !matches!(token.kind, Kind::Ident | Kind::Keyword) {
            return None;
        }
        let text = &token.text;

        if text.len() > MAX_KEYWORD_LENGTH {
            return None;
        }

        self.keyword_buf.clear();
        self.keyword_buf.push_str(&text);
        self.keyword_buf.make_ascii_lowercase();
        match &self.keyword_buf[..] {
            "named" => {
                if self.peek_keyword("only") {
                    return Some("named only");
                }
            }
            "set" => {
                if self.peek_keyword("annotation") {
                    return Some("set annotation");
                }
                if self.peek_keyword("type") {
                    return Some("set type");
                }
            }
            "extension" => {
                if self.peek_keyword("package") {
                    return Some("extension package");
                }
            }
            "order" => {
                if self.peek_keyword("by") {
                    return Some("order by");
                }
            }
            _ => {}
        }
        return None;
    }

    fn peek_keyword(&mut self, kw: &str) -> bool {
        self.peek()
            .as_ref()
            .and_then(|res| res.as_ref().ok())
            .map(|t| {
                (t.kind == Kind::Ident || t.kind == Kind::Keyword)
                    && t.text.eq_ignore_ascii_case(kw)
            })
            .unwrap_or(false)
    }
}

pub fn parse_value(token: &Token<'_>) -> Result<Option<Value>, String> {
    use Kind::*;
    let text = &token.text;
    let string_value = match token.kind {
        Argument => {
            if text[1..].starts_with('`') {
                text[2..text.len() - 1].replace("``", "`")
            } else {
                text[1..].to_string()
            }
        }
        DecimalConst => {
            return text[..text.len() - 1]
                .replace("_", "")
                .parse()
                .map(Value::Decimal)
                .map(Some)
                .map_err(|e| format!("can't parse decimal: {}", e))
        }
        FloatConst => {
            return text
                .replace("_", "")
                .parse::<f64>()
                .map_err(|e| format!("can't parse std::float64: {}", e))
                .and_then(|num| {
                    if num == f64::INFINITY || num == -f64::INFINITY {
                        return Err("number is out of range for std::float64".to_string());
                    }
                    if num == 0.0 {
                        let mend = text.find(|c| c == 'e' || c == 'E').unwrap_or(text.len());
                        let mantissa = &text[..mend];
                        if mantissa.chars().any(|c| c != '0' && c != '.') {
                            return Err("number is out of range for std::float64".to_string());
                        }
                    }
                    Ok(num)
                })
                .map(Value::Float)
                .map(Some);
        }
        IntConst => {
            // We read unsigned here, because unary minus will only
            // be identified on the parser stage. And there is a number
            // -9223372036854775808 which can't be represented in
            // i64 as absolute (positive) value.
            // Python has no problem of representing such a positive
            // value, though.
            return u64::from_str(&text.replace("_", ""))
                .map(|x| Value::Int(x as i64))
                .map(Some)
                .map_err(|e| format!("error reading int: {}", e));
        }
        BigIntConst => {
            let dec = text[..text.len() - 1]
                .replace("_", "")
                .parse::<BigDecimal>()
                .map_err(|e| format!("error reading bigint: {}", e))?;
            // this conversion to decimal and back to string
            // fixes thing like `1e2n` which we support for bigints
            return Ok(Some(Value::BigInt(
                dec.to_bigint()
                    .ok_or_else(|| "number is not integer".to_string())?,
            )));
        }
        BinStr => {
            return unquote_bytes(&text).map(Value::Bytes).map(Some);
        }

        Str => unquote_string(&text)
            .map_err(|s| s.to_string())?
            .to_string(),
        BacktickName => text[1..text.len() - 1].replace("``", "`"),
        Ident | Keyword => text.to_string(),
        Substitution => text[2..text.len() - 1].to_string(),
        _ => return Ok(None),
    };
    Ok(Some(Value::String(string_value)))
}
