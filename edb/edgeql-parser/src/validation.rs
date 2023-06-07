use std::iter::Peekable;
use std::str::FromStr;

use bigdecimal::num_bigint::ToBigInt;
use bigdecimal::BigDecimal;

use crate::TokenValue;
use crate::tokenizer::{Kind, SpannedToken, TokenStream, MAX_KEYWORD_LENGTH};
use crate::utils::bytes::unquote_bytes;
use crate::utils::strings::unquote_string;


pub fn parse_value(token: &SpannedToken) -> Result<Option<TokenValue>, String> {
    use Kind::*;
    let text = token.token.text;
    let string_value = match token.token.kind {
        Argument => {
            if text[1..].starts_with('`') {
                text[2..text.len() - 1].replace("``", "`")
            } else {
                text[1..].to_string()
            }
        }
        DecimalConst => text[..text.len() - 1].replace("_", ""),
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
                .map(TokenValue::Float)
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
                .map(TokenValue::Int)
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
            dec.to_bigint()
                .ok_or_else(|| "number is not integer".to_string())?
                .to_str_radix(16)
        }
        BinStr => {
            return unquote_bytes(text).map(TokenValue::Bytes).map(Some);
        }

        Str => unquote_string(text).map_err(|s| s.to_string())?.to_string(),
        BacktickName => text[1..text.len() - 1].replace("``", "`"),
        Ident | Keyword => text.to_string(),
        Substitution => text[2..text.len() - 1].to_string(),
        _ => return Ok(None),
    };
    Ok(Some(TokenValue::String(string_value)))
}

pub fn combine_multi_word_keywords(
    token: &SpannedToken,
    tok_iter: &mut Peekable<TokenStream>,
    keyword_buf: &mut String,
) -> Option<&'static str> {
    if !matches!(token.token.kind, Kind::Ident | Kind::Keyword) {
        return None;
    }
    let value = token.token.text;

    if value.len() > MAX_KEYWORD_LENGTH {
        return None;
    }

    keyword_buf.clear();
    keyword_buf.push_str(value);
    keyword_buf.make_ascii_lowercase();
    match &keyword_buf[..] {
        "named" if peek_keyword(tok_iter, "only") => Some("named only"),
        "set" if peek_keyword(tok_iter, "annotation") => Some("set annotation"),
        "set" if peek_keyword(tok_iter, "type") => Some("set type"),
        "extension" if peek_keyword(tok_iter, "package") => Some("extension package"),
        "order" if peek_keyword(tok_iter, "by") => Some("order by"),
        _ => None,
    }
}

fn peek_keyword(iter: &mut Peekable<TokenStream>, kw: &str) -> bool {
    iter.peek()
        .and_then(|res| res.as_ref().ok())
        .map(|t| {
            (t.token.kind == Kind::Ident || t.token.kind == Kind::Keyword) && t.token.text.eq_ignore_ascii_case(kw)
        })
        .unwrap_or(false)
}
