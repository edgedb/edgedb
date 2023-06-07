use std::str::FromStr;

use bigdecimal::num_bigint::ToBigInt;
use bigdecimal::BigDecimal;

use crate::tokenizer::{Kind, MAX_KEYWORD_LENGTH};
use crate::utils::bytes::unquote_bytes;
use crate::utils::strings::unquote_string;
use crate::{CowToken, Error, TokenValue};

pub fn parse_value(token: &CowToken<'_>) -> Result<Option<TokenValue>, String> {
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
                .map(TokenValue::Decimal)
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
                .map(|x| TokenValue::Int(x as i64))
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
            return Ok(Some(TokenValue::BigInt(
                dec.to_bigint()
                    .ok_or_else(|| "number is not integer".to_string())?,
            )));
        }
        BinStr => {
            return unquote_bytes(&text).map(TokenValue::Bytes).map(Some);
        }

        Str => unquote_string(&text)
            .map_err(|s| s.to_string())?
            .to_string(),
        BacktickName => text[1..text.len() - 1].replace("``", "`"),
        Ident | Keyword => text.to_string(),
        Substitution => text[2..text.len() - 1].to_string(),
        _ => return Ok(None),
    };
    Ok(Some(TokenValue::String(string_value)))
}

pub fn combine_multi_word_keywords(
    token: &CowToken,
    next: &Option<Option<Result<CowToken, Error>>>,
    keyword_buf: &mut String,
) -> Option<&'static str> {
    if !matches!(token.kind, Kind::Ident | Kind::Keyword) {
        return None;
    }
    let text = &token.text;

    if text.len() > MAX_KEYWORD_LENGTH {
        return None;
    }

    keyword_buf.clear();
    keyword_buf.push_str(&text);
    keyword_buf.make_ascii_lowercase();
    match &keyword_buf[..] {
        "named" if peek_keyword(next, "only") => Some("named only"),
        "set" if peek_keyword(next, "annotation") => Some("set annotation"),
        "set" if peek_keyword(next, "type") => Some("set type"),
        "extension" if peek_keyword(next, "package") => Some("extension package"),
        "order" if peek_keyword(next, "by") => Some("order by"),
        _ => None,
    }
}

fn peek_keyword(next: &Option<Option<Result<CowToken, Error>>>, kw: &str) -> bool {
    next.as_ref()
        .and_then(|x| x.as_ref())
        .and_then(|res| res.as_ref().ok())
        .map(|t| {
            (t.kind == Kind::Ident || t.kind == Kind::Keyword) && t.text.eq_ignore_ascii_case(kw)
        })
        .unwrap_or(false)
}
