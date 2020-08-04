use std::borrow::Cow;
use std::fmt::{self, Write};
use std::error::Error;
use std::char;

use crate::tokenizer::is_keyword;

/// Error returned from `unquote_string` function
///
/// Opaque for now
#[derive(Debug)]
pub struct UnquoteError(String);

/// Converts the string into edgeql-compatible name (of a column or a property)
///
/// # Examples
/// ```
/// use edgeql_parser::helpers::quote_name;
/// assert_eq!(quote_name("col1"), "col1");
/// assert_eq!(quote_name("another name"), "`another name`");
/// assert_eq!(quote_name("with `quotes`"), "`with ``quotes```");
/// ```
pub fn quote_name(s: &str) -> Cow<str> {
    if s.chars().all(|c| c.is_alphanumeric() || c == '_') {
        let lower = s.to_ascii_lowercase();
        if !is_keyword(&lower) {
            return s.into();
        }
    }
    let escaped = s.replace('`', "``");
    let mut s = String::with_capacity(escaped.len()+2);
    s.push('`');
    s.push_str(&escaped);
    s.push('`');
    return s.into();
}

pub fn quote_string(s: &str) -> String {
    let mut buf = String::with_capacity(s.len() + 2);
    buf.push('"');
    for c in s.chars() {
        match c {
            '"' => {
                buf.push('\\');
                buf.push('"');
            }
            '\x00'..='\x08' | '\x0B' | '\x0C' | '\x0E'..='\x1F' |
            '\u{007F}' | '\u{0080}'..='\u{009F}'
            => {
                write!(buf, "\\x{:02x}", c as u32).unwrap();
            }
            c => buf.push(c),
        }
    }
    buf.push('"');
    return buf;
}

pub fn unquote_string<'a>(value: &'a str) -> Result<Cow<'a, str>, UnquoteError>
{
    if value.starts_with('r') {
        Ok(value[2..value.len()-1].into())
    } else if value.starts_with('$') {
        let msize = 2 + value[1..].find('$')
            .ok_or_else(|| "invalid dollar-quoted string".to_string())
            .map_err(UnquoteError)?;
        Ok(value[msize..value.len()-msize].into())
    } else {
        Ok(_unquote_string(&value[1..value.len()-1])
            .map_err(UnquoteError)?.into())
    }
}

fn _unquote_string<'a>(s: &'a str) -> Result<String, String> {
    let mut res = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => {
                let c = chars.next()
                    .ok_or_else(|| "quoted string cannot end in slash"
                                   .to_string())?;
                match c {
                    c@'"' | c@'\\' | c@'/' | c@'\'' => res.push(c),
                    'b' => res.push('\u{0010}'),
                    'f' => res.push('\u{000C}'),
                    'n' => res.push('\n'),
                    'r' => res.push('\r'),
                    't' => res.push('\t'),
                    'x' => {
                        let hex = chars.as_str().get(0..2);
                        let code = hex.and_then(|s| {
                            u8::from_str_radix(s, 16).ok()
                        }).ok_or_else(|| {
                            format!("invalid string literal: \
                                invalid escape sequence '\\x{}'",
                                hex.unwrap_or_else(|| chars.as_str())
                                .escape_debug())
                        })?;
                        if code > 0x7f {
                            return Err(format!(
                                "invalid string literal: \
                                 invalid escape sequence '\\x{:x}' \
                                 (only ascii allowed)", code));
                        }
                        res.push(code as char);
                        chars.nth(1);
                    }
                    'u' => {
                        let hex = chars.as_str().get(0..4);
                        let ch = hex.and_then(|s| {
                                u32::from_str_radix(s, 16).ok()
                            })
                            .and_then(|code| char::from_u32(code))
                            .ok_or_else(|| {
                                format!("invalid string literal: \
                                    invalid escape sequence '\\u{}'",
                                    hex.unwrap_or_else(|| chars.as_str())
                                    .escape_debug())
                            })?;
                        res.push(ch);
                        chars.nth(3);
                    }
                    'U' => {
                        let hex = chars.as_str().get(0..8);
                        let ch = hex.and_then(|s| {
                                u32::from_str_radix(s, 16).ok()
                            })
                            .and_then(|code| char::from_u32(code))
                            .ok_or_else(|| {
                                format!("invalid string literal: \
                                    invalid escape sequence '\\U{}'",
                                    hex.unwrap_or_else(|| chars.as_str())
                                    .escape_debug())
                            })?;
                        res.push(ch);
                        chars.nth(7);
                    },
                    '\r' | '\n' => {
                        let nleft = chars.as_str().trim_start().len();
                        let nskip = chars.as_str().len() - nleft;
                        if nskip > 0 {
                            chars.nth(nskip - 1);
                        }
                    }
                    c => {
                        return Err(format!(
                            "invalid string literal: \
                             invalid escape sequence '\\{}'",
                            c.escape_debug()));
                    }
                }
            }
            c => res.push(c),
        }
    }

    Ok(res)
}

#[test]
fn unquote_unicode_string() {
    assert_eq!(_unquote_string(r#"\x09"#).unwrap(), "\u{09}");
    assert_eq!(_unquote_string(r#"\u000A"#).unwrap(), "\u{000A}");
    assert_eq!(_unquote_string(r#"\u000D"#).unwrap(), "\u{000D}");
    assert_eq!(_unquote_string(r#"\u0020"#).unwrap(), "\u{0020}");
    assert_eq!(_unquote_string(r#"\uFFFF"#).unwrap(), "\u{FFFF}");
}

#[test]
fn newline_escaping_str() {
    assert_eq!(_unquote_string(r"hello \
                                world").unwrap(), "hello world");

    assert_eq!(_unquote_string(r"bb\
aa \
            bb").unwrap(), "bbaa bb");
    assert_eq!(_unquote_string(r"bb\

        aa").unwrap(), "bbaa");
    assert_eq!(_unquote_string(r"bb\
        \
        aa").unwrap(), "bbaa");
    assert_eq!(_unquote_string("bb\\\r   aa").unwrap(), "bbaa");
    assert_eq!(_unquote_string("bb\\\r\n   aa").unwrap(), "bbaa");
}

#[test]
fn complex_strings() {
    assert_eq!(_unquote_string(r#"\u0009 hello \u000A there"#).unwrap(),
        "\u{0009} hello \u{000A} there");

    assert_eq!(_unquote_string(r#"\x62:\u2665:\U000025C6"#).unwrap(),
        "\u{62}:\u{2665}:\u{25C6}");
}

impl fmt::Display for UnquoteError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl Error for UnquoteError {}
