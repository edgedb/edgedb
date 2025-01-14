use std::borrow::Cow;
use std::char;
use std::error::Error;
use std::fmt::{self, Write};

use crate::keywords;

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
        if keywords::lookup(&lower).is_none() {
            return s.into();
        }
    }
    let escaped = s.replace('`', "``");
    let mut s = String::with_capacity(escaped.len() + 2);
    s.push('`');
    s.push_str(&escaped);
    s.push('`');
    s.into()
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
            '\\' => {
                buf.push('\\');
                buf.push('\\');
            }
            '\x00'..='\x08'
            | '\x0B'
            | '\x0C'
            | '\x0E'..='\x1F'
            | '\u{007F}'
            | '\u{0080}'..='\u{009F}' => {
                write!(buf, "\\x{:02x}", c as u32).unwrap();
            }
            c => buf.push(c),
        }
    }
    buf.push('"');
    buf
}

pub fn unquote_string(value: &str) -> Result<Cow<str>, UnquoteError> {
    if value.starts_with('r') {
        Ok(value[2..value.len() - 1].into())
    } else if let Some(stripped) = value.strip_prefix('$') {
        let msize = 2 + stripped
            .find('$')
            .ok_or_else(|| "invalid dollar-quoted string".to_string())
            .map_err(UnquoteError)?;
        Ok(value[msize..value.len() - msize].into())
    } else {
        let end_trim = if value.ends_with("\\(") { 2 } else { 1 };

        Ok(_unquote_string(&value[1..value.len() - end_trim])
            .map_err(UnquoteError)?
            .into())
    }
}

fn _unquote_string(s: &str) -> Result<String, String> {
    let mut res = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => {
                let c = chars
                    .next()
                    .ok_or_else(|| "quoted string cannot end in slash".to_string())?;
                match c {
                    c @ '"' | c @ '\\' | c @ '/' | c @ '\'' => res.push(c),
                    'b' => res.push('\u{0008}'),
                    'f' => res.push('\u{000C}'),
                    'n' => res.push('\n'),
                    'r' => res.push('\r'),
                    't' => res.push('\t'),
                    'x' => {
                        let hex = chars.as_str().get(0..2);
                        let code = hex
                            .and_then(|s| u8::from_str_radix(s, 16).ok())
                            .ok_or_else(|| {
                                format!(
                                    "invalid string literal: \
                                invalid escape sequence '\\x{}'",
                                    hex.unwrap_or(chars.as_str()).escape_debug()
                                )
                            })?;
                        if code > 0x7f || code == 0 {
                            return Err(format!(
                                "invalid string literal: \
                                 invalid escape sequence '\\x{:x}' \
                                 (only non-null ascii allowed)",
                                code
                            ));
                        }
                        res.push(code as char);
                        chars.nth(1);
                    }
                    'u' => {
                        let hex = chars.as_str().get(0..4);
                        let ch = hex
                            .and_then(|s| u32::from_str_radix(s, 16).ok())
                            .and_then(char::from_u32)
                            .and_then(|c| if c == '\0' { None } else { Some(c) })
                            .ok_or_else(|| {
                                format!(
                                    "invalid string literal: \
                                    invalid escape sequence '\\u{}'",
                                    hex.unwrap_or(chars.as_str()).escape_debug()
                                )
                            })?;
                        res.push(ch);
                        chars.nth(3);
                    }
                    'U' => {
                        let hex = chars.as_str().get(0..8);
                        let ch = hex
                            .and_then(|s| u32::from_str_radix(s, 16).ok())
                            .and_then(char::from_u32)
                            .and_then(|c| if c == '\0' { None } else { Some(c) })
                            .ok_or_else(|| {
                                format!(
                                    "invalid string literal: \
                                    invalid escape sequence '\\U{}'",
                                    hex.unwrap_or(chars.as_str()).escape_debug()
                                )
                            })?;
                        res.push(ch);
                        chars.nth(7);
                    }
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
                            c.escape_debug()
                        ));
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
    assert_eq!(_unquote_string(r"\x09").unwrap(), "\u{09}");
    assert_eq!(_unquote_string(r"\u000A").unwrap(), "\u{000A}");
    assert_eq!(_unquote_string(r"\u000D").unwrap(), "\u{000D}");
    assert_eq!(_unquote_string(r"\u0020").unwrap(), "\u{0020}");
    assert_eq!(_unquote_string(r"\uFFFF").unwrap(), "\u{FFFF}");
}

#[test]
fn unquote_string_error() {
    assert_eq!(
        _unquote_string(r"\x00").unwrap_err(),
        "invalid string literal: \
             invalid escape sequence '\\x0' (only non-null ascii allowed)"
    );
    assert_eq!(
        _unquote_string(r"\u0000").unwrap_err(),
        "invalid string literal: invalid escape sequence '\\u0000'"
    );
    assert_eq!(
        _unquote_string(r"\U00000000").unwrap_err(),
        "invalid string literal: invalid escape sequence '\\U00000000'"
    );
}

#[test]
fn newline_escaping_str() {
    assert_eq!(
        _unquote_string(
            r"hello \
                                world"
        )
        .unwrap(),
        "hello world"
    );

    assert_eq!(
        _unquote_string(
            r"bb\
aa \
            bb"
        )
        .unwrap(),
        "bbaa bb"
    );
    assert_eq!(
        _unquote_string(
            r"bb\

        aa"
        )
        .unwrap(),
        "bbaa"
    );
    assert_eq!(
        _unquote_string(
            r"bb\
        \
        aa"
        )
        .unwrap(),
        "bbaa"
    );
    assert_eq!(_unquote_string("bb\\\r   aa").unwrap(), "bbaa");
    assert_eq!(_unquote_string("bb\\\r\n   aa").unwrap(), "bbaa");
}

#[test]
fn test_quote_string() {
    assert_eq!(quote_string(r"\n"), r#""\\n""#);
    assert_eq!(unquote_string(&quote_string(r"\n")).unwrap(), r"\n");
}

#[test]
fn complex_strings() {
    assert_eq!(
        _unquote_string(r"\u0009 hello \u000A there").unwrap(),
        "\u{0009} hello \u{000A} there"
    );

    assert_eq!(
        _unquote_string(r"\x62:\u2665:\U000025C6").unwrap(),
        "\u{62}:\u{2665}:\u{25C6}"
    );
}

impl fmt::Display for UnquoteError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl Error for UnquoteError {}
