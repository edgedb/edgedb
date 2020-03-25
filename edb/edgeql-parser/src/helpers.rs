use std::borrow::Cow;
use std::fmt::Write;

use crate::tokenizer::is_keyword;


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
