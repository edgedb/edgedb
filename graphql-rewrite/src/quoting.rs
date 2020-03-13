use crate::tokenizer::Error;


pub fn unquote_block_string<'a>(src: &'a str) -> Result<String, Error> {
    debug_assert!(src.starts_with("\"\"\"") && src.ends_with("\"\"\""));
    let indent = src[3..src.len()-3].lines().skip(1)
        .filter_map(|line| {
            let trimmed = line.trim_start().len();
            if trimmed > 0 {
                Some(line.len() - trimmed)
            } else {
                None  // skip whitespace-only lines
            }
        })
        .min().unwrap_or(0);
    let mut result = String::with_capacity(src.len()-6);
    let mut lines = src[3..src.len()-3].lines();
    if let Some(first) = lines.next() {
        let stripped = first.trim();
        if !stripped.is_empty() {
            result.push_str(stripped);
            result.push('\n');
        }
    }
    let mut last_line = 0;
    for line in lines {
        last_line = result.len();
        if line.len() > indent {
            result.push_str(&line[indent..].replace(r#"\""""#, r#"""""#));
        }
        result.push('\n');
    }
    if result[last_line..].trim().is_empty() {
        result.truncate(last_line);
    }

    Ok(result)
}

pub fn unquote_string<'a>(s: &'a str) -> Result<String, Error>
{
    let mut res = String::with_capacity(s.len());
    debug_assert!(s.starts_with('"') && s.ends_with('"'));
    let mut chars = s[1..s.len()-1].chars();
    let mut temp_code_point = String::with_capacity(4);
    while let Some(c) = chars.next() {
        match c {
            '\\' => {
                match chars.next().expect("slash cant be at the end") {
                    c@'"' | c@'\\' | c@'/' => res.push(c),
                    'b' => res.push('\u{0010}'),
                    'f' => res.push('\u{000C}'),
                    'n' => res.push('\n'),
                    'r' => res.push('\r'),
                    't' => res.push('\t'),
                    'u' => {
                        temp_code_point.clear();
                        for _ in 0..4 {
                            match chars.next() {
                                Some(inner_c) => temp_code_point.push(inner_c),
                                None => return Err(Error::unexpected_message(
                                    format_args!("\\u must have 4 characters after it, only found '{}'", temp_code_point)
                                )),
                            }
                        }

                        // convert our hex string into a u32, then convert that into a char
                        match u32::from_str_radix(&temp_code_point, 16).map(std::char::from_u32) {
                            Ok(Some(unicode_char)) => res.push(unicode_char),
                            _ => {
                                return Err(Error::unexpected_message(
                                    format_args!("{} is not a valid unicode code point", temp_code_point)))
                            }
                        }
                    },
                    c => {
                        return Err(Error::unexpected_message(
                            format_args!("bad escaped char {:?}", c)));
                    }
                }
            }
            c => res.push(c),
        }
    }

    Ok(res)
}

#[cfg(test)]
mod tests {
    use super::unquote_string;

    #[test]
    fn unquote_unicode_string() {
        // basic tests
        assert_eq!(unquote_string(r#""\u0009""#).expect(""), "\u{0009}");
        assert_eq!(unquote_string(r#""\u000A""#).expect(""), "\u{000A}");
        assert_eq!(unquote_string(r#""\u000D""#).expect(""), "\u{000D}");
        assert_eq!(unquote_string(r#""\u0020""#).expect(""), "\u{0020}");
        assert_eq!(unquote_string(r#""\uFFFF""#).expect(""), "\u{FFFF}");

        // a more complex string
        assert_eq!(unquote_string(r#""\u0009 hello \u000A there""#).expect(""), "\u{0009} hello \u{000A} there");
    }
}
