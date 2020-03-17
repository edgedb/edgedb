use std::fmt;

use combine::{StreamOnce, Positioned};
use combine::error::{StreamError};
use combine::stream::{Resetable};
use combine::easy::{Error, Errors};

use crate::position::Pos;


#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Kind {
    Punctuator,
    Name,
    IntValue,
    FloatValue,
    StringValue,
    BlockString,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct Token<'a> {
    pub kind: Kind,
    pub value: &'a str,
}

#[derive(Debug, PartialEq)]
pub struct TokenStream<'a> {
    buf: &'a str,
    position: Pos,
    off: usize,
    next_state: Option<(usize, Token<'a>, usize, Pos)>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Checkpoint {
    position: Pos,
    off: usize,
}

impl<'a> StreamOnce for TokenStream<'a> {
    type Item = Token<'a>;
    type Range = Token<'a>;
    type Position = Pos;
    type Error = Errors<Token<'a>, Token<'a>, Pos>;

    fn uncons(&mut self) -> Result<Self::Item, Error<Token<'a>, Token<'a>>> {
        if let Some((at, tok, off, pos)) = self.next_state {
            if at == self.off {
                self.off = off;
                self.position = pos;
                return Ok(tok);
            }
        }
        let old_pos = self.off;
        let (kind, len) = self.peek_token()?;
        let value = &self.buf[self.off-len..self.off];
        self.skip_whitespace();
        let token = Token { kind, value };
        // TODO(tailhook) optimize this better
        self.position.character += value.chars().count();
        self.position.token += 1;
        self.next_state = Some((old_pos, token, self.off, self.position));
        Ok(token)
    }
}

impl<'a> Positioned for TokenStream<'a> {
    fn position(&self) -> Self::Position {
        self.position
    }
}

impl<'a> Resetable for TokenStream<'a> {
    type Checkpoint = Checkpoint;
    fn checkpoint(&self) -> Self::Checkpoint {
        Checkpoint {
            position: self.position,
            off: self.off,
        }
    }
    fn reset(&mut self, checkpoint: Checkpoint) {
        self.position = checkpoint.position;
        self.off = checkpoint.off;
    }
}

// NOTE: we expect that first character is always digit or minus, as returned
// by tokenizer
fn check_int(value: &str) -> bool {
    value == "0" || value == "-0" ||
       (!value.starts_with('0') && value != "-" && !value.starts_with("-0")
       && value[1..].chars().all(|x| x >= '0' && x <= '9'))
}

fn check_dec(value: &str) -> bool {
    !value.is_empty() && value.chars().all(|x| x >= '0' && x <= '9')
}

fn check_exp(value: &str) -> bool {
    if value.is_empty() {
        return false;
    }
    let first = value.chars().next().unwrap();
    if first != '-' && first != '+' && (first <= '0' || first >= '9') {
        return false;
    }

    value[1..].chars().all(|x| x >= '0' && x <= '9')
}

fn check_float(value: &str, exponent: Option<usize>, real: Option<usize>)
    -> bool
{
    match (exponent, real) {
        (Some(e), Some(r)) if e < r => false,
        (Some(e), Some(r))
        => check_int(&value[..r]) &&
           check_dec(&value[r+1..e]) &&
           check_exp(&value[e+1..]),
        (Some(e), None)
        => check_int(&value[..e]) && check_exp(&value[e+1..]),
        (None, Some(r))
        => check_int(&value[..r]) && check_dec(&value[r+1..]),
        (None, None) => unreachable!(),
    }
}

impl<'a> TokenStream<'a> {
    pub fn new(s: &str) -> TokenStream {
        let mut me = TokenStream {
            buf: s,
            position: Pos { line: 1, column: 1, character: 0, token: 0 },
            off: 0,
            next_state: None,
        };
        me.skip_whitespace();
        me
    }

    fn peek_token(&mut self)
        -> Result<(Kind, usize), Error<Token<'a>, Token<'a>>>
    {
        use self::Kind::*;
        let mut iter = self.buf[self.off..].char_indices();
        let cur_char = match iter.next() {
            Some((_, x)) => x,
            None => return Err(Error::end_of_input()),
        };

        match cur_char {
            '!' | '$' | ':' | '=' | '@' | '|' |
            '(' | ')' | '[' | ']' | '{' | '}' | '&' => {
                self.position.column += 1;
                self.off += 1;

                Ok((Punctuator, 1))
            }
            '.' => {
                if iter.as_str().starts_with("..") {
                    self.position.column += 3;
                    self.off += 3;

                    Ok((Punctuator, 3))
                } else {
                    Err(
                        Error::unexpected_message(
                        format_args!("bare dot {:?} is not supported, \
                            only \"...\"", cur_char)
                        )
                    )
                }
            }
            '_' | 'a'..='z' | 'A'..='Z' => {
                while let Some((idx, cur_char)) = iter.next() {
                    match cur_char {
                        '_' | 'a'..='z' | 'A'..='Z' | '0'..='9' => continue,
                        _ => {
                            self.position.column += idx;
                            self.off += idx;
                            return Ok((Name, idx));
                        }
                    }
                }
                let len = self.buf.len() - self.off;
                self.position.column += len;
                self.off += len;

                Ok((Name, len))
            }
            '-' | '0'..='9' => {
                let mut exponent = None;
                let mut real = None;
                let len = loop {
                    let (idx, cur_char) = match iter.next() {
                        Some(pair) => pair,
                        None => break self.buf.len() - self.off,
                    };
                    match cur_char {
                        // just scan for now, will validate later on
                        ' ' | '\n' | '\r' | '\t' | ',' | '#' |
                        '!' | '$' | ':' | '=' | '@' | '|' | '&' |
                        '(' | ')' | '[' | ']' | '{' | '}'
                        => break idx,
                        '.' => real = Some(idx),
                        'e' | 'E' => exponent = Some(idx),
                        _ => {},
                    }
                };

                if exponent.is_some() || real.is_some() {
                    let value = &self.buf[self.off..][..len];
                    if !check_float(value, exponent, real) {
                        return Err(
                            Error::unexpected_message(
                                format_args!("unsupported float {:?}", value)
                            )
                        );
                    }
                    self.position.column += len;
                    self.off += len;

                    Ok((FloatValue, len))
                } else {
                    let value = &self.buf[self.off..][..len];
                    if !check_int(value) {
                        return Err(
                            Error::unexpected_message(
                                format_args!("unsupported integer {:?}", value)
                            )
                        );
                    }
                    self.position.column += len;
                    self.off += len;

                    Ok((IntValue, len))
                }
            }
            '"' => {
                if iter.as_str().starts_with("\"\"") {
                    let tail = &iter.as_str()[2..];
                    for (end_idx, _) in tail.match_indices("\"\"\"") {
                        if !tail[..end_idx].ends_with('\\') {
                            self.update_position(end_idx + 6);
                            return Ok((BlockString, end_idx + 6));
                        }
                    }

                    Err(
                        Error::unexpected_message(
                            "unterminated block string value"
                        )
                    )
                } else {
                    let mut nchars = 1;
                    let mut escaped = false;
                    for (idx, cur_char) in iter {
                        nchars += 1;
                        match cur_char {
                            '"' if escaped => {}
                            '"' => {
                                self.position.column += nchars;
                                self.off += idx+1;
                                return Ok((StringValue, idx+1));
                            }
                            '\n' => {
                                return Err(
                                    Error::unexpected_message(
                                        "unterminated string value"
                                    )
                                );
                            }

                            _ => {

                            }
                        }

                        // if we aren't escaped and the current char is a \, we are now escaped
                        escaped = !escaped && cur_char == '\\';
                    }
                    Err(
                        Error::unexpected_message(
                            "unterminated string value"
                        )
                    )
                }
            }
            _ => Err(
                    Error::unexpected_message(
                        format_args!("unexpected character {:?}", cur_char)
                    )
            ),
        }
    }

    fn skip_whitespace(&mut self) {
        let mut iter = self.buf[self.off..].char_indices();
        let idx = loop {
            let (idx, cur_char) = match iter.next() {
                Some(pair) => pair,
                None => break self.buf.len() - self.off,
            };
            match cur_char {
                '\u{feff}' | '\r' => continue,
                '\t' => self.position.column += 8,
                '\n' => {
                    self.position.column = 1;
                    self.position.line += 1;
                }
                // comma is also entirely ignored in spec
                ' ' | ',' => {
                    self.position.column += 1;
                    continue;
                }
                //comment
                '#' => {
                    while let Some((_, cur_char)) = iter.next() {
                        // TODO(tailhook) ensure SourceCharacter
                        if cur_char == '\r' || cur_char == '\n' {
                            self.position.column = 1;
                            self.position.line += 1;
                            break;
                        }
                    }
                    continue;
                }
                _ => break idx,
            }
        };
        // TODO(tailhook) optimize this better
        self.position.character += self.buf[self.off..][..idx].chars().count();
        self.off += idx;
    }

    fn update_position(&mut self, len: usize) {
        let val = &self.buf[self.off..][..len];
        self.off += len;
        let lines = val.as_bytes().iter().filter(|&&x| x == b'\n').count();
        self.position.line += lines;
        if lines > 0 {
            let line_offset = val.rfind('\n').unwrap()+1;
            let num = val[line_offset..].chars().count();
            self.position.column = num + 1;
        } else {
            let num = val.chars().count();
            self.position.column += num;
        }
    }
}

impl<'a> fmt::Display for Token<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}[{:?}]", self.value, self.kind)
    }
}

#[cfg(test)]
mod test {
    use super::{Kind, TokenStream};
    use super::Kind::*;
    use combine::easy::Error;

    use combine::{StreamOnce, Positioned};

    fn tok_str(s: &str) -> Vec<&str> {
        let mut r = Vec::new();
        let mut s = TokenStream::new(s);
        loop {
            match s.uncons() {
                Ok(x) => r.push(x.value),
                Err(ref e) if e == &Error::end_of_input() => break,
                Err(e) => panic!("Parse error at {}: {}", s.position(), e),
            }
        }
        return r;
    }
    fn tok_typ(s: &str) -> Vec<Kind> {
        let mut r = Vec::new();
        let mut s = TokenStream::new(s);
        loop {
            match s.uncons() {
                Ok(x) => r.push(x.kind),
                Err(ref e) if e == &Error::end_of_input() => break,
                Err(e) => panic!("Parse error at {}: {}", s.position(), e),
            }
        }
        return r;
    }

    #[test]
    fn comments_and_commas() {
        assert_eq!(tok_str("# hello { world }"), &[] as &[&str]);
        assert_eq!(tok_str("# x\n,,,"), &[] as &[&str]);
        assert_eq!(tok_str(", ,,  ,,,  # x"), &[] as &[&str]);
    }

    #[test]
    fn simple() {
        assert_eq!(tok_str("a { b }"), ["a", "{", "b", "}"]);
        assert_eq!(tok_typ("a { b }"), [Name, Punctuator, Name, Punctuator]);
    }

    #[test]
    fn query() {
        assert_eq!(tok_str("query Query {
            object { field }
        }"), ["query", "Query", "{", "object", "{", "field", "}", "}"]);
    }

    #[test]
    fn fragment() {
        assert_eq!(tok_str("a { ...b }"), ["a", "{", "...", "b", "}"]);
    }

    #[test]
    fn int() {
        assert_eq!(tok_str("0"), ["0"]);
        assert_eq!(tok_str("0,"), ["0"]);
        assert_eq!(tok_str("0# x"), ["0"]);
        assert_eq!(tok_typ("0"), [IntValue]);
        assert_eq!(tok_str("-0"), ["-0"]);
        assert_eq!(tok_typ("-0"), [IntValue]);
        assert_eq!(tok_str("-1"), ["-1"]);
        assert_eq!(tok_typ("-1"), [IntValue]);
        assert_eq!(tok_str("-132"), ["-132"]);
        assert_eq!(tok_typ("-132"), [IntValue]);
        assert_eq!(tok_str("132"), ["132"]);
        assert_eq!(tok_typ("132"), [IntValue]);
        assert_eq!(tok_str("a(x: 10) { b }"),
            ["a", "(", "x", ":", "10", ")", "{", "b", "}"]);
        assert_eq!(tok_typ("a(x: 10) { b }"),
            [Name, Punctuator, Name, Punctuator, IntValue, Punctuator,
                Punctuator, Name, Punctuator]);
    }

    // TODO(tailhook) fix errors in parser and check error message
    #[test] #[should_panic] fn zero_int() { tok_str("01"); }
    #[test] #[should_panic] fn zero_int4() { tok_str("00001"); }
    #[test] #[should_panic] fn minus_int() { tok_str("-"); }
    #[test] #[should_panic] fn minus_zero_int() { tok_str("-01"); }
    #[test] #[should_panic] fn minus_zero_int4() { tok_str("-00001"); }
    #[test] #[should_panic] fn letters_int() { tok_str("0bbc"); }

    #[test]
    fn float() {
        assert_eq!(tok_str("0.0"), ["0.0"]);
        assert_eq!(tok_typ("0.0"), [FloatValue]);
        assert_eq!(tok_str("-0.0"), ["-0.0"]);
        assert_eq!(tok_typ("-0.0"), [FloatValue]);
        assert_eq!(tok_str("-1.0"), ["-1.0"]);
        assert_eq!(tok_typ("-1.0"), [FloatValue]);
        assert_eq!(tok_str("-1.023"), ["-1.023"]);
        assert_eq!(tok_typ("-1.023"), [FloatValue]);
        assert_eq!(tok_str("-132.0"), ["-132.0"]);
        assert_eq!(tok_typ("-132.0"), [FloatValue]);
        assert_eq!(tok_str("132.0"), ["132.0"]);
        assert_eq!(tok_typ("132.0"), [FloatValue]);
        assert_eq!(tok_str("0e+0"), ["0e+0"]);
        assert_eq!(tok_typ("0e+0"), [FloatValue]);
        assert_eq!(tok_str("0.0e+0"), ["0.0e+0"]);
        assert_eq!(tok_typ("0.0e+0"), [FloatValue]);
        assert_eq!(tok_str("-0e+0"), ["-0e+0"]);
        assert_eq!(tok_typ("-0e+0"), [FloatValue]);
        assert_eq!(tok_str("-1e+0"), ["-1e+0"]);
        assert_eq!(tok_typ("-1e+0"), [FloatValue]);
        assert_eq!(tok_str("-132e+0"), ["-132e+0"]);
        assert_eq!(tok_typ("-132e+0"), [FloatValue]);
        assert_eq!(tok_str("132e+0"), ["132e+0"]);
        assert_eq!(tok_typ("132e+0"), [FloatValue]);
        assert_eq!(tok_str("a(x: 10.0) { b }"),
            ["a", "(", "x", ":", "10.0", ")", "{", "b", "}"]);
        assert_eq!(tok_typ("a(x: 10.0) { b }"),
            [Name, Punctuator, Name, Punctuator, FloatValue, Punctuator,
                Punctuator, Name, Punctuator]);
        assert_eq!(tok_str("1.23e4"), ["1.23e4"]);
        assert_eq!(tok_typ("1.23e4"), [FloatValue]);
    }

    // TODO(tailhook) fix errors in parser and check error message
    #[test] #[should_panic] fn no_int_float() { tok_str(".0"); }
    #[test] #[should_panic] fn no_int_float1() { tok_str(".1"); }
    #[test] #[should_panic] fn zero_float() { tok_str("01.0"); }
    #[test] #[should_panic] fn zero_float4() { tok_str("00001.0"); }
    #[test] #[should_panic] fn minus_float() { tok_str("-.0"); }
    #[test] #[should_panic] fn minus_zero_float() { tok_str("-01.0"); }
    #[test] #[should_panic] fn minus_zero_float4() { tok_str("-00001.0"); }
    #[test] #[should_panic] fn letters_float() { tok_str("0bbc.0"); }
    #[test] #[should_panic] fn letters_float2() { tok_str("0.bbc"); }
    #[test] #[should_panic] fn letters_float3() { tok_str("0.bbce0"); }
    #[test] #[should_panic] fn no_exp_sign_float() { tok_str("0e0"); }
    #[test] #[should_panic] fn unterminated_string() { tok_str(r#""hello\""#); }
    #[test] #[should_panic] fn extra_unterminated_string() { tok_str(r#""hello\\\""#); }

    #[test]
    fn string() {
        assert_eq!(tok_str(r#""""#), [r#""""#]);
        assert_eq!(tok_typ(r#""""#), [StringValue]);
        assert_eq!(tok_str(r#""hello""#), [r#""hello""#]);
        assert_eq!(tok_str(r#""hello\\""#), [r#""hello\\""#]);
        assert_eq!(tok_str(r#""hello\\\\""#), [r#""hello\\\\""#]);
        assert_eq!(tok_str(r#""he\\llo""#), [r#""he\\llo""#]);
        assert_eq!(tok_typ(r#""hello""#), [StringValue]);
        assert_eq!(tok_str(r#""my\"quote""#), [r#""my\"quote""#]);
        assert_eq!(tok_typ(r#""my\"quote""#), [StringValue]);
    }

    #[test]
    fn block_string() {
        assert_eq!(tok_str(r#""""""""#), [r#""""""""#]);
        assert_eq!(tok_typ(r#""""""""#), [BlockString]);
        assert_eq!(tok_str(r#""""hello""""#), [r#""""hello""""#]);
        assert_eq!(tok_typ(r#""""hello""""#), [BlockString]);
        assert_eq!(tok_str(r#""""my "quote" """"#), [r#""""my "quote" """"#]);
        assert_eq!(tok_typ(r#""""my "quote" """"#), [BlockString]);
        assert_eq!(tok_str(r#""""\"""quote" """"#), [r#""""\"""quote" """"#]);
        assert_eq!(tok_typ(r#""""\"""quote" """"#), [BlockString]);
    }
}
