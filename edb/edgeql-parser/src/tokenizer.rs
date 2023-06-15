use std::fmt;
use std::borrow::Cow;
use std::str::CharIndices;

use bigdecimal::BigDecimal;
use bigdecimal::num_bigint::BigInt;
use memchr::memmem::find;

use crate::validation::Validator;
use crate::position::{Pos, Span};


// Current max keyword length is 10, but we're reserving some space
pub const MAX_KEYWORD_LENGTH: usize = 16;

#[derive(Debug, Clone)]
pub struct Token<'a> {
    pub kind: Kind,
    pub text: Cow<'a, str>,

    /// Parsed during validation.
    pub value: Option<Value>,

    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
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

#[cfg_attr(feature="wasm-bindgen",
    wasm_bindgen::prelude::wasm_bindgen(js_name=TokenKind))]
#[cfg_attr(feature="wasm-bindgen", derive(serde::Serialize))]
#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub enum Kind {
    Assign,           // :=
    SubAssign,        // -=
    AddAssign,        // +=
    Arrow,            // ->
    Coalesce,         // ??
    Namespace,        // ::
    BackwardLink,     // .<
    FloorDiv,         // //
    Concat,           // ++
    GreaterEq,        // >=
    LessEq,           // <=
    NotEq,            // !=
    NotDistinctFrom,  // ?=
    DistinctFrom,     // ?!=
    Comma,            // ,
    OpenParen,        // (
    CloseParen,       // )
    OpenBracket,      // [
    CloseBracket,     // ]
    OpenBrace,        // {
    CloseBrace,       // }
    Dot,              // .
    Semicolon,        // ;
    Colon,            // :
    Add,              // +
    Sub,              // -
    DoubleSplat,      // **
    Mul,              // *
    Div,              // /
    Modulo,           // %
    Pow,              // ^
    Less,             // <
    Greater,          // >
    Eq,               // =
    Ampersand,        // &
    Pipe,             // |
    At,               // @
    Argument,         // $something, $`something`
    DecimalConst,
    FloatConst,
    IntConst,
    BigIntConst,
    BinStr,           // b"xx", b'xx'
    Str,              // "xx", 'xx', r"xx", r'xx', $$xx$$
    BacktickName,     // `xx`
    Substitution,     // \(name)
    Keyword,
    Ident,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
struct TokenStub<'a> {
    pub kind: Kind,
    pub text: &'a str,
}

#[derive(Debug, PartialEq)]
pub struct Tokenizer<'a> {
    buf: &'a str,
    position: Pos,
    off: usize,
    dot: bool,
    next_state: Option<(usize, TokenStub<'a>, usize, Pos, Pos)>,
    keyword_buf: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Checkpoint {
    position: Pos,
    off: usize,
    dot: bool,
}

impl<'a> Iterator for Tokenizer<'a> {
    type Item = Result<Token<'a>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let start = self.current_pos();

        Some(self.read_token()?.map(|(token, end)| Token {
            kind: token.kind,
            text: token.text.into(),
            value: None,
            span: Span { start, end },
        })
        .map_err(|e| {
            let end = self.position;
            e.with_span(Span { start, end })
        }))
    }
}

impl<'a> Tokenizer<'a> {
    pub fn new(s: &str) -> Tokenizer {
        let mut me = Tokenizer {
            buf: s,
            position: Pos { line: 1, column: 1, offset: 0 },
            off: 0,
            dot: false,
            next_state: None,
            // Current max keyword length is 10, but we're reserving some
            // space
            keyword_buf: String::with_capacity(MAX_KEYWORD_LENGTH),
        };
        me.skip_whitespace();
        me
    }

    /// Start stream a with a modified position
    ///
    /// Note: we assume that the current position is at the start of slice `s`
    pub fn new_at(s: &str, position: Pos) -> Tokenizer {
        let mut me = Tokenizer {
            buf: s,
            position,
            off: 0,
            dot: false,
            next_state: None,
            keyword_buf: String::with_capacity(MAX_KEYWORD_LENGTH),
        };
        me.skip_whitespace();
        me
    }

    pub fn validated_values(self) -> Validator<'a> {
        Validator::new(self)
    }

    pub fn checkpoint(&self) -> Checkpoint {
        Checkpoint {
            position: self.position,
            off: self.off,
            dot: self.dot,
        }
    }

    pub fn reset(&mut self, checkpoint: Checkpoint) {
        self.position = checkpoint.position;
        self.off = checkpoint.off;
        self.dot = checkpoint.dot;
    }

    pub fn current_pos(&self) -> Pos {
        self.position
    }

    fn read_token(&mut self)
        -> Option<Result<(TokenStub<'a>, Pos), Error>>
    {
        // This quickly resets the stream one token back
        // (the most common reset that used quite often)
        if let Some((at, tok, off, end, next)) = self.next_state {
            if at == self.off {
                self.off = off;
                self.position = next;
                return Some(Ok((tok, end)));
            }
        }

        let old_pos = self.off;
        let (kind, len) = match self.peek_token()? {
            Ok(x) => x,
            Err(e) => return Some(Err(e))
        };

        // note we may want to get rid of "update_position" here as it's
        // faster to update 'as you go', but this is easier to get right first
        self.update_position(len);
        self.dot = matches!(kind, Kind::Dot);
        let value = &self.buf[self.off-len..self.off];
        let end = self.position;

        self.skip_whitespace();
        let token = TokenStub { kind, text: value };
        // This is for quick reset on token back
        self.next_state = Some((old_pos, token, self.off, end, self.position));
        Some(Ok((token, end)))
    }

    fn peek_token(&mut self)
        -> Option<Result<(Kind, usize), Error>>
    {
        let tail = &self.buf[self.off..];
        let mut iter = tail.char_indices();

        let (_, cur_char) = iter.next()?;
        Some(self.peek_token_inner(cur_char, tail, &mut iter))
    }

    fn peek_token_inner(
        &mut self, cur_char: char, tail: &str, iter: &mut CharIndices<'_>
    ) -> Result<(Kind, usize), Error> {
        use self::Kind::*;

        match cur_char {
            ':' => match iter.next() {
                Some((_, '=')) => return Ok((Assign, 2)),
                Some((_, ':')) => return Ok((Namespace, 2)),
                _ => return Ok((Colon, 1)),
            },
            '-' => match iter.next() {
                Some((_, '>')) => return Ok((Arrow, 2)),
                Some((_, '=')) => return Ok((SubAssign, 2)),
                _ => return Ok((Sub, 1)),
            },
            '>' => match iter.next() {
                Some((_, '=')) => return Ok((GreaterEq, 2)),
                _ => return Ok((Greater, 1)),
            },
            '<' => match iter.next() {
                Some((_, '=')) => return Ok((LessEq, 2)),
                _ => return Ok((Less, 1)),
            },
            '+' => match iter.next() {
                Some((_, '=')) => return Ok((AddAssign, 2)),
                Some((_, '+')) => return Ok((Concat, 2)),
                _ => return Ok((Add, 1)),
            },
            '/' => match iter.next() {
                Some((_, '/')) => return Ok((FloorDiv, 2)),
                _ => return Ok((Div, 1)),
            },
            '.' => match iter.next() {
                Some((_, '<')) => return Ok((BackwardLink, 2)),
                _ => return Ok((Dot, 1)),
            },
            '?' => match iter.next() {
                Some((_, '?')) => return Ok((Coalesce, 2)),
                Some((_, '=')) => return Ok((NotDistinctFrom, 2)),
                Some((_, '!')) => {
                    if let Some((_, '=')) = iter.next() {
                        return Ok((DistinctFrom, 3));
                    } else {
                        return Err(Error::new(
                            "`?!` is not an operator, \
                                did you mean `?!=` ?"))
                    }
                }
                _ => {
                    return Err(Error::new(
                        "Bare `?` is not an operator, \
                            did you mean `?=` or `??` ?"))
                }
            },
            '!' => match iter.next() {
                Some((_, '=')) => return Ok((NotEq, 2)),
                _ => {
                    return Err(Error::new(
                        "Bare `!` is not an operator, \
                            did you mean `!=`?"));
                }
            },
            '"' | '\'' => self.parse_string(0, false, false),
            '`' => {
                while let Some((idx, c)) = iter.next() {
                    if c == '`' {
                        if let Some((_, '`')) = iter.next() {
                            continue;
                        }
                        let val = &tail[..idx+1];
                        if val.starts_with("`@") {
                            return Err(Error::new(
                                "backtick-quoted name cannot \
                                    start with char `@`"));
                        }
                        if val.starts_with("`$") {
                            return Err(Error::new(
                                "backtick-quoted name cannot \
                                    start with char `$`"));
                        }
                        if val.contains("::") {
                            return Err(Error::new(
                                "backtick-quoted name cannot \
                                    contain `::`"));
                        }
                        if val.starts_with("`__") && val.ends_with("__`") {
                            return Err(Error::new(
                                "backtick-quoted names surrounded by double \
                                    underscores are forbidden"));
                        }
                        if idx == 1 {
                            return Err(Error::new(
                                "backtick quotes cannot be empty"));
                        }
                        return Ok((BacktickName, idx+1));
                    }
                    check_prohibited(c, false)?;
                }
                return Err(Error::new(
                    "unterminated backtick name"));
            }
            '=' => return Ok((Eq, 1)),
            ',' => return Ok((Comma, 1)),
            '(' => return Ok((OpenParen, 1)),
            ')' => return Ok((CloseParen, 1)),
            '[' => return Ok((OpenBracket, 1)),
            ']' => return Ok((CloseBracket, 1)),
            '{' => return Ok((OpenBrace, 1)),
            '}' => return Ok((CloseBrace, 1)),
            ';' => return Ok((Semicolon, 1)),
            '*' => match iter.next() {
                Some((_, '*')) => return Ok((DoubleSplat, 2)),
                _ => return Ok((Mul, 1)),
            },
            '%' => return Ok((Modulo, 1)),
            '^' => return Ok((Pow, 1)),
            '&' => return Ok((Ampersand, 1)),
            '|' => return Ok((Pipe, 1)),
            '@' => return Ok((At, 1)),
            c if c == '_' || c.is_alphabetic() => {
                let end_idx = loop {
                    match iter.next() {
                        Some((idx, '"')) | Some((idx, '\'')) => {
                            let prefix = &tail[..idx];
                            let (raw, binary) = match prefix {
                                "r" => (true, false),
                                "b" => (false, true),
                                "rb" => (true, true),
                                "br" => (true, true),
                                _ => return Err(Error::new(
                                    format_args!("prefix {:?} \
                                    is not allowed for strings, \
                                    allowed: `b`, `r`",
                                    prefix))),
                            };
                            return self.parse_string(idx, raw, binary);
                        }
                        Some((idx, '`')) => {
                            let prefix = &tail[..idx];
                            return Err(Error::new(
                                format_args!("prefix {:?} is not \
                                allowed for field names, perhaps missing \
                                comma or dot?", prefix)));
                        }
                        Some((_, c))
                            if c == '_' || c.is_alphanumeric() => continue,
                        Some((idx, _)) => break idx,
                        None => break self.buf.len() - self.off,
                    }
                };
                let val = &tail[..end_idx];
                if self.is_keyword(val) {
                    return Ok((Keyword, end_idx));
                } else if val.starts_with("__") && val.ends_with("__") {
                    return Err(Error::new(
                        "identifiers surrounded by double \
                            underscores are forbidden"));
                } else {
                    return Ok((Ident, end_idx));
                }
            }
            '0'..='9' => {
                if self.dot {
                    let len = loop {
                        match iter.next() {
                            Some((_, '0'..='9')) => continue,
                            Some((_, c)) if c.is_alphabetic() => {
                                return Err(Error::new(
                                    format_args!("unexpected char {:?}, \
                                        only integers are allowed after dot \
                                        (for tuple access)", c)
                                ));
                            }
                            Some((idx, _)) => break idx,
                            None => break self.buf.len() - self.off,
                        }
                    };
                    if cur_char == '0' && len > 1 {
                        return Err(Error::new(
                            "leading zeros are not allowed in numbers"));
                    }
                    Ok((IntConst, len))
                } else {
                    self.parse_number()
                }
            }
            '$' => {
                let mut has_letter = false;
                if let Some((_, c)) = iter.next() {
                    match c {
                        '$' => {
                            let suffix = &self.buf[self.off+2..];
                            let end = find(suffix.as_bytes(), b"$$");
                            if let Some(end) = end {
                                for c in self.buf[self.off+2..][..end].chars() {
                                    check_prohibited(c, false)?;
                                }
                                return Ok((Str, 2+end+2));
                            } else {
                                return Err(Error::new(
                                    "unterminated string started with $$"
                                ));
                            }
                        }
                        '`' => {
                            while let Some((idx, c)) = iter.next() {
                                if c == '`' {
                                    if let Some((_, '`')) = iter.next() {
                                        continue;
                                    }
                                    let var = &tail[..idx+1];
                                    if var.starts_with("$`@") {
                                        return Err(
                                            Error::new(
                                                "backtick-quoted argument \
                                                cannot start with char `@`",
                                            ));
                                    }
                                    if var.contains("::") {
                                        return Err(
                                            Error::new(
                                                "backtick-quoted argument \
                                                cannot contain `::`"));
                                    }
                                    if var.starts_with("$`__") &&
                                        var.ends_with("__`")
                                    {
                                        return Err(
                                            Error::new(
                                                "backtick-quoted arguments \
                                                surrounded by double \
                                                underscores are forbidden"));
                                    }
                                    if idx == 2 {
                                        return Err(
                                            Error::new(
                                                "backtick-quoted argument cannot be empty"
                                            ));
                                    }
                                    return Ok((Argument, idx+1));
                                }
                                check_prohibited(c, false)?;
                            }
                            return Err(Error::new(
                                "unterminated backtick argument"));
                        }
                        '0'..='9' => { }
                        c if c.is_alphabetic() || c == '_' => {
                            has_letter = true;
                        }
                        _ => return Err(Error::new(
                            "bare $ is not allowed")),
                    }
                } else {
                    return Err(Error::new(
                        "bare $ is not allowed"));
                }
                let end_idx = loop {
                    match iter.next() {
                        Some((end_idx, '$')) => {
                            let msize = end_idx+1;
                            let marker = &self.buf[self.off..][..msize];
                            if let Some('0'..='9') = marker[1..].chars().next()
                            {
                                return Err(Error::new(
                                    "dollar quote must not start with a digit",
                                ));
                            }
                            if !marker.is_ascii() {
                                return Err(Error::new(
                                    "dollar quote supports only ascii chars"));
                            }
                            if let Some(end) = find(
                                self.buf[self.off+msize..].as_bytes(),
                                marker.as_bytes())
                            {
                                let data = &self.buf[self.off+msize..][..end];
                                for c in data.chars() {
                                    check_prohibited(c, false)?;
                                }
                                return Ok((Str, msize+end+msize));
                            } else {
                                return Err(Error::new(format_args!(
                                    "unterminated string started with {:?}", marker
                                )));
                            }
                        }
                        Some((_, '0'..='9')) => continue,
                        Some((_, c)) if c.is_alphabetic() || c == '_' => {
                            has_letter = true;
                            continue;
                        }
                        Some((end_idx, _)) => break end_idx,
                        None => break self.buf.len() - self.off,
                    }
                };
                if has_letter {
                    let name = &tail[1..];
                    if let Some('0'..='9') = name.chars().next() {
                        return Err(Error::new(
                            format_args!("the {:?} is not a valid \
                            argument, either name starting with letter \
                            or only digits are expected",
                            &tail[..end_idx])));
                    }
                }
                return Ok((Argument, end_idx));
            }
            '\\' => match iter.next() {
                Some((_, '(')) => {
                    let len = loop {
                        match iter.next() {
                            Some((_, '_')) => continue,
                            Some((_, c)) if c.is_alphanumeric() => continue,
                            Some((idx, ')')) => break idx,
                            Some((_, _)) => {
                                return Err(Error::new(
                                    "only alphanumerics are allowed in \
                                     \\(name) token"));
                            }
                            None => {
                                return Err(Error::new(
                                    "unclosed \\(name) token"));
                            }
                        }
                    };
                    Ok((Substitution, len+1))
                }
                _ => return Err(
                    Error::new(
                        format_args!("unexpected character {:?}", cur_char)
                    )
                ),
            }
            _ => return Err(
                Error::new(
                    format_args!("unexpected character {:?}", cur_char)
                )
            ),
        }
    }

    fn parse_string(&mut self, quote_off: usize, raw: bool, binary: bool)
        -> Result<(Kind, usize), Error>
    {
        let mut iter = self.buf[self.off+quote_off..].char_indices();
        let open_quote = iter.next().unwrap().1;
        if binary {
            while let Some((idx, c)) = iter.next() {
                match c {
                    '\\' if !raw => match iter.next() {
                        // skip any next char, even quote
                        Some((_, _)) => continue,
                        None => break,
                    }
                    c if c as u32 > 0x7f => {
                        return Err(Error::new(
                            format_args!("invalid bytes literal: character \
                                {:?} is unexpected, only ascii chars are \
                                allowed in bytes literals", c)));
                    }
                    c if c == open_quote => {
                        return Ok((Kind::BinStr, quote_off+idx+1))
                    }
                    _ => {}
                }
            }
        } else {
            while let Some((idx, c)) = iter.next() {
                match c {
                    '\\' if !raw => match iter.next() {
                        // skip any next char, even quote
                        Some((_, _)) => continue,
                        None => break,
                    }
                    c if c == open_quote => {
                        return Ok((Kind::Str, quote_off+idx+1))
                    }
                    _ => check_prohibited(c, true)?,
                }
            }
        }
        return Err(Error::new(
            format_args!("unterminated string, quoted by `{}`", open_quote)));
    }

    fn parse_number(&mut self)
        -> Result<(Kind, usize), Error>
    {
        #[derive(PartialEq, PartialOrd)]
        enum Break {
            Dot,
            Exponent,
            Letter,
            End,
        }
        use self::Kind::*;
        let mut iter = self.buf[self.off+1..].char_indices();
        let mut suffix = None;
        let mut decimal = false;
        // decimal part
        let (mut bstate, dec_len) = loop {
            match iter.next() {
                Some((_, '0'..='9')) => continue,
                Some((_, '_')) => continue,
                Some((idx, 'e')) => break (Break::Exponent, idx+1),
                Some((idx, '.')) => break (Break::Dot, idx+1),
                Some((idx, c)) if c.is_alphabetic() => {
                    suffix = Some(idx+1);
                    break (Break::Letter, idx+1);
                }
                Some((idx, _)) => break (Break::End, idx+1),
                None => break (Break::End, self.buf.len() - self.off),
            }
        };
        if self.buf.as_bytes()[self.off] == b'0' && dec_len > 1 {
            return Err(Error::new(
                "unexpected leading zeros are not allowed in numbers"));
        }
        if bstate == Break::End {
            return Ok((IntConst, dec_len));
        }
        if bstate == Break::Dot {
            decimal = true;
            bstate = loop {
                if let Some((idx, c)) = iter.next() {
                    match c {
                        '0'..='9' => continue,
                        '_' => {
                            if idx+1 == dec_len+1 {
                                return Err(Error::new(
                                    "expected digit after dot, \
                                    found underscore"))
                            }
                            continue;
                        }
                        'e' => {
                            if idx+1 == dec_len+1 {
                                return Err(Error::new(
                                    "expected digit after dot, \
                                    found exponent"))
                            }
                            break Break::Exponent;
                        }
                        '.' => return Err(Error::new(
                            "unexpected extra decimal dot in number")),
                        c if c.is_alphabetic() => {
                            if idx == dec_len {
                                return Err(Error::new(
                                    "expected digit after dot, found suffix"))
                            }
                            suffix = Some(idx+1);
                            break Break::Letter;
                        }
                        _ => {
                            if idx+1 == dec_len+1 {
                                return Err(Error::new(
                                    "expected digit after dot, \
                                    found end of decimal"))
                            }
                            return Ok((FloatConst, idx+1));
                        }
                    }
                } else {
                    if self.buf.len() - self.off == dec_len+1 {
                        return Err(Error::new(
                            "expected digit after dot, found end of decimal"))
                    }
                    return Ok((FloatConst, self.buf.len() - self.off));
                }
            }
        }
        if bstate == Break::Exponent {
            match iter.next() {
                Some((_, '0'..='9')) => {},
                Some((_, c@'+')) | Some((_, c@'-'))=> {
                    if c == '-' {
                        decimal = true;
                    }
                    match iter.next() {
                        Some((_, '0'..='9')) => {},
                        Some((_, '.')) => return Err(
                            Error::new(
                            "unexpected extra decimal dot in number")),
                        _ => return Err(Error::new(
                            "unexpected optional `+` or `-` followed by digits must \
                                follow `e` in float const")),
                    }
                }
                _ => return Err(Error::new(
                    "unexpected optional `+` or `-` followed by digits must \
                        follow `e` in float const")),
            }
            loop {
                match iter.next() {
                    Some((_, '0'..='9')) => continue,
                    Some((_, '_')) => continue,
                    Some((_, '.')) => return Err(
                        Error::new(
                        "unexpected extra decimal dot in number")),
                    Some((idx, c)) if c.is_alphabetic() => {
                        suffix = Some(idx+1);
                        break;
                    }
                    Some((idx, _)) => return Ok((FloatConst, idx+1)),
                    None => return Ok((FloatConst, self.buf.len() - self.off)),
                }
            }
        }
        let soff = suffix.expect("tokenizer integrity error");
        let end = loop {
            if let Some((idx, c)) = iter.next() {
                if c != '_' && !c.is_alphanumeric() {
                    break idx+1;
                }
            } else {
                break self.buf.len() - self.off;
            }
        };
        let suffix = &self.buf[self.off+soff..self.off+end];
        if suffix == "n" {
            if decimal {
                return Ok((DecimalConst, end));
            } else {
                return Ok((BigIntConst, end));
            }
        } else {
            let suffix = if suffix.len() > 8 {
                Cow::Owned(format!("{}...", &suffix[..8]))
            } else {
                Cow::Borrowed(suffix)
            };
            let val = if soff < 20 {
                &self.buf[self.off..][..soff]
            } else {
                "123"
            };
            if suffix.starts_with('O') {
                return Err(Error::new(
                    format_args!("suffix {:?} is invalid for \
                        numbers, perhaps mixed up letter `O` \
                        with zero `0`?", suffix)));
            } else if decimal {
                return Err(Error::new(
                    format_args!("suffix {:?} is invalid for \
                        numbers, perhaps you wanted `{}n` (decimal)?",
                        suffix, val)));
            } else {
                return Err(Error::new(
                    format_args!("suffix {:?} is invalid for \
                        numbers, perhaps you wanted `{}n` (bigint)?",
                        suffix, val)));
            }
        }
    }

    fn skip_whitespace(&mut self) {
        let mut iter = self.buf[self.off..].char_indices();
        let idx = 'outer: loop {
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
                ' ' => {
                    self.position.column += 1;
                    continue;
                }
                //comment
                '#' => {
                    while let Some((idx, cur_char)) = iter.next() {
                        if check_prohibited(cur_char, false).is_err() {
                            // can't return error from skip_whitespace
                            // but we return up to this char, so the tokenizer
                            // chokes on it next time is invoked
                            break 'outer idx;
                        }
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
        self.off += idx;
        self.position.offset += idx as u64;
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
        self.position.offset += len as u64;
    }

    fn is_keyword(&mut self, s: &str) -> bool {
        if s.len() > MAX_KEYWORD_LENGTH {
            return false;
        }
        self.keyword_buf.clear();
        self.keyword_buf.push_str(s);
        self.keyword_buf.make_ascii_lowercase();
        return is_keyword(&self.keyword_buf)
    }
}

impl<'a> fmt::Display for TokenStub<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}[{:?}]", self.text, self.kind)
    }
}

impl<'a> fmt::Display for Token<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}[{:?}]", self.text, self.kind)
    }
}

fn check_prohibited(c: char, escape: bool)
    -> Result<(), Error>
{
    match c {
        '\0' if escape => {
            Err(Error::new(
                "character U+0000 is not allowed"
            ))
        }
        '\0' |
        '\u{202A}' | '\u{202B}' | '\u{202C}' | '\u{202D}' |
        '\u{202E}' | '\u{2066}' | '\u{2067}' | '\u{2068}' |
        '\u{2069}' => {
            if escape {
                Err(Error::new(format!(
                    "character U+{0:04X} is not allowed, \
                     use escaped form \\u{0:04x}",
                    c as u32)))
            } else {
                Err(Error::new(
                    format!("character U+{:04X} is not allowed", c as u32)))
            }
        }
        _ => Ok(())
    }
}

/// Check if the lowercase name is a keyword
pub fn is_keyword(s: &str) -> bool {
    match s {
        // # Reserved keywords #
          // Keep in sync with keywords::CURRENT_RESERVED_KEYWORDS
        | "__source__"
        | "__subject__"
        | "__type__"
        | "__std__"
        | "__edgedbsys__"
        | "__edgedbtpl__"
        | "__new__"
        | "__old__"
        | "__specified__"
        | "administer"
        | "analyze"
        | "alter"
        | "and"
        | "anytuple"
        | "anytype"
        | "by"
        | "commit"
        | "configure"
        | "create"
        | "delete"
        | "describe"
        | "detached"
        | "distinct"
        | "do"
        | "drop"
        | "else"
        | "exists"
        | "extending"
        | "false"
        | "filter"
        | "for"
        | "group"
        | "if"
        | "ilike"
        | "in"
        | "insert"
        | "introspect"
        | "is"
        | "like"
        | "limit"
        | "module"
        | "not"
        | "offset"
        | "optional"
        | "or"
        | "rollback"
        | "select"
        | "set"
        | "start"
        | "true"
        | "typeof"
        | "update"
        | "variadic"
        | "with"
          // Keep in sync with keywords::CURRENT_RESERVED_KEYWORDS
        // # Future reserved keywords #
          // Keep in sync with keywords::FUTURE_RESERVED_KEYWORDS
        | "anyarray"
        | "begin"
        | "case"
        | "check"
        | "deallocate"
        | "discard"
        | "end"
        | "explain"
        | "fetch"
        | "get"
        | "global"
        | "grant"
        | "import"
        | "listen"
        | "load"
        | "lock"
        | "match"
        | "move"
        | "notify"
        | "prepare"
        | "partition"
        | "raise"
        | "refresh"
        | "revoke"
        | "on"
        | "over"
        | "when"
        | "window"
        | "never"
          // Keep in sync with keywords::FUTURE_RESERVED_KEYWORDS
        // # Partial reserved keywords #
          // Keep in sync with keywords::PARTIAL_RESERVED_KEYWORDS
        | "except"
        | "intersect"
        | "union"
          // Keep in sync with keywords::PARTIAL_RESERVED_KEYWORDS
        => true,
        _ => false,
    }
}

impl <'a> std::cmp::PartialEq for Token<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind && self.text == other.text && self.value == other.value
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
