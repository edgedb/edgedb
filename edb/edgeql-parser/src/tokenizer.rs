use std::fmt;
use std::borrow::Cow;

use combine::{StreamOnce, Positioned};
use combine::error::{StreamError};
use combine::stream::{ResetStream};
use combine::easy::{Error, Errors};
use twoway::find_str;

use crate::position::Pos;


// Current max keyword length is 10, but we're reserving some space
pub const MAX_KEYWORD_LENGTH: usize = 16;


#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub enum Kind {
    Assign,           // :=
    SubAssign,        // -=
    AddAssign,        // +=
    Arrow,            // ->
    Coalesce,         // ??
    Namespace,        // ::
    ForwardLink,      // .>
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
    Keyword,
    Ident,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct Token<'a> {
    pub kind: Kind,
    pub value: &'a str,
}

#[derive(Debug, Clone)]
pub struct SpannedToken<'a> {
    pub token: Token<'a>,
    pub start: Pos,
    pub end: Pos,
}

#[derive(Debug, PartialEq)]
pub struct TokenStream<'a> {
    buf: &'a str,
    position: Pos,
    off: usize,
    dot: bool,
    next_state: Option<(usize, Token<'a>, usize, Pos, Pos)>,
    keyword_buf: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Checkpoint {
    position: Pos,
    off: usize,
    dot: bool,
}

impl<'a, 'b> Iterator for &'b mut TokenStream<'a> {
    type Item = Result<SpannedToken<'a>, Error<Token<'a>, Token<'a>>>;
    fn next(&mut self) -> Option<Self::Item> {
        let start = Positioned::position(self);
        match self.read_token() {
            Ok((token, end)) => Some(Ok(SpannedToken {
                token, start, end,
            })),
            Err(e) if e == Error::end_of_input() => None,
            Err(e) => Some(Err(e)),
        }
    }
}

impl<'a> StreamOnce for TokenStream<'a> {
    type Token = Token<'a>;
    type Range = Token<'a>;
    type Position = Pos;
    type Error = Errors<Token<'a>, Token<'a>, Pos>;

    fn uncons(&mut self) -> Result<Self::Token, Error<Token<'a>, Token<'a>>> {
        self.read_token().map(|(t, _)| t)
    }
}

impl<'a> Positioned for TokenStream<'a> {
    fn position(&self) -> Self::Position {
        self.position
    }
}

impl<'a> ResetStream for TokenStream<'a> {
    type Checkpoint = Checkpoint;
    fn checkpoint(&self) -> Self::Checkpoint {
        Checkpoint {
            position: self.position,
            off: self.off,
            dot: self.dot,
        }
    }
    fn reset(&mut self, checkpoint: Checkpoint) -> Result<(), Self::Error> {
        self.position = checkpoint.position;
        self.off = checkpoint.off;
        self.dot = checkpoint.dot;
        Ok(())
    }
}

impl<'a> TokenStream<'a> {
    pub fn new(s: &str) -> TokenStream {
        let mut me = TokenStream {
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
    pub fn new_at(s: &str, position: Pos) -> TokenStream {
        let mut me = TokenStream {
            buf: s,
            position: position,
            off: 0,
            dot: false,
            next_state: None,
            keyword_buf: String::with_capacity(MAX_KEYWORD_LENGTH),
        };
        me.skip_whitespace();
        me
    }

    pub fn current_pos(&self) -> Pos {
        self.position
    }

    fn read_token(&mut self)
        -> Result<(Token<'a>, Pos), Error<Token<'a>, Token<'a>>>
    {
        // This quickly resets the stream one token back
        // (the most common reset that used quite often)
        if let Some((at, tok, off, end, next)) = self.next_state {
            if at == self.off {
                self.off = off;
                self.position = next;
                return Ok((tok, end));
            }
        }
        let old_pos = self.off;
        let (kind, len) = self.peek_token()?;

        // note we may want to get rid of "update_position" here as it's
        // faster to update 'as you go', but this is easier to get right first
        self.update_position(len);
        self.dot = match kind {
            Kind::Dot | Kind::ForwardLink => true,
            _ => false,
        };
        let value = &self.buf[self.off-len..self.off];
        let end = self.position;

        self.skip_whitespace();
        let token = Token { kind, value };
        // This is for quick reset on token back
        self.next_state = Some((old_pos, token, self.off, end, self.position));
        Ok((token, end))
    }

    fn peek_token(&mut self)
        -> Result<(Kind, usize), Error<Token<'a>, Token<'a>>>
    {
        use self::Kind::*;
        let tail = &self.buf[self.off..];
        let mut iter = tail.char_indices();
        let cur_char = match iter.next() {
            Some((_, x)) => x,
            None => return Err(Error::end_of_input()),
        };

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
                Some((_, '>')) => return Ok((ForwardLink, 2)),
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
                        return Err(Error::unexpected_static_message(
                            "`?!` is not an operator, \
                                did you mean `?!=` ?"))
                    }
                }
                _ => {
                    return Err(Error::unexpected_static_message(
                        "Bare `?` is not an operator, \
                            did you mean `?=` or `??` ?"))
                }
            },
            '!' => match iter.next() {
                Some((_, '=')) => return Ok((NotEq, 2)),
                _ => {
                    return Err(Error::unexpected_static_message(
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
                            return Err(Error::unexpected_static_message(
                                "backtick-quoted name cannot \
                                    start with char `@`"));
                        }
                        if val.contains("::") {
                            return Err(Error::unexpected_static_message(
                                "backtick-quoted name cannot \
                                    contain `::`"));
                        }
                        if val.starts_with("`__") && val.ends_with("__`") {
                            return Err(Error::unexpected_static_message(
                                "backtick-quoted names surrounded by double \
                                    underscores are forbidden"));
                        }
                        if idx == 1 {
                            return Err(Error::unexpected_static_message(
                                "backtick quotes cannot be empty"));
                        }
                        return Ok((BacktickName, idx+1));
                    }
                }
                return Err(Error::unexpected_static_message(
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
            '*' => return Ok((Mul, 1)),
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
                                _ => return Err(Error::unexpected_format(
                                    format_args!("prefix {:?} \
                                    is not allowed for strings, \
                                    allowed: `b`, `r`",
                                    prefix))),
                            };
                            return self.parse_string(idx, raw, binary);
                        }
                        Some((idx, '`')) => {
                            let prefix = &tail[..idx];
                            return Err(Error::unexpected_format(
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
                    return Err(Error::unexpected_static_message(
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
                                return Err(Error::unexpected_format(
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
                        return Err(Error::unexpected_static_message(
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
                            if let Some(end) = find_str(
                                &self.buf[self.off+2..], "$$")
                            {
                                return Ok((Str, 2+end+2));
                            } else {
                                return Err(Error::unexpected_static_message(
                                    "unterminated string started \
                                        with $$"));
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
                                            Error::unexpected_static_message(
                                                "backtick-quoted argument \
                                                cannot start with char `@`",
                                            ));
                                    }
                                    if var.contains("::") {
                                        return Err(
                                            Error::unexpected_static_message(
                                                "backtick-quoted argument \
                                                cannot contain `::`"));
                                    }
                                    if var.starts_with("$`__") &&
                                        var.ends_with("__`")
                                    {
                                        return Err(
                                            Error::unexpected_static_message(
                                                "backtick-quoted arguments \
                                                surrounded by double \
                                                underscores are forbidden"));
                                    }
                                    if idx == 2 {
                                        return Err(
                                            Error::unexpected_static_message(
                                                "backtick-quoted argument \
                                                cannot be empty"));
                                    }
                                    return Ok((Argument, idx+1));
                                }
                            }
                            return Err(Error::unexpected_static_message(
                                "unterminated backtick argument"));
                        }
                        '0'..='9' => { }
                        c if c.is_alphabetic() || c == '_' => {
                            has_letter = true;
                        }
                        _ => return Err(Error::unexpected_static_message(
                            "bare $ is not allowed")),
                    }
                } else {
                    return Err(Error::unexpected_static_message(
                        "bare $ is not allowed"));
                }
                let end_idx = loop {
                    match iter.next() {
                        Some((end_idx, '$')) => {
                            let msize = end_idx+1;
                            let marker = &self.buf[self.off..][..msize];
                            if let Some('0'..='9') = marker[1..].chars().next()
                            {
                                return Err(Error::unexpected_static_message(
                                    "dollar quote must not start with a digit",
                                ));
                            }
                            if !marker.is_ascii() {
                                return Err(Error::unexpected_static_message(
                                    "dollar quote supports only ascii chars"));
                            }
                            if let Some(end) = find_str(
                                &self.buf[self.off+msize..],
                                &marker)
                            {
                                return Ok((Str, msize+end+msize));
                            } else {
                                return Err(Error::unexpected_format(
                                    format_args!("unterminated string started \
                                        with {:?}", marker)));
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
                        return Err(Error::unexpected_format(
                            format_args!("the {:?} is not a valid \
                            argument, either name starting with letter \
                            or only digits are expected",
                            &tail[..end_idx])));
                    }
                }
                return Ok((Argument, end_idx));
            }
            _ => return Err(
                Error::unexpected_format(
                    format_args!("unexpected character {:?}", cur_char)
                )
            ),
        }
    }

    fn parse_string(&mut self, quote_off: usize, raw: bool, binary: bool)
        -> Result<(Kind, usize), Error<Token<'a>, Token<'a>>>
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
                        return Err(Error::unexpected_format(
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
                    _ => {}
                }
            }
        }
        return Err(Error::unexpected_format(
            format_args!("unterminated string, quoted by `{}`", open_quote)));
    }

    fn parse_number(&mut self)
        -> Result<(Kind, usize), Error<Token<'a>, Token<'a>>>
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
            return Err(Error::unexpected_static_message(
                "leading zeros are not allowed in numbers"));
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
                        'e' => break Break::Exponent,
                        '.' => return Err(Error::unexpected_static_message(
                            "extra decimal dot in number")),
                        c if c.is_alphabetic() => {
                            suffix = Some(idx+1);
                            break Break::Letter;
                        }
                        _ => return Ok((FloatConst, idx+1)),
                    }
                } else {
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
                            Error::unexpected_static_message(
                            "extra decimal dot in number")),
                        _ => return Err(Error::unexpected_static_message(
                            "optional `+` or `-` followed by digits must \
                                follow `e` in float const")),
                    }
                }
                _ => return Err(Error::unexpected_static_message(
                    "optional `+` or `-` followed by digits must \
                        follow `e` in float const")),
            }
            loop {
                match iter.next() {
                    Some((_, '0'..='9')) => continue,
                    Some((_, '.')) => return Err(
                        Error::unexpected_static_message(
                        "extra decimal dot in number")),
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
            if suffix.chars().next() == Some('O') {
                return Err(Error::unexpected_format(
                    format_args!("suffix {:?} is invalid for \
                        numbers, perhaps mixed up letter `O` \
                        with zero `0`?", suffix)));
            } else if decimal {
                return Err(Error::unexpected_format(
                    format_args!("suffix {:?} is invalid for \
                        numbers, perhaps you wanted `{}n` (decimal)?",
                        suffix, val)));
            } else {
                return Err(Error::unexpected_format(
                    format_args!("suffix {:?} is invalid for \
                        numbers, perhaps you wanted `{}n` (bigint)?",
                        suffix, val)));
            }
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
                ' ' => {
                    self.position.column += 1;
                    continue;
                }
                //comment
                '#' => {
                    while let Some((_, cur_char)) = iter.next() {
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

impl<'a> fmt::Display for Token<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}[{:?}]", self.value, self.kind)
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
        | "alter"
        | "and"
        | "anytuple"
        | "anytype"
        | "commit"
        | "configure"
        | "create"
        | "declare"
        | "delete"
        | "describe"
        | "detached"
        | "distinct"
        | "drop"
        | "else"
        | "empty"
        | "exists"
        | "extending"
        | "false"
        | "filter"
        | "for"
        | "function"
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
        | "order"
        | "release"
        | "reset"
        | "rollback"
        | "select"
        | "set"
        | "start"
        | "true"
        | "typeof"
        | "update"
        | "union"
        | "variadic"
        | "with"
          // Keep in sync with keywords::CURRENT_RESERVED_KEYWORDS
        // # Future reserved keywords #
          // Keep in sync with keywords::FUTURE_RESERVED_KEYWORDS
        | "analyze"
        | "anyarray"
        | "begin"
        | "case"
        | "check"
        | "deallocate"
        | "discard"
        | "do"
        | "end"
        | "execute"
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
        | "policy"
        | "raise"
        | "refresh"
        | "reindex"
        | "revoke"
        | "over"
        | "when"
        | "window"
          // Keep in sync with keywords::FUTURE_RESERVED_KEYWORDS
        => true,
        _ => false,
    }
}
