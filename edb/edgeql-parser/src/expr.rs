use crate::position::{InflatedPos, Pos};
use crate::tokenizer::{self, Kind};

/// Error of expression checking
///
/// See [check][].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{}: tokenizer error: {}", _1, _0)]
    Tokenizer(String, Pos),
    #[error(
        "{}: closing bracket mismatch, opened {:?} at {}, encountered {:?}",
        closing_pos,
        opened,
        opened_pos,
        encountered
    )]
    BracketMismatch {
        opened: &'static str,
        encountered: &'static str,
        opened_pos: Pos,
        closing_pos: Pos,
    },
    #[error("{}: extra closing bracket {:?}", _1, _0)]
    ExtraBracket(&'static str, Pos),
    #[error("{}: bracket {:?} has never been closed", _1, _0)]
    MissingBracket(&'static str, Pos),
    #[error(
        "{}: token {:?} is not allowed in expression \
             (try parenthesize the expression)",
        _1,
        _0
    )]
    UnexpectedToken(String, Pos),
    #[error("expression is empty")]
    Empty,
}

fn bracket_str(tok: Kind) -> &'static str {
    use crate::tokenizer::Kind::*;

    match tok {
        OpenBracket => "[",
        CloseBracket => "]",
        OpenBrace => "{",
        CloseBrace => "}",
        OpenParen => "(",
        CloseParen => ")",
        _ => unreachable!("token is not a bracket"),
    }
}

fn matching_bracket(tok: Kind) -> Kind {
    use crate::tokenizer::Kind::*;

    match tok {
        OpenBracket => CloseBracket,
        OpenBrace => CloseBrace,
        OpenParen => CloseParen,
        _ => unreachable!("token is not a bracket"),
    }
}

/// Minimal validation of expression
///
/// This is used for substitutions in migrations. This check merely ensures
/// that overall structure of the statement is not ruined. Mostly checks for
/// matching brackets and quotes closed.
///
/// More specificaly current implementation checks that expression is not
/// empty, checks for valid tokens, matching braces and disallows comma `,`and
/// semicolon `;` outside of brackets.
///
/// This is NOT a security measure.
pub fn check(text: &str) -> Result<(), Error> {
    use crate::tokenizer::Kind::*;
    use Error::*;

    let mut brackets = Vec::new();
    let mut parser = &mut tokenizer::Tokenizer::new(text);
    let mut empty = true;
    for token in &mut parser {
        let token = match token {
            Ok(t) => t,
            Err(crate::tokenizer::Error { message, .. }) => {
                return Err(Tokenizer(message, parser.current_pos()));
            }
        };
        let pos = token.span.start;
        let pos = InflatedPos::from_offset(text.as_bytes(), pos)
            .unwrap()
            .deflate();

        empty = false;
        match token.kind {
            Comma | Semicolon if brackets.is_empty() => {
                return Err(UnexpectedToken(token.text.into(), pos));
            }
            OpenParen | OpenBracket | OpenBrace => {
                brackets.push((token.kind, pos));
            }
            CloseParen | CloseBracket | CloseBrace => match brackets.pop() {
                Some((opened, opened_pos)) => {
                    if matching_bracket(opened) != token.kind {
                        return Err(BracketMismatch {
                            opened: bracket_str(opened),
                            opened_pos,
                            encountered: bracket_str(token.kind),
                            closing_pos: pos,
                        });
                    }
                }
                None => {
                    return Err(ExtraBracket(bracket_str(token.kind), pos));
                }
            },
            _ => {}
        }
    }
    if let Some((bracket, pos)) = brackets.pop() {
        return Err(MissingBracket(bracket_str(bracket), pos));
    }
    if empty {
        return Err(Empty);
    }
    Ok(())
}
