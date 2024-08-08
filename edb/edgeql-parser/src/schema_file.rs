use crate::position::Pos;
use crate::tokenizer;
use crate::tokenizer::Tokenizer;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SchemaFileError {
    #[error("{}: bracket `{}` has never been closed", pos, kind)]
    MissingBracket { pos: Pos, kind: char },
    #[error(
        "{}: closing bracket mismatch, opened `{}` at {}, encountered `{}`",
        closing_pos,
        opened,
        opened_pos,
        encountered
    )]
    BracketMismatch {
        opened: char,
        opened_pos: Pos,
        closing_pos: Pos,
        encountered: char,
    },
    #[error("{}: extra closing bracket `{}`", pos, kind)]
    ExtraBracket { pos: Pos, kind: char },
    #[error("{}: tokenizer error: {}", pos, error)]
    TokenizerError { pos: Pos, error: String },
}

fn match_bracket(
    open: char,
    encountered: char,
    pos: Pos,
    brackets: &mut Vec<(char, char, Pos)>,
) -> Result<(), SchemaFileError> {
    use SchemaFileError::*;

    match brackets.pop() {
        Some((_, exp, _)) if exp == encountered => Ok(()),
        Some((opened, _, opened_pos)) => Err(BracketMismatch {
            opened,
            opened_pos,
            closing_pos: pos,
            encountered,
        }),
        None => Err(ExtraBracket { pos, kind: open }),
    }
}

pub fn validate(text: &str) -> Result<(), SchemaFileError> {
    use tokenizer::Kind::*;
    use SchemaFileError::*;

    let mut token_stream = Tokenizer::new(text);
    let mut brackets = Vec::new();
    loop {
        let pos = token_stream.current_pos();
        match token_stream.next() {
            Some(Ok(tok)) => match tok.kind {
                OpenParen => brackets.push(('(', ')', pos)),
                OpenBrace => brackets.push(('{', '}', pos)),
                OpenBracket => brackets.push(('[', ']', pos)),
                CloseParen => match_bracket('(', ')', pos, &mut brackets)?,
                CloseBrace => match_bracket('{', '}', pos, &mut brackets)?,
                CloseBracket => match_bracket('[', ']', pos, &mut brackets)?,
                _ => {}
            },
            None => break,
            Some(Err(e)) => {
                return Err(TokenizerError {
                    pos: token_stream.current_pos(),
                    error: e.message,
                });
            }
        }
    }
    if let Some((kind, _, pos)) = brackets.pop() {
        return Err(MissingBracket { kind, pos });
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::validate;

    fn check(s: &str) -> String {
        validate(s)
            .map(|_| String::new())
            .map_err(|e| {
                let s = e.to_string();
                assert!(!s.is_empty());
                s
            })
            .unwrap_or_else(|e| e)
    }

    #[test]
    fn test_normal() {
        assert_eq!(check("alias X := (SELECT 1)"), "");
    }

    #[test]
    fn test_braces() {
        assert_eq!(
            check("type X { property y := '}';"),
            "1:8: bracket `{` has never been closed"
        );

        assert_eq!(
            check("type X { property y -> z; )"),
            "1:27: closing bracket mismatch, \
            opened `{` at 1:8, encountered `)`"
        );

        assert_eq!(
            check("type X\nproperty y; }"),
            "2:13: extra closing bracket `{`"
        );

        assert_eq!(check("type X { property y := (select 1)}"), "");

        assert_eq!(
            check("type X { property y := (select 1})"),
            "1:33: closing bracket mismatch, \
            opened `(` at 1:24, encountered `}`"
        );

        assert_eq!(
            check("type X { property y := (select 1"),
            "1:24: bracket `(` has never been closed"
        );

        assert_eq!(
            check("type X { property y := (select 1)}}"),
            "1:35: extra closing bracket `{`"
        );

        assert_eq!(check("type X { property y := .z[1]}"), "");
    }

    #[test]
    fn test_str() {
        assert_eq!(
            check("create type X { \"} "),
            "1:17: tokenizer error: \
                unterminated string, quoted by `\"`"
        );
    }
}
