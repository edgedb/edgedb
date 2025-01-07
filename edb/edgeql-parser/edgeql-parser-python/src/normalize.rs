use std::collections::BTreeSet;

use edgeql_parser::keywords::Keyword;
use edgeql_parser::position::{Pos, Span};
use edgeql_parser::tokenizer::{Kind, Token, Tokenizer, Value};

use blake2::{Blake2b512, Digest};

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Variable {
    pub value: Value,
}

pub struct Entry {
    pub processed_source: String,
    pub hash: [u8; 64],
    pub tokens: Vec<Token<'static>>,
    pub variables: Vec<Vec<Variable>>,
    pub named_args: bool,
    pub first_arg: Option<usize>,
}

/// PackedEntry is a compact Entry for serialization purposes
#[derive(serde::Serialize, serde::Deserialize)]
pub struct PackedEntry {
    pub tokens: Vec<Token<'static>>,
    pub variables: Vec<Vec<Variable>>,
    pub named_args: bool,
    pub first_arg: Option<usize>,
}

impl From<Entry> for PackedEntry {
    fn from(val: Entry) -> Self {
        PackedEntry {
            tokens: val.tokens,
            variables: val.variables,
            named_args: val.named_args,
            first_arg: val.first_arg,
        }
    }
}

impl From<PackedEntry> for Entry {
    fn from(val: PackedEntry) -> Self {
        let processed_source = serialize_tokens(&val.tokens[..]);
        Entry {
            hash: hash(&processed_source),
            processed_source,
            tokens: val.tokens,
            variables: val.variables,
            named_args: val.named_args,
            first_arg: val.first_arg,
        }
    }
}

#[derive(Debug)]
pub enum Error {
    Tokenizer(String, u64),
    Assertion(String, Pos),
}

pub fn normalize(text: &str) -> Result<Entry, Error> {
    let tokens = Tokenizer::new(text)
        .validated_values()
        .with_eof()
        .map(|x| x.map(|t| t.cloned()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| Error::Tokenizer(e.message, e.span.start))?;

    let (named_args, var_idx) = match scan_vars(&tokens) {
        Some(pair) => pair,
        None => {
            // don't extract from invalid query, let python code do its work
            let processed_source = serialize_tokens(&tokens);
            return Ok(Entry {
                hash: hash(&processed_source),
                processed_source,
                tokens,
                variables: Vec::new(),
                named_args: false,
                first_arg: None,
            });
        }
    };
    let mut rewritten_tokens = Vec::with_capacity(tokens.len());
    let mut all_variables = Vec::new();
    let mut variables = Vec::new();
    let mut counter = var_idx;
    let mut next_var = || {
        let n = counter;
        counter += 1;
        if named_args {
            format!("$__edb_arg_{}", n)
        } else {
            format!("${}", n)
        }
    };
    let mut last_was_set = false;
    for tok in &tokens {
        let mut is_set = false;
        match tok.kind {
            Kind::IntConst
            // Don't replace `.12` because this is a tuple access
            if !matches!(rewritten_tokens.last(),
                Some(Token { kind: Kind::Dot, .. }))
            // Don't replace 'LIMIT 1' as a special case
            && (tok.text != "1"
                || !matches!(rewritten_tokens.last(),
                    Some(Token { kind: Kind::Keyword(Keyword("limit")), .. })))
            && tok.text != "9223372036854775808"
            => {
                rewritten_tokens.push(arg_type_cast(
                    "int64", next_var(), tok.span
                ));
                variables.push(Variable {
                    value: tok.value.clone().unwrap(),
                });
                continue;
            }
            Kind::FloatConst => {
                rewritten_tokens.push(arg_type_cast(
                    "float64", next_var(), tok.span
                ));
                variables.push(Variable {
                    value: tok.value.clone().unwrap(),
                });
                continue;
            }
            Kind::BigIntConst => {
                rewritten_tokens.push(arg_type_cast(
                    "bigint", next_var(), tok.span
                ));
                variables.push(Variable {
                    value: tok.value.clone().unwrap(),
                });
                continue;
            }
            Kind::DecimalConst => {
                rewritten_tokens.push(arg_type_cast(
                    "decimal", next_var(), tok.span
                ));
                variables.push(Variable {
                    value: tok.value.clone().unwrap(),
                });
                continue;
            }
            Kind::Str => {
                rewritten_tokens.push(arg_type_cast(
                    "str", next_var(), tok.span
                ));
                variables.push(Variable {
                    value: tok.value.clone().unwrap(),
                });
                continue;
            }
            Kind::Keyword(Keyword(kw))
            if (
                matches!(kw, "configure"|"create"|"alter"|"drop"|"start"|"analyze")
                || (last_was_set && kw == "global")
            ) => {
                let processed_source = serialize_tokens(&tokens);
                return Ok(Entry {
                    hash: hash(&processed_source),
                    processed_source,
                    tokens,
                    variables: Vec::new(),
                    named_args: false,
                    first_arg: None,
                });
            }
            // Split on semicolons.
            // N.B: This naive statement splitting on semicolons works
            // because the only statements with internal semis are DDL
            // statements, which we don't support anyway.
            Kind::Semicolon => {
                all_variables.push(variables);
                variables = Vec::new();
                rewritten_tokens.push(tok.clone());
            }
            Kind::Keyword(Keyword("set")) => {
                is_set = true;
                rewritten_tokens.push(tok.clone());
            }
            _ => rewritten_tokens.push(tok.clone()),
        }
        last_was_set = is_set;
    }

    all_variables.push(variables);
    // N.B: We always serialize the tokens to produce
    // processed_source, even when no changes have been made. This is
    // because when Source gets serialized, it always uses a
    // PackedEntry, which will result in it being normalized *there*,
    // and so if we don't do it *here*, then we won't be able to hit
    // the persistent cache in cases where we didn't reserialize the
    // tokens.
    // TODO: Rework the caching to avoid needing to do this.
    let processed_source = serialize_tokens(&rewritten_tokens[..]);
    Ok(Entry {
        hash: hash(&processed_source),
        processed_source,
        named_args,
        first_arg: if counter <= var_idx {
            None
        } else {
            Some(var_idx)
        },
        tokens: rewritten_tokens,
        variables: all_variables,
    })
}

fn is_operator(token: &Token) -> bool {
    use edgeql_parser::tokenizer::Kind::*;
    match token.kind {
        Assign | SubAssign | AddAssign | Arrow | Coalesce | Namespace | DoubleSplat
        | BackwardLink | FloorDiv | Concat | GreaterEq | LessEq | NotEq | NotDistinctFrom
        | DistinctFrom | Comma | OpenParen | CloseParen | OpenBracket | CloseBracket
        | OpenBrace | CloseBrace | Dot | Semicolon | Colon | Add | Sub | Mul | Div | Modulo
        | Pow | Less | Greater | Eq | Ampersand | Pipe | At => true,
        DecimalConst | FloatConst | IntConst | BigIntConst | BinStr | Parameter
        | ParameterAndType | Str | BacktickName | Keyword(_) | Ident | Substitution | EOI
        | Epsilon | StartBlock | StartExtension | StartFragment | StartMigration
        | StartSDLDocument | StrInterpStart | StrInterpCont | StrInterpEnd => false,
    }
}

fn serialize_tokens(tokens: &[Token]) -> String {
    use edgeql_parser::tokenizer::Kind::Parameter;

    let mut buf = String::new();
    let mut needs_space = false;
    for token in tokens {
        if matches!(token.kind, Kind::EOI) {
            break;
        }

        if needs_space && !is_operator(token) && token.kind != Parameter {
            buf.push(' ');
        }
        buf.push_str(&token.text);
        needs_space = !is_operator(token);
    }
    buf
}

fn scan_vars<'x, 'y: 'x, I>(tokens: I) -> Option<(bool, usize)>
where
    I: IntoIterator<Item = &'x Token<'x>>,
{
    let mut max_visited = None::<usize>;
    let mut names = BTreeSet::new();
    for t in tokens {
        if t.kind == Kind::Parameter {
            if let Ok(v) = t.text[1..].parse() {
                if max_visited.map(|old| v > old).unwrap_or(true) {
                    max_visited = Some(v);
                }
            } else {
                names.insert(&t.text[..]);
            }
        }
    }
    if names.is_empty() {
        let next = max_visited.map(|x| x.checked_add(1)).unwrap_or(Some(0))?;
        Some((false, next))
    } else if max_visited.is_some() {
        return None; // mixed arguments
    } else {
        Some((true, names.len()))
    }
}

fn hash(text: &str) -> [u8; 64] {
    let mut result = [0u8; 64];
    result.copy_from_slice(&Blake2b512::new_with_prefix(text.as_bytes()).finalize());
    result
}

/// Produces tokens corresponding to (<lit typ>$var)
fn arg_type_cast(typ: &'static str, var: String, span: Span) -> Token<'static> {
    // the `lit` is required so these tokens have different text than an actual
    // type cast and parameter, so their hashes don't clash.
    Token {
        kind: Kind::ParameterAndType,
        text: format!("<lit {typ}>{var}").into(),
        value: None,
        span,
    }
}

#[cfg(test)]
mod test {
    use super::scan_vars;
    use edgeql_parser::tokenizer::{Token, Tokenizer};

    fn tokenize(s: &str) -> Vec<Token> {
        let mut r = Vec::new();
        let mut s = Tokenizer::new(s);
        loop {
            match s.next() {
                Some(Ok(x)) => r.push(x),
                None => break,
                Some(Err(e)) => panic!("Parse error at {}: {}", s.current_pos(), e.message),
            }
        }
        r
    }

    #[test]
    fn none() {
        assert_eq!(scan_vars(&tokenize("SELECT 1+1")).unwrap(), (false, 0));
    }

    #[test]
    fn numeric() {
        assert_eq!(scan_vars(&tokenize("$0 $1 $2")).unwrap(), (false, 3));
        assert_eq!(scan_vars(&tokenize("$2 $3 $2")).unwrap(), (false, 4));
        assert_eq!(scan_vars(&tokenize("$0 $0 $0")).unwrap(), (false, 1));
        assert_eq!(scan_vars(&tokenize("$10 $100")).unwrap(), (false, 101));
    }

    #[test]
    fn named() {
        assert_eq!(scan_vars(&tokenize("$a")).unwrap(), (true, 1));
        assert_eq!(scan_vars(&tokenize("$b $c $d")).unwrap(), (true, 3));
        assert_eq!(scan_vars(&tokenize("$b $c $b")).unwrap(), (true, 2));
        assert_eq!(
            scan_vars(&tokenize("$a $b $b $a $c $xx")).unwrap(),
            (true, 4)
        );
    }

    #[test]
    fn mixed() {
        assert_eq!(scan_vars(&tokenize("$a $0")), None);
        assert_eq!(scan_vars(&tokenize("$0 $a")), None);
        assert_eq!(scan_vars(&tokenize("$b $c $100")), None);
        assert_eq!(scan_vars(&tokenize("$10 $xx $yy")), None);
    }
}
