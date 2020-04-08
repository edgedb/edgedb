use std::collections::BTreeSet;

use edgeql_parser::tokenizer::{TokenStream, SpannedToken, Token, Kind};
use edgeql_parser::position::Pos;

const VARIABLES: &[&str] = &[
    "$_edb_arg__0",
    "$_edb_arg__1",
    "$_edb_arg__2",
    "$_edb_arg__3",
    "$_edb_arg__4",
    "$_edb_arg__5",
    "$_edb_arg__6",
    "$_edb_arg__7",
    "$_edb_arg__8",
    "$_edb_arg__9",
    "$_edb_arg__10",
    "$_edb_arg__11",
    "$_edb_arg__12",
    "$_edb_arg__13",
    "$_edb_arg__14",
    "$_edb_arg__15",
    "$_edb_arg__16",
    "$_edb_arg__17",
    "$_edb_arg__18",
    "$_edb_arg__19",
    "$_edb_arg__20",
    "$_edb_arg__21",
    "$_edb_arg__22",
    "$_edb_arg__23",
    "$_edb_arg__24",
    "$_edb_arg__25",
    "$_edb_arg__26",
    "$_edb_arg__27",
    "$_edb_arg__28",
    "$_edb_arg__29",
    "$_edb_arg__30",
];

#[derive(Debug, PartialEq)]
pub enum Value {
    Str(String),
    Int(String),
    Float(String),
    BigInt(String),
    Decimal(String),
}

#[derive(Debug, PartialEq)]
pub struct Variable {
    pub value: Value,
}

#[derive(Debug)]
pub struct Entry<'a> {
    pub key: String,
    pub tokens: Vec<SpannedToken<'a>>,
    pub variables: Vec<Variable>,
    pub end_pos: Pos,
}

#[derive(Debug)]
pub enum Error {
    Tokenizer(String, Pos),
}

fn push_var<'x>(res: &mut Vec<SpannedToken<'x>>,
    typ: &'x str, var_name: &'x str)
{
    res.push(SpannedToken {
        token: Token {
            kind: Kind::Less,
            value: "<",
        },
        start: Pos { line: 0, column: 0, offset: 0},
        end: Pos { line: 0, column: 0, offset: 0},
    });
    res.push(SpannedToken {
        token: Token {
            kind: Kind::Ident,
            value: typ,
        },
        start: Pos { line: 0, column: 0, offset: 0},
        end: Pos { line: 0, column: 0, offset: 0},
    });
    res.push(SpannedToken {
        token: Token {
            kind: Kind::Greater,
            value: ">",
        },
        start: Pos { line: 0, column: 0, offset: 0},
        end: Pos { line: 0, column: 0, offset: 0},
    });
    res.push(SpannedToken {
        token: Token {
            kind: Kind::Argument,
            value: var_name,
        },
        start: Pos { line: 0, column: 0, offset: 0},
        end: Pos { line: 0, column: 0, offset: 0},
    });
}

fn scan_vars<'x, 'y: 'x, I>(tokens: I) -> Option<(bool, usize)>
    where I: IntoIterator<Item=&'x Token<'y>>,
{
    let mut max_visited = None::<usize>;
    let mut names = BTreeSet::new();
    for t in tokens {
        if t.kind == Kind::Argument {
            if let Ok(v) = t.value[1..].parse() {
                if max_visited.map(|old| v > old).unwrap_or(true) {
                    max_visited = Some(v);
                }
            } else {
                names.insert(t.value);
            }
        }
    }
    if names.is_empty() {
        let next = max_visited.map(|x| x.checked_add(1)).unwrap_or(Some(0))?;
        Some((false, next))
    } else if max_visited.is_some() {
        return None  // mixed arguments
    } else {
        Some((true, names.len()))
    }
}

pub fn rewrite<'x>(text: &'x str)
    -> Result<Entry<'x>, Error>
{
    use combine::easy::Error::*;
    let mut token_stream = TokenStream::new(&text);
    let mut tokens = Vec::new();
    for res in &mut token_stream {
        match res {
            Ok(t) => tokens.push(t),
            Err(Unexpected(s)) => {
                return Err(Error::Tokenizer(
                    s.to_string(), token_stream.current_pos()));
            }
            Err(e) => {
                return Err(Error::Tokenizer(
                    e.to_string(), token_stream.current_pos()));
            }
        }
    }
    let end_pos = token_stream.current_pos();
    let (named_vars, var_index) = match
        scan_vars(tokens.iter().map(|t| &t.token))
    {
        Some(pair) => pair,
        None => {
            // don't extract from invalid query, let python code do its work
            return Ok(Entry {
                key: serialize_tokens(&tokens),
                tokens,
                variables: Vec::new(),
                end_pos,
            });
        }
    };
    let mut rewritten_tokens = Vec::with_capacity(tokens.len());
    let mut variables = Vec::new();
    for tok in &tokens {
        if variables.len() >= VARIABLES.len() {
            rewritten_tokens.push(tok.clone());
            continue;
        }
        match tok.token.kind {
            Kind::IntConst
            // Don't replace `.12` because this is a tuple access
            if !matches!(rewritten_tokens.last(),
                Some(SpannedToken {
                    token: Token { kind: Kind::Dot, .. },
                    ..
                }))
            // Don't replace 'LIMIT 1' as a special case
            && (tok.token.value != "1"
                || !matches!(rewritten_tokens.last(),
                    Some(SpannedToken {
                        token: Token { value: "LIMIT", kind: Kind::Keyword },
                        ..
                    })))
            => {
                let name = VARIABLES[variables.len()];
                push_var(&mut rewritten_tokens, "int64", name);
                variables.push(Variable {
                    value: Value::Int(tok.token.value.to_string()),
                });
                continue;
            }
            // TODO(tailhook)
            // Kind::FloatConst => todo!(),
            // Kind::BigIntConst => todo!(),
            // Kind::DecimalConst => todo!(),
            // Kind::Str => todo!(),
            Kind::Keyword
            if matches!(tok.token.value, "CONFIGURE"|"CREATE"|"ALTER"|"DROP")
            => {
                return Ok(Entry {
                    key: serialize_tokens(&tokens),
                    tokens,
                    variables: Vec::new(),
                    end_pos,
                });
            }
            _ => rewritten_tokens.push(tok.clone()),
        }
    }
    return Ok(Entry {
        key: serialize_tokens(&rewritten_tokens[..]),
        tokens: rewritten_tokens,
        variables,
        end_pos,
    });
}

fn is_operator(token: &Token) -> bool {
    use edgeql_parser::tokenizer::Kind::*;
    match token.kind {
        | Assign
        | SubAssign
        | AddAssign
        | Arrow
        | Coalesce
        | Namespace
        | ForwardLink
        | BackwardLink
        | FloorDiv
        | Concat
        | GreaterEq
        | LessEq
        | NotEq
        | NotDistinctFrom
        | DistinctFrom
        | Comma
        | OpenParen
        | CloseParen
        | OpenBracket
        | CloseBracket
        | OpenBrace
        | CloseBrace
        | Dot
        | Semicolon
        | Colon
        | Add
        | Sub
        | Mul
        | Div
        | Modulo
        | Pow
        | Less
        | Greater
        | Eq
        | Ampersand
        | Pipe
        | At
        => true,
        | DecimalConst
        | FloatConst
        | IntConst
        | BigIntConst
        | BinStr
        | Argument
        | Str
        | BacktickName
        | Keyword
        | Ident
        => false,
    }
}

fn serialize_tokens(tokens: &[SpannedToken<'_>]) -> String {
    use edgeql_parser::tokenizer::Kind::Argument;
    let mut buf = String::new();
    let mut needs_space = false;
    for SpannedToken { ref token, .. } in tokens {
        if needs_space && !is_operator(token) && token.kind != Argument {
            buf.push(' ');
        }
        buf.push_str(token.value);
        needs_space = !is_operator(token);
    }
    return buf;
}

#[cfg(test)]
mod test {
    use super::scan_vars;
    use combine::{StreamOnce, Positioned, easy::Error};
    use edgeql_parser::tokenizer::{TokenStream, SpannedToken, Token};

    fn tokenize<'x>(s: &'x str) -> Vec<Token<'x>> {
        let mut r = Vec::new();
        let mut s = TokenStream::new(s);
        loop {
            match s.uncons() {
                Ok(x) => r.push(x),
                Err(ref e) if e == &Error::end_of_input() => break,
                Err(e) => panic!("Parse error at {}: {}", s.position(), e),
            }
        }
        return r;
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
        assert_eq!(scan_vars(&tokenize("$a $b $b $a $c $xx")).unwrap(),
            (true, 4));
    }

    #[test]
    fn mixed() {
        assert_eq!(scan_vars(&tokenize("$a $0")), None);
        assert_eq!(scan_vars(&tokenize("$0 $a")), None);
        assert_eq!(scan_vars(&tokenize("$b $c $100")), None);
        assert_eq!(scan_vars(&tokenize("$10 $xx $yy")), None);
    }

}
