use edgeql_parser::tokenizer::{Kind, TokenStream};
use edgeql_parser::tokenizer::Kind::*;
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

fn tok_err(s: &str) -> String {
    let mut s = TokenStream::new(s);
    loop {
        match s.uncons() {
            Ok(_) => {}
            Err(ref e) if e == &Error::end_of_input() => break,
            Err(e) => return format!("{}", e),
        }
    }
    panic!("No error, where error expected");
}

#[test]
fn whitespace_and_comments() {
    assert_eq!(tok_str("# hello { world }"), &[] as &[&str]);
    assert_eq!(tok_str("# x\n  "), &[] as &[&str]);
    assert_eq!(tok_str("  # x"), &[] as &[&str]);
}

#[test]
fn idents() {
    assert_eq!(tok_str("a bc d127"), ["a", "bc", "d127"]);
    assert_eq!(tok_typ("a bc d127"), [Ident, Ident, Ident]);
    assert_eq!(tok_str("тест тест_abc abc_тест"),
                       ["тест", "тест_abc", "abc_тест"]);
    assert_eq!(tok_typ("тест тест_abc abc_тест"), [Ident, Ident, Ident]);
    assert_eq!(tok_err(" + __test__"),
        "Unexpected `identifiers surrounded by double underscores \
        are forbidden`");
}

#[test]
fn keywords() {
    assert_eq!(tok_str("SELECT a"), ["SELECT", "a"]);
    assert_eq!(tok_typ("SELECT a"), [Keyword, Ident]);
    assert_eq!(tok_str("with Select"), ["with", "Select"]);
    assert_eq!(tok_typ("with Select"), [Keyword, Keyword]);
}

#[test]
fn colon_tokens() {
    assert_eq!(tok_str("a :=b"), ["a", ":=", "b"]);
    assert_eq!(tok_typ("a :=b"), [Ident, Assign, Ident]);
    assert_eq!(tok_str("a : = b"), ["a", ":", "=", "b"]);
    assert_eq!(tok_typ("a : = b"), [Ident, Colon, Eq, Ident]);
    assert_eq!(tok_str("a ::= b"), ["a", "::", "=", "b"]);
    assert_eq!(tok_typ("a ::= b"), [Ident, Namespace, Eq, Ident]);
}

#[test]
fn dash_tokens() {
    assert_eq!(tok_str("a-b -> c"), ["a", "-", "b", "->", "c"]);
    assert_eq!(tok_typ("a-b -> c"), [Ident, Sub, Ident, Arrow, Ident]);
    assert_eq!(tok_str("a - > b"), ["a", "-", ">", "b"]);
    assert_eq!(tok_typ("a - > b"), [Ident, Sub, Greater, Ident]);
    assert_eq!(tok_str("a --> b"), ["a", "-", "->", "b"]);
    assert_eq!(tok_typ("a --> b"), [Ident, Sub, Arrow, Ident]);
}

#[test]
fn greater_tokens() {
    assert_eq!(tok_str("a >= c"), ["a", ">=", "c"]);
    assert_eq!(tok_typ("a >= c"), [Ident, GreaterEq, Ident]);
    assert_eq!(tok_str("a > = b"), ["a", ">", "=", "b"]);
    assert_eq!(tok_typ("a > = b"), [Ident, Greater, Eq, Ident]);
    assert_eq!(tok_str("a>b"), ["a", ">", "b"]);
    assert_eq!(tok_typ("a>b"), [Ident, Greater, Ident]);
}

#[test]
fn less_tokens() {
    assert_eq!(tok_str("a <= c"), ["a", "<=", "c"]);
    assert_eq!(tok_typ("a <= c"), [Ident, LessEq, Ident]);
    assert_eq!(tok_str("a < = b"), ["a", "<", "=", "b"]);
    assert_eq!(tok_typ("a < = b"), [Ident, Less, Eq, Ident]);
    assert_eq!(tok_str("a<b"), ["a", "<", "b"]);
    assert_eq!(tok_typ("a<b"), [Ident, Less, Ident]);
}

#[test]
fn plus_tokens() {
    assert_eq!(tok_str("a+b += c"), ["a", "+", "b", "+=", "c"]);
    assert_eq!(tok_typ("a+b += c"), [Ident, Add, Ident, AddAssign, Ident]);
    assert_eq!(tok_str("a + = b"), ["a", "+", "=", "b"]);
    assert_eq!(tok_typ("a + = b"), [Ident, Add, Eq, Ident]);
    assert_eq!(tok_str("a ++= b"), ["a", "++", "=", "b"]);
    assert_eq!(tok_typ("a ++= b"), [Ident, Concat, Eq, Ident]);
    assert_eq!(tok_str("1+1"), ["1", "+", "1"]);
    assert_eq!(tok_typ("1+1"), [IntConst, Add, IntConst]);
}

#[test]
fn not_equals_tokens() {
    assert_eq!(tok_str("a != c"), ["a", "!=", "c"]);
    assert_eq!(tok_typ("a != c"), [Ident, NotEq, Ident]);
    assert_eq!(tok_str("a!=b"), ["a", "!=", "b"]);
    assert_eq!(tok_typ("a!=b"), [Ident, NotEq, Ident]);
    assert_eq!(tok_err("a ! = b"),
        "Unexpected `Bare `!` is not an operator, \
         did you mean `!=`?`");
}

#[test]
fn question_tokens() {
    assert_eq!(tok_str("a??b ?= c"), ["a", "??", "b", "?=", "c"]);
    assert_eq!(tok_typ("a??b ?= c"),
               [Ident, Coalesce, Ident, NotDistinctFrom, Ident]);
    assert_eq!(tok_str("a ?!= b"), ["a", "?!=", "b"]);
    assert_eq!(tok_typ("a ?!= b"), [Ident, DistinctFrom, Ident]);
    assert_eq!(tok_err("a ? b"),
        "Unexpected `Bare `?` is not an operator, \
         did you mean `?=` or `??` ?`");

    assert_eq!(tok_err("something ?!"),
        "Unexpected ``?!` is not an operator, \
         did you mean `?!=` ?`");
}

#[test]
fn dot_tokens() {
    assert_eq!(tok_str("a.b .> c"), ["a", ".", "b", ".>", "c"]);
    assert_eq!(tok_typ("a.b .> c"), [Ident, Dot, Ident, ForwardLink, Ident]);
    assert_eq!(tok_str("a . > b"), ["a", ".", ">", "b"]);
    assert_eq!(tok_typ("a . > b"), [Ident, Dot, Greater, Ident]);
    assert_eq!(tok_str("a .>> b"), ["a", ".>", ">", "b"]);
    assert_eq!(tok_typ("a .>> b"), [Ident, ForwardLink, Greater, Ident]);
    assert_eq!(tok_str("a ..> b"), ["a", ".", ".>", "b"]);
    assert_eq!(tok_typ("a ..> b"), [Ident, Dot, ForwardLink, Ident]);

    assert_eq!(tok_str("a.b .< c"), ["a", ".", "b", ".<", "c"]);
    assert_eq!(tok_typ("a.b .< c"), [Ident, Dot, Ident, BackwardLink, Ident]);
    assert_eq!(tok_str("a . < b"), ["a", ".", "<", "b"]);
    assert_eq!(tok_typ("a . < b"), [Ident, Dot, Less, Ident]);
    assert_eq!(tok_str("a .<< b"), ["a", ".<", "<", "b"]);
    assert_eq!(tok_typ("a .<< b"), [Ident, BackwardLink, Less, Ident]);
    assert_eq!(tok_str("a ..< b"), ["a", ".", ".<", "b"]);
    assert_eq!(tok_typ("a ..< b"), [Ident, Dot, BackwardLink, Ident]);
}

#[test]
fn tuple_dot_vs_float() {
    assert_eq!(tok_str("tuple.1.<"), ["tuple", ".", "1", ".<"]);
    assert_eq!(tok_typ("tuple.1.<"), [Ident, Dot, IntConst, BackwardLink]);
    assert_eq!(tok_str("1.<"), ["1.", "<"]);
    assert_eq!(tok_typ("1.<"), [FloatConst, Less]);
    assert_eq!(tok_str("1.e123"), ["1.e123"]);
    assert_eq!(tok_typ("1.e123"), [FloatConst]);
    assert_eq!(tok_str("tuple.1.e123"), ["tuple", ".", "1", ".", "e123"]);
    assert_eq!(tok_typ("tuple.1.e123"), [Ident, Dot, IntConst, Dot, Ident]);
}

#[test]
fn div_tokens() {
    assert_eq!(tok_str("a // c"), ["a", "//", "c"]);
    assert_eq!(tok_typ("a // c"), [Ident, FloorDiv, Ident]);
    assert_eq!(tok_str("a / / b"), ["a", "/", "/", "b"]);
    assert_eq!(tok_typ("a / / b"), [Ident, Div, Div, Ident]);
    assert_eq!(tok_str("a/b"), ["a", "/", "b"]);
    assert_eq!(tok_typ("a/b"), [Ident, Div, Ident]);
}

#[test]
fn single_char_tokens() {
    assert_eq!(tok_str(".;:+-*"), [".", ";", ":", "+", "-", "*"]);
    assert_eq!(tok_typ(".;:+-*"), [Dot, Semicolon, Colon, Add, Sub, Mul]);
    assert_eq!(tok_str("/%^<>"), ["/", "%", "^", "<", ">"]);
    assert_eq!(tok_typ("/%^<>"), [Div, Modulo, Pow, Less, Greater]);
    assert_eq!(tok_str("=&|@"), ["=", "&", "|", "@"]);
    assert_eq!(tok_typ("=&|@"), [Eq, Ampersand, Pipe, At]);

    assert_eq!(tok_str(". ; : + - *"), [".", ";", ":", "+", "-", "*"]);
    assert_eq!(tok_typ(". ; : + - *"), [Dot, Semicolon, Colon, Add, Sub, Mul]);
    assert_eq!(tok_str("/ % ^ < >"), ["/", "%", "^", "<", ">"]);
    assert_eq!(tok_typ("/ % ^ < >"), [Div, Modulo, Pow, Less, Greater]);
    assert_eq!(tok_str("= & | @"), ["=", "&", "|", "@"]);
    assert_eq!(tok_typ("= & | @"), [Eq, Ampersand, Pipe, At]);
}

#[test]
fn integer() {
    assert_eq!(tok_str("0"), ["0"]);
    assert_eq!(tok_typ("0"), [IntConst]);
    assert_eq!(tok_str("*0"), ["*", "0"]);
    assert_eq!(tok_typ("*0"), [Mul, IntConst]);
    assert_eq!(tok_str("123"), ["123"]);
    assert_eq!(tok_typ("123"), [IntConst]);

    assert_eq!(tok_str("0 "), ["0"]);
    assert_eq!(tok_typ("0 "), [IntConst]);
    assert_eq!(tok_str("123 "), ["123"]);
    assert_eq!(tok_typ("123 "), [IntConst]);

    assert_eq!(tok_err("01"),
        "Unexpected `leading zeros are not allowed in numbers`");
}

#[test]
fn bigint() {
    assert_eq!(tok_str("0n"), ["0n"]);
    assert_eq!(tok_typ("0n"), [BigIntConst]);
    assert_eq!(tok_str("*0n"), ["*", "0n"]);
    assert_eq!(tok_typ("*0n"), [Mul, BigIntConst]);
    assert_eq!(tok_str("123n"), ["123n"]);
    assert_eq!(tok_typ("123n"), [BigIntConst]);

    assert_eq!(tok_str("0n "), ["0n"]);
    assert_eq!(tok_typ("0n "), [BigIntConst]);
    assert_eq!(tok_str("123n "), ["123n"]);
    assert_eq!(tok_typ("123n "), [BigIntConst]);
    assert_eq!(tok_err("01n"),
        "Unexpected `leading zeros are not allowed in numbers`");
}

#[test]
fn float() {
    assert_eq!(tok_str("0."), ["0."]);
    assert_eq!(tok_typ("0."), [FloatConst]);
    assert_eq!(tok_str("     0.0"), ["0.0"]);
    assert_eq!(tok_typ("     0.0"), [FloatConst]);
    assert_eq!(tok_str("123.999"), ["123.999"]);
    assert_eq!(tok_typ("123.999"), [FloatConst]);
    assert_eq!(tok_str("123.999e3"), ["123.999e3"]);
    assert_eq!(tok_typ("123.999e3"), [FloatConst]);
    assert_eq!(tok_str("123.999e+99"), ["123.999e+99"]);
    assert_eq!(tok_typ("123.999e+99"), [FloatConst]);
    assert_eq!(tok_str("2345.567e-7"), ["2345.567e-7"]);
    assert_eq!(tok_typ("2345.567e-7"), [FloatConst]);
    assert_eq!(tok_str("123e3"), ["123e3"]);
    assert_eq!(tok_typ("123e3"), [FloatConst]);
    assert_eq!(tok_str("123e+99"), ["123e+99"]);
    assert_eq!(tok_typ("123e+99"), [FloatConst]);
    assert_eq!(tok_str("2345e-7"), ["2345e-7"]);
    assert_eq!(tok_typ("2345e-7"), [FloatConst]);

    assert_eq!(tok_str("0. "), ["0."]);
    assert_eq!(tok_typ("0. "), [FloatConst]);
    assert_eq!(tok_str("     0.0 "), ["0.0"]);
    assert_eq!(tok_typ("     0.0 "), [FloatConst]);
    assert_eq!(tok_str("123.999 "), ["123.999"]);
    assert_eq!(tok_typ("123.999 "), [FloatConst]);
    assert_eq!(tok_str("123.999e3 "), ["123.999e3"]);
    assert_eq!(tok_typ("123.999e3 "), [FloatConst]);
    assert_eq!(tok_str("123.999e+99 "), ["123.999e+99"]);
    assert_eq!(tok_typ("123.999e+99 "), [FloatConst]);
    assert_eq!(tok_str("2345.567e-7 "), ["2345.567e-7"]);
    assert_eq!(tok_typ("2345.567e-7 "), [FloatConst]);
    assert_eq!(tok_str("123e3 "), ["123e3"]);
    assert_eq!(tok_typ("123e3 "), [FloatConst]);
    assert_eq!(tok_str("123e+99 "), ["123e+99"]);
    assert_eq!(tok_typ("123e+99 "), [FloatConst]);
    assert_eq!(tok_str("2345e-7 "), ["2345e-7"]);
    assert_eq!(tok_typ("2345e-7 "), [FloatConst]);

    assert_eq!(tok_err("01.2"),
        "Unexpected `leading zeros are not allowed in numbers`");
}

#[test]
fn decimal() {
    assert_eq!(tok_str("0.n"), ["0.n"]);
    assert_eq!(tok_typ("0.n"), [DecimalConst]);
    assert_eq!(tok_str("     0.0n"), ["0.0n"]);
    assert_eq!(tok_typ("     0.0n"), [DecimalConst]);
    assert_eq!(tok_str("123.999n"), ["123.999n"]);
    assert_eq!(tok_typ("123.999n"), [DecimalConst]);
    assert_eq!(tok_str("123.999e3n"), ["123.999e3n"]);
    assert_eq!(tok_typ("123.999e3n"), [DecimalConst]);
    assert_eq!(tok_str("123.999e+99n"), ["123.999e+99n"]);
    assert_eq!(tok_typ("123.999e+99n"), [DecimalConst]);
    assert_eq!(tok_str("2345.567e-7n"), ["2345.567e-7n"]);
    assert_eq!(tok_typ("2345.567e-7n"), [DecimalConst]);
    assert_eq!(tok_str("123e3n"), ["123e3n"]);
    assert_eq!(tok_typ("123e3n"), [DecimalConst]);
    assert_eq!(tok_str("123e+99n"), ["123e+99n"]);
    assert_eq!(tok_typ("123e+99n"), [DecimalConst]);
    assert_eq!(tok_str("2345e-7n"), ["2345e-7n"]);
    assert_eq!(tok_typ("2345e-7n"), [DecimalConst]);

    assert_eq!(tok_str("0.n "), ["0.n"]);
    assert_eq!(tok_typ("0.n "), [DecimalConst]);
    assert_eq!(tok_str("     0.0n "), ["0.0n"]);
    assert_eq!(tok_typ("     0.0n "), [DecimalConst]);
    assert_eq!(tok_str("123.999n "), ["123.999n"]);
    assert_eq!(tok_typ("123.999n "), [DecimalConst]);
    assert_eq!(tok_str("123.999e3n "), ["123.999e3n"]);
    assert_eq!(tok_typ("123.999e3n "), [DecimalConst]);
    assert_eq!(tok_str("123.999e+99n "), ["123.999e+99n"]);
    assert_eq!(tok_typ("123.999e+99n "), [DecimalConst]);
    assert_eq!(tok_str("2345.567e-7n "), ["2345.567e-7n"]);
    assert_eq!(tok_typ("2345.567e-7n "), [DecimalConst]);
    assert_eq!(tok_str("123e3n "), ["123e3n"]);
    assert_eq!(tok_typ("123e3n "), [DecimalConst]);
    assert_eq!(tok_str("123e+99n "), ["123e+99n"]);
    assert_eq!(tok_typ("123e+99n "), [DecimalConst]);
    assert_eq!(tok_str("2345e-7n "), ["2345e-7n"]);
    assert_eq!(tok_typ("2345e-7n "), [DecimalConst]);

    assert_eq!(tok_err("01.0n"),
        "Unexpected `leading zeros are not allowed in numbers`");
}

#[test]
fn numbers_from_py() {
    assert_eq!(tok_str("SELECT 3.5432;"), ["SELECT", "3.5432", ";"]);
    assert_eq!(tok_typ("SELECT 3.5432;"), [Keyword, FloatConst, Semicolon]);
    assert_eq!(tok_str("SELECT +3.5432;"), ["SELECT", "+", "3.5432", ";"]);
    assert_eq!(tok_typ("SELECT +3.5432;"),
        [Keyword, Add, FloatConst, Semicolon]);
    assert_eq!(tok_str("SELECT -3.5432;"), ["SELECT", "-", "3.5432", ";"]);
    assert_eq!(tok_typ("SELECT -3.5432;"),
        [Keyword, Sub, FloatConst, Semicolon]);
    assert_eq!(tok_str("SELECT 354.32;"), ["SELECT", "354.32", ";"]);
    assert_eq!(tok_typ("SELECT 354.32;"), [Keyword, FloatConst, Semicolon]);
    assert_eq!(tok_str("SELECT 35400000000000.32;"),
        ["SELECT", "35400000000000.32", ";"]);
    assert_eq!(tok_typ("SELECT 35400000000000.32;"),
        [Keyword, FloatConst, Semicolon]);
    assert_eq!(tok_str("SELECT 35400000000000000000.32;"),
        ["SELECT", "35400000000000000000.32", ";"]);
    assert_eq!(tok_typ("SELECT 35400000000000000000.32;"),
        [Keyword, FloatConst, Semicolon]);
    assert_eq!(tok_str("SELECT 3.5432e20;"),
        ["SELECT", "3.5432e20", ";"]);
    assert_eq!(tok_typ("SELECT 3.5432e20;"),
        [Keyword, FloatConst, Semicolon]);
    assert_eq!(tok_str("SELECT 3.5432e+20;"),
        ["SELECT", "3.5432e+20", ";"]);
    assert_eq!(tok_typ("SELECT 3.5432e+20;"),
        [Keyword, FloatConst, Semicolon]);
    assert_eq!(tok_str("SELECT 3.5432e-20;"),
        ["SELECT", "3.5432e-20", ";"]);
    assert_eq!(tok_typ("SELECT 3.5432e-20;"),
        [Keyword, FloatConst, Semicolon]);
    assert_eq!(tok_str("SELECT 354.32e-20;"),
        ["SELECT", "354.32e-20", ";"]);
    assert_eq!(tok_typ("SELECT 354.32e-20;"),
        [Keyword, FloatConst, Semicolon]);
    assert_eq!(tok_str("SELECT -0n;"),
        ["SELECT", "-", "0n", ";"]);
    assert_eq!(tok_typ("SELECT -0n;"),
        [Keyword, Sub, BigIntConst, Semicolon]);
    assert_eq!(tok_str("SELECT 0n;"),
        ["SELECT", "0n", ";"]);
    assert_eq!(tok_typ("SELECT 0n;"),
        [Keyword, BigIntConst, Semicolon]);
    assert_eq!(tok_str("SELECT 1n;"),
        ["SELECT", "1n", ";"]);
    assert_eq!(tok_typ("SELECT 1n;"),
        [Keyword, BigIntConst, Semicolon]);
    assert_eq!(tok_str("SELECT -1n;"),
        ["SELECT", "-", "1n", ";"]);
    assert_eq!(tok_typ("SELECT -1n;"),
        [Keyword, Sub, BigIntConst, Semicolon]);
    assert_eq!(tok_str("SELECT 100000n;"),
        ["SELECT", "100000n", ";"]);
    assert_eq!(tok_typ("SELECT 100000n;"),
        [Keyword, BigIntConst, Semicolon]);
    assert_eq!(tok_str("SELECT -100000n;"),
        ["SELECT", "-", "100000n", ";"]);
    assert_eq!(tok_typ("SELECT -100000n;"),
        [Keyword, Sub, BigIntConst, Semicolon]);
    assert_eq!(tok_str("SELECT -354.32n;"),
        ["SELECT", "-", "354.32n", ";"]);
    assert_eq!(tok_typ("SELECT -354.32n;"),
        [Keyword, Sub, DecimalConst, Semicolon]);
    assert_eq!(tok_str("SELECT 35400000000000.32n;"),
        ["SELECT", "35400000000000.32n", ";"]);
    assert_eq!(tok_typ("SELECT 35400000000000.32n;"),
        [Keyword, DecimalConst, Semicolon]);
    assert_eq!(tok_str("SELECT -35400000000000000000.32n;"),
        ["SELECT", "-", "35400000000000000000.32n", ";"]);
    assert_eq!(tok_typ("SELECT -35400000000000000000.32n;"),
        [Keyword, Sub, DecimalConst, Semicolon]);
    assert_eq!(tok_str("SELECT 3.5432e20n;"),
        ["SELECT", "3.5432e20n", ";"]);
    assert_eq!(tok_typ("SELECT 3.5432e20n;"),
        [Keyword, DecimalConst, Semicolon]);
    assert_eq!(tok_str("SELECT -3.5432e+20n;"),
        ["SELECT", "-", "3.5432e+20n", ";"]);
    assert_eq!(tok_typ("SELECT -3.5432e+20n;"),
        [Keyword, Sub, DecimalConst, Semicolon]);
    assert_eq!(tok_str("SELECT 3.5432e-20n;"),
        ["SELECT", "3.5432e-20n", ";"]);
    assert_eq!(tok_typ("SELECT 3.5432e-20n;"),
        [Keyword, DecimalConst, Semicolon]);
    assert_eq!(tok_str("SELECT 354.32e-20n;"),
        ["SELECT", "354.32e-20n", ";"]);
    assert_eq!(tok_typ("SELECT 354.32e-20n;"),
        [Keyword, DecimalConst, Semicolon]);
}

#[test]
fn num_errors() {
    assert_eq!(tok_err("1.0.x"),
        "Unexpected `extra decimal dot in number`");
    assert_eq!(tok_err("1.0e1."),
        "Unexpected `extra decimal dot in number`");
    assert_eq!(tok_err("1.0e."),
        "Unexpected `optional `+` or `-` \
        followed by digits must follow `e` in float const`");
    assert_eq!(tok_err("1.0e"),
        "Unexpected `optional `+` or `-` \
        followed by digits must follow `e` in float const`");
    assert_eq!(tok_err("1.0ex"),
        "Unexpected `optional `+` or `-` \
        followed by digits must follow `e` in float const`");
    assert_eq!(tok_err("1.0en"),
        "Unexpected `optional `+` or `-` \
        followed by digits must follow `e` in float const`");
    assert_eq!(tok_err("1.0e "),
        "Unexpected `optional `+` or `-` \
        followed by digits must follow `e` in float const`");
    assert_eq!(tok_err("1.0e+"),
        "Unexpected `optional `+` or `-` \
        followed by digits must follow `e` in float const`");
    assert_eq!(tok_err("1.0e+ "),
        "Unexpected `optional `+` or `-` \
        followed by digits must follow `e` in float const`");
    assert_eq!(tok_err("1.0e+x"),
        "Unexpected `optional `+` or `-` \
        followed by digits must follow `e` in float const`");
    assert_eq!(tok_err("1.0e+n"),
        "Unexpected `optional `+` or `-` \
        followed by digits must follow `e` in float const`");
    assert_eq!(tok_err("1234numeric"),
        "Unexpected `suffix \"numeric\" \
        is invalid for numbers, perhaps you wanted `1234n` (bigint)?`");
    assert_eq!(tok_err("1234some_l0ng_trash"),
        "Unexpected `suffix \"some_l0n...\" \
        is invalid for numbers, perhaps you wanted `1234n` (bigint)?`");
    assert_eq!(tok_err("100O00"),
        "Unexpected `suffix \"O00\" is invalid for numbers, \
        perhaps mixed up letter `O` with zero `0`?`");
}

#[test]
fn tuple_paths() {
    assert_eq!(tok_str("tup.1.2.3.4.5"),
        ["tup", ".", "1", ".", "2", ".", "3", ".", "4", ".", "5"]);
    assert_eq!(tok_typ("tup.1.2.3.4.5"),
        [Ident, Dot, IntConst, Dot, IntConst,
                Dot, IntConst, Dot, IntConst, Dot, IntConst]);
    assert_eq!(tok_str("tup.1.2.>3.4.>5"),
        ["tup", ".", "1", ".", "2", ".>", "3", ".", "4", ".>", "5"]);
    assert_eq!(tok_typ("tup.1.2.>3.4.>5"),
        [Ident, Dot, IntConst, Dot, IntConst,
                ForwardLink, IntConst, Dot, IntConst, ForwardLink, IntConst]);
    assert_eq!(tok_str("$0.1.2.3.4.5"),
        ["$0", ".", "1", ".", "2", ".", "3", ".", "4", ".", "5"]);
    assert_eq!(tok_typ("$0.1.2.3.4.5"),
        [Argument, Dot, IntConst, Dot, IntConst,
                Dot, IntConst, Dot, IntConst, Dot, IntConst]);
    assert_eq!(tok_err("tup.1n"),
        "Unexpected `unexpected char \'n\', only integers \
        are allowed after dot (for tuple access)`");

    assert_eq!(tok_err("tup.01"),
        "Unexpected `leading zeros are not allowed in numbers`");
}

#[test]
fn strings() {
    assert_eq!(tok_str(r#" ""  "#), [r#""""#]);
    assert_eq!(tok_typ(r#" ""  "#), [Str]);
    assert_eq!(tok_str(r#" ''  "#), [r#"''"#]);
    assert_eq!(tok_typ(r#" ''  "#), [Str]);
    assert_eq!(tok_str(r#" r""  "#), [r#"r"""#]);
    assert_eq!(tok_typ(r#" r""  "#), [Str]);
    assert_eq!(tok_str(r#" r''  "#), [r#"r''"#]);
    assert_eq!(tok_typ(r#" r''  "#), [Str]);
    assert_eq!(tok_str(r#" b""  "#), [r#"b"""#]);
    assert_eq!(tok_typ(r#" b""  "#), [BinStr]);
    assert_eq!(tok_str(r#" b''  "#), [r#"b''"#]);
    assert_eq!(tok_typ(r#" b''  "#), [BinStr]);
    assert_eq!(tok_err(r#" ``  "#),
        "Unexpected `backtick quotes cannot be empty`");

    assert_eq!(tok_str(r#" "hello"  "#), [r#""hello""#]);
    assert_eq!(tok_typ(r#" "hello"  "#), [Str]);
    assert_eq!(tok_str(r#" 'hello'  "#), [r#"'hello'"#]);
    assert_eq!(tok_typ(r#" 'hello'  "#), [Str]);
    assert_eq!(tok_str(r#" r"hello"  "#), [r#"r"hello""#]);
    assert_eq!(tok_typ(r#" r"hello"  "#), [Str]);
    assert_eq!(tok_str(r#" r'hello'  "#), [r#"r'hello'"#]);
    assert_eq!(tok_typ(r#" r'hello'  "#), [Str]);
    assert_eq!(tok_str(r#" b"hello"  "#), [r#"b"hello""#]);
    assert_eq!(tok_typ(r#" b"hello"  "#), [BinStr]);
    assert_eq!(tok_str(r#" b'hello'  "#), [r#"b'hello'"#]);
    assert_eq!(tok_typ(r#" b'hello'  "#), [BinStr]);
    assert_eq!(tok_str(r#" `hello`  "#), [r#"`hello`"#]);
    assert_eq!(tok_typ(r#" `hello`  "#), [BacktickName]);

    assert_eq!(tok_str(r#" "hello""#), [r#""hello""#]);
    assert_eq!(tok_typ(r#" "hello""#), [Str]);
    assert_eq!(tok_str(r#" 'hello'"#), [r#"'hello'"#]);
    assert_eq!(tok_typ(r#" 'hello'"#), [Str]);
    assert_eq!(tok_str(r#" r"hello""#), [r#"r"hello""#]);
    assert_eq!(tok_typ(r#" r"hello""#), [Str]);
    assert_eq!(tok_str(r#" r'hello'"#), [r#"r'hello'"#]);
    assert_eq!(tok_typ(r#" r'hello'"#), [Str]);
    assert_eq!(tok_str(r#" b"hello""#), [r#"b"hello""#]);
    assert_eq!(tok_typ(r#" b"hello""#), [BinStr]);
    assert_eq!(tok_str(r#" b'hello'"#), [r#"b'hello'"#]);
    assert_eq!(tok_typ(r#" b'hello'"#), [BinStr]);
    assert_eq!(tok_str(r#" `hello`"#), [r#"`hello`"#]);
    assert_eq!(tok_typ(r#" `hello`"#), [BacktickName]);

    assert_eq!(tok_str(r#" "h\"ello" "#), [r#""h\"ello""#]);
    assert_eq!(tok_typ(r#" "h\"ello" "#), [Str]);
    assert_eq!(tok_str(r#" 'h\'ello' "#), [r#"'h\'ello'"#]);
    assert_eq!(tok_typ(r#" 'h\'ello' "#), [Str]);
    assert_eq!(tok_str(r#" r"hello\" "#), [r#"r"hello\""#]);
    assert_eq!(tok_typ(r#" r"hello\" "#), [Str]);
    assert_eq!(tok_str(r#" r'hello\' "#), [r#"r'hello\'"#]);
    assert_eq!(tok_typ(r#" r'hello\' "#), [Str]);
    assert_eq!(tok_str(r#" b"h\"ello" "#), [r#"b"h\"ello""#]);
    assert_eq!(tok_typ(r#" b"h\"ello" "#), [BinStr]);
    assert_eq!(tok_str(r#" b'h\'ello' "#), [r#"b'h\'ello'"#]);
    assert_eq!(tok_typ(r#" b'h\'ello' "#), [BinStr]);
    assert_eq!(tok_str(r#" `hello\` "#), [r#"`hello\`"#]);
    assert_eq!(tok_typ(r#" `hello\` "#), [BacktickName]);
    assert_eq!(tok_str(r#" `hel``lo` "#), [r#"`hel``lo`"#]);
    assert_eq!(tok_typ(r#" `hel``lo` "#), [BacktickName]);

    assert_eq!(tok_str(r#" "h'el`lo" "#), [r#""h'el`lo""#]);
    assert_eq!(tok_typ(r#" "h'el`lo" "#), [Str]);
    assert_eq!(tok_str(r#" 'h"el`lo' "#), [r#"'h"el`lo'"#]);
    assert_eq!(tok_typ(r#" 'h"el`lo' "#), [Str]);
    assert_eq!(tok_str(r#" r"h'el`lo" "#), [r#"r"h'el`lo""#]);
    assert_eq!(tok_typ(r#" r"h'el`lo" "#), [Str]);
    assert_eq!(tok_str(r#" r'h"el`lo' "#), [r#"r'h"el`lo'"#]);
    assert_eq!(tok_typ(r#" r'h"el`lo' "#), [Str]);
    assert_eq!(tok_str(r#" b"h'el`lo" "#), [r#"b"h'el`lo""#]);
    assert_eq!(tok_typ(r#" b"h'el`lo" "#), [BinStr]);
    assert_eq!(tok_str(r#" b'h"el`lo' "#), [r#"b'h"el`lo'"#]);
    assert_eq!(tok_typ(r#" b'h"el`lo' "#), [BinStr]);
    assert_eq!(tok_str(r#" `h'el"lo` "#), [r#"`h'el"lo`"#]);
    assert_eq!(tok_typ(r#" `h'el"lo\` "#), [BacktickName]);

    assert_eq!(tok_str(" \"hel\nlo\" "), ["\"hel\nlo\""]);
    assert_eq!(tok_typ(" \"hel\nlo\" "), [Str]);
    assert_eq!(tok_str(" 'hel\nlo' "), ["'hel\nlo'"]);
    assert_eq!(tok_typ(" 'hel\nlo' "), [Str]);
    assert_eq!(tok_str(" r\"hel\nlo\" "), ["r\"hel\nlo\""]);
    assert_eq!(tok_typ(" r\"hel\nlo\" "), [Str]);
    assert_eq!(tok_str(" r'hel\nlo' "), ["r'hel\nlo'"]);
    assert_eq!(tok_typ(" r'hel\nlo' "), [Str]);
    assert_eq!(tok_str(" b\"hel\nlo\" "), ["b\"hel\nlo\""]);
    assert_eq!(tok_typ(" b\"hel\nlo\" "), [BinStr]);
    assert_eq!(tok_str(" b'hel\nlo' "), ["b'hel\nlo'"]);
    assert_eq!(tok_typ(" b'hel\nlo' "), [BinStr]);
    assert_eq!(tok_str(" `hel\nlo` "), ["`hel\nlo`"]);
    assert_eq!(tok_typ(" `hel\nlo` "), [BacktickName]);

    assert_eq!(tok_err(r#""hello"#),
        "Unexpected `unterminated string, quoted by `\"``");
    assert_eq!(tok_err(r#"'hello"#),
        "Unexpected `unterminated string, quoted by `'``");
    assert_eq!(tok_err(r#"r"hello"#),
        "Unexpected `unterminated string, quoted by `\"``");
    assert_eq!(tok_err(r#"r'hello"#),
        "Unexpected `unterminated string, quoted by `'``");
    assert_eq!(tok_err(r#"b"hello"#),
        "Unexpected `unterminated string, quoted by `\"``");
    assert_eq!(tok_err(r#"b'hello"#),
        "Unexpected `unterminated string, quoted by `'``");
    assert_eq!(tok_err(r#"`hello"#),
        "Unexpected `unterminated backtick name`");

    assert_eq!(tok_err(r#"name`type`"#),
        "Unexpected `prefix \"name\" is not allowed for field names, \
        perhaps missing comma or dot?`");
    assert_eq!(tok_err(r#"User`type`"#),
        "Unexpected `prefix \"User\" is not allowed for field names, \
        perhaps missing comma or dot?`");
    assert_eq!(tok_err(r#"r`hello"#),
        "Unexpected `prefix \"r\" is not allowed for field names, \
        perhaps missing comma or dot?`");
    assert_eq!(tok_err(r#"b`hello"#),
        "Unexpected `prefix \"b\" is not allowed for field names, \
        perhaps missing comma or dot?`");
    assert_eq!(tok_err(r#"test"hello""#),
        "Unexpected `prefix \"test\" is not allowed for strings, \
        allowed: `b`, `r``");
    assert_eq!(tok_err(r#"test'hello'"#),
        "Unexpected `prefix \"test\" is not allowed for strings, \
        allowed: `b`, `r``");
    assert_eq!(tok_err(r#"`@x`"#),
        "Unexpected `backtick-quoted name cannot start with char `@``");
    assert_eq!(tok_err(r#"`a::b`"#),
        "Unexpected `backtick-quoted name cannot contain `::``");
    assert_eq!(tok_err(r#"`__x__`"#),
        "Unexpected `backtick-quoted names surrounded by double \
                    underscores are forbidden`");
}

#[test]
fn test_dollar() {
    assert_eq!(tok_str("select $$ something $$; x"),
                       ["select", "$$ something $$", ";", "x"]);
    assert_eq!(tok_typ("select $$ something $$; x"),
                       [Keyword, Str, Semicolon, Ident]);
    assert_eq!(tok_str("select $a$ ; $b$ ; $b$ ; $a$; x"),
                       ["select", "$a$ ; $b$ ; $b$ ; $a$", ";", "x"]);
    assert_eq!(tok_typ("select $a$ ; $b$ ; $b$ ; $a$; x"),
                       [Keyword, Str, Semicolon, Ident]);
    assert_eq!(tok_str("select $a$ ; $b$ ; $a$; x"),
                       ["select", "$a$ ; $b$ ; $a$", ";", "x"]);
    assert_eq!(tok_typ("select $a$ ; $b$ ; $a$; x"),
                       [Keyword, Str, Semicolon, Ident]);
    assert_eq!(tok_err("select $$ ; $ab$ test;"),
        "Unexpected `unterminated string started with $$`");
    assert_eq!(tok_err("select $a$ ; $$ test;"),
        "Unexpected `unterminated string started with \"$a$\"`");
    assert_eq!(tok_err("select $0$"),
        "Unexpected `dollar quote must not start with a digit`");
    assert_eq!(tok_err("select $фыва$"),
        "Unexpected `dollar quote supports only ascii chars`");
    assert_eq!(tok_str("select $a$a$ ; $a$ test;"),
        ["select", "$a$a$ ; $a$", "test", ";"]);
    assert_eq!(tok_typ("select $a$a$ ; $a$ test;"),
        [Keyword, Str, Ident, Semicolon]);
    assert_eq!(tok_str("select $a+b; $b test; $a+b; $b ;"),
        ["select", "$a", "+", "b", ";", "$b", "test",
         ";", "$a", "+", "b", ";", "$b", ";"]);
    assert_eq!(tok_typ("select $a+b; $b test; $a+b; $b ;"),
        [Keyword, Argument, Add, Ident, Semicolon, Argument, Ident,
         Semicolon, Argument, Add, Ident, Semicolon, Argument, Semicolon]);
    assert_eq!(tok_str("select $def x$y test; $def x$y"),
        ["select", "$def", "x", "$y", "test",
         ";", "$def", "x", "$y"]);
    assert_eq!(tok_typ("select $def x$y test; $def x$y"),
        [Keyword, Argument, Ident, Argument, Ident,
         Semicolon, Argument, Ident, Argument]);
    assert_eq!(tok_str("select $`x``y` + $0 + $`zz` + $1.2 + $фыва"),
        ["select", "$`x``y`", "+", "$0", "+", "$`zz`", "+", "$1", ".", "2",
         "+", "$фыва"]);
    assert_eq!(tok_typ("select $`x``y` + $0 + $`zz` + $1.2 + $фыва"),
        [Keyword, Argument, Add, Argument, Add, Argument,
         Add, Argument, Dot, IntConst, Add, Argument]);
    assert_eq!(tok_err(r#"$-"#),
        "Unexpected `bare $ is not allowed`");
    assert_eq!(tok_err(r#"$0abc"#),
        "Unexpected `the \"$0abc\" is not a valid argument, \
         either name starting with letter or only digits are expected`");
    assert_eq!(tok_err(r#"-$"#),
        "Unexpected `bare $ is not allowed`");
    assert_eq!(tok_err(r#" $``  "#),
        "Unexpected `backtick-quoted argument cannot be empty`");
    assert_eq!(tok_err(r#"$`@x`"#),
        "Unexpected `backtick-quoted argument cannot \
        start with char `@``");
    assert_eq!(tok_err(r#"$`a::b`"#),
        "Unexpected `backtick-quoted argument cannot contain `::``");
    assert_eq!(tok_err(r#"$`__x__`"#),
        "Unexpected `backtick-quoted arguments surrounded by double \
                    underscores are forbidden`");
}

#[test]
fn invalid_suffix() {
    assert_eq!(tok_err("SELECT 1d;"), "Unexpected `suffix \"d\" \
        is invalid for numbers, perhaps you wanted `1n` (bigint)?`");
}
