use edgeql_parser::tokenizer::Kind::*;
use edgeql_parser::tokenizer::{Kind, Tokenizer};

fn tok_str(s: &str) -> Vec<String> {
    let mut r = Vec::new();
    let mut s = Tokenizer::new(s).validated_values();
    loop {
        match s.next() {
            Some(Ok(x)) => r.push(x.text.to_string()),
            None => break,
            Some(Err(e)) => panic!("Parse error at {}: {}", e.span.start, e.message),
        }
    }
    r
}

fn tok_typ(s: &str) -> Vec<Kind> {
    let mut r = Vec::new();
    let mut s = Tokenizer::new(s).validated_values();
    loop {
        match s.next() {
            Some(Ok(x)) => r.push(x.kind),
            None => break,
            Some(Err(e)) => panic!("Parse error at {}: {}", e.span.start, e.message),
        }
    }
    r
}

fn tok_err(s: &str) -> String {
    let mut s = Tokenizer::new(s).validated_values();
    loop {
        match s.next() {
            Some(Ok(_)) => {}
            None => break,
            Some(Err(e)) => return e.message.to_string(),
        }
    }
    panic!("No error, where error expected");
}

fn keyword(kw: &'static str) -> Kind {
    Keyword(edgeql_parser::keywords::Keyword(kw))
}

#[test]
fn whitespace_and_comments() {
    assert_eq!(tok_str("# hello { world }"), &[] as &[&str]);
    assert_eq!(tok_str("# x\n  "), &[] as &[&str]);
    assert_eq!(tok_str("  # x"), &[] as &[&str]);
    assert_eq!(
        tok_err("  # xxx \u{202A} yyy"),
        "unexpected character '\\u{202a}'"
    );
}

#[test]
fn idents() {
    assert_eq!(tok_str("a bc d127"), ["a", "bc", "d127"]);
    assert_eq!(tok_typ("a bc d127"), [Ident, Ident, Ident]);
    assert_eq!(
        tok_str("тест тест_abc abc_тест"),
        ["тест", "тест_abc", "abc_тест"]
    );
    assert_eq!(tok_typ("тест тест_abc abc_тест"), [Ident, Ident, Ident]);
    assert_eq!(
        tok_err(" + __test__"),
        "identifiers surrounded by double underscores are forbidden"
    );
    assert_eq!(tok_str("_1024"), ["_1024"]);
    assert_eq!(tok_typ("_1024"), [Ident]);
}

#[test]
fn keywords() {
    assert_eq!(tok_str("SELECT a"), ["SELECT", "a"]);
    assert_eq!(tok_typ("SELECT a"), [keyword("select"), Ident]);
    assert_eq!(tok_str("with Select"), ["with", "Select"]);
    assert_eq!(tok_typ("with Select"), [keyword("with"), keyword("select")]);
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
    assert_eq!(
        tok_err("a ! = b"),
        "Bare `!` is not an operator, \
         did you mean `!=`?"
    );
}

#[test]
fn question_tokens() {
    assert_eq!(tok_str("a??b ?= c"), ["a", "??", "b", "?=", "c"]);
    assert_eq!(
        tok_typ("a??b ?= c"),
        [Ident, Coalesce, Ident, NotDistinctFrom, Ident]
    );
    assert_eq!(tok_str("a ?!= b"), ["a", "?!=", "b"]);
    assert_eq!(tok_typ("a ?!= b"), [Ident, DistinctFrom, Ident]);
    assert_eq!(
        tok_err("a ? b"),
        "Bare `?` is not an operator, \
         did you mean `?=` or `??` ?"
    );

    assert_eq!(
        tok_err("something ?!"),
        "`?!` is not an operator, \
         did you mean `?!=` ?"
    );
}

#[test]
fn dot_tokens() {
    assert_eq!(tok_str("a.b .> c"), ["a", ".", "b", ".", ">", "c"]);
    assert_eq!(
        tok_typ("a.b .> c"),
        [Ident, Dot, Ident, Dot, Greater, Ident]
    );
    assert_eq!(tok_str("a . > b"), ["a", ".", ">", "b"]);
    assert_eq!(tok_typ("a . > b"), [Ident, Dot, Greater, Ident]);
    assert_eq!(tok_str("a .>> b"), ["a", ".", ">", ">", "b"]);
    assert_eq!(tok_typ("a .>> b"), [Ident, Dot, Greater, Greater, Ident]);
    assert_eq!(tok_str("a ..> b"), ["a", ".", ".", ">", "b"]);
    assert_eq!(tok_typ("a ..> b"), [Ident, Dot, Dot, Greater, Ident]);

    assert_eq!(tok_str("a.b .< c"), ["a", ".", "b", ".<", "c"]);
    assert_eq!(
        tok_typ("a.b .< c"),
        [Ident, Dot, Ident, BackwardLink, Ident]
    );
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
    assert_eq!(
        tok_typ(". ; : + - *"),
        [Dot, Semicolon, Colon, Add, Sub, Mul]
    );
    assert_eq!(tok_str("/ % ^ < >"), ["/", "%", "^", "<", ">"]);
    assert_eq!(tok_typ("/ % ^ < >"), [Div, Modulo, Pow, Less, Greater]);
    assert_eq!(tok_str("= & | @"), ["=", "&", "|", "@"]);
    assert_eq!(tok_typ("= & | @"), [Eq, Ampersand, Pipe, At]);
}

#[test]
fn splats() {
    assert_eq!(tok_str("*"), ["*"]);
    assert_eq!(tok_typ("*"), [Mul]);
    assert_eq!(tok_str("**"), ["**"]);
    assert_eq!(tok_typ("**"), [DoubleSplat]);
    assert_eq!(tok_str("* *"), ["*", "*"]);
    assert_eq!(tok_typ("* *"), [Mul, Mul]);
    assert_eq!(tok_str("User.*,"), ["User", ".", "*", ","]);
    assert_eq!(tok_typ("User.*,"), [Ident, Dot, Mul, Comma]);
    assert_eq!(tok_str("User.**,"), ["User", ".", "**", ","]);
    assert_eq!(tok_typ("User.**,"), [Ident, Dot, DoubleSplat, Comma]);
    assert_eq!(tok_str("User {*}"), ["User", "{", "*", "}"]);
    assert_eq!(tok_typ("User {*}"), [Ident, OpenBrace, Mul, CloseBrace]);
    assert_eq!(tok_str("User {**}"), ["User", "{", "**", "}"]);
    assert_eq!(
        tok_typ("User {**}"),
        [Ident, OpenBrace, DoubleSplat, CloseBrace]
    );
}

#[test]
fn integer() {
    assert_eq!(tok_str("0"), ["0"]);
    assert_eq!(tok_typ("0"), [IntConst]);
    assert_eq!(tok_str("*0"), ["*", "0"]);
    assert_eq!(tok_typ("*0"), [Mul, IntConst]);
    assert_eq!(tok_str("123"), ["123"]);
    assert_eq!(tok_typ("123"), [IntConst]);
    assert_eq!(tok_str("123_"), ["123_"]);
    assert_eq!(tok_typ("123_"), [IntConst]);
    assert_eq!(tok_str("123_456"), ["123_456"]);
    assert_eq!(tok_typ("123_456"), [IntConst]);

    assert_eq!(tok_str("0 "), ["0"]);
    assert_eq!(tok_typ("0 "), [IntConst]);
    assert_eq!(tok_str("123 "), ["123"]);
    assert_eq!(tok_typ("123 "), [IntConst]);
    assert_eq!(tok_str("123_ "), ["123_"]);
    assert_eq!(tok_typ("123_ "), [IntConst]);
    assert_eq!(tok_str("123_456 "), ["123_456"]);
    assert_eq!(tok_typ("123_456 "), [IntConst]);
}

#[test]
fn bigint() {
    assert_eq!(tok_str("0n"), ["0n"]);
    assert_eq!(tok_typ("0n"), [BigIntConst]);
    assert_eq!(tok_str("*0n"), ["*", "0n"]);
    assert_eq!(tok_typ("*0n"), [Mul, BigIntConst]);
    assert_eq!(tok_str("123n"), ["123n"]);
    assert_eq!(tok_typ("123n"), [BigIntConst]);
    assert_eq!(tok_str("123e3n"), ["123e3n"]);
    assert_eq!(tok_typ("123e3n"), [BigIntConst]);
    assert_eq!(tok_str("123e+99n"), ["123e+99n"]);
    assert_eq!(tok_typ("123e+99n"), [BigIntConst]);
    assert_eq!(tok_str("123_n"), ["123_n"]);
    assert_eq!(tok_typ("123_n"), [BigIntConst]);
    assert_eq!(tok_str("123_456n"), ["123_456n"]);
    assert_eq!(tok_typ("123_456n"), [BigIntConst]);

    assert_eq!(tok_str("0n "), ["0n"]);
    assert_eq!(tok_typ("0n "), [BigIntConst]);
    assert_eq!(tok_str("123n "), ["123n"]);
    assert_eq!(tok_typ("123n "), [BigIntConst]);
    assert_eq!(tok_str("123e3n "), ["123e3n"]);
    assert_eq!(tok_typ("123e3n "), [BigIntConst]);
    assert_eq!(tok_str("123e+99n "), ["123e+99n"]);
    assert_eq!(tok_typ("123e+99n "), [BigIntConst]);
    assert_eq!(tok_str("123_n "), ["123_n"]);
    assert_eq!(tok_typ("123_n "), [BigIntConst]);
    assert_eq!(tok_str("123_456n "), ["123_456n"]);
    assert_eq!(tok_typ("123_456n "), [BigIntConst]);
}

#[test]
fn float() {
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
    assert_eq!(tok_str("123e+99_"), ["123e+99_"]);
    assert_eq!(tok_typ("123e+99_"), [FloatConst]);
    assert_eq!(tok_str("123e+9_9"), ["123e+9_9"]);
    assert_eq!(tok_typ("123e+9_9"), [FloatConst]);
    assert_eq!(tok_str("2345e-7"), ["2345e-7"]);
    assert_eq!(tok_typ("2345e-7"), [FloatConst]);
    assert_eq!(tok_str("2_345e-7"), ["2_345e-7"]);
    assert_eq!(tok_typ("2_345e-7"), [FloatConst]);
    assert_eq!(tok_str("1_023.9_099"), ["1_023.9_099"]);
    assert_eq!(tok_typ("1_023.9_099"), [FloatConst]);
    assert_eq!(tok_str("1_023_.9_099_"), ["1_023_.9_099_"]);
    assert_eq!(tok_typ("1_023_.9_099_"), [FloatConst]);

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
    assert_eq!(tok_str("123e+99_ "), ["123e+99_"]);
    assert_eq!(tok_typ("123e+99_ "), [FloatConst]);
    assert_eq!(tok_str("2345e-7 "), ["2345e-7"]);
    assert_eq!(tok_typ("2345e-7 "), [FloatConst]);
    assert_eq!(tok_str("1_023_.9_099_ "), ["1_023_.9_099_"]);
    assert_eq!(tok_typ("1_023_.9_099_ "), [FloatConst]);

    assert_eq!(
        tok_err("01.2"),
        "unexpected leading zeros are not allowed in numbers"
    );
}

#[test]
fn decimal() {
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
    assert_eq!(tok_str("2345e-7n"), ["2345e-7n"]);
    assert_eq!(tok_typ("2345e-7n"), [DecimalConst]);
    assert_eq!(tok_str("2_345e-7n"), ["2_345e-7n"]);
    assert_eq!(tok_typ("2_345e-7n"), [DecimalConst]);
    assert_eq!(tok_str("1_023.9_099n"), ["1_023.9_099n"]);
    assert_eq!(tok_typ("1_023.9_099n"), [DecimalConst]);
    assert_eq!(tok_str("1_023_.9_099_n"), ["1_023_.9_099_n"]);
    assert_eq!(tok_typ("1_023_.9_099_n"), [DecimalConst]);
    assert_eq!(tok_str("2_345e-7n"), ["2_345e-7n"]);
    assert_eq!(tok_typ("2_345e-7n"), [DecimalConst]);
    assert_eq!(tok_str("2_345e-7_7n"), ["2_345e-7_7n"]);
    assert_eq!(tok_typ("2_345e-7_7n"), [DecimalConst]);

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
    assert_eq!(tok_str("2345e-7n "), ["2345e-7n"]);
    assert_eq!(tok_typ("2345e-7n "), [DecimalConst]);

    assert_eq!(
        tok_err("01.0n"),
        "unexpected leading zeros are not allowed in numbers"
    );
}

#[test]
fn numbers_from_py() {
    assert_eq!(tok_str("SELECT 3.5432;"), ["SELECT", "3.5432", ";"]);
    assert_eq!(
        tok_typ("SELECT 3.5432;"),
        [keyword("select"), FloatConst, Semicolon]
    );
    assert_eq!(tok_str("SELECT +3.5432;"), ["SELECT", "+", "3.5432", ";"]);
    assert_eq!(
        tok_typ("SELECT +3.5432;"),
        [keyword("select"), Add, FloatConst, Semicolon]
    );
    assert_eq!(tok_str("SELECT -3.5432;"), ["SELECT", "-", "3.5432", ";"]);
    assert_eq!(
        tok_typ("SELECT -3.5432;"),
        [keyword("select"), Sub, FloatConst, Semicolon]
    );
    assert_eq!(tok_str("SELECT 354.32;"), ["SELECT", "354.32", ";"]);
    assert_eq!(
        tok_typ("SELECT 354.32;"),
        [keyword("select"), FloatConst, Semicolon]
    );
    assert_eq!(
        tok_str("SELECT 35400000000000.32;"),
        ["SELECT", "35400000000000.32", ";"]
    );
    assert_eq!(
        tok_typ("SELECT 35400000000000.32;"),
        [keyword("select"), FloatConst, Semicolon]
    );
    assert_eq!(
        tok_str("SELECT 35400000000000000000.32;"),
        ["SELECT", "35400000000000000000.32", ";"]
    );
    assert_eq!(
        tok_typ("SELECT 35400000000000000000.32;"),
        [keyword("select"), FloatConst, Semicolon]
    );
    assert_eq!(tok_str("SELECT 3.5432e20;"), ["SELECT", "3.5432e20", ";"]);
    assert_eq!(
        tok_typ("SELECT 3.5432e20;"),
        [keyword("select"), FloatConst, Semicolon]
    );
    assert_eq!(tok_str("SELECT 3.5432e+20;"), ["SELECT", "3.5432e+20", ";"]);
    assert_eq!(
        tok_typ("SELECT 3.5432e+20;"),
        [keyword("select"), FloatConst, Semicolon]
    );
    assert_eq!(tok_str("SELECT 3.5432e-20;"), ["SELECT", "3.5432e-20", ";"]);
    assert_eq!(
        tok_typ("SELECT 3.5432e-20;"),
        [keyword("select"), FloatConst, Semicolon]
    );
    assert_eq!(tok_str("SELECT 354.32e-20;"), ["SELECT", "354.32e-20", ";"]);
    assert_eq!(
        tok_typ("SELECT 354.32e-20;"),
        [keyword("select"), FloatConst, Semicolon]
    );
    assert_eq!(tok_str("SELECT -0n;"), ["SELECT", "-", "0n", ";"]);
    assert_eq!(
        tok_typ("SELECT -0n;"),
        [keyword("select"), Sub, BigIntConst, Semicolon]
    );
    assert_eq!(tok_str("SELECT 0n;"), ["SELECT", "0n", ";"]);
    assert_eq!(
        tok_typ("SELECT 0n;"),
        [keyword("select"), BigIntConst, Semicolon]
    );
    assert_eq!(tok_str("SELECT 1n;"), ["SELECT", "1n", ";"]);
    assert_eq!(
        tok_typ("SELECT 1n;"),
        [keyword("select"), BigIntConst, Semicolon]
    );
    assert_eq!(tok_str("SELECT -1n;"), ["SELECT", "-", "1n", ";"]);
    assert_eq!(
        tok_typ("SELECT -1n;"),
        [keyword("select"), Sub, BigIntConst, Semicolon]
    );
    assert_eq!(tok_str("SELECT 100000n;"), ["SELECT", "100000n", ";"]);
    assert_eq!(
        tok_typ("SELECT 100000n;"),
        [keyword("select"), BigIntConst, Semicolon]
    );
    assert_eq!(tok_str("SELECT -100000n;"), ["SELECT", "-", "100000n", ";"]);
    assert_eq!(
        tok_typ("SELECT -100000n;"),
        [keyword("select"), Sub, BigIntConst, Semicolon]
    );
    assert_eq!(tok_str("SELECT -354.32n;"), ["SELECT", "-", "354.32n", ";"]);
    assert_eq!(
        tok_typ("SELECT -354.32n;"),
        [keyword("select"), Sub, DecimalConst, Semicolon]
    );
    assert_eq!(
        tok_str("SELECT 35400000000000.32n;"),
        ["SELECT", "35400000000000.32n", ";"]
    );
    assert_eq!(
        tok_typ("SELECT 35400000000000.32n;"),
        [keyword("select"), DecimalConst, Semicolon]
    );
    assert_eq!(
        tok_str("SELECT -35400000000000000000.32n;"),
        ["SELECT", "-", "35400000000000000000.32n", ";"]
    );
    assert_eq!(
        tok_typ("SELECT -35400000000000000000.32n;"),
        [keyword("select"), Sub, DecimalConst, Semicolon]
    );
    assert_eq!(tok_str("SELECT 3.5432e20n;"), ["SELECT", "3.5432e20n", ";"]);
    assert_eq!(
        tok_typ("SELECT 3.5432e20n;"),
        [keyword("select"), DecimalConst, Semicolon]
    );
    assert_eq!(
        tok_str("SELECT -3.5432e+20n;"),
        ["SELECT", "-", "3.5432e+20n", ";"]
    );
    assert_eq!(
        tok_typ("SELECT -3.5432e+20n;"),
        [keyword("select"), Sub, DecimalConst, Semicolon]
    );
    assert_eq!(
        tok_str("SELECT 3.5432e-20n;"),
        ["SELECT", "3.5432e-20n", ";"]
    );
    assert_eq!(
        tok_typ("SELECT 3.5432e-20n;"),
        [keyword("select"), DecimalConst, Semicolon]
    );
    assert_eq!(
        tok_str("SELECT 354.32e-20n;"),
        ["SELECT", "354.32e-20n", ";"]
    );
    assert_eq!(
        tok_typ("SELECT 354.32e-20n;"),
        [keyword("select"), DecimalConst, Semicolon]
    );
}

#[test]
fn num_errors() {
    assert_eq!(
        tok_err("0. "),
        "expected digit after dot, found end of decimal"
    );
    assert_eq!(
        tok_err("1.<"),
        "expected digit after dot, found end of decimal"
    );
    assert_eq!(tok_err("0.n"), "expected digit after dot, found suffix");
    assert_eq!(tok_err("0.e1"), "expected digit after dot, found exponent");
    assert_eq!(tok_err("0.e1n"), "expected digit after dot, found exponent");
    assert_eq!(
        tok_err("0."),
        "expected digit after dot, found end of decimal"
    );
    assert_eq!(tok_err("1.0.x"), "unexpected extra decimal dot in number");
    assert_eq!(tok_err("1.0e1."), "unexpected extra decimal dot in number");
    assert_eq!(
        tok_err("1.0e."),
        "unexpected optional `+` or `-` \
        followed by digits must follow `e` in float const"
    );
    assert_eq!(
        tok_err("1.0e"),
        "unexpected optional `+` or `-` \
        followed by digits must follow `e` in float const"
    );
    assert_eq!(
        tok_err("1.0ex"),
        "unexpected optional `+` or `-` \
        followed by digits must follow `e` in float const"
    );
    assert_eq!(
        tok_err("1.0en"),
        "unexpected optional `+` or `-` \
        followed by digits must follow `e` in float const"
    );
    assert_eq!(
        tok_err("1.0e "),
        "unexpected optional `+` or `-` \
        followed by digits must follow `e` in float const"
    );
    assert_eq!(
        tok_err("1.0e_"),
        "unexpected optional `+` or `-` \
        followed by digits must follow `e` in float const"
    );
    assert_eq!(
        tok_err("1.0e_ "),
        "unexpected optional `+` or `-` \
        followed by digits must follow `e` in float const"
    );
    assert_eq!(
        tok_err("1.0e_1"),
        "unexpected optional `+` or `-` \
        followed by digits must follow `e` in float const"
    );
    assert_eq!(
        tok_err("1.0e+"),
        "unexpected optional `+` or `-` \
        followed by digits must follow `e` in float const"
    );
    assert_eq!(
        tok_err("1.0e+ "),
        "unexpected optional `+` or `-` \
        followed by digits must follow `e` in float const"
    );
    assert_eq!(
        tok_err("1.0e+x"),
        "unexpected optional `+` or `-` \
        followed by digits must follow `e` in float const"
    );
    assert_eq!(
        tok_err("1.0e+n"),
        "unexpected optional `+` or `-` \
        followed by digits must follow `e` in float const"
    );
    assert_eq!(
        tok_err("1234numeric"),
        "suffix \"numeric\" \
        is invalid for numbers, perhaps you wanted `1234n` (bigint)?"
    );
    assert_eq!(
        tok_err("1234some_l0ng_trash"),
        "suffix \"some_l0n...\" \
        is invalid for numbers, perhaps you wanted `1234n` (bigint)?"
    );
    assert_eq!(
        tok_err("100O00"),
        "suffix \"O00\" is invalid for numbers, \
        perhaps mixed up letter `O` with zero `0`?"
    );
    assert_eq!(
        tok_err("01"),
        "unexpected leading zeros are not allowed in numbers"
    );
    assert_eq!(
        tok_err("01n"),
        "unexpected leading zeros are not allowed in numbers"
    );
    assert_eq!(
        tok_err("01_n"),
        "unexpected leading zeros are not allowed in numbers"
    );
    assert_eq!(
        tok_err("0_1_n"),
        "unexpected leading zeros are not allowed in numbers"
    );
    assert_eq!(
        tok_err("0_1n"),
        "unexpected leading zeros are not allowed in numbers"
    );
}

#[test]
fn tuple_paths() {
    assert_eq!(
        tok_str("tup.1.2.3.4.5"),
        ["tup", ".", "1", ".", "2", ".", "3", ".", "4", ".", "5"]
    );
    assert_eq!(
        tok_typ("tup.1.2.3.4.5"),
        [Ident, Dot, IntConst, Dot, IntConst, Dot, IntConst, Dot, IntConst, Dot, IntConst]
    );
    assert_eq!(
        tok_err("tup.1.2.>3.4.>5"),
        "unexpected extra decimal dot in number"
    );
    assert_eq!(
        tok_str("$0.1.2.3.4.5"),
        ["$0", ".", "1", ".", "2", ".", "3", ".", "4", ".", "5"]
    );
    assert_eq!(
        tok_typ("$0.1.2.3.4.5"),
        [Parameter, Dot, IntConst, Dot, IntConst, Dot, IntConst, Dot, IntConst, Dot, IntConst]
    );
    assert_eq!(
        tok_err("tup.1n"),
        "unexpected char \'n\', only integers \
        are allowed after dot (for tuple access)"
    );

    assert_eq!(
        tok_err("tup.01"),
        "leading zeros are not allowed in numbers"
    );
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
    assert_eq!(tok_str(r#" br""  "#), [r#"br"""#]);
    assert_eq!(tok_typ(r#" br""  "#), [BinStr]);
    assert_eq!(tok_str(r#" br''  "#), [r#"br''"#]);
    assert_eq!(tok_typ(r#" br''  "#), [BinStr]);
    assert_eq!(tok_err(r#" ``  "#), "backtick quotes cannot be empty");

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
    assert_eq!(tok_str(r#" rb"hello"  "#), [r#"rb"hello""#]);
    assert_eq!(tok_typ(r#" rb"hello"  "#), [BinStr]);
    assert_eq!(tok_str(r#" rb'hello'  "#), [r#"rb'hello'"#]);
    assert_eq!(tok_typ(r#" rb'hello'  "#), [BinStr]);
    assert_eq!(tok_str(r#" `hello`  "#), [r#"`hello`"#]);
    assert_eq!(tok_typ(r#" `hello`  "#), [Ident]);

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
    assert_eq!(tok_str(r#" rb"hello""#), [r#"rb"hello""#]);
    assert_eq!(tok_typ(r#" rb"hello""#), [BinStr]);
    assert_eq!(tok_str(r#" rb'hello'"#), [r#"rb'hello'"#]);
    assert_eq!(tok_typ(r#" rb'hello'"#), [BinStr]);
    assert_eq!(tok_str(r#" `hello`"#), [r#"`hello`"#]);
    assert_eq!(tok_typ(r#" `hello`"#), [Ident]);

    assert_eq!(tok_str(r#" "h\"ello" "#), [r#""h\"ello""#]);
    assert_eq!(tok_typ(r#" "h\"ello" "#), [Str]);
    assert_eq!(tok_str(r" 'h\'ello' "), [r"'h\'ello'"]);
    assert_eq!(tok_typ(r" 'h\'ello' "), [Str]);
    assert_eq!(tok_str(r#" r"hello\" "#), [r#"r"hello\""#]);
    assert_eq!(tok_typ(r#" r"hello\" "#), [Str]);
    assert_eq!(tok_str(r" r'hello\' "), [r"r'hello\'"]);
    assert_eq!(tok_typ(r" r'hello\' "), [Str]);
    assert_eq!(tok_str(r#" b"h\"ello" "#), [r#"b"h\"ello""#]);
    assert_eq!(tok_typ(r#" b"h\"ello" "#), [BinStr]);
    assert_eq!(tok_str(r" b'h\'ello' "), [r"b'h\'ello'"]);
    assert_eq!(tok_typ(r" b'h\'ello' "), [BinStr]);
    assert_eq!(tok_str(r#" rb"hello\" "#), [r#"rb"hello\""#]);
    assert_eq!(tok_typ(r#" rb"hello\" "#), [BinStr]);
    assert_eq!(tok_str(r" rb'hello\' "), [r"rb'hello\'"]);
    assert_eq!(tok_typ(r" rb'hello\' "), [BinStr]);
    assert_eq!(tok_str(r" `hello\` "), [r"`hello\`"]);
    assert_eq!(tok_typ(r" `hello\` "), [Ident]);
    assert_eq!(tok_str(r#" `hel``lo` "#), [r#"`hel``lo`"#]);
    assert_eq!(tok_typ(r#" `hel``lo` "#), [Ident]);

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
    assert_eq!(tok_str(r#" rb"h'el`lo" "#), [r#"rb"h'el`lo""#]);
    assert_eq!(tok_typ(r#" rb"h'el`lo" "#), [BinStr]);
    assert_eq!(tok_str(r#" rb'h"el`lo' "#), [r#"rb'h"el`lo'"#]);
    assert_eq!(tok_typ(r#" rb'h"el`lo' "#), [BinStr]);
    assert_eq!(tok_str(r#" `h'el"lo` "#), [r#"`h'el"lo`"#]);
    assert_eq!(tok_typ(r#" `h'el"lo\` "#), [Ident]);

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
    assert_eq!(tok_typ(" rb'hel\nlo' "), [BinStr]);
    assert_eq!(tok_typ(" br'hel\nlo' "), [BinStr]);
    assert_eq!(tok_str(" rb'hel\nlo' "), ["rb'hel\nlo'"]);
    assert_eq!(tok_str(" br'hel\nlo' "), ["br'hel\nlo'"]);
    assert_eq!(tok_str(" `hel\nlo` "), ["`hel\nlo`"]);
    assert_eq!(tok_typ(" `hel\nlo` "), [Ident]);

    assert_eq!(tok_err(r#""hello"#), "unterminated string, quoted by `\"`");
    assert_eq!(tok_err(r#"'hello"#), "unterminated string, quoted by `'`");
    assert_eq!(tok_err(r#"r"hello"#), "unterminated string, quoted by `\"`");
    assert_eq!(tok_err(r#"r'hello"#), "unterminated string, quoted by `'`");
    assert_eq!(tok_err(r#"b"hello"#), "unterminated string, quoted by `\"`");
    assert_eq!(tok_err(r#"b'hello"#), "unterminated string, quoted by `'`");
    assert_eq!(tok_err(r#"`hello"#), "unterminated backtick name");

    assert_eq!(
        tok_err(r#"name`type`"#),
        "prefix \"name\" is not allowed for field names, \
        perhaps missing comma or dot?"
    );
    assert_eq!(
        tok_err(r#"User`type`"#),
        "prefix \"User\" is not allowed for field names, \
        perhaps missing comma or dot?"
    );
    assert_eq!(
        tok_err(r#"r`hello"#),
        "prefix \"r\" is not allowed for field names, \
        perhaps missing comma or dot?"
    );
    assert_eq!(
        tok_err(r#"b`hello"#),
        "prefix \"b\" is not allowed for field names, \
        perhaps missing comma or dot?"
    );
    assert_eq!(
        tok_err(r#"test"hello""#),
        "prefix \"test\" is not allowed for strings, \
        allowed: `b`, `r`"
    );
    assert_eq!(
        tok_err(r#"test'hello'"#),
        "prefix \"test\" is not allowed for strings, \
        allowed: `b`, `r`"
    );
    assert_eq!(
        tok_err(r#"`@x`"#),
        "backtick-quoted name cannot start with char `@`"
    );
    assert_eq!(
        tok_err(r#"`$x`"#),
        "backtick-quoted name cannot start with char `$`"
    );
    assert_eq!(
        tok_err(r#"`a::b`"#),
        "backtick-quoted name cannot contain `::`"
    );
    assert_eq!(
        tok_err(r#"`__x__`"#),
        "backtick-quoted names surrounded by double \
                    underscores are forbidden"
    );
}

#[test]
fn string_prohibited_chars() {
    assert_eq!(
        tok_err("'xxx \u{202A}'"),
        "character U+202A is not allowed, use escaped form \\u202a"
    );
    assert_eq!(
        tok_err("\"\u{202A} yyy\""),
        "character U+202A is not allowed, use escaped form \\u202a"
    );
    assert_eq!(
        tok_err("r\"\u{202A}\""),
        "character U+202A is not allowed, use escaped form \\u202a"
    );
    assert_eq!(
        tok_err("r'\u{202A}'"),
        "character U+202A is not allowed, use escaped form \\u202a"
    );
    assert_eq!(
        tok_err("b'\u{202A}'"),
        "invalid bytes literal: character '\\u{202a}' \
         is unexpected, only ascii chars are allowed in bytes literals"
    );
    assert_eq!(
        tok_err("b\"\u{202A}\""),
        "invalid bytes literal: character '\\u{202a}' \
         is unexpected, only ascii chars are allowed in bytes literals"
    );
    assert_eq!(tok_err("`\u{202A}`"), "character U+202A is not allowed");
    assert_eq!(tok_err("$`\u{202A}`"), "character U+202A is not allowed");
    assert_eq!(
        tok_err("$x\u{202A}$ inner $x\u{202A}$"),
        "unexpected character '\\u{202a}'"
    );
    assert_eq!(tok_err("$$ \u{202A} $$"), "character U+202A is not allowed");
    assert_eq!(
        tok_err("$hello$ \u{202A} $hello$"),
        "character U+202A is not allowed"
    );
    assert_eq!(tok_err("'xxx \0'"), "character U+0000 is not allowed");
    assert_eq!(tok_err("xxx \0"), "unexpected character '\\0'");
    assert_eq!(tok_err("xxx $x$\0$x$"), "character U+0000 is not allowed");
}

#[test]
fn test_dollar() {
    assert_eq!(
        tok_str("select $$ something $$; x"),
        ["select", "$$ something $$", ";", "x"]
    );
    assert_eq!(
        tok_typ("select $$ something $$; x"),
        [keyword("select"), Str, Semicolon, Ident]
    );
    assert_eq!(
        tok_str("select $a$ ; $b$ ; $b$ ; $a$; x"),
        ["select", "$a$ ; $b$ ; $b$ ; $a$", ";", "x"]
    );
    assert_eq!(
        tok_typ("select $a$ ; $b$ ; $b$ ; $a$; x"),
        [keyword("select"), Str, Semicolon, Ident]
    );
    assert_eq!(
        tok_str("select $a$ ; $b$ ; $a$; x"),
        ["select", "$a$ ; $b$ ; $a$", ";", "x"]
    );
    assert_eq!(
        tok_typ("select $a$ ; $b$ ; $a$; x"),
        [keyword("select"), Str, Semicolon, Ident]
    );
    assert_eq!(
        tok_err("select $$ ; $ab$ test;"),
        "unterminated string started with $$"
    );
    assert_eq!(
        tok_err("select $a$ ; $$ test;"),
        "unterminated string started with \"$a$\""
    );
    assert_eq!(
        tok_err("select $0$"),
        "dollar quote must not start with a digit"
    );
    assert_eq!(
        tok_err("select $фыва$"),
        "dollar quote supports only ascii chars"
    );
    assert_eq!(
        tok_str("select $a$a$ ; $a$ test;"),
        ["select", "$a$a$ ; $a$", "test", ";"]
    );
    assert_eq!(
        tok_typ("select $a$a$ ; $a$ test;"),
        [keyword("select"), Str, Ident, Semicolon]
    );
    assert_eq!(
        tok_str("select $a+b; $b test; $a+b; $b ;"),
        ["select", "$a", "+", "b", ";", "$b", "test", ";", "$a", "+", "b", ";", "$b", ";"]
    );
    assert_eq!(
        tok_typ("select $a+b; $b test; $a+b; $b ;"),
        [
            keyword("select"),
            Parameter,
            Add,
            Ident,
            Semicolon,
            Parameter,
            Ident,
            Semicolon,
            Parameter,
            Add,
            Ident,
            Semicolon,
            Parameter,
            Semicolon
        ]
    );
    assert_eq!(
        tok_str("select $def x$y test; $def x$y"),
        ["select", "$def", "x", "$y", "test", ";", "$def", "x", "$y"]
    );
    assert_eq!(
        tok_typ("select $def x$y test; $def x$y"),
        [
            keyword("select"),
            Parameter,
            Ident,
            Parameter,
            Ident,
            Semicolon,
            Parameter,
            Ident,
            Parameter
        ]
    );
    assert_eq!(
        tok_str("select $`x``y` + $0 + $`zz` + $1.2 + $фыва"),
        [
            "select",
            "$`x``y`",
            "+",
            "$0",
            "+",
            "$`zz`",
            "+",
            "$1",
            ".",
            "2",
            "+",
            "$фыва"
        ]
    );
    assert_eq!(
        tok_typ("select $`x``y` + $0 + $`zz` + $1.2 + $фыва"),
        [
            keyword("select"),
            Parameter,
            Add,
            Parameter,
            Add,
            Parameter,
            Add,
            Parameter,
            Dot,
            IntConst,
            Add,
            Parameter
        ]
    );
    assert_eq!(tok_err(r#"$-"#), "bare $ is not allowed");
    assert_eq!(
        tok_err(r#"$0abc"#),
        "the \"$0abc\" is not a valid argument, \
         either name starting with letter or only digits are expected"
    );
    assert_eq!(tok_err(r#"-$"#), "bare $ is not allowed");
    assert_eq!(
        tok_err(r#" $``  "#),
        "backtick-quoted argument cannot be empty"
    );
    assert_eq!(
        tok_err(r#"$`@x`"#),
        "backtick-quoted argument cannot \
        start with char `@`"
    );
    assert_eq!(
        tok_err(r#"$`a::b`"#),
        "backtick-quoted argument cannot contain `::`"
    );
    assert_eq!(
        tok_err(r#"$`__x__`"#),
        "backtick-quoted arguments surrounded by double \
                    underscores are forbidden"
    );
}

#[test]
fn invalid_suffix() {
    assert_eq!(
        tok_err("SELECT 1d;"),
        "suffix \"d\" \
        is invalid for numbers, perhaps you wanted `1n` (bigint)?"
    );
}

#[test]
fn test_substitution() {
    assert_eq!(tok_str("SELECT \\(expr);"), ["SELECT", "\\(expr)", ";"]);
    assert_eq!(
        tok_typ("SELECT \\(expr);"),
        [keyword("select"), Substitution, Semicolon]
    );
    assert_eq!(
        tok_str("SELECT \\(other_Name1);"),
        ["SELECT", "\\(other_Name1)", ";"]
    );
    assert_eq!(
        tok_typ("SELECT \\(other_Name1);"),
        [keyword("select"), Substitution, Semicolon]
    );
    assert_eq!(
        tok_err("SELECT \\(some-name);"),
        "only alphanumerics are allowed in \\(name) token"
    );
    assert_eq!(tok_err("SELECT \\(some_name"), "unclosed \\(name) token");
}
