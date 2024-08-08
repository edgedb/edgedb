use edgeql_parser::expr::check;

#[test]
fn test_valid() {
    check("1").unwrap();
    check(" 42    ").unwrap();
    check("42 # )").unwrap();
    check("33 ++ 44").unwrap();
    check("33 ++ '44'").unwrap();
    check("(1, 2) # tuple").unwrap();
    check("# next line\n 2+2").unwrap();
    check("{}").unwrap();
    check("()").unwrap();
    check(".user.name").unwrap();
    check("call(me.maybe)").unwrap();
    check("bad +/- grammar **** but --- allowed").unwrap();
}

fn check_err(s: &str) -> String {
    check(s).unwrap_err().to_string()
}

#[test]
fn test_empty() {
    assert_eq!(check_err(""), "expression is empty");
    assert_eq!(check_err("   "), "expression is empty");
    assert_eq!(check_err("# xxx + yyy"), "expression is empty");
}

#[test]
fn bad_token() {
    assert_eq!(
        check_err("'quote"),
        "1:1: tokenizer error: unterminated string, quoted by `'`"
    );
    assert_eq!(
        check_err("\\(quote"),
        "1:1: tokenizer error: unclosed \\(name) token"
    );
}

#[test]
fn bracket_mismatch() {
    assert_eq!(
        check_err("(a[12)]"),
        "1:6: closing bracket mismatch, \
            opened \"[\" at 1:3, encountered \")\""
    );
    assert_eq!(
        check_err("(a12]"),
        "1:5: closing bracket mismatch, \
            opened \"(\" at 1:1, encountered \"]\""
    );
    assert_eq!(
        check_err("{'}']"),
        "1:5: closing bracket mismatch, \
            opened \"{\" at 1:1, encountered \"]\""
    );
}

#[test]
fn extra_brackets() {
    assert_eq!(check_err("func())"), "1:7: extra closing bracket \")\"");
    assert_eq!(check_err("{} + x]"), "1:7: extra closing bracket \"]\"");
    assert_eq!(
        check_err("{'xxx(yyy'})"),
        "1:12: extra closing bracket \")\""
    );
}

#[test]
fn missing_brackets() {
    assert_eq!(
        check_err("func((1, 2)"),
        "1:5: bracket \"(\" has never been closed"
    );
    assert_eq!(
        check_err("{(1, 2), (3, '}')"),
        "1:1: bracket \"{\" has never been closed"
    );
    assert_eq!(
        check_err("{((())[[()"),
        "1:8: bracket \"[\" has never been closed"
    );
}

#[test]
fn delimiter() {
    assert_eq!(
        check_err("1, 2"),
        "1:2: token \",\" is not allowed in expression \
         (try parenthesize the expression)"
    );
    check("(1, 2)").unwrap();

    assert_eq!(
        check_err("create type Type1;"),
        "1:18: token \";\" is not allowed in expression \
         (try parenthesize the expression)"
    );
    // this doesn't work, but is fun to see
    check("{create if not exists type Type1; SELECT Type1}").unwrap();
}
