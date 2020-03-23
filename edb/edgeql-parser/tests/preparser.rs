use edgeql_parser::preparser::full_statement;

fn test_statement(data: &[u8], len: usize) {
    for i in 0..len-1 {
        let c = full_statement(&data[..i], None).unwrap_err();
        let parsed_len = full_statement(data, Some(c)).unwrap();
        assert_eq!(len, parsed_len, "at {}", i);
    }
    for i in len..data.len() {
        let parsed_len = full_statement(&data[..i], None).unwrap();
        assert_eq!(len, parsed_len);
    }
}


#[test]
fn test_simple() {
    test_statement(b"select 1+1; some trailer", 11);
}

#[test]
fn test_quotes() {
    test_statement(b"select \"x\"; some trailer", 11);
}

#[test]
fn test_quoted_semicolon() {
    test_statement(b"select \"a;\"; some trailer", 12);
}

#[test]
fn test_single_quoted_semicolon() {
    test_statement(b"select 'a;'; some trailer", 12);
}

#[test]
fn test_backtick_quoted_semicolon() {
    test_statement(b"select `a;`; some trailer", 12);
}

#[test]
fn test_commented_semicolon() {
    test_statement(b"select # test;\n1+1;", 19);
}

#[test]
fn test_continuation() {
    test_statement(b"select 'a;'; '", 12);
}

#[test]
fn test_quoted_continuation() {
    test_statement(b"select \"a; \";", 13);
}

#[test]
fn test_single_quoted_continuation() {
    test_statement(b"select 'a; ' ;", 14);
}

#[test]
fn test_backtick_quoted_continuation() {
    test_statement(b"select `a;test`+1;", 18);
}

#[test]
fn test_dollar_semicolon() {
    test_statement(b"select $$ ; $$ test;", 20);
    test_statement(b"select $$$$;", 12);
    test_statement(b"select $$$ ; $$;", 16);
    test_statement(b"select $some_L0ng_name$ ; $some_L0ng_name$;", 43);
}

#[test]
fn test_nested_dollar() {
    test_statement(b"select $a$ ; $b$ ; $b$ ; $a$; x", 29);
    test_statement(b"select $a$ ; $b$ ; $a$; x", 23);
}

#[test]
fn test_dollar_continuation() {
    test_statement(b"select $$ ; $ab$ test; $$ ;", 27);
    test_statement(b"select $a$ ; $$ test; $a$ ;", 27);
    test_statement(b"select $a$ ; test; $a$ ;", 24);
    test_statement(b"select $a$a$ ; $$ test; $a$;", 28);
    test_statement(b"select $a$ ; $b$ ; $c$ ; $b$ test; $a$;", 39);
}

#[test]
fn test_dollar_var() {
    test_statement(b"select $a+b; $ test; $a+b; $ ;", 12);
    test_statement(b"select $a b; $ test; $a b; $ ;", 12);
}

#[test]
fn test_schema() {
    test_statement(br###"
        CREATE MIGRATION movies TO {
            module default {
                type Movie {
                    required property title -> str;
                    # the year of release
                    property year -> int64;
                    required link director -> Person;
                    multi link actors -> Person;
                }
                type Person {
                    required property first_name -> str;
                    required property last_name -> str;
                }
            }
        };
        "###, 540);
}
