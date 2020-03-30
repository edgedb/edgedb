use std::collections::BTreeMap;

use edb_graphql_parser::{Pos};
use num_bigint::BigInt;

use graphql_rewrite::{rewrite, Variable, Value};
use graphql_rewrite::{PyToken, PyTokenKind};


#[test]
fn test_no_args() {
    let entry = rewrite(None, r###"
        query {
            object(filter: {field: {eq: "test"}}) {
                field
            }
        }
    "###).unwrap();
    assert_eq!(entry.key, "\
        query($_edb_arg__0:String!){\
            object(filter:{field:{eq:$_edb_arg__0}}){\
                field\
            }\
        }\
    ");
    assert_eq!(entry.variables, vec![
        Variable {
            token: PyToken {
                kind: PyTokenKind::String,
                value: r#""test""#.into(),
                position: Some(Pos { line: 3, column: 41,
                                     character: 57, token: 12 }),
            },
            value: Value::Str("test".into()),
        }
    ]);
}

#[test]
fn test_no_query() {
    let entry = rewrite(None, r###"
        {
            object(filter: {field: {eq: "test"}}) {
                field
            }
        }
    "###).unwrap();
    assert_eq!(entry.key, "\
        query($_edb_arg__0:String!){\
            object(filter:{field:{eq:$_edb_arg__0}}){\
                field\
            }\
        }\
    ");
    assert_eq!(entry.variables, vec![
        Variable {
            token: PyToken {
                kind: PyTokenKind::String,
                value: r#""test""#.into(),
                position: Some(Pos { line: 3, column: 41,
                                     character: 51, token: 11 }),
            },
            value: Value::Str("test".into()),
        }
    ]);
}

#[test]
fn test_no_name() {
    let entry = rewrite(None, r###"
        query($x: String) {
            object(filter: {field: {eq: "test"}}, y: $x) {
                field
            }
        }
    "###).unwrap();
    assert_eq!(entry.key, "\
        query($x:String $_edb_arg__0:String!){\
            object(filter:{field:{eq:$_edb_arg__0}}y:$x){\
                field\
            }\
        }\
    ");
    assert_eq!(entry.variables, vec![
        Variable {
            token: PyToken {
                kind: PyTokenKind::String,
                value: r#""test""#.into(),
                position: Some(Pos { line: 3, column: 41,
                                     character: 69, token: 18 }),
            },
            value: Value::Str("test".into()),
        }
    ]);
}

#[test]
fn test_name_args() {
    let entry = rewrite(Some("Hello"), r###"
        query Hello($x: String, $y: String!) {
            object(filter: {field: {eq: "test"}}, x: $x, y: $y) {
                field
            }
        }
    "###).unwrap();
    assert_eq!(entry.key, "\
        query Hello($x:String $y:String!$_edb_arg__0:String!){\
            object(filter:{field:{eq:$_edb_arg__0}}x:$x y:$y){\
                field\
            }\
        }\
    ");
    assert_eq!(entry.variables, vec![
        Variable {
            token: PyToken {
                kind: PyTokenKind::String,
                value: r#""test""#.into(),
                position: Some(Pos { line: 3, column: 41,
                                     character: 88, token: 24 }),
            },
            value: Value::Str("test".into()),
        }
    ]);
}

#[test]
fn test_name() {
    let entry = rewrite(Some("Hello"), r###"
        query Hello {
            object(filter: {field: {eq: "test"}}) {
                field
            }
        }
    "###).unwrap();
    assert_eq!(entry.key, "\
        query Hello($_edb_arg__0:String!){\
            object(filter:{field:{eq:$_edb_arg__0}}){\
                field\
            }\
        }\
    ");
    assert_eq!(entry.variables, vec![
        Variable {
            token: PyToken {
                kind: PyTokenKind::String,
                value: r#""test""#.into(),
                position: Some(Pos { line: 3, column: 41,
                                     character: 63, token: 13 }),
            },
            value: Value::Str("test".into()),
        }
    ]);
}

#[test]
fn test_default_name() {
    let entry = rewrite(None, r###"
        query Hello {
            object(filter: {field: {eq: "test"}}) {
                field
            }
        }
    "###).unwrap();
    assert_eq!(entry.key, "\
        query Hello($_edb_arg__0:String!){\
            object(filter:{field:{eq:$_edb_arg__0}}){\
                field\
            }\
        }\
    ");
    assert_eq!(entry.variables, vec![
        Variable {
            token: PyToken {
                kind: PyTokenKind::String,
                value: r#""test""#.into(),
                position: Some(Pos { line: 3, column: 41,
                                     character: 63, token: 13 }),
            },
            value: Value::Str("test".into()),
        }
    ]);
}

#[test]
fn test_other() {
    let entry = rewrite(Some("Hello"), r###"
        query Other {
            object(filter: {field: {eq: "test1"}}) {
                field
            }
        }
        query Hello {
            object(filter: {field: {eq: "test2"}}) {
                field
            }
        }
    "###).unwrap();
    assert_eq!(entry.key, "\
        query Other{\
            object(filter:{field:{eq:\"test1\"}}){\
                field\
            }\
        }\
        query Hello($_edb_arg__0:String!){\
            object(filter:{field:{eq:$_edb_arg__0}}){\
                field\
            }\
        }\
    ");
    assert_eq!(entry.variables, vec![
        Variable {
            token: PyToken {
                kind: PyTokenKind::String,
                value: r#""test2""#.into(),
                position: Some(Pos { line: 8, column: 41,
                                     character: 184, token: 34 }),
            },
            value: Value::Str("test2".into()),
        }
    ]);
}

#[test]
fn test_defaults() {
    let entry = rewrite(Some("Hello"), r###"
        query Hello($x: String = "xxx", $y: String! = "yyy") {
            object(filter: {field: {eq: "test"}}, x: $x, y: $y) {
                field
            }
        }
    "###).unwrap();
    assert_eq!(entry.key, "\
        query Hello($x:String!$y:String!$_edb_arg__0:String!){\
            object(filter:{field:{eq:$_edb_arg__0}}x:$x y:$y){\
                field\
            }\
        }\
    ");
    let mut defaults = BTreeMap::new();
    defaults.insert("x".to_owned(), Variable {
        value: Value::Str("xxx".into()),
        token: PyToken {
            kind: PyTokenKind::Equals,
            value: "=".into(),
            position: Some(Pos { line: 2, column: 32,
                                 character: 32, token: 7 }),
        },
    });
    defaults.insert("y".to_owned(), Variable {
        value: Value::Str("yyy".into()),
        token: PyToken {
            kind: PyTokenKind::Equals,
            value: "=".into(),
            position: Some(Pos { line: 2, column: 53,
                                 character: 53, token: 14 }),
        }
    });
    assert_eq!(entry.defaults, defaults);
}

#[test]
fn test_int32() {
    let entry = rewrite(None, r###"
        query {
            object(filter: {field: {eq: 17}}) {
                field
            }
        }
    "###).unwrap();
    assert_eq!(entry.key, "\
        query($_edb_arg__0:Int!){\
            object(filter:{field:{eq:$_edb_arg__0}}){\
                field\
            }\
        }\
    ");
    assert_eq!(entry.variables, vec![
        Variable {
            token: PyToken {
                kind: PyTokenKind::Int,
                value: r#"17"#.into(),
                position: Some(Pos { line: 3, column: 41,
                                     character: 57, token: 12 }),
            },
            value: Value::Int32(17),
        }
    ]);
}

#[test]
fn test_int64() {
    let entry = rewrite(None, r###"
        query {
            object(filter: {field: {eq: 17123456790}}) {
                field
            }
        }
    "###).unwrap();
    assert_eq!(entry.key, "\
        query($_edb_arg__0:Int64!){\
            object(filter:{field:{eq:$_edb_arg__0}}){\
                field\
            }\
        }\
    ");
    assert_eq!(entry.variables, vec![
        Variable {
            token: PyToken {
                kind: PyTokenKind::Int,
                value: r#"17123456790"#.into(),
                position: Some(Pos { line: 3, column: 41,
                                     character: 57, token: 12 }),
            },
            value: Value::Int64(17123456790),
        }
    ]);
}

#[test]
fn test_bigint() {
    let entry = rewrite(None, r###"
        query {
            object(filter: {field: {eq: 171234567901234567890}}) {
                field
            }
        }
    "###).unwrap();
    assert_eq!(entry.key, "\
        query($_edb_arg__0:Bigint!){\
            object(filter:{field:{eq:$_edb_arg__0}}){\
                field\
            }\
        }\
    ");
    assert_eq!(entry.variables, vec![
        Variable {
            token: PyToken {
                kind: PyTokenKind::Int,
                value: r#"171234567901234567890"#.into(),
                position: Some(Pos { line: 3, column: 41,
                                     character: 57, token: 12 }),
            },
            value: Value::BigInt("171234567901234567890".into()),
        }
    ]);
}
