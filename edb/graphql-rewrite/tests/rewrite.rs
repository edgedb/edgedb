use edb_graphql_parser::{Pos};

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
