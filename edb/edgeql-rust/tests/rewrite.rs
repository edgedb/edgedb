use edgeql_rust::rewrite::{rewrite, Value, Variable};


#[test]
fn test_verbatim() {
    let entry = rewrite(r###"
        SELECT $1 + $2
    "###).unwrap();
    assert_eq!(entry.key, "SELECT$1+$2");
    assert_eq!(entry.variables, vec![]);
}

#[test]
fn test_configure() {
    let entry = rewrite(r###"
        CONFIGURE SYSTEM SET some_setting := 7
    "###).unwrap();
    assert_eq!(entry.key, "CONFIGURE SYSTEM SET some_setting:=7");
    assert_eq!(entry.variables, vec![]);
}

#[test]
fn test_int() {
    let entry = rewrite(r###"
        SELECT 1 + 2
    "###).unwrap();
    assert_eq!(entry.key, "SELECT<int64>$0+<int64>$1");
    assert_eq!(entry.variables, vec![
        Variable {
            value: Value::Int(1),
        },
        Variable {
            value: Value::Int(2),
        }
    ]);
}

#[test]
fn test_positional() {
    let entry = rewrite(r###"
        SELECT <int64>$0 + 2
    "###).unwrap();
    assert_eq!(entry.key, "SELECT<int64>$0+<int64>$1");
    assert_eq!(entry.variables, vec![
        Variable {
            value: Value::Int(2),
        }
    ]);
}

#[test]
fn test_named() {
    let entry = rewrite(r###"
        SELECT <int64>$test_var + 2
    "###).unwrap();
    assert_eq!(entry.key, "SELECT<int64>$test_var+<int64>$__edb_arg_1");
    assert_eq!(entry.variables, vec![
        Variable {
            value: Value::Int(2),
        }
    ]);
}

#[test]
fn test_limit_1() {
    let entry = rewrite(r###"
        SELECT User { one := 1 } LIMIT 1
    "###).unwrap();
    assert_eq!(entry.key, "SELECT User{one:=<int64>$0}LIMIT 1");
    assert_eq!(entry.variables, vec![
        Variable {
            value: Value::Int(1),
        },
    ]);
}

#[test]
fn test_tuple_access() {
    let entry = rewrite(r###"
        SELECT User { one := 2, two := .field.2, three := .field  . 3 }
    "###).unwrap();
    assert_eq!(entry.key,
        "SELECT User{one:=<int64>$0,two:=.field.2,three:=.field.3}");
    assert_eq!(entry.variables, vec![
        Variable {
            value: Value::Int(2),
        },
    ]);
}
