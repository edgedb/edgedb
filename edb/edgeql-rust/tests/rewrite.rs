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
    assert_eq!(entry.key, "SELECT<int64>$_edb_arg__0+<int64>$_edb_arg__1");
    assert_eq!(entry.variables, vec![
        Variable {
            value: Value::Int("1".into()),
        },
        Variable {
            value: Value::Int("2".into()),
        }
    ]);
}

#[test]
fn test_limit_1() {
    let entry = rewrite(r###"
        SELECT User { one := 1 } LIMIT 1
    "###).unwrap();
    assert_eq!(entry.key, "SELECT User{one:=<int64>$_edb_arg__0}LIMIT 1");
    assert_eq!(entry.variables, vec![
        Variable {
            value: Value::Int("1".into()),
        },
    ]);
}
