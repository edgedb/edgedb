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
    assert_eq!(entry.key, "CONFIGURE SYSTEM SET some_setting := 7");
    assert_eq!(entry.variables, vec![
        Variable {
            value: Value::Int("1".into()),
        }
    ]);
}
