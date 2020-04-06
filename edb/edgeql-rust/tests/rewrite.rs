use edgeql_rust::rewrite::{rewrite, Value, Variable};


#[test]
fn test_no_args() {
    let entry = rewrite(r###"
        SELECT 1
    "###).unwrap();
    assert_eq!(entry.key, "SELECT $_edb_arg__0");
    assert_eq!(entry.variables, vec![
        Variable {
            value: Value::Int("1".into()),
        }
    ]);
}
