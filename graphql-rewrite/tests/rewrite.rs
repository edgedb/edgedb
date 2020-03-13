use graphql_rewrite::{rewrite, Variable};


#[test]
fn test_no_args() {
    let entry = rewrite(None, r###"
        query {
            object(filter: {field: {eq: "test"}}) {
                field
            }
        }
    "###).unwrap();
    assert_eq!(entry.key, r###"
        query Query($_edb_arg__0: String!) {
            object(filter: {field: {eq: $_edb_arg__0}}) {
                field
            }
        }
    "###);
    assert_eq!(entry.variables, vec![
        Variable::Str("test".into()),
    ]);
    /*
    assert_eq!(entry.tokens.map(|t| t.text, vec![
        ...
    ]);
    */
}

#[test]
fn test_skip_operation_none() {
    todo!();
}

#[test]
fn test_skip_operation_name() {
    todo!();
}
