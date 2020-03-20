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
    assert_eq!(entry.key, "\
        query($_edb_arg__0:String!){\
            object(filter:{field:{eq:$_edb_arg__0}}){\
                field\
            }\
        }\
    ");
    assert_eq!(entry.variables, vec![
        Variable::Str("test".into()),
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
        Variable::Str("test".into()),
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
        Variable::Str("test".into()),
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
        Variable::Str("test".into()),
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
        Variable::Str("test".into()),
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
        Variable::Str("test".into()),
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
        Variable::Str("test2".into()),
    ]);
}
