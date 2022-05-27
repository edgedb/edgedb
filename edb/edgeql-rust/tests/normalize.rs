use edgeql_rust::normalize::{normalize, Value, Variable};


#[test]
fn test_verbatim() {
    let entry = normalize(r###"
        SELECT $1 + $2
    "###).unwrap();
    assert_eq!(entry.processed_source, "SELECT$1+$2");
    assert_eq!(entry.variables, vec![vec![]]);
}

#[test]
fn test_configure() {
    let entry = normalize(r###"
        CONFIGURE INSTANCE SET some_setting := 7
    "###).unwrap();
    assert_eq!(entry.processed_source, "CONFIGURE INSTANCE SET some_setting:=7");
    assert_eq!(entry.variables, vec![] as Vec<Vec<Variable>>);
}

#[test]
fn test_int() {
    let entry = normalize(r###"
        SELECT 1 + 2
    "###).unwrap();
    assert_eq!(entry.processed_source,
        "SELECT(<__std__::int64>$0)+(<__std__::int64>$1)");
    assert_eq!(entry.variables, vec![vec![
        Variable {
            value: Value::Int(1),
        },
        Variable {
            value: Value::Int(2),
        }
    ]]);
}

#[test]
fn test_str() {
    let entry = normalize(r###"
        SELECT "x" + "yy"
    "###).unwrap();
    assert_eq!(entry.processed_source,
        "SELECT(<__std__::str>$0)+(<__std__::str>$1)");
    assert_eq!(entry.variables, vec![vec![
        Variable {
            value: Value::Str("x".into()),
        },
        Variable {
            value: Value::Str("yy".into()),
        }
    ]]);
}

#[test]
fn test_float() {
    let entry = normalize(r###"
        SELECT 1.5 + 23.25
    "###).unwrap();
    assert_eq!(entry.processed_source,
        "SELECT(<__std__::float64>$0)+(<__std__::float64>$1)");
    assert_eq!(entry.variables, vec![vec![
        Variable {
            value: Value::Float(1.5),
        },
        Variable {
            value: Value::Float(23.25),
        }
    ]]);
}

#[test]
fn test_bigint() {
    let entry = normalize(r###"
        SELECT 1n + 23n
    "###).unwrap();
    assert_eq!(entry.processed_source,
        "SELECT(<__std__::bigint>$0)+(<__std__::bigint>$1)");
    assert_eq!(entry.variables, vec![vec![
        Variable {
            value: Value::BigInt(1.into()),
        },
        Variable {
            value: Value::BigInt(23.into()),
        }
    ]]);
}

#[test]
fn test_bigint_exponent() {
    let entry = normalize(r###"
        SELECT 1e10n + 23e13n
    "###).unwrap();
    assert_eq!(entry.processed_source,
        "SELECT(<__std__::bigint>$0)+(<__std__::bigint>$1)");
    assert_eq!(entry.variables, vec![vec![
        Variable {
            value: Value::BigInt(10000000000u64.into()),
        },
        Variable {
            value: Value::BigInt(230000000000000u64.into()),
        }
    ]]);
}

#[test]
fn test_decimal() {
    let entry = normalize(r###"
        SELECT 1.33n + 23.77n
    "###).unwrap();
    assert_eq!(entry.processed_source,
        "SELECT(<__std__::decimal>$0)+(<__std__::decimal>$1)");
    assert_eq!(entry.variables, vec![vec![
        Variable {
            value: Value::Decimal("1.33".parse().unwrap()),
        },
        Variable {
            value: Value::Decimal("23.77".parse().unwrap()),
        }
    ]]);
}

#[test]
fn test_positional() {
    let entry = normalize(r###"
        SELECT <int64>$0 + 2
    "###).unwrap();
    assert_eq!(entry.processed_source, "SELECT<int64>$0+(<__std__::int64>$1)");
    assert_eq!(entry.variables, vec![vec![
        Variable {
            value: Value::Int(2),
        }
    ]]);
}

#[test]
fn test_named() {
    let entry = normalize(r###"
        SELECT <int64>$test_var + 2
    "###).unwrap();
    assert_eq!(entry.processed_source,
        "SELECT<int64>$test_var+(<__std__::int64>$__edb_arg_1)");
    assert_eq!(entry.variables, vec![vec![
        Variable {
            value: Value::Int(2),
        }
    ]]);
}

#[test]
fn test_limit_1() {
    let entry = normalize(r###"
        SELECT User { one := 1 } LIMIT 1
    "###).unwrap();
    assert_eq!(entry.processed_source,
        "SELECT User{one:=(<__std__::int64>$0)}LIMIT 1");
    assert_eq!(entry.variables, vec![vec![
        Variable {
            value: Value::Int(1),
        },
    ]]);
}

#[test]
fn test_tuple_access() {
    let entry = normalize(r###"
        SELECT User { one := 2, two := .field.2, three := .field  . 3 }
    "###).unwrap();
    assert_eq!(entry.processed_source,
        "SELECT User{one:=(<__std__::int64>$0),\
                     two:=.field.2,three:=.field.3}");
    assert_eq!(entry.variables, vec![vec![
        Variable {
            value: Value::Int(2),
        },
    ]]);
}

#[test]
fn test_script() {
    let entry = normalize(r###"
        SELECT 1 + 2;
        SELECT 2;
    "###).unwrap();
    assert_eq!(
        entry.processed_source,
        "SELECT(<__std__::int64>$0)+(<__std__::int64>$1);\
        SELECT(<__std__::int64>$2);",
    );
    assert_eq!(entry.variables, vec![
        vec![
            Variable {
                value: Value::Int(1),
            },
            Variable {
                value: Value::Int(2),
            }
        ],
        vec![
            Variable {
                value: Value::Int(2),
            }
        ],
    ]);
}

#[test]
fn test_script_with_args() {
    let entry = normalize(r###"
        SELECT 2 + $1;
        SELECT $1 + 2;
    "###).unwrap();
    assert_eq!(
        entry.processed_source,
        "SELECT(<__std__::int64>$2)+$1;SELECT$1+(<__std__::int64>$3);",
    );
    assert_eq!(entry.variables, vec![
        vec![
            Variable {
                value: Value::Int(2),
            }
        ],
        vec![
            Variable {
                value: Value::Int(2),
            }
        ],
    ]);
}
