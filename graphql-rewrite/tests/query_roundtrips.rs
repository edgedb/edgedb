extern crate graphql_parser;
#[cfg(test)] #[macro_use] extern crate pretty_assertions;

use std::io::Read;
use std::fs::File;

use graphql_parser::parse_query;

fn roundtrip(filename: &str) {
    let mut buf = String::with_capacity(1024);
    let path = format!("tests/queries/{}.graphql", filename);
    let mut f = File::open(&path).unwrap();
    f.read_to_string(&mut buf).unwrap();
    let ast = parse_query::<String>(&buf).unwrap().to_owned();
    assert_eq!(ast.to_string(), buf);
}

fn roundtrip2(filename: &str) {
    let mut buf = String::with_capacity(1024);
    let source = format!("tests/queries/{}.graphql", filename);
    let target = format!("tests/queries/{}_canonical.graphql", filename);
    let mut f = File::open(&source).unwrap();
    f.read_to_string(&mut buf).unwrap();
    let ast = parse_query::<String>(&buf).unwrap().to_owned();

    let mut buf = String::with_capacity(1024);
    let mut f = File::open(&target).unwrap();
    f.read_to_string(&mut buf).unwrap();
    assert_eq!(ast.to_string(), buf);
}

#[test] fn minimal() { roundtrip("minimal"); }
#[test] fn minimal_query() { roundtrip("minimal_query"); }
#[test] fn named_query() { roundtrip("named_query"); }
#[test] fn query_vars() { roundtrip("query_vars"); }
#[test] fn query_var_defaults() { roundtrip("query_var_defaults"); }
#[test] fn query_var_defaults1() { roundtrip("query_var_default_string"); }
#[test] fn query_var_defaults2() { roundtrip("query_var_default_float"); }
#[test] fn query_var_defaults3() { roundtrip("query_var_default_list"); }
#[test] fn query_var_defaults4() { roundtrip("query_var_default_object"); }
#[test] fn query_aliases() { roundtrip("query_aliases"); }
#[test] fn query_arguments() { roundtrip("query_arguments"); }
#[test] fn query_directive() { roundtrip("query_directive"); }
#[test] fn mutation_directive() { roundtrip("mutation_directive"); }
#[test] fn subscription_directive() { roundtrip("subscription_directive"); }
#[test] fn string_literal() { roundtrip("string_literal"); }
#[test] fn triple_quoted_literal() { roundtrip("triple_quoted_literal"); }
#[test] fn query_list_arg() { roundtrip("query_list_argument"); }
#[test] fn query_object_arg() { roundtrip("query_object_argument"); }
#[test] fn nested_selection() { roundtrip("nested_selection"); }
#[test] fn inline_fragment() { roundtrip("inline_fragment"); }
#[test] fn inline_fragment_dir() { roundtrip("inline_fragment_dir"); }
#[test] fn fragment_spread() { roundtrip("fragment_spread"); }
#[test] fn minimal_mutation() { roundtrip("minimal_mutation"); }
#[test] fn fragment() { roundtrip("fragment"); }
#[test] fn directive_args() { roundtrip("directive_args"); }
#[test] fn kitchen_sink() { roundtrip2("kitchen-sink"); }
