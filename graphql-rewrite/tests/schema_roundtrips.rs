extern crate graphql_parser;
#[cfg(test)] #[macro_use] extern crate pretty_assertions;

use std::io::Read;
use std::fs::File;

use graphql_parser::parse_schema;

fn roundtrip(filename: &str) {
    let mut buf = String::with_capacity(1024);
    let path = format!("tests/schemas/{}.graphql", filename);
    let mut f = File::open(&path).unwrap();
    f.read_to_string(&mut buf).unwrap();
    let ast = parse_schema::<String>(&buf).unwrap().to_owned();
    assert_eq!(ast.to_string(), buf);
}

fn roundtrip2(filename: &str) {
    let mut buf = String::with_capacity(1024);
    let source = format!("tests/schemas/{}.graphql", filename);
    let target = format!("tests/schemas/{}_canonical.graphql", filename);
    let mut f = File::open(&source).unwrap();
    f.read_to_string(&mut buf).unwrap();
    let ast = parse_schema::<String>(&buf).unwrap();

    let mut buf = String::with_capacity(1024);
    let mut f = File::open(&target).unwrap();
    f.read_to_string(&mut buf).unwrap();
    assert_eq!(ast.to_string(), buf);
}

#[test] fn minimal() { roundtrip("minimal"); }
#[test] fn scalar_type() { roundtrip("scalar_type"); }
#[test] fn extend_scalar() { roundtrip("extend_scalar"); }
#[test] fn minimal_type() { roundtrip("minimal_type"); }
#[test] fn implements() { roundtrip("implements"); }
#[test] fn implements_amp() { roundtrip2("implements_amp"); }
#[test] fn simple_object() { roundtrip("simple_object"); }
#[test] fn extend_object() { roundtrip("extend_object"); }
#[test] fn interface() { roundtrip("interface"); }
#[test] fn extend_interface() { roundtrip("extend_interface"); }
#[test] fn union() { roundtrip("union"); }
#[test] fn empty_union() { roundtrip("empty_union"); }
#[test] fn union_extension() { roundtrip("union_extension"); }
#[test] fn enum_type() { roundtrip("enum"); }
#[test] fn extend_enum() { roundtrip("extend_enum"); }
#[test] fn input_type() { roundtrip("input_type"); }
#[test] fn extend_input() { roundtrip2("extend_input"); }
#[test] fn directive() { roundtrip("directive"); }
#[test] fn kitchen_sink() { roundtrip2("kitchen-sink"); }
#[test] fn directive_descriptions() { roundtrip2("directive_descriptions"); }
