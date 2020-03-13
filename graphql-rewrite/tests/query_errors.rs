extern crate graphql_parser;
#[cfg(test)] #[macro_use] extern crate pretty_assertions;

use std::io::Read;
use std::fs::File;

use graphql_parser::parse_query;

fn test_error(filename: &str) {
    let mut buf = String::with_capacity(1024);
    let path = format!("tests/query_errors/{}.txt", filename);
    let mut f = File::open(&path).unwrap();
    f.read_to_string(&mut buf).unwrap();
    let mut iter = buf.splitn(2, "\n---\n");
    let graphql = iter.next().unwrap();
    let expected = iter.next().expect("file should contain error message");
    let err = parse_query::<String>(graphql).unwrap_err();
    assert_eq!(err.to_string(), expected);
}

#[test] fn invalid_curly_brace() { test_error("invalid_curly_brace"); }
#[test] fn bad_args() { test_error("bad_args"); }
