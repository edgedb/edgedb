use criterion::{criterion_group, criterion_main, Criterion};

use edgeql_parser::parser::{self, Terminal};
use edgeql_parser::tokenizer::{self, Error};

const CONTENT: &str = include_str!("../../lib/std/25-numoperators.edgeql");

fn criterion_benchmark(c: &mut Criterion) {
    let tokens = tokenizer::Tokenizer::new(CONTENT)
        .validated_values()
        .with_eof()
        .map(|r| r.map(Terminal::from_token))
        .collect::<Result<Vec<_>, Error>>()
        .unwrap();

    let parser_spec_file = "/home/aljaz/EdgeDB/edgedb/EdgeQLBlockSpec";
    let spec_json = std::fs::read_to_string(parser_spec_file).unwrap();
    let spec = parser::Spec::from_json(&spec_json).unwrap();

    c.bench_function("parse-numoperators", |b| {
        b.iter(|| parser::parse(&spec, tokens.clone()))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
