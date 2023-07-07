use criterion::{criterion_group, criterion_main, Criterion};

use edgeql_parser::parser::{self, Terminal};
use edgeql_parser::tokenizer::{self, Error};

// const CONTENT: &str = include_str!("../../lib/std/25-numoperators.edgeql");
const CONTENT: &str = r#"
SELECT (
    {1, 2, {}, {1, 3}},
    (false or true) and (false or false or 'asx' in {'hello', 'world', 'asx'}),
    str_upper('saxasxasxasxasxasxasx')
);
"#;

fn criterion_benchmark(c: &mut Criterion) {
    let parser_spec_file = "../../EdgeQLBlockSpec";
    let spec_json = std::fs::read_to_string(parser_spec_file).unwrap();
    let spec = parser::Spec::from_json(&spec_json).unwrap();

    c.bench_function("tokenize-parse", |b| {
        b.iter(|| {
            let tokens = tokenizer::Tokenizer::new(CONTENT)
                .validated_values()
                .with_eof()
                .map(|r| r.map(Terminal::from_token))
                .collect::<Result<Vec<_>, Error>>()
                .unwrap();

            let ctx = parser::Context::new(&spec);
            parser::parse(&tokens, &ctx);
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
