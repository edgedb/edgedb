use std::collections::HashMap;

use gel_jwt::{Key, KeyType, SigningContext};

#[divan::bench(args = [&KeyType::ES256, &KeyType::RS256, &KeyType::HS256])]
fn bench_jwt_signing(b: divan::Bencher, key_type: &KeyType) {
    let key = Key::generate(None, *key_type).unwrap();
    let claims = HashMap::from([("sub".to_string(), "test".to_string())]);
    let ctx = SigningContext::default();

    b.bench_local(move || {
        key.sign(&claims, &ctx)
    });
}

#[divan::bench(args = [&KeyType::ES256, &KeyType::RS256, &KeyType::HS256])]
fn bench_jwt_validation(b: divan::Bencher, key_type: &KeyType) {
    let key = Key::generate(None, *key_type).unwrap();
    let claims = HashMap::from([("sub".to_string(), "test".to_string())]);
    let ctx = SigningContext::default();
    let token = key.sign(&claims, &ctx).unwrap();

    b.bench_local(move || {
        key.validate(&token, &ctx)
    });
}

#[divan::bench(args = [&KeyType::ES256, &KeyType::RS256, &KeyType::HS256])]
fn bench_jwt_encode(b: divan::Bencher, key_type: &KeyType) {
    let key = Key::generate(None, *key_type).unwrap();

    b.bench_local(move || {
        let claims = HashMap::from([("sub".to_string(), "test".to_string())]);
        let ctx = SigningContext::default();
        key.sign(&claims, &ctx)
    });
}

fn main() {
    // Run registered benchmarks.
    divan::main();
}
