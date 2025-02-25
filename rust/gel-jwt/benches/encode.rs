use std::collections::HashMap;

use gel_jwt::{KeyType, PrivateKey, SigningContext, ValidationContext};

const KEY_TYPES: &[KeyType] = &[KeyType::ES256, KeyType::RS256, KeyType::HS256];

#[divan::bench(args = KEY_TYPES)]
fn bench_jwt_signing(b: divan::Bencher, key_type: KeyType) {
    let key = PrivateKey::generate(None, key_type).unwrap();
    let claims = HashMap::from([("sub".to_string(), "test".into())]);
    let ctx = SigningContext::default();

    b.bench_local(move || key.sign(claims.clone(), &ctx));
}

#[divan::bench(args = KEY_TYPES)]
fn bench_jwt_validation(b: divan::Bencher, key_type: KeyType) {
    let key = PrivateKey::generate(None, key_type).unwrap();
    let claims = HashMap::from([("sub".to_string(), "test".into())]);
    let ctx = SigningContext::default();
    let token = key.sign(claims, &ctx).unwrap();
    let ctx = ValidationContext::default();

    b.bench_local(move || key.validate(&token, &ctx));
}

#[divan::bench(args = KEY_TYPES)]
fn bench_jwt_encode(b: divan::Bencher, key_type: KeyType) {
    let key = PrivateKey::generate(None, key_type).unwrap();

    b.bench_local(move || {
        let claims = HashMap::from([("sub".to_string(), "test".into())]);
        let ctx = SigningContext::default();
        key.sign(claims, &ctx).unwrap()
    });
}

fn main() {
    // Run registered benchmarks.
    divan::main();
}
