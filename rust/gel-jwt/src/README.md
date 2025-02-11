# JWT support

This crate provides support for JWT tokens. The JWT signing and verification is done
using the `jsonwebtoken` crate, while the key loading is performed here via the
`rsa`/`p256` crates.

## Key types

HS256: symmetric key
RS256: asymmetric key (RSA 2048+ + SHA256)
ES256: asymmetric key (P-256 + SHA256)

## Supported key formats

HS256: raw data
RS256: PKCS1/PKCS8 PEM
ES256: SEC1/PKCS8 PEM

