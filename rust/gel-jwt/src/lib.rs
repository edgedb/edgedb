#[cfg(feature = "python_extension")]
pub mod python;

use std::fmt::Debug;
use thiserror::Error;

mod bare_key;
mod key;
mod registry;
mod sig;

pub use bare_key::{BareKey, BarePrivateKey, BarePublicKey};
pub use key::{Key, KeyType, PrivateKey, PublicKey};
pub use registry::KeyRegistry;
pub use sig::{Any, SigningContext, ValidationContext, ValidationType};

#[derive(Error, Debug, Eq, PartialEq)]
pub enum ValidationError {
    /// The token format or signature was invalid
    #[error("Invalid token")]
    Invalid(OpaqueValidationFailureReason),
    /// The key is invalid
    #[error(transparent)]
    KeyError(#[from] KeyError),
}

impl ValidationError {
    /// Display an error not intended for the end-user as it may leak information about the keys
    /// and/or tokens.
    pub fn error_string_not_for_user(&self) -> String {
        match self {
            ValidationError::Invalid(OpaqueValidationFailureReason::Failure(s)) => {
                format!("Invalid token: {}", s)
            }
            ValidationError::Invalid(OpaqueValidationFailureReason::InvalidClaimValue(
                claim,
                value,
            )) => format!("Invalid claim value for {claim}: {value:?}"),
            ValidationError::Invalid(OpaqueValidationFailureReason::InvalidHeader(
                header,
                value,
                expected,
            )) => format!("Invalid header {header}: {value:?}, expected {expected:?}"),
            ValidationError::Invalid(OpaqueValidationFailureReason::NoAppropriateKey) => {
                "No appropriate key found".to_string()
            }
            ValidationError::Invalid(OpaqueValidationFailureReason::InvalidSignature) => {
                "Invalid signature".to_string()
            }
            ValidationError::KeyError(error) => format!("Key error: {}", error),
        }
    }
}

/// A reason for validation failure that is opaque to debugging or printing to avoid
/// leaking information about the token failure.
#[derive(Eq, PartialEq)]
pub enum OpaqueValidationFailureReason {
    NoAppropriateKey,
    InvalidSignature,
    InvalidClaimValue(String, Option<String>),
    InvalidHeader(String, String, Option<String>),
    Failure(String),
}

impl From<OpaqueValidationFailureReason> for ValidationError {
    fn from(reason: OpaqueValidationFailureReason) -> Self {
        ValidationError::Invalid(reason)
    }
}

impl std::fmt::Debug for OpaqueValidationFailureReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "...")
    }
}

#[derive(Error, Debug)]
pub enum SignatureError {
    /// The token format or signature was invalid
    #[error("Signature operation failed: {0}")]
    SignatureError(String),
    /// No appropriate key was found
    #[error("No appropriate signing key found")]
    NoAppropriateKey,
    /// The key is invalid
    #[error(transparent)]
    KeyError(#[from] KeyError),
}

#[derive(Error, Debug, Eq, PartialEq)]
pub enum KeyError {
    #[error("Invalid PEM format")]
    InvalidPem,
    #[error("Invalid JSON format")]
    InvalidJson,
    #[error("Unsupported key type: {0}")]
    UnsupportedKeyType(String),
    #[error("Invalid EC key parameters")]
    InvalidEcParameters,
    #[error("Failed to decode key")]
    DecodeError,
    #[error("Failed to encode key")]
    EncodeError,
    #[error("Failed to validate key pair: {0:?}")]
    KeyValidationError(KeyValidationError),
}

#[derive(Debug, Eq, PartialEq)]
pub struct KeyValidationError(String);

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, time::Duration};

    use super::*;

    #[test]
    fn test_key_registry_add_remove() {
        let mut registry = KeyRegistry::default();
        registry.add_key(PrivateKey::generate(Some("1".to_owned()), KeyType::HS256).unwrap());
        registry.add_key(PrivateKey::generate(Some("2".to_owned()), KeyType::HS256).unwrap());
        registry.add_key(PrivateKey::generate(Some("3".to_owned()), KeyType::HS256).unwrap());
        assert_eq!(registry.len(), 3);
        assert!(!registry.is_empty());
        assert!(registry.remove_kid("1"));
        assert_eq!(registry.len(), 2);
        assert!(!registry.remove_kid("1"));
        assert_eq!(registry.len(), 2);
        assert!(registry.remove_kid("2"));
        assert_eq!(registry.len(), 1);
        assert!(!registry.remove_kid("2"));
        assert_eq!(registry.len(), 1);
        assert!(registry.remove_kid("3"));
        assert_eq!(registry.len(), 0);
        assert!(!registry.remove_kid("3"));
    }

    #[test]
    fn test_key_registry_re_add() {
        let mut registry = KeyRegistry::default();
        let key = PrivateKey::generate(Some("1".to_owned()), KeyType::HS256).unwrap();

        registry.add_key(key.clone_key());
        assert_eq!(registry.len(), 1);
        registry.add_key(key.clone_key());
        assert_eq!(registry.len(), 1);
        registry.remove_kid("1");
        assert_eq!(registry.len(), 0);
        registry.add_key(key);
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn test_key_registry_add_dupe_kid() {
        let mut registry = KeyRegistry::default();
        let key = PrivateKey::generate(Some("1".to_owned()), KeyType::HS256).unwrap();
        registry.add_key(key.clone_key());
        assert_eq!(registry.len(), 1);
        registry.add_key(key.clone_key());
        assert_eq!(registry.len(), 1);

        let key2 = PrivateKey::generate(Some("1".to_owned()), KeyType::RS256).unwrap();
        registry.add_key(key2.clone_key());
        assert_eq!(registry.len(), 1);
        registry.add_key(key2.clone_key());
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn test_sign() {
        let key = PrivateKey::generate(Some("1".to_owned()), KeyType::HS256).unwrap();
        let claims = HashMap::from([("hello".to_owned(), "world".into())]);
        let signing_ctx = SigningContext {
            expiry: Some(Duration::from_secs(600)),
            issuer: Some("issuer".to_owned()),
            audience: Some("audience".to_owned()),
            ..Default::default()
        };
        let mut validation_ctx = ValidationContext::default();
        validation_ctx.require_claim("aud");
        validation_ctx.require_claim_with_allow_list("iss", &["issuer"]);

        let token = key.sign(claims.clone(), &signing_ctx).unwrap();
        println!("token: {}", token);
        let decoded = key.validate(&token, &validation_ctx).unwrap();
        assert_eq!(decoded, claims);
    }

    #[test]
    fn test_sign_no_expiry() {
        let key = PrivateKey::generate(Some("1".to_owned()), KeyType::HS256).unwrap();
        let claims = HashMap::from([("hello".to_owned(), "world".into())]);
        let signing_ctx = SigningContext {
            issuer: Some("issuer".to_owned()),
            audience: Some("audience".to_owned()),
            ..Default::default()
        };
        let token = key.sign(claims.clone(), &signing_ctx).unwrap();
        let mut validation_ctx = ValidationContext::default();
        validation_ctx.require_claim("aud");
        validation_ctx.require_claim_with_allow_list("iss", &["issuer"]);
        let decoded = key
            .validate(&token, &validation_ctx)
            .map_err(|e| e.error_string_not_for_user())
            .unwrap();
        assert_eq!(decoded, claims);
    }

    #[test]
    fn load_from_empty() {
        let mut registry = KeyRegistry::<PrivateKey>::default();
        let added = registry.add_from_any("").unwrap();
        assert_eq!(added, 0);
        registry.add_from_pem("").unwrap();
        assert_eq!(added, 0);
        registry.add_from_jwkset("{\"keys\":[]}").unwrap();
        assert_eq!(added, 0);
    }

    #[test]
    fn test_google_jwkset() {
        let mut registry = KeyRegistry::<Key>::default();
        let added = registry
            .add_from_jwkset(include_str!("testcases/jwkset-goog.json"))
            .unwrap();
        assert_eq!(added, 2);
    }

    #[test]
    fn test_microsoft_jwkset() {
        let mut registry = KeyRegistry::<Key>::default();
        let added = registry
            .add_from_jwkset(include_str!("testcases/jwkset-msft.json"))
            .unwrap();
        assert_eq!(added, 9);
    }

    #[test]
    fn test_slack_jwkset() {
        let mut registry = KeyRegistry::<Key>::default();
        let added = registry
            .add_from_jwkset(include_str!("testcases/jwkset-slck.json"))
            .unwrap();
        assert_eq!(added, 1);
    }

    #[test]
    fn test_apple_jwkset() {
        let mut registry = KeyRegistry::<Key>::default();
        let added = registry
            .add_from_jwkset(include_str!("testcases/jwkset-aapl.json"))
            .unwrap();
        assert_eq!(added, 3);
    }

    #[test]
    fn load_keys_from_jwkset() {
        let mut registry = KeyRegistry::<PrivateKey>::default();
        let added = registry
            .add_from_jwkset(include_str!("testcases/jwkset-pub.json"))
            .unwrap();
        assert_eq!(added, 0);
        let mut registry = KeyRegistry::<PrivateKey>::default();
        let added = registry
            .add_from_jwkset(include_str!("testcases/jwkset-prv.json"))
            .unwrap();
        assert_eq!(added, 3);
    }

    #[test]
    fn load_pub_keys_from_jwkset() {
        let mut registry = KeyRegistry::<PublicKey>::default();
        let added = registry
            .add_from_jwkset(include_str!("testcases/jwkset-pub.json"))
            .unwrap();
        assert_eq!(added, 2);
        let mut registry = KeyRegistry::<PublicKey>::default();
        let added = registry
            .add_from_jwkset(include_str!("testcases/jwkset-prv.json"))
            .unwrap();
        assert_eq!(added, 3);
    }

    #[test]
    fn validate_tokens_from_jwkset() {
        let mut registry = KeyRegistry::<PrivateKey>::default();
        registry
            .add_from_jwkset(include_str!("testcases/jwkset-prv.json"))
            .unwrap();
        let keys = registry.into_keys().collect::<Vec<_>>();

        let mut registry = KeyRegistry::<PrivateKey>::default();
        registry
            .add_from_jwkset(include_str!("testcases/jwkset-prv.json"))
            .unwrap();

        let claims = HashMap::from([("test".to_owned(), "value".into())]);
        let signing_ctx = SigningContext {
            issuer: Some("test-issuer".to_owned()),
            audience: Some("test-audience".to_owned()),
            ..Default::default()
        };
        let mut validation_ctx = ValidationContext::default();
        validation_ctx.require_claim_with_allow_list("iss", &["test-issuer"]);
        validation_ctx.require_claim_with_allow_list("aud", &["test-audience"]);

        // Generate and validate a token with each key
        for key in &keys {
            let token = key.sign(claims.clone(), &signing_ctx).unwrap();
            let decoded = registry.validate(&token, &validation_ctx).unwrap();
            assert_eq!(decoded, claims);
        }

        // Generate and validate a token with each key against the public keys
        let mut registry = KeyRegistry::<PublicKey>::default();
        registry
            .add_from_jwkset(include_str!("testcases/jwkset-prv.json"))
            .unwrap();
        for key in &keys {
            let token = key.sign(claims.clone(), &signing_ctx).unwrap();
            let decoded = registry.validate(&token, &validation_ctx).unwrap();
            assert_eq!(decoded, claims);
        }
    }

    #[test]
    fn test_validate_tokens_from_jwkset_named() {
        let mut key1 = PrivateKey::generate(Some("1".to_owned()), KeyType::HS256).unwrap();
        let mut key2 = PrivateKey::generate(Some("2".to_owned()), KeyType::HS256).unwrap();

        let claims = HashMap::from([("test".to_owned(), "value".into())]);
        let signing_ctx = SigningContext {
            issuer: Some("test-issuer".to_owned()),
            audience: Some("test-audience".to_owned()),
            ..Default::default()
        };
        let validation_ctx = ValidationContext::default();
        let token = key1.sign(claims, &signing_ctx).unwrap();

        // Swap the keys so the signature is no longer valid with the specified kid
        key1.set_kid(Some("2".to_owned()));
        key2.set_kid(Some("1".to_owned()));

        let mut registry = KeyRegistry::<PrivateKey>::default();
        registry.add_key(key1);
        registry.add_key(key2);

        let decoded = registry.validate(&token, &validation_ctx).unwrap_err();
        assert_eq!(
            decoded,
            ValidationError::Invalid(OpaqueValidationFailureReason::InvalidSignature),
            "{}",
            decoded.error_string_not_for_user()
        );
    }

    #[test]
    fn test_validate_tokens_from_jwkset_named_allow_deny() {
        let key = PrivateKey::generate(Some("1".to_owned()), KeyType::HS256).unwrap();
        let mut registry = KeyRegistry::<PrivateKey>::default();
        registry.add_key(key);

        let claims = HashMap::from([("jti".to_owned(), "1234".into())]);
        let signing_ctx = SigningContext::default();
        let mut validation_ctx = ValidationContext::default();
        let token = registry.sign(claims.clone(), &signing_ctx).unwrap();

        // With no claim validation, the token should be valid
        let res = registry.validate(&token, &validation_ctx);
        assert!(
            res.is_ok(),
            "{}",
            res.unwrap_err().error_string_not_for_user()
        );

        validation_ctx.require_claim_with_allow_list("jti", &["1234"]);
        let decoded = registry.validate(&token, &validation_ctx).unwrap();
        assert_eq!(decoded, Default::default());

        let claims = HashMap::from([("jti".to_owned(), "bad".into())]);
        let token = registry.sign(claims, &signing_ctx).unwrap();
        let decoded = registry.validate(&token, &validation_ctx).unwrap_err();
        assert_eq!(
            decoded,
            ValidationError::Invalid(OpaqueValidationFailureReason::InvalidClaimValue(
                "jti".to_string(),
                Some("bad".to_string())
            ))
        );

        validation_ctx.require_claim_with_deny_list("jti", &["bad"]);
        let decoded = registry.validate(&token, &validation_ctx).unwrap_err();
        assert_eq!(
            decoded,
            ValidationError::Invalid(OpaqueValidationFailureReason::InvalidClaimValue(
                "jti".to_string(),
                Some("bad".to_string())
            ))
        );
    }

    #[test]
    fn test_any_json() {
        let map: HashMap<String, Any> = HashMap::from([
            ("hello".to_owned(), "world".into()),
            ("empty".to_owned(), Any::None),
            ("bool".to_owned(), Any::Bool(true)),
            ("number".to_owned(), Any::Number(123)),
            (
                "array".to_owned(),
                Any::Array(vec![Any::String("1".into()), Any::String("2".into())]),
            ),
        ]);
        let json = serde_json::to_string(&map).unwrap();
        assert!(json.contains("\"hello\":\"world\""));
        assert!(json.contains("\"empty\":null"));
        assert!(json.contains("\"bool\":true"));
        assert!(json.contains("\"number\":123"));
        assert!(json.contains("\"array\":[\"1\",\"2\"]"));
    }
}
