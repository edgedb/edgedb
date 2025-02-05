use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    time::Duration,
};

use bare_key::{BareKey, BarePublicKey};
use jsonwebtoken::{Algorithm, Header, Validation};
use serde::{Deserialize, Serialize};
use thiserror::Error;

mod bare_key;

#[derive(Error, Debug)]
pub enum KeyError {
    #[error("Invalid PEM format")]
    InvalidPem,
    #[error("Unsupported key type: {0}")]
    UnsupportedKeyType(String),
    #[error("Invalid EC key parameters")]
    InvalidEcParameters,
    #[error("Failed to decode key")]
    DecodeError,
    #[error("Failed to encode key")]
    EncodeError,
    #[error("Failed to validate key pair: {0:?}")]
    KeyValidationError(#[from] KeyValidationError),
}

#[derive(Error, Debug)]
#[error(transparent)]
pub struct KeyValidationError(#[from] ring::error::KeyRejected);

trait IsKey {
    type Inner: std::hash::Hash + Eq + Debug;

    fn inner(&self) -> &Self::Inner;
    fn into_inner(self) -> (Option<String>, Self::Inner);
}

impl IsKey for Key {
    type Inner = KeyInner;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    fn into_inner(self) -> (Option<String>, Self::Inner) {
        (self.kid, self.inner)
    }
}

impl IsKey for PublicKey {
    type Inner = PublicKeyInner;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    fn into_inner(self) -> (Option<String>, Self::Inner) {
        (self.kid, self.inner)
    }
}

#[allow(private_bounds)]
pub struct KeyRegistry<K: IsKey> {
    named_keys: HashMap<String, K::Inner>,
    unnamed_keys: HashSet<K::Inner>,
}

impl<K: IsKey> Default for KeyRegistry<K> {
    fn default() -> Self {
        Self {
            named_keys: HashMap::default(),
            unnamed_keys: HashSet::default(),
        }
    }
}

#[allow(private_bounds)]
impl<K: IsKey> KeyRegistry<K> {
    /// Add a key to the registry. If the key already exists, it will be
    /// replaced. If the key specifies the same kid as another key already
    /// added, the new key will replace the old one.
    pub fn add_key(&mut self, key: K) {
        let (kid, inner) = key.into_inner();

        if let Some(kid) = kid {
            self.unnamed_keys.remove(&inner);
            self.named_keys.insert(kid, inner);
        } else {
            self.named_keys.retain(|_, v| v != &inner);
            self.unnamed_keys.insert(inner);
        }
    }

    pub fn remove_key(&mut self, key: &K) {
        let inner = key.inner();
        self.named_keys.retain(|_, v| v != inner);
        self.unnamed_keys.remove(inner);
    }

    pub fn remove_kid(&mut self, kid: &str) {
        self.named_keys.remove(kid);
    }

    /// Get the number of keys in the registry.
    pub fn len(&self) -> usize {
        self.named_keys.len() + self.unnamed_keys.len()
    }

    /// Check if the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.named_keys.is_empty() && self.unnamed_keys.is_empty()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyType {
    RS256,
    ES256,
    HS256,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SigningContext {
    pub expiry: Option<Duration>,
    pub issuer: Option<String>,
    pub audience: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct Token {
    #[serde(rename = "kid", default, skip_serializing_if = "Option::is_none")]
    pub key_id: Option<String>,
    #[serde(rename = "exp", default, skip_serializing_if = "Option::is_none")]
    pub expiry: Option<usize>,
    #[serde(rename = "iss", default, skip_serializing_if = "Option::is_none")]
    pub issuer: Option<String>,
    #[serde(rename = "aud", default, skip_serializing_if = "Option::is_none")]
    pub audience: Option<String>,
    #[serde(flatten)]
    claims: HashMap<String, String>,
}

impl Default for SigningContext {
    fn default() -> Self {
        Self {
            expiry: None,
            issuer: None,
            audience: None,
        }
    }
}

pub struct Key {
    key_type: KeyType,
    kid: Option<String>,
    inner: KeyInner,
}

impl Key {
    pub fn from_bare_key(kid: Option<String>, key: BareKey) -> Result<Self, KeyError> {
        let key_type = key.key_type();
        let encoding_key = (&key).try_into()?;
        let inner = KeyInner {
            bare_key: key,
            encoding_key,
        };
        Ok(Self {
            key_type,
            kid,
            inner,
        })
    }

    pub fn generate(kid: Option<String>, kty: KeyType) -> Result<Self, KeyError> {
        let key = BareKey::generate(kty)?;
        Self::from_bare_key(kid, key)
    }

    fn header(&self) -> Header {
        let mut header = Header::default();
        match self.key_type {
            KeyType::HS256 => {}
            KeyType::ES256 => header.alg = jsonwebtoken::Algorithm::ES256,
            KeyType::RS256 => header.alg = jsonwebtoken::Algorithm::RS256,
        }
        header
    }

    pub fn sign(
        &self,
        claims: &HashMap<String, String>,
        ctx: &SigningContext,
    ) -> Result<String, String> {
        let header = self.header();

        let token = Token {
            key_id: self.kid.clone(),
            expiry: ctx.expiry.map(|d| {
                (std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    + d)
                    .as_secs() as usize
            }),
            issuer: ctx.issuer.clone(),
            audience: ctx.audience.clone(),
            claims: claims.clone(),
        };
        Ok(jsonwebtoken::encode(&header, &token, &self.inner.encoding_key).unwrap())
    }

    pub fn validate(
        &self,
        token: &str,
        ctx: &SigningContext,
    ) -> Result<HashMap<String, String>, String> {
        let public = self.inner.bare_key.to_public().map_err(|e| e.to_string())?;
        let public_key =
            PublicKey::from_bare_public_key(self.kid.clone(), public).map_err(|e| e.to_string())?;
        public_key.validate(token, ctx)
    }
}

struct KeyInner {
    bare_key: BareKey,
    encoding_key: jsonwebtoken::EncodingKey,
}

impl std::hash::Hash for KeyInner {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.bare_key.hash(state);
    }
}

impl PartialEq for KeyInner {
    fn eq(&self, other: &Self) -> bool {
        self.bare_key == other.bare_key
    }
}

impl std::fmt::Debug for KeyInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.bare_key.fmt(f)
    }
}

impl Eq for KeyInner {}

#[derive(Debug, PartialEq, Eq)]
pub struct PublicKey {
    kid: Option<String>,
    inner: PublicKeyInner,
}

impl PublicKey {
    pub fn from_bare_public_key(kid: Option<String>, key: BarePublicKey) -> Result<Self, KeyError> {
        let decoding_key: jsonwebtoken::DecodingKey = (&key).try_into()?;
        let inner = PublicKeyInner {
            decoding_key,
            bare_key: key,
        };
        Ok(Self { kid, inner })
    }

    pub fn validate(
        &self,
        token: &str,
        ctx: &SigningContext,
    ) -> Result<HashMap<String, String>, String> {
        let mut validation = Validation::new(match self.inner.bare_key.key_type() {
            KeyType::ES256 => Algorithm::ES256,
            KeyType::HS256 => Algorithm::HS256,
            KeyType::RS256 => Algorithm::RS256,
        });
        if ctx.expiry.is_none() {
            validation.required_spec_claims.remove("exp");
        }
        if let Some(issuer) = ctx.issuer.as_ref() {
            validation.iss = Some(HashSet::from([issuer.to_string()]));
        } else {
            validation.required_spec_claims.remove("iss");
        }
        if let Some(audience) = ctx.audience.as_ref() {
            validation.aud = Some(HashSet::from([audience.to_string()]));
        } else {
            validation.required_spec_claims.remove("aud");
        }
        let token_data =
            jsonwebtoken::decode::<Token>(token, &self.inner.decoding_key, &validation)
                .map_err(|e| e.to_string())?;
        Ok(token_data.claims.claims)
    }
}

pub struct PublicKeyInner {
    bare_key: BarePublicKey,
    decoding_key: jsonwebtoken::DecodingKey,
}

impl std::fmt::Debug for PublicKeyInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.bare_key.fmt(f)
    }
}

impl std::hash::Hash for PublicKeyInner {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.bare_key.hash(state);
    }
}

impl PartialEq for PublicKeyInner {
    fn eq(&self, other: &Self) -> bool {
        self.bare_key == other.bare_key
    }
}

impl Eq for PublicKeyInner {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_registry() {
        let mut registry = KeyRegistry::default();
        registry.add_key(Key::generate(Some("1".to_owned()), KeyType::HS256).unwrap());
        registry.add_key(Key::generate(Some("2".to_owned()), KeyType::HS256).unwrap());
        registry.add_key(Key::generate(Some("3".to_owned()), KeyType::HS256).unwrap());
        assert_eq!(registry.len(), 3);
        assert!(!registry.is_empty());
    }

    #[test]
    fn test_sign() {
        let key = Key::generate(Some("1".to_owned()), KeyType::HS256).unwrap();
        let claims = HashMap::from([("hello".to_owned(), "world".to_owned())]);
        let signing_ctx = SigningContext {
            expiry: Some(Duration::from_secs(10)),
            issuer: Some("issuer".to_owned()),
            audience: Some("audience".to_owned()),
        };
        let token = key.sign(&claims, &signing_ctx).unwrap();
        println!("token: {}", token);
        let decoded = key.validate(&token, &signing_ctx).unwrap();
        assert_eq!(decoded, claims);
    }

    #[test]
    fn test_sign_no_expiry() {
        let key = Key::generate(Some("1".to_owned()), KeyType::HS256).unwrap();
        let claims = HashMap::from([("hello".to_owned(), "world".to_owned())]);
        let signing_ctx = SigningContext {
            expiry: None,
            issuer: Some("issuer".to_owned()),
            audience: Some("audience".to_owned()),
        };
        let token = key.sign(&claims, &signing_ctx).unwrap();
        let decoded = key.validate(&token, &signing_ctx).unwrap();
        assert_eq!(decoded, claims);
    }
}
