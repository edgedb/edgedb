use jsonwebtoken::{Algorithm, Header, Validation};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Debug, sync::Arc};

use crate::{
    bare_key::{BareKeyInner, SerializedKey},
    registry::IsKey,
    Any, BareKey, BarePrivateKey, BarePublicKey, KeyError, OpaqueValidationFailureReason,
    SignatureError, SigningContext, ValidationContext, ValidationError, ValidationType,
};

#[derive(Clone, Copy, Debug, derive_more::Display, PartialEq, Eq)]
pub enum KeyType {
    RS256,
    ES256,
    HS256,
}

#[derive(Serialize, Deserialize)]
struct Token {
    #[serde(rename = "exp", default, skip_serializing_if = "Option::is_none")]
    pub expiry: Option<usize>,
    #[serde(rename = "iss", default, skip_serializing_if = "Option::is_none")]
    pub issuer: Option<String>,
    #[serde(rename = "aud", default, skip_serializing_if = "Option::is_none")]
    pub audience: Option<String>,
    #[serde(rename = "iat", default, skip_serializing_if = "Option::is_none")]
    pub issued_at: Option<usize>,
    #[serde(rename = "nbf", default, skip_serializing_if = "Option::is_none")]
    pub not_before: Option<usize>,
    #[serde(flatten)]
    claims: HashMap<String, Any>,
}

/// A private key with key-signing capabilities.
pub struct PrivateKey {
    pub(crate) kid: Option<String>,
    pub(crate) inner: Arc<PrivateKeyInner>,
}

impl PrivateKey {
    pub fn key_type(&self) -> KeyType {
        self.inner.bare_key.key_type()
    }

    pub fn set_kid(&mut self, kid: Option<String>) {
        self.kid = kid;
    }

    pub fn from_bare_private_key(
        kid: Option<String>,
        key: BarePrivateKey,
    ) -> Result<Self, KeyError> {
        let encoding_key = (&key).try_into()?;
        let decoding_key = (&key.to_public()?).try_into()?;
        let inner = PrivateKeyInner {
            bare_key: key,
            encoding_key,
            decoding_key,
        }
        .into();
        Ok(Self { kid, inner })
    }

    pub fn generate(kid: Option<String>, kty: KeyType) -> Result<Self, KeyError> {
        let key = BarePrivateKey::generate(kty)?;
        Self::from_bare_private_key(kid, key)
    }

    pub fn clone_key(&self) -> Self {
        Self {
            kid: self.kid.clone(),
            inner: self.inner.clone(),
        }
    }

    pub fn sign(
        &self,
        claims: HashMap<String, Any>,
        ctx: &SigningContext,
    ) -> Result<String, SignatureError> {
        sign_token(
            self.key_type(),
            &self.inner.encoding_key,
            self.kid.as_deref(),
            claims,
            ctx,
        )
    }

    pub fn validate(
        &self,
        token: &str,
        ctx: &ValidationContext,
    ) -> Result<HashMap<String, Any>, ValidationError> {
        validate_token(
            self.key_type(),
            &self.inner.decoding_key,
            self.kid.as_deref(),
            token,
            ctx,
        )
    }
}

impl IsKey for PrivateKey {
    type Inner = Arc<PrivateKeyInner>;

    fn key_type(inner: &Self::Inner) -> KeyType {
        inner.bare_key.key_type()
    }

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    fn from_inner(kid: Option<String>, inner: Self::Inner) -> Self {
        PrivateKey { kid, inner }
    }

    fn into_inner(self) -> (Option<String>, Self::Inner) {
        (self.kid, self.inner)
    }

    fn get_serialized_key(key: SerializedKey) -> Option<Self> {
        match key {
            SerializedKey::Private(kid, key) => {
                Some(PrivateKey::from_bare_private_key(kid, key).ok()?)
            }
            _ => None,
        }
    }

    fn to_serialized_key(kid: Option<&str>, key: &Self::Inner) -> SerializedKey {
        SerializedKey::Private(kid.map(String::from), key.bare_key.clone_key())
    }

    fn from_pem(pem: &str) -> Result<Vec<Result<Self, KeyError>>, KeyError> {
        BarePrivateKey::from_pem_multiple(pem).map(|keys| {
            keys.into_iter()
                .map(|k| k.and_then(|bare_key| PrivateKey::from_bare_private_key(None, bare_key)))
                .collect()
        })
    }

    fn to_pem(inner: &Self::Inner) -> String {
        inner.bare_key.to_pem()
    }

    fn decoding_key(inner: &Self::Inner) -> &jsonwebtoken::DecodingKey {
        &inner.decoding_key
    }

    fn encoding_key(inner: &Self::Inner) -> Option<&jsonwebtoken::EncodingKey> {
        Some(&inner.encoding_key)
    }
}

pub(crate) fn sign_token(
    key_type: KeyType,
    encoding_key: &jsonwebtoken::EncodingKey,
    kid: Option<&str>,
    claims: HashMap<String, Any>,
    ctx: &SigningContext,
) -> Result<String, SignatureError> {
    let mut header = Header {
        kid: kid.map(String::from),
        ..Default::default()
    };
    match key_type {
        KeyType::HS256 => {}
        KeyType::ES256 => header.alg = jsonwebtoken::Algorithm::ES256,
        KeyType::RS256 => header.alg = jsonwebtoken::Algorithm::RS256,
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as usize;

    let (issued_at, not_before) = if let Some(not_before) = ctx.not_before {
        (
            Some(now),
            Some(now.saturating_sub(not_before.as_secs() as usize)),
        )
    } else {
        (None, None)
    };

    let expiry = ctx.expiry.map(|d| d.as_secs() as isize);
    let expiry = if expiry == Some(0) {
        // Ensure that a token that expires now expires with enough notice for
        // the leeway option to be ignored. This isn't a great solution, but
        // it's challenging to test expiring tokens otherwise.
        Some(now.saturating_sub(120))
    } else {
        expiry.map(|d| now.saturating_add_signed(d))
    };

    let token = Token {
        expiry,
        issuer: ctx.issuer.clone(),
        audience: ctx.audience.clone(),
        issued_at,
        not_before,
        claims,
    };

    jsonwebtoken::encode(&header, &token, encoding_key)
        .map_err(|e| SignatureError::SignatureError(e.to_string()))
}

/// Returns the raw claims from the token, including those we may have added
/// as part of the signature process.
pub(crate) fn validate_token(
    key_type: KeyType,
    decoding_key: &jsonwebtoken::DecodingKey,
    kid: Option<&str>,
    token: &str,
    ctx: &ValidationContext,
) -> Result<HashMap<String, Any>, ValidationError> {
    let mut validation = Validation::new(match key_type {
        KeyType::ES256 => Algorithm::ES256,
        KeyType::HS256 => Algorithm::HS256,
        KeyType::RS256 => Algorithm::RS256,
    });

    validation.validate_aud = false;

    match ctx.expiry {
        ValidationType::Ignore => {
            validation.required_spec_claims.remove("exp");
            validation.validate_exp = false;
        }
        ValidationType::Allow => {
            validation.required_spec_claims.remove("exp");
            validation.validate_exp = true;
        }
        ValidationType::Reject => {
            validation.required_spec_claims.remove("exp");
            validation.validate_exp = false;
        }
        ValidationType::Require => {
            // The default
        }
    }

    match ctx.not_before {
        ValidationType::Ignore => {
            validation.validate_nbf = false;
        }
        ValidationType::Allow => {
            validation.validate_nbf = true;
        }
        ValidationType::Reject => {
            validation.validate_nbf = false;
        }
        ValidationType::Require => {
            validation.required_spec_claims.insert("nbf".to_string());
            validation.validate_nbf = true;
        }
    }

    let token = jsonwebtoken::decode::<HashMap<String, Any>>(token, decoding_key, &validation)
        .map_err(|e| match e.kind() {
            jsonwebtoken::errors::ErrorKind::InvalidSignature => {
                OpaqueValidationFailureReason::InvalidSignature
            }
            _ => OpaqueValidationFailureReason::Failure(format!("{:?}", e.kind())),
        })?;

    if let (Some(token_kid), Some(expected_kid)) = (token.header.kid, kid) {
        if token_kid != expected_kid {
            return Err(OpaqueValidationFailureReason::InvalidHeader(
                "kid".to_string(),
                token_kid,
                Some(expected_kid.to_string()),
            )
            .into());
        }
    }

    for (claim, values) in &ctx.allow_list {
        let value = token.claims.get(claim);
        match value {
            Some(Any::String(value)) => {
                if !values.contains(value.as_ref()) {
                    return Err(OpaqueValidationFailureReason::InvalidClaimValue(
                        claim.to_string(),
                        Some(value.to_string()),
                    )
                    .into());
                }
            }
            Some(Any::Array(array_values)) => {
                for v in array_values.iter() {
                    if let Any::String(v) = v {
                        if !values.contains(v.as_ref()) {
                            return Err(OpaqueValidationFailureReason::InvalidClaimValue(
                                claim.to_string(),
                                Some(v.to_string()),
                            )
                            .into());
                        }
                    } else {
                        return Err(OpaqueValidationFailureReason::InvalidClaimValue(
                            claim.to_string(),
                            None,
                        )
                        .into());
                    }
                }
            }
            _ => {
                return Err(OpaqueValidationFailureReason::InvalidClaimValue(
                    claim.to_string(),
                    None,
                )
                .into());
            }
        }
    }

    for (claim, values) in &ctx.deny_list {
        let value = token.claims.get(claim);
        match value {
            Some(Any::String(value)) => {
                if values.contains(value.as_ref()) {
                    return Err(OpaqueValidationFailureReason::InvalidClaimValue(
                        claim.to_string(),
                        Some(value.to_string()),
                    )
                    .into());
                }
            }
            Some(Any::Array(array_values)) => {
                for v in array_values.iter() {
                    if let Any::String(v) = v {
                        if values.contains(v.as_ref()) {
                            return Err(OpaqueValidationFailureReason::InvalidClaimValue(
                                claim.to_string(),
                                Some(v.to_string()),
                            )
                            .into());
                        }
                    } else {
                        return Err(OpaqueValidationFailureReason::InvalidClaimValue(
                            claim.to_string(),
                            None,
                        )
                        .into());
                    }
                }
            }
            _ => {
                return Err(OpaqueValidationFailureReason::InvalidClaimValue(
                    claim.to_string(),
                    None,
                )
                .into());
            }
        }
    }

    // Remove any claims that were validated automatically and reject any that should not
    // be present.
    let mut claims = token.claims;
    claims.remove("exp");
    for claim in ctx.claims.iter() {
        claims.remove(claim.0);
    }

    if ctx.expiry == ValidationType::Reject {
        if let Some(exp) = claims.remove("exp") {
            return Err(OpaqueValidationFailureReason::InvalidClaimValue(
                "exp".to_string(),
                Some(format!("{exp:?}")),
            )
            .into());
        }
    }
    if ctx.not_before == ValidationType::Reject {
        if let Some(nbf) = claims.remove("nbf") {
            return Err(OpaqueValidationFailureReason::InvalidClaimValue(
                "nbf".to_string(),
                Some(format!("{nbf:?}")),
            )
            .into());
        }
    }

    Ok(claims)
}

pub(crate) struct PrivateKeyInner {
    pub(crate) bare_key: BarePrivateKey,
    pub(crate) encoding_key: jsonwebtoken::EncodingKey,
    pub(crate) decoding_key: jsonwebtoken::DecodingKey,
}

impl std::hash::Hash for PrivateKeyInner {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.bare_key.hash(state);
    }
}

impl PartialEq for PrivateKeyInner {
    fn eq(&self, other: &Self) -> bool {
        self.bare_key == other.bare_key
    }
}

impl std::fmt::Debug for PrivateKeyInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.bare_key.fmt(f)
    }
}

impl Eq for PrivateKeyInner {}

/// A public key with key-validation capabilities.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublicKey {
    kid: Option<String>,
    inner: Arc<PublicKeyInner>,
}

impl PublicKey {
    pub fn key_type(&self) -> KeyType {
        self.inner.bare_key.key_type()
    }

    pub fn set_kid(&mut self, kid: Option<String>) {
        self.kid = kid;
    }

    pub fn from_bare_public_key(kid: Option<String>, key: BarePublicKey) -> Result<Self, KeyError> {
        let decoding_key: jsonwebtoken::DecodingKey = (&key).try_into()?;
        let inner = PublicKeyInner {
            decoding_key,
            bare_key: key,
        }
        .into();
        Ok(Self { kid, inner })
    }

    pub fn validate(
        &self,
        token: &str,
        ctx: &ValidationContext,
    ) -> Result<HashMap<String, Any>, ValidationError> {
        validate_token(
            self.key_type(),
            &self.inner.decoding_key,
            self.kid.as_deref(),
            token,
            ctx,
        )
    }
}

impl IsKey for PublicKey {
    type Inner = Arc<PublicKeyInner>;

    fn key_type(inner: &Self::Inner) -> KeyType {
        inner.bare_key.key_type()
    }

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    fn from_inner(kid: Option<String>, inner: Self::Inner) -> Self {
        PublicKey { kid, inner }
    }

    fn into_inner(self) -> (Option<String>, Self::Inner) {
        (self.kid, self.inner)
    }

    fn get_serialized_key(key: SerializedKey) -> Option<Self> {
        match key {
            SerializedKey::Private(kid, key) => {
                Some(PublicKey::from_bare_public_key(kid, key.to_public().ok()?).ok()?)
            }
            SerializedKey::Public(kid, key) => {
                Some(PublicKey::from_bare_public_key(kid, key).ok()?)
            }
            _ => None,
        }
    }

    fn to_serialized_key(kid: Option<&str>, key: &Self::Inner) -> SerializedKey {
        SerializedKey::Public(kid.map(String::from), key.bare_key.clone_key())
    }

    fn from_pem(pem: &str) -> Result<Vec<Result<Self, KeyError>>, KeyError> {
        BarePublicKey::from_pem_multiple(pem).map(|keys| {
            keys.into_iter()
                .map(|k| k.and_then(|bare_key| PublicKey::from_bare_public_key(None, bare_key)))
                .collect()
        })
    }

    fn to_pem(inner: &Self::Inner) -> String {
        inner.bare_key.to_pem()
    }

    fn decoding_key(inner: &Self::Inner) -> &jsonwebtoken::DecodingKey {
        &inner.decoding_key
    }

    fn encoding_key(_: &Self::Inner) -> Option<&jsonwebtoken::EncodingKey> {
        None
    }
}

pub struct PublicKeyInner {
    pub(crate) bare_key: BarePublicKey,
    pub(crate) decoding_key: jsonwebtoken::DecodingKey,
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

/// A key which is either a private or public key.
pub struct Key {
    kid: Option<String>,
    inner: KeyInner,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum KeyInner {
    Private(Arc<PrivateKeyInner>),
    Public(Arc<PublicKeyInner>),
}

impl KeyInner {}

impl Key {
    pub fn key_type(&self) -> KeyType {
        match &self.inner {
            KeyInner::Private(inner) => inner.bare_key.key_type(),
            KeyInner::Public(inner) => inner.bare_key.key_type(),
        }
    }

    pub fn from_bare_key(kid: Option<String>, key: BareKey) -> Result<Self, KeyError> {
        Ok(match key.inner {
            BareKeyInner::Private(inner) => {
                PrivateKey::from_bare_private_key(kid, BarePrivateKey { inner })?.into()
            }
            BareKeyInner::Public(inner) => {
                PublicKey::from_bare_public_key(kid, BarePublicKey { inner })?.into()
            }
        })
    }

    pub fn from_bare_private_key(
        kid: Option<String>,
        key: BarePrivateKey,
    ) -> Result<Self, KeyError> {
        Ok(PrivateKey::from_bare_private_key(kid, key)?.into())
    }

    pub fn from_bare_public_key(kid: Option<String>, key: BarePublicKey) -> Result<Self, KeyError> {
        Ok(PublicKey::from_bare_public_key(kid, key)?.into())
    }
}

impl From<PrivateKey> for Key {
    fn from(key: PrivateKey) -> Self {
        Key {
            kid: key.kid,
            inner: KeyInner::Private(key.inner),
        }
    }
}

impl From<PublicKey> for Key {
    fn from(key: PublicKey) -> Self {
        Key {
            kid: key.kid,
            inner: KeyInner::Public(key.inner),
        }
    }
}

impl IsKey for Key {
    type Inner = KeyInner;

    fn key_type(inner: &Self::Inner) -> KeyType {
        match inner {
            KeyInner::Private(inner) => inner.bare_key.key_type(),
            KeyInner::Public(inner) => inner.bare_key.key_type(),
        }
    }

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    fn from_inner(kid: Option<String>, inner: Self::Inner) -> Self {
        Key { kid, inner }
    }

    fn into_inner(self) -> (Option<String>, Self::Inner) {
        (self.kid, self.inner)
    }

    fn get_serialized_key(key: SerializedKey) -> Option<Self> {
        match key {
            SerializedKey::Private(kid, key) => {
                Some(PrivateKey::from_bare_private_key(kid, key).ok()?.into())
            }
            SerializedKey::Public(kid, key) => {
                Some(PublicKey::from_bare_public_key(kid, key).ok()?.into())
            }
            _ => None,
        }
    }

    fn to_serialized_key(kid: Option<&str>, key: &Self::Inner) -> SerializedKey {
        match key {
            KeyInner::Private(inner) => {
                SerializedKey::Private(kid.map(String::from), inner.bare_key.clone_key())
            }
            KeyInner::Public(inner) => {
                SerializedKey::Public(kid.map(String::from), inner.bare_key.clone_key())
            }
        }
    }

    fn from_pem(pem: &str) -> Result<Vec<Result<Self, KeyError>>, KeyError> {
        let keys = BareKey::from_pem_multiple(pem)?;
        let mut results = Vec::new();
        for key in keys {
            results.push(key.and_then(|key| Self::from_bare_key(None, key)));
        }
        Ok(results)
    }

    fn to_pem(inner: &Self::Inner) -> String {
        match inner {
            KeyInner::Private(inner) => inner.bare_key.to_pem(),
            KeyInner::Public(inner) => inner.bare_key.to_pem(),
        }
    }

    fn decoding_key(inner: &Self::Inner) -> &jsonwebtoken::DecodingKey {
        match inner {
            KeyInner::Private(inner) => &inner.decoding_key,
            KeyInner::Public(inner) => &inner.decoding_key,
        }
    }

    fn encoding_key(inner: &Self::Inner) -> Option<&jsonwebtoken::EncodingKey> {
        match inner {
            KeyInner::Private(inner) => Some(&inner.encoding_key),
            KeyInner::Public(_) => None,
        }
    }
}
