use base64ct::Encoding;
use const_oid::db::rfc5912::{ID_EC_PUBLIC_KEY, RSA_ENCRYPTION, SECP_256_R_1};
use der::{asn1::BitString, Any, AnyRef, Decode, Encode, SliceReader};
use elliptic_curve::{generic_array::GenericArray, sec1::FromEncodedPoint};
use p256::elliptic_curve::{sec1::ToEncodedPoint, JwkEcKey};
use pem::Pem;
use pkcs1::{DecodeRsaPrivateKey, UintRef};
use pkcs8::{
    spki::{AlgorithmIdentifier, SubjectPublicKeyInfoOwned},
    PrivateKeyInfo,
};
use rand::{rngs::ThreadRng, Rng};
use ring::{
    rand::SystemRandom,
    signature::{RsaKeyPair, ECDSA_P256_SHA256_FIXED_SIGNING},
};
use rsa::{
    pkcs1::EncodeRsaPrivateKey,
    traits::{PrivateKeyParts, PublicKeyParts},
    BigUint, RsaPrivateKey,
};
use rustls_pki_types::PrivatePkcs1KeyDer;
use sec1::{EcParameters, EcPrivateKey};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, str::FromStr, vec::Vec};

use crate::{KeyError, KeyType, KeyValidationError};

const MIN_OCT_LEN_BYTES: usize = 16;
const DEFAULT_GEN_OCT_LEN_BYTES: usize = 32;
const MIN_RSA_KEY_BITS: usize = 2048;
const DEFAULT_GEN_RSA_KEY_BITS: usize = 2048;

#[derive(zeroize::ZeroizeOnDrop, Eq, PartialEq, Clone)]
pub(crate) struct HmacKey {
    key: zeroize::Zeroizing<Vec<u8>>,
}

impl std::hash::Hash for HmacKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.key.hash(state);
    }
}

#[derive(derive_more::Debug, Serialize, Deserialize)]
pub struct SerializedKeys {
    pub keys: Vec<SerializedKey>,
}

/// Deserialize
#[derive(derive_more::Debug)]
pub enum SerializedKey {
    Private(Option<String>, BarePrivateKey),
    Public(Option<String>, BarePublicKey),
    #[debug("UnknownOrInvalid({_0}, {_0}, ...)")]
    UnknownOrInvalid(
        #[allow(unused)] KeyError,
        String,
        HashMap<String, serde_json::Value>,
    ),
}

impl<'de> serde::Deserialize<'de> for SerializedKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let map: HashMap<String, serde_json::Value> = HashMap::deserialize(deserializer)?;
        let get = |k: &'static str| {
            map.get(k)
                .map(|s| s.as_str().unwrap_or_default())
                .unwrap_or_default()
        };

        let kty = get("kty");
        let kid = map
            .get("kid")
            .map(|v| v.as_str().unwrap_or_default().to_owned());

        match kty {
            "RSA" => {
                // Check if private key by looking for p,q components
                if map.contains_key("p") && map.contains_key("q") {
                    // Private key
                    match BarePrivateKey::from_jwt_rsa(
                        get("n"),
                        get("e"),
                        get("d"),
                        get("p"),
                        get("q"),
                    ) {
                        Ok(key) => Ok(SerializedKey::Private(kid, key)),
                        Err(e) => Ok(SerializedKey::UnknownOrInvalid(e, kty.to_string(), map)),
                    }
                } else {
                    // Public key
                    match BarePublicKey::from_jwt_rsa(get("n"), get("e")) {
                        Ok(key) => Ok(SerializedKey::Public(kid, key)),
                        Err(e) => Ok(SerializedKey::UnknownOrInvalid(e, kty.to_string(), map)),
                    }
                }
            }
            "EC" => {
                // Check if private key by looking for d component
                if map.contains_key("d") {
                    // Private key
                    match BarePrivateKey::from_jwt_ec(get("crv"), get("d"), get("x"), get("y")) {
                        Ok(key) => Ok(SerializedKey::Private(kid, key)),
                        Err(e) => Ok(SerializedKey::UnknownOrInvalid(e, kty.to_string(), map)),
                    }
                } else {
                    // Public key
                    match BarePublicKey::from_jwt_ec(get("crv"), get("x"), get("y")) {
                        Ok(key) => Ok(SerializedKey::Public(kid, key)),
                        Err(e) => Ok(SerializedKey::UnknownOrInvalid(e, kty.to_string(), map)),
                    }
                }
            }
            "oct" => match BarePrivateKey::from_jwt_oct(get("k")) {
                Ok(key) => Ok(SerializedKey::Private(kid, key)),
                Err(e) => Ok(SerializedKey::UnknownOrInvalid(e, kty.to_string(), map)),
            },
            _ => Ok(SerializedKey::UnknownOrInvalid(
                KeyError::UnsupportedKeyType(kty.to_string()),
                kty.to_string(),
                map,
            )),
        }
    }
}

impl serde::Serialize for SerializedKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;

        let b64 = |s: &[u8]| zeroize::Zeroizing::new(base64ct::Base64UrlUnpadded::encode_string(s));

        match self {
            SerializedKey::Private(kid, key) => {
                let mut map = serializer.serialize_map(None)?;
                match &key.inner {
                    BarePrivateKeyInner::RS256(key) => {
                        let rsa = RsaPrivateKey::from_pkcs1_der(key.secret_pkcs1_der())
                            .map_err(serde::ser::Error::custom)?;
                        if let Some(kid) = kid {
                            map.serialize_entry("kid", kid)?;
                        }
                        map.serialize_entry("kty", "RSA")?;
                        map.serialize_entry("n", &b64(&rsa.n().to_bytes_be()))?;
                        map.serialize_entry("e", &b64(&rsa.e().to_bytes_be()))?;
                        map.serialize_entry("d", &b64(&rsa.d().to_bytes_be()))?;

                        // Add dp, dq, qi
                        let dp = rsa
                            .dp()
                            .map(|dp| dp.to_bytes_be())
                            .ok_or(serde::ser::Error::custom("RSA private key must have dp"))?;
                        let dq = rsa
                            .dq()
                            .map(|dq| dq.to_bytes_be())
                            .ok_or(serde::ser::Error::custom("RSA private key must have dq"))?;

                        map.serialize_entry("dp", &b64(&dp))?;
                        map.serialize_entry("dq", &b64(&dq))?;
                        if rsa.primes().len() == 2 {
                            map.serialize_entry("p", &b64(&rsa.primes()[0].to_bytes_be()))?;
                            map.serialize_entry("q", &b64(&rsa.primes()[1].to_bytes_be()))?;
                        } else {
                            return Err(serde::ser::Error::custom(
                                "RSA private key must have 2 primes",
                            ));
                        }

                        // Note special handling: qi should be a positive integer. Becuase we always source
                        // these RSA keys from PKCS1 or RsaPrivateKey, we know that qi is always positive.
                        let qi = rsa
                            .qinv()
                            .ok_or(serde::ser::Error::custom("RSA private key must have qi"))?;
                        if qi.sign() < Default::default() {
                            return Err(serde::ser::Error::custom("qi must be a positive integer"));
                        }
                        let (_, qi) = qi.to_bytes_be();
                        map.serialize_entry("qi", &b64(&qi))?;
                    }
                    BarePrivateKeyInner::ES256(key) => {
                        if let Some(kid) = kid {
                            map.serialize_entry("kid", kid)?;
                        }
                        map.serialize_entry("kty", "EC")?;
                        map.serialize_entry("crv", "P-256")?;
                        let public_key = key.public_key();
                        let point = public_key.to_encoded_point(false);
                        map.serialize_entry("x", &b64(point.x().unwrap()))?;
                        map.serialize_entry("y", &b64(point.y().unwrap()))?;
                        map.serialize_entry("d", &b64(key.to_bytes().as_ref()))?;
                    }
                    BarePrivateKeyInner::HS256(key) => {
                        if let Some(kid) = kid {
                            map.serialize_entry("kid", kid)?;
                        }
                        map.serialize_entry("kty", "oct")?;
                        map.serialize_entry("k", &b64(&key.key))?;
                    }
                }
                map.end()
            }
            SerializedKey::Public(kid, key) => {
                let mut map = serializer.serialize_map(None)?;
                match &key.inner {
                    BarePublicKeyInner::RS256 { n, e } => {
                        if let Some(kid) = kid {
                            map.serialize_entry("kid", kid)?;
                        }
                        map.serialize_entry("kty", "RSA")?;
                        map.serialize_entry("n", &b64(&n.to_bytes_be()))?;
                        map.serialize_entry("e", &b64(&e.to_bytes_be()))?;
                    }
                    BarePublicKeyInner::ES256(key) => {
                        if let Some(kid) = kid {
                            map.serialize_entry("kid", kid)?;
                        }
                        map.serialize_entry("kty", "EC")?;
                        map.serialize_entry("crv", "P-256")?;
                        let point = key.to_encoded_point(false);
                        map.serialize_entry("x", &b64(point.x().unwrap()))?;
                        map.serialize_entry("y", &b64(point.y().unwrap()))?;
                    }
                    BarePublicKeyInner::HS256(key) => {
                        if let Some(kid) = kid {
                            map.serialize_entry("kid", kid)?;
                        }
                        map.serialize_entry("kty", "oct")?;
                        map.serialize_entry("k", &b64(&key.key))?;
                    }
                }
                map.end()
            }
            SerializedKey::UnknownOrInvalid(_, kty, map) => {
                let mut new_map = serializer.serialize_map(None)?;
                new_map.serialize_entry("kty", kty)?;
                for (k, v) in map {
                    new_map.serialize_entry(k, v)?;
                }
                new_map.end()
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct BareKey {
    pub(crate) inner: BareKeyInner,
}

#[derive(Debug, Hash, PartialEq, Eq)]
pub(crate) enum BareKeyInner {
    Private(BarePrivateKeyInner),
    Public(BarePublicKeyInner),
}

impl BareKey {
    fn from_unvalidated(inner: BareKeyInner) -> Result<Self, KeyError> {
        match inner {
            BareKeyInner::Private(inner) => Ok(Self {
                inner: BareKeyInner::Private(inner.validate()?),
            }),
            BareKeyInner::Public(inner) => Ok(Self {
                inner: BareKeyInner::Public(inner.validate()?),
            }),
        }
    }

    pub fn key_type(&self) -> KeyType {
        match &self.inner {
            BareKeyInner::Private(key) => key.key_type(),
            BareKeyInner::Public(key) => key.key_type(),
        }
    }

    /// Load a key from a PEM-encoded string. Supported formats are PKCS1, PKCS8,
    /// SEC1, and `JWT OCTAL KEY`.
    pub fn from_pem(pem: &str) -> Result<Self, KeyError> {
        let key = parse_pem(pem)?;
        Self::from_parsed_unvalidated(&key).and_then(Self::from_unvalidated)
    }

    pub fn from_pem_multiple(pem: &str) -> Result<Vec<Result<Self, KeyError>>, KeyError> {
        let mut keys = Vec::new();
        let pems = pem::parse_many(pem).map_err(|_| KeyError::DecodeError)?;
        for pem in pems {
            let key = Self::from_parsed_unvalidated(&pem).and_then(Self::from_unvalidated);
            keys.push(key);
        }
        Ok(keys)
    }

    fn from_parsed_unvalidated(pem: &Pem) -> Result<BareKeyInner, KeyError> {
        match pem.tag() {
            "JWT OCTAL KEY" => handle_oct_key(pem).map(BareKeyInner::Private),
            // EC never appears in a raw "ECPublicKey" form, so treat this as
            // SPKI format.
            // https://www.rfc-editor.org/rfc/rfc5915
            "PUBLIC KEY" | "EC PUBLIC KEY" => handle_spki_pubkey(pem).map(BareKeyInner::Public),
            "RSA PUBLIC KEY" => handle_rsa_pubkey(pem).map(BareKeyInner::Public),
            "EC PRIVATE KEY" => handle_ec_key(pem.contents()).map(BareKeyInner::Private),
            "RSA PRIVATE KEY" => handle_rsa_key(pem).map(BareKeyInner::Private),
            "PRIVATE KEY" => handle_pkcs8_key(pem.contents()).map(BareKeyInner::Private),
            tag => Err(KeyError::UnsupportedKeyType(tag.to_string())),
        }
    }

    pub fn try_to_public(&self) -> Result<BarePublicKey, KeyError> {
        match &self.inner {
            BareKeyInner::Private(key) => BarePublicKey::from_unvalidated(key.try_into()?),
            BareKeyInner::Public(key) => Ok(BarePublicKey { inner: key.clone() }),
        }
    }

    pub fn try_to_private(&self) -> Result<BarePrivateKey, KeyError> {
        match &self.inner {
            BareKeyInner::Private(key) => Ok(BarePrivateKey { inner: key.clone() }),
            BareKeyInner::Public(_) => {
                Err(KeyError::UnsupportedKeyType("No private key".to_string()))
            }
        }
    }

    pub fn try_into_public(self) -> Result<BarePublicKey, KeyError> {
        match &self.inner {
            BareKeyInner::Private(key) => BarePublicKey::from_unvalidated(key.try_into()?),
            BareKeyInner::Public(key) => Ok(BarePublicKey { inner: key.clone() }),
        }
    }

    pub fn try_into_private(self) -> Result<BarePrivateKey, KeyError> {
        match &self.inner {
            BareKeyInner::Private(key) => Ok(BarePrivateKey { inner: key.clone() }),
            BareKeyInner::Public(_) => {
                Err(KeyError::UnsupportedKeyType("No private key".to_string()))
            }
        }
    }

    pub fn to_pem(&self) -> String {
        match &self.inner {
            BareKeyInner::Private(key) => key.to_pem(),
            BareKeyInner::Public(key) => key.to_pem(),
        }
    }

    pub fn clone_key(&self) -> Self {
        match &self.inner {
            BareKeyInner::Private(key) => BareKey {
                inner: BareKeyInner::Private(key.clone()),
            },
            BareKeyInner::Public(key) => BareKey {
                inner: BareKeyInner::Public(key.clone()),
            },
        }
    }
}

/// A bare private key contains one of the following:
///
/// - An RSA private key
/// - An ECDSA private key (P-256)
/// - A symmetric key
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct BarePrivateKey {
    pub(crate) inner: BarePrivateKeyInner,
}

impl std::fmt::Debug for BarePrivateKeyInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            BarePrivateKeyInner::RS256(_) => write!(f, "RS256(...)"),
            BarePrivateKeyInner::ES256(_) => write!(f, "ES256(...)"),
            BarePrivateKeyInner::HS256(_) => write!(f, "HS256(...)"),
        }
    }
}

impl std::hash::Hash for BarePrivateKeyInner {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match &self {
            BarePrivateKeyInner::RS256(key) => {
                let Ok(key) = RsaPrivateKey::from_pkcs1_der(key.secret_pkcs1_der()) else {
                    return;
                };
                key.n().hash(state);
            }
            BarePrivateKeyInner::ES256(key) => {
                let key = key.public_key();
                let point = key.to_encoded_point(false);
                point.hash(state);
            }
            BarePrivateKeyInner::HS256(key) => key.hash(state),
        }
    }
}

impl PartialEq for BarePrivateKeyInner {
    fn eq(&self, other: &Self) -> bool {
        match (&self, &other) {
            (BarePrivateKeyInner::RS256(a), BarePrivateKeyInner::RS256(b)) => {
                let Ok(a) = RsaPrivateKey::from_pkcs1_der(a.secret_pkcs1_der()) else {
                    return false;
                };
                let Ok(b) = RsaPrivateKey::from_pkcs1_der(b.secret_pkcs1_der()) else {
                    return false;
                };
                a.n() == b.n() && a.e() == b.e()
            }
            (BarePrivateKeyInner::ES256(a), BarePrivateKeyInner::ES256(b)) => {
                let a = a.public_key();
                let b = b.public_key();
                let a = a.to_encoded_point(false);
                let b = b.to_encoded_point(false);
                a == b
            }
            (BarePrivateKeyInner::HS256(a), BarePrivateKeyInner::HS256(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for BarePrivateKeyInner {}

pub(crate) enum BarePrivateKeyInner {
    /// APIs expose PKCS1 more than PKCS8 so we can work with that
    RS256(rustls_pki_types::PrivatePkcs1KeyDer<'static>),
    /// Use the raw p256 secret key
    ES256(p256::SecretKey),
    /// Bag 'o' bytes (self-zeroing).
    HS256(HmacKey),
}

impl Clone for BarePrivateKeyInner {
    fn clone(&self) -> Self {
        match self {
            BarePrivateKeyInner::RS256(key) => BarePrivateKeyInner::RS256(key.clone_key()),
            BarePrivateKeyInner::ES256(key) => BarePrivateKeyInner::ES256(key.clone()),
            BarePrivateKeyInner::HS256(key) => BarePrivateKeyInner::HS256(key.clone()),
        }
    }
}

/// In debug mode, using the openssl command to generate RSA keys is much faster
/// than ring.
#[allow(unused)]
#[cfg(unix)]
fn optional_openssl_rsa_keygen(bits: usize) -> Option<BarePrivateKey> {
    use std::process::Command;
    // Try to call `openssl genrsa {bits} > /dev/null 2>&1` and then parse the stdout
    // as PEM. If we fail, just return None.
    let output = Command::new("openssl")
        .args(["genrsa", &bits.to_string()])
        .output()
        .ok()?;
    if output.status.success() {
        let rsa = BarePrivateKey::from_pem(&String::from_utf8(output.stdout).ok()?).ok()?;
        Some(rsa)
    } else {
        None
    }
}

#[allow(unused)]
#[cfg(not(unix))]
fn optional_openssl_rsa_keygen(bits: usize) -> Option<rsa::RsaPrivateKey> {
    None
}

impl BarePrivateKey {
    fn from_unvalidated(inner: BarePrivateKeyInner) -> Result<Self, KeyError> {
        Ok(Self {
            inner: inner.validate()?,
        })
    }

    /// Generate a new key of the given type. This may be slow for RSA keys
    /// when running without compiler optimizations.
    pub fn generate(key_type: KeyType) -> Result<Self, KeyError> {
        match key_type {
            KeyType::RS256 => {
                // Because keygen is so slow in debug mode, we use openssl to generate.
                #[cfg(debug_assertions)]
                {
                    let rsa = optional_openssl_rsa_keygen(DEFAULT_GEN_RSA_KEY_BITS);
                    if let Some(rsa) = rsa {
                        return Ok(rsa);
                    }
                }

                let key =
                    rsa::RsaPrivateKey::new(&mut ThreadRng::default(), DEFAULT_GEN_RSA_KEY_BITS)
                        .unwrap();
                let key = key.to_pkcs1_der().unwrap();
                Self::from_unvalidated(BarePrivateKeyInner::RS256(PrivatePkcs1KeyDer::from(
                    key.to_bytes().to_vec(),
                )))
            }
            KeyType::ES256 => {
                let key = ring::signature::EcdsaKeyPair::generate_pkcs8(
                    &ECDSA_P256_SHA256_FIXED_SIGNING,
                    &SystemRandom::new(),
                )
                .unwrap();
                Self::from_unvalidated(handle_pkcs8_key(key.as_ref())?)
            }
            KeyType::HS256 => {
                let mut rng = ThreadRng::default();
                let mut key = zeroize::Zeroizing::new(vec![0; DEFAULT_GEN_OCT_LEN_BYTES]);
                rng.fill(key.as_mut_slice());
                Self::from_unvalidated(BarePrivateKeyInner::HS256(HmacKey { key }))
            }
        }
    }

    /// Load an ECDSA key from a JWK.
    pub fn from_jwt_ec(crv: &str, d: &str, x: &str, y: &str) -> Result<Self, KeyError> {
        if crv != "P-256" {
            return Err(KeyError::UnsupportedKeyType(crv.to_string()));
        }

        // TODO: Not an ideal way to parse
        let validation = |c: char| !c.is_alphanumeric() && c != '-' && c != '_';
        if x.contains(validation) || y.contains(validation) || d.contains(validation) {
            return Err(KeyError::DecodeError);
        }
        let jwk = JwkEcKey::from_str(&format!(
            r#"{{"kty":"EC","crv":"P-256","x":"{}","y":"{}","d":"{}"}}"#,
            x, y, d
        ))
        .map_err(|_| KeyError::DecodeError)?;

        let key: p256::elliptic_curve::SecretKey<p256::NistP256> =
            jwk.to_secret_key().map_err(|_| KeyError::DecodeError)?;

        Self::from_unvalidated(BarePrivateKeyInner::ES256(key))
    }

    /// Load an RSA key from a JWK.
    pub fn from_jwt_rsa(n: &str, e: &str, d: &str, p: &str, q: &str) -> Result<Self, KeyError> {
        let n = BigUint::from_bytes_be(&b64_decode(n)?);
        let e = BigUint::from_bytes_be(&b64_decode(e)?);
        let d = BigUint::from_bytes_be(&b64_decode(d)?);
        let p = BigUint::from_bytes_be(&b64_decode(p)?);
        let q = BigUint::from_bytes_be(&b64_decode(q)?);
        let primes = vec![p, q];

        let rsa = rsa::RsaPrivateKey::from_components(n, e, d, primes)
            .map_err(|_| KeyError::DecodeError)?;
        let key = rsa
            .to_pkcs1_der()
            .to_owned()
            .map_err(|_| KeyError::DecodeError)?;
        Self::from_unvalidated(BarePrivateKeyInner::RS256(PrivatePkcs1KeyDer::from(
            key.to_bytes().to_vec(),
        )))
    }

    /// Load an HMAC key from a base64-encoded string.
    pub fn from_jwt_oct(k: &str) -> Result<Self, KeyError> {
        let key = b64_decode(k)?;
        Self::from_unvalidated(BarePrivateKeyInner::HS256(HmacKey { key }))
    }

    /// Load an HMAC key from a raw byte slice.
    pub fn from_raw_oct(key: &[u8]) -> Result<Self, KeyError> {
        Self::from_unvalidated(BarePrivateKeyInner::HS256(HmacKey {
            key: key.to_vec().into(),
        }))
    }

    /// Load a key from a PEM-encoded string. Supported formats are PKCS1, PKCS8,
    /// SEC1, and `JWT OCTAL KEY`.
    pub fn from_pem(pem: &str) -> Result<Self, KeyError> {
        BareKey::from_pem(pem)?.try_into_private()
    }

    pub fn from_pem_multiple(pem: &str) -> Result<Vec<Result<Self, KeyError>>, KeyError> {
        Ok(BareKey::from_pem_multiple(pem)?
            .into_iter()
            .map(|key| key.and_then(|k| k.try_into_private()))
            .collect())
    }

    pub fn to_public(&self) -> Result<BarePublicKey, KeyError> {
        let inner = (&(self.inner)).try_into()?;
        Ok(BarePublicKey { inner })
    }

    pub fn into_public(self) -> Result<BarePublicKey, KeyError> {
        let inner = (&(self.inner)).try_into()?;
        Ok(BarePublicKey { inner })
    }

    pub fn clone_key(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }

    pub fn to_pem(&self) -> String {
        self.inner.to_pem()
    }

    pub fn to_pem_public(&self) -> Result<String, KeyError> {
        self.inner.to_pem_public()
    }

    pub fn key_type(&self) -> KeyType {
        self.inner.key_type()
    }
}

impl BarePrivateKeyInner {
    pub fn key_type(&self) -> KeyType {
        match &self {
            BarePrivateKeyInner::RS256(..) => KeyType::RS256,
            BarePrivateKeyInner::ES256(..) => KeyType::ES256,
            BarePrivateKeyInner::HS256(..) => KeyType::HS256,
        }
    }

    pub fn to_pem(&self) -> String {
        let key = match &self {
            BarePrivateKeyInner::RS256(key) => {
                pem::encode(&Pem::new("RSA PRIVATE KEY", key.secret_pkcs1_der()))
            }
            BarePrivateKeyInner::ES256(key) => {
                let pkcs8 = pkcs8_from_ec(key).unwrap();
                pem::encode(&Pem::new("PRIVATE KEY", pkcs8))
            }
            BarePrivateKeyInner::HS256(key) => {
                pem::encode(&Pem::new("JWT OCTAL KEY", key.key.as_slice()))
            }
        };
        key
    }

    /// Export this private key to a public key in PEM format.
    pub fn to_pem_public(&self) -> Result<String, KeyError> {
        let key = match &self {
            BarePrivateKeyInner::RS256(key) => {
                let pkcs1 = pkcs1::RsaPrivateKey::from_der(key.secret_pkcs1_der())
                    .map_err(|_| KeyError::DecodeError)?;
                BarePublicKeyInner::RS256 {
                    n: BigUint::from_bytes_be(pkcs1.modulus.as_bytes()),
                    e: BigUint::from_bytes_be(pkcs1.public_exponent.as_bytes()),
                }
                .to_pem()
            }
            BarePrivateKeyInner::ES256(key) => BarePublicKeyInner::ES256(key.public_key()).to_pem(),
            _ => return Err(KeyError::UnsupportedKeyType(self.key_type().to_string())),
        };
        Ok(key)
    }

    fn validate(self) -> Result<Self, KeyError> {
        match &self {
            BarePrivateKeyInner::RS256(key) => {
                validate_rsa_key_pair(key.secret_pkcs1_der())?;
                Ok(self)
            }
            BarePrivateKeyInner::ES256(key) => {
                validate_ecdsa_key_pair(key)?;
                Ok(self)
            }
            BarePrivateKeyInner::HS256(key) => {
                if key.key.len() < MIN_OCT_LEN_BYTES {
                    return Err(KeyError::UnsupportedKeyType(format!(
                        "oct key ({} bytes) < {} bytes",
                        key.key.len(),
                        MIN_OCT_LEN_BYTES
                    )));
                }
                Ok(self)
            }
        }
    }
}

fn parse_pem(pem: &str) -> Result<Pem, KeyError> {
    pem::parse(pem).map_err(|_| KeyError::InvalidPem)
}

fn handle_oct_key(key: &Pem) -> Result<BarePrivateKeyInner, KeyError> {
    let key = key.contents().to_vec().into();
    Ok(BarePrivateKeyInner::HS256(HmacKey { key }))
}

fn handle_ec_key(key: &[u8]) -> Result<BarePrivateKeyInner, KeyError> {
    let mut reader = SliceReader::new(key).map_err(|_| KeyError::DecodeError)?;
    let decoded_key = EcPrivateKey::decode(&mut reader).map_err(|_| KeyError::DecodeError)?;

    if let Some(parameters) = decoded_key.parameters {
        if parameters.named_curve() == Some(SECP_256_R_1) {
            let key = p256::SecretKey::from_slice(decoded_key.private_key)
                .map_err(|_| KeyError::DecodeError)?;
            return Ok(BarePrivateKeyInner::ES256(key));
        }
    }

    Err(KeyError::InvalidEcParameters)
}

fn handle_rsa_key(key: &Pem) -> Result<BarePrivateKeyInner, KeyError> {
    let mut reader = SliceReader::new(key.contents()).map_err(|_| KeyError::DecodeError)?;
    let _decoded_key =
        pkcs1::RsaPrivateKey::decode(&mut reader).map_err(|_| KeyError::DecodeError)?;

    Ok(BarePrivateKeyInner::RS256(PrivatePkcs1KeyDer::from(
        key.contents().to_vec(),
    )))
}

fn handle_pkcs8_key(key: &[u8]) -> Result<BarePrivateKeyInner, KeyError> {
    let mut reader = SliceReader::new(key).map_err(|_| KeyError::DecodeError)?;
    let decoded_key = PrivateKeyInfo::decode(&mut reader).map_err(|_| KeyError::DecodeError)?;

    match decoded_key.algorithm.oid {
        ID_EC_PUBLIC_KEY => {
            // Ensure the curve is P-256
            if decoded_key.algorithm.parameters_oid() != Ok(SECP_256_R_1) {
                return Err(KeyError::InvalidEcParameters);
            }
            let mut reader =
                SliceReader::new(decoded_key.private_key).map_err(|_| KeyError::DecodeError)?;
            let key = EcPrivateKey::decode(&mut reader).map_err(|_| KeyError::DecodeError)?;
            let key =
                p256::SecretKey::from_slice(key.private_key).map_err(|_| KeyError::DecodeError)?;
            Ok(BarePrivateKeyInner::ES256(key))
        }
        RSA_ENCRYPTION => {
            RsaKeyPair::from_der(decoded_key.private_key)
                .map_err(|e| KeyError::KeyValidationError(KeyValidationError(e.to_string())))?;
            Ok(BarePrivateKeyInner::RS256(PrivatePkcs1KeyDer::from(
                decoded_key.private_key.to_vec(),
            )))
        }
        _ => Err(KeyError::UnsupportedKeyType(
            decoded_key.algorithm.oid.to_string(),
        )),
    }
}

fn pkcs8_from_ec(key: &p256::SecretKey) -> Result<Vec<u8>, KeyError> {
    let key_bytes = key.to_bytes();
    let public_key_bytes = key.public_key().to_sec1_bytes().into_vec();
    let mut vec = Vec::new();
    EcPrivateKey {
        private_key: key_bytes.as_ref(),
        parameters: Some(EcParameters::NamedCurve(SECP_256_R_1)),
        public_key: Some(public_key_bytes.as_ref()),
    }
    .encode_to_vec(&mut vec)
    .map_err(|_| KeyError::EncodeError)?;

    let pkcs8 = pkcs8::PrivateKeyInfo {
        algorithm: AlgorithmIdentifier {
            oid: ID_EC_PUBLIC_KEY,
            parameters: Some(AnyRef::from(&EcParameters::NamedCurve(SECP_256_R_1))),
        },
        private_key: &vec,
        public_key: None,
    };
    let mut buf = Vec::new();
    pkcs8
        .encode_to_vec(&mut buf)
        .map_err(|_| KeyError::EncodeError)?;
    Ok(buf)
}

impl TryInto<jsonwebtoken::EncodingKey> for &BarePrivateKey {
    type Error = KeyError;

    fn try_into(self) -> Result<jsonwebtoken::EncodingKey, Self::Error> {
        match &self.inner {
            BarePrivateKeyInner::RS256(key) => Ok(jsonwebtoken::EncodingKey::from_rsa_der(
                key.secret_pkcs1_der(),
            )),
            BarePrivateKeyInner::ES256(key) => {
                Ok(jsonwebtoken::EncodingKey::from_ec_der(&pkcs8_from_ec(key)?))
            }
            BarePrivateKeyInner::HS256(key) => Ok(jsonwebtoken::EncodingKey::from_secret(&key.key)),
        }
    }
}

impl TryInto<jsonwebtoken::DecodingKey> for &BarePublicKey {
    type Error = KeyError;

    fn try_into(self) -> Result<jsonwebtoken::DecodingKey, Self::Error> {
        match &self.inner {
            BarePublicKeyInner::RS256 { n, e } => {
                Ok(jsonwebtoken::DecodingKey::from_rsa_raw_components(
                    &n.to_bytes_be(),
                    &e.to_bytes_be(),
                ))
            }
            BarePublicKeyInner::ES256(key) => {
                Ok(jsonwebtoken::DecodingKey::from_ec_der(&key.to_sec1_bytes()))
            }
            BarePublicKeyInner::HS256(key) => Ok(jsonwebtoken::DecodingKey::from_secret(&key.key)),
        }
    }
}

impl TryFrom<&BarePrivateKeyInner> for BarePublicKeyInner {
    type Error = KeyError;

    fn try_from(key: &BarePrivateKeyInner) -> Result<Self, Self::Error> {
        match key {
            BarePrivateKeyInner::RS256(key) => {
                let rsa = pkcs1::RsaPrivateKey::from_der(key.secret_pkcs1_der())
                    .map_err(|_| KeyError::DecodeError)?;
                let n = BigUint::from_bytes_be(rsa.modulus.as_bytes());
                let e = BigUint::from_bytes_be(rsa.public_exponent.as_bytes());
                Ok(BarePublicKeyInner::RS256 { n, e })
            }
            BarePrivateKeyInner::ES256(key) => {
                let pk = key.public_key();
                Ok(BarePublicKeyInner::ES256(pk))
            }
            BarePrivateKeyInner::HS256(key) => Ok(BarePublicKeyInner::HS256(key.clone())),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct BarePublicKey {
    pub(crate) inner: BarePublicKeyInner,
}

impl std::fmt::Debug for BarePublicKeyInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            BarePublicKeyInner::RS256 { n, e } => write!(f, "RS256({n}, {e})"),
            BarePublicKeyInner::ES256(pk) => write!(f, "ES256({pk:?})"),
            BarePublicKeyInner::HS256(_key) => write!(f, "HS256(...)"),
        }
    }
}

impl std::hash::Hash for BarePublicKeyInner {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match &self {
            BarePublicKeyInner::RS256 { n, e } => {
                n.hash(state);
                e.hash(state);
            }
            BarePublicKeyInner::ES256(pk) => {
                pk.to_encoded_point(false).hash(state);
            }
            BarePublicKeyInner::HS256(key) => {
                key.hash(state);
            }
        }
    }
}

impl Eq for BarePublicKeyInner {}

impl PartialEq for BarePublicKeyInner {
    fn eq(&self, other: &Self) -> bool {
        match (&self, &other) {
            (
                BarePublicKeyInner::RS256 { n: n1, e: e1 },
                BarePublicKeyInner::RS256 { n: n2, e: e2 },
            ) => n1 == n2 && e1 == e2,
            (BarePublicKeyInner::ES256(pk1), BarePublicKeyInner::ES256(pk2)) => pk1 == pk2,
            (BarePublicKeyInner::HS256(key1), BarePublicKeyInner::HS256(key2)) => key1 == key2,
            _ => false,
        }
    }
}

#[derive(Clone)]
pub(crate) enum BarePublicKeyInner {
    RS256 { n: BigUint, e: BigUint },
    ES256(p256::PublicKey),
    HS256(HmacKey),
}

impl BarePublicKey {
    fn from_unvalidated(inner: BarePublicKeyInner) -> Result<Self, KeyError> {
        Ok(Self {
            inner: inner.validate()?,
        })
    }

    /// Load an ECDSA public key from a JWK.
    pub fn from_jwt_ec(crv: &str, x: &str, y: &str) -> Result<Self, KeyError> {
        if crv != "P-256" {
            return Err(KeyError::UnsupportedKeyType(format!(
                "EC curve ({}) not supported",
                crv
            )));
        }

        let x = b64_decode(x)?;
        let y = b64_decode(y)?;
        let x = GenericArray::<u8, p256::U32>::from_slice(x.as_slice());
        let y = GenericArray::<u8, p256::U32>::from_slice(y.as_slice());
        let point = p256::EncodedPoint::from_affine_coordinates(x, y, false);
        let key = p256::PublicKey::from_encoded_point(&point)
            .into_option()
            .ok_or(KeyError::DecodeError)?;
        Self::from_unvalidated(BarePublicKeyInner::ES256(key))
    }

    /// Load an RSA public key from a JWK.
    pub fn from_jwt_rsa(n: &str, e: &str) -> Result<Self, KeyError> {
        let n = BigUint::from_bytes_be(&b64_decode(n)?);
        let e = BigUint::from_bytes_be(&b64_decode(e)?);
        Self::from_unvalidated(BarePublicKeyInner::RS256 { n, e })
    }

    pub fn from_jwt_oct(k: &str) -> Result<Self, KeyError> {
        let key = b64_decode(k)?;
        Self::from_unvalidated(BarePublicKeyInner::HS256(HmacKey { key }))
    }

    /// Creates a `BarePublicKey` from a PEM-encoded public or private key. If the
    /// PEM-encoded file contains a private key, it will be converted to a public key
    /// and the private key data will be discarded.
    ///
    /// Supported formats include the private key formats from [`BareKey::from_pem`],
    /// `SPKI`-containers (`PUBLIC KEY` and `EC PUBLIC KEY`), and `RSA PUBLIC KEY`
    /// traditional-style keys (`RsaPublicKey`).
    pub fn from_pem(pem: &str) -> Result<Self, KeyError> {
        let key = BareKey::from_pem(pem)?;
        key.try_to_public()
    }

    pub fn from_pem_multiple(pem: &str) -> Result<Vec<Result<Self, KeyError>>, KeyError> {
        Ok(BareKey::from_pem_multiple(pem)?
            .into_iter()
            .map(|key| key.and_then(|k| k.try_to_public()))
            .collect())
    }

    pub fn clone_key(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }

    pub fn key_type(&self) -> KeyType {
        self.inner.key_type()
    }

    pub fn to_pem(&self) -> String {
        self.inner.to_pem()
    }
}

impl BarePublicKeyInner {
    pub fn key_type(&self) -> KeyType {
        match &self {
            BarePublicKeyInner::RS256 { .. } => KeyType::RS256,
            BarePublicKeyInner::ES256(..) => KeyType::ES256,
            BarePublicKeyInner::HS256(..) => KeyType::HS256,
        }
    }

    pub fn to_pem(&self) -> String {
        // We use unwrap() here but these cases should not be reachable
        match &self {
            BarePublicKeyInner::RS256 { n, e } => {
                let mut v = Vec::new();
                pkcs1::RsaPublicKey {
                    modulus: UintRef::new(&n.to_bytes_be()).unwrap(),
                    public_exponent: UintRef::new(&e.to_bytes_be()).unwrap(),
                }
                .encode_to_vec(&mut v)
                .unwrap();
                pem::encode(&Pem::new("RSA PUBLIC KEY", v))
            }
            BarePublicKeyInner::ES256(spki) => {
                let spki = SubjectPublicKeyInfoOwned {
                    algorithm: AlgorithmIdentifier {
                        oid: ID_EC_PUBLIC_KEY,
                        parameters: Some(
                            AnyRef::from(&EcParameters::NamedCurve(SECP_256_R_1)).into(),
                        ),
                    },
                    subject_public_key: BitString::from_bytes(&spki.to_sec1_bytes()).unwrap(),
                };
                let mut v = vec![];
                spki.encode_to_vec(&mut v).unwrap();
                pem::encode(&Pem::new("PUBLIC KEY", v))
            }
            BarePublicKeyInner::HS256(key) => {
                pem::encode(&Pem::new("JWT OCTAL KEY", key.key.as_slice()))
            }
        }
    }

    fn validate(self) -> Result<Self, KeyError> {
        match &self {
            BarePublicKeyInner::RS256 { n, e } => validate_rsa_pubkey(n, e),
            BarePublicKeyInner::ES256(pk) => validate_ecdsa_pubkey(pk),
            BarePublicKeyInner::HS256(key) => {
                if key.key.len() < MIN_OCT_LEN_BYTES {
                    return Err(KeyError::UnsupportedKeyType(format!(
                        "oct key ({} bytes) < {} bytes",
                        key.key.len(),
                        MIN_OCT_LEN_BYTES
                    )));
                }
                Ok(())
            }
        }?;
        Ok(self)
    }
}

fn handle_spki_pubkey(key: &Pem) -> Result<BarePublicKeyInner, KeyError> {
    let mut reader = SliceReader::new(key.contents()).map_err(|_| KeyError::DecodeError)?;
    let decoded_key = pkcs8::SubjectPublicKeyInfo::<Any, BitString>::decode(&mut reader)
        .map_err(|_| KeyError::DecodeError)?;

    match decoded_key.algorithm.oid {
        ID_EC_PUBLIC_KEY => {
            let pk = p256::PublicKey::from_sec1_bytes(decoded_key.subject_public_key.raw_bytes())
                .map_err(|_| KeyError::DecodeError)?;
            Ok(BarePublicKeyInner::ES256(pk))
        }
        RSA_ENCRYPTION => {
            let pub_key = pkcs1::RsaPublicKey::from_der(decoded_key.subject_public_key.raw_bytes())
                .map_err(|_| KeyError::DecodeError)?;
            Ok(BarePublicKeyInner::RS256 {
                n: BigUint::from_bytes_be(pub_key.modulus.as_bytes()),
                e: BigUint::from_bytes_be(pub_key.public_exponent.as_bytes()),
            })
        }
        _ => Err(KeyError::UnsupportedKeyType(
            decoded_key.algorithm.oid.to_string(),
        )),
    }
}

fn handle_rsa_pubkey(key: &Pem) -> Result<BarePublicKeyInner, KeyError> {
    let mut reader = SliceReader::new(key.contents()).map_err(|_| KeyError::DecodeError)?;
    let decoded_key =
        pkcs1::RsaPublicKey::decode(&mut reader).map_err(|_| KeyError::DecodeError)?;
    Ok(BarePublicKeyInner::RS256 {
        n: BigUint::from_bytes_be(decoded_key.modulus.as_bytes()),
        e: BigUint::from_bytes_be(decoded_key.public_exponent.as_bytes()),
    })
}

/// Decode a base64 string with optional padding, since jwcrypto also seems to
/// accept this.
///
/// > JWKs make use of the base64url encoding as defined in RFC 4648 As allowed
/// > by Section 3.2 of the RFC, this specification mandates that base64url
/// > encoding when used with JWKs MUST NOT use padding. Notes on implementing
/// > base64url encoding can be found in the JWS specification.
fn b64_decode(s: &str) -> Result<zeroize::Zeroizing<Vec<u8>>, KeyError> {
    let vec = if s.ends_with('=') {
        base64ct::Base64Url::decode_vec(s).map_err(|_| KeyError::DecodeError)?
    } else {
        base64ct::Base64UrlUnpadded::decode_vec(s).map_err(|_| KeyError::DecodeError)?
    };
    Ok(zeroize::Zeroizing::new(vec))
}

fn validate_ecdsa_key_pair(key: &p256::SecretKey) -> Result<(), KeyError> {
    let pkcs8_bytes = pkcs8_from_ec(key)?;
    let _keypair = ring::signature::EcdsaKeyPair::from_pkcs8(
        &ECDSA_P256_SHA256_FIXED_SIGNING,
        &pkcs8_bytes,
        &SystemRandom::new(),
    )
    .map_err(|e| KeyError::KeyValidationError(KeyValidationError(e.to_string())))?;
    Ok(())
}

fn validate_rsa_key_pair(pkcs8: &[u8]) -> Result<(), KeyError> {
    let _keypair = ring::signature::RsaKeyPair::from_der(pkcs8)
        .map_err(|e| KeyError::KeyValidationError(KeyValidationError(e.to_string())))?;
    Ok(())
}

fn validate_rsa_pubkey(n: &BigUint, e: &BigUint) -> Result<(), KeyError> {
    // TODO: Should we validate more than this?
    if e == &BigUint::from(3_u8) {
        return Err(KeyError::UnsupportedKeyType("RSA e=3".to_string()));
    }
    if n.bits() < MIN_RSA_KEY_BITS {
        return Err(KeyError::UnsupportedKeyType(format!(
            "RSA n ({}) < {} bits",
            n.bits(),
            MIN_RSA_KEY_BITS
        )));
    }
    Ok(())
}

fn validate_ecdsa_pubkey(_pk: &p256::PublicKey) -> Result<(), KeyError> {
    // TODO: Should we validate more than this?
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::hash::{Hash, Hasher};

    use super::*;
    use rstest::*;

    #[test]
    fn test_fallback_rsa_keygen() {
        let rsa = optional_openssl_rsa_keygen(DEFAULT_GEN_RSA_KEY_BITS);
        if let Some(rsa) = rsa {
            println!("{}", rsa.to_pem());
        } else {
            println!("Failed to generate RSA key");
        }
    }

    fn load_test_file(filename: &str) -> String {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/testcases")
            .join(filename);
        eprintln!("FILE: {}", path.display());
        std::fs::read_to_string(path).unwrap()
    }

    #[rstest]
    #[case::ec_pk8("prime256v1-prv-pkcs8.pem")]
    #[case::ec_sec1("prime256v1-prv-sec1.pem")]
    #[case::rsa_pkcs1("rsa2048-prv-pkcs1.pem")]
    #[case::rsa_pkcs8("rsa2048-prv-pkcs8.pem")]
    fn test_from_pem_private(#[case] pem: &str) {
        let input = load_test_file(pem);
        eprintln!("IN:\n{input}");
        let key = BarePrivateKey::from_pem(&input).unwrap();
        eprintln!("OUT:\n{}", key.to_pem());
        let key = BarePrivateKey::from_pem(&key.to_pem()).expect("Failed to round-trip");

        let key_type = key.key_type();
        let encoding_key = (&key).try_into().unwrap();
        let token = match key_type {
            KeyType::RS256 => jsonwebtoken::encode(
                &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
                &["claim"],
                &encoding_key,
            )
            .unwrap(),
            KeyType::ES256 => jsonwebtoken::encode(
                &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::ES256),
                &["claim"],
                &encoding_key,
            )
            .unwrap(),
            _ => unreachable!(),
        };
        println!("{}", token);
    }

    #[rstest]
    #[case::ec_pk8("prime256v1-prv-pkcs8.pem")]
    #[case::ec_sec1("prime256v1-prv-sec1.pem")]
    #[case::ec_spki_unc("prime256v1-pub-spki-uncompressed.pem")]
    #[case::ec_spki("prime256v1-pub-spki.pem")]
    fn test_from_pem_public_ec(#[case] pem: &str) {
        let key = BarePublicKey::from_pem(&load_test_file(pem)).unwrap();
        println!("{}", key.to_pem());
        BarePublicKey::from_pem(&key.to_pem()).expect("Failed to round-trip");
    }

    #[rstest]
    #[case::rsa_pkcs1("rsa2048-prv-pkcs1.pem")]
    #[case::rsa_pkcs8("rsa2048-prv-pkcs8.pem")]
    #[case::rsa_spki("rsa2048-pub-pkcs1.pem")]
    #[case::rsa_spki_pkcs8("rsa2048-pub-pkcs8.pem")]
    fn test_from_pem_public_rsa(#[case] pem: &str) {
        let key = BarePublicKey::from_pem(&load_test_file(pem)).unwrap();
        println!("{}", key.to_pem());
        BarePublicKey::from_pem(&key.to_pem()).expect("Failed to round-trip");
    }

    /// Test that the equality and hash functions work for BarePublicKey and BareKey. All
    /// key forms should be equal.
    #[test]
    fn test_eq_hash() {
        let key1 = BarePrivateKey::from_pem(&load_test_file("rsa2048-prv-pkcs1.pem")).unwrap();

        for key in [
            "rsa2048-prv-pkcs1.pem",
            "rsa2048-prv-pkcs8.pem",
            "rsa2048-pub-pkcs1.pem",
            "rsa2048-pub-pkcs8.pem",
        ] {
            if key.contains("pub") {
                let key1: BarePublicKey = key1.to_public().unwrap();
                let key2 = BarePublicKey::from_pem(&load_test_file(key)).unwrap();
                assert_eq!(key1, key2);
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                key1.hash(&mut hasher);
                let hash1 = hasher.finish();
                hasher = std::collections::hash_map::DefaultHasher::new();
                key2.hash(&mut hasher);
                let hash2 = hasher.finish();
                assert_eq!(hash1, hash2);
            } else {
                let key2 = BarePrivateKey::from_pem(&load_test_file(key)).unwrap();
                assert_eq!(key1, key2);
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                key1.hash(&mut hasher);
                let hash1 = hasher.finish();
                hasher = std::collections::hash_map::DefaultHasher::new();
                key2.hash(&mut hasher);
                let hash2 = hasher.finish();
                assert_eq!(hash1, hash2);
            }
        }
    }

    #[test]
    fn test_jwt_ec_key() {
        let key = BarePrivateKey::from_jwt_ec(
            "P-256",
            "w0pL1NOlKBOMtSOvUf6aFeEguWFCclQjWrWqHtHdEA8",
            "ZX_Ajm_22hdQbXImmtmaG-9TQ2z5Dt5Hbia0JzibvXc",
            "9r0Do-XFPyMYM6XCtOAT8AgY2xyRYLuS4U-_xXHDjeE",
        )
        .unwrap();
        println!("{}", key.to_pem());
    }

    #[test]
    fn test_jwt_rsa_key() {
        let e = "AQAB";
        let n = r#"oW-OMq9ATezmeSGLlTbp--Epar64s7qZSi2hTgmdmlaJdpDO8X_EunUIB4DLyPEsOH45-W
            P2xxmw9Uv0UHfvfHsqOKx6vyLjSkDcrUddBWLWhJ5vVm2iHW8FGtYmaLWcHyyh2QiVQUriUNo3HtQqGRKBw9V2X
            gIJ4tzIysuxiMM0uFs8IAvl6TX7MHgUnW4rohyDCJiWLs8UDHpdN3mBpIiokrRr_iTTWNb5m_HKWGJ7RBsLaRsX
            VhxgxZm2PrEEcgb5XlcBbRqOD-5LilCGw5IcX4y12vl_zGpdn-X63UjZmgjRyXKNLh7pOMyKDvWl5vp89w-DKTV
            5oN6CkVnI5w"#
            .replace(char::is_whitespace, "");
        let d = r#"QkfWhrnMeZIP6GDc-dUTiV5fTlvi4qv0vu9wIGWzRwhLpRn8VUwDnhhpxQbc5HIcmU8-B0
            ZDLmi-bmASfa1Ybu_0nFM4jFxLHJP35s77grgbYlTYWpBltJb97hBJsckKwgPlqYGsIiQYOmD1q5spc6TVEW4Fj
            MBihbnnWNf72q2_1CeYgBmLxaMDukUJ8gAaRXkGT0_4YBVBioPUpt_JrfX4dvtJlV3ehXnjN2KiH0xxXHinYdQr
            NSjrUSMUFRCNvSadmuYp1Aoxgsa43VoNAQqbvDRzxjX8eqjdXykVU_ILLwveH9NpZVho727Vd2ISvhwjtjDYMLY
            q6H_Rj6yrTQ"#
            .replace(char::is_whitespace, "");
        let p = r#"1Ce5utgQeHjSPQ_WbUzNt2wRCN8_VbH2LcmPzvxx1XfP7N8FpPs7isx5RpGnrAcVlxq9bI
            MgKq5wtEW2mK4rHB9n9kIxQwDGD7YGOSU3uK-Mi_ygm7ytTo3keMQ9Vj_W05UCT4l8RHvHwU6h-hvCIcN0TnHO0
            mX4JsAgRB-XmuU"#
            .replace(char::is_whitespace, "");
        let q = r#"wsx4ar__O_4dAva_emh7nOSAarF0UBrCuckHImCHwCM62mntXXhjAyY7t9BMQ4ccgYLNeW
            1l9lKpP3orkpYY1wsRMWGrQyDZlKqwNp-x5IG7c5RescuCJ4Yy5JO_PmtXOwukWH7YUTk7nWCCYNCxfHCsxvr-X
            T4oct9FZAtHu9s"#
            .replace(char::is_whitespace, "");
        let key = BarePrivateKey::from_jwt_rsa(&n, e, &d, &p, &q).unwrap();
        println!("{}", key.to_pem());
    }

    #[test]
    fn test_hs256_key_generation() {
        let key = BarePrivateKey::generate(KeyType::HS256).unwrap();
        let pem = key.to_pem();
        println!("{}", pem);
    }

    #[test]
    fn test_es256_key_generation() {
        let key = BarePrivateKey::generate(KeyType::ES256).unwrap();
        let pem = key.to_pem();
        println!("{}", pem);
        let key2 = BarePrivateKey::from_pem(&pem).expect("Failed to round-trip");
        println!("{}", key2.to_pem());
    }

    #[test]
    fn test_rs256_key_generation() {
        let key = BarePrivateKey::generate(KeyType::RS256).unwrap();
        let pem = key.to_pem();
        println!("{}", pem);
        let key2 = BarePrivateKey::from_pem(&pem).expect("Failed to round-trip");
        println!("{}", key2.to_pem());
    }

    #[test]
    fn test_deserialize_private_keys() {
        let json = load_test_file("jwkset-prv.json");
        let keys: SerializedKeys = serde_json::from_str(&json).unwrap();
        println!("{:?}", keys);

        println!("{}", serde_json::to_string(&keys).unwrap());
    }

    #[test]
    fn test_deserialize_public_keys() {
        let json = load_test_file("jwkset-pub.json");
        let keys: SerializedKeys = serde_json::from_str(&json).unwrap();
        println!("{:?}", keys);
        println!("{}", serde_json::to_string(&keys).unwrap());
    }
}
