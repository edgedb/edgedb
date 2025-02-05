use base64ct::Encoding;
use const_oid::db::rfc5912::{ID_EC_PUBLIC_KEY, RSA_ENCRYPTION, SECP_256_R_1};
use der::{asn1::BitString, Any, AnyRef, Decode, Encode, SliceReader};
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
use rsa::{pkcs1::EncodeRsaPrivateKey, traits::PublicKeyParts, BigUint, RsaPrivateKey};
use rustls_pki_types::PrivatePkcs1KeyDer;
use sec1::{EcParameters, EcPrivateKey};
use std::{str::FromStr, vec::Vec};

use super::{KeyError, KeyType, KeyValidationError};

pub struct BareKey {
    pub(crate) inner: BareKeyInner,
}

impl std::fmt::Debug for BareKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            BareKeyInner::RS256(_) => write!(f, "RS256(...)"),
            BareKeyInner::ES256(_) => write!(f, "ES256(...)"),
            BareKeyInner::HS256(_) => write!(f, "HS256(...)"),
        }
    }
}

impl std::hash::Hash for BareKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match &self.inner {
            BareKeyInner::RS256(key) => {
                let Ok(key) = RsaPrivateKey::from_pkcs1_der(key.secret_pkcs1_der()) else {
                    return;
                };
                key.n().hash(state);
            }
            BareKeyInner::ES256(key) => {
                let key = key.public_key();
                let point = key.to_encoded_point(false);
                point.hash(state);
            }
            BareKeyInner::HS256(key) => key.hash(state),
        }
    }
}

impl PartialEq for BareKey {
    fn eq(&self, other: &Self) -> bool {
        match (&self.inner, &other.inner) {
            (BareKeyInner::RS256(a), BareKeyInner::RS256(b)) => {
                let Ok(a) = RsaPrivateKey::from_pkcs1_der(a.secret_pkcs1_der()) else {
                    return false;
                };
                let Ok(b) = RsaPrivateKey::from_pkcs1_der(b.secret_pkcs1_der()) else {
                    return false;
                };
                a.n() == b.n() && a.e() == b.e()
            }
            (BareKeyInner::ES256(a), BareKeyInner::ES256(b)) => {
                let a = a.public_key();
                let b = b.public_key();
                let a = a.to_encoded_point(false);
                let b = b.to_encoded_point(false);
                a == b
            }
            (BareKeyInner::HS256(a), BareKeyInner::HS256(b)) => a == b,
            _ => false,
        }
    }
}

pub(crate) enum BareKeyInner {
    /// APIs expose PKCS1 more than PKCS8 so we can work with that
    RS256(rustls_pki_types::PrivatePkcs1KeyDer<'static>),
    /// Use the raw p256 secret key
    ES256(p256::SecretKey),
    /// Bag 'o' bytes
    HS256(Box<[u8]>),
}

/// In debug mode, using the openssl command to generate RSA keys is much faster
/// than ring.
#[allow(unused)]
#[cfg(unix)]
fn optional_openssl_rsa_keygen(bits: usize) -> Option<BareKey> {
    use std::process::Command;
    // Try to call `openssl genrsa {bits} > /dev/null 2>&1` and then parse the stdout
    // as PEM. If we fail, just return None.
    let output = Command::new("openssl")
        .args(["genrsa", &bits.to_string()])
        .output()
        .ok()?;
    if output.status.success() {
        let rsa = BareKey::from_pem(&String::from_utf8(output.stdout).ok()?).ok()?;
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

impl BareKey {
    /// Generate a new key of the given type. This may be slow for RSA keys.
    pub fn generate(key_type: KeyType) -> Result<Self, KeyError> {
        match key_type {
            KeyType::RS256 => {
                // Because keygen is so slow in debug mode, we use openssl to generate.
                #[cfg(debug_assertions)]
                {
                    let rsa = optional_openssl_rsa_keygen(2048);
                    if let Some(rsa) = rsa {
                        return Ok(rsa);
                    }
                }

                let key = rsa::RsaPrivateKey::new(&mut ThreadRng::default(), 2048).unwrap();
                let key = key.to_pkcs1_der().unwrap();
                Self {
                    inner: BareKeyInner::RS256(PrivatePkcs1KeyDer::from(key.to_bytes().to_vec())),
                }
                .validate()
            }
            KeyType::ES256 => {
                let key = ring::signature::EcdsaKeyPair::generate_pkcs8(
                    &ECDSA_P256_SHA256_FIXED_SIGNING,
                    &mut SystemRandom::new(),
                )
                .unwrap();
                handle_pkcs8_key(key.as_ref())?.validate()
            }
            KeyType::HS256 => {
                let mut rng = ThreadRng::default();
                let mut key = [0; 64];
                rng.fill(&mut key);
                Self {
                    inner: BareKeyInner::HS256(key.as_slice().into()),
                }
                .validate()
            }
        }
    }

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

        Self {
            inner: BareKeyInner::ES256(key),
        }
        .validate()
    }

    pub fn from_jwt_rsa(n: &str, e: &str, d: &str, p: &str, q: &str) -> Result<Self, KeyError> {
        let n = BigUint::from_bytes_be(&base64ct::Base64UrlUnpadded::decode_vec(n).unwrap());
        let e = BigUint::from_bytes_be(&base64ct::Base64UrlUnpadded::decode_vec(e).unwrap());
        let d = BigUint::from_bytes_be(&base64ct::Base64UrlUnpadded::decode_vec(d).unwrap());
        let p = BigUint::from_bytes_be(&base64ct::Base64UrlUnpadded::decode_vec(p).unwrap());
        let q = BigUint::from_bytes_be(&base64ct::Base64UrlUnpadded::decode_vec(q).unwrap());
        let primes = vec![p, q];

        let rsa = rsa::RsaPrivateKey::from_components(n, e, d, primes)
            .map_err(|_| KeyError::DecodeError)?;
        let key = rsa
            .to_pkcs1_der()
            .to_owned()
            .map_err(|_| KeyError::DecodeError)?;
        Self {
            inner: BareKeyInner::RS256(PrivatePkcs1KeyDer::from(key.to_bytes().to_vec())),
        }
        .validate()
    }

    pub fn from_pem(pem: &str) -> Result<Self, KeyError> {
        let key = parse_pem(pem)?;
        Self::from_parsed(&key)
    }

    fn from_parsed(pem: &Pem) -> Result<Self, KeyError> {
        let key = match pem.tag() {
            tag if tag.starts_with("EC") => handle_ec_key(pem.contents()),
            tag if tag.starts_with("RSA") => handle_rsa_key(pem),
            _ => handle_pkcs8_key(pem.contents()),
        }?;

        key.validate()
    }

    pub fn from_pem_multiple(pem: &str) -> Result<Vec<Self>, KeyError> {
        let mut keys = Vec::new();
        let pems = pem::parse_many(pem).map_err(|_| KeyError::DecodeError)?;
        for pem in pems {
            let key = Self::from_parsed(&pem)?;
            keys.push(key);
        }
        Ok(keys)
    }

    pub fn to_public(&self) -> Result<BarePublicKey, KeyError> {
        self.try_into()
    }

    pub fn to_pem(&self) -> String {
        let key = match &self.inner {
            BareKeyInner::RS256(key) => {
                pem::encode(&Pem::new("RSA PRIVATE KEY", key.secret_pkcs1_der()))
            }
            BareKeyInner::ES256(key) => {
                let pkcs8 = pkcs8_from_ec(key).unwrap();
                pem::encode(&Pem::new("PRIVATE KEY", pkcs8))
            }
            BareKeyInner::HS256(key) => pem::encode(&Pem::new("JWT OCTAL KEY", key.as_ref())),
        };
        key
    }

    fn validate(self) -> Result<Self, KeyError> {
        match &self.inner {
            BareKeyInner::RS256(key) => {
                validate_rsa_key_pair(key.secret_pkcs1_der()).map_err(|_| KeyError::DecodeError)?;
                Ok(self)
            }
            BareKeyInner::ES256(key) => {
                validate_ecdsa_key_pair(key).map_err(|_| KeyError::DecodeError)?;
                Ok(self)
            }
            BareKeyInner::HS256(key) => {
                if key.len() < 32 {
                    return Err(KeyError::DecodeError);
                }
                Ok(self)
            }
        }
    }

    pub fn key_type(&self) -> KeyType {
        match &self.inner {
            BareKeyInner::RS256(..) => KeyType::RS256,
            BareKeyInner::ES256(..) => KeyType::ES256,
            BareKeyInner::HS256(..) => KeyType::HS256,
        }
    }
}

fn parse_pem(pem: &str) -> Result<Pem, KeyError> {
    pem::parse(pem).map_err(|_| KeyError::InvalidPem)
}

fn handle_ec_key(key: &[u8]) -> Result<BareKey, KeyError> {
    let mut reader = SliceReader::new(key).map_err(|_| KeyError::DecodeError)?;
    let decoded_key = EcPrivateKey::decode(&mut reader).map_err(|_| KeyError::DecodeError)?;

    if let Some(parameters) = decoded_key.parameters {
        if parameters.named_curve() == Some(SECP_256_R_1) {
            let key = p256::SecretKey::from_slice(decoded_key.private_key)
                .map_err(|_| KeyError::DecodeError)?;
            return Ok(BareKey {
                inner: BareKeyInner::ES256(key),
            });
        }
    }

    Err(KeyError::InvalidEcParameters)
}

fn handle_rsa_key(key: &Pem) -> Result<BareKey, KeyError> {
    let mut reader = SliceReader::new(key.contents()).map_err(|_| KeyError::DecodeError)?;
    let _decoded_key =
        pkcs1::RsaPrivateKey::decode(&mut reader).map_err(|_| KeyError::DecodeError)?;

    Ok(BareKey {
        inner: BareKeyInner::RS256(PrivatePkcs1KeyDer::from(key.contents().to_vec())),
    })
}

fn handle_pkcs8_key(key: &[u8]) -> Result<BareKey, KeyError> {
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
            Ok(BareKey {
                inner: BareKeyInner::ES256(key),
            })
        }
        RSA_ENCRYPTION => {
            RsaKeyPair::from_der(decoded_key.private_key).map_err(KeyValidationError)?;
            Ok(BareKey {
                inner: BareKeyInner::RS256(PrivatePkcs1KeyDer::from(
                    decoded_key.private_key.to_vec(),
                )),
            })
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

impl TryInto<jsonwebtoken::EncodingKey> for &BareKey {
    type Error = KeyError;

    fn try_into(self) -> Result<jsonwebtoken::EncodingKey, Self::Error> {
        match &self.inner {
            BareKeyInner::RS256(key) => Ok(jsonwebtoken::EncodingKey::from_rsa_der(
                key.secret_pkcs1_der(),
            )),
            BareKeyInner::ES256(key) => Ok(jsonwebtoken::EncodingKey::from_ec_der(&pkcs8_from_ec(
                &key,
            )?)),
            BareKeyInner::HS256(key) => Ok(jsonwebtoken::EncodingKey::from_secret(&key)),
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
            BarePublicKeyInner::HS256(key) => Ok(jsonwebtoken::DecodingKey::from_secret(&key)),
        }
    }
}

impl TryFrom<&BareKey> for BarePublicKey {
    type Error = KeyError;

    fn try_from(key: &BareKey) -> Result<Self, Self::Error> {
        match &key.inner {
            BareKeyInner::RS256(key) => {
                let rsa = pkcs1::RsaPrivateKey::from_der(key.secret_pkcs1_der())
                    .map_err(|_| KeyError::DecodeError)?;
                let n = BigUint::from_bytes_be(rsa.modulus.as_bytes());
                let e = BigUint::from_bytes_be(rsa.public_exponent.as_bytes());
                Ok(BarePublicKey {
                    inner: BarePublicKeyInner::RS256 { n, e },
                })
            }
            BareKeyInner::ES256(key) => {
                let pk = key.public_key();
                Ok(BarePublicKey {
                    inner: BarePublicKeyInner::ES256(pk),
                })
            }
            BareKeyInner::HS256(key) => Ok(BarePublicKey {
                inner: BarePublicKeyInner::HS256(key.clone()),
            }),
        }
    }
}

pub struct BarePublicKey {
    pub(crate) inner: BarePublicKeyInner,
}

impl std::fmt::Debug for BarePublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            BarePublicKeyInner::RS256 { n, e } => write!(f, "RS256({n}, {e})"),
            BarePublicKeyInner::ES256(pk) => write!(f, "ES256({pk:?})"),
            BarePublicKeyInner::HS256(_key) => write!(f, "HS256(...)"),
        }
    }
}

impl std::hash::Hash for BarePublicKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match &self.inner {
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

impl Eq for BarePublicKey {}

impl PartialEq for BarePublicKey {
    fn eq(&self, other: &Self) -> bool {
        match (&self.inner, &other.inner) {
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

pub(crate) enum BarePublicKeyInner {
    RS256 { n: BigUint, e: BigUint },
    ES256(p256::PublicKey),
    HS256(Box<[u8]>),
}

impl BarePublicKey {
    /// Creates a `BarePublicKey` from a PEM-encoded public or private key. If the
    /// PEM-encoded file contains a private key, it will be converted to a public key
    /// and the private key data will be discarded.
    pub fn from_pem(pem: &str) -> Result<Self, KeyError> {
        let pem_data = parse_pem(pem)?;

        match pem_data.tag() {
            // EC never appears in a raw "ECPublicKey" form, so treat this as
            // SPKI format.
            // https://www.rfc-editor.org/rfc/rfc5915
            "PUBLIC KEY" | "EC PUBLIC KEY" => handle_spki_pubkey(&pem_data),
            "RSA PUBLIC KEY" => handle_rsa_pubkey(&pem_data),
            "PRIVATE KEY" | "EC PRIVATE KEY" | "RSA PRIVATE KEY" => {
                let key = BareKey::from_pem(pem)?;
                (&key).try_into()
            }
            tag => Err(KeyError::UnsupportedKeyType(tag.to_string())),
        }?
        .validate()
    }

    pub fn to_pem(&self) -> String {
        // We use unwrap() here but these cases should not be reachable
        match &self.inner {
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
            BarePublicKeyInner::HS256(key) => pem::encode(&Pem::new("JWT OCTAL KEY", key.as_ref())),
        }
    }

    pub fn key_type(&self) -> KeyType {
        match &self.inner {
            BarePublicKeyInner::RS256 { .. } => KeyType::RS256,
            BarePublicKeyInner::ES256(..) => KeyType::ES256,
            BarePublicKeyInner::HS256(..) => KeyType::HS256,
        }
    }

    fn validate(self) -> Result<Self, KeyError> {
        match &self.inner {
            BarePublicKeyInner::RS256 { n, e } => validate_rsa_pubkey(n, e),
            BarePublicKeyInner::ES256(pk) => validate_ecdsa_pubkey(pk),
            BarePublicKeyInner::HS256(key) => {
                if key.len() < 32 {
                    return Err(KeyError::DecodeError);
                }
                Ok(())
            }
        }?;
        Ok(self)
    }
}

fn handle_spki_pubkey(key: &Pem) -> Result<BarePublicKey, KeyError> {
    let mut reader = SliceReader::new(key.contents()).map_err(|_| KeyError::DecodeError)?;
    let decoded_key = pkcs8::SubjectPublicKeyInfo::<Any, BitString>::decode(&mut reader)
        .map_err(|_| KeyError::DecodeError)?;

    match decoded_key.algorithm.oid {
        ID_EC_PUBLIC_KEY => {
            let pk = p256::PublicKey::from_sec1_bytes(decoded_key.subject_public_key.raw_bytes())
                .map_err(|_| KeyError::DecodeError)?;
            Ok(BarePublicKey {
                inner: BarePublicKeyInner::ES256(pk),
            })
        }
        RSA_ENCRYPTION => {
            let pub_key = pkcs1::RsaPublicKey::from_der(decoded_key.subject_public_key.raw_bytes())
                .map_err(|_| KeyError::DecodeError)?;
            Ok(BarePublicKey {
                inner: BarePublicKeyInner::RS256 {
                    n: BigUint::from_bytes_be(pub_key.modulus.as_bytes()),
                    e: BigUint::from_bytes_be(pub_key.public_exponent.as_bytes()),
                },
            })
        }
        _ => Err(KeyError::UnsupportedKeyType(
            decoded_key.algorithm.oid.to_string(),
        )),
    }
}

fn handle_rsa_pubkey(key: &Pem) -> Result<BarePublicKey, KeyError> {
    let mut reader = SliceReader::new(key.contents()).map_err(|_| KeyError::DecodeError)?;
    let decoded_key =
        pkcs1::RsaPublicKey::decode(&mut reader).map_err(|_| KeyError::DecodeError)?;
    Ok(BarePublicKey {
        inner: BarePublicKeyInner::RS256 {
            n: BigUint::from_bytes_be(decoded_key.modulus.as_bytes()),
            e: BigUint::from_bytes_be(decoded_key.public_exponent.as_bytes()),
        },
    })
}

fn validate_ecdsa_key_pair(key: &p256::SecretKey) -> Result<(), KeyError> {
    let pkcs8_bytes = pkcs8_from_ec(key)?;
    let _keypair = ring::signature::EcdsaKeyPair::from_pkcs8(
        &ECDSA_P256_SHA256_FIXED_SIGNING,
        &pkcs8_bytes,
        &mut SystemRandom::new(),
    )
    .map_err(KeyValidationError)?;
    Ok(())
}

fn validate_rsa_key_pair(pkcs8: &[u8]) -> Result<(), KeyError> {
    let _keypair = ring::signature::RsaKeyPair::from_der(pkcs8).map_err(KeyValidationError)?;
    Ok(())
}

fn validate_rsa_pubkey(n: &BigUint, e: &BigUint) -> Result<(), KeyError> {
    // TODO: Should we validate more than this?
    if e == &BigUint::from(3_u8) {
        return Err(KeyError::UnsupportedKeyType("RSA e=3".to_string()));
    }
    if n.bits() < 2048 {
        return Err(KeyError::UnsupportedKeyType("RSA n < 2048".to_string()));
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
        let rsa = optional_openssl_rsa_keygen(2048);
        if let Some(rsa) = rsa {
            println!("{}", rsa.to_pem());
        } else {
            println!("Failed to generate RSA key");
        }
    }

    fn load_test_pem(filename: &str) -> String {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/testcases")
            .join(filename);
        eprintln!("{}", path.display());
        std::fs::read_to_string(path).unwrap()
    }

    #[rstest]
    #[case::ec_pk8("prime256v1-prv-pkcs8.pem")]
    #[case::ec_sec1("prime256v1-prv-sec1.pem")]
    #[case::rsa_pkcs1("rsa2048-prv-pkcs1.pem")]
    #[case::rsa_pkcs8("rsa2048-prv-pkcs8.pem")]
    fn test_from_pem_private(#[case] pem: &str) {
        let input = load_test_pem(pem);
        eprintln!("IN:\n{input}");
        let key = BareKey::from_pem(&input).unwrap();
        eprintln!("OUT:\n{}", key.to_pem());
        let key = BareKey::from_pem(&key.to_pem()).expect("Failed to round-trip");

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
        let key = BarePublicKey::from_pem(&load_test_pem(pem)).unwrap();
        println!("{}", key.to_pem());
        BarePublicKey::from_pem(&key.to_pem()).expect("Failed to round-trip");
    }

    #[rstest]
    #[case::rsa_pkcs1("rsa2048-prv-pkcs1.pem")]
    #[case::rsa_pkcs8("rsa2048-prv-pkcs8.pem")]
    #[case::rsa_spki("rsa2048-pub-pkcs1.pem")]
    #[case::rsa_spki_pkcs8("rsa2048-pub-pkcs8.pem")]
    fn test_from_pem_public_rsa(#[case] pem: &str) {
        let key = BarePublicKey::from_pem(&load_test_pem(pem)).unwrap();
        println!("{}", key.to_pem());
        BarePublicKey::from_pem(&key.to_pem()).expect("Failed to round-trip");
    }

    /// Test that the equality and hash functions work for BarePublicKey and BareKey. All
    /// key forms should be equal.
    #[test]
    fn test_eq_hash() {
        let key1 = BareKey::from_pem(&load_test_pem("rsa2048-prv-pkcs1.pem")).unwrap();

        for key in [
            "rsa2048-prv-pkcs1.pem",
            "rsa2048-prv-pkcs8.pem",
            "rsa2048-pub-pkcs1.pem",
            "rsa2048-pub-pkcs8.pem",
        ] {
            if key.contains("pub") {
                let key1: BarePublicKey = (&key1).try_into().unwrap();
                let key2 = BarePublicKey::from_pem(&load_test_pem(key)).unwrap();
                assert_eq!(key1, key2);
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                key1.hash(&mut hasher);
                let hash1 = hasher.finish();
                hasher = std::collections::hash_map::DefaultHasher::new();
                key2.hash(&mut hasher);
                let hash2 = hasher.finish();
                assert_eq!(hash1, hash2);
            } else {
                let key2 = BareKey::from_pem(&load_test_pem(key)).unwrap();
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
        let key = BareKey::from_jwt_ec(
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
        let key = BareKey::from_jwt_rsa(&n, e, &d, &p, &q).unwrap();
        println!("{}", key.to_pem());
    }

    #[test]
    fn test_hs256_key_generation() {
        let key = BareKey::generate(KeyType::HS256).unwrap();
        let pem = key.to_pem();
        println!("{}", pem);
    }

    #[test]
    fn test_es256_key_generation() {
        let key = BareKey::generate(KeyType::ES256).unwrap();
        let pem = key.to_pem();
        println!("{}", pem);
        let key2 = BareKey::from_pem(&pem).expect("Failed to round-trip");
        println!("{}", key2.to_pem());
    }

    #[test]
    fn test_rs256_key_generation() {
        let key = BareKey::generate(KeyType::RS256).unwrap();
        let pem = key.to_pem();
        println!("{}", pem);
        let key2 = BareKey::from_pem(&pem).expect("Failed to round-trip");
        println!("{}", key2.to_pem());
    }
}
