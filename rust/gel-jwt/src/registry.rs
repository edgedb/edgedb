use crate::{
    bare_key::{SerializedKey, SerializedKeys},
    key::*,
    Any, KeyError, OpaqueValidationFailureReason, SignatureError, SigningContext,
    ValidationContext, ValidationError,
};
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fmt::Debug,
};

pub(crate) trait IsKey {
    type Inner: std::hash::Hash + Eq + Debug + Clone;

    fn inner(&self) -> &Self::Inner;
    fn key_type(inner: &Self::Inner) -> KeyType;
    fn from_inner(kid: Option<String>, inner: Self::Inner) -> Self;
    fn into_inner(self) -> (Option<String>, Self::Inner);
    fn get_serialized_key(key: SerializedKey) -> Option<Self>
    where
        Self: Sized;
    fn to_serialized_key(kid: Option<&str>, inner: &Self::Inner) -> SerializedKey;
    fn from_pem(pem: &str) -> Result<Vec<Result<Self, KeyError>>, KeyError>
    where
        Self: Sized;
    fn to_pem(inner: &Self::Inner) -> String;
    fn encoding_key(inner: &Self::Inner) -> Option<&jsonwebtoken::EncodingKey>;
    fn decoding_key(inner: &Self::Inner) -> &jsonwebtoken::DecodingKey;
}

/// A collection of [`Key`] or [`PublicKey`] objects.
#[allow(private_bounds)]
pub struct KeyRegistry<K: IsKey> {
    // TODO: this could probably be optimized, especially if we can
    // generate key signatures.
    /// Map from key identifier (kid) to ordinal
    named_keys: HashMap<String, usize>,
    /// Set of ordinals for unnamed keys
    unnamed_keys: HashSet<usize>,
    /// Map from key to ordinal/kid for quick lookup
    key_to_ordinal: HashMap<K::Inner, (usize, Option<String>)>,
    /// Set of active ordinals
    active_keys: BTreeSet<usize>,
    /// Next ordinal to use for a new key
    next: usize,
}

impl<K: IsKey> Default for KeyRegistry<K> {
    fn default() -> Self {
        Self {
            named_keys: HashMap::default(),
            unnamed_keys: HashSet::default(),
            key_to_ordinal: HashMap::default(),
            active_keys: BTreeSet::default(),
            next: 0,
        }
    }
}

#[allow(private_bounds)]
impl<K: IsKey> KeyRegistry<K> {
    /// Clear the registry.
    pub fn clear(&mut self) {
        *self = Self::default();
    }

    pub fn into_keys(self) -> impl Iterator<Item = K> {
        self.key_to_ordinal
            .into_iter()
            .map(|(key, (_, kid))| K::from_inner(kid, key))
    }

    /// Add a key to the registry. If the key already exists, it will be
    /// replaced. If the key specifies the same kid as another key already
    /// added, the new key will replace the old one.
    ///
    /// Adding a key, even if it already exists, will make it the active key.
    pub fn add_key(&mut self, key: K) {
        self.remove_key(&key);

        let (kid, inner) = key.into_inner();

        // If the kid still exists, we need to remove that key too
        if let Some(kid) = &kid {
            if self.named_keys.contains_key(kid) {
                self.remove_kid(kid);
            }
        }

        // Key is new, add it to the registry
        let ordinal = self.next;
        self.next += 1;
        self.key_to_ordinal.insert(inner, (ordinal, kid.clone()));
        self.active_keys.insert(ordinal);

        if let Some(kid) = kid {
            self.named_keys.insert(kid, ordinal);
        } else {
            self.unnamed_keys.insert(ordinal);
        }
    }

    /// Remove a key from the registry by its key.
    pub fn remove_key(&mut self, key: &K) {
        let inner = key.inner();
        if let Some((ordinal, kid)) = self.key_to_ordinal.remove(inner) {
            if let Some(kid) = kid {
                self.named_keys.remove(&kid);
            } else {
                self.unnamed_keys.remove(&ordinal);
            }
            self.active_keys.remove(&ordinal);
        }
    }

    /// Remove a key from the registry by its kid. Note: O(N).
    pub fn remove_kid(&mut self, kid: &str) -> bool {
        if let Some(ordinal) = self.named_keys.remove(kid) {
            self.active_keys.remove(&ordinal);
            self.key_to_ordinal.retain(|_, &mut (v, _)| v != ordinal);
            true
        } else {
            false
        }
    }

    /// Get the number of keys in the registry.
    pub fn len(&self) -> usize {
        self.key_to_ordinal.len()
    }

    /// Check if the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.key_to_ordinal.is_empty()
    }

    /// Add keys from a JWKSet.
    pub fn add_from_jwkset(&mut self, jwkset: &str) -> Result<usize, KeyError> {
        let loaded: SerializedKeys =
            serde_json::from_str(jwkset).map_err(|_| KeyError::InvalidJson)?;
        let mut added = 0;
        for key in loaded.keys {
            if let Some(key) = K::get_serialized_key(key) {
                self.add_key(key);
                added += 1;
            } else {
                // TODO: log unknown or invalid key
            }
        }
        Ok(added)
    }

    /// Add keys from a PEM file.
    pub fn add_from_pem(&mut self, pem: &str) -> Result<usize, KeyError> {
        let keys = K::from_pem(pem)?;
        let mut added = 0;
        for key in keys {
            if let Ok(key) = key {
                self.add_key(key);
                added += 1;
            } else {
                // TODO: log unknown or invalid key
            }
        }
        Ok(added)
    }

    /// Add keys from a source string which can be either a JWK set or a PEM file with
    /// 1 or more keys.
    pub fn add_from_any(&mut self, source: &str) -> Result<usize, KeyError> {
        let source = source.trim();
        if source.is_empty() {
            return Ok(0);
        }

        // Get the first non-whitespace character
        let first_char = source.chars().next().unwrap_or_default();
        if first_char == '{' {
            self.add_from_jwkset(source)
        } else if first_char == '-' {
            self.add_from_pem(source)
        } else {
            Err(KeyError::UnsupportedKeyType(format!(
                "Expected JWK set or PEM file, got {}",
                first_char
            )))
        }
    }

    pub fn to_pem(&self) -> String {
        let mut pem = String::new();
        for (k, (_, _)) in &self.key_to_ordinal {
            pem.push_str(&K::to_pem(k));
        }
        pem
    }

    pub fn to_json(&self) -> Result<String, KeyError> {
        serde_json::to_string(&SerializedKeys {
            keys: self
                .key_to_ordinal
                .iter()
                .map(|(k, (_, kid))| K::to_serialized_key(kid.as_deref(), k))
                .collect(),
        })
        .map_err(|_| KeyError::EncodeError)
    }

    /// Get the active key and kid.
    fn active_key(&self) -> Option<(Option<&str>, &K::Inner)> {
        if let Some(&i) = self.active_keys.last() {
            for (k, &(v, ref kid)) in &self.key_to_ordinal {
                if v == i {
                    if let Some(kid) = kid {
                        return Some((Some(kid.as_str()), k));
                    } else {
                        return Some((None, k));
                    }
                }
            }
        }
        None
    }

    pub fn validate(
        &self,
        token: &str,
        ctx: &ValidationContext,
    ) -> Result<HashMap<String, Any>, ValidationError> {
        // If we have a named key that matches, use that.
        if !self.named_keys.is_empty() {
            if let Ok(header) = jsonwebtoken::decode_header(token) {
                if let Some(header_kid) = header.kid {
                    for (key, (_, kid)) in &self.key_to_ordinal {
                        if kid.as_deref() == Some(header_kid.as_str()) {
                            return validate_token(
                                K::key_type(key),
                                K::decoding_key(key),
                                None,
                                token,
                                ctx,
                            );
                        }
                    }
                }
            }
        }

        let mut result = None;
        for (key, _) in self.key_to_ordinal.iter() {
            let last_result =
                validate_token(K::key_type(key), K::decoding_key(key), None, token, ctx);
            match last_result {
                Ok(result) => return Ok(result),
                Err(e) => result = Some(e),
            }
        }
        Err(result.unwrap_or(OpaqueValidationFailureReason::NoAppropriateKey.into()))
    }

    pub fn sign(
        &self,
        claims: HashMap<String, Any>,
        ctx: &SigningContext,
    ) -> Result<String, SignatureError> {
        let (kid, key) = self.active_key().ok_or(SignatureError::NoAppropriateKey)?;
        let encoding_key = K::encoding_key(key).ok_or(SignatureError::NoAppropriateKey)?;
        sign_token(K::key_type(key), encoding_key, kid, claims, ctx)
    }
}

impl KeyRegistry<PrivateKey> {
    pub fn can_sign(&self) -> bool {
        self.has_private_keys() || self.has_symmetric_keys()
    }

    pub fn can_validate(&self) -> bool {
        self.has_public_keys() || self.has_symmetric_keys()
    }

    pub fn has_private_keys(&self) -> bool {
        !self.is_empty()
    }

    pub fn has_public_keys(&self) -> bool {
        self.key_to_ordinal
            .iter()
            .any(|(k, _)| k.bare_key.key_type() != KeyType::HS256)
    }

    pub fn has_symmetric_keys(&self) -> bool {
        self.key_to_ordinal
            .iter()
            .any(|(k, _)| k.bare_key.key_type() == KeyType::HS256)
    }
}

impl KeyRegistry<PublicKey> {
    pub fn can_sign(&self) -> bool {
        self.has_private_keys() || self.has_symmetric_keys()
    }

    pub fn can_validate(&self) -> bool {
        self.has_public_keys() || self.has_symmetric_keys()
    }

    pub fn has_public_keys(&self) -> bool {
        !self.is_empty()
    }

    pub fn has_private_keys(&self) -> bool {
        false
    }

    pub fn has_symmetric_keys(&self) -> bool {
        false
    }
}

impl KeyRegistry<Key> {
    pub fn can_sign(&self) -> bool {
        self.has_private_keys() || self.has_symmetric_keys()
    }

    pub fn can_validate(&self) -> bool {
        self.has_public_keys() || self.has_symmetric_keys()
    }

    pub fn has_private_keys(&self) -> bool {
        for k in self.key_to_ordinal.keys() {
            if let KeyInner::Private(_) = k {
                return true;
            }
        }
        false
    }

    pub fn has_public_keys(&self) -> bool {
        for k in self.key_to_ordinal.keys() {
            if let KeyInner::Public(_) = k {
                return true;
            }
            if let KeyInner::Private(k) = k {
                if k.bare_key.key_type() != KeyType::HS256 {
                    return true;
                }
            }
        }
        false
    }

    pub fn has_symmetric_keys(&self) -> bool {
        for k in self.key_to_ordinal.keys() {
            if let KeyInner::Private(k) = k {
                if k.bare_key.key_type() == KeyType::HS256 {
                    return true;
                }
            }
        }
        false
    }

    /// Export the registry as a PEM file containing only the public keys.
    /// This will fail if the registry contains symmetric keys.
    pub fn to_pem_public(&self) -> Result<String, KeyError> {
        let mut pem = String::new();
        for (k, (_, _)) in &self.key_to_ordinal {
            match k {
                KeyInner::Private(k) => {
                    pem.push_str(&k.bare_key.to_pem_public()?);
                }
                KeyInner::Public(k) => {
                    pem.push_str(&k.bare_key.to_pem());
                }
            }
        }
        Ok(pem)
    }

    /// Export the registry as a JSON object containing only the public keys.
    /// This will fail if the registry contains symmetric keys.
    pub fn to_json_public(&self) -> Result<String, KeyError> {
        let mut keys = Vec::new();
        for (k, (_, kid)) in &self.key_to_ordinal {
            match k {
                KeyInner::Private(k) => {
                    keys.push(SerializedKey::Public(
                        kid.clone(),
                        k.bare_key.to_public()?.clone_key(),
                    ));
                }
                KeyInner::Public(k) => {
                    keys.push(SerializedKey::Public(kid.clone(), k.bare_key.clone_key()));
                }
            }
        }
        serde_json::to_string(&SerializedKeys { keys }).map_err(|_| KeyError::EncodeError)
    }
}
