use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use crate::{
    bare_key::SerializedKey, Any, BarePrivateKey, Key, KeyError, KeyRegistry, KeyType,
    SignatureError, ValidationError,
};
use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    types::{PyBytes, PyDict, PyList},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

impl From<KeyError> for PyErr {
    fn from(value: KeyError) -> Self {
        PyValueError::new_err(value.to_string())
    }
}

impl From<SignatureError> for PyErr {
    fn from(value: SignatureError) -> Self {
        PyValueError::new_err(value.to_string())
    }
}

impl From<ValidationError> for PyErr {
    fn from(value: ValidationError) -> Self {
        PyValueError::new_err(value.to_string() + ":" + value.error_string_not_for_user().as_str())
    }
}

#[pyclass]
pub struct SigningCtx {
    context: crate::SigningContext,
}

#[pymethods]
impl SigningCtx {
    #[new]
    pub fn new() -> PyResult<Self> {
        Ok(Self {
            context: crate::SigningContext::default(),
        })
    }

    pub fn set_issuer(&mut self, issuer: &str) {
        self.context.issuer = Some(issuer.to_string());
    }

    pub fn set_audience(&mut self, audience: &str) {
        self.context.audience = Some(audience.to_string());
    }

    pub fn set_not_before(&mut self, not_before: usize) {
        self.context.not_before = Some(Duration::from_secs(not_before as u64));
    }

    pub fn set_expiry(&mut self, expiry: usize) {
        self.context.expiry = Some(Duration::from_secs(expiry as u64));
    }

    pub fn allow(&mut self, claim: &str, values: Bound<PyList>) -> PyResult<()> {
        self.context
            .allow
            .insert(claim.to_string(), values.extract()?);
        Ok(())
    }

    pub fn deny(&mut self, claim: &str, values: Bound<PyList>) -> PyResult<()> {
        self.context
            .deny
            .insert(claim.to_string(), values.extract()?);
        Ok(())
    }
}

#[pyclass]
pub struct JWKSet {
    registry: KeyRegistry<Key>,
    context: crate::SigningContext,
}

#[pymethods]
impl JWKSet {
    #[new]
    pub fn new() -> PyResult<Self> {
        let registry = KeyRegistry::<Key>::default();
        Ok(Self {
            registry,
            context: crate::SigningContext::default(),
        })
    }

    #[staticmethod]
    pub fn from_hs256_key(key: Bound<PyBytes>) -> PyResult<Self> {
        let key = BarePrivateKey::from_raw_oct(key.as_bytes())?;
        let mut registry = KeyRegistry::<Key>::default();
        registry.add_key(Key::from_bare_private_key(None, key)?);
        Ok(Self {
            registry,
            context: crate::SigningContext::default(),
        })
    }

    #[pyo3(signature = (*, kid, kty))]
    pub fn generate(&mut self, kid: Option<&str>, kty: &str) -> PyResult<()> {
        let key = match kty {
            "HS256" => BarePrivateKey::generate(KeyType::HS256),
            "RS256" => BarePrivateKey::generate(KeyType::RS256),
            "ES256" => BarePrivateKey::generate(KeyType::ES256),
            _ => return Err(PyValueError::new_err("Invalid key type")),
        }?;
        self.registry
            .add_key(Key::from_bare_private_key(kid.map(String::from), key)?);
        Ok(())
    }

    #[pyo3(signature = (*, kid, kty, **kwargs))]
    pub fn add(
        &mut self,
        kid: Option<&str>,
        kty: &str,
        kwargs: Option<Bound<PyDict>>,
    ) -> PyResult<()> {
        let mut map = serde_json::Map::default();
        if let Some(kwargs) = kwargs {
            for (key, value) in kwargs.iter() {
                let key = key.extract::<String>()?;
                let value = value.extract::<String>()?;
                map.insert(key, value.into());
            }
        }
        if let Some(kid) = kid {
            map.insert("kid".to_string(), kid.to_string().into());
        }
        let kty = match kty {
            "HS256" => "oct",
            "RS256" => "RSA",
            "ES256" => "EC",
            _ => return Err(PyValueError::new_err("Invalid key type")),
        };
        map.insert("kty".to_string(), kty.to_string().into());
        let key: SerializedKey = serde_json::from_value(serde_json::Value::Object(map))
            .map_err(|e| PyValueError::new_err(format!("Error creating key: {e}")))?;
        match key {
            SerializedKey::Private(kid, key) => {
                self.registry.add_key(Key::from_bare_private_key(kid, key)?);
            }
            SerializedKey::Public(kid, key) => {
                self.registry.add_key(Key::from_bare_public_key(kid, key)?);
            }
            SerializedKey::UnknownOrInvalid(error, _, _) => {
                return Err(PyValueError::new_err(format!("Invalid key: {error}")));
            }
        }
        Ok(())
    }

    pub fn load(&mut self, keys: &str) -> PyResult<usize> {
        let count = self.registry.add_from_any(keys)?;
        Ok(count)
    }

    pub fn load_json(&mut self, keys: &str) -> PyResult<usize> {
        let count = self.registry.add_from_jwkset(keys)?;
        Ok(count)
    }

    pub fn set_issuer(&mut self, issuer: &str) {
        self.context.issuer = Some(issuer.to_string());
    }

    pub fn set_audience(&mut self, audience: &str) {
        self.context.audience = Some(audience.to_string());
    }

    pub fn set_not_before(&mut self, not_before: usize) {
        self.context.not_before = Some(Duration::from_secs(not_before as u64));
    }

    pub fn set_expiry(&mut self, expiry: usize) {
        self.context.expiry = Some(Duration::from_secs(expiry as u64));
    }

    pub fn allow(&mut self, claim: &str, values: Bound<PyList>) -> PyResult<()> {
        self.context
            .allow
            .insert(claim.to_string(), values.extract()?);
        Ok(())
    }

    pub fn deny(&mut self, claim: &str, values: Bound<PyList>) -> PyResult<()> {
        self.context
            .deny
            .insert(claim.to_string(), values.extract()?);
        Ok(())
    }

    #[pyo3(signature = (*, private_keys=true))]
    pub fn export_pem(&self, private_keys: bool) -> PyResult<Vec<u8>> {
        if private_keys {
            Ok(self.registry.to_pem().into_bytes())
        } else {
            Ok(self.registry.to_pem_public()?.into_bytes())
        }
    }

    #[pyo3(signature = (*, private_keys=true))]
    pub fn export_json(&self, private_keys: bool) -> PyResult<Vec<u8>> {
        Ok(if private_keys {
            self.registry.to_json()?
        } else {
            self.registry.to_json_public()?
        }
        .into_bytes())
    }

    pub fn can_sign(&self) -> bool {
        self.registry.can_sign()
    }

    /// Sign a claims object with the default or given signing context.
    #[pyo3(signature = (claims, *, ctx=None))]
    pub fn sign(&self, claims: Bound<PyDict>, ctx: Option<&SigningCtx>) -> PyResult<String> {
        let claims = claims.extract()?;
        let token = self
            .registry
            .sign(claims, ctx.map(|c| &c.context).unwrap_or(&self.context))?;
        Ok(token)
    }

    pub fn validate(&self, token: &str) -> PyResult<HashMap<String, Any>> {
        let claims = self.registry.validate(token, &self.context)?;
        Ok(claims)
    }

    pub fn __repr__(&self) -> String {
        format!("JWKSet(keys={})", self.registry.len())
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct GelClaims {
    #[serde(rename = "edb.i")]
    instances: Option<Vec<String>>,
    #[serde(rename = "edb.i.all")]
    all_instances: bool,
    #[serde(rename = "edb.r")]
    roles: Option<Vec<String>>,
    #[serde(rename = "edb.r.all")]
    all_roles: bool,
    #[serde(rename = "edb.d")]
    databases: Option<Vec<String>>,
    #[serde(rename = "edb.d.all")]
    all_databases: bool,
    #[serde(rename = "jti")]
    jti: uuid::Uuid,
}

fn vec_from_list_or_tuple(value: Bound<PyAny>) -> PyResult<Vec<String>> {
    if let Ok(list) = value.extract::<Vec<String>>() {
        Ok(list)
    } else {
        let mut list = Vec::new();
        let iter = value.try_iter()?;
        for item in iter {
            let item = item?;
            if let Ok(item) = item.extract::<String>() {
                list.push(item);
            } else {
                return Err(PyValueError::new_err(
                    "Expected a list or other iterable of strings",
                ));
            }
        }
        Ok(list)
    }
}

/// A very basic cache for JWKSets.
#[pyclass]
pub struct JWKSetCache {
    cache: HashMap<String, (Instant, Py<JWKSet>)>,
    expiry_seconds: usize,
}

#[pymethods]
impl JWKSetCache {
    #[new]
    pub fn new(expiry_seconds: usize) -> PyResult<Self> {
        Ok(Self {
            cache: HashMap::new(),
            expiry_seconds,
        })
    }

    /// Get a JWKSet from the cache and returns whether the cache is fresh or stale.
    pub fn get(&mut self, py: Python, key: &str) -> PyResult<(bool, Option<Py<JWKSet>>)> {
        if let Some((expiry, registry)) = self.cache.get_mut(key) {
            if Instant::now() > *expiry {
                // Temporarily extend the expiry time by 60 seconds to avoid multiple fetches
                *expiry = Instant::now() + Duration::from_secs(60);
                return Ok((false, Some(registry.clone_ref(py))));
            } else {
                return Ok((true, Some(registry.clone_ref(py))));
            }
        }
        Ok((false, None))
    }

    /// Set a JWKSet in the cache, resetting the expiry time.
    pub fn set(&mut self, key: &str, registry: Py<JWKSet>) {
        self.cache.insert(
            key.to_string(),
            (
                Instant::now() + Duration::from_secs(self.expiry_seconds as _),
                registry,
            ),
        );
    }
}

#[pyfunction]
#[pyo3(signature = (registry, *, instances=None, roles=None, databases=None))]
fn generate_gel_token(
    registry: &JWKSet,
    instances: Option<Bound<PyAny>>,
    roles: Option<Bound<PyAny>>,
    databases: Option<Bound<PyAny>>,
) -> PyResult<String> {
    let mut claims = GelClaims::default();

    if let Some(instances) = instances {
        claims.instances = Some(vec_from_list_or_tuple(instances)?);
    } else {
        claims.all_instances = true;
    }

    if let Some(roles) = roles {
        claims.roles = Some(vec_from_list_or_tuple(roles)?);
    } else {
        claims.all_roles = true;
    }

    if let Some(databases) = databases {
        claims.databases = Some(vec_from_list_or_tuple(databases)?);
    } else {
        claims.all_databases = true;
    }

    claims.jti = Uuid::new_v4();

    let claims = HashMap::from([
        ("edb.i".to_string(), Any::from(claims.instances)),
        ("edb.i.all".to_string(), Any::from(claims.all_instances)),
        ("edb.r".to_string(), Any::from(claims.roles)),
        ("edb.r.all".to_string(), Any::from(claims.all_roles)),
        ("edb.d".to_string(), Any::from(claims.databases)),
        ("edb.d.all".to_string(), Any::from(claims.all_databases)),
        ("jti".to_string(), Any::from(claims.jti.to_string())),
    ]);

    let token = registry.registry.sign(claims, &registry.context)?;
    Ok(format!("edbt1_{}", token))
}

#[pymodule]
pub fn _jwt(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<JWKSet>()?;
    m.add_class::<JWKSetCache>()?;
    m.add_class::<SigningCtx>()?;
    m.add_function(wrap_pyfunction!(generate_gel_token, m)?)?;
    Ok(())
}
