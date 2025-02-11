use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use crate::{
    bare_key::SerializedKey, Any, BarePrivateKey, Key, KeyError, KeyRegistry, KeyType,
    SignatureError, ValidationError, ValidationType,
};
use base64ct::{Base64Unpadded, Encoding};
use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    types::{PyBytes, PyDict},
};
use serde::{Deserialize, Serialize};
use tracing::warn;
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
        PyValueError::new_err(format!("{}: {}", value, value.error_string_not_for_user()))
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

    pub fn set_expiry(&mut self, expiry: isize) {
        self.context.expiry = Some(Duration::from_secs(expiry.max(0) as u64));
    }
}

#[pyclass]
pub struct ValidationCtx {
    context: crate::ValidationContext,
}

#[pymethods]
impl ValidationCtx {
    #[new]
    pub fn new() -> PyResult<Self> {
        Ok(Self {
            context: crate::ValidationContext::default(),
        })
    }

    pub fn allow(&mut self, claim: &str, values: Bound<PyAny>) -> PyResult<()> {
        let values = vec_from_list_or_tuple(values)?;
        self.context
            .allow_list
            .insert(claim.to_string(), values.into_iter().collect());
        Ok(())
    }

    pub fn deny(&mut self, claim: &str, values: Bound<PyAny>) -> PyResult<()> {
        let values = vec_from_list_or_tuple(values)?;
        self.context
            .deny_list
            .insert(claim.to_string(), values.into_iter().collect());
        Ok(())
    }

    pub fn require_expiry(&mut self) {
        self.context.expiry = ValidationType::Require;
    }

    pub fn ignore_expiry(&mut self) {
        self.context.expiry = ValidationType::Ignore;
    }
}

#[pyclass]
pub struct JWKSet {
    registry: KeyRegistry<Key>,
    default_signing_ctx: Py<SigningCtx>,
    default_validation_ctx: Py<ValidationCtx>,
}

#[pymethods]
impl JWKSet {
    #[new]
    pub fn new(py: Python) -> PyResult<Self> {
        let registry = KeyRegistry::<Key>::default();
        Ok(Self {
            registry,
            default_signing_ctx: Py::new(py, SigningCtx::new()?)?,
            default_validation_ctx: Py::new(py, ValidationCtx::new()?)?,
        })
    }

    #[staticmethod]
    pub fn from_hs256_key(py: Python, key: Bound<PyBytes>) -> PyResult<Self> {
        let key = BarePrivateKey::from_raw_oct(key.as_bytes())?;
        let mut registry = KeyRegistry::<Key>::default();
        registry.add_key(Key::from_bare_private_key(None, key)?);
        Ok(Self {
            registry,
            default_signing_ctx: Py::new(py, SigningCtx::new()?)?,
            default_validation_ctx: Py::new(py, ValidationCtx::new()?)?,
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

    #[getter]
    pub fn default_signing_context(&self, py: Python) -> Py<SigningCtx> {
        self.default_signing_ctx.clone_ref(py)
    }

    #[getter]
    pub fn default_validation_context(&self, py: Python) -> Py<ValidationCtx> {
        self.default_validation_ctx.clone_ref(py)
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

    /// Sign a claims object with the default or given signing context.
    #[pyo3(signature = (claims, *, ctx=None))]
    pub fn sign(
        &self,
        py: Python,
        claims: Bound<PyDict>,
        ctx: Option<&SigningCtx>,
    ) -> PyResult<String> {
        let claims = claims.extract()?;
        let token = self.registry.sign(
            claims,
            ctx.map(|c| &c.context)
                .unwrap_or(&self.default_signing_ctx.borrow(py).context),
        )?;
        Ok(token)
    }

    /// Validate a token with the default or given validation context.
    #[pyo3(signature = (token, *, ctx=None))]
    pub fn validate(
        &self,
        py: Python,
        token: &str,
        ctx: Option<&ValidationCtx>,
    ) -> PyResult<HashMap<String, Any>> {
        let claims = self.registry.validate(
            token,
            ctx.map(|c| &c.context)
                .unwrap_or(&self.default_validation_ctx.borrow(py).context),
        )?;
        Ok(claims)
    }

    pub fn can_sign(&self) -> bool {
        self.registry.can_sign()
    }

    pub fn can_validate(&self) -> bool {
        self.registry.can_validate()
    }

    pub fn has_public_keys(&self) -> bool {
        self.registry.has_public_keys()
    }

    pub fn has_private_keys(&self) -> bool {
        self.registry.has_private_keys()
    }

    pub fn has_symmetric_keys(&self) -> bool {
        self.registry.has_symmetric_keys()
    }

    pub fn __repr__(&self) -> String {
        format!("JWKSet(keys={})", self.registry.len())
    }

    pub fn __len__(&self) -> usize {
        self.registry.len()
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

/// Generate a token with optional additional claims.
#[pyfunction]
#[pyo3(signature = (registry, *, instances=None, roles=None, databases=None, **kwargs))]
fn generate_gel_token(
    py: Python,
    registry: &JWKSet,
    instances: Option<Bound<PyAny>>,
    roles: Option<Bound<PyAny>>,
    databases: Option<Bound<PyAny>>,
    kwargs: Option<Bound<PyDict>>,
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

    let mut claims_map = HashMap::new();
    if claims.all_instances {
        claims_map.insert("edb.i.all".to_string(), Any::from(true));
    } else if let Some(instances) = claims.instances {
        claims_map.insert("edb.i".to_string(), Any::from(instances));
    }

    if claims.all_roles {
        claims_map.insert("edb.r.all".to_string(), Any::from(true));
    } else if let Some(roles) = claims.roles {
        claims_map.insert("edb.r".to_string(), Any::from(roles));
    }

    if claims.all_databases {
        claims_map.insert("edb.d.all".to_string(), Any::from(true));
    } else if let Some(databases) = claims.databases {
        claims_map.insert("edb.d".to_string(), Any::from(databases));
    }

    if let Some(kwargs) = kwargs {
        for (key, value) in kwargs.iter() {
            let key = key.extract::<String>()?;
            let value = value.extract::<Any>()?;
            claims_map.insert(key, value);
        }
    }

    // Add a JTI if and only if it's not already present.
    if !claims_map.contains_key("jti") {
        claims.jti = Uuid::new_v4();
        // Encode UUID as base64 to make the token shorter
        let jti_base64 = Base64Unpadded::encode_string(claims.jti.as_bytes());
        claims_map.insert("jti".to_string(), Any::from(jti_base64));
    }

    let token = registry
        .registry
        .sign(claims_map, &registry.default_signing_ctx.borrow(py).context)?;
    Ok(format!("edbt1_{}", token))
}

#[derive(Debug, Default)]
enum TokenMatch {
    #[default]
    None,
    All,
    Some(HashSet<String>),
}

impl TokenMatch {
    fn from_claims(
        claims: &HashMap<String, Any>,
        all_key: &str,
        array_key: &str,
    ) -> PyResult<Self> {
        if claims.contains_key(all_key) {
            Ok(TokenMatch::All)
        } else {
            let Some(array) = claims.get(array_key).and_then(|v| v.as_array()) else {
                warn!("Missing claims array key: {array_key}");
                return Err(PyErr::new::<PyValueError, _>(
                    "authentication failed: malformed JWT",
                ));
            };
            Ok(TokenMatch::Some(
                array
                    .iter()
                    .map(|v| v.as_str().unwrap_or_default().to_string())
                    .collect::<HashSet<_>>(),
            ))
        }
    }

    fn matches(&self, value: &str) -> bool {
        match self {
            TokenMatch::All => true,
            TokenMatch::Some(set) => set.contains(value),
            TokenMatch::None => false,
        }
    }
}

#[derive(Debug, Default)]
struct TokenClaims {
    instances: TokenMatch,
    roles: TokenMatch,
    databases: TokenMatch,
}

#[pyfunction]
#[pyo3(signature = (registry, token, user, dbname, instance_name))]
fn validate_gel_token(
    py: Python,
    registry: &JWKSet,
    token: &str,
    user: &str,
    dbname: &str,
    instance_name: &str,
) -> PyResult<Option<String>> {
    let mut token_version = 0;
    let encoded_token = if let Some(stripped) = token.strip_prefix("nbwt1_") {
        token_version = 1;
        stripped
    } else if let Some(stripped) = token.strip_prefix("nbwt_") {
        stripped
    } else if let Some(stripped) = token.strip_prefix("edbt1_") {
        token_version = 1;
        stripped
    } else if let Some(stripped) = token.strip_prefix("edbt_") {
        stripped
    } else {
        warn!(
            "Invalid token prefix: [{}...]",
            &token[0..token.len().min(7)]
        );
        return Ok(Some("authentication failed: malformed JWT".to_string()));
    };

    // Validate and decode the JWT
    let decoded = match registry.registry.validate(
        encoded_token,
        &registry.default_validation_ctx.borrow(py).context,
    ) {
        Ok(claims) => claims,
        Err(e) => {
            warn!("Invalid token: {}", e.error_string_not_for_user());
            return Ok(Some(
                "authentication failed: Verification failed".to_string(),
            ));
        }
    };

    let claims = if token_version == 0 {
        // Legacy v0 token: "edgedb.server.any_role" is a boolean, "edgedb.server.roles" is an array of strings
        let roles =
            TokenMatch::from_claims(&decoded, "edgedb.server.any_role", "edgedb.server.roles")?;
        TokenClaims {
            roles,
            instances: TokenMatch::All,
            databases: TokenMatch::All,
        }
    } else {
        // New v1 token: "edb.{i,r,d}.all" are booleans, "edb.{i,r,d}" are arrays of strings
        let instances = TokenMatch::from_claims(&decoded, "edb.i.all", "edb.i")?;
        let roles = TokenMatch::from_claims(&decoded, "edb.r.all", "edb.r")?;
        let databases = TokenMatch::from_claims(&decoded, "edb.d.all", "edb.d")?;
        TokenClaims {
            instances,
            roles,
            databases,
        }
    };

    if !claims.instances.matches(instance_name) {
        warn!("Instance not in token: {instance_name}");
        return Ok(Some(
            "authentication failed: secret key does not authorize access to this instance"
                .to_string(),
        ));
    }
    if !claims.roles.matches(user) {
        warn!("Role not in token: {user}");
        return Ok(Some(format!(
            "authentication failed: secret key does not authorize access in role {user:?}"
        )));
    }
    if !claims.databases.matches(dbname) {
        warn!("Database not in token: {dbname}");
        return Ok(Some(format!(
            "authentication failed: secret key does not authorize access to database {dbname:?}"
        )));
    }

    Ok(None)
}

#[pymodule]
pub fn _jwt(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<JWKSet>()?;
    m.add_class::<JWKSetCache>()?;
    m.add_class::<SigningCtx>()?;
    m.add_class::<ValidationCtx>()?;
    m.add_function(wrap_pyfunction!(generate_gel_token, m)?)?;
    m.add_function(wrap_pyfunction!(validate_gel_token, m)?)?;
    Ok(())
}
