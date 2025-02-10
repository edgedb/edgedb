use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    time::Duration,
};

use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct SigningContext {
    pub expiry: Option<Duration>,
    pub issuer: Option<String>,
    pub audience: Option<String>,
    pub not_before: Option<Duration>,
}

#[derive(Clone, Serialize, Deserialize, Default, Debug, PartialEq, Eq)]
pub enum ValidationType {
    /// Require the claim to be absent and fail if it is present.
    Reject,
    /// Ignore the claim.
    Ignore,
    /// If the claim is present, it must be valid.
    #[default]
    Allow,
    /// Require the claim to be present and be valid.
    Require,
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct ValidationContext {
    pub allow_list: HashMap<String, HashSet<String>>,
    pub deny_list: HashMap<String, HashSet<String>>,
    pub claims: HashMap<String, ValidationType>,
    pub expiry: ValidationType,
    pub not_before: ValidationType,
}

impl ValidationContext {
    pub fn require_claim_with_allow_list(&mut self, claim: &str, values: &[&str]) {
        self.claims
            .insert(claim.to_string(), ValidationType::Require);
        self.allow_list.insert(
            claim.to_string(),
            values.iter().map(|s| s.to_string()).collect(),
        );
    }

    pub fn require_claim_with_deny_list(&mut self, claim: &str, values: &[&str]) {
        self.claims
            .insert(claim.to_string(), ValidationType::Require);
        self.deny_list.insert(
            claim.to_string(),
            values.iter().map(|s| s.to_string()).collect(),
        );
    }

    pub fn require_claim(&mut self, claim: &str) {
        self.claims
            .insert(claim.to_string(), ValidationType::Require);
    }

    pub fn reject_claim(&mut self, claim: &str) {
        self.claims
            .insert(claim.to_string(), ValidationType::Reject);
    }

    pub fn ignore_claim(&mut self, claim: &str) {
        self.claims
            .insert(claim.to_string(), ValidationType::Ignore);
    }

    pub fn allow_claim(&mut self, claim: &str) {
        self.claims.insert(claim.to_string(), ValidationType::Allow);
    }
}

/// A type similar to `serde_json::Value` that can be serialized and deserialized
/// from a JWT token.
#[derive(Clone, serde::Serialize, serde::Deserialize, Debug, PartialEq)]
#[serde(untagged)]
pub enum Any {
    None,
    String(Cow<'static, str>),
    Bool(bool),
    Number(isize),
    Array(Vec<Any>),
    Object(HashMap<Cow<'static, str>, Any>),
}

impl Any {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Any::String(s) => Some(s.as_ref()),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&[Any]> {
        match self {
            Any::Array(a) => Some(a),
            _ => None,
        }
    }

    pub fn as_object(&self) -> Option<&HashMap<Cow<'static, str>, Any>> {
        match self {
            Any::Object(o) => Some(o),
            _ => None,
        }
    }
}

impl From<bool> for Any {
    fn from(value: bool) -> Self {
        Any::Bool(value)
    }
}

impl From<&'static str> for Any {
    fn from(value: &'static str) -> Self {
        Any::String(Cow::Borrowed(value))
    }
}

impl From<String> for Any {
    fn from(value: String) -> Self {
        Any::String(Cow::Owned(value))
    }
}

impl<T> From<Option<T>> for Any
where
    T: Into<Any>,
{
    fn from(value: Option<T>) -> Self {
        value.map(T::into).unwrap_or(Any::None)
    }
}

impl<T> From<Vec<T>> for Any
where
    T: Into<Any>,
{
    fn from(value: Vec<T>) -> Self {
        Any::Array(value.into_iter().map(T::into).collect())
    }
}

#[cfg(feature = "python_extension")]
impl<'py> pyo3::FromPyObject<'py> for Any {
    fn extract_bound(ob: &pyo3::Bound<'py, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        use pyo3::types::PyAnyMethods;
        if ob.is_none() {
            return Ok(Any::None);
        }
        if let Ok(value) = ob.extract::<bool>() {
            return Ok(Any::Bool(value));
        }
        if let Ok(value) = ob.extract::<isize>() {
            return Ok(Any::Number(value));
        }
        if let Ok(value) = ob.extract::<String>() {
            return Ok(Any::String(Cow::Owned(value)));
        }
        let res: Result<pyo3::Bound<pyo3::types::PyList>, pyo3::PyErr> = ob.extract();
        if let Ok(list) = res {
            let mut items = Vec::new();
            for item in list {
                items.push(Any::extract_bound(&item)?);
            }
            return Ok(Any::Array(items));
        }
        let res: Result<pyo3::Bound<pyo3::types::PyDict>, pyo3::PyErr> = ob.extract();
        if let Ok(dict) = res {
            let mut items = HashMap::new();
            for (k, v) in dict {
                items.insert(Cow::Owned(k.extract::<String>()?), Any::extract_bound(&v)?);
            }
            return Ok(Any::Object(items));
        }
        Err(pyo3::PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Invalid Any value",
        ))
    }
}

#[cfg(feature = "python_extension")]
impl<'py> pyo3::IntoPyObject<'py> for Any {
    type Target = pyo3::PyAny;
    type Output = pyo3::Bound<'py, pyo3::PyAny>;
    type Error = pyo3::PyErr;
    fn into_pyobject(self, py: pyo3::Python<'py>) -> Result<Self::Output, Self::Error> {
        use pyo3::IntoPyObjectExt;

        Ok(match self {
            Any::None => py.None(),
            Any::String(s) => s.as_ref().into_py_any(py)?,
            Any::Bool(b) => b.into_py_any(py)?,
            Any::Number(n) => n.into_py_any(py)?,
            Any::Array(a) => a.into_py_any(py)?,
            Any::Object(o) => o.into_py_any(py)?,
        }
        .into_bound(py))
    }
}
