//! Parses DSNs for database connections. There are some small differences with
//! how `libpq` works:
//!
//!  - Unrecognized options are supported and collected in a map.
//!  - `database` is recognized as an alias for `dbname`
//!  - `[host1,host2]` is considered valid for psql
use std::{borrow::Cow, collections::HashMap};
use thiserror::Error;

mod host;
mod params;
mod passfile;
mod raw_params;
mod url;

pub use host::{Host, HostType, ToAddrsSyncVec};
pub use params::{ConnectionParameters, Ssl, SslParameters};
pub use passfile::{Password, PasswordWarning};
pub use raw_params::{RawConnectionParameters, SslMode, SslVersion};
pub use url::{parse_postgres_dsn, parse_postgres_dsn_env};

pub trait UserProfile {
    fn username(&self) -> Option<Cow<str>>;
    fn homedir(&self) -> Option<Cow<str>>;
}

pub trait EnvVar {
    fn read(&self, name: &'static str) -> Option<Cow<str>>;
}

impl<K, V> EnvVar for HashMap<K, V>
where
    K: std::hash::Hash + Eq + std::borrow::Borrow<str>,
    V: std::borrow::Borrow<str>,
{
    fn read(&self, name: &'static str) -> Option<Cow<str>> {
        self.get(name).map(|value| value.borrow().into())
    }
}

impl EnvVar for std::env::Vars {
    fn read(&self, name: &'static str) -> Option<Cow<str>> {
        if let Ok(value) = std::env::var(name) {
            Some(value.into())
        } else {
            None
        }
    }
}

impl EnvVar for &[(&str, &str)] {
    fn read(&self, name: &'static str) -> Option<Cow<str>> {
        for (key, value) in self.iter() {
            if *key == name {
                return Some((*value).into());
            }
        }
        None
    }
}

impl EnvVar for () {
    fn read(&self, _: &'static str) -> Option<Cow<str>> {
        None
    }
}

#[derive(Error, Debug, PartialEq, Eq)]
#[allow(clippy::enum_variant_names)]
pub enum ParseError {
    #[error(
        "Invalid DSN: scheme is expected to be either \"postgresql\" or \"postgres\", got {0}"
    )]
    InvalidScheme(String),

    #[error("Invalid value for parameter \"{0}\": \"{1}\"")]
    InvalidParameter(String, String),

    #[error("Invalid percent encoding")]
    InvalidPercentEncoding,

    #[error("Invalid port: \"{0}\"")]
    InvalidPort(String),

    #[error("Unexpected number of ports, must be either a single port or the same number as the host count: \"{0}\"")]
    InvalidPortCount(String),

    #[error("Invalid hostname: \"{0}\"")]
    InvalidHostname(String),

    #[error("Invalid query parameter: \"{0}\"")]
    InvalidQueryParameter(String),

    #[error("Invalid TLS version: \"{0}\"")]
    InvalidTLSVersion(String),

    #[error("Could not determine the connection {0}")]
    MissingRequiredParameter(String),

    #[error("URL parse error: {0}")]
    UrlParseError(#[from] ::url::ParseError),
}
