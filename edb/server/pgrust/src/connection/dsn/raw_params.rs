use super::{Host, HostType, ParseError};
use serde_derive::Serialize;
use std::borrow::Cow;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Convert from an environment variable or query string to the parameter type.
trait FromEnv
where
    Self: Sized,
{
    fn from(env: Cow<str>) -> Result<Self, ParseError>;
}

macro_rules! from_env_impl {
    ($ty:ty: $expr:expr) => {
        impl FromEnv for $ty {
            fn from(env: Cow<str>) -> Result<Self, ParseError> {
                ($expr(env))
            }
        }
    };
}

// Define one of these per param type
from_env_impl!(Vec<Option<HostType>>: |e: Cow<str>| parse_host_param(&e));
from_env_impl!(Vec<Option<u16>>: |e: Cow<str>| parse_port_param(&e));
from_env_impl!(Cow<'_, str>: |e: Cow<str>| Ok(e.into_owned().into()));
from_env_impl!(Cow<'_, Path>: |e: Cow<str>| Ok(PathBuf::from(e.into_owned()).into()));
from_env_impl!(isize: |e: Cow<str>| parse_connect_timeout(e));
from_env_impl!(bool: |e: Cow<str>| Ok(e == "1" || e == "true" || e == "on" || e == "yes"));
from_env_impl!(SslMode: |e: Cow<str>| SslMode::try_from(e.as_ref()));
from_env_impl!(SslVersion: |e: Cow<str>| e.try_into());

trait ToEnv {
    fn to(&self) -> Cow<str>;
}

macro_rules! to_env_impl {
    ($ty:ty: |$self:ident| $expr:expr) => {
        impl ToEnv for $ty {
            fn to(&self) -> Cow<str> {
                let $self = self;
                $expr
            }
        }
    };
}

to_env_impl!(Cow<'_, str>: |e| Cow::Borrowed(e));
to_env_impl!(Cow<'_, Path>: |e| e.to_string_lossy());
to_env_impl!(Vec<Option<HostType>>: |e| {
    Cow::Owned(e.iter().map(|h| match h {
        Some(ht) => ht.to_string(),
        None => String::new(),
    }).collect::<Vec<_>>().join(","))
});
to_env_impl!(Vec<Option<u16>>: |e| {
    Cow::Owned(e.iter().map(|p| p.map_or(String::new(), |v| v.to_string()))
        .collect::<Vec<_>>().join(","))
});
to_env_impl!(isize: |e| Cow::Owned(e.to_string()));
to_env_impl!(bool: |e| Cow::Owned(if *e { "1" } else { "0" }.to_string()));
to_env_impl!(SslMode: |e| Cow::Owned(e.to_string()));
to_env_impl!(SslVersion: |e| Cow::Owned(e.to_string()));

trait RawToOwned {
    type Owned;
    fn raw_to_owned(&self) -> Self::Owned;
}

impl<'a, T: ?Sized> RawToOwned for Cow<'a, T>
where
    T: ToOwned + 'static,
    Cow<'static, T>: From<<T as ToOwned>::Owned>,
{
    type Owned = Cow<'static, T>;
    fn raw_to_owned(&self) -> <Self as RawToOwned>::Owned {
        ToOwned::to_owned(self.as_ref()).into()
    }
}

macro_rules! trivial_raw_to_owned {
    ($ty:ident $(< $($generic:ident),* >)?) => {
        impl $(<$($generic),*>)? RawToOwned for $ty $(<$($generic),*>)? where Self: Clone {
            type Owned = Self;
            fn raw_to_owned(&self) -> Self::Owned {
                self.clone()
            }
        }
    };
}

trivial_raw_to_owned!(Vec<T>);
trivial_raw_to_owned!(isize);
trivial_raw_to_owned!(bool);
trivial_raw_to_owned!(SslMode);
trivial_raw_to_owned!(SslVersion);

macro_rules! define_params {
    ($lifetime:lifetime, $( #[doc = $doc:literal]  $name:ident: $ty:ty $(, env = $env:literal)? $(, query_only = $query_only:ident)?; )* ) => {
        /// [`RawConnectionParameters`] represents the raw, parsed connection parameters.
        ///
        /// These parameters map directly to the parameters in the DSN and perform only
        /// basic validation.
        #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
        pub struct RawConnectionParameters<$lifetime> {
            $(
                #[doc = $doc]
                pub $name: Option<$ty>,
            )*
            /// Any additional settings we don't recognize
            pub server_settings: Option<HashMap<Cow<'a, str>, Cow<'a, str>>>,
        }

        impl<'a> From<RawConnectionParameters<$lifetime>> for HashMap<String, String> {
            fn from(params: RawConnectionParameters<$lifetime>) -> HashMap<String, String> {
                let mut map = HashMap::new();

                $(
                    if let Some(value) = params.$name {
                        map.insert(stringify!($name).to_string(), <$ty as ToEnv>::to(&value).into_owned());
                    }
                )*

                if let Some(server_settings) = params.server_settings {
                    map.extend(server_settings.into_iter().map(|(k, v)| (k.into_owned(), v.into_owned())));
                }

                map
            }
        }

        impl <$lifetime> RawConnectionParameters<$lifetime> {
            pub fn to_static(&self) -> RawConnectionParameters<'static> {
                $(
                    let $name = self.$name.as_ref().map(|v| v.raw_to_owned());
                )*

                let server_settings = self.server_settings.as_ref().map(|m| {
                    m.iter().map(|(k, v)| (k.raw_to_owned(), v.raw_to_owned())).collect()
                });

                RawConnectionParameters::<'static> {
                    $(
                        $name,
                    )*
                    server_settings,
                }
            }

            /// Apply environment variables to the parameters.
            pub fn apply_env(&mut self, env: impl crate::connection::dsn::EnvVar) -> Result<(), ParseError> {
                $(
                    $(
                        if self.$name.is_none() {
                            if let Some(env_value) = env.read($env) {
                                self.$name = Some(FromEnv::from(env_value)?);
                            }
                        }
                    )?
                )*
                Ok(())
            }

            /// Set a parameter by query string name.
            pub fn set_by_name(&mut self, name: &str, value: Cow<'a, str>) -> Result<(), ParseError> {
                match name {
                    $(
                        stringify!($name) => {
                            self.$name = Some(FromEnv::from(value)?);
                        },
                    )*
                    _ => {
                        self.server_settings
                            .get_or_insert_with(HashMap::new)
                            .insert(Cow::Owned(name.to_string()), value);
                    }
                }
                Ok(())
            }

            /// Get a parameter by query string name.
            pub fn get_by_name(&self, name: &str) -> Option<Cow<str>> {
                match name {
                    $(
                        stringify!($name) => {
                            self.$name.as_ref().map(|value| <$ty as ToEnv>::to(&value))
                        },
                    )*
                    _ => {
                        self.server_settings
                            .as_ref()
                            .and_then(|settings| settings.get(name))
                            .map(|value| <Cow<str> as ToEnv>::to(&value))
                    }
                }
            }

            /// Visit the query-only parameters. These are the parameters that never appears anywhere other than in the query string.
            pub(crate) fn visit_query_only(&self, mut f: impl for<'b> FnMut(&'b str, &'b str)) {
                $(
                    $(
                        stringify!($query_only);
                        if let Some(value) = &self.$name {
                            f(stringify!($name), &value.to());
                        }
                    )?
                )*

                if let Some(settings) = &self.server_settings {
                    for (key, value) in settings {
                        f(key, value);
                    }
                }
            }

            /// Returns all field names as a vector of static string slices.
            pub fn field_names() -> Vec<&'static str> {
                vec![
                    $(
                        stringify!($name),
                    )*
                ]
            }
        }
    };
}

impl<'a> RawConnectionParameters<'a> {
    pub fn hosts(&self) -> Result<Vec<Host>, ParseError> {
        Self::merge_hosts_and_ports(
            self.host.as_deref().unwrap_or_default(),
            self.port.as_deref().unwrap_or_default(),
        )
    }

    fn merge_hosts_and_ports(
        host_types: &[Option<HostType>],
        mut specified_ports: &[Option<u16>],
    ) -> Result<Vec<Host>, ParseError> {
        let mut hosts = vec![];

        if host_types.is_empty() {
            return Self::merge_hosts_and_ports(
                &[
                    Some(HostType::Path("/var/run/postgresql".to_string())),
                    Some(HostType::Path("/run/postgresql".to_string())),
                    Some(HostType::Path("/tmp".to_string())),
                    Some(HostType::Path("/private/tmp".to_string())),
                    Some(HostType::Hostname("localhost".to_string())),
                ],
                specified_ports,
            );
        }

        if specified_ports.is_empty() {
            specified_ports = &[Some(5432)];
        } else if specified_ports.len() != host_types.len() && specified_ports.len() > 1 {
            return Err(ParseError::InvalidPortCount(format!("{specified_ports:?}")));
        }

        for (i, host_type) in host_types.iter().enumerate() {
            let host_type = host_type
                .clone()
                .unwrap_or_else(|| HostType::Path("/var/run/postgresql".to_string()));
            let port = specified_ports[i % specified_ports.len()].unwrap_or(5432);

            hosts.push(Host(host_type, port));
        }
        Ok(hosts)
    }
}

define_params!('a,
    /// The host to connect to.
    host: Vec<Option<HostType>>, env = "PGHOST";
    /// The port to connect to.
    port: Vec<Option<u16>>, env = "PGPORT";
    /// The database to connect to.
    dbname: Cow<'a, str>, env = "PGDATABASE";
    /// The user to connect as.
    user: Cow<'a, str>, env = "PGUSER";
    /// The password to use when connecting.
    password: Cow<'a, str>, env = "PGPASSWORD";

    /// The path to the passfile.
    passfile: Cow<'a, Path>, env = "PGPASSFILE", query_only = query_only;
    /// The timeout for the connection to be established.
    connect_timeout: isize, env = "PGCONNECT_TIMEOUT", query_only = query_only;
    /// The SSL mode to use.
    sslmode: SslMode, env = "PGSSLMODE", query_only = query_only;
    /// The SSL certificate to use.
    sslcert: Cow<'a, Path>, env = "PGSSLCERT", query_only = query_only;
    /// The SSL key to use.
    sslkey: Cow<'a, Path>, env = "PGSSLKEY", query_only = query_only;
    /// The SSL password to use.
    sslpassword: Cow<'a, str>, query_only = query_only;
    /// The SSL root certificate to use.
    sslrootcert: Cow<'a, Path>, env = "PGSSLROOTCERT", query_only = query_only;
    /// The path to the CRL file.
    sslcrl: Cow<'a, Path>, env = "PGSSLCRL", query_only = query_only;
    /// The minimum SSL protocol version to use.
    ssl_min_protocol_version: SslVersion, env = "PGSSLMINPROTOCOLVERSION", query_only = query_only;
    /// The maximum SSL protocol version to use.
    ssl_max_protocol_version: SslVersion, env = "PGSSLMAXPROTOCOLVERSION", query_only = query_only;

    /// The path to the file for TLS key log.
    keylog_filename: Cow<'a, Path>;
);

impl RawConnectionParameters<'_> {
    pub fn to_url(&self) -> String {
        super::url::params_to_url(self)
    }
}

/// SSL mode for PostgreSQL connections.
///
/// For more information, see the [PostgreSQL documentation](https://www.postgresql.org/docs/current/libpq-ssl.html).
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum SslMode {
    /// "I don't care about security, and I don't want to pay the overhead of encryption."
    #[serde(rename = "disable")]
    Disable,
    /// "I don't care about security, but I will pay the overhead of encryption if the server insists on it."
    #[serde(rename = "allow")]
    Allow,
    /// "I don't care about encryption, but I wish to pay the overhead of  encryption if the server supports it."
    #[serde(rename = "prefer")]
    Prefer,
    /// "I want my data to be encrypted, and I accept the overhead. I trust that the network will make sure I always connect to the server I want."
    #[serde(rename = "require")]
    Require,
    /// "I want my data encrypted, and I accept the overhead. I want to be sure that I connect to a server that I trust."
    #[serde(rename = "verify_ca")]
    VerifyCA,
    /// "I want my data encrypted, and I accept the overhead. I want to be sure that I connect to a server I trust, and that it's the one I specify."
    #[serde(rename = "verify_full")]
    VerifyFull,
}

impl TryFrom<&str> for SslMode {
    type Error = ParseError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "allow" => Ok(SslMode::Allow),
            "prefer" => Ok(SslMode::Prefer),
            "require" => Ok(SslMode::Require),
            "verify_ca" | "verify-ca" => Ok(SslMode::VerifyCA),
            "verify_full" | "verify-full" => Ok(SslMode::VerifyFull),
            "disable" => Ok(SslMode::Disable),
            _ => Err(ParseError::InvalidParameter(
                "sslmode".to_string(),
                s.to_string(),
            )),
        }
    }
}
impl std::fmt::Display for SslMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            SslMode::Disable => "disable",
            SslMode::Allow => "allow",
            SslMode::Prefer => "prefer",
            SslMode::Require => "require",
            SslMode::VerifyCA => "verify-ca",
            SslMode::VerifyFull => "verify-full",
        };
        f.write_str(s)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SslVersion {
    Tls1,
    Tls1_1,
    Tls1_2,
    Tls1_3,
}

impl std::fmt::Display for SslVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            SslVersion::Tls1 => "TLSv1",
            SslVersion::Tls1_1 => "TLSv1.1",
            SslVersion::Tls1_2 => "TLSv1.2",
            SslVersion::Tls1_3 => "TLSv1.3",
        };
        f.write_str(s)
    }
}

impl serde::Serialize for SslVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(match self {
            SslVersion::Tls1 => "TLSv1",
            SslVersion::Tls1_1 => "TLSv1.1",
            SslVersion::Tls1_2 => "TLSv1.2",
            SslVersion::Tls1_3 => "TLSv1.3",
        })
    }
}

impl<'a> TryFrom<Cow<'a, str>> for SslVersion {
    type Error = ParseError;
    fn try_from(value: Cow<str>) -> Result<SslVersion, Self::Error> {
        Ok(match value.to_lowercase().as_ref() {
            "tls_1" | "tlsv1" => SslVersion::Tls1,
            "tls_1.1" | "tlsv1.1" => SslVersion::Tls1_1,
            "tls_1.2" | "tlsv1.2" => SslVersion::Tls1_2,
            "tls_1.3" | "tlsv1.3" => SslVersion::Tls1_3,
            _ => return Err(ParseError::InvalidTLSVersion(value.to_string())),
        })
    }
}

impl From<SslVersion> for openssl::ssl::SslVersion {
    fn from(val: SslVersion) -> Self {
        match val {
            SslVersion::Tls1 => openssl::ssl::SslVersion::TLS1,
            SslVersion::Tls1_1 => openssl::ssl::SslVersion::TLS1_1,
            SslVersion::Tls1_2 => openssl::ssl::SslVersion::TLS1_2,
            SslVersion::Tls1_3 => openssl::ssl::SslVersion::TLS1_3,
        }
    }
}

fn parse_host_param(value: &str) -> Result<Vec<Option<HostType>>, ParseError> {
    value
        .split(',')
        .map(|host| {
            if host.is_empty() {
                Ok(None)
            } else {
                HostType::try_from_str(host).map(Some)
            }
        })
        .collect()
}

fn parse_port_param(port: &str) -> Result<Vec<Option<u16>>, ParseError> {
    port.split(',')
        .map(|port| {
            (!port.is_empty())
                .then(|| str::parse::<u16>(port))
                .transpose()
        })
        .collect::<Result<Vec<Option<u16>>, _>>()
        .map_err(|_| ParseError::InvalidPort(port.to_string()))
}

fn parse_connect_timeout(timeout: Cow<str>) -> Result<isize, ParseError> {
    let seconds = timeout.parse::<isize>().map_err(|_| {
        ParseError::InvalidParameter("connect_timeout".to_string(), timeout.to_string())
    })?;
    Ok(seconds)
}
