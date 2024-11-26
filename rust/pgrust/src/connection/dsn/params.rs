use super::{
    parse_postgres_dsn, EnvVar, Host, HostType, ParseError, Password, RawConnectionParameters,
    SslMode, SslVersion,
};
use serde_derive::Serialize;
use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::Duration;

impl<'a, I: Into<RawConnectionParameters<'a>>, E: EnvVar> TryInto<ConnectionParameters> for (I, E) {
    type Error = ParseError;
    fn try_into(self) -> Result<ConnectionParameters, ParseError> {
        let mut raw = self.0.into();
        raw.apply_env(self.1)?;
        raw.try_into()
    }
}

impl TryInto<ConnectionParameters> for String {
    type Error = ParseError;
    fn try_into(self) -> Result<ConnectionParameters, ParseError> {
        let params = parse_postgres_dsn(&self)?;
        params.try_into()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct ConnectionParameters {
    pub hosts: Vec<Host>,
    pub database: String,
    pub user: String,
    pub password: Password,
    pub connect_timeout: Option<Duration>,
    pub server_settings: HashMap<String, String>,
    pub ssl: Ssl,
}

impl From<ConnectionParameters> for RawConnectionParameters<'static> {
    fn from(val: ConnectionParameters) -> Self {
        let mut raw_params = RawConnectionParameters::default();

        if !val.hosts.is_empty() {
            let hosts: Vec<Option<HostType>> =
                val.hosts.iter().map(|h| Some(h.0.clone())).collect();
            raw_params.host = Some(hosts);
            let ports: Vec<Option<u16>> = val.hosts.iter().map(|h| Some(h.1)).collect();
            raw_params.port = Some(ports);
        }

        raw_params.dbname = Some(val.database.into());
        raw_params.user = Some(val.user.into());

        match val.password {
            Password::Specified(ref pw) => {
                raw_params.password = Some(pw.to_string().into());
            }
            Password::Passfile(ref path) => {
                raw_params.passfile = Some(path.clone().into());
            }
            _ => {}
        }

        if let Some(timeout) = val.connect_timeout {
            raw_params.connect_timeout = Some(timeout.as_secs() as isize);
        }

        match val.ssl {
            Ssl::Disable => {
                raw_params.sslmode = Some(SslMode::Disable);
            }
            Ssl::Enable(mode, ref params) => {
                raw_params.sslmode = Some(mode);
                if let Some(ref cert) = params.cert {
                    raw_params.sslcert = Some(cert.clone().into());
                }
                if let Some(ref key) = params.key {
                    raw_params.sslkey = Some(key.clone().into());
                }
                if let Some(ref password) = params.password {
                    raw_params.sslpassword = Some(password.to_string().into());
                }
                if let Some(ref rootcert) = params.rootcert {
                    raw_params.sslrootcert = Some(rootcert.clone().into());
                }
                if let Some(ref crl) = params.crl {
                    raw_params.sslcrl = Some(crl.clone().into());
                }
                raw_params.ssl_min_protocol_version = params.min_protocol_version;
                raw_params.ssl_max_protocol_version = params.max_protocol_version;
            }
        }

        raw_params.server_settings = Some(
            val.server_settings
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        );

        raw_params
    }
}

impl TryFrom<RawConnectionParameters<'_>> for ConnectionParameters {
    type Error = ParseError;

    fn try_from(raw_params: RawConnectionParameters<'_>) -> Result<Self, Self::Error> {
        fn merge_hosts_and_ports(
            host_types: &[Option<HostType>],
            mut specified_ports: &[Option<u16>],
        ) -> Result<Vec<Host>, ParseError> {
            let mut hosts = vec![];

            if host_types.is_empty() {
                return merge_hosts_and_ports(
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

        let hosts = merge_hosts_and_ports(
            &raw_params.host.unwrap_or_default(),
            &raw_params.port.unwrap_or_default(),
        )?;

        if hosts.is_empty() {
            return Err(ParseError::MissingRequiredParameter("host".to_string()));
        }

        let user = raw_params
            .user
            .ok_or_else(|| ParseError::MissingRequiredParameter("user".to_string()))?;
        let database = raw_params.dbname.unwrap_or_else(|| user.clone());

        let password = match raw_params.password {
            Some(p) => Password::Specified(p.into_owned()),
            None => match raw_params.passfile {
                Some(passfile) => Password::Passfile(passfile.into_owned()),
                None => Password::Unspecified,
            },
        };

        let connect_timeout = raw_params.connect_timeout.and_then(|seconds| {
            if seconds <= 0 {
                None
            } else {
                Some(Duration::from_secs(seconds.max(2) as u64))
            }
        });

        let any_tcp = hosts
            .iter()
            .any(|host| matches!(host.0, HostType::Hostname(..) | HostType::IP(..)));

        let ssl_mode = raw_params.sslmode.unwrap_or({
            if any_tcp {
                SslMode::Prefer
            } else {
                SslMode::Disable
            }
        });

        let ssl = if ssl_mode == SslMode::Disable {
            Ssl::Disable
        } else {
            let mut ssl = SslParameters::default();
            if ssl_mode >= SslMode::Require {
                ssl.rootcert = raw_params.sslrootcert.map(|s| s.into_owned());
                ssl.crl = raw_params.sslcrl.map(|s| s.into_owned());
            }
            ssl.key = raw_params.sslkey.map(|s| s.into_owned());
            ssl.cert = raw_params.sslcert.map(|s| s.into_owned());
            ssl.min_protocol_version = raw_params.ssl_min_protocol_version;
            ssl.max_protocol_version = raw_params.ssl_max_protocol_version;
            ssl.password = raw_params.sslpassword.map(|s| s.into_owned());
            ssl.keylog_filename = raw_params.keylog_filename.map(|s| s.into_owned());
            Ssl::Enable(ssl_mode, ssl)
        };

        Ok(ConnectionParameters {
            hosts,
            database: database.into_owned(),
            user: user.into_owned(),
            password,
            connect_timeout,
            server_settings: raw_params
                .server_settings
                .unwrap_or_default()
                .into_iter()
                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                .collect(),
            ssl,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[allow(clippy::large_enum_variant)]
pub enum Ssl {
    #[default]
    Disable,
    Enable(SslMode, SslParameters),
}

#[derive(Default, Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SslParameters {
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
    pub password: Option<String>,
    pub rootcert: Option<PathBuf>,
    pub crl: Option<PathBuf>,
    pub min_protocol_version: Option<SslVersion>,
    pub max_protocol_version: Option<SslVersion>,
    pub keylog_filename: Option<PathBuf>,
}

impl Ssl {
    /// Resolve the SSL paths relative to the home directory.
    pub fn resolve(&mut self, home_dir: &Path) -> Result<(), std::io::Error> {
        let postgres_dir = home_dir;
        let Ssl::Enable(mode, params) = self else {
            return Ok(());
        };
        if *mode >= SslMode::Require {
            let root_cert = params
                .rootcert
                .clone()
                .unwrap_or_else(|| postgres_dir.join("root.crt"));
            if root_cert.exists() {
                params.rootcert = Some(root_cert);
            } else if *mode > SslMode::Require {
                return Err(std::io::Error::new(ErrorKind::NotFound,
                    format!("Root certificate not found: {root_cert:?}. Either provide the file or change sslmode to disable SSL certificate verification.")));
            }

            let crl = params
                .crl
                .clone()
                .unwrap_or_else(|| postgres_dir.join("root.crl"));
            if crl.exists() {
                params.crl = Some(crl);
            }
        }
        let key = params
            .key
            .clone()
            .unwrap_or_else(|| postgres_dir.join("postgresql.key"));
        if key.exists() {
            params.key = Some(key);
        }
        let cert = params
            .cert
            .clone()
            .unwrap_or_else(|| postgres_dir.join("postgresql.crt"));
        if cert.exists() {
            params.cert = Some(cert);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{HostType, ParseError};
    use rstest::rstest;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    #[rstest]
    #[case("example.com", HostType::Hostname("example.com".to_string()))]
    // This should probably parse as IPv4
    #[case("0", HostType::Hostname("0".to_string()))]
    #[case(
        "192.168.1.1",
        HostType::IP(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), None)
    )]
    #[case(
        "2001:db8::1",
        HostType::IP(IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)), None)
    )]
    #[case("2001:db8::1%eth0", HostType::IP(IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)), Some("eth0".to_string())))]
    #[case("/var/run/postgresql", HostType::Path("/var/run/postgresql".to_string()))]
    #[case("@abstract", HostType::Abstract("abstract".to_string()))]
    fn test_host_type_roundtrip(#[case] input: &str, #[case] expected: HostType) {
        let parsed = HostType::try_from_str(input).unwrap();
        assert_eq!(parsed, expected, "{input} should have succeeded");
        assert_eq!(parsed.to_string(), input, "{input} should have succeeded");
    }

    #[rstest]
    #[case("", ParseError::InvalidHostname("".to_string()))]
    #[case("example.com:80", ParseError::InvalidHostname("example.com:80".to_string()))]
    #[case("[::1]", ParseError::InvalidHostname("[::1]".to_string()))]
    #[case("2001:db8::1%", ParseError::InvalidHostname("2001:db8::1%".to_string()))]
    #[case("not:valid:ipv6", ParseError::InvalidHostname("not:valid:ipv6".to_string()))]
    fn test_host_type_failures(#[case] input: &str, #[case] expected_error: ParseError) {
        let result = HostType::try_from_str(input);
        assert!(result.is_err(), "{input} should have failed");
        assert_eq!(
            result.unwrap_err(),
            expected_error,
            "{input} should have failed"
        );
    }
}
