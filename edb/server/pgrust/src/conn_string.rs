use itertools::Itertools;
use percent_encoding::percent_decode_str;
use serde_derive::Serialize;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::ErrorKind;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;
use thiserror::Error;
use url::Url;

#[derive(Error, Debug, PartialEq, Eq)]
#[allow(clippy::enum_variant_names)]
pub enum ParseError {
    #[error(
        "Invalid DSN: scheme is expected to be either \"postgresql\" or \"postgres\", got {0}"
    )]
    InvalidScheme(String),

    #[error("Invalid value for parameter \"{0}\": \"{1}\"")]
    InvalidParameter(String, String),

    #[error("Invalid port: \"{0}\"")]
    InvalidPort(String),

    #[error("Unexpected number of ports, must be either a single port or the same number as the host count: \"{0}\"")]
    InvalidPortCount(String),

    #[error("Invalid hostname: \"{0}\"")]
    InvalidHostname(String),

    #[error("Could not determine the connection {0}")]
    MissingRequiredParameter(String),

    #[error("URL parse error: {0}")]
    UrlParseError(#[from] url::ParseError),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub enum Host {
    Hostname(String, u16),
    IP(IpAddr, u16, Option<String>),
    Path(String, u16),
    Abstract(String, u16),
}

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub enum Password {
    /// The password is unspecified and should be read from the user's default
    /// passfile if it exists.
    #[default]
    Unspecified,
    /// The password was specified.
    Specified(String),
    /// The passfile is specified.
    Passfile(PathBuf),
}

#[derive(Serialize)]
pub enum PasswordWarning {
    NotFile(PathBuf),
    NotExists(PathBuf),
    NotAccessible(PathBuf),
    Permissions(PathBuf, u32),
}

impl std::fmt::Display for PasswordWarning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PasswordWarning::NotFile(path) => write!(f, "Password file {path:?} is not a plain file"),
            PasswordWarning::NotExists(path) => write!(f, "Password file {path:?} does not exist"),
            PasswordWarning::NotAccessible(path) => write!(f, "Password file {path:?} is not accessible"),
            PasswordWarning::Permissions(path, mode) => write!(f, "Password file {path:?} has group or world access ({mode:o}); permissions should be u=rw (0600) or less"),
        }
    }
}

#[cfg(windows)]
const PGPASSFILE: &str = "pgpass.conf";
#[cfg(not(windows))]
const PGPASSFILE: &str = ".pgpass";

impl Password {
    pub fn password(&self) -> Option<&str> {
        match self {
            Password::Specified(password) => Some(password),
            _ => None,
        }
    }

    /// Attempt to resolve a password against the given homedir.
    pub fn resolve(
        &mut self,
        home: &Path,
        hosts: &[Host],
        database: &str,
        user: &str,
    ) -> Result<Option<PasswordWarning>, std::io::Error> {
        let passfile = match self {
            Password::Unspecified => {
                let passfile = home.join(PGPASSFILE);
                // Don't warn about implicit missing or inaccessible files
                if !matches!(passfile.try_exists(), Ok(true)) {
                    *self = Password::Unspecified;
                    return Ok(None);
                }
                if !passfile.is_file() {
                    *self = Password::Unspecified;
                    return Ok(None);
                }
                passfile
            }
            Password::Specified(_) => return Ok(None),
            Password::Passfile(passfile) => {
                let passfile = passfile.clone();
                if matches!(passfile.try_exists(), Ok(false)) {
                    *self = Password::Unspecified;
                    return Ok(Some(PasswordWarning::NotExists(passfile)));
                }
                if passfile.exists() && !passfile.is_file() {
                    *self = Password::Unspecified;
                    return Ok(Some(PasswordWarning::NotFile(passfile)));
                }
                passfile
            }
        };

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let metadata = match passfile.metadata() {
                Err(err) if err.kind() == ErrorKind::PermissionDenied => {
                    *self = Password::Unspecified;
                    return Ok(Some(PasswordWarning::NotAccessible(passfile)));
                }
                res => res?,
            };
            let permissions = metadata.permissions();
            let mode = permissions.mode();

            if mode & (0o070) != 0 {
                *self = Password::Unspecified;
                return Ok(Some(PasswordWarning::Permissions(passfile, mode)));
            }
        }

        let file = match OpenOptions::new().read(true).open(&passfile) {
            Err(err) if err.kind() == ErrorKind::PermissionDenied => {
                *self = Password::Unspecified;
                return Ok(Some(PasswordWarning::NotAccessible(passfile)));
            }
            res => res?,
        };
        if let Some(password) = read_password_file(
            hosts,
            database,
            user,
            std::io::read_to_string(file)?.split('\n'),
        ) {
            *self = Password::Specified(password);
        } else {
            *self = Password::Unspecified;
        }
        Ok(None)
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

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[allow(clippy::large_enum_variant)]
pub enum Ssl {
    #[default]
    Disable,
    Enable(SslMode, SslParameters),
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum SslMode {
    #[serde(rename = "allow")]
    Allow,
    #[serde(rename = "prefer")]
    Prefer,
    #[serde(rename = "require")]
    Require,
    #[serde(rename = "verify_ca")]
    VerifyCA,
    #[serde(rename = "verify_full")]
    VerifyFull,
}

#[derive(Default, Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SslParameters {
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
    pub password: Option<String>,
    pub rootcert: Option<PathBuf>,
    pub crl: Option<PathBuf>,
    pub min_protocol_version: Option<String>,
    pub max_protocol_version: Option<String>,
    pub keylog_filename: Option<PathBuf>,
    pub verify_crl_check_chain: Option<bool>,
}

#[derive(Default, Debug, Serialize)]
pub struct SslPaths {
    pub rootcert: Option<PathBuf>,
    pub crl: Option<PathBuf>,
    pub key: Option<PathBuf>,
    pub cert: Option<PathBuf>,
}

impl Ssl {
    /// Resolve the SSL paths relative to the home directory.
    pub fn resolve(&self, home_dir: &Path) -> Result<SslPaths, std::io::Error> {
        let postgres_dir = home_dir;
        let Ssl::Enable(mode, params) = self else {
            return Ok(SslPaths::default());
        };
        let mut paths = SslPaths::default();
        if *mode >= SslMode::Require {
            let root_cert = params
                .rootcert
                .clone()
                .unwrap_or_else(|| postgres_dir.join("root.crt"));
            if root_cert.exists() {
                paths.rootcert = Some(root_cert);
            } else if *mode > SslMode::Require {
                return Err(std::io::Error::new(ErrorKind::NotFound,
                    format!("Root certificate not found: {root_cert:?}. Either provide the file or change sslmode to disable SSL certificate verification.")));
            }

            let crl = params
                .crl
                .clone()
                .unwrap_or_else(|| postgres_dir.join("root.crl"));
            if crl.exists() {
                paths.crl = Some(crl);
            }
        }
        let key = params
            .key
            .clone()
            .unwrap_or_else(|| postgres_dir.join("postgresql.key"));
        if key.exists() {
            paths.key = Some(key);
        }
        let cert = params
            .cert
            .clone()
            .unwrap_or_else(|| postgres_dir.join("postgresql.crt"));
        if cert.exists() {
            paths.cert = Some(cert);
        }
        Ok(paths)
    }
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

fn maybe_decode(str: Cow<str>) -> Cow<str> {
    if str.contains('%') {
        if let Ok(str) = percent_decode_str(&str).decode_utf8() {
            str.into_owned().into()
        } else {
            str.into_owned().into()
        }
    } else {
        str
    }
}

fn parse_port(port: &str) -> Result<u16, ParseError> {
    if port.contains('%') {
        let decoded = percent_decode_str(port)
            .decode_utf8()
            .map_err(|_| ParseError::InvalidPort(port.to_string()))?;
        decoded
            .parse::<u16>()
            .map_err(|_| ParseError::InvalidPort(port.to_string()))
    } else {
        port.parse::<u16>()
            .map_err(|_| ParseError::InvalidPort(port.to_string()))
    }
}

fn parse_hostlist(
    hostspecs: &[&str],
    mut specified_ports: &[u16],
) -> Result<Vec<Host>, ParseError> {
    let mut hosts = vec![];

    if specified_ports.is_empty() {
        specified_ports = &[5432];
    } else if specified_ports.len() != hostspecs.len() && specified_ports.len() > 1 {
        return Err(ParseError::InvalidPortCount(format!("{specified_ports:?}")));
    }

    for (i, hostspec) in hostspecs.iter().enumerate() {
        let port = specified_ports[i % specified_ports.len()];

        let host = if hostspec.starts_with('/') {
            Host::Path(hostspec.to_string(), port)
        } else if hostspec.starts_with('[') {
            // Handling IPv6 address
            let end_bracket = hostspec
                .find(']')
                .ok_or_else(|| ParseError::InvalidHostname(hostspec.to_string()))?;

            // Extract interface (optional) after %
            let (interface, ipv6_part, port_part) = if let Some(pos) = hostspec.find('%') {
                (
                    Some(hostspec[pos + 1..end_bracket].to_string()),
                    &hostspec[1..pos],
                    &hostspec[end_bracket + 1..],
                )
            } else {
                (
                    None,
                    &hostspec[1..end_bracket],
                    &hostspec[end_bracket + 1..],
                )
            };
            let addr = Ipv6Addr::from_str(ipv6_part)
                .map_err(|_| ParseError::InvalidHostname(hostspec.to_string()))?;

            let port = if let Some(stripped) = port_part.strip_prefix(':') {
                parse_port(stripped)?
            } else {
                port
            };
            Host::IP(IpAddr::V6(addr), port, interface)
        } else {
            let parts: Vec<&str> = hostspec.split(':').collect();
            let addr = parts[0].to_string();
            let port = if parts.len() > 1 {
                parse_port(parts[1])?
            } else {
                port
            };

            if let Ok(ip) = Ipv4Addr::from_str(&addr) {
                Host::IP(IpAddr::V4(ip), port, None)
            } else {
                Host::Hostname(addr, port)
            }
        };

        hosts.push(host)
    }
    Ok(hosts)
}

pub fn parse_postgres_url(
    url_str: &str,
    env: impl EnvVar,
) -> Result<ConnectionParameters, ParseError> {
    let url_str = if let Some(url) = url_str.strip_prefix("postgres://") {
        url
    } else if let Some(url) = url_str.strip_prefix("postgresql://") {
        url
    } else {
        return Err(ParseError::InvalidScheme(
            url_str.split(':').next().unwrap_or_default().to_owned(),
        ));
    };

    let path_or_query = url_str.find(|c| c == '?' || c == '/');
    let (authority, path_and_query) = match path_or_query {
        Some(index) => url_str.split_at(index),
        None => (url_str, ""),
    };

    let (auth, host) = match authority.split_once('@') {
        Some((auth, host)) => (auth, host),
        None => ("", authority),
    };

    let url = Url::parse(&format!("unused://{auth}@host{path_and_query}"))?;

    let mut server_settings = HashMap::new();
    let mut host: Option<Cow<str>> = if host.is_empty() {
        None
    } else {
        Some(host.into())
    }
    .map(maybe_decode);
    let mut port = None;

    let mut user: Option<Cow<str>> = match url.username() {
        "" => None,
        user => {
            let decoded = percent_decode_str(user);
            if let Ok(user) = decoded.decode_utf8() {
                Some(user)
            } else {
                Some(user.into())
            }
        }
    };
    let mut password: Option<Cow<str>> = url.password().map(|p| p.into()).map(maybe_decode);
    let mut database: Option<Cow<str>> = match url.path() {
        "" | "/" => None,
        path => Some(path.trim_start_matches('/').into()),
    }
    .map(maybe_decode);

    let mut passfile = None;
    let mut connect_timeout = None;

    let mut sslmode = None;
    let mut sslcert = None;
    let mut sslkey = None;
    let mut sslpassword = None;
    let mut sslrootcert = None;
    let mut sslcrl = None;
    let mut ssl_min_protocol_version = None;
    let mut ssl_max_protocol_version = None;

    for (name, value) in url.query_pairs() {
        match name.as_ref() {
            "host" => {
                if host.is_none() {
                    host = Some(value);
                }
            }
            "port" => {
                if port.is_none() {
                    port = Some(
                        value
                            .split(',')
                            .map(parse_port)
                            .collect::<Result<Vec<u16>, _>>()?,
                    );
                }
            }
            "dbname" | "database" => {
                if database.is_none() {
                    database = Some(value);
                }
            }
            "user" => {
                if user.is_none() {
                    user = Some(value);
                }
            }
            "password" => {
                if password.is_none() {
                    password = Some(value);
                }
            }
            "passfile" => passfile = Some(value),
            "connect_timeout" => connect_timeout = Some(value),

            "sslmode" => sslmode = Some(value),
            "sslcert" => sslcert = Some(value),
            "sslkey" => sslkey = Some(value),
            "sslpassword" => sslpassword = Some(value),
            "sslrootcert" => sslrootcert = Some(value),
            "sslcrl" => sslcrl = Some(value),
            "ssl_min_protocol_version" => ssl_min_protocol_version = Some(value),
            "ssl_max_protocol_version" => ssl_max_protocol_version = Some(value),

            name => {
                server_settings.insert(name.to_string(), value.to_string());
            }
        };
    }

    if host.is_none() {
        host = env.read("PGHOST");
    }
    if port.is_none() {
        if let Some(value) = env.read("PGPORT") {
            port = Some(
                value
                    .split(',')
                    .map(parse_port)
                    .collect::<Result<Vec<u16>, _>>()?,
            );
        }
    }

    if host.is_none() {
        host = Some("/run/postgresql,/var/run/postgresql,/tmp,/private/tmp,localhost".into());
    }
    let host = host
        .as_ref()
        .map(|s| s.split(',').collect_vec())
        .unwrap_or_default();
    let hosts = parse_hostlist(&host, port.as_deref().unwrap_or_default())?;

    if hosts.is_empty() {
        return Err(ParseError::MissingRequiredParameter("address".to_string()));
    }

    if user.is_none() {
        user = env.read("PGUSER");
    }
    if password.is_none() {
        password = env.read("PGPASSWORD");
    }
    if database.is_none() {
        database = env.read("PGDATABASE");
    }
    if database.is_none() {
        database = user.clone();
    }

    let Some(user) = user else {
        return Err(ParseError::MissingRequiredParameter("user".to_string()));
    };
    let Some(database) = database else {
        return Err(ParseError::MissingRequiredParameter("database".to_string()));
    };

    let password = match password {
        Some(p) => Password::Specified(p.into_owned()),
        None => {
            if let Some(passfile) = passfile.or_else(|| env.read("PGPASSFILE")) {
                Password::Passfile(passfile.into_owned().into())
            } else {
                Password::Unspecified
            }
        }
    };

    if connect_timeout.is_none() {
        connect_timeout = env.read("PGCONNECT_TIMEOUT");
    }

    // Match the same behavior of libpq
    // https://www.postgresql.org/docs/current/libpq-connect.html
    let connect_timeout = match connect_timeout {
        None => None,
        Some(s) => {
            let seconds = s.parse::<isize>().map_err(|_| {
                ParseError::InvalidParameter("connect_timeout".to_string(), s.to_string())
            })?;
            if seconds <= 0 {
                None
            } else {
                Some(Duration::from_secs(seconds.max(2) as _))
            }
        }
    };

    let any_tcp = hosts
        .iter()
        .any(|host| matches!(host, Host::Hostname(..) | Host::IP(..)));

    if sslmode.is_none() {
        sslmode = env.read("PGSSLMODE");
    }

    if sslmode.is_none() && any_tcp {
        sslmode = Some("prefer".into());
    }

    let ssl = if let Some(sslmode) = sslmode {
        if sslmode == "disable" {
            Ssl::Disable
        } else {
            let sslmode = match sslmode.as_ref() {
                "allow" => SslMode::Allow,
                "prefer" => SslMode::Prefer,
                "require" => SslMode::Require,
                "verify_ca" | "verify-ca" => SslMode::VerifyCA,
                "verify_full" | "verify-full" => SslMode::VerifyFull,
                _ => {
                    return Err(ParseError::InvalidParameter(
                        "sslmode".to_string(),
                        sslmode.to_string(),
                    ))
                }
            };
            let mut ssl = SslParameters::default();
            if sslmode >= SslMode::Require {
                if sslrootcert.is_none() {
                    sslrootcert = env.read("PGSSLROOTCERT");
                }
                ssl.rootcert = sslrootcert.map(|s| PathBuf::from(s.into_owned()));
                if sslcrl.is_none() {
                    sslcrl = env.read("PGSSLCRL");
                }
                ssl.crl = sslcrl.map(|s| PathBuf::from(s.into_owned()));
            }
            if sslkey.is_none() {
                sslkey = env.read("PGSSLKEY");
            }
            ssl.key = sslkey.map(|s| PathBuf::from(s.into_owned()));
            if sslcert.is_none() {
                sslcert = env.read("PGSSLCERT");
            }
            ssl.cert = sslcert.map(|s| PathBuf::from(s.into_owned()));
            if ssl_min_protocol_version.is_none() {
                ssl_min_protocol_version = env.read("PGSSLMINPROTOCOLVERSION");
            }
            ssl.min_protocol_version = ssl_min_protocol_version.map(|s| s.into_owned());
            if ssl_max_protocol_version.is_none() {
                ssl_max_protocol_version = env.read("PGSSLMAXPROTOCOLVERSION");
            }
            ssl.max_protocol_version = ssl_max_protocol_version.map(|s| s.into_owned());

            // There is no environment variable equivalent to this option
            ssl.password = sslpassword.map(|s| s.into_owned());

            Ssl::Enable(sslmode, ssl)
        }
    } else {
        Ssl::Disable
    };

    Ok(ConnectionParameters {
        hosts,
        database: database.into_owned(),
        user: user.into_owned(),
        password,
        connect_timeout,
        server_settings,
        ssl,
    })
}

fn read_password_file(
    hosts: &[Host],
    database: &str,
    user: &str,
    reader: impl Iterator<Item = impl AsRef<str>>,
) -> Option<String> {
    for line in reader {
        let line = line.as_ref().trim();

        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let mut parts = vec![String::new()];
        let mut backslash = false;
        for c in line.chars() {
            if backslash {
                parts.last_mut().unwrap().push(c);
                backslash = false;
                continue;
            }
            if c == '\\' {
                backslash = true;
                continue;
            }
            if c == ':' && parts.len() <= 4 {
                parts.push(String::new());
                continue;
            }
            parts.last_mut().unwrap().push(c);
        }

        if parts.len() == 5 {
            for host in hosts {
                let port = match host {
                    Host::Hostname(hostname, port) => {
                        if parts[0] != "*" && parts[0] != hostname.as_str() {
                            continue;
                        }
                        *port
                    }
                    Host::IP(hostname, port, _) => {
                        if parts[0] != "*" && str::parse(&parts[0]) != Ok(*hostname) {
                            continue;
                        }
                        *port
                    }
                    Host::Path(_, port) | Host::Abstract(_, port) => {
                        if parts[0] != "*" && parts[0] != "localhost" {
                            continue;
                        }
                        *port
                    }
                };
                if parts[1] != "*" && str::parse(&parts[1]) != Ok(port) {
                    continue;
                }
                if parts[2] != "*" && parts[2] != database {
                    continue;
                }
                if parts[3] != "*" && parts[3] != user {
                    continue;
                }
                return Some(parts.pop().unwrap());
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_parse_hostlist() {
        assert_eq!(
            parse_hostlist(&["hostname"], &[1234]),
            Ok(vec![Host::Hostname("hostname".to_string(), 1234)])
        );
        assert_eq!(
            parse_hostlist(&["hostname:4321"], &[1234]),
            Ok(vec![Host::Hostname("hostname".to_string(), 4321)])
        );
        assert_eq!(
            parse_hostlist(&["/path"], &[1234]),
            Ok(vec![Host::Path("/path".to_string(), 1234)])
        );
        assert_eq!(
            parse_hostlist(&["[2001:db8::1234]", "[::1]"], &[1234]),
            Ok(vec![
                Host::IP(
                    IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 0x1234)),
                    1234,
                    None
                ),
                Host::IP(IpAddr::V6(Ipv6Addr::LOCALHOST), 1234, None),
            ])
        );
        assert_eq!(
            parse_hostlist(&["[2001:db8::1234%eth0]"], &[1234]),
            Ok(vec![Host::IP(
                IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 0x1234)),
                1234,
                Some("eth0".to_string())
            ),])
        );
    }

    #[test]
    fn test_parse_password_file() {
        let input = r#"
abc:*:*:user:password from pgpass for user@abc
localhost:*:*:*:password from pgpass for localhost
cde:5433:*:*:password from pgpass for cde:5433

*:*:*:testuser:password from pgpass for testuser
*:*:testdb:*:password from pgpass for testdb
# comment
*:*:test\:db:test\\:password from pgpass with escapes
        "#
        .trim();

        for (host, database, user, output) in [
            (
                Host::Hostname("abc".to_owned(), 1234),
                "database",
                "user",
                Some("password from pgpass for user@abc"),
            ),
            (
                Host::Hostname("localhost".to_owned(), 1234),
                "database",
                "user",
                Some("password from pgpass for localhost"),
            ),
            (
                Host::Path("/tmp".into(), 1234),
                "database",
                "user",
                Some("password from pgpass for localhost"),
            ),
            (
                Host::Hostname("hmm".to_owned(), 1234),
                "database",
                "testuser",
                Some("password from pgpass for testuser"),
            ),
            (
                Host::Hostname("hostname".to_owned(), 1234),
                "test:db",
                r#"test\"#,
                Some("password from pgpass with escapes"),
            ),
            (
                Host::Hostname("doesntexist".to_owned(), 1234),
                "db",
                "user",
                None,
            ),
        ] {
            assert_eq!(
                read_password_file(&[host], database, user, input.split('\n')),
                output.map(|s| s.to_owned())
            );
        }
    }

    #[test]
    fn test_parse_dsn() {
        assert_eq!(
            parse_postgres_url(
                "postgres://",
                [
                    ("PGUSER", "user"),
                    ("PGDATABASE", "testdb"),
                    ("PGPASSWORD", "passw"),
                    ("PGHOST", "host"),
                    ("PGPORT", "123"),
                    ("PGCONNECT_TIMEOUT", "8"),
                ]
                .as_slice()
            )
            .unwrap(),
            ConnectionParameters {
                hosts: vec![Host::Hostname("host".to_string(), 123,),],
                database: "testdb".to_string(),
                user: "user".to_string(),
                password: Password::Specified("passw".to_string(),),
                connect_timeout: Some(Duration::from_secs(8)),
                ssl: Ssl::Enable(SslMode::Prefer, Default::default()),
                ..Default::default()
            }
        );

        assert_eq!(
            parse_postgres_url("postgres://user:pass@host:1234/database", ()).unwrap(),
            ConnectionParameters {
                hosts: vec![Host::Hostname("host".to_string(), 1234,),],
                database: "database".to_string(),
                user: "user".to_string(),
                password: Password::Specified("pass".to_string(),),
                ssl: Ssl::Enable(SslMode::Prefer, Default::default()),
                ..Default::default()
            }
        );

        assert_eq!(
            parse_postgres_url("postgresql://user@host1:1111,host2:2222/db", ()).unwrap(),
            ConnectionParameters {
                hosts: vec![
                    Host::Hostname("host1".to_string(), 1111,),
                    Host::Hostname("host2".to_string(), 2222,),
                ],
                database: "db".to_string(),
                user: "user".to_string(),
                password: Password::Unspecified,
                ssl: Ssl::Enable(SslMode::Prefer, Default::default()),
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_dsn_with_slashes() {
        assert_eq!(
            parse_postgres_url(
                r#"postgres://test\\@fgh/test\:db?passfile=/tmp/tmpkrjuaje4"#,
                ()
            )
            .unwrap(),
            ConnectionParameters {
                hosts: vec![Host::Hostname("fgh".to_string(), 5432,),],
                database: r#"test\:db"#.to_string(),
                user: r#"test\\"#.to_string(),
                password: Password::Passfile("/tmp/tmpkrjuaje4".to_string().into(),),
                ssl: Ssl::Enable(SslMode::Prefer, Default::default()),
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_dns_with_params() {
        assert_eq!(parse_postgres_url("postgresql://me:ask@127.0.0.1:888/db?param=sss&param=123&host=testhost&user=testuser&port=2222&database=testdb&sslmode=verify_full&aa=bb", ()).unwrap(), ConnectionParameters {
                hosts: vec![
                        Host::IP(
                            IpAddr::V4(Ipv4Addr::LOCALHOST),
                            888,
                            None,
                        ),
                    ],
                    database: "db".to_string(),
                    user: "me".to_string(),
                    password: Password::Specified(
                        "ask".to_string(),
                    ),
                    server_settings: HashMap::from_iter([
                        ("aa".to_string(), "bb".to_string()),
                        ("param".to_string(), "123".to_string())
                    ]),
                    ssl: Ssl::Enable(SslMode::VerifyFull, Default::default()),
                    ..Default::default()
        })
    }

    #[test]
    fn test_dsn_with_escapes() {
        assert_eq!(
            parse_postgres_url("postgresql://us%40r:p%40ss@h%40st1,h%40st2:543%33/d%62", ())
                .unwrap(),
            ConnectionParameters {
                hosts: vec![
                    Host::Hostname("h@st1".to_string(), 5432,),
                    Host::Hostname("h@st2".to_string(), 5433,),
                ],
                database: "db".to_string(),
                user: "us@r".to_string(),
                password: Password::Specified("p@ss".to_string(),),
                ssl: Ssl::Enable(SslMode::Prefer, Default::default()),
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_dsn_no_slash() {
        assert_eq!(
            parse_postgres_url("postgres://user@?port=56226&host=%2Ftmp", ()).unwrap(),
            ConnectionParameters {
                hosts: vec![Host::Path("/tmp".to_string(), 56226,),],
                database: "user".to_string(),
                user: "user".to_string(),
                password: Password::Unspecified,
                ssl: Ssl::Disable,
                ..Default::default()
            }
        );
    }
}
