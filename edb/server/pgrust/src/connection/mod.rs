use std::collections::HashMap;

use crate::{auth, errors::PgServerError, protocol::ParseError};

mod conn;
pub mod dsn;
pub mod openssl;
mod raw_conn;
mod stream;
pub mod tokio;

pub use conn::Client;
use dsn::HostType;
pub use raw_conn::connect_raw_ssl;

macro_rules! __invalid_state {
    ($error:literal) => {{
        eprintln!(
            "Invalid connection state: {}\n{}",
            $error,
            ::std::backtrace::Backtrace::capture()
        );
        #[allow(deprecated)]
        $crate::connection::ConnectionError::__InvalidState
    }};
}
pub(crate) use __invalid_state as invalid_state;

#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    /// Invalid state error, suggesting a logic error in code rather than a server or client failure.
    /// Use the `invalid_state!` macro instead which will print a backtrace.
    #[error("Invalid state")]
    #[deprecated = "Use invalid_state!"]
    __InvalidState,

    /// Error returned by the server.
    #[error("Server error: {0}")]
    ServerError(#[from] PgServerError),

    /// The server sent something we didn't expect
    #[error("Unexpected server response: {0}")]
    UnexpectedResponse(String),

    /// Error related to SCRAM authentication.
    #[error("SCRAM: {0}")]
    Scram(#[from] auth::SCRAMError),

    /// I/O error encountered during connection operations.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// UTF-8 decoding error.
    #[error("UTF8 error: {0}")]
    Utf8Error(#[from] std::str::Utf8Error),

    /// SSL-related error.
    #[error("SSL error: {0}")]
    SslError(#[from] SslError),

    #[error("Protocol error: {0}")]
    ParseError(#[from] ParseError),
}

#[derive(Debug, thiserror::Error)]
pub enum SslError {
    #[error("SSL is not supported by this client transport")]
    SslUnsupportedByClient,
    #[error("SSL was required by the client, but not offered by server (rejected SSL)")]
    SslRequiredByClient,
    #[error("OpenSSL error: {0}")]
    OpenSslError(#[from] ::openssl::ssl::Error),
    #[error("OpenSSL error: {0}")]
    OpenSslErrorStack(#[from] ::openssl::error::ErrorStack),
}

/// A sufficient set of required parameters to connect to a given transport.
#[derive(Clone, Default, derive_more::Debug)]
pub struct Credentials {
    pub username: String,
    #[debug(skip)]
    pub password: String,
    pub database: String,
    pub server_settings: HashMap<String, String>,
}

#[derive(Clone, Debug)]
/// The resolved target of a connection attempt.
pub enum ResolvedTarget {
    SocketAddr(std::net::SocketAddr),
    #[cfg(unix)]
    UnixSocketAddr(std::os::unix::net::SocketAddr),
}

impl ResolvedTarget {
    /// Resolves the target addresses for a given host.
    pub fn to_addrs_sync(host: &dsn::Host) -> Result<Vec<ResolvedTarget>, std::io::Error> {
        use std::net::{SocketAddr, ToSocketAddrs};

        let mut resolved_targets = Vec::new();
        let dsn::Host(host_type, port) = host;
        match host_type {
            HostType::Hostname(hostname) => {
                let socket_addrs = (hostname.as_str(), *port).to_socket_addrs()?;
                for addr in socket_addrs {
                    resolved_targets.push(Self::SocketAddr(addr));
                }
            }
            HostType::IP(ip, None) => {
                resolved_targets.push(ResolvedTarget::SocketAddr(SocketAddr::new(*ip, *port)))
            }
            HostType::IP(std::net::IpAddr::V4(_), Some(_)) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Scope IDs only supported for IPv6",
                ));
            }
            HostType::IP(std::net::IpAddr::V6(ip), Some(scope_id)) => {
                if let Ok(scope_id) = str::parse::<u32>(scope_id) {
                    resolved_targets.push(ResolvedTarget::SocketAddr(
                        std::net::SocketAddrV6::new(*ip, *port, 0, scope_id).into(),
                    ));
                } else {
                    // TODO: Resolve non-numeric scope IDs
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Only numeric scope IDs are supported",
                    ));
                };
            }
            HostType::Path(path) => {
                use std::os::unix::net::SocketAddr;
                resolved_targets.push(ResolvedTarget::UnixSocketAddr(SocketAddr::from_pathname(
                    std::path::PathBuf::from(path).join(format!(".s.PGSQL.{port}")),
                )?))
            }
            #[cfg(target_os = "linux")]
            HostType::Abstract(abstract_path) => {
                use std::os::linux::net::SocketAddrExt;
                use std::os::unix::net::SocketAddr;
                resolved_targets.push(ResolvedTarget::UnixSocketAddr(
                    SocketAddr::from_abstract_name(format!("{abstract_path}/.s.PGSQL.{port}"))?,
                ))
            }
            #[cfg(not(target_os = "linux"))]
            HostType::Abstract(abstract_path) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Abstract unix namespace paths unsupported on this platform",
                ));
            }
        }
        Ok(resolved_targets)
    }
}
