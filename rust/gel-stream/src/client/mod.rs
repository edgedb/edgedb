use std::{
    borrow::Cow,
    net::{IpAddr, SocketAddr},
    os::linux::net::SocketAddrExt,
    path::Path,
    sync::Arc,
};

#[cfg(feature = "openssl")]
pub mod openssl;
#[cfg(feature = "rustls")]
pub mod rustls;
#[cfg(feature = "tokio")]
pub mod tokio_stream;

pub mod stream;

mod connection;

pub use connection::Connector;

macro_rules! __invalid_state {
    ($error:literal) => {{
        eprintln!(
            "Invalid connection state: {}\n{}",
            $error,
            ::std::backtrace::Backtrace::capture()
        );
        #[allow(deprecated)]
        $crate::client::ConnectionError::__InvalidState
    }};
}
pub(crate) use __invalid_state as invalid_state;
use rustls_pki_types::{CertificateDer, CertificateRevocationListDer, PrivateKeyDer, ServerName};

#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    /// Invalid state error, suggesting a logic error in code rather than a server or client failure.
    /// Use the `invalid_state!` macro instead which will print a backtrace.
    #[error("Invalid state")]
    #[deprecated = "Use invalid_state!"]
    __InvalidState,

    /// I/O error encountered during connection operations.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// UTF-8 decoding error.
    #[error("UTF8 error: {0}")]
    Utf8Error(#[from] std::str::Utf8Error),

    /// SSL-related error.
    #[error("SSL error: {0}")]
    SslError(#[from] SslError),
}

#[derive(Debug, thiserror::Error)]
pub enum SslError {
    #[error("SSL is not supported by this client transport")]
    SslUnsupportedByClient,

    #[cfg(feature = "openssl")]
    #[error("OpenSSL error: {0}")]
    OpenSslError(#[from] ::openssl::ssl::Error),
    #[cfg(feature = "openssl")]
    #[error("OpenSSL error: {0}")]
    OpenSslErrorStack(#[from] ::openssl::error::ErrorStack),

    #[cfg(feature = "rustls")]
    #[error("Rustls error: {0}")]
    RustlsError(#[from] ::rustls::Error),

    #[cfg(feature = "rustls")]
    #[error("Webpki error: {0}")]
    WebpkiError(::webpki::Error),

    #[cfg(feature = "rustls")]
    #[error("Verifier builder error: {0}")]
    VerifierBuilderError(#[from] ::rustls::server::VerifierBuilderError),

    #[error("Invalid DNS name: {0}")]
    InvalidDnsNameError(#[from] ::rustls_pki_types::InvalidDnsNameError),

    #[error("SSL I/O error: {0}")]
    SslIoError(#[from] std::io::Error),
}

impl SslError {
    pub fn common_error(&self) -> Option<CommonError> {
        match self {
            #[cfg(feature = "rustls")]
            SslError::RustlsError(::rustls::Error::InvalidCertificate(
                ::rustls::CertificateError::NotValidForName,
            )) => Some(CommonError::InvalidCertificateForName),
            #[cfg(feature = "openssl")]
            SslError::OpenSslError(e) => {
                if let Some(ssl_error) = e.ssl_error() {
                    // TODO: Not quite right
                    if ssl_error
                        .errors()
                        .iter()
                        .any(|e| e.code() & 0x00fffff == 134)
                    {
                        Some(CommonError::InvalidCertificateForName)
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
pub enum CommonError {
    #[error("The certificate's subject name(s) do not match the name of the host")]
    InvalidCertificateForName,
}

// Note that we choose rustls when both openssl and rustls are enabled.

#[cfg(all(feature = "openssl", not(feature = "rustls")))]
pub type Ssl = ::openssl::ssl::Ssl;
#[cfg(feature = "rustls")]
pub type Ssl = ::rustls::ClientConnection;

#[cfg(feature = "tokio")]
pub type Stream = tokio_stream::TokioStream;

/// Verification modes for TLS that are a superset of both PostgreSQL and EdgeDB/Gel.
///
/// Postgres offers six levels: `disable`, `allow`, `prefer`, `require`, `verify-ca` and `verify-full`.
///
/// EdgeDB/Gel offers three levels: `insecure`, `no_host_verification' and 'strict'.
///
/// This table maps the various levels:
///
/// | Postgres | EdgeDB/Gel | `TlsServerCertVerify` enum |
/// | -------- | ----------- | ----------------- |
/// | require  | insecure    | `Insecure`        |
/// | verify-ca | no_host_verification | `IgnoreHostname`        |
/// | verify-full | strict | `VerifyFull`      |
///
/// Note that both EdgeDB/Gel and Postgres may alter certificate validation levels
/// when custom root certificates are provided. This must be done in the
/// `SslParameters` struct by the caller.
#[derive(Default, Copy, Clone, Debug, PartialEq, Eq)]
pub enum TlsServerCertVerify {
    /// Do not verify the server's certificate. Only confirm that the server is
    /// using TLS.
    Insecure,
    /// Verify the server's certificate using the CA (ignore hostname).
    IgnoreHostname,
    /// Verify the server's certificate using the CA and hostname.
    #[default]
    VerifyFull,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum TlsCert {
    /// Use the system's default certificate.
    #[default]
    System,
    /// Use a custom root certificate only.
    Custom(CertificateDer<'static>),
}

#[derive(Default, Debug, PartialEq, Eq)]
pub struct SslParameters {
    pub server_cert_verify: TlsServerCertVerify,
    pub cert: Option<CertificateDer<'static>>,
    pub key: Option<PrivateKeyDer<'static>>,
    pub root_cert: TlsCert,
    pub crl: Vec<CertificateRevocationListDer<'static>>,
    pub min_protocol_version: Option<SslVersion>,
    pub max_protocol_version: Option<SslVersion>,
    pub enable_keylog: bool,
    pub sni_override: Option<Cow<'static, str>>,
    pub alpn: Option<Cow<'static, [Cow<'static, str>]>>,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SslVersion {
    Tls1,
    Tls1_1,
    Tls1_2,
    Tls1_3,
}

pub struct Target {
    inner: TargetInner,
}

#[allow(private_bounds)]
impl Target {
    /// Create a new target for a Unix socket.
    #[cfg(unix)]
    pub fn new_unix_path<'s>(path: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        let path = ResolvedTarget::from(std::os::unix::net::SocketAddr::from_pathname(path)?);
        Ok(Self {
            inner: TargetInner::NoTls(path.into()),
        })
    }

    /// Create a new target for a Unix socket.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn new_unix_domain<'s>(domain: impl AsRef<[u8]>) -> Result<Self, std::io::Error> {
        let domain =
            ResolvedTarget::from(std::os::unix::net::SocketAddr::from_abstract_name(domain)?);
        Ok(Self {
            inner: TargetInner::NoTls(domain.into()),
        })
    }

    /// Create a new target for a TCP socket.
    pub fn new_tcp(host: impl TcpResolve) -> Self {
        Self {
            inner: TargetInner::NoTls(host.into()),
        }
    }

    /// Create a new target for a TCP socket with TLS.
    pub fn new_tcp_tls(host: impl TcpResolve, params: SslParameters) -> Self {
        Self {
            inner: TargetInner::Tls(host.into(), params.into()),
        }
    }

    /// Create a new target for a TCP socket with STARTTLS.
    pub fn new_tcp_starttls(host: impl TcpResolve, params: SslParameters) -> Self {
        Self {
            inner: TargetInner::StartTls(host.into(), params.into()),
        }
    }

    /// Get the name of the target. For resolved IP addresses, this is the string representation of the IP address.
    /// For unresolved hostnames, this is the hostname.
    fn name(&self) -> Option<ServerName> {
        self.maybe_resolved().name()
    }

    fn maybe_resolved(&self) -> &MaybeResolvedTarget {
        match &self.inner {
            TargetInner::NoTls(target) => &target,
            TargetInner::Tls(target, _) => &target,
            TargetInner::StartTls(target, _) => &target,
        }
    }

    fn maybe_ssl(&self) -> Option<&SslParameters> {
        match &self.inner {
            TargetInner::NoTls(_) => None,
            TargetInner::Tls(_, params) => Some(params),
            TargetInner::StartTls(_, params) => Some(params),
        }
    }
}

#[derive(Clone, Debug, derive_more::From)]
pub(crate) enum MaybeResolvedTarget {
    Resolved(ResolvedTarget),
    Unresolved(Cow<'static, str>, u16, Option<Cow<'static, str>>),
}

impl MaybeResolvedTarget {
    fn name(&self) -> Option<ServerName> {
        match self {
            MaybeResolvedTarget::Resolved(ResolvedTarget::SocketAddr(addr)) => {
                Some(ServerName::IpAddress(addr.ip().into()))
            }
            MaybeResolvedTarget::Unresolved(host, _, _) => {
                Some(ServerName::DnsName(host.to_string().try_into().ok()?))
            }
            _ => None,
        }
    }
}

/// The type of connection.
#[derive(Clone, Debug)]
enum TargetInner {
    NoTls(MaybeResolvedTarget),
    Tls(MaybeResolvedTarget, Arc<SslParameters>),
    StartTls(MaybeResolvedTarget, Arc<SslParameters>),
}

#[derive(Clone, Debug, derive_more::From)]
/// The resolved target of a connection attempt.
pub enum ResolvedTarget {
    SocketAddr(std::net::SocketAddr),
    #[cfg(unix)]
    UnixSocketAddr(std::os::unix::net::SocketAddr),
}

trait TcpResolve {
    fn into(self) -> MaybeResolvedTarget;
}

impl<S: AsRef<str>> TcpResolve for (S, u16) {
    fn into(self) -> MaybeResolvedTarget {
        if let Ok(addr) = self.0.as_ref().parse::<IpAddr>() {
            MaybeResolvedTarget::Resolved(ResolvedTarget::SocketAddr(SocketAddr::new(addr, self.1)))
        } else {
            MaybeResolvedTarget::Unresolved(Cow::Owned(self.0.as_ref().to_owned()), self.1, None)
        }
    }
}

impl TcpResolve for SocketAddr {
    fn into(self) -> MaybeResolvedTarget {
        MaybeResolvedTarget::Resolved(ResolvedTarget::SocketAddr(self))
    }
}

trait TlsInit {
    type Tls;
    fn init(params: &SslParameters, name: Option<ServerName>) -> Result<Self::Tls, SslError>;
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    #[test]
    fn test_target() {
        let target = Target::new_tcp(("localhost", 5432));
        assert_eq!(
            target.name(),
            Some(ServerName::DnsName("localhost".try_into().unwrap()))
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    #[ntest::timeout(30_000)]
    async fn test_target_unix() -> Result<(), std::io::Error> {
        use tokio::io::AsyncReadExt;

        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("gel-stream-test");

        // Create a unix socket and connect to it
        let socket = tokio::net::UnixListener::bind(&path)?;

        let accept_task = tokio::spawn(async move {
            let (mut stream, _) = socket.accept().await.unwrap();
            let mut buf = String::new();
            stream.read_to_string(&mut buf).await.unwrap();
            assert_eq!(buf, "Hello, world!");
        });

        let connect_task = tokio::spawn(async {
            let target = Target::new_unix_path(path)?;
            let mut stm = Connector::new(target).unwrap().connect().await.unwrap();
            stm.write_all(b"Hello, world!").await?;
            Ok::<_, std::io::Error>(())
        });

        accept_task.await.unwrap();
        connect_task.await.unwrap().unwrap();

        Ok(())
    }

    #[tokio::test]
    #[ntest::timeout(30_000)]
    async fn test_target_tcp() -> Result<(), std::io::Error> {
        // Create a TCP listener on a random port
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        let accept_task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = String::new();
            stream.read_to_string(&mut buf).await.unwrap();
            assert_eq!(buf, "Hello, world!");
        });

        let connect_task = tokio::spawn(async move {
            let target = Target::new_tcp(("127.0.0.1", addr.port()));
            let mut stm = Connector::new(target).unwrap().connect().await.unwrap();
            stm.write_all(b"Hello, world!").await?;
            Ok::<_, std::io::Error>(())
        });

        accept_task.await.unwrap();
        connect_task.await.unwrap().unwrap();

        Ok(())
    }

    fn load_test_cert() -> rustls_pki_types::CertificateDer<'static> {
        rustls_pemfile::certs(
            &mut include_str!("../../../../tests/certs/server.cert.pem").as_bytes(),
        )
        .next()
        .unwrap()
        .unwrap()
    }

    fn load_test_ca() -> rustls_pki_types::CertificateDer<'static> {
        rustls_pemfile::certs(&mut include_str!("../../../../tests/certs/ca.cert.pem").as_bytes())
            .next()
            .unwrap()
            .unwrap()
    }

    fn load_test_key() -> rustls_pki_types::PrivateKeyDer<'static> {
        rustls_pemfile::private_key(
            &mut include_str!("../../../../tests/certs/server.key.pem").as_bytes(),
        )
        .unwrap()
        .unwrap()
    }

    async fn spawn_tls_server(
        expected_hostname: Option<&str>,
        server_alpn: Option<&[&str]>,
        expected_alpn: Option<&str>,
    ) -> Result<
        (
            SocketAddr,
            tokio::task::JoinHandle<Result<(), std::io::Error>>,
        ),
        std::io::Error,
    > {
        use ::rustls::{ServerConfig, ServerConnection};

        let _ = ::rustls::crypto::ring::default_provider().install_default();

        // Create a TCP listener on a random port
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        // Load TLS cert and key
        let cert = load_test_cert();
        let key = load_test_key();

        // Configure rustls server
        let mut config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)
            .unwrap();
        config.alpn_protocols = server_alpn
            .map(|alpn| alpn.iter().map(|s| s.as_bytes().to_vec()).collect())
            .unwrap_or_default();

        let tls_config = Arc::new(config);
        let expected_alpn = expected_alpn.map(|alpn| alpn.as_bytes().to_vec());
        let expected_hostname = expected_hostname.map(|sni| sni.to_string());
        let accept_task = tokio::spawn(async move {
            let (tcp_stream, _) = listener.accept().await.unwrap();
            let tls_conn = ServerConnection::new(tls_config).unwrap();
            let mut stream =
                rustls_tokio_stream::TlsStream::new_server_side_from(tcp_stream, tls_conn, None);
            let handshake = stream.handshake().await?;
            eprintln!("handshake: {:?}", handshake);
            assert_eq!(handshake.alpn, expected_alpn);
            assert_eq!(handshake.sni, expected_hostname);
            let mut buf = String::new();
            stream.read_to_string(&mut buf).await.unwrap();
            assert_eq!(buf, "Hello, world!");
            stream.shutdown().await?;
            Ok::<_, std::io::Error>(())
        });
        Ok((addr, accept_task))
    }

    /// The certificate is not valid for 127.0.0.1, so the connection should fail.
    #[tokio::test]
    #[ntest::timeout(30_000)]
    async fn test_target_tcp_tls_verify_full_fails() -> Result<(), std::io::Error> {
        let (addr, accept_task) = spawn_tls_server(None, None, None).await?;

        let connect_task = tokio::spawn(async move {
            let target = Target::new_tcp_tls(
                ("127.0.0.1", addr.port()),
                SslParameters {
                    root_cert: TlsCert::Custom(load_test_ca()),
                    ..Default::default()
                },
            );
            let stm = Connector::new(target).unwrap().connect().await;
            assert!(
                matches!(&stm, Err(ConnectionError::SslError(ssl)) if ssl.common_error() == Some(CommonError::InvalidCertificateForName)),
                "{stm:?}"
            );
            Ok::<_, std::io::Error>(())
        });

        accept_task.await.unwrap().unwrap_err();
        connect_task.await.unwrap().unwrap();

        Ok(())
    }

    /// The certificate is valid for "localhost", so the connection should succeed.
    #[tokio::test]
    #[ntest::timeout(30_000)]
    async fn test_target_tcp_tls_verify_full_ok() -> Result<(), std::io::Error> {
        let (addr, accept_task) = spawn_tls_server(Some("localhost"), None, None).await?;

        let connect_task = tokio::spawn(async move {
            let target = Target::new_tcp_tls(
                ("localhost", addr.port()),
                SslParameters {
                    root_cert: TlsCert::Custom(load_test_ca()),
                    ..Default::default()
                },
            );
            let mut stm = Connector::new(target).unwrap().connect().await?;
            stm.write_all(b"Hello, world!").await?;
            stm.shutdown().await?;
            Ok::<_, ConnectionError>(())
        });

        accept_task.await.unwrap().unwrap();
        connect_task.await.unwrap().unwrap();

        Ok(())
    }

    #[tokio::test]
    #[ntest::timeout(30_000)]
    async fn test_target_tcp_tls_insecure() -> Result<(), std::io::Error> {
        let (addr, accept_task) = spawn_tls_server(None, None, None).await?;

        let connect_task = tokio::spawn(async move {
            let target = Target::new_tcp_tls(
                ("127.0.0.1", addr.port()),
                SslParameters {
                    server_cert_verify: TlsServerCertVerify::Insecure,
                    ..Default::default()
                },
            );
            let mut stm = Connector::new(target).unwrap().connect().await.unwrap();
            stm.write_all(b"Hello, world!").await?;
            stm.shutdown().await?;
            Ok::<_, std::io::Error>(())
        });

        accept_task.await.unwrap().unwrap();
        connect_task.await.unwrap().unwrap();

        Ok(())
    }

    /// Test that we can override the SNI.
    #[tokio::test]
    #[ntest::timeout(30_000)]
    async fn test_target_tcp_tls_sni_override() -> Result<(), std::io::Error> {
        let (addr, accept_task) = spawn_tls_server(Some("www.google.com"), None, None).await?;

        let connect_task = tokio::spawn(async move {
            let target = Target::new_tcp_tls(
                ("127.0.0.1", addr.port()),
                SslParameters {
                    server_cert_verify: TlsServerCertVerify::Insecure,
                    sni_override: Some(Cow::Borrowed("www.google.com")),
                    ..Default::default()
                },
            );
            let mut stm = Connector::new(target).unwrap().connect().await.unwrap();
            stm.write_all(b"Hello, world!").await.unwrap();
            stm.shutdown().await?;
            Ok::<_, std::io::Error>(())
        });

        accept_task.await.unwrap().unwrap();
        connect_task.await.unwrap().unwrap();

        Ok(())
    }

    /// Test that we can override the ALPN.
    #[tokio::test]
    #[ntest::timeout(30_000)]
    async fn test_target_tcp_tls_alpn_override() -> Result<(), std::io::Error> {
        let (addr, accept_task) =
            spawn_tls_server(None, Some(&["nope", "accepted"]), Some("accepted")).await?;

        let connect_task = tokio::spawn(async move {
            let target = Target::new_tcp_tls(
                ("127.0.0.1", addr.port()),
                SslParameters {
                    server_cert_verify: TlsServerCertVerify::Insecure,
                    alpn: Some(Cow::Borrowed(&[
                        Cow::Borrowed("accepted"),
                        Cow::Borrowed("fake"),
                    ])),
                    ..Default::default()
                },
            );
            let mut stm = Connector::new(target).unwrap().connect().await.unwrap();
            stm.write_all(b"Hello, world!").await.unwrap();
            stm.shutdown().await?;
            Ok::<_, std::io::Error>(())
        });

        accept_task.await.unwrap().unwrap();
        connect_task.await.unwrap().unwrap();

        Ok(())
    }

    #[cfg(feature = "__manual_tests")]
    #[tokio::test]
    async fn test_live_server_manual_google_com() {
        let target = Target::new_tcp_tls(("www.google.com", 443), SslParameters::default());
        let mut stm = Connector::new(target).unwrap().connect().await.unwrap();
        stm.write_all(b"GET / HTTP/1.0\r\n\r\n").await.unwrap();
        // HTTP/1. .....
        assert_eq!(stm.read_u8().await.unwrap(), b'H');
    }

    /// Normally connecting to Google's IP will send an invalid SNI and fail.
    /// This test ensures that we can override the SNI to the correct hostname.
    #[cfg(feature = "__manual_tests")]
    #[tokio::test]
    async fn test_live_server_google_com_override_sni() {
        use std::net::ToSocketAddrs;

        let addr = "www.google.com:443"
            .to_socket_addrs()
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let target = Target::new_tcp_tls(
            addr,
            SslParameters {
                sni_override: Some(Cow::Borrowed("www.google.com")),
                ..Default::default()
            },
        );
        let mut stm = Connector::new(target).unwrap().connect().await.unwrap();
        stm.write_all(b"GET / HTTP/1.0\r\n\r\n").await.unwrap();
        // HTTP/1. .....
        assert_eq!(stm.read_u8().await.unwrap(), b'H');
    }
}
