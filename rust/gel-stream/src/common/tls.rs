use crate::{SslError, Stream};
use rustls_pki_types::{CertificateDer, CertificateRevocationListDer, PrivateKeyDer, ServerName};
use std::{borrow::Cow, future::Future, sync::Arc};

use super::BaseStream;

// Note that we choose rustls when both openssl and rustls are enabled.

#[cfg(all(feature = "openssl", not(feature = "rustls")))]
pub type Ssl = crate::common::openssl::OpensslDriver;
#[cfg(feature = "rustls")]
pub type Ssl = crate::common::rustls::RustlsDriver;
#[cfg(not(any(feature = "openssl", feature = "rustls")))]
pub type Ssl = NullTlsDriver;

pub trait TlsDriver: Default + Send + Sync + Unpin + 'static {
    type Stream: Stream + Send;
    type ClientParams: Unpin + Send;
    type ServerParams: Unpin + Send;

    #[allow(unused)]
    fn init_client(
        params: &TlsParameters,
        name: Option<ServerName>,
    ) -> Result<Self::ClientParams, SslError>;
    #[allow(unused)]
    fn init_server(params: &TlsServerParameters) -> Result<Self::ServerParams, SslError>;

    fn upgrade_client<S: Stream>(
        params: Self::ClientParams,
        stream: S,
    ) -> impl Future<Output = Result<(Self::Stream, TlsHandshake), SslError>> + Send;
    fn upgrade_server<S: Stream>(
        params: TlsServerParameterProvider,
        stream: S,
    ) -> impl Future<Output = Result<(Self::Stream, TlsHandshake), SslError>> + Send;
}

#[derive(Default)]
pub struct NullTlsDriver;

#[allow(unused)]
impl TlsDriver for NullTlsDriver {
    type Stream = BaseStream;
    type ClientParams = ();
    type ServerParams = ();

    fn init_client(
        params: &TlsParameters,
        name: Option<ServerName>,
    ) -> Result<Self::ClientParams, SslError> {
        Err(SslError::SslUnsupportedByClient)
    }

    fn init_server(params: &TlsServerParameters) -> Result<Self::ServerParams, SslError> {
        Err(SslError::SslUnsupportedByClient)
    }

    async fn upgrade_client<S: Stream>(
        params: Self::ClientParams,
        stream: S,
    ) -> Result<(Self::Stream, TlsHandshake), SslError> {
        Err(SslError::SslUnsupportedByClient)
    }

    async fn upgrade_server<S: Stream>(
        params: TlsServerParameterProvider,
        stream: S,
    ) -> Result<(Self::Stream, TlsHandshake), SslError> {
        Err(SslError::SslUnsupportedByClient)
    }
}

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
/// `TlsParameters` struct by the caller.
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
pub struct TlsParameters {
    pub server_cert_verify: TlsServerCertVerify,
    pub cert: Option<CertificateDer<'static>>,
    pub key: Option<PrivateKeyDer<'static>>,
    pub root_cert: TlsCert,
    pub crl: Vec<CertificateRevocationListDer<'static>>,
    pub min_protocol_version: Option<SslVersion>,
    pub max_protocol_version: Option<SslVersion>,
    pub enable_keylog: bool,
    pub sni_override: Option<Cow<'static, str>>,
    pub alpn: TlsAlpn,
}

impl TlsParameters {
    pub fn insecure() -> Self {
        Self {
            server_cert_verify: TlsServerCertVerify::Insecure,
            ..Default::default()
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SslVersion {
    Tls1,
    Tls1_1,
    Tls1_2,
    Tls1_3,
}

#[derive(Default, Debug, PartialEq, Eq)]
pub enum TlsClientCertVerify {
    /// Do not verify the client's certificate, just ignore it.
    #[default]
    Ignore,
    /// If a client certificate is provided, validate it.
    Optional(Vec<CertificateDer<'static>>),
    /// Validate that a client certificate exists and is valid. This configuration
    /// may not be ideal, because it does not fail the client-side handshake.
    Validate(Vec<CertificateDer<'static>>),
}

#[derive(derive_more::Debug, derive_more::Constructor)]
pub struct TlsKey {
    #[debug("key(...)")]
    pub(crate) key: PrivateKeyDer<'static>,
    #[debug("cert(...)")]
    pub(crate) cert: CertificateDer<'static>,
}

#[derive(Debug, Clone)]
pub struct TlsServerParameterProvider {
    inner: TlsServerParameterProviderInner,
}

impl TlsServerParameterProvider {
    pub fn new(params: TlsServerParameters) -> Self {
        Self {
            inner: TlsServerParameterProviderInner::Static(Arc::new(params)),
        }
    }

    pub fn with_lookup(
        lookup: impl Fn(Option<ServerName>) -> Arc<TlsServerParameters> + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner: TlsServerParameterProviderInner::Lookup(Arc::new(lookup)),
        }
    }

    pub fn lookup(&self, name: Option<ServerName>) -> Arc<TlsServerParameters> {
        match &self.inner {
            TlsServerParameterProviderInner::Static(params) => params.clone(),
            TlsServerParameterProviderInner::Lookup(lookup) => lookup(name),
        }
    }
}

#[derive(derive_more::Debug, Clone)]
enum TlsServerParameterProviderInner {
    Static(Arc<TlsServerParameters>),
    #[debug("Lookup(...)")]
    #[allow(clippy::type_complexity)]
    Lookup(Arc<dyn Fn(Option<ServerName>) -> Arc<TlsServerParameters> + Send + Sync + 'static>),
}

#[derive(Debug)]
pub struct TlsServerParameters {
    pub client_cert_verify: TlsClientCertVerify,
    pub min_protocol_version: Option<SslVersion>,
    pub max_protocol_version: Option<SslVersion>,
    pub server_certificate: TlsKey,
    pub alpn: TlsAlpn,
}

#[derive(Debug, Default, Eq, PartialEq)]
pub struct TlsAlpn {
    /// The split form (ie: ["AB", "ABCD"])
    alpn_parts: Cow<'static, [Cow<'static, [u8]>]>,
}

impl TlsAlpn {
    pub fn new(alpn: &'static [&'static [u8]]) -> Self {
        let alpn = alpn.iter().map(|s| Cow::Borrowed(*s)).collect::<Vec<_>>();
        Self {
            alpn_parts: Cow::Owned(alpn),
        }
    }

    pub fn new_str(alpn: &'static [&'static str]) -> Self {
        let alpn = alpn
            .iter()
            .map(|s| Cow::Borrowed(s.as_bytes()))
            .collect::<Vec<_>>();
        Self {
            alpn_parts: Cow::Owned(alpn),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.alpn_parts.is_empty()
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.alpn_parts.len() * 2);
        for part in self.alpn_parts.iter() {
            bytes.push(part.len() as u8);
            bytes.extend_from_slice(part.as_ref());
        }
        bytes
    }

    pub fn as_vec_vec(&self) -> Vec<Vec<u8>> {
        let mut vec = Vec::with_capacity(self.alpn_parts.len());
        for part in self.alpn_parts.iter() {
            vec.push(part.to_vec());
        }
        vec
    }
}

#[derive(Debug, Clone, Default)]
pub struct TlsHandshake {
    pub alpn: Option<Cow<'static, [u8]>>,
    pub sni: Option<Cow<'static, str>>,
    pub cert: Option<CertificateDer<'static>>,
}
