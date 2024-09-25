use futures::{stream, Stream, StreamExt};
use std::{
    hash::Hash,
    net::{SocketAddr, ToSocketAddrs},
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tracing::trace;

use crate::{
    stream::{StreamProperties, TransportType},
    stream_type::StreamType,
};

#[derive(Clone, Debug, derive_more::From)]
pub enum ListenerAddress {
    Tcp(SocketAddr),
    #[cfg(unix)]
    Unix(Arc<std::os::unix::net::SocketAddr>),
}

impl ListenerAddress {
    pub fn tcp_addr(&self) -> Option<SocketAddr> {
        match self {
            ListenerAddress::Tcp(addr) => Some(*addr),
            #[cfg(unix)]
            ListenerAddress::Unix(_) => None,
        }
    }
}

impl Hash for ListenerAddress {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            ListenerAddress::Tcp(x) => x.hash(state),
            ListenerAddress::Unix(x) => format!("{x:?}").hash(state),
        }
    }
}

impl Eq for ListenerAddress {}
impl PartialEq for ListenerAddress {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ListenerAddress::Tcp(x), ListenerAddress::Tcp(y)) => x.eq(y),
            (ListenerAddress::Unix(x), ListenerAddress::Unix(y)) => {
                format!("{x:?}").eq(&format!("{y:?}"))
            }
            _ => false,
        }
    }
}

/// Implemented by the embedder to configure the live state of the listener.
pub trait ListenerConfig: std::fmt::Debug + Send + Sync + 'static {
    /// Returns a stream of [`ListenerAddress`]es, allowing the server to
    /// reconfigure the listening port at any time.
    fn listen_address(&self) -> impl Stream<Item = std::io::Result<Vec<ListenerAddress>>> + Send;

    /// Process the SSL SNI, returning the SSL configuration and an optional tenant ID.
    fn ssl_config_sni(&self, hostname: Option<&str>) -> Result<(SslConfig, Option<String>), ()>;

    fn jwt_key(&self)
        -> Result<jwt::algorithm::openssl::PKeyWithDigest<openssl::pkey::Public>, ()>;

    /// Returns true if the given [`StreamType`] is supported at this
    /// time.
    fn is_supported(
        &self,
        stream_type: Option<StreamType>,
        transport_type: TransportType,
        stream_props: &StreamProperties,
    ) -> bool;
}

pub struct SslConfig {
    inner: Arc<Mutex<SslConfigInner>>,
}

impl SslConfig {
    pub fn new(cert: PathBuf, key: PathBuf) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SslConfigInner::Unconfigured { cert, key })),
        }
    }
    pub(crate) fn maybe_configure(
        &self,
        f: impl FnOnce(&mut openssl::ssl::SslContextBuilder),
    ) -> openssl::ssl::SslContext {
        let mut inner = self.inner.lock().unwrap();
        match &mut *inner {
            SslConfigInner::Unconfigured { cert, key } => {
                use openssl::pkey::PKey;
                use openssl::ssl::SslContext;
                use openssl::x509::X509;
                use std::fs::File;
                use std::io::Read;

                let mut ctx_builder = SslContext::builder(openssl::ssl::SslMethod::tls()).unwrap();

                // Load the certificate
                let mut cert_file = File::open(cert).unwrap();
                let mut cert_contents = Vec::new();
                cert_file.read_to_end(&mut cert_contents).unwrap();
                let cert = X509::from_pem(&cert_contents).unwrap();
                ctx_builder.set_certificate(&cert).unwrap();

                // Load the private key
                let mut key_file = File::open(key).unwrap();
                let mut key_contents = Vec::new();
                key_file.read_to_end(&mut key_contents).unwrap();
                let key = PKey::private_key_from_pem(&key_contents).unwrap();
                ctx_builder.set_private_key(&key).unwrap();

                // Apply any additional configuration
                f(&mut ctx_builder);

                // Build the context and update the inner state
                let context = ctx_builder.build();
                *inner = SslConfigInner::Configured {
                    context: context.clone(),
                };
                context
            }
            SslConfigInner::Configured { context } => context.clone(),
        }
    }
}

enum SslConfigInner {
    Unconfigured { cert: PathBuf, key: PathBuf },
    Configured { context: openssl::ssl::SslContext },
}

#[derive(Debug)]
pub struct TestListenerConfig {
    addrs: Vec<SocketAddr>,
}

impl TestListenerConfig {
    pub fn new(s: impl ToSocketAddrs) -> Self {
        let addrs = s.to_socket_addrs().unwrap().collect();
        Self { addrs }
    }
}

const MOCK_KEY: &str = r#"-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgwT5cQa55iRfc/q7I
uHXWqSw0enO7zQUhbSxj1G8cVfGhRANCAAT/ROscvp1DCIhbA8mbcpQupILxEUVq
f4r3nlQZCrteNogGAnV+IC2sCGjZuK9xknSMXT7EFkmNkCmTqPeaJKjv
-----END PRIVATE KEY-----"#;

fn mock_key_private() -> openssl::pkey::PKey<openssl::pkey::Private> {
    openssl::pkey::PKey::private_key_from_pem(MOCK_KEY.as_bytes()).unwrap()
}

fn mock_key_public() -> openssl::pkey::PKey<openssl::pkey::Public> {
    openssl::pkey::PKey::public_key_from_pem(MOCK_KEY.as_bytes()).unwrap()
}

impl ListenerConfig for TestListenerConfig {
    fn is_supported(
        &self,
        stream_type: Option<StreamType>,
        transport_type: TransportType,
        stream_props: &StreamProperties,
    ) -> bool {
        eprintln!("is_supported? stream_type={stream_type:?} transport_type={transport_type:?} stream_props={stream_props:?}");
        true
    }

    fn jwt_key(&self) -> Result<jwt::PKeyWithDigest<openssl::pkey::Public>, ()> {
        let key = mock_key_public();
        Ok(jwt::PKeyWithDigest {
            digest: openssl::hash::MessageDigest::sha256(),
            key,
        })
    }

    fn ssl_config_sni(&self, hostname: Option<&str>) -> Result<(SslConfig, Option<String>), ()> {
        Ok((
            SslConfig::new(
                "../../../tests/certs/server.cert.pem".into(),
                "../../../tests/certs/server.key.pem".into(),
            ),
            None,
        ))
    }

    fn listen_address(&self) -> impl Stream<Item = std::io::Result<Vec<ListenerAddress>>> {
        let addrs = self
            .addrs
            .iter()
            .map(|addr| ListenerAddress::Tcp(addr.clone()))
            .collect();
        stream::select_all(vec![
            stream::once(async { Ok(addrs) }).boxed(),
            stream::pending().boxed(),
        ])
    }
}
