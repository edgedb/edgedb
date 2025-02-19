use futures::FutureExt;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::WebPkiServerVerifier;
use rustls::server::{Acceptor, ClientHello, WebPkiClientVerifier};
use rustls::{
    ClientConfig, ClientConnection, DigitallySignedStruct, RootCertStore, ServerConfig,
    SignatureScheme,
};
use rustls_pki_types::{
    CertificateDer, CertificateRevocationListDer, DnsName, ServerName, UnixTime,
};
use rustls_platform_verifier::Verifier;
use rustls_tokio_stream::TlsStream;

use super::tokio_stream::TokioStream;
use crate::{
    RewindStream, SslError, Stream, TlsClientCertVerify, TlsDriver, TlsHandshake,
    TlsServerParameterProvider, TlsServerParameters,
};
use crate::{TlsCert, TlsParameters, TlsServerCertVerify};
use std::borrow::Cow;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

#[derive(Default)]
pub struct RustlsDriver;

impl TlsDriver for RustlsDriver {
    type Stream = TlsStream;
    type ClientParams = ClientConnection;
    type ServerParams = Arc<ServerConfig>;

    fn init_client(
        params: &TlsParameters,
        name: Option<ServerName>,
    ) -> Result<Self::ClientParams, SslError> {
        let _ = ::rustls::crypto::ring::default_provider().install_default();

        let TlsParameters {
            server_cert_verify,
            root_cert,
            cert,
            key,
            crl,
            min_protocol_version: _,
            max_protocol_version: _,
            alpn,
            enable_keylog,
            sni_override,
        } = params;

        let verifier = make_verifier(server_cert_verify, root_cert, crl.clone())?;

        let config = ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(verifier);

        // Load client certificate and key if provided
        let mut config = if let (Some(cert), Some(key)) = (cert, key) {
            config
                .with_client_auth_cert(vec![cert.clone()], key.clone_key())
                .map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Failed to set client auth cert",
                    )
                })?
        } else {
            config.with_no_client_auth()
        };

        // Configure ALPN if provided
        config.alpn_protocols = alpn.as_vec_vec();

        // Configure keylog if provided
        if *enable_keylog {
            config.key_log = Arc::new(rustls::KeyLogFile::new());
        }

        let name = if let Some(sni_override) = sni_override {
            ServerName::try_from(sni_override.to_string())?
        } else if let Some(name) = name {
            name.to_owned()
        } else {
            config.enable_sni = false;
            ServerName::IpAddress(IpAddr::V4(Ipv4Addr::from_bits(0)).into())
        };

        Ok(ClientConnection::new(Arc::new(config), name)?)
    }

    fn init_server(params: &TlsServerParameters) -> Result<Self::ServerParams, SslError> {
        let builder = match &params.client_cert_verify {
            TlsClientCertVerify::Ignore => ServerConfig::builder().with_no_client_auth(),
            TlsClientCertVerify::Optional(certs) => {
                let mut roots = RootCertStore::empty();
                roots.add_parsable_certificates(
                    certs.iter().map(|c| CertificateDer::from_slice(c.as_ref())),
                );
                ServerConfig::builder().with_client_cert_verifier(
                    WebPkiClientVerifier::builder(roots.into())
                        .allow_unauthenticated()
                        .build()?,
                )
            }
            TlsClientCertVerify::Validate(certs) => {
                let mut roots = RootCertStore::empty();
                roots.add_parsable_certificates(
                    certs.iter().map(|c| CertificateDer::from_slice(c.as_ref())),
                );
                ServerConfig::builder()
                    .with_client_cert_verifier(WebPkiClientVerifier::builder(roots.into()).build()?)
            }
        };

        let mut config = builder.with_single_cert(
            vec![params.server_certificate.cert.clone()],
            params.server_certificate.key.clone_key(),
        )?;

        config.alpn_protocols = params.alpn.as_vec_vec();

        Ok(Arc::new(config))
    }

    async fn upgrade_client<S: Stream>(
        params: Self::ClientParams,
        stream: S,
    ) -> Result<(Self::Stream, TlsHandshake), SslError> {
        // Note that we only support Tokio TcpStream for rustls.
        let stream = stream
            .downcast::<TokioStream>()
            .map_err(|_| crate::SslError::SslUnsupportedByClient)?;
        let TokioStream::Tcp(stream) = stream else {
            return Err(crate::SslError::SslUnsupportedByClient);
        };

        let mut stream = TlsStream::new_client_side(stream, params, None);

        match stream.handshake().await {
            Ok(handshake) => Ok((
                stream,
                TlsHandshake {
                    alpn: handshake.alpn.map(|alpn| Cow::Owned(alpn.to_vec())),
                    sni: handshake.sni.map(|sni| Cow::Owned(sni.to_string())),
                    cert: None,
                },
            )),
            Err(e) => {
                let kind = e.kind();
                if let Some(e2) = e.into_inner() {
                    match e2.downcast::<::rustls::Error>() {
                        Ok(e) => Err(crate::SslError::RustlsError(*e)),
                        Err(e) => Err(std::io::Error::new(kind, e).into()),
                    }
                } else {
                    Err(std::io::Error::from(kind).into())
                }
            }
        }
    }

    async fn upgrade_server<S: Stream>(
        params: TlsServerParameterProvider,
        stream: S,
    ) -> Result<(Self::Stream, TlsHandshake), SslError> {
        let stream = stream
            .downcast::<RewindStream<TokioStream>>()
            .map_err(|_| crate::SslError::SslUnsupportedByClient)?;
        let (stream, buffer) = stream.into_inner();
        let TokioStream::Tcp(stream) = stream else {
            return Err(crate::SslError::SslUnsupportedByClient);
        };

        let mut acceptor = Acceptor::default();
        acceptor.read_tls(&mut buffer.as_slice())?;
        let server_config_provider = Arc::new(move |client_hello: ClientHello| {
            let params = params.clone();
            let server_name = client_hello
                .server_name()
                .map(|name| ServerName::DnsName(DnsName::try_from(name.to_string()).unwrap()));
            async move {
                let params = params.lookup(server_name);
                let config = RustlsDriver::init_server(&params)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
                Ok::<_, std::io::Error>(config)
            }
            .boxed()
        });
        let mut stream = TlsStream::new_server_side_from_acceptor(
            acceptor,
            stream,
            server_config_provider,
            None,
        );

        match stream.handshake().await {
            Ok(handshake) => Ok((
                stream,
                TlsHandshake {
                    alpn: handshake.alpn.map(|alpn| Cow::Owned(alpn.to_vec())),
                    sni: handshake.sni.map(|sni| Cow::Owned(sni.to_string())),
                    cert: None,
                },
            )),
            Err(e) => {
                let kind = e.kind();
                if let Some(e2) = e.into_inner() {
                    match e2.downcast::<::rustls::Error>() {
                        Ok(e) => Err(crate::SslError::RustlsError(*e)),
                        Err(e) => Err(std::io::Error::new(kind, e).into()),
                    }
                } else {
                    Err(std::io::Error::from(kind).into())
                }
            }
        }
    }
}

fn make_verifier(
    server_cert_verify: &TlsServerCertVerify,
    root_cert: &TlsCert,
    crls: Vec<CertificateRevocationListDer<'static>>,
) -> Result<Arc<dyn ServerCertVerifier>, crate::SslError> {
    if *server_cert_verify == TlsServerCertVerify::Insecure {
        return Ok(Arc::new(NullVerifier));
    }

    if let TlsCert::Custom(root) = root_cert {
        let mut roots = RootCertStore::empty();
        let (loaded, ignored) = roots.add_parsable_certificates([root.clone()]);
        if loaded == 0 || ignored > 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid certificate",
            )
            .into());
        }

        let verifier = WebPkiServerVerifier::builder(Arc::new(roots))
            .with_crls(crls)
            .build()?;
        if *server_cert_verify == TlsServerCertVerify::IgnoreHostname {
            return Ok(Arc::new(IgnoreHostnameVerifier::new(verifier)));
        }
        return Ok(verifier);
    }

    if *server_cert_verify == TlsServerCertVerify::IgnoreHostname {
        return Ok(Arc::new(IgnoreHostnameVerifier::new(Arc::new(
            Verifier::new(),
        ))));
    }

    Ok(Arc::new(Verifier::new()))
}

#[derive(Debug)]
struct IgnoreHostnameVerifier {
    verifier: Arc<dyn ServerCertVerifier>,
}

impl IgnoreHostnameVerifier {
    fn new(verifier: Arc<dyn ServerCertVerifier>) -> Self {
        Self { verifier }
    }
}

impl ServerCertVerifier for IgnoreHostnameVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        self.verifier.verify_server_cert(
            end_entity,
            intermediates,
            &ServerName::DnsName(DnsName::try_from("").unwrap()),
            ocsp_response,
            now,
        )
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.verifier.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.verifier.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.verifier.supported_verify_schemes()
    }
}

#[derive(Debug)]
struct NullVerifier;

impl ServerCertVerifier for NullVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        use SignatureScheme::*;
        vec![
            RSA_PKCS1_SHA1,
            ECDSA_SHA1_Legacy,
            RSA_PKCS1_SHA256,
            ECDSA_NISTP256_SHA256,
            RSA_PKCS1_SHA384,
            ECDSA_NISTP384_SHA384,
            RSA_PKCS1_SHA512,
            ECDSA_NISTP521_SHA512,
            RSA_PSS_SHA256,
            RSA_PSS_SHA384,
            RSA_PSS_SHA512,
            ED25519,
            ED448,
        ]
    }
}
