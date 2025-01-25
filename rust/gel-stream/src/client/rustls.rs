use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::WebPkiServerVerifier;
use rustls::{
    ClientConfig, ClientConnection, DigitallySignedStruct, RootCertStore, SignatureScheme,
};
use rustls_pki_types::{
    CertificateDer, CertificateRevocationListDer, DnsName, ServerName, UnixTime,
};
use rustls_platform_verifier::Verifier;

use super::stream::{Stream, StreamWithUpgrade};
use super::tokio_stream::TokioStream;
use super::{TlsCert, TlsInit, TlsParameters, TlsServerCertVerify};
use std::any::Any;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

impl<S: Stream + 'static> StreamWithUpgrade for (S, Option<ClientConnection>) {
    type Base = S;
    type Config = ClientConnection;
    type Upgrade = rustls_tokio_stream::TlsStream;

    async fn secure_upgrade(self) -> Result<Self::Upgrade, super::SslError>
    where
        Self: Sized,
    {
        let Some(tls) = self.1 else {
            return Err(super::SslError::SslUnsupportedByClient);
        };

        // Note that we only support Tokio TcpStream for rustls.
        let stream = &mut Some(self.0) as &mut dyn Any;
        let Some(stream) = stream.downcast_mut::<Option<TokioStream>>() else {
            return Err(super::SslError::SslUnsupportedByClient);
        };

        let stream = stream.take().unwrap();
        let TokioStream::Tcp(stream) = stream else {
            return Err(super::SslError::SslUnsupportedByClient);
        };

        let mut stream = rustls_tokio_stream::TlsStream::new_client_side(stream, tls, None);
        let res = stream.handshake().await;

        // Potentially unwrap the error to get the underlying error.
        if let Err(e) = res {
            let kind = e.kind();
            if let Some(e2) = e.into_inner() {
                match e2.downcast::<::rustls::Error>() {
                    Ok(e) => return Err(super::SslError::RustlsError(*e)),
                    Err(e) => return Err(std::io::Error::new(kind, e).into()),
                }
            } else {
                return Err(std::io::Error::from(kind).into());
            }
        }

        Ok(stream)
    }
}

fn make_verifier(
    server_cert_verify: &TlsServerCertVerify,
    root_cert: &TlsCert,
    crls: Vec<CertificateRevocationListDer<'static>>,
) -> Result<Arc<dyn ServerCertVerifier>, super::SslError> {
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

impl TlsInit for ClientConnection {
    type Tls = ClientConnection;

    fn init(
        parameters: &TlsParameters,
        name: Option<ServerName>,
    ) -> Result<Self::Tls, super::SslError> {
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
        } = parameters;

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
        if let Some(alpn_protocols) = alpn {
            config.alpn_protocols = alpn_protocols
                .iter()
                .map(|p| p.as_bytes().to_vec())
                .collect();
        }

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
