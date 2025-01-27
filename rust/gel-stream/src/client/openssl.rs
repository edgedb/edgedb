use std::pin::Pin;

use openssl::{
    ssl::{SslContextBuilder, SslMethod, SslVerifyMode},
    x509::{verify::X509VerifyFlags, X509VerifyResult},
};
use rustls_pki_types::ServerName;

use super::{
    stream::{Stream, StreamWithUpgrade},
    SslError, SslVersion, TlsCert, TlsInit, TlsParameters, TlsServerCertVerify,
};

impl<S: Stream> StreamWithUpgrade for (S, Option<openssl::ssl::Ssl>) {
    type Base = S;
    type Config = openssl::ssl::Ssl;
    type Upgrade = tokio_openssl::SslStream<S>;

    async fn secure_upgrade(self) -> Result<Self::Upgrade, SslError>
    where
        Self: Sized,
    {
        let Some(tls) = self.1 else {
            return Err(super::SslError::SslUnsupportedByClient);
        };

        let mut stream = tokio_openssl::SslStream::new(tls, self.0)?;
        let res = Pin::new(&mut stream).do_handshake().await;
        if res.is_err() {
            if stream.ssl().verify_result() != X509VerifyResult::OK {
                return Err(SslError::OpenSslErrorVerify(stream.ssl().verify_result()));
            }
        }
        res.map_err(SslError::OpenSslError)?;
        Ok(stream)
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

impl TlsInit for openssl::ssl::Ssl {
    type Tls = openssl::ssl::Ssl;

    fn init(parameters: &TlsParameters, name: Option<ServerName>) -> Result<Self, SslError> {
        let TlsParameters {
            server_cert_verify,
            root_cert,
            cert,
            key,
            crl,
            min_protocol_version,
            max_protocol_version,
            alpn,
            sni_override,
            enable_keylog,
        } = parameters;

        let mut ssl = SslContextBuilder::new(SslMethod::tls_client())?;

        // Load root cert
        if let TlsCert::Custom(root) = root_cert {
            let root = openssl::x509::X509::from_der(root.as_ref())?;
            ssl.cert_store_mut().add_cert(root)?;
            ssl.set_verify(SslVerifyMode::PEER);
        } else if *server_cert_verify == TlsServerCertVerify::Insecure {
            ssl.set_verify(SslVerifyMode::NONE);
        }

        // Load CRL
        if !crl.is_empty() {
            // The openssl crate doesn't yet have add_crl, so we need to use the raw FFI
            use foreign_types::ForeignTypeRef;
            let ptr = ssl.cert_store_mut().as_ptr();

            extern "C" {
                pub fn X509_STORE_add_crl(
                    store: *mut openssl_sys::X509_STORE,
                    x: *mut openssl_sys::X509_CRL,
                ) -> openssl_sys::c_int;
            }

            for crl in crl {
                let crl = openssl::x509::X509Crl::from_der(crl.as_ref())?;
                let crl_ptr = crl.as_ptr();
                let res = unsafe { X509_STORE_add_crl(ptr, crl_ptr) };
                if res != 1 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Failed to add CRL to store",
                    )
                    .into());
                }
            }

            ssl.verify_param_mut()
                .set_flags(X509VerifyFlags::CRL_CHECK | X509VerifyFlags::CRL_CHECK_ALL)?;
            ssl.cert_store_mut()
                .set_flags(X509VerifyFlags::CRL_CHECK | X509VerifyFlags::CRL_CHECK_ALL)?;
        }

        // Load certificate chain and private key
        if let (Some(cert), Some(key)) = (cert.as_ref(), key.as_ref()) {
            let builder = openssl::x509::X509::from_der(cert.as_ref())?;
            ssl.set_certificate(&builder)?;
            let builder = openssl::pkey::PKey::private_key_from_pem(&key.secret_der())?;
            ssl.set_private_key(&builder)?;
        }

        // Configure hostname verification
        if *server_cert_verify == TlsServerCertVerify::VerifyFull {
            ssl.set_verify(SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT);
        }

        ssl.set_min_proto_version(min_protocol_version.map(|s| s.into()))?;
        ssl.set_max_proto_version(max_protocol_version.map(|s| s.into()))?;

        // Configure key log filename
        if *enable_keylog {
            if let Ok(path) = std::env::var("SSLKEYLOGFILE") {
                // "The callback is invoked whenever TLS key material is generated, and is passed a line of NSS SSLKEYLOGFILE-formatted text.
                // This can be used by tools like Wireshark to decrypt message traffic. The line does not contain a trailing newline.
                ssl.set_keylog_callback(move |_ssl, msg| {
                    let Ok(mut file) = std::fs::OpenOptions::new().append(true).open(&path) else {
                        return;
                    };
                    let _ = std::io::Write::write_all(&mut file, msg.as_bytes());
                });
            }
        }

        if *server_cert_verify == TlsServerCertVerify::VerifyFull {
            if let Some(hostname) = sni_override {
                ssl.verify_param_mut().set_host(hostname)?;
            } else if let Some(ServerName::DnsName(hostname)) = &name {
                ssl.verify_param_mut().set_host(hostname.as_ref())?;
            } else if let Some(ServerName::IpAddress(ip)) = &name {
                ssl.verify_param_mut().set_ip((*ip).into())?;
            }
        }

        let mut ssl = openssl::ssl::Ssl::new(&ssl.build())?;
        ssl.set_connect_state();

        // Set hostname if it's not an IP address
        if let Some(hostname) = sni_override {
            ssl.set_hostname(hostname)?;
        } else if let Some(ServerName::DnsName(hostname)) = &name {
            ssl.set_hostname(hostname.as_ref())?;
        }

        if let Some(alpn) = alpn {
            let alpn = alpn
                .iter()
                .map(|s| {
                    let bytes = s.as_bytes();
                    let mut vec = Vec::with_capacity(bytes.len() + 1);
                    vec.push(bytes.len() as u8);
                    vec.extend_from_slice(bytes);
                    vec
                })
                .flatten()
                .collect::<Vec<_>>();
            ssl.set_alpn_protos(&alpn)?;
        }

        Ok(ssl)
    }
}
