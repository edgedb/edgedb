use std::pin::Pin;

use openssl::{
    ssl::{SslContextBuilder, SslVerifyMode},
    x509::verify::X509VerifyFlags,
};

use super::{
    dsn::{SslMode, SslParameters},
    stream::{Stream, StreamWithUpgrade},
    SslError,
};

impl<S: Stream> StreamWithUpgrade for (S, openssl::ssl::Ssl) {
    type Base = S;
    type Config = openssl::ssl::Ssl;
    type Upgrade = tokio_openssl::SslStream<S>;

    async fn secure_upgrade(self) -> Result<Self::Upgrade, super::ConnectionError>
    where
        Self: Sized,
    {
        let mut stream =
            tokio_openssl::SslStream::new(self.1, self.0).map_err(SslError::OpenSslErrorStack)?;
        Pin::new(&mut stream)
            .do_handshake()
            .await
            .map_err(SslError::OpenSslError)?;
        Ok(stream)
    }
}

/// Given a set of [`SslParameters`], configures an OpenSSL context.
pub fn create_ssl_client_context(
    mut ssl: SslContextBuilder,
    ssl_mode: SslMode,
    parameters: SslParameters,
) -> Result<SslContextBuilder, Box<dyn std::error::Error>> {
    let SslParameters {
        cert,
        key,
        password,
        rootcert,
        crl,
        min_protocol_version,
        max_protocol_version,
        keylog_filename,
    } = parameters;

    if ssl_mode >= SslMode::Require {
        // Load root cert
        if let Some(root) = rootcert {
            ssl.set_ca_file(root)?;
            ssl.set_verify(SslVerifyMode::PEER);
        } else if ssl_mode == SslMode::Require {
            ssl.set_verify(SslVerifyMode::NONE);
        }

        // Load CRL
        if let Some(crl) = &crl {
            ssl.set_ca_file(crl)?;
            ssl.verify_param_mut()
                .set_flags(X509VerifyFlags::CRL_CHECK | X509VerifyFlags::CRL_CHECK_ALL)?;
        }
    }

    // Load certificate chain and private key
    if let (Some(cert), Some(key)) = (cert.as_ref(), key.as_ref()) {
        let builder = openssl::x509::X509::from_pem(&std::fs::read(cert)?)?;
        ssl.set_certificate(&builder)?;
        let key = std::fs::read(key)?;
        let key = if let Some(password) = password {
            openssl::pkey::PKey::private_key_from_pem_passphrase(&key, password.as_bytes())?
        } else {
            openssl::pkey::PKey::private_key_from_pem(&key)?
        };
        ssl.set_private_key(&key)?;
    }

    // Configure hostname verification
    if ssl_mode == SslMode::VerifyFull {
        ssl.set_verify(SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT);
    }

    ssl.set_min_proto_version(min_protocol_version.map(|s| s.into()))?;
    ssl.set_max_proto_version(max_protocol_version.map(|s| s.into()))?;

    // Configure key log filename
    if let Some(keylog_filename) = &keylog_filename {
        let path = keylog_filename.clone();
        // "The callback is invoked whenever TLS key material is generated, and is passed a line of NSS SSLKEYLOGFILE-formatted text.
        // This can be used by tools like Wireshark to decrypt message traffic. The line does not contain a trailing newline.
        ssl.set_keylog_callback(move |_ssl, msg| {
            let Ok(mut file) = std::fs::OpenOptions::new().append(true).open(&path) else {
                return;
            };
            let _ = std::io::Write::write_all(&mut file, msg.as_bytes());
        });
    }

    Ok(ssl)
}

#[cfg(test)]
mod tests {
    use openssl::ssl::SslMethod;
    use std::path::Path;

    use super::*;

    #[test]
    fn create_ssl() {
        let cert_path = Path::new("../../tests/certs").canonicalize().unwrap();

        let ssl = SslContextBuilder::new(SslMethod::tls()).unwrap();
        let ssl = create_ssl_client_context(
            ssl,
            SslMode::VerifyFull,
            SslParameters {
                cert: Some(cert_path.join("client.cert.pem")),
                key: Some(cert_path.join("client.key.pem")),
                ..Default::default()
            },
        )
        .unwrap();

        let _context = ssl.build();
    }
}
