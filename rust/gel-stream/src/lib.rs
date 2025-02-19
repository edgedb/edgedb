// We don't want to warn about unused code when 1) either client or server is not
// enabled, or 2) no crypto backend is enabled.
#![cfg_attr(
    not(all(
        all(feature = "client", feature = "server"),
        any(feature = "rustls", feature = "openssl")
    )),
    allow(unused)
)]

#[cfg(feature = "client")]
mod client;
#[cfg(feature = "server")]
mod server;

#[cfg(feature = "client")]
pub use client::Connector;

#[cfg(feature = "server")]
pub use server::Acceptor;

mod common;
#[cfg(feature = "openssl")]
pub use common::openssl::OpensslDriver;
#[cfg(feature = "rustls")]
pub use common::rustls::RustlsDriver;
pub use common::{stream::*, target::*, tls::*, BaseStream};

#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
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
    #[error("SSL is already upgraded or is in the process of upgrading")]
    SslAlreadyUpgraded,

    #[cfg(feature = "openssl")]
    #[error("OpenSSL error: {0}")]
    OpenSslError(#[from] ::openssl::ssl::Error),
    #[cfg(feature = "openssl")]
    #[error("OpenSSL error: {0}")]
    OpenSslErrorStack(#[from] ::openssl::error::ErrorStack),
    #[cfg(feature = "openssl")]
    #[error("OpenSSL certificate verification error: {0}")]
    OpenSslErrorVerify(#[from] ::openssl::x509::X509VerifyResult),

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
    /// Returns a common error for any time of crypto backend.
    pub fn common_error(&self) -> Option<CommonError> {
        match self {
            #[cfg(feature = "rustls")]
            SslError::RustlsError(::rustls::Error::InvalidCertificate(cert_err)) => {
                match cert_err {
                    ::rustls::CertificateError::NotValidForName => {
                        Some(CommonError::InvalidCertificateForName)
                    }
                    ::rustls::CertificateError::Revoked => Some(CommonError::CertificateRevoked),
                    ::rustls::CertificateError::Expired => Some(CommonError::CertificateExpired),
                    ::rustls::CertificateError::UnknownIssuer => Some(CommonError::InvalidIssuer),
                    _ => None,
                }
            }
            #[cfg(feature = "openssl")]
            SslError::OpenSslErrorVerify(e) => match e.as_raw() {
                openssl_sys::X509_V_ERR_HOSTNAME_MISMATCH => {
                    Some(CommonError::InvalidCertificateForName)
                }
                openssl_sys::X509_V_ERR_IP_ADDRESS_MISMATCH => {
                    Some(CommonError::InvalidCertificateForName)
                }
                openssl_sys::X509_V_ERR_CERT_REVOKED => Some(CommonError::CertificateRevoked),
                openssl_sys::X509_V_ERR_CERT_HAS_EXPIRED => Some(CommonError::CertificateExpired),
                openssl_sys::X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT
                | openssl_sys::X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT_LOCALLY => {
                    Some(CommonError::InvalidIssuer)
                }
                _ => None,
            },
            _ => None,
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
pub enum CommonError {
    #[error("The certificate's subject name(s) do not match the name of the host")]
    InvalidCertificateForName,
    #[error("The certificate has been revoked")]
    CertificateRevoked,
    #[error("The certificate has expired")]
    CertificateExpired,
    #[error("The certificate was issued by an untrusted authority")]
    InvalidIssuer,
}
