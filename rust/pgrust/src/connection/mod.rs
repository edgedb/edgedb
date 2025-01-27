use std::collections::HashMap;

use crate::errors::{edgedb::EdbError, PgServerError};
use db_proto::ParseError;
mod conn;
pub mod dsn;
mod flow;
pub(crate) mod queue;
mod raw_conn;

pub use conn::{Client, PGConnError};
pub use flow::{
    CopyDataSink, DataSink, DoneHandling, ExecuteSink, FlowAccumulator, Format, MaxRows, Oid,
    Param, Pipeline, PipelineBuilder, Portal, QuerySink, Statement,
};
use gel_stream::client::ConnectionError;
pub use raw_conn::RawClient;

macro_rules! __invalid_state {
    ($error:literal) => {{
        eprintln!(
            "Invalid connection state: {}\n{}",
            $error,
            ::std::backtrace::Backtrace::capture()
        );
        #[allow(deprecated)]
        $crate::connection::PGConnectionError::__InvalidState
    }};
}
pub(crate) use __invalid_state as invalid_state;

#[derive(Debug, thiserror::Error)]
pub enum PGConnectionError {
    /// Invalid state error, suggesting a logic error in code rather than a server or client failure.
    /// Use the `invalid_state!` macro instead which will print a backtrace.
    #[error("Invalid state")]
    #[deprecated = "Use invalid_state!"]
    __InvalidState,

    /// Error during connection setup.
    #[error("Connection error: {0}")]
    ConnectionError(#[from] ConnectionError),

    /// Error returned by the server.
    #[error("Server error: {0}")]
    ServerError(#[from] PgServerError),

    /// Error returned by the server.
    #[error("Server error: {0}")]
    EdbServerError(#[from] EdbError),

    /// The server sent something we didn't expect
    #[error("Unexpected server response: {0}")]
    UnexpectedResponse(String),

    /// Error related to SCRAM authentication.
    #[error("SCRAM: {0}")]
    Scram(#[from] gel_auth::scram::SCRAMError),

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
