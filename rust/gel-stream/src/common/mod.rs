pub mod stream;
pub mod target;
pub mod tls;

#[cfg(feature = "openssl")]
pub mod openssl;
#[cfg(feature = "rustls")]
pub mod rustls;
#[cfg(feature = "tokio")]
pub mod tokio_stream;

#[cfg(feature = "tokio")]
pub type BaseStream = tokio_stream::TokioStream;

#[cfg(not(feature = "tokio"))]
pub type BaseStream = ();
