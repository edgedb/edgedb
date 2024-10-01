use super::{invalid_state, ConnectionError, SslError};
use std::pin::Pin;

pub trait Stream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin {}
impl<T> Stream for T where T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin {}

/// A trait for streams that can be upgraded to a secure connection.
///
/// This trait is usually implemented by tuples that represent a connection that can be
/// upgraded from an insecure to a secure state, typically through SSL/TLS.
pub trait StreamWithUpgrade: Unpin {
    type Base: Stream;
    type Upgrade: Stream;
    type Config: Unpin;

    /// Perform a secure upgrade operation and return the new, wrapped connection.
    #[allow(async_fn_in_trait)]
    async fn secure_upgrade(self) -> Result<Self::Upgrade, ConnectionError>
    where
        Self: Sized;
}

impl<S: Stream> StreamWithUpgrade for (S, ()) {
    type Base = S;
    type Upgrade = S;
    type Config = ();

    async fn secure_upgrade(self) -> Result<Self::Upgrade, ConnectionError>
    where
        Self: Sized,
    {
        Err(ConnectionError::SslError(SslError::SslUnsupportedByClient))
    }
}

pub struct UpgradableStream<B: Stream, C: Unpin>
where
    (B, C): StreamWithUpgrade,
{
    inner: UpgradableStreamInner<B, C>,
}

impl<B: Stream, C: Unpin> From<(B, C)> for UpgradableStream<B, C>
where
    (B, C): StreamWithUpgrade,
{
    #[inline(always)]
    fn from(value: (B, C)) -> Self {
        Self::new(value.0, value.1)
    }
}

impl<B: Stream, C: Unpin> UpgradableStream<B, C>
where
    (B, C): StreamWithUpgrade,
{
    #[inline(always)]
    pub fn new(base: B, config: C) -> Self {
        UpgradableStream {
            inner: UpgradableStreamInner::Base(base, config),
        }
    }

    pub async fn secure_upgrade(&mut self) -> Result<(), ConnectionError>
    where
        (B, C): StreamWithUpgrade,
    {
        match std::mem::replace(&mut self.inner, UpgradableStreamInner::Upgrading) {
            UpgradableStreamInner::Base(base, config) => {
                self.inner =
                    UpgradableStreamInner::Upgraded((base, config).secure_upgrade().await?);
                Ok(())
            }
            UpgradableStreamInner::Upgraded(..) => Err(invalid_state!(
                "Attempted to upgrade an already upgraded stream"
            )),
            UpgradableStreamInner::Upgrading => Err(invalid_state!(
                "Attempted to upgrade a stream that is already in the process of upgrading"
            )),
        }
    }
}

impl<B: Stream, C: Unpin> tokio::io::AsyncRead for UpgradableStream<B, C>
where
    (B, C): StreamWithUpgrade,
{
    #[inline(always)]
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let inner = &mut self.get_mut().inner;
        match inner {
            UpgradableStreamInner::Base(base, _) => Pin::new(base).poll_read(cx, buf),
            UpgradableStreamInner::Upgraded(upgraded) => Pin::new(upgraded).poll_read(cx, buf),
            UpgradableStreamInner::Upgrading => std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cannot read while upgrading",
            ))),
        }
    }
}

impl<B: Stream, C: Unpin> tokio::io::AsyncWrite for UpgradableStream<B, C>
where
    (B, C): StreamWithUpgrade,
{
    #[inline(always)]
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let inner = &mut self.get_mut().inner;
        match inner {
            UpgradableStreamInner::Base(base, _) => Pin::new(base).poll_write(cx, buf),
            UpgradableStreamInner::Upgraded(upgraded) => Pin::new(upgraded).poll_write(cx, buf),
            UpgradableStreamInner::Upgrading => std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cannot write while upgrading",
            ))),
        }
    }

    #[inline(always)]
    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let inner = &mut self.get_mut().inner;
        match inner {
            UpgradableStreamInner::Base(base, _) => Pin::new(base).poll_flush(cx),
            UpgradableStreamInner::Upgraded(upgraded) => Pin::new(upgraded).poll_flush(cx),
            UpgradableStreamInner::Upgrading => std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cannot flush while upgrading",
            ))),
        }
    }

    #[inline(always)]
    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let inner = &mut self.get_mut().inner;
        match inner {
            UpgradableStreamInner::Base(base, _) => Pin::new(base).poll_shutdown(cx),
            UpgradableStreamInner::Upgraded(upgraded) => Pin::new(upgraded).poll_shutdown(cx),
            UpgradableStreamInner::Upgrading => std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cannot shutdown while upgrading",
            ))),
        }
    }

    #[inline(always)]
    fn is_write_vectored(&self) -> bool {
        match &self.inner {
            UpgradableStreamInner::Base(base, _) => base.is_write_vectored(),
            UpgradableStreamInner::Upgraded(upgraded) => upgraded.is_write_vectored(),
            UpgradableStreamInner::Upgrading => false,
        }
    }

    #[inline(always)]
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let inner = &mut self.get_mut().inner;
        match inner {
            UpgradableStreamInner::Base(base, _) => Pin::new(base).poll_write_vectored(cx, bufs),
            UpgradableStreamInner::Upgraded(upgraded) => {
                Pin::new(upgraded).poll_write_vectored(cx, bufs)
            }
            UpgradableStreamInner::Upgrading => std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cannot write vectored while upgrading",
            ))),
        }
    }
}

enum UpgradableStreamInner<B: Stream, C: Unpin>
where
    (B, C): StreamWithUpgrade,
{
    Base(B, C),
    Upgraded(<(B, C) as StreamWithUpgrade>::Upgrade),
    Upgrading,
}
