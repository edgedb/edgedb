#[cfg(feature = "tokio")]
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use std::future::Future;
#[cfg(feature = "tokio")]
use std::{
    any::Any,
    io::IoSlice,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{Ssl, SslError, TlsDriver, TlsHandshake, TlsServerParameterProvider};

#[cfg(feature = "tokio")]
pub trait Stream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static {
    fn downcast<S: Stream + 'static>(self) -> Result<S, Self>
    where
        Self: Sized + 'static,
    {
        // Note that we only support Tokio TcpStream for rustls.
        let mut holder = Some(self);
        let stream = &mut holder as &mut dyn Any;
        let Some(stream) = stream.downcast_mut::<Option<S>>() else {
            return Err(holder.take().unwrap());
        };
        let stream = stream.take().unwrap();
        Ok(stream)
    }
}

#[cfg(feature = "tokio")]
impl<T> Stream for T where T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static {}

#[cfg(not(feature = "tokio"))]
pub trait Stream: 'static {}
#[cfg(not(feature = "tokio"))]
impl<S: Stream, D: TlsDriver> Stream for UpgradableStream<S, D> {}
#[cfg(not(feature = "tokio"))]
impl Stream for () {}

pub trait StreamUpgrade: Stream {
    fn secure_upgrade(&mut self) -> impl Future<Output = Result<(), SslError>> + Send;
    fn handshake(&self) -> Option<&TlsHandshake>;
}

#[allow(private_bounds)]
#[derive(derive_more::Debug)]
pub struct UpgradableStream<S: Stream, D: TlsDriver = Ssl> {
    inner: UpgradableStreamInner<S, D>,
}

#[allow(private_bounds)]
impl<S: Stream, D: TlsDriver> UpgradableStream<S, D> {
    #[inline(always)]
    pub(crate) fn new_client(base: S, config: Option<D::ClientParams>) -> Self {
        UpgradableStream {
            inner: UpgradableStreamInner::BaseClient(base, config),
        }
    }

    #[inline(always)]
    pub(crate) fn new_server(base: S, config: Option<TlsServerParameterProvider>) -> Self {
        UpgradableStream {
            inner: UpgradableStreamInner::BaseServer(base, config),
        }
    }

    /// Consume the `UpgradableStream` and return the underlying stream as a [`Box<dyn Stream>`].
    pub fn into_boxed(self) -> Result<Box<dyn Stream>, Self> {
        match self.inner {
            UpgradableStreamInner::BaseClient(base, _) => Ok(Box::new(base)),
            UpgradableStreamInner::BaseServer(base, _) => Ok(Box::new(base)),
            UpgradableStreamInner::Upgraded(upgraded, _) => Ok(Box::new(upgraded)),
            UpgradableStreamInner::Upgrading => Err(self),
        }
    }
}

impl<S: Stream, D: TlsDriver> StreamUpgrade for UpgradableStream<S, D> {
    async fn secure_upgrade(&mut self) -> Result<(), SslError> {
        match std::mem::replace(&mut self.inner, UpgradableStreamInner::Upgrading) {
            UpgradableStreamInner::BaseClient(base, config) => {
                let Some(config) = config else {
                    return Err(SslError::SslUnsupportedByClient);
                };
                let (upgraded, handshake) = D::upgrade_client(config, base).await?;
                self.inner = UpgradableStreamInner::Upgraded(upgraded, handshake);
                Ok(())
            }
            UpgradableStreamInner::BaseServer(base, config) => {
                let Some(config) = config else {
                    return Err(SslError::SslUnsupportedByClient);
                };
                let (upgraded, handshake) = D::upgrade_server(config, base).await?;
                self.inner = UpgradableStreamInner::Upgraded(upgraded, handshake);
                Ok(())
            }
            UpgradableStreamInner::Upgraded(..) => Err(SslError::SslAlreadyUpgraded),
            UpgradableStreamInner::Upgrading => Err(SslError::SslAlreadyUpgraded),
        }
    }

    fn handshake(&self) -> Option<&TlsHandshake> {
        match &self.inner {
            UpgradableStreamInner::Upgraded(_, handshake) => Some(handshake),
            _ => None,
        }
    }
}

#[cfg(feature = "tokio")]
impl<S: Stream, D: TlsDriver> tokio::io::AsyncRead for UpgradableStream<S, D> {
    #[inline(always)]
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let inner = &mut self.get_mut().inner;
        match inner {
            UpgradableStreamInner::BaseClient(base, _) => Pin::new(base).poll_read(cx, buf),
            UpgradableStreamInner::BaseServer(base, _) => Pin::new(base).poll_read(cx, buf),
            UpgradableStreamInner::Upgraded(upgraded, _) => Pin::new(upgraded).poll_read(cx, buf),
            UpgradableStreamInner::Upgrading => std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cannot read while upgrading",
            ))),
        }
    }
}

#[cfg(feature = "tokio")]
impl<S: Stream, D: TlsDriver> tokio::io::AsyncWrite for UpgradableStream<S, D> {
    #[inline(always)]
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let inner = &mut self.get_mut().inner;
        match inner {
            UpgradableStreamInner::BaseClient(base, _) => Pin::new(base).poll_write(cx, buf),
            UpgradableStreamInner::BaseServer(base, _) => Pin::new(base).poll_write(cx, buf),
            UpgradableStreamInner::Upgraded(upgraded, _) => Pin::new(upgraded).poll_write(cx, buf),
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
            UpgradableStreamInner::BaseClient(base, _) => Pin::new(base).poll_flush(cx),
            UpgradableStreamInner::BaseServer(base, _) => Pin::new(base).poll_flush(cx),
            UpgradableStreamInner::Upgraded(upgraded, _) => Pin::new(upgraded).poll_flush(cx),
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
            UpgradableStreamInner::BaseClient(base, _) => Pin::new(base).poll_shutdown(cx),
            UpgradableStreamInner::BaseServer(base, _) => Pin::new(base).poll_shutdown(cx),
            UpgradableStreamInner::Upgraded(upgraded, _) => Pin::new(upgraded).poll_shutdown(cx),
            UpgradableStreamInner::Upgrading => std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cannot shutdown while upgrading",
            ))),
        }
    }

    #[inline(always)]
    fn is_write_vectored(&self) -> bool {
        match &self.inner {
            UpgradableStreamInner::BaseClient(base, _) => base.is_write_vectored(),
            UpgradableStreamInner::BaseServer(base, _) => base.is_write_vectored(),
            UpgradableStreamInner::Upgraded(upgraded, _) => upgraded.is_write_vectored(),
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
            UpgradableStreamInner::BaseClient(base, _) => {
                Pin::new(base).poll_write_vectored(cx, bufs)
            }
            UpgradableStreamInner::BaseServer(base, _) => {
                Pin::new(base).poll_write_vectored(cx, bufs)
            }
            UpgradableStreamInner::Upgraded(upgraded, _) => {
                Pin::new(upgraded).poll_write_vectored(cx, bufs)
            }
            UpgradableStreamInner::Upgrading => std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cannot write vectored while upgrading",
            ))),
        }
    }
}

#[derive(derive_more::Debug)]
enum UpgradableStreamInner<S: Stream, D: TlsDriver> {
    #[debug("BaseClient(..)")]
    BaseClient(S, Option<D::ClientParams>),
    #[debug("BaseServer(..)")]
    BaseServer(S, Option<TlsServerParameterProvider>),
    #[debug("Upgraded(..)")]
    Upgraded(D::Stream, TlsHandshake),
    #[debug("Upgrading")]
    Upgrading,
}

pub trait Rewindable {
    fn rewind(&mut self, bytes: &[u8]) -> std::io::Result<()>;
}

pub struct RewindStream<S> {
    buffer: Vec<u8>,
    inner: S,
}

impl<S> RewindStream<S> {
    pub fn new(inner: S) -> Self {
        RewindStream {
            buffer: Vec::new(),
            inner,
        }
    }

    pub fn rewind(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    pub fn into_inner(self) -> (S, Vec<u8>) {
        (self.inner, self.buffer)
    }
}

#[cfg(feature = "tokio")]
impl<S: AsyncRead + Unpin> AsyncRead for RewindStream<S> {
    #[inline(always)]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if !self.buffer.is_empty() {
            let to_read = std::cmp::min(buf.remaining(), self.buffer.len());
            let data = self.buffer.drain(..to_read).collect::<Vec<_>>();
            buf.put_slice(&data);
            Poll::Ready(Ok(()))
        } else {
            Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }
}

#[cfg(feature = "tokio")]
impl<S: AsyncWrite + Unpin> AsyncWrite for RewindStream<S> {
    #[inline(always)]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    #[inline(always)]
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    #[inline(always)]
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    #[inline(always)]
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    #[inline(always)]
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<Result<usize, std::io::Error>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }
}

impl<S: Stream> Rewindable for RewindStream<S> {
    fn rewind(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.rewind(bytes);
        Ok(())
    }
}

impl<S: Stream + Rewindable, D: TlsDriver> Rewindable for UpgradableStream<S, D>
where
    D::Stream: Rewindable,
{
    fn rewind(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        match &mut self.inner {
            UpgradableStreamInner::BaseClient(stm, _) => stm.rewind(bytes),
            UpgradableStreamInner::BaseServer(stm, _) => stm.rewind(bytes),
            UpgradableStreamInner::Upgraded(stm, _) => stm.rewind(bytes),
            UpgradableStreamInner::Upgrading => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Cannot rewind a stream that is upgrading",
            )),
        }
    }
}
