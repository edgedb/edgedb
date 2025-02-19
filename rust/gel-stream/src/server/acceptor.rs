use crate::{
    common::tokio_stream::TokioListenerStream, ConnectionError, LocalAddress, ResolvedTarget,
    RewindStream, Ssl, SslError, StreamUpgrade, TlsDriver, TlsServerParameterProvider,
    UpgradableStream,
};
use futures::{FutureExt, StreamExt};
use std::{
    future::Future,
    pin::Pin,
    task::{ready, Poll},
};
use std::{net::SocketAddr, path::Path};

use super::Connection;

pub struct Acceptor {
    resolved_target: ResolvedTarget,
    tls_provider: Option<TlsServerParameterProvider>,
    should_upgrade: bool,
}

impl Acceptor {
    pub fn new_tcp(addr: SocketAddr) -> Self {
        Self {
            resolved_target: ResolvedTarget::SocketAddr(addr),
            tls_provider: None,
            should_upgrade: false,
        }
    }

    pub fn new_tcp_tls(addr: SocketAddr, provider: TlsServerParameterProvider) -> Self {
        Self {
            resolved_target: ResolvedTarget::SocketAddr(addr),
            tls_provider: Some(provider),
            should_upgrade: true,
        }
    }

    pub fn new_tcp_starttls(addr: SocketAddr, provider: TlsServerParameterProvider) -> Self {
        Self {
            resolved_target: ResolvedTarget::SocketAddr(addr),
            tls_provider: Some(provider),
            should_upgrade: false,
        }
    }

    #[cfg(unix)]
    pub fn new_unix_path(path: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        Ok(Self {
            resolved_target: ResolvedTarget::from(std::os::unix::net::SocketAddr::from_pathname(
                path,
            )?),
            tls_provider: None,
            should_upgrade: false,
        })
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn new_unix_domain(domain: impl AsRef<[u8]>) -> Result<Self, std::io::Error> {
        use std::os::linux::net::SocketAddrExt;
        Ok(Self {
            resolved_target: ResolvedTarget::from(
                std::os::unix::net::SocketAddr::from_abstract_name(domain)?,
            ),
            tls_provider: None,
            should_upgrade: false,
        })
    }

    pub async fn bind(
        self,
    ) -> Result<
        impl ::futures::Stream<Item = Result<Connection, ConnectionError>> + LocalAddress,
        ConnectionError,
    > {
        let stream = self.resolved_target.listen_raw().await?;
        Ok(AcceptedStream {
            stream,
            should_upgrade: self.should_upgrade,
            upgrade_future: None,
            tls_provider: self.tls_provider,
            _phantom: None,
        })
    }

    #[allow(private_bounds)]
    pub async fn bind_explicit<D: TlsDriver>(
        self,
    ) -> Result<
        impl ::futures::Stream<Item = Result<Connection<D>, ConnectionError>> + LocalAddress,
        ConnectionError,
    > {
        let stream = self.resolved_target.listen_raw().await?;
        Ok(AcceptedStream {
            stream,
            should_upgrade: self.should_upgrade,
            upgrade_future: None,
            tls_provider: self.tls_provider,
            _phantom: None,
        })
    }

    pub async fn accept_one(self) -> Result<Connection, std::io::Error> {
        let mut stream = self.resolved_target.listen().await?;
        let (stream, _target) = stream.next().await.unwrap()?;
        Ok(UpgradableStream::new_server(
            RewindStream::new(stream),
            None::<TlsServerParameterProvider>,
        ))
    }
}

struct AcceptedStream<D: TlsDriver = Ssl> {
    stream: TokioListenerStream,
    should_upgrade: bool,
    tls_provider: Option<TlsServerParameterProvider>,
    #[allow(clippy::type_complexity)]
    upgrade_future:
        Option<Pin<Box<dyn Future<Output = Result<Connection<D>, SslError>> + Send + 'static>>>,
    // Avoid using PhantomData because it fails to implement certain auto-traits
    _phantom: Option<&'static D>,
}

impl<D: TlsDriver> LocalAddress for AcceptedStream<D> {
    fn local_address(&self) -> std::io::Result<ResolvedTarget> {
        self.stream.local_address()
    }
}

impl<D: TlsDriver> futures::Stream for AcceptedStream<D> {
    type Item = Result<Connection<D>, ConnectionError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Some(mut upgrade_future) = self.upgrade_future.take() {
            match upgrade_future.poll_unpin(cx) {
                Poll::Ready(Ok(conn)) => {
                    return Poll::Ready(Some(Ok(conn)));
                }
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Some(Err(e.into())));
                }
                Poll::Pending => {
                    self.upgrade_future = Some(upgrade_future);
                    return Poll::Pending;
                }
            }
        }
        let r = ready!(self.stream.poll_next_unpin(cx));
        let Some(r) = r else {
            return Poll::Ready(None);
        };
        let (stream, _target) = r?;
        let mut stream =
            UpgradableStream::new_server(RewindStream::new(stream), self.tls_provider.clone());
        if self.should_upgrade {
            let mut upgrade_future = Box::pin(async move {
                stream.secure_upgrade().await?;
                Ok::<_, SslError>(stream)
            });
            match upgrade_future.poll_unpin(cx) {
                Poll::Ready(Ok(stream)) => {
                    return Poll::Ready(Some(Ok(stream)));
                }
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Some(Err(e.into())));
                }
                Poll::Pending => {
                    self.upgrade_future = Some(upgrade_future);
                    return Poll::Pending;
                }
            }
        }
        Poll::Ready(Some(Ok(stream)))
    }
}
