use crate::{
    config::ListenerAddress,
    hyper::{HyperStream, HyperUpgradedStream},
    stream_type::{known_protocol, StreamType},
};
use core::str;
use hyper::{HeaderMap, Version};
use openssl::{ssl::NameType, x509::X509};
use std::{
    io::{ErrorKind, IoSlice},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{unix::UCred, TcpStream, UnixStream},
};

macro_rules! stream_properties {
    (
        $(#[doc=$doc:literal] pub $name:ident: $type:ty),+ $(,)?
    ) => {
        pub struct StreamProperties {
            /// A parent transport, if one exists
            pub parent: Option<Arc<StreamProperties>>,
            /// The underlying transport type
            pub transport: TransportType,
            $(
                #[doc=$doc]
                pub $name: $type,
            )+
        }

        impl StreamProperties {
            pub fn new(transport: TransportType) -> Self {
                StreamProperties {
                    parent: None,
                    transport,
                    $($name: None),+
                }
            }

            $(
                pub fn $name(&self) -> &$type {
                    &self.$name
                }
            )+
        }

        impl std::fmt::Debug for StreamProperties {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let mut debug_struct = f.debug_struct("StreamProperties");

                debug_struct.field("transport", &self.transport);
                $(
                    if let Some($name) = &self.$name {
                        debug_struct.field(stringify!($name), $name);
                    }
                )+
                if let Some(parent) = &self.parent {
                    debug_struct.field("parent", parent);
                }

                debug_struct.finish()
            }
        }
    };
}

stream_properties! {
    /// The language or protocol of the stream
    pub language: Option<StreamType>,
    /// The local address of the connection
    pub local_addr: Option<ListenerAddress>,
    /// The peer address of the connection
    pub peer_addr: Option<ListenerAddress>,
    /// The peer credentials (for Unix domain sockets)
    pub peer_creds: Option<UCred>,
    /// The HTTP version used (for HTTP connections)
    pub http_version: Option<Version>,
    /// The HTTP request headers (for HTTP connections)
    pub request_headers: Option<HeaderMap>,
    /// The peer's SSL certificate (for SSL connections)
    pub peer_certificate: Option<X509>,
    /// The SSL/TLS version.
    pub ssl_version: Option<openssl::ssl::SslVersion>,
    /// The SSL/TLS version.
    pub ssl_cipher_name: Option<&'static str>,
    /// The Server Name Indication (SNI) provided by the client (for SSL connections)
    pub server_name_indication: Option<String>,
    /// The negotiated protocol (e.g., for ALPN in SSL connections, protocol for WebSocket)
    pub protocol: Option<&'static str>,
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
}

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

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TransportType {
    Tcp,
    Unix,
    Ssl,
    Http,
    WebSocket,
}

/// As we may be dealing with multiple types of streams, we have one top-level stream type that
/// dispatches to the appropriate underlying stream type.
pub struct ListenerStream {
    stream_properties: Arc<StreamProperties>,
    inner: ListenerStreamInner,
}

enum ListenerStreamInner {
    /// Raw TCP stream.
    Tcp(RewindStream<TcpStream>),
    /// Raw Unix stream.
    #[cfg(unix)]
    Unix(RewindStream<UnixStream>),
    /// Stream tunneled through HTTP request/response.
    Http(HyperStream),
    /// Upgraded stream (WebSocket, CONNECT, etc).
    WebSocket(HyperUpgradedStream),
    /// SSL stream.
    Ssl(RewindStream<tokio_openssl::SslStream<RewindStream<TcpStream>>>),
}

impl ListenerStream {
    pub fn new_tcp(stream: TcpStream) -> Self {
        let stream_properties = StreamProperties {
            peer_addr: stream.peer_addr().ok().map(|s| s.into()),
            local_addr: stream.peer_addr().ok().map(|s| s.into()),
            ..StreamProperties::new(TransportType::Tcp)
        }
        .into();
        ListenerStream {
            stream_properties,
            inner: ListenerStreamInner::Tcp(RewindStream::new(stream)),
        }
    }

    #[cfg(unix)]
    pub fn new_unix(
        stream: UnixStream,
        local_addr: Option<ListenerAddress>,
        peer_addr: Option<ListenerAddress>,
        peer_creds: Option<UCred>,
    ) -> Self {
        let stream_properties = StreamProperties {
            peer_addr,
            local_addr,
            peer_creds,
            ..StreamProperties::new(TransportType::Unix)
        }
        .into();
        ListenerStream {
            stream_properties,
            inner: ListenerStreamInner::Unix(RewindStream::new(stream)),
        }
    }

    pub fn new_websocket(stream_props: StreamProperties, stream: HyperUpgradedStream) -> Self {
        ListenerStream {
            stream_properties: stream_props.into(),
            inner: ListenerStreamInner::WebSocket(stream),
        }
    }

    pub async fn peek(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        match &mut self.inner {
            ListenerStreamInner::Tcp(stream) => {
                if !stream.buffer.is_empty() {
                    todo!()
                }
                stream.inner.peek(buf).await
            }
            ListenerStreamInner::Ssl(stream) => {
                if !stream.buffer.is_empty() {
                    todo!()
                }
                Pin::new(&mut stream.inner)
                    .peek(buf)
                    .await
                    .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))
            }
            _ => unimplemented!(),
        }
    }

    pub fn rewind(&mut self, buffer: &[u8]) {
        match &mut self.inner {
            ListenerStreamInner::Tcp(stream) => stream.rewind(buffer),
            ListenerStreamInner::Ssl(stream) => stream.rewind(buffer),
            _ => unimplemented!(),
        }
    }

    pub async fn start_ssl(self, ssl: openssl::ssl::Ssl) -> Result<Self, std::io::Error> {
        match self.inner {
            ListenerStreamInner::Tcp(stream) => {
                let mut parent_stream_properties = self.stream_properties.clone();

                let mut ssl_stream = tokio_openssl::SslStream::new(ssl, stream)?;
                let is_server = ssl_stream.ssl().is_server();
                let ssl = Pin::new(&mut ssl_stream);
                if is_server {
                    ssl.accept().await
                } else {
                    ssl.connect().await
                }
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

                let ssl = ssl_stream.ssl();
                let stream_properties = StreamProperties {
                    parent: Some(parent_stream_properties),
                    protocol: ssl.selected_alpn_protocol().and_then(known_protocol),
                    peer_certificate: ssl.peer_certificate(),
                    ssl_version: ssl.version2(),
                    ssl_cipher_name: ssl.current_cipher().map(|c| c.name()),
                    server_name_indication: ssl
                        .servername(NameType::HOST_NAME)
                        .map(|s| s.to_string()),
                    ..StreamProperties::new(TransportType::Ssl)
                }
                .into();

                Ok(ListenerStream {
                    stream_properties,
                    inner: ListenerStreamInner::Ssl(RewindStream::new(ssl_stream)),
                })
            }
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "SSL connection cannot be establed on this transport",
            )),
        }
    }

    /// If the underlying stream is SSL or a WebSocket, retrieves the negotiated
    /// protocol.
    pub fn selected_protocol(&self) -> Option<&'static str> {
        self.stream_properties.protocol
    }

    /// Returns the transport type of the underlying stream.
    pub fn transport_type(&self) -> TransportType {
        match self.inner {
            ListenerStreamInner::Tcp(..) => TransportType::Tcp,
            ListenerStreamInner::Ssl(..) => TransportType::Ssl,
            ListenerStreamInner::Http(..) => TransportType::Http,
            ListenerStreamInner::WebSocket(..) => TransportType::WebSocket,
            ListenerStreamInner::Unix(..) => TransportType::Unix,
        }
    }

    /// Returns the peer address of the underlying stream.
    #[inline(always)]
    pub fn props(&self) -> &StreamProperties {
        &self.stream_properties
    }

    /// Returns the peer address of the underlying stream.
    #[inline(always)]
    pub fn props_clone(&self) -> Arc<StreamProperties> {
        self.stream_properties.clone()
    }

    /// Returns the local address of the underlying stream.
    pub fn local_addr(&self) -> Option<&ListenerAddress> {
        self.stream_properties.local_addr.as_ref()
    }

    /// Returns the peer address of the underlying stream.
    pub fn peer_addr(&self) -> Option<&ListenerAddress> {
        self.stream_properties.local_addr.as_ref()
    }
}

macro_rules! with_mut_stream {
    ($self:expr, $stream:ident, $action:expr) => {
        match &mut $self.inner {
            ListenerStreamInner::Tcp($stream) => {
                let $stream = Pin::new($stream);
                $action
            }
            ListenerStreamInner::Ssl($stream) => {
                let $stream = Pin::new($stream);
                $action
            }
            ListenerStreamInner::Http($stream) => {
                let $stream = Pin::new($stream);
                $action
            }
            ListenerStreamInner::WebSocket($stream) => {
                let $stream = Pin::new($stream);
                $action
            }
            ListenerStreamInner::Unix($stream) => {
                let $stream = Pin::new($stream);
                $action
            }
        }
    };
}

macro_rules! with_ref_stream {
    ($self:expr, $stream:ident, $action:expr) => {
        match &$self.inner {
            ListenerStreamInner::Tcp($stream) => {
                let $stream = Pin::new($stream);
                $action
            }
            ListenerStreamInner::Ssl($stream) => {
                let $stream = Pin::new($stream);
                $action
            }
            ListenerStreamInner::Http($stream) => {
                let $stream = Pin::new($stream);
                $action
            }
            ListenerStreamInner::WebSocket($stream) => {
                let $stream = Pin::new($stream);
                $action
            }
            ListenerStreamInner::Unix($stream) => {
                let $stream = Pin::new($stream);
                $action
            }
        }
    };
}

impl std::fmt::Debug for ListenerStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}({:?})", self.transport_type(), self.props())
    }
}

impl AsyncRead for ListenerStream {
    #[inline]
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        with_mut_stream!(self.get_mut(), stream, stream.poll_read(cx, buf))
    }
}

impl AsyncWrite for ListenerStream {
    #[inline]
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        with_mut_stream!(self.get_mut(), stream, stream.poll_write(cx, buf))
    }

    #[inline]
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        with_mut_stream!(self.get_mut(), stream, stream.poll_flush(cx))
    }

    #[inline]
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        with_mut_stream!(self.get_mut(), stream, stream.poll_shutdown(cx))
    }

    #[inline]
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        with_mut_stream!(self.get_mut(), stream, stream.poll_write_vectored(cx, bufs))
    }

    #[inline]
    fn is_write_vectored(&self) -> bool {
        with_ref_stream!(self, stream, stream.as_ref().is_write_vectored())
    }
}
