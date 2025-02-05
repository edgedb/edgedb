use hyper::{
    body::{Body, Buf, Bytes},
    upgrade::Upgraded,
};
use hyper_util::rt::TokioIo;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::config::ListenerAddress;

/// A stream that wraps a [hyper::upgrade::Upgraded].
pub struct HyperUpgradedStream {
    inner: TokioIo<Upgraded>,
}

impl HyperUpgradedStream {
    pub fn new(upgraded: Upgraded) -> Self {
        HyperUpgradedStream {
            inner: TokioIo::new(upgraded),
        }
    }
}

impl AsyncRead for HyperUpgradedStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for HyperUpgradedStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<Result<usize, std::io::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

/// A stream that wraps a `hyper::body::Incoming` for reads, and provides
/// an mpsc channel of frames (bounded) for writes for a response body.
///
/// Note that an HTTP/1.x and HTTP/2 request/response pair _might_ be
/// technically duplex but we explicitly convert them to simplex here
/// because we cannot guarantee that a middleware box hasn't tampered with
/// the state.
pub struct HyperStream {
    state: StreamState,
    response_body_rx: tokio::sync::mpsc::Receiver<Bytes>,
    pub(crate) local_addr: ListenerAddress,
    pub(crate) peer_addr: ListenerAddress,
}

enum StreamState {
    Reading {
        incoming: hyper::body::Incoming,
        partial_frame: hyper::body::Bytes,
        response_body_tx: tokio::sync::mpsc::Sender<Bytes>,
    },
    Writing(tokio::sync::mpsc::Sender<Bytes>),
    Shutdown,
}

impl HyperStream {
    pub fn new(
        incoming: hyper::body::Incoming,
        local_addr: ListenerAddress,
        peer_addr: ListenerAddress,
    ) -> Self {
        let (response_body_tx, response_body_rx) = tokio::sync::mpsc::channel(10); // Adjust buffer size as needed
        HyperStream {
            state: StreamState::Reading {
                incoming,
                partial_frame: Bytes::new(),
                response_body_tx,
            },
            response_body_rx,
            local_addr,
            peer_addr,
        }
    }
}

impl AsyncRead for HyperStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();

        match &mut this.state {
            StreamState::Reading {
                incoming,
                partial_frame,
                ..
            } => {
                // If there are any partial bytes, copy them to the buffer first
                if !partial_frame.is_empty() {
                    let len = std::cmp::min(partial_frame.len(), buf.remaining());
                    buf.put_slice(&partial_frame[..len]);
                    partial_frame.advance(len);
                    if partial_frame.is_empty() {
                        *partial_frame = Bytes::new();
                    }
                    return Poll::Ready(Ok(()));
                }

                loop {
                    // Read from the incoming stream
                    break match Pin::new(&mut *incoming).poll_frame(cx) {
                        Poll::Ready(Some(Ok(mut data))) => {
                            // Ignore trailers
                            let Some(data) = data.data_mut() else {
                                continue;
                            };
                            let len = std::cmp::min(data.len(), buf.remaining());
                            buf.put_slice(&data[..len]);
                            if len < data.len() {
                                *partial_frame = data.slice(len..);
                            }
                            Poll::Ready(Ok(()))
                        }
                        Poll::Ready(Some(Err(e))) => {
                            Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::Other, e)))
                        }
                        Poll::Ready(None) => Poll::Ready(Ok(())),
                        Poll::Pending => Poll::Pending,
                    };
                }
            }
            StreamState::Writing(_) | StreamState::Shutdown => {
                Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "Stream is in writing or shutdown state",
                )))
            }
        }
    }
}

impl tokio::io::AsyncWrite for HyperStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        loop {
            break match &mut this.state {
                StreamState::Reading {
                    response_body_tx, ..
                } => {
                    // Transition to Writing state
                    let tx = response_body_tx.clone();
                    this.state = StreamState::Writing(tx);
                    // Fall through to Writing case
                    continue;
                }
                StreamState::Writing(outgoing) => {
                    match outgoing.try_send(Bytes::copy_from_slice(buf)) {
                        Ok(_) => Poll::Ready(Ok(buf.len())),
                        // **** NOCOMMIT **** This is wrong!
                        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                            todo!("This need to register the waker!")
                        }
                        Err(e) => {
                            Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::Other, e)))
                        }
                    }
                }
                StreamState::Shutdown => Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "Stream has been shut down",
                ))),
            };
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        match &this.state {
            StreamState::Writing(outgoing) => {
                // **** NOCOMMIT **** This is wrong!
                todo!("This need to register the waker!");
            }
            StreamState::Reading { .. } => Poll::Ready(Ok(())),
            StreamState::Shutdown => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Stream has been shut down",
            ))),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        this.state = StreamState::Shutdown;
        Poll::Ready(Ok(()))
    }
}

impl hyper::body::Body for HyperStream {
    type Data = hyper::body::Bytes;
    type Error = std::io::Error;
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        this.response_body_rx
            .poll_recv(cx)
            .map(|option| option.map(|bytes| Ok(hyper::body::Frame::data(bytes))))
    }

    fn is_end_stream(&self) -> bool {
        self.response_body_rx.is_closed()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        hyper::body::SizeHint::default()
    }
}
