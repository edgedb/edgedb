use super::state_machine::{
    Authentication, ConnectionDrive, ConnectionSslRequirement, ConnectionState,
    ConnectionStateSend, ConnectionStateType, ConnectionStateUpdate,
};
use super::{
    stream::{Stream, StreamWithUpgrade, UpgradableStream},
    ConnectionError, Credentials,
};
use crate::protocol::{meta, SSLResponse, StructBuffer};
use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::AsyncWriteExt;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tracing::trace;

#[derive(Clone, Default, Debug)]
pub struct ConnectionParams {
    pub ssl: bool,
    pub params: HashMap<String, String>,
    pub cancellation_key: (i32, i32),
    pub auth: Authentication,
}

pub struct ConnectionDriver {
    send_buffer: Vec<u8>,
    upgrade: bool,
    params: ConnectionParams,
}

impl ConnectionStateSend for ConnectionDriver {
    fn send_initial(
        &mut self,
        message: crate::protocol::definition::InitialBuilder,
    ) -> Result<(), std::io::Error> {
        self.send_buffer.extend(message.to_vec());
        Ok(())
    }
    fn send(
        &mut self,
        message: crate::protocol::definition::FrontendBuilder,
    ) -> Result<(), std::io::Error> {
        self.send_buffer.extend(message.to_vec());
        Ok(())
    }
    fn upgrade(&mut self) -> Result<(), std::io::Error> {
        self.upgrade = true;
        self.params.ssl = true;
        Ok(())
    }
}

impl ConnectionStateUpdate for ConnectionDriver {
    fn state_changed(&mut self, state: ConnectionStateType) {
        trace!("State: {state:?}");
    }
    fn cancellation_key(&mut self, pid: i32, key: i32) {
        self.params.cancellation_key = (pid, key);
    }
    fn parameter(&mut self, name: &str, value: &str) {
        self.params.params.insert(name.to_owned(), value.to_owned());
    }
    fn auth(&mut self, auth: Authentication) {
        trace!("Auth: {auth:?}");
        self.params.auth = auth;
    }
}

impl ConnectionDriver {
    pub fn new() -> Self {
        Self {
            send_buffer: Vec::new(),
            upgrade: false,
            params: ConnectionParams::default(),
        }
    }

    async fn drive_bytes<B: Stream, C: Unpin>(
        &mut self,
        state: &mut ConnectionState,
        drive: &[u8],
        message_buffer: &mut StructBuffer<meta::Message>,
        stream: &mut UpgradableStream<B, C>,
    ) -> Result<(), ConnectionError>
    where
        (B, C): StreamWithUpgrade,
    {
        message_buffer.push_fallible(drive, |msg| {
            state.drive(ConnectionDrive::Message(msg), self)
        })?;
        loop {
            if !self.send_buffer.is_empty() {
                println!("Write:");
                hexdump::hexdump(&self.send_buffer);
                stream.write_all(&self.send_buffer).await?;
                self.send_buffer.clear();
            }
            if self.upgrade {
                self.upgrade = false;
                stream.secure_upgrade().await?;
                state.drive(ConnectionDrive::SslReady, self)?;
            } else {
                break;
            }
        }
        Ok(())
    }

    async fn drive<B: Stream, C: Unpin>(
        &mut self,
        state: &mut ConnectionState,
        drive: ConnectionDrive<'_>,
        stream: &mut UpgradableStream<B, C>,
    ) -> Result<(), ConnectionError>
    where
        (B, C): StreamWithUpgrade,
    {
        state.drive(drive, self)?;
        loop {
            if !self.send_buffer.is_empty() {
                println!("Write:");
                hexdump::hexdump(&self.send_buffer);
                stream.write_all(&self.send_buffer).await?;
                self.send_buffer.clear();
            }
            if self.upgrade {
                self.upgrade = false;
                stream.secure_upgrade().await?;
                state.drive(ConnectionDrive::SslReady, self)?;
            } else {
                break;
            }
        }
        Ok(())
    }
}

/// A raw, fully-authenticated stream connection to a backend server.
pub struct RawClient<B: Stream, C: Unpin>
where
    (B, C): StreamWithUpgrade,
{
    stream: UpgradableStream<B, C>,
    params: ConnectionParams,
}

impl<B: Stream, C: Unpin> RawClient<B, C>
where
    (B, C): StreamWithUpgrade,
{
    pub fn params(&self) -> &ConnectionParams {
        &self.params
    }
}

impl<B: Stream, C: Unpin> AsyncRead for RawClient<B, C>
where
    (B, C): StreamWithUpgrade,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_read(cx, buf)
    }
}

impl<B: Stream, C: Unpin> AsyncWrite for RawClient<B, C>
where
    (B, C): StreamWithUpgrade,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        Pin::new(&mut self.get_mut().stream).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}

pub async fn connect_raw_ssl<B: Stream, C: Unpin>(
    credentials: Credentials,
    ssl_mode: ConnectionSslRequirement,
    config: C,
    socket: B,
) -> Result<RawClient<B, C>, ConnectionError>
where
    (B, C): StreamWithUpgrade,
{
    let mut state = ConnectionState::new(credentials, ssl_mode);
    let mut stream = UpgradableStream::from((socket, config));

    let mut update = ConnectionDriver::new();
    update
        .drive(&mut state, ConnectionDrive::Initial, &mut stream)
        .await?;

    let mut struct_buffer: StructBuffer<meta::Message> = StructBuffer::<meta::Message>::default();

    while !state.is_ready() {
        let mut buffer = [0; 1024];
        let n = tokio::io::AsyncReadExt::read(&mut stream, &mut buffer).await?;
        if n == 0 {
            Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof))?;
        }
        println!("Read:");
        hexdump::hexdump(&buffer[..n]);
        if state.read_ssl_response() {
            let ssl_response = SSLResponse::new(&buffer);
            update
                .drive(
                    &mut state,
                    ConnectionDrive::SslResponse(ssl_response),
                    &mut stream,
                )
                .await?;
            continue;
        }

        update
            .drive_bytes(&mut state, &buffer[..n], &mut struct_buffer, &mut stream)
            .await?;
    }
    Ok(RawClient {
        stream,
        params: update.params,
    })
}
