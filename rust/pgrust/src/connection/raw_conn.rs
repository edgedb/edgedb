use super::{invalid_state, Credentials, PGConnectionError};
use crate::handshake::{
    client::{
        ConnectionDrive, ConnectionState, ConnectionStateSend, ConnectionStateType,
        ConnectionStateUpdate,
    },
    ConnectionSslRequirement,
};
use crate::protocol::postgres::{FrontendBuilder, InitialBuilder};
use crate::protocol::{postgres::data::SSLResponse, postgres::meta};
use db_proto::StructBuffer;
use gel_auth::AuthType;
use gel_stream::client::{
    stream::{Stream, StreamWithUpgrade, UpgradableStream},
    Connector,
};
use std::collections::HashMap;
use std::pin::Pin;
use tokio::io::AsyncWriteExt;
use tracing::{trace, Level};

#[derive(Clone, Default, Debug)]
pub struct ConnectionParams {
    pub ssl: bool,
    pub params: HashMap<String, String>,
    pub cancellation_key: (i32, i32),
    pub auth: AuthType,
}

pub struct ConnectionDriver {
    send_buffer: Vec<u8>,
    upgrade: bool,
    params: ConnectionParams,
}

impl ConnectionStateSend for ConnectionDriver {
    fn send_initial(&mut self, message: InitialBuilder) -> Result<(), std::io::Error> {
        self.send_buffer.extend(message.to_vec());
        Ok(())
    }
    fn send(&mut self, message: FrontendBuilder) -> Result<(), std::io::Error> {
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
    fn auth(&mut self, auth: AuthType) {
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
    ) -> Result<(), PGConnectionError>
    where
        (B, C): StreamWithUpgrade,
    {
        message_buffer.push_fallible(drive, |msg| {
            state.drive(ConnectionDrive::Message(msg), self)
        })?;
        loop {
            if !self.send_buffer.is_empty() {
                if tracing::enabled!(Level::TRACE) {
                    trace!("Write:");
                    for s in hexdump::hexdump_iter(&self.send_buffer) {
                        trace!("{}", s);
                    }
                }
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
    ) -> Result<(), PGConnectionError>
    where
        (B, C): StreamWithUpgrade,
    {
        state.drive(drive, self)?;
        loop {
            if !self.send_buffer.is_empty() {
                if tracing::enabled!(Level::TRACE) {
                    trace!("Write:");
                    for s in hexdump::hexdump_iter(&self.send_buffer) {
                        trace!("{}", s);
                    }
                }
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

/// A raw client connection stream to a Postgres server, fully authenticated and
/// ready to send queries.
///
/// This can be connected to a remote server using `connect`, or can be created
/// with a pre-existing, pre-authenticated stream.
#[derive(derive_more::Debug)]
pub struct RawClient {
    #[debug(skip)]
    stream: Pin<Box<dyn Stream>>,
    params: ConnectionParams,
}

impl RawClient {
    /// Create a new `RawClient` from a given fully-authenticated stream.
    #[inline]
    pub fn new<S: Stream + 'static>(stream: S, params: ConnectionParams) -> Self {
        Self {
            stream: Box::pin(stream),
            params,
        }
    }

    /// Create a new `RawClient` from a given fully-authenticated and boxed stream.
    #[inline]
    pub fn new_boxed(stream: Box<dyn Stream>, params: ConnectionParams) -> Self {
        Self {
            stream: Box::into_pin(stream),
            params,
        }
    }

    /// Attempt to connect to a Postgres server using a given connector and SSL requirement.
    pub async fn connect(
        credentials: Credentials,
        ssl_mode: ConnectionSslRequirement,
        connector: Connector,
    ) -> Result<RawClient, PGConnectionError> {
        let mut state = ConnectionState::new(credentials, ssl_mode);
        let mut stream = connector.connect().await?;

        let mut update = ConnectionDriver::new();
        update
            .drive(&mut state, ConnectionDrive::Initial, &mut stream)
            .await?;

        let mut struct_buffer: StructBuffer<meta::Message> =
            StructBuffer::<meta::Message>::default();

        while !state.is_ready() {
            let mut buffer = [0; 1024];
            let n = tokio::io::AsyncReadExt::read(&mut stream, &mut buffer).await?;
            if n == 0 {
                Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof))?;
            }
            if tracing::enabled!(Level::TRACE) {
                trace!("Read:");
                let bytes: &[u8] = &buffer[..n];
                for s in hexdump::hexdump_iter(bytes) {
                    trace!("{}", s);
                }
            }
            if state.read_ssl_response() {
                let ssl_response = SSLResponse::new(&buffer)?;
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

        // This should not be possible -- we've fully upgraded the stream by now
        let Ok(stream) = stream.into_choice() else {
            return Err(invalid_state!("Connection was not ready"));
        };

        Ok(RawClient::new_boxed(stream.into_boxed(), update.params))
    }

    /// Consume the `RawClient` and return the underlying stream and connection parameters.
    #[inline]
    pub fn into_parts(self) -> (Pin<Box<dyn Stream>>, ConnectionParams) {
        (self.stream, self.params)
    }
}
