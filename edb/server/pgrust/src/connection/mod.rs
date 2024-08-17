use crate::{
    auth::{self, generate_salted_password, ClientEnvironment, ClientTransaction, Sha256Out},
    protocol::{
        builder,
        definition::{FrontendBuilder, InitialBuilder},
        match_message, AuthenticationMessage, AuthenticationOk, AuthenticationSASL,
        AuthenticationSASLContinue, AuthenticationSASLFinal, BackendKeyData, ErrorResponse,
        Message, ParameterStatus, ReadyForQuery,
    },
};
use base64::Engine;
use rand::Rng;
use tracing::warn;

#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("Invalid state")]
    InvalidState,
    #[error("Server error: {0}")]
    ServerError(String),
    #[error("SCRAM: {0}")]
    Scram(#[from] auth::SCRAMError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("UTF8 error: {0}")]
    Utf8Error(#[from] std::str::Utf8Error),
}

#[derive(Clone, Default)]
pub struct Credentials {
    pub username: String,
    pub password: String,
    pub database: String,
}

struct ClientEnvironmentImpl {
    credentials: Credentials,
}

impl ClientEnvironment for ClientEnvironmentImpl {
    fn generate_nonce(&self) -> String {
        let nonce: [u8; 32] = rand::thread_rng().r#gen();
        base64::engine::general_purpose::STANDARD.encode(nonce)
    }
    fn get_salted_password(&self, salt: &[u8], iterations: usize) -> Sha256Out {
        generate_salted_password(&self.credentials.password, salt, iterations)
    }
}

enum ConnectionStateImpl {
    /// Uninitialized connection state. Requires an initialization message to
    /// start.
    Initializing(Credentials),
    /// The initial connection string has been sent and we are waiting for an
    /// auth response.
    Connecting(Credentials),
    /// The server has requested SCRAM auth. This holds a sub-state-machine that
    /// manages a SCRAM challenge.
    Scram(ClientTransaction, ClientEnvironmentImpl),
    /// The authentication is successful and we are synchronizing server
    /// parameters.
    Connected,
    /// The server is ready for queries.
    Ready,
    /// The connection failed.
    Error,
}

pub enum ConnectionStateType {
    Connecting,
    Authenticating,
    Synchronizing,
    Ready,
}

pub trait ConnectionStateSend {
    fn send_initial(&mut self, message: InitialBuilder) -> Result<(), std::io::Error>;
    fn send(&mut self, message: FrontendBuilder) -> Result<(), std::io::Error>;
}

/// A callback for connection state changes.
pub trait ConnectionStateUpdate: ConnectionStateSend {
    fn parameter(&self, _name: &str, _value: &str) {}
    fn cancellation_key(&self, _pid: i32, _key: i32) {}
    fn state_changed(&self, _state: ConnectionStateType) {}
}

/// The state machine for a Postgres connection. The state machine is driven
/// with calls to [`Self::drive`].
pub struct ConnectionState(ConnectionStateImpl);

impl ConnectionState {
    pub fn new(credentials: Credentials) -> Self {
        Self(ConnectionStateImpl::Initializing(credentials))
    }

    pub fn is_ready(&self) -> bool {
        matches!(self.0, ConnectionStateImpl::Ready)
    }

    pub fn drive(
        &mut self,
        message: Option<Message>,
        update: &mut impl ConnectionStateUpdate,
    ) -> Result<(), ConnectionError> {
        use ConnectionStateImpl::*;
        let state = &mut self.0;
        let message = message.ok_or(ConnectionError::InvalidState);
        match state {
            Initializing(credentials) => {
                update.send_initial(InitialBuilder::StartupMessage(builder::StartupMessage {
                    params: &[
                        builder::StartupNameValue {
                            name: "user",
                            value: &credentials.username,
                        },
                        builder::StartupNameValue {
                            name: "database",
                            value: &credentials.database,
                        },
                    ],
                }))?;
                *state = Connecting(std::mem::take(credentials));
                update.state_changed(ConnectionStateType::Connecting);
            }
            Connecting(credentials) => {
                match_message!(message?, Backend {
                    (AuthenticationOk) => {
                        tracing::trace!("auth ok");
                        *state = Connected;
                        update.state_changed(ConnectionStateType::Synchronizing);
                    },
                    (AuthenticationSASL as sasl) => {
                        for mech in sasl.mechanisms() {
                            tracing::trace!("sasl: {:?}", mech);
                        }
                        let credentials = credentials.clone();
                        let mut tx = ClientTransaction::new("".into());
                        let env = ClientEnvironmentImpl { credentials };
                        let Some(initial_message) = tx.process_message(&[], &env)? else {
                            return Err(auth::SCRAMError::ProtocolError.into());
                        };
                        update.send(builder::SASLInitialResponse {
                            mechanism: "SCRAM-SHA-256",
                            response: &initial_message,
                        }.into())?;
                        *state = Scram(tx, env);
                        update.state_changed(ConnectionStateType::Authenticating);
                    },
                    (ErrorResponse as error) => {
                        for field in error.fields() {
                            eprintln!("error: {} {:?}", field.etype(), field.value());
                        }
                        return Err(ConnectionError::ServerError("todo".into()));
                    },
                    message => {
                        let mlen = message.mlen();
                        warn!("Connecting Unknown message: {} (len {mlen})", message.mtype() as char)
                    },
                });
            }
            Scram(tx, env) => {
                match_message!(message?, Backend {
                    (AuthenticationSASLContinue as sasl) => {
                        let Some(message) = tx.process_message(&sasl.data(), env)? else {
                            return Err(auth::SCRAMError::ProtocolError.into());
                        };
                        update.send(builder::SASLResponse {
                            response: &message,
                        }.into())?;
                    },
                    (AuthenticationSASLFinal as sasl) => {
                        let None = tx.process_message(&sasl.data(), env)? else {
                            return Err(auth::SCRAMError::ProtocolError.into());
                        };
                    },
                    (AuthenticationOk) => {
                        eprintln!("auth ok");
                        eprintln!("-> Connected");
                        *state = Connected;
                        update.state_changed(ConnectionStateType::Synchronizing);
                    },
                    (AuthenticationMessage as auth) => {
                        eprintln!("SCRAM Unknown auth message: {}", auth.status())
                    },
                    (ErrorResponse as error) => {
                        for field in error.fields() {
                            eprintln!("error: {} {:?}", field.etype(), field.value());
                        }
                        return Err(ConnectionError::ServerError("todo".into()));
                    },
                    message => {
                        let mlen = message.mlen();
                        eprintln!("SCRAM Unknown message: {} (len {mlen})", message.mtype() as char)
                    },
                });
            }
            Connected => {
                match_message!(message?, Backend {
                    (ParameterStatus as param) => {
                        eprintln!("param: {:?}={:?}", param.name(), param.value());
                        update.parameter(param.name().try_into()?, param.value().try_into()?);
                    },
                    (BackendKeyData as key_data) => {
                        eprintln!("key={:?} pid={:?}", key_data.key(), key_data.pid());
                        update.cancellation_key(key_data.pid(), key_data.key());
                    },
                    (ReadyForQuery as ready) => {
                        eprintln!("ready: {:?}", ready.status() as char);
                        eprintln!("-> Ready");
                        *state = Ready;
                        update.state_changed(ConnectionStateType::Ready);
                    },
                    (ErrorResponse as error) => {
                        for field in error.fields() {
                            eprintln!("error: {} {:?}", field.etype(), field.value());
                        }
                        *state = Error;
                        return Err(ConnectionError::ServerError("todo".into()));
                    },
                    message => {
                        let mlen = message.mlen();
                        warn!("Connected Unknown message: {} (len {mlen})", message.mtype() as char)
                    },
                });
            }
            Ready | Error => return Err(ConnectionError::InvalidState),
        }
        Ok(())
    }
}
