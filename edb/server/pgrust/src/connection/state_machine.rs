use std::collections::HashMap;

use super::{invalid_state, ConnectionError, Credentials, ServerErrorField};
use crate::{
    auth::{self, generate_salted_password, ClientEnvironment, ClientTransaction, Sha256Out},
    connection::SslError,
    protocol::{
        builder,
        definition::{FrontendBuilder, InitialBuilder},
        match_message, AuthenticationCleartextPassword, AuthenticationMD5Password,
        AuthenticationMessage, AuthenticationOk, AuthenticationSASL, AuthenticationSASLContinue,
        AuthenticationSASLFinal, BackendKeyData, ErrorResponse, Message, ParameterStatus,
        ReadyForQuery, SSLResponse,
    },
};
use base64::Engine;
use rand::Rng;
use tracing::{trace, warn};

#[derive(Debug)]
struct ClientEnvironmentImpl {
    credentials: Credentials,
}

impl ClientEnvironment for ClientEnvironmentImpl {
    fn generate_nonce(&self) -> String {
        let nonce: [u8; 32] = rand::thread_rng().r#gen();
        base64::engine::general_purpose::STANDARD.encode(nonce)
    }
    fn get_salted_password(&self, salt: &[u8], iterations: usize) -> Sha256Out {
        generate_salted_password(self.credentials.password.as_bytes(), salt, iterations)
    }
}

#[derive(Debug)]
enum ConnectionStateImpl {
    /// Uninitialized connection state. Requires an initialization message to
    /// start.
    SslInitializing(Credentials, ConnectionSslRequirement),
    /// SSL upgrade message was sent, awaiting server response.
    SslWaiting(Credentials, ConnectionSslRequirement),
    /// SSL upgrade in progress, waiting for handshake to complete.
    SslConnecting(Credentials),
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

#[derive(Clone, Copy, Debug)]
pub enum ConnectionStateType {
    Connecting,
    SslConnecting,
    Authenticating,
    Synchronizing,
    Ready,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum Authentication {
    #[default]
    None,
    Password,
    Md5,
    ScramSha256,
}

#[derive(Debug)]
pub enum ConnectionDrive<'a> {
    Initial,
    Message(Message<'a>),
    SslResponse(SSLResponse<'a>),
    SslReady,
}

impl<'a> ConnectionDrive<'a> {
    pub fn message(&self) -> Result<&Message<'a>, ConnectionError> {
        match self {
            ConnectionDrive::Message(msg) => Ok(msg),
            _ => Err(invalid_state!(
                "Expected Message variant, but got a different ConnectionDrive variant"
            )),
        }
    }
}

pub trait ConnectionStateSend {
    fn send_initial(&mut self, message: InitialBuilder) -> Result<(), std::io::Error>;
    fn send(&mut self, message: FrontendBuilder) -> Result<(), std::io::Error>;
    fn upgrade(&mut self) -> Result<(), std::io::Error>;
}

/// A callback for connection state changes.
#[allow(unused)]
pub trait ConnectionStateUpdate: ConnectionStateSend {
    fn parameter(&mut self, name: &str, value: &str) {}
    fn cancellation_key(&mut self, pid: i32, key: i32) {}
    fn state_changed(&mut self, state: ConnectionStateType) {}
    fn server_error(&mut self, error: &ErrorResponse) {}
    fn auth(&mut self, auth: Authentication) {}
}

#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub enum ConnectionSslRequirement {
    #[default]
    Disable,
    Optional,
    Required,
}

/// ASCII state diagram for the connection state machine
///
/// ```mermaid
/// stateDiagram-v2
///     [*] --> SslInitializing: SSL not disabled
///     [*] --> Initializing: SSL disabled
///     SslInitializing --> SslWaiting: Send SSL request
///     SslWaiting --> SslConnecting: SSL accepted
///     SslWaiting --> Connecting: SSL rejected (if not required)
///     SslConnecting --> Connecting: SSL handshake complete
///     Initializing --> Connecting: Send startup message
///     Connecting --> Connected: Authentication successful
///     Connecting --> Scram: SCRAM auth requested
///     Scram --> Connected: SCRAM auth successful
///     Connected --> Ready: Parameter sync complete
///     Ready --> [*]: Connection closed
///     state Error {
///         [*] --> [*]: Any state can transition to Error
///     }
/// ```
///
/// The state machine for a Postgres connection. The state machine is driven
/// with calls to [`Self::drive`].
pub struct ConnectionState(ConnectionStateImpl);

impl ConnectionState {
    pub fn new(credentials: Credentials, ssl_mode: ConnectionSslRequirement) -> Self {
        if ssl_mode == ConnectionSslRequirement::Disable {
            Self(ConnectionStateImpl::Initializing(credentials))
        } else {
            Self(ConnectionStateImpl::SslInitializing(credentials, ssl_mode))
        }
    }

    pub fn is_ready(&self) -> bool {
        matches!(self.0, ConnectionStateImpl::Ready)
    }

    pub fn read_ssl_response(&self) -> bool {
        matches!(self.0, ConnectionStateImpl::SslWaiting(..))
    }

    pub fn drive(
        &mut self,
        drive: ConnectionDrive,
        update: &mut impl ConnectionStateUpdate,
    ) -> Result<(), ConnectionError> {
        use ConnectionStateImpl::*;
        let state = &mut self.0;
        trace!("Received drive {drive:?} in state {state:?}");
        match state {
            SslInitializing(credentials, mode) => {
                if !matches!(drive, ConnectionDrive::Initial) {
                    return Err(invalid_state!(
                        "Expected Initial drive for SslInitializing state"
                    ));
                }
                update.send_initial(InitialBuilder::SSLRequest(builder::SSLRequest::default()))?;
                *state = SslWaiting(std::mem::take(credentials), *mode);
                update.state_changed(ConnectionStateType::Connecting);
            }
            SslWaiting(credentials, mode) => {
                let ConnectionDrive::SslResponse(response) = drive else {
                    return Err(invalid_state!(
                        "Expected SslResponse drive for SslWaiting state"
                    ));
                };

                if *mode == ConnectionSslRequirement::Disable {
                    // Should not be possible
                    return Err(invalid_state!("SSL mode is Disable in SslWaiting state"));
                }

                if response.code() == b'S' {
                    // Accepted
                    update.upgrade()?;
                    *state = SslConnecting(std::mem::take(credentials));
                    update.state_changed(ConnectionStateType::SslConnecting);
                } else if response.code() == b'N' {
                    // Rejected
                    if *mode == ConnectionSslRequirement::Required {
                        return Err(ConnectionError::SslError(SslError::SslRequiredByClient));
                    }
                    Self::send_startup_message(credentials, update)?;
                    *state = Connecting(std::mem::take(credentials));
                } else {
                    return Err(ConnectionError::UnexpectedServerResponse(format!(
                        "Unexpected SSL response from server: {:?}",
                        response.code() as char
                    )));
                }
            }
            SslConnecting(credentials) => {
                let ConnectionDrive::SslReady = drive else {
                    return Err(invalid_state!(
                        "Expected SslReady drive for SslConnecting state"
                    ));
                };
                Self::send_startup_message(credentials, update)?;
                *state = Connecting(std::mem::take(credentials));
            }
            Initializing(credentials) => {
                if !matches!(drive, ConnectionDrive::Initial) {
                    return Err(invalid_state!(
                        "Expected Initial drive for Initializing state"
                    ));
                }
                Self::send_startup_message(credentials, update)?;
                *state = Connecting(std::mem::take(credentials));
                update.state_changed(ConnectionStateType::Connecting);
            }
            Connecting(credentials) => {
                match_message!(drive.message()?, Backend {
                    (AuthenticationOk) => {
                        trace!("auth ok");
                        *state = Connected;
                        update.state_changed(ConnectionStateType::Synchronizing);
                    },
                    (AuthenticationSASL as sasl) => {
                        let mut found_scram_sha256 = false;
                        for mech in sasl.mechanisms() {
                            trace!("auth sasl: {:?}", mech);
                            if mech == "SCRAM-SHA-256" {
                                found_scram_sha256 = true;
                                break;
                            }
                        }
                        if !found_scram_sha256 {
                            return Err(ConnectionError::UnexpectedServerResponse("Server requested SASL authentication but does not support SCRAM-SHA-256".into()));
                        }
                        let credentials = credentials.clone();
                        let mut tx = ClientTransaction::new("".into());
                        let env = ClientEnvironmentImpl { credentials };
                        let Some(initial_message) = tx.process_message(&[], &env)? else {
                            return Err(auth::SCRAMError::ProtocolError.into());
                        };
                        update.auth(Authentication::ScramSha256);
                        update.send(builder::SASLInitialResponse {
                            mechanism: "SCRAM-SHA-256",
                            response: &initial_message,
                        }.into())?;
                        *state = Scram(tx, env);
                        update.state_changed(ConnectionStateType::Authenticating);
                    },
                    (AuthenticationMD5Password as md5) => {
                        trace!("auth md5");
                        let md5_hash = auth::md5_password(&credentials.password, &credentials.username, &md5.salt());
                        update.auth(Authentication::Md5);
                        update.send(builder::PasswordMessage {
                            password: &md5_hash,
                        }.into())?;
                    },
                    (AuthenticationCleartextPassword) => {
                        trace!("auth cleartext");
                        update.auth(Authentication::Password);
                        update.send(builder::PasswordMessage {
                            password: &credentials.password,
                        }.into())?;
                    },
                    (ErrorResponse as error) => {
                        *state = Error;
                        update.server_error(&error);
                        return Err(error_to_server_error(error));
                    },
                    message => {
                        log_unknown_message(message, "Connecting")
                    },
                });
            }
            Scram(tx, env) => {
                match_message!(drive.message()?, Backend {
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
                        trace!("auth ok");
                        *state = Connected;
                        update.state_changed(ConnectionStateType::Synchronizing);
                    },
                    (AuthenticationMessage as auth) => {
                        trace!("SCRAM Unknown auth message: {}", auth.status())
                    },
                    (ErrorResponse as error) => {
                        *state = Error;
                        update.server_error(&error);
                        return Err(error_to_server_error(error));
                    },
                    message => {
                        log_unknown_message(message, "SCRAM")
                    },
                });
            }
            Connected => {
                match_message!(drive.message()?, Backend {
                    (ParameterStatus as param) => {
                        trace!("param: {:?}={:?}", param.name(), param.value());
                        update.parameter(param.name().try_into()?, param.value().try_into()?);
                    },
                    (BackendKeyData as key_data) => {
                        trace!("key={:?} pid={:?}", key_data.key(), key_data.pid());
                        update.cancellation_key(key_data.pid(), key_data.key());
                    },
                    (ReadyForQuery as ready) => {
                        trace!("ready: {:?}", ready.status() as char);
                        trace!("-> Ready");
                        *state = Ready;
                        update.state_changed(ConnectionStateType::Ready);
                    },
                    (ErrorResponse as error) => {
                        *state = Error;
                        update.server_error(&error);
                        return Err(error_to_server_error(error));
                    },
                    message => {
                        log_unknown_message(message, "Connected")
                    },
                });
            }
            Ready | Error => {
                return Err(invalid_state!("Unexpected drive for Ready or Error state"))
            }
        }
        Ok(())
    }

    fn send_startup_message(
        credentials: &Credentials,
        update: &mut impl ConnectionStateUpdate,
    ) -> Result<(), std::io::Error> {
        let mut params = vec![
            builder::StartupNameValue {
                name: "user",
                value: &credentials.username,
            },
            builder::StartupNameValue {
                name: "database",
                value: &credentials.database,
            },
        ];
        for (name, value) in &credentials.server_settings {
            params.push(builder::StartupNameValue { name, value })
        }

        update.send_initial(InitialBuilder::StartupMessage(builder::StartupMessage {
            params: &params,
        }))
    }
}

fn log_unknown_message(message: &Message, state: &str) {
    warn!(
        "Unexpected message {:?} (length {}) received in {} state",
        message.mtype(),
        message.mlen(),
        state
    );
}

fn error_to_server_error(error: ErrorResponse) -> ConnectionError {
    let mut code = String::new();
    let mut message = String::new();
    let mut extra = HashMap::new();

    for field in error.fields() {
        let value = field.value().to_string_lossy().into_owned();
        match ServerErrorField::try_from(field.etype()) {
            Ok(ServerErrorField::Code) => code = value,
            Ok(ServerErrorField::Message) => message = value,
            Ok(field_type) => {
                extra.insert(field_type, value);
            }
            Err(_) => warn!(
                "Unxpected server error field: {:?} ({:?})",
                field.etype() as char,
                value
            ),
        }
    }

    ConnectionError::ServerError {
        code,
        message,
        extra,
    }
}
