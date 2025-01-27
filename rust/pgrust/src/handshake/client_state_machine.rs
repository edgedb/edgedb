use super::ConnectionSslRequirement;
use crate::{
    connection::{invalid_state, Credentials, PGConnectionError, SslError},
    errors::PgServerError,
    protocol::postgres::{
        builder,
        data::{
            AuthenticationCleartextPassword, AuthenticationMD5Password, AuthenticationMessage,
            AuthenticationOk, AuthenticationSASL, AuthenticationSASLContinue,
            AuthenticationSASLFinal, BackendKeyData, ErrorResponse, Message, NoticeResponse,
            ParameterStatus, ReadyForQuery, SSLResponse,
        },
        FrontendBuilder, InitialBuilder,
    },
};
use base64::Engine;
use db_proto::{match_message, ParseError};
use gel_auth::{
    scram::{generate_salted_password, ClientEnvironment, ClientTransaction, Sha256Out},
    AuthType,
};
use rand::Rng;
use tracing::{error, trace, warn};

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
    Connecting(Credentials, bool),
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

#[derive(Debug)]
pub enum ConnectionDrive<'a> {
    Initial,
    Message(Result<Message<'a>, ParseError>),
    SslResponse(SSLResponse<'a>),
    SslReady,
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
    fn server_error(&mut self, error: &PgServerError) {
        error!("Server error during handshake: {:?}", error);
    }
    fn server_notice(&mut self, notice: &PgServerError) {
        warn!("Server notice during handshake: {:?}", notice);
    }
    fn auth(&mut self, auth: AuthType) {}
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
#[derive(Debug)]
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

    pub fn is_error(&self) -> bool {
        matches!(self.0, ConnectionStateImpl::Error)
    }

    pub fn is_done(&self) -> bool {
        self.is_ready() || self.is_error()
    }

    pub fn read_ssl_response(&self) -> bool {
        matches!(self.0, ConnectionStateImpl::SslWaiting(..))
    }

    pub fn drive(
        &mut self,
        drive: ConnectionDrive,
        update: &mut impl ConnectionStateUpdate,
    ) -> Result<(), PGConnectionError> {
        use ConnectionStateImpl::*;
        trace!("Received drive {drive:?} in state {:?}", self.0);
        match (&mut self.0, drive) {
            (SslInitializing(credentials, mode), ConnectionDrive::Initial) => {
                update.send_initial(InitialBuilder::SSLRequest(builder::SSLRequest::default()))?;
                self.0 = SslWaiting(std::mem::take(credentials), *mode);
                update.state_changed(ConnectionStateType::Connecting);
            }
            (SslWaiting(credentials, mode), ConnectionDrive::SslResponse(response)) => {
                if *mode == ConnectionSslRequirement::Disable {
                    // Should not be possible
                    return Err(invalid_state!("SSL mode is Disable in SslWaiting state"));
                }

                if response.code() == b'S' {
                    // Accepted
                    update.upgrade()?;
                    self.0 = SslConnecting(std::mem::take(credentials));
                    update.state_changed(ConnectionStateType::SslConnecting);
                } else if response.code() == b'N' {
                    // Rejected
                    if *mode == ConnectionSslRequirement::Required {
                        return Err(PGConnectionError::SslError(SslError::SslRequiredByClient));
                    }
                    Self::send_startup_message(credentials, update)?;
                    self.0 = Connecting(std::mem::take(credentials), false);
                } else {
                    return Err(PGConnectionError::UnexpectedResponse(format!(
                        "Unexpected SSL response from server: {:?}",
                        response.code() as char
                    )));
                }
            }
            (SslConnecting(credentials), ConnectionDrive::SslReady) => {
                Self::send_startup_message(credentials, update)?;
                self.0 = Connecting(std::mem::take(credentials), false);
            }
            (Initializing(credentials), ConnectionDrive::Initial) => {
                Self::send_startup_message(credentials, update)?;
                self.0 = Connecting(std::mem::take(credentials), false);
                update.state_changed(ConnectionStateType::Connecting);
            }
            (Connecting(credentials, sent_auth), ConnectionDrive::Message(message)) => {
                match_message!(message, Backend {
                    (AuthenticationOk) => {
                        if !*sent_auth {
                            update.auth(AuthType::Trust);
                        }
                        trace!("auth ok");
                        self.0 = Connected;
                        update.state_changed(ConnectionStateType::Synchronizing);
                    },
                    (AuthenticationSASL as sasl) => {
                        *sent_auth = true;
                        let mut found_scram_sha256 = false;
                        for mech in sasl.mechanisms() {
                            trace!("auth sasl: {:?}", mech);
                            if mech == "SCRAM-SHA-256" {
                                found_scram_sha256 = true;
                                break;
                            }
                        }
                        if !found_scram_sha256 {
                            return Err(PGConnectionError::UnexpectedResponse("Server requested SASL authentication but does not support SCRAM-SHA-256".into()));
                        }
                        let credentials = credentials.clone();
                        let mut tx = ClientTransaction::new("".into());
                        let env = ClientEnvironmentImpl { credentials };
                        let Some(initial_message) = tx.process_message(&[], &env)? else {
                            return Err(gel_auth::scram::SCRAMError::ProtocolError.into());
                        };
                        update.auth(AuthType::ScramSha256);
                        update.send(builder::SASLInitialResponse {
                            mechanism: "SCRAM-SHA-256",
                            response: &initial_message,
                        }.into())?;
                        self.0 = Scram(tx, env);
                        update.state_changed(ConnectionStateType::Authenticating);
                    },
                    (AuthenticationMD5Password as md5) => {
                        *sent_auth = true;
                        trace!("auth md5");
                        let md5_hash = gel_auth::md5::md5_password(&credentials.password, &credentials.username, &md5.salt());
                        update.auth(AuthType::Md5);
                        update.send(builder::PasswordMessage {
                            password: &md5_hash,
                        }.into())?;
                    },
                    (AuthenticationCleartextPassword) => {
                        *sent_auth = true;
                        trace!("auth cleartext");
                        update.auth(AuthType::Plain);
                        update.send(builder::PasswordMessage {
                            password: &credentials.password,
                        }.into())?;
                    },
                    (NoticeResponse as notice) => {
                        let err = PgServerError::from(notice);
                        update.server_notice(&err);
                    },
                    (ErrorResponse as error) => {
                        self.0 = Error;
                        let err = PgServerError::from(error);
                        update.server_error(&err);
                        return Err(err.into());
                    },
                    message => {
                        log_unknown_message(message, "Connecting")?
                    },
                });
            }
            (Scram(tx, env), ConnectionDrive::Message(message)) => {
                match_message!(message, Backend {
                    (AuthenticationSASLContinue as sasl) => {
                        let Some(message) = tx.process_message(&sasl.data(), env)? else {
                            return Err(gel_auth::scram::SCRAMError::ProtocolError.into());
                        };
                        update.send(builder::SASLResponse {
                            response: &message,
                        }.into())?;
                    },
                    (AuthenticationSASLFinal as sasl) => {
                        let None = tx.process_message(&sasl.data(), env)? else {
                            return Err(gel_auth::scram::SCRAMError::ProtocolError.into());
                        };
                    },
                    (AuthenticationOk) => {
                        trace!("auth ok");
                        self.0 = Connected;
                        update.state_changed(ConnectionStateType::Synchronizing);
                    },
                    (AuthenticationMessage as auth) => {
                        trace!("SCRAM Unknown auth message: {}", auth.status())
                    },
                    (NoticeResponse as notice) => {
                        let err = PgServerError::from(notice);
                        update.server_notice(&err);
                    },
                    (ErrorResponse as error) => {
                        self.0 = Error;
                        let err = PgServerError::from(error);
                        update.server_error(&err);
                        return Err(err.into());
                    },
                    message => {
                        log_unknown_message(message, "SCRAM")?
                    },
                });
            }
            (Connected, ConnectionDrive::Message(message)) => {
                match_message!(message, Backend {
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
                        self.0 = Ready;
                        update.state_changed(ConnectionStateType::Ready);
                    },
                    (NoticeResponse as notice) => {
                        let err = PgServerError::from(notice);
                        update.server_notice(&err);
                    },
                    (ErrorResponse as error) => {
                        self.0 = Error;
                        let err = PgServerError::from(error);
                        update.server_error(&err);
                        return Err(err.into());
                    },
                    message => {
                        log_unknown_message(message, "Connected")?
                    },
                });
            }
            (Ready, _) | (Error, _) => {
                return Err(invalid_state!("Unexpected drive for Ready or Error state"))
            }
            _ => return Err(invalid_state!("Unexpected (state, drive) combination")),
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

fn log_unknown_message(
    message: Result<Message, ParseError>,
    state: &str,
) -> Result<(), ParseError> {
    match message {
        Ok(message) => {
            warn!(
                "Unexpected message {:?} (length {}) received in {} state",
                message.mtype(),
                message.mlen(),
                state
            );
            Ok(())
        }
        Err(e) => {
            error!("Corrupted message received in {} state", state);
            Err(e)
        }
    }
}
