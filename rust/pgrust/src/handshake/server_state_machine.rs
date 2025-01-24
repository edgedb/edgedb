use super::ConnectionSslRequirement;
use crate::{
    connection::PGConnectionError,
    errors::{
        PgError, PgErrorConnectionException, PgErrorFeatureNotSupported,
        PgErrorInvalidAuthorizationSpecification, PgServerError, PgServerErrorField,
    },
    protocol::postgres::{data::*, *},
};
use db_proto::{match_message, ParseError, StructBuffer};
use gel_auth::{
    handshake::{ServerAuth, ServerAuthDrive, ServerAuthError, ServerAuthResponse},
    AuthType, CredentialData,
};
use std::str::Utf8Error;
use tracing::{error, trace, warn};

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
    /// Raw bytes from a client.
    RawMessage(&'a [u8]),
    /// Initial message from client.
    Initial(Result<InitialMessage<'a>, ParseError>),
    /// Non-initial message from the client.
    Message(Result<Message<'a>, ParseError>),
    /// SSL is ready.
    SslReady,
    /// Provide authentication information. The environment may supply credential data
    /// that doesn't match the auth type. In such cases, the server will try to adapt
    /// the auth data appropriately.
    ///
    /// Additionally, the environment can provide a "Trust" credential for automatic
    /// success or a "Deny" credential for automatic failure. The server will simulate
    /// a login process before unconditionally succeeding or failing in these cases.
    AuthInfo(AuthType, CredentialData),
    /// Once authorized, the server may sync any number of parameters until ready.
    Parameter(String, String),
    /// Ready, handshake complete.
    Ready(i32, i32),
    /// Fail the connection with a Postgres error code and message.
    Fail(PgError, &'a str),
}

pub trait ConnectionStateSend {
    /// Send the response to the SSL initiation.
    fn send_ssl(&mut self, message: builder::SSLResponse) -> Result<(), std::io::Error>;
    /// Send an ordinary message.
    fn send(&mut self, message: BackendBuilder) -> Result<(), std::io::Error>;
    /// Perform the SSL upgrade.
    fn upgrade(&mut self) -> Result<(), std::io::Error>;
    /// Notify the environment that a user and database were selected.
    fn auth(&mut self, user: String, database: String) -> Result<(), std::io::Error>;
    /// Notify the environment that parameters are requested.
    fn params(&mut self) -> Result<(), std::io::Error>;
}

/// A callback for connection state changes.
#[allow(unused)]
pub trait ConnectionStateUpdate: ConnectionStateSend {
    fn parameter(&mut self, name: &str, value: &str) {}
    fn state_changed(&mut self, state: ConnectionStateType) {}
    fn server_error(&mut self, error: &PgServerError) {}
}

#[derive(Debug)]
pub enum ConnectionEvent<'a> {
    SendSSL(builder::SSLResponse<'a>),
    Send(BackendBuilder<'a>),
    Upgrade,
    Auth(String, String),
    Params,
    Parameter(&'a str, &'a str),
    StateChanged(ConnectionStateType),
    ServerError(&'a PgServerError),
}

impl<F> ConnectionStateSend for F
where
    F: FnMut(ConnectionEvent) -> Result<(), std::io::Error>,
{
    fn send_ssl(&mut self, message: builder::SSLResponse) -> Result<(), std::io::Error> {
        self(ConnectionEvent::SendSSL(message))
    }

    fn send(&mut self, message: BackendBuilder) -> Result<(), std::io::Error> {
        self(ConnectionEvent::Send(message))
    }

    fn upgrade(&mut self) -> Result<(), std::io::Error> {
        self(ConnectionEvent::Upgrade)
    }

    fn auth(&mut self, user: String, database: String) -> Result<(), std::io::Error> {
        self(ConnectionEvent::Auth(user, database))
    }

    fn params(&mut self) -> Result<(), std::io::Error> {
        self(ConnectionEvent::Params)
    }
}

impl<F> ConnectionStateUpdate for F
where
    F: FnMut(ConnectionEvent) -> Result<(), std::io::Error>,
{
    fn parameter(&mut self, name: &str, value: &str) {
        let _ = self(ConnectionEvent::Parameter(name, value));
    }

    fn state_changed(&mut self, state: ConnectionStateType) {
        let _ = self(ConnectionEvent::StateChanged(state));
    }

    fn server_error(&mut self, error: &PgServerError) {
        let _ = self(ConnectionEvent::ServerError(error));
    }
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)] // Auth is much larger
enum ServerStateImpl {
    /// Initial state, enum indicates whether SSL is required (or None if enabled)
    Initial(Option<ConnectionSslRequirement>),
    /// SSL connection is being established
    SslConnecting,
    /// Waiting for AuthInfo
    AuthInfo(String),
    /// Authentication process has begun
    Authenticating(ServerAuth),
    /// Synchronizing connection parameters
    Synchronizing,
    /// Connection is ready for queries
    Ready,
    /// An error has occurred
    Error,
}

#[derive(derive_more::Debug)]
pub struct ServerState {
    state: ServerStateImpl,
    #[debug(skip)]
    initial_buffer: StructBuffer<meta::InitialMessage>,
    #[debug(skip)]
    buffer: StructBuffer<meta::Message>,
}

fn send_error(
    update: &mut impl ConnectionStateUpdate,
    code: PgError,
    message: &str,
) -> std::io::Result<()> {
    let error = PgServerError::new(code, message, Default::default());
    update.server_error(&error);
    update.send(BackendBuilder::ErrorResponse(builder::ErrorResponse {
        fields: &[
            builder::ErrorField {
                etype: PgServerErrorField::Severity as _,
                value: "ERROR",
            },
            builder::ErrorField {
                etype: PgServerErrorField::SeverityNonLocalized as _,
                value: "ERROR",
            },
            builder::ErrorField {
                etype: PgServerErrorField::Code as _,
                value: std::str::from_utf8(&code.to_code()).unwrap(),
            },
            builder::ErrorField {
                etype: PgServerErrorField::Message as _,
                value: message,
            },
        ],
    }))
}

#[derive(Debug, derive_more::Display, thiserror::Error)]
enum ServerError {
    IO(#[from] std::io::Error),
    Protocol(#[from] PgError),
    Utf8Error(#[from] Utf8Error),
}

impl From<ServerAuthError> for ServerError {
    fn from(value: ServerAuthError) -> Self {
        match value {
            ServerAuthError::InvalidAuthorizationSpecification => {
                ServerError::Protocol(PgError::InvalidAuthorizationSpecification(
                    PgErrorInvalidAuthorizationSpecification::InvalidAuthorizationSpecification,
                ))
            }
            ServerAuthError::InvalidPassword => {
                ServerError::Protocol(PgError::InvalidAuthorizationSpecification(
                    PgErrorInvalidAuthorizationSpecification::InvalidPassword,
                ))
            }
            ServerAuthError::InvalidSaslMessage(_) => ServerError::Protocol(
                PgError::ConnectionException(PgErrorConnectionException::ProtocolViolation),
            ),
            ServerAuthError::UnsupportedAuthType => ServerError::Protocol(
                PgError::FeatureNotSupported(PgErrorFeatureNotSupported::FeatureNotSupported),
            ),
            ServerAuthError::InvalidMessageType => ServerError::Protocol(
                PgError::ConnectionException(PgErrorConnectionException::ProtocolViolation),
            ),
        }
    }
}

const PROTOCOL_ERROR: ServerError = ServerError::Protocol(PgError::ConnectionException(
    PgErrorConnectionException::ProtocolViolation,
));
const AUTH_ERROR: ServerError = ServerError::Protocol(PgError::InvalidAuthorizationSpecification(
    PgErrorInvalidAuthorizationSpecification::InvalidAuthorizationSpecification,
));
const PROTOCOL_VERSION_ERROR: ServerError = ServerError::Protocol(PgError::FeatureNotSupported(
    PgErrorFeatureNotSupported::FeatureNotSupported,
));

impl ServerState {
    pub fn new(ssl_requirement: ConnectionSslRequirement) -> Self {
        Self {
            state: ServerStateImpl::Initial(Some(ssl_requirement)),
            initial_buffer: Default::default(),
            buffer: Default::default(),
        }
    }

    pub fn is_ready(&self) -> bool {
        matches!(self.state, ServerStateImpl::Ready)
    }

    pub fn is_error(&self) -> bool {
        matches!(self.state, ServerStateImpl::Error)
    }

    pub fn is_done(&self) -> bool {
        self.is_ready() || self.is_error()
    }

    pub fn drive(
        &mut self,
        drive: ConnectionDrive,
        update: &mut impl ConnectionStateUpdate,
    ) -> Result<(), PGConnectionError> {
        trace!("SERVER DRIVE: {:?} {:?}", self.state, drive);
        let res = match drive {
            ConnectionDrive::RawMessage(raw) => match self.state {
                ServerStateImpl::Initial(..) => self.initial_buffer.push_fallible(raw, |message| {
                    self.state
                        .drive_inner(ConnectionDrive::Initial(message), update)
                }),
                ServerStateImpl::Authenticating(..) => self.buffer.push_fallible(raw, |message| {
                    self.state
                        .drive_inner(ConnectionDrive::Message(message), update)
                }),
                _ => {
                    error!("Unexpected drive in state {:?}", self.state);
                    Err(PROTOCOL_ERROR)
                }
            },
            drive => self.state.drive_inner(drive, update),
        };

        match res {
            Ok(_) => Ok(()),
            Err(ServerError::IO(e)) => Err(e.into()),
            Err(ServerError::Utf8Error(e)) => Err(e.into()),
            Err(ServerError::Protocol(code)) => {
                self.state = ServerStateImpl::Error;
                send_error(update, code, "Connection error")?;
                Err(PgServerError::new(code, "Connection error", Default::default()).into())
            }
        }
    }
}

impl ServerStateImpl {
    fn drive_inner(
        &mut self,
        drive: ConnectionDrive,
        update: &mut impl ConnectionStateUpdate,
    ) -> Result<(), ServerError> {
        use ServerStateImpl::*;

        match (&mut *self, drive) {
            (Initial(ssl), ConnectionDrive::Initial(initial_message)) => {
                match_message!(initial_message, InitialMessage {
                    (StartupMessage as startup) => {
                        let mut user = String::new();
                        let mut database = String::new();
                        for param in startup.params() {
                            if param.name() == "user" {
                                user = param.value().to_owned()?;
                            } else if param.name() == "database" {
                                database = param.value().to_owned()?;
                            }
                            trace!("param: {:?}={:?}", param.name(), param.value());
                            update.parameter(param.name().to_str()?, param.value().to_str()?);
                        }
                        if user.is_empty() {
                            return Err(AUTH_ERROR);
                        }
                        if database.is_empty() {
                            database = user.clone();
                        }
                        *self = AuthInfo(user.clone());
                        update.auth(user, database)?;
                    },
                    (SSLRequest) => {
                        let Some(ssl) = *ssl else {
                            return Err(PROTOCOL_ERROR);
                        };
                        if ssl == ConnectionSslRequirement::Disable {
                            update.send_ssl(builder::SSLResponse { code: b'N' })?;
                            update.upgrade()?;
                        } else {
                            update.send_ssl(builder::SSLResponse { code: b'S' })?;
                            *self = SslConnecting;
                        }
                    },
                    unknown => {
                        log_unknown_initial_message(unknown, "Initial")?;
                    }
                });
            }
            (SslConnecting, ConnectionDrive::SslReady) => {
                *self = Initial(None);
            }
            (SslConnecting, _) => {
                return Err(PROTOCOL_ERROR);
            }
            (AuthInfo(username), ConnectionDrive::AuthInfo(auth_type, credential_data)) => {
                let mut auth = ServerAuth::new(username.clone(), auth_type, credential_data);
                match auth.drive(ServerAuthDrive::Initial) {
                    ServerAuthResponse::Initial(AuthType::Plain, _) => {
                        update.send(BackendBuilder::AuthenticationCleartextPassword(
                            Default::default(),
                        ))?;
                    }
                    ServerAuthResponse::Initial(AuthType::Md5, salt) => {
                        update.send(BackendBuilder::AuthenticationMD5Password(
                            builder::AuthenticationMD5Password {
                                salt: salt.try_into().unwrap(),
                            },
                        ))?;
                    }
                    ServerAuthResponse::Initial(AuthType::ScramSha256, _) => {
                        update.send(BackendBuilder::AuthenticationSASL(
                            builder::AuthenticationSASL {
                                mechanisms: &["SCRAM-SHA-256"],
                            },
                        ))?;
                    }
                    ServerAuthResponse::Complete(..) => {
                        update.send(BackendBuilder::AuthenticationOk(Default::default()))?;
                        *self = Synchronizing;
                        update.params()?;
                        return Ok(());
                    }
                    ServerAuthResponse::Error(e) => {
                        error!("Authentication error in initial state: {e:?}");
                        return Err(e.into());
                    }
                    response => {
                        error!("Unexpected response: {response:?}");
                        return Err(PROTOCOL_ERROR);
                    }
                }
                *self = Authenticating(auth);
            }
            (Authenticating(auth), ConnectionDrive::Message(message)) => {
                trace!("auth = {auth:?}, initial = {}", auth.is_initial_message());
                match_message!(message, Message {
                    (PasswordMessage as password) if matches!(auth.auth_type(), AuthType::Plain | AuthType::Md5) => {
                        match auth.drive(ServerAuthDrive::Message(auth.auth_type(), password.password().to_bytes())) {
                            ServerAuthResponse::Complete(..) => {
                                update.send(BackendBuilder::AuthenticationOk(Default::default()))?;
                                *self = Synchronizing;
                                update.params()?;
                            }
                            ServerAuthResponse::Error(e) => {
                                error!("Authentication error for password message: {e:?}");
                                return Err(e.into())
                            },
                            response => {
                                error!("Unexpected response for password message: {response:?}");
                                return Err(PROTOCOL_ERROR);
                            }
                        }
                    },
                    (SASLInitialResponse as sasl) if auth.is_initial_message() => {
                        if sasl.mechanism() != "SCRAM-SHA-256" {
                            error!("Unexpected mechanism: {:?}", sasl.mechanism());
                            return Err(PROTOCOL_ERROR);
                        }
                        match auth.drive(ServerAuthDrive::Message(AuthType::ScramSha256, sasl.response().as_ref())) {
                            ServerAuthResponse::Continue(final_message) => {
                                update.send(BackendBuilder::AuthenticationSASLContinue(builder::AuthenticationSASLContinue {
                                    data: &final_message,
                                }))?;
                            }
                            ServerAuthResponse::Error(e) => {
                                error!("Authentication error for SASL initial response: {e:?}");
                                return Err(e.into())
                            },
                            response => {
                                error!("Unexpected response for SASL initial response: {response:?}");
                                return Err(PROTOCOL_ERROR);
                            }
                        }
                    },
                    (SASLResponse as sasl) if !auth.is_initial_message() => {
                        match auth.drive(ServerAuthDrive::Message(AuthType::ScramSha256, sasl.response().as_ref())) {
                            ServerAuthResponse::Complete(data) => {
                                update.send(BackendBuilder::AuthenticationSASLFinal(builder::AuthenticationSASLFinal {
                                    data: &data,
                                }))?;
                                update.send(BackendBuilder::AuthenticationOk(Default::default()))?;
                                *self = Synchronizing;
                                update.params()?;
                            }
                            ServerAuthResponse::Error(e) => {
                                error!("Authentication error for SASL response: {e:?}");
                                return Err(e.into())
                            },
                            response => {
                                error!("Unexpected response for SASL response: {response:?}");
                                return Err(PROTOCOL_ERROR);
                            }
                        }
                    },
                    unknown => {
                        log_unknown_message(unknown, "Authenticating")?;
                    }
                });
            }
            (Synchronizing, ConnectionDrive::Parameter(name, value)) => {
                update.send(BackendBuilder::ParameterStatus(builder::ParameterStatus {
                    name: &name,
                    value: &value,
                }))?;
            }
            (Synchronizing, ConnectionDrive::Ready(pid, key)) => {
                update.send(BackendBuilder::BackendKeyData(builder::BackendKeyData {
                    pid,
                    key,
                }))?;
                update.send(BackendBuilder::ReadyForQuery(builder::ReadyForQuery {
                    status: b'I',
                }))?;
                *self = Ready;
            }
            (_, ConnectionDrive::Fail(error, _)) => {
                return Err(ServerError::Protocol(error));
            }
            _ => {
                error!("Unexpected drive in state {:?}", self);
                return Err(PROTOCOL_ERROR);
            }
        }

        Ok(())
    }
}

fn log_unknown_initial_message(
    message: Result<InitialMessage, ParseError>,
    state: &str,
) -> Result<(), ServerError> {
    match message {
        Ok(message) => {
            warn!(
                "Unexpected message {:?} (length {}) received in {} state",
                message.protocol_version(),
                message.mlen(),
                state
            );
            Err(PROTOCOL_VERSION_ERROR)
        }
        Err(e) => {
            error!("Corrupted message received in {} state {:?}", state, e);
            Err(PROTOCOL_ERROR)
        }
    }
}

fn log_unknown_message(
    message: Result<Message, ParseError>,
    state: &str,
) -> Result<(), ServerError> {
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
            error!("Corrupted message received in {} state {:?}", state, e);
            Err(PROTOCOL_ERROR)
        }
    }
}
