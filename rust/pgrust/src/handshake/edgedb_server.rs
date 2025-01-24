use crate::{
    connection::PGConnectionError,
    errors::edgedb::EdbError,
    protocol::edgedb::{data::*, *},
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
    Authenticating,
    Synchronizing,
    Ready,
}

#[derive(Debug)]
pub enum ConnectionDrive<'a> {
    RawMessage(&'a [u8]),
    Message(Result<Message<'a>, ParseError>),
    AuthInfo(AuthType, CredentialData),
    Parameter(String, String),
    Ready([u8; 32]),
    Fail(EdbError, &'a str),
}

pub trait ConnectionStateSend {
    fn send(&mut self, message: EdgeDBBackendBuilder) -> Result<(), std::io::Error>;
    fn auth(
        &mut self,
        user: String,
        database: String,
        branch: String,
    ) -> Result<(), std::io::Error>;
    fn params(&mut self) -> Result<(), std::io::Error>;
}

#[allow(unused)]
pub trait ConnectionStateUpdate: ConnectionStateSend {
    fn parameter(&mut self, name: &str, value: &str) {}
    fn state_changed(&mut self, state: ConnectionStateType) {}
    fn server_error(&mut self, error: &EdbError) {}
}

#[derive(Debug)]
pub enum ConnectionEvent<'a> {
    Send(EdgeDBBackendBuilder<'a>),
    Auth(String, String, String),
    Params,
    Parameter(&'a str, &'a str),
    StateChanged(ConnectionStateType),
    ServerError(EdbError),
}

impl<F> ConnectionStateSend for F
where
    F: FnMut(ConnectionEvent) -> Result<(), std::io::Error>,
{
    fn send(&mut self, message: EdgeDBBackendBuilder) -> Result<(), std::io::Error> {
        self(ConnectionEvent::Send(message))
    }

    fn auth(
        &mut self,
        user: String,
        database: String,
        branch: String,
    ) -> Result<(), std::io::Error> {
        self(ConnectionEvent::Auth(user, database, branch))
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

    fn server_error(&mut self, error: &EdbError) {
        let _ = self(ConnectionEvent::ServerError(*error));
    }
}

#[derive(Debug, derive_more::Display, thiserror::Error)]
enum ServerError {
    IO(#[from] std::io::Error),
    Protocol(#[from] EdbError),
    Utf8Error(#[from] Utf8Error),
}

impl From<ServerAuthError> for ServerError {
    fn from(value: ServerAuthError) -> Self {
        match value {
            ServerAuthError::InvalidAuthorizationSpecification => {
                ServerError::Protocol(EdbError::AuthenticationError)
            }
            ServerAuthError::InvalidPassword => {
                ServerError::Protocol(EdbError::AuthenticationError)
            }
            ServerAuthError::InvalidSaslMessage(_) => {
                ServerError::Protocol(EdbError::ProtocolError)
            }
            ServerAuthError::UnsupportedAuthType => {
                ServerError::Protocol(EdbError::UnsupportedFeatureError)
            }
            ServerAuthError::InvalidMessageType => ServerError::Protocol(EdbError::ProtocolError),
        }
    }
}

const PROTOCOL_ERROR: ServerError = ServerError::Protocol(EdbError::ProtocolError);
const AUTH_ERROR: ServerError = ServerError::Protocol(EdbError::AuthenticationError);
const PROTOCOL_VERSION_ERROR: ServerError =
    ServerError::Protocol(EdbError::UnsupportedProtocolVersionError);

#[derive(Debug, Default)]
#[allow(clippy::large_enum_variant)] // Auth is much larger
enum ServerStateImpl {
    #[default]
    Initial,
    AuthInfo(String),
    Authenticating(ServerAuth),
    Synchronizing,
    Ready,
    Error,
}

pub struct ServerState {
    state: ServerStateImpl,
    buffer: StructBuffer<meta::Message>,
}

impl ServerState {
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
            ConnectionDrive::RawMessage(raw) => self.buffer.push_fallible(raw, |message| {
                trace!("Parsed message: {message:?}");
                self.state
                    .drive_inner(ConnectionDrive::Message(message), update)
            }),
            drive => self.state.drive_inner(drive, update),
        };

        match res {
            Ok(_) => Ok(()),
            Err(ServerError::IO(e)) => Err(e.into()),
            Err(ServerError::Utf8Error(e)) => Err(e.into()),
            Err(ServerError::Protocol(code)) => {
                self.state = ServerStateImpl::Error;
                send_error(update, code, "Connection error")?;
                Err(code.into())
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
            (Initial, ConnectionDrive::Message(message)) => {
                match_message!(message, Message {
                    (ClientHandshake as handshake) => {
                        trace!("ClientHandshake: {handshake:?}");

                        // The handshake should generate an event rather than hardcoding the min/max protocol versions.

                        // No extensions are supported
                        if !handshake.extensions().is_empty() {
                            update.send(EdgeDBBackendBuilder::ServerHandshake(builder::ServerHandshake { major_ver: 2, minor_ver: 0, extensions: &[] }))?;
                            return Ok(());
                        }

                        // We support 1.x and 2.0
                        let major_ver = handshake.major_ver();
                        let minor_ver = handshake.minor_ver();
                        match (major_ver, minor_ver) {
                            (..=0, _) => {
                                update.send(EdgeDBBackendBuilder::ServerHandshake(builder::ServerHandshake { major_ver: 1, minor_ver: 0, extensions: &[] }))?;
                                return Ok(());
                            }
                            (1, 1..) => {
                                // 1.(1+) never existed
                                return Err(PROTOCOL_VERSION_ERROR);
                            }
                            (2, 1..) | (3.., _) => {
                                update.send(EdgeDBBackendBuilder::ServerHandshake(builder::ServerHandshake { major_ver: 2, minor_ver: 0, extensions: &[] }))?;
                                return Ok(());
                            }
                            _ => {}
                        }

                        let mut user = String::new();
                        let mut database = String::new();
                        let mut branch = String::new();
                        for param in handshake.params() {
                            match param.name().to_str()? {
                                "user" => user = param.value().to_owned()?,
                                "database" => database = param.value().to_owned()?,
                                "branch" => branch = param.value().to_owned()?,
                                _ => {}
                            }
                            update.parameter(param.name().to_str()?, param.value().to_str()?);
                        }
                        if user.is_empty() {
                            return Err(AUTH_ERROR);
                        }
                        if database.is_empty() {
                            database = user.clone();
                        }
                        *self = AuthInfo(user.clone());
                        update.auth(user, database, branch)?;
                    },
                    unknown => {
                        log_unknown_message(unknown, "Initial")?;
                    }
                });
            }
            (AuthInfo(username), ConnectionDrive::AuthInfo(auth_type, credential_data)) => {
                let mut auth = ServerAuth::new(username.clone(), auth_type, credential_data);
                match auth.drive(ServerAuthDrive::Initial) {
                    ServerAuthResponse::Initial(AuthType::ScramSha256, _) => {
                        update.send(EdgeDBBackendBuilder::AuthenticationRequiredSASLMessage(
                            builder::AuthenticationRequiredSASLMessage {
                                methods: &["SCRAM-SHA-256"],
                            },
                        ))?;
                    }
                    ServerAuthResponse::Complete(..) => {
                        update.send(EdgeDBBackendBuilder::AuthenticationOk(Default::default()))?;
                        *self = Synchronizing;
                        update.params()?;
                        return Ok(());
                    }
                    ServerAuthResponse::Error(e) => return Err(e.into()),
                    _ => return Err(PROTOCOL_ERROR),
                }
                *self = Authenticating(auth);
            }
            (Authenticating(auth), ConnectionDrive::Message(message)) => {
                match_message!(message, Message {
                    (AuthenticationSASLInitialResponse as sasl) if auth.is_initial_message() => {
                        match auth.drive(ServerAuthDrive::Message(AuthType::ScramSha256, sasl.sasl_data().as_ref())) {
                            ServerAuthResponse::Continue(final_message) => {
                                update.send(EdgeDBBackendBuilder::AuthenticationSASLContinue(builder::AuthenticationSASLContinue {
                                    sasl_data: &final_message,
                                }))?;
                            }
                            ServerAuthResponse::Error(e) => return Err(e.into()),
                            _ => return Err(PROTOCOL_ERROR),
                        }
                    },
                    (AuthenticationSASLResponse as sasl) if !auth.is_initial_message() => {
                        match auth.drive(ServerAuthDrive::Message(AuthType::ScramSha256, sasl.sasl_data().as_ref())) {
                            ServerAuthResponse::Complete(data) => {
                                update.send(EdgeDBBackendBuilder::AuthenticationSASLFinal(builder::AuthenticationSASLFinal {
                                    sasl_data: &data,
                                }))?;
                                update.send(EdgeDBBackendBuilder::AuthenticationOk(Default::default()))?;
                                *self = Synchronizing;
                                update.params()?;
                            }
                            ServerAuthResponse::Error(e) => return Err(e.into()),
                            _ => return Err(PROTOCOL_ERROR),
                        }
                    },
                    unknown => {
                        log_unknown_message(unknown, "Authenticating")?;
                    }
                });
            }
            (Synchronizing, ConnectionDrive::Parameter(name, value)) => {
                update.send(EdgeDBBackendBuilder::ParameterStatus(
                    builder::ParameterStatus {
                        name: name.as_bytes(),
                        value: value.as_bytes(),
                    },
                ))?;
            }
            (Synchronizing, ConnectionDrive::Ready(key_data)) => {
                update.send(EdgeDBBackendBuilder::ServerKeyData(
                    builder::ServerKeyData { data: key_data },
                ))?;
                update.send(EdgeDBBackendBuilder::ReadyForCommand(
                    builder::ReadyForCommand {
                        annotations: &[],
                        transaction_state: 0x49,
                    },
                ))?;
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

fn send_error(
    update: &mut impl ConnectionStateUpdate,
    code: EdbError,
    message: &str,
) -> std::io::Result<()> {
    update.server_error(&code);
    update.send(EdgeDBBackendBuilder::ErrorResponse(
        builder::ErrorResponse {
            severity: ErrorSeverity::Error as _,
            error_code: code as i32,
            message,
            attributes: &[],
        },
    ))
}

#[allow(unused)]
enum ErrorSeverity {
    Error = 0x78,
    Fatal = 0xc8,
    Panic = 0xff,
}
