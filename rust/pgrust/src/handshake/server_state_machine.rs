use super::ConnectionSslRequirement;
use crate::{
    auth::{ServerTransaction, StoredHash, StoredKey},
    connection::ConnectionError,
    errors::{
        PgError, PgErrorConnectionException, PgErrorFeatureNotSupported,
        PgErrorInvalidAuthorizationSpecification, PgServerError, PgServerErrorField,
    },
    handshake::AuthType,
    protocol::{
        builder, definition::BackendBuilder, match_message, InitialMessage, Message, ParseError,
        PasswordMessage, SASLInitialResponse, SASLResponse, SSLRequest, StartupMessage,
    },
};
use rand::Rng;
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

#[derive(Debug, Clone)]
pub struct ServerCredentials {
    pub auth_type: AuthType,
    pub credential_data: CredentialData,
}

#[derive(Debug, Clone)]
pub enum CredentialData {
    /// A credential that always succeeds, regardless of input password. Due to
    /// the design of SCRAM-SHA-256, this cannot be used with that auth type.
    Trust,
    /// A credential that always fails, regardless of the input password.
    Deny,
    /// A plain-text password.
    Plain(String),
    /// A stored MD5 hash + salt.
    Md5(StoredHash),
    /// A stored SCRAM-SHA-256 key.
    Scram(StoredKey),
}

impl CredentialData {
    pub fn new(ty: AuthType, username: String, password: String) -> Self {
        match ty {
            AuthType::Deny => Self::Deny,
            AuthType::Trust => Self::Trust,
            AuthType::Plain => Self::Plain(password),
            AuthType::Md5 => Self::Md5(StoredHash::generate(password.as_bytes(), &username)),
            AuthType::ScramSha256 => {
                let salt: [u8; 32] = rand::thread_rng().gen();
                Self::Scram(StoredKey::generate(password.as_bytes(), &salt, 4096))
            }
        }
    }
}

/// Internal flag used to store a predetermined result: ie, a connection that
/// must succeed for fail regardless of the correctness of the credential.
///
/// Used for testing purposes, and to disguise timing in cases where a user may
/// not exist.
#[derive(Debug, Clone, Eq, PartialEq)]
enum PredeterminedResult {
    Trust,
    Deny,
}

#[derive(Debug)]
struct ServerEnvironmentImpl {
    ssl_requirement: ConnectionSslRequirement,
    pid: i32,
    key: i32,
}

#[derive(Debug)]
pub enum ConnectionDrive<'a> {
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
    AuthInfo(String, AuthType, CredentialData),
    /// Once authorized, the server may sync any number of parameters until ready.
    Parameter(String, String),
    /// Ready, handshake complete.
    Ready,
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
    fn auth(&mut self, user: String, data: String) -> Result<(), std::io::Error>;
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
enum ServerStateImpl {
    /// Initial state, boolean indicates whether SSL is required
    Initial(bool),
    /// SSL connection is being established
    SslConnecting,
    /// Authentication process has begun
    Authenticating,
    /// Password-based authentication in progress
    AuthenticatingPassword(String, CredentialData),
    /// MD5 authentication in progress
    AuthenticatingMD5(Option<PredeterminedResult>, [u8; 4], StoredHash),
    /// SASL authentication in progress
    AuthenticatingSASL(ServerTransaction, Option<PredeterminedResult>, StoredKey),
    /// Synchronizing connection parameters
    Synchronizing,
    /// Connection is ready for queries
    Ready,
    /// An error has occurred
    Error,
}

pub struct ServerState {
    state: ServerStateImpl,
    environment: ServerEnvironmentImpl,
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

const PROTOCOL_ERROR: ServerError = ServerError::Protocol(PgError::ConnectionException(
    PgErrorConnectionException::ProtocolViolation,
));
const AUTH_ERROR: ServerError = ServerError::Protocol(PgError::InvalidAuthorizationSpecification(
    PgErrorInvalidAuthorizationSpecification::InvalidAuthorizationSpecification,
));
const PASSWORD_ERROR: ServerError =
    ServerError::Protocol(PgError::InvalidAuthorizationSpecification(
        PgErrorInvalidAuthorizationSpecification::InvalidPassword,
    ));
const PROTOCOL_VERSION_ERROR: ServerError = ServerError::Protocol(PgError::FeatureNotSupported(
    PgErrorFeatureNotSupported::FeatureNotSupported,
));

impl ServerState {
    pub fn new(ssl_requirement: ConnectionSslRequirement, pid: i32, key: i32) -> Self {
        Self {
            state: ServerStateImpl::Initial(false),
            environment: ServerEnvironmentImpl {
                ssl_requirement,
                pid,
                key,
            },
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
    ) -> Result<(), ConnectionError> {
        match self.drive_inner(drive, update) {
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

    fn drive_inner(
        &mut self,
        drive: ConnectionDrive,
        update: &mut impl ConnectionStateUpdate,
    ) -> Result<(), ServerError> {
        use ServerStateImpl::*;

        match (&mut self.state, drive) {
            (Initial(ssl_active), ConnectionDrive::Initial(initial_message)) => {
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
                            // Postgres returns invalid_authorization_specification if no user is specified
                            return Err(AUTH_ERROR);
                        }
                        if database.is_empty() {
                            // Postgres uses the username as the database if not specified
                            database = user.clone();
                        }
                        self.state = Authenticating;
                        update.auth(user, database)?;
                    },
                    (SSLRequest) => {
                        if *ssl_active {
                            return Err(PROTOCOL_ERROR);
                        }
                        if self.environment.ssl_requirement == ConnectionSslRequirement::Disable {
                            update.send_ssl(builder::SSLResponse { code: b'N' })?;
                            update.upgrade()?;
                        } else {
                            update.send_ssl(builder::SSLResponse { code: b'S' })?;
                            self.state = SslConnecting;
                        }
                    },
                    unknown => {
                        log_unknown_initial_message(unknown, "Initial")?;
                    }
                });
            }
            (SslConnecting, ConnectionDrive::SslReady) => {
                self.state = Initial(true);
            }
            (SslConnecting, _) => {
                return Err(PROTOCOL_ERROR);
            }
            (Authenticating, ConnectionDrive::AuthInfo(username, auth_type, credential_data)) => {
                match auth_type {
                    AuthType::Deny => {
                        return Err(AUTH_ERROR);
                    }
                    AuthType::Trust => {
                        update.send(BackendBuilder::AuthenticationOk(Default::default()))?;
                        self.state = Synchronizing;
                        update.params()?;
                    }
                    AuthType::Plain => {
                        update.send(BackendBuilder::AuthenticationCleartextPassword(
                            Default::default(),
                        ))?;
                        self.state = AuthenticatingPassword(username, credential_data);
                    }
                    AuthType::Md5 => {
                        let salt = rand::random();
                        let (result, hash) = match credential_data {
                            CredentialData::Deny => {
                                let md5 = StoredHash::generate(b"", &username);
                                (Some(PredeterminedResult::Deny), md5)
                            }
                            CredentialData::Trust => {
                                let md5 = StoredHash::generate(b"", &username);
                                (Some(PredeterminedResult::Trust), md5)
                            }
                            CredentialData::Md5(md5) => (None, md5),
                            CredentialData::Plain(password) => {
                                let md5 = StoredHash::generate(password.as_bytes(), &username);
                                (None, md5)
                            }
                            CredentialData::Scram(..) => {
                                return Err(AUTH_ERROR);
                            }
                        };
                        self.state = AuthenticatingMD5(result, salt, hash);
                        update.send(BackendBuilder::AuthenticationMD5Password(
                            builder::AuthenticationMD5Password { salt },
                        ))?;
                    }
                    AuthType::ScramSha256 => {
                        let salt: [u8; 32] = rand::random();
                        match credential_data {
                            CredentialData::Trust | CredentialData::Md5(..) => {
                                return Err(AUTH_ERROR);
                            }
                            CredentialData::Deny => {
                                // Create fake scram data
                                let scram = StoredKey::generate("".as_bytes(), &salt, 4096);
                                self.state = AuthenticatingSASL(
                                    ServerTransaction::default(),
                                    Some(PredeterminedResult::Deny),
                                    scram,
                                );
                            }
                            CredentialData::Plain(password) => {
                                // Upgrade password to SCRAM
                                let scram = StoredKey::generate(password.as_bytes(), &salt, 4096);
                                self.state =
                                    AuthenticatingSASL(ServerTransaction::default(), None, scram);
                            }
                            CredentialData::Scram(scram) => {
                                self.state =
                                    AuthenticatingSASL(ServerTransaction::default(), None, scram);
                            }
                        }
                        update.send(BackendBuilder::AuthenticationSASL(
                            builder::AuthenticationSASL {
                                mechanisms: &["SCRAM-SHA-256"],
                            },
                        ))?;
                    }
                }
            }
            (AuthenticatingPassword(username, data), ConnectionDrive::Message(message)) => {
                match_message!(message, Message {
                    (PasswordMessage as password) => {
                        let client_password = password.password();
                        let success = match data {
                            CredentialData::Deny => {
                                false
                            },
                            CredentialData::Trust => {
                                true
                            },
                            CredentialData::Plain(password) => {
                                let md5_1 = StoredHash::generate(password.as_bytes(), username);
                                let md5_2 = StoredHash::generate(client_password.to_bytes(), username);
                                md5_1 == md5_2
                            }
                            CredentialData::Md5(md5) => {
                                let md5_1 = StoredHash::generate(client_password.to_bytes(), username);
                                md5_1 == *md5
                            },
                            CredentialData::Scram(scram) => {
                                // We can test a password by hashing it with the same salt and iteration count
                                let key = StoredKey::generate(client_password.to_bytes(), &scram.salt, scram.iterations);
                                key.stored_key == scram.stored_key
                            }
                        };
                        if success {
                            update.send(BackendBuilder::AuthenticationOk(Default::default()))?;
                            self.state = Synchronizing;
                            update.params()?;
                        } else {
                            return Err(PASSWORD_ERROR);
                        }
                    },
                    unknown => {
                        log_unknown_message(unknown, "Password")?;
                    }
                });
            }
            (AuthenticatingMD5(results, salt, md5), ConnectionDrive::Message(message)) => {
                match_message!(message, Message {
                    (PasswordMessage as password) => {
                        let password = password.password();
                        let success = match (results, md5) {
                            (Some(PredeterminedResult::Deny), _) => {
                                false
                            },
                            (Some(PredeterminedResult::Trust), _) => {
                                true
                            },
                            (None, md5) => {
                                md5.matches(password.to_bytes(), *salt)
                            },
                        };
                        if success {
                            update.send(BackendBuilder::AuthenticationOk(Default::default()))?;
                            self.state = Synchronizing;
                            update.params()?;
                        } else {
                            return Err(PASSWORD_ERROR);
                        }
                    },
                    unknown => {
                        log_unknown_message(unknown, "MD5")?;
                    }
                });
            }
            (AuthenticatingSASL(tx, result, data), ConnectionDrive::Message(message)) => {
                if tx.initial() {
                    match_message!(message, Message {
                        (SASLInitialResponse as sasl) => {
                            match tx.process_message(sasl.response().as_ref(), data) {
                                Ok(Some(final_message)) => {
                                    update.send(BackendBuilder::AuthenticationSASLContinue(builder::AuthenticationSASLContinue {
                                        data: &final_message,
                                    }))?;
                                },
                                Ok(None) => return Err(PASSWORD_ERROR),
                                Err(e) => {
                                    error!("SCRAM auth failed: {e:?}");
                                    return Err(PASSWORD_ERROR);
                                }
                            }
                        },
                        unknown => {
                            warn!("Protocol error: unknown or malformed message: {unknown:?}");
                            return Err(PROTOCOL_ERROR);
                        }
                    });
                } else {
                    match_message!(message, Message {
                        (SASLResponse as sasl) => {
                            match tx.process_message(sasl.response().as_ref(), data) {
                                Ok(Some(final_message)) => {
                                    if *result == Some(PredeterminedResult::Deny) {
                                        return Err(PASSWORD_ERROR)
                                    }
                                    self.state = Synchronizing;
                                    update.send(BackendBuilder::AuthenticationSASLFinal(builder::AuthenticationSASLFinal {
                                        data: &final_message,
                                    }))?;
                                    update.send(BackendBuilder::AuthenticationOk(Default::default()))?;
                                    update.params()?;
                                },
                                Ok(None) => return Err(PASSWORD_ERROR),
                                Err(e) => {
                                    error!("SCRAM auth failed: {e:?}");
                                    return Err(PASSWORD_ERROR);
                                }
                            }
                        },
                        unknown => {
                            log_unknown_message(unknown, "SASL")?;
                        }
                    });
                };
            }
            (Synchronizing, ConnectionDrive::Parameter(name, value)) => {
                update.send(BackendBuilder::ParameterStatus(builder::ParameterStatus {
                    name: &name,
                    value: &value,
                }))?;
            }
            (Synchronizing, ConnectionDrive::Ready) => {
                update.send(BackendBuilder::BackendKeyData(builder::BackendKeyData {
                    key: self.environment.key,
                    pid: self.environment.pid,
                }))?;
                update.send(BackendBuilder::ReadyForQuery(builder::ReadyForQuery {
                    status: b'I',
                }))?;
                self.state = Ready;
            }
            (_, ConnectionDrive::Fail(error, _)) => {
                return Err(ServerError::Protocol(error));
            }
            _ => {
                error!("Unexpected drive in state {:?}", self.state);
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
