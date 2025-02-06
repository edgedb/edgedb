use crate::{
    md5::StoredHash,
    scram::{SCRAMError, ServerTransaction, StoredKey},
    AuthType, CredentialData,
};
use tracing::error;

#[derive(Debug)]
pub enum ServerAuthResponse {
    Initial(AuthType, Vec<u8>),
    Continue(Vec<u8>),
    Complete(Vec<u8>),
    Error(ServerAuthError),
}

#[derive(Debug, thiserror::Error)]
pub enum ServerAuthError {
    #[error("Invalid authorization specification")]
    InvalidAuthorizationSpecification,
    #[error("Invalid password")]
    InvalidPassword,
    #[error("Invalid SASL message ({0})")]
    InvalidSaslMessage(SCRAMError),
    #[error("Unsupported authentication type")]
    UnsupportedAuthType,
    #[error("Invalid message type")]
    InvalidMessageType,
}

#[derive(Debug)]
enum ServerAuthState {
    Initial,
    Password(CredentialData),
    MD5([u8; 4], CredentialData),
    Sasl(ServerTransaction, StoredKey),
}

#[derive(Debug)]
pub enum ServerAuthDrive<'a> {
    Initial,
    Message(AuthType, &'a [u8]),
}

#[derive(Debug)]
pub struct ServerAuth {
    state: ServerAuthState,
    username: String,
    auth_type: AuthType,
    credential_data: CredentialData,
}

impl ServerAuth {
    pub fn new(username: String, auth_type: AuthType, credential_data: CredentialData) -> Self {
        Self {
            state: ServerAuthState::Initial,
            username,
            auth_type,
            credential_data,
        }
    }

    pub fn is_initial_message(&self) -> bool {
        match &self.state {
            ServerAuthState::Initial => false,
            ServerAuthState::Sasl(tx, _) => tx.initial(),
            _ => true,
        }
    }

    pub fn auth_type(&self) -> AuthType {
        self.auth_type
    }

    pub fn drive(&mut self, drive: ServerAuthDrive) -> ServerAuthResponse {
        match (&mut self.state, drive) {
            (ServerAuthState::Initial, ServerAuthDrive::Initial) => self.handle_initial(),
            (ServerAuthState::Password(data), ServerAuthDrive::Message(AuthType::Plain, input)) => {
                let client_password = input;
                let success = match data {
                    CredentialData::Deny => false,
                    CredentialData::Trust => true,
                    CredentialData::Plain(password) => client_password == password.as_bytes(),
                    CredentialData::Md5(md5) => {
                        let md5_1 = StoredHash::generate(client_password, &self.username);
                        md5_1 == *md5
                    }
                    CredentialData::Scram(scram) => {
                        let key =
                            StoredKey::generate(client_password, &scram.salt, scram.iterations);
                        key.stored_key == scram.stored_key
                    }
                };
                if success {
                    ServerAuthResponse::Complete(Vec::new())
                } else {
                    ServerAuthResponse::Error(ServerAuthError::InvalidPassword)
                }
            }
            (ServerAuthState::MD5(salt, data), ServerAuthDrive::Message(AuthType::Md5, input)) => {
                let success = match data {
                    CredentialData::Deny => false,
                    CredentialData::Trust => true,
                    CredentialData::Plain(password) => {
                        let server_md5 = StoredHash::generate(password.as_bytes(), &self.username);
                        server_md5.matches(input, *salt)
                    }
                    CredentialData::Md5(server_md5) => server_md5.matches(input, *salt),
                    CredentialData::Scram(_) => {
                        // Unreachable
                        false
                    }
                };

                if success {
                    ServerAuthResponse::Complete(Vec::new())
                } else {
                    ServerAuthResponse::Error(ServerAuthError::InvalidPassword)
                }
            }
            (
                ServerAuthState::Sasl(tx, data),
                ServerAuthDrive::Message(AuthType::ScramSha256, input),
            ) => {
                let initial = tx.initial();
                match tx.process_message(input, data) {
                    Ok(final_message) => {
                        if initial {
                            ServerAuthResponse::Continue(final_message)
                        } else {
                            ServerAuthResponse::Complete(final_message)
                        }
                    }
                    Err(e) => ServerAuthResponse::Error(ServerAuthError::InvalidSaslMessage(e)),
                }
            }
            (_, drive) => {
                error!("Received invalid drive {drive:?} in state {:?}", self.state);
                ServerAuthResponse::Error(ServerAuthError::InvalidMessageType)
            }
        }
    }

    fn handle_initial(&mut self) -> ServerAuthResponse {
        match self.auth_type {
            AuthType::Deny => {
                ServerAuthResponse::Error(ServerAuthError::InvalidAuthorizationSpecification)
            }
            AuthType::Trust => ServerAuthResponse::Complete(Vec::new()),
            AuthType::Plain => {
                self.state = ServerAuthState::Password(self.credential_data.clone());
                ServerAuthResponse::Initial(AuthType::Plain, Vec::new())
            }
            AuthType::Md5 => {
                let salt: [u8; 4] = rand::random();
                match self.credential_data {
                    CredentialData::Scram(..) => {
                        ServerAuthResponse::Error(ServerAuthError::UnsupportedAuthType)
                    }
                    _ => {
                        self.state = ServerAuthState::MD5(salt, self.credential_data.clone());
                        ServerAuthResponse::Initial(AuthType::Md5, salt.into())
                    }
                }
            }
            AuthType::ScramSha256 => {
                let salt: [u8; 32] = rand::random();
                let scram = match &self.credential_data {
                    CredentialData::Scram(scram) => scram.clone(),
                    CredentialData::Plain(password) => {
                        StoredKey::generate(password.as_bytes(), &salt, 4096)
                    }
                    CredentialData::Deny => StoredKey::generate(b"", &salt, 4096),
                    _ => {
                        return ServerAuthResponse::Error(ServerAuthError::UnsupportedAuthType);
                    }
                };
                let tx = ServerTransaction::default();
                self.state = ServerAuthState::Sasl(tx, scram);
                ServerAuthResponse::Initial(AuthType::ScramSha256, Vec::new())
            }
        }
    }
}
