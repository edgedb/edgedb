pub mod handshake;
pub mod md5;
pub mod scram;
pub mod stringprep;
mod stringprep_table;

use rand::Rng;

/// Specifies the type of authentication or indicates the authentication method used for a connection.
#[derive(Debug, Default, Copy, Clone, Eq, PartialEq)]
pub enum AuthType {
    /// Denies a login or indicates that a connection was denied.
    ///
    /// When used with the server, this will cause it to emulate the given
    /// authentication type, but unconditionally return a failure.
    ///
    /// This is used for testing purposes, and to emulate timing when a user
    /// does not exist.
    #[default]
    Deny,
    /// Trusts a login without requiring authentication, or indicates
    /// that a connection required no authentication.
    ///
    /// When used with the server side of the handshake, this will cause it to
    /// emulate the given authentication type, but unconditionally succeed.
    /// Not compatible with SCRAM-SHA-256 as that protocol requires server and client
    /// to cryptographically agree on a password.
    Trust,
    /// Plain text authentication, or indicates that plain text authentication was required.
    Plain,
    /// MD5 password authentication, or indicates that MD5 password authentication was required.
    Md5,
    /// SCRAM-SHA-256 authentication, or indicates that SCRAM-SHA-256 authentication was required.
    ScramSha256,
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
    Md5(md5::StoredHash),
    /// A stored SCRAM-SHA-256 key.
    Scram(scram::StoredKey),
}

impl CredentialData {
    pub fn new(ty: AuthType, username: String, password: String) -> Self {
        match ty {
            AuthType::Deny => Self::Deny,
            AuthType::Trust => Self::Trust,
            AuthType::Plain => Self::Plain(password),
            AuthType::Md5 => Self::Md5(md5::StoredHash::generate(password.as_bytes(), &username)),
            AuthType::ScramSha256 => {
                let salt: [u8; 32] = rand::thread_rng().gen();
                Self::Scram(scram::StoredKey::generate(password.as_bytes(), &salt, 4096))
            }
        }
    }

    pub fn auth_type(&self) -> AuthType {
        match self {
            CredentialData::Trust => AuthType::Trust,
            CredentialData::Deny => AuthType::Deny,
            CredentialData::Plain(..) => AuthType::Plain,
            CredentialData::Md5(..) => AuthType::Md5,
            CredentialData::Scram(..) => AuthType::ScramSha256,
        }
    }
}
