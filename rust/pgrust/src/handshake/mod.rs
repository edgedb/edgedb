#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub enum ConnectionSslRequirement {
    /// SSL is disabled, and it is an error to attempt to use it.
    #[default]
    Disable,
    /// SSL is optional, but we prefer to use it.
    Optional,
    /// SSL is required and it is an error to reject it.
    Required,
}

mod client_state_machine;
pub mod edgedb_server;
mod server_state_machine;

pub mod client {
    pub use super::client_state_machine::*;
}

pub mod server {
    pub use super::server_state_machine::*;
}

#[cfg(test)]
mod tests {
    use super::{
        client,
        server::{self, ConnectionDrive},
        ConnectionSslRequirement,
    };
    use crate::{
        connection::Credentials,
        errors::{PgError, PgServerError},
        protocol::postgres::{data::*, *},
    };
    use gel_auth::{AuthType, CredentialData};
    use rstest::rstest;
    use std::collections::VecDeque;

    #[derive(Debug, Default)]
    struct ConnectionPipe {
        cmsg: VecDeque<(bool, Vec<u8>)>,
        smsg: VecDeque<(bool, Vec<u8>)>,
        sparams: bool,
        sauth_user: Option<String>,
        cauth: Option<AuthType>,
        cerror: Option<PgError>,
        serror: Option<PgError>,
    }

    impl client::ConnectionStateUpdate for ConnectionPipe {
        fn auth(&mut self, auth: AuthType) {
            eprintln!("Client: Auth = {auth:?}");
            self.cauth = Some(auth);
        }
        fn cancellation_key(&mut self, _pid: i32, _key: i32) {}
        fn parameter(&mut self, _name: &str, _value: &str) {}
        fn server_error(&mut self, error: &PgServerError) {
            self.cerror = Some(error.code);
        }
        fn state_changed(&mut self, state: client::ConnectionStateType) {
            eprintln!("Client: Start = {state:?}");
        }
    }

    impl client::ConnectionStateSend for ConnectionPipe {
        fn send(&mut self, message: FrontendBuilder) -> Result<(), std::io::Error> {
            eprintln!("Client -> Server {message:?}");
            self.smsg.push_back((false, message.to_vec()));
            Ok(())
        }
        fn send_initial(&mut self, message: InitialBuilder) -> Result<(), std::io::Error> {
            eprintln!("Client -> Server {message:?}");
            self.smsg.push_back((true, message.to_vec()));
            Ok(())
        }
        fn upgrade(&mut self) -> Result<(), std::io::Error> {
            unimplemented!()
        }
    }

    impl server::ConnectionStateUpdate for ConnectionPipe {
        fn state_changed(&mut self, _state: server::ConnectionStateType) {}
        fn parameter(&mut self, _name: &str, _value: &str) {}
        fn server_error(&mut self, error: &PgServerError) {
            self.serror = Some(error.code);
        }
    }

    impl server::ConnectionStateSend for ConnectionPipe {
        fn auth(&mut self, user: String, database: String) -> Result<(), std::io::Error> {
            eprintln!("Server: auth request {user}/{database}");
            self.sauth_user = Some(user);
            Ok(())
        }
        fn params(&mut self) -> Result<(), std::io::Error> {
            eprintln!("Server: param request");
            self.sparams = true;
            Ok(())
        }
        fn send(&mut self, message: BackendBuilder) -> Result<(), std::io::Error> {
            eprintln!("Server -> Client {message:?}");
            self.cmsg.push_back((false, message.to_vec()));
            Ok(())
        }
        fn send_ssl(&mut self, message: builder::SSLResponse) -> Result<(), std::io::Error> {
            self.cmsg.push_back((true, message.to_vec()));
            Ok(())
        }
        fn upgrade(&mut self) -> Result<(), std::io::Error> {
            unimplemented!()
        }
    }

    /// We test the full matrix of server and client combinations.
    #[rstest]
    #[test_log::test]
    fn test_both(
        #[values(
            AuthType::Deny,
            AuthType::Trust,
            AuthType::Plain,
            AuthType::Md5,
            AuthType::ScramSha256
        )]
        auth_type: AuthType,
        #[values(
            AuthType::Deny,
            AuthType::Trust,
            AuthType::Plain,
            AuthType::Md5,
            AuthType::ScramSha256
        )]
        credential_type: AuthType,
        #[values(true, false)] correct_password: bool,
    ) {
        let mut client = client::ConnectionState::new(
            Credentials {
                username: "user".to_string(),
                password: "password".to_string(),
                database: "database".to_string(),
                ..Default::default()
            },
            ConnectionSslRequirement::Disable,
        );
        let mut server = server::ServerState::new(ConnectionSslRequirement::Disable);

        // We test all variations here, but not all combinations will result in
        // valid auth, even with a correct password.
        let expect_success = match (auth_type, credential_type, correct_password) {
            // If the server is set to trust, we always succeed (as no password is exchanged)
            (AuthType::Trust, ..) => true,
            // If the server is asking for a denial auth type, it'll always fail
            (AuthType::Deny, ..) => false,
            // If the credential is denial, it'll always fail
            (_, AuthType::Deny, _) => false,
            // SCRAM succeeds if the credential is SCRAM or Password (it cannot
            // succeed with a Trust credential because the server also sends a
            // verifier to the client.
            (AuthType::ScramSha256, AuthType::ScramSha256 | AuthType::Plain, correct) => correct,
            (AuthType::ScramSha256, _, _) => false,
            // Other auth types will always succeed if credential type is trust
            (_, AuthType::Trust, _) => true,
            // MD5 succeeds if the credential is not SCRAM
            (AuthType::Md5, AuthType::Md5 | AuthType::Plain, correct) => correct,
            (AuthType::Md5, _, _) => false,
            // Plain text works in all cases
            (AuthType::Plain, _, correct) => correct,
        };

        let mut client_error = false;
        let mut server_error = false;

        let mut pipe = ConnectionPipe::default();
        // This one can never fail
        client
            .drive(client::ConnectionDrive::Initial, &mut pipe)
            .unwrap();
        let mut max_iterations: i32 = 100;
        loop {
            max_iterations -= 1;
            if max_iterations == 0 {
                panic!("Failed to complete");
            }
            if let Some(user) = pipe.sauth_user.take() {
                eprintln!("Sending auth");
                let password = if correct_password {
                    "password".to_owned()
                } else {
                    "incorrect".to_owned()
                };
                let data = CredentialData::new(credential_type, user.clone(), password);
                server_error |= server
                    .drive(ConnectionDrive::AuthInfo(auth_type, data), &mut pipe)
                    .is_err();
            }
            if pipe.sparams {
                server_error |= server
                    .drive(
                        ConnectionDrive::Parameter("param1".to_owned(), "value".to_owned()),
                        &mut pipe,
                    )
                    .is_err();
                server_error |= server
                    .drive(
                        ConnectionDrive::Parameter("param2".to_owned(), "value".to_owned()),
                        &mut pipe,
                    )
                    .is_err();
                server_error |= server
                    .drive(ConnectionDrive::Ready(1234, 4567), &mut pipe)
                    .is_err();
            }
            while let Some((initial, msg)) = pipe.smsg.pop_front() {
                if initial {
                    server_error |= server
                        .drive(
                            ConnectionDrive::Initial(InitialMessage::new(&msg)),
                            &mut pipe,
                        )
                        .is_err();
                } else {
                    server_error |= server
                        .drive(ConnectionDrive::Message(Message::new(&msg)), &mut pipe)
                        .is_err();
                }
            }
            while let Some((ssl, msg)) = pipe.cmsg.pop_front() {
                if ssl {
                    unimplemented!()
                } else {
                    client_error |= client
                        .drive(
                            client::ConnectionDrive::Message(Message::new(&msg)),
                            &mut pipe,
                        )
                        .is_err();
                }
            }
            if client.is_done() && server.is_done() {
                break;
            }
        }

        if expect_success {
            assert!(
                client.is_ready() && server.is_ready(),
                "client={:?} server={:?}",
                client,
                server
            );
        } else {
            assert!(client_error && server_error);
            assert!(pipe.cerror.is_some() && pipe.serror.is_some());
            assert!(client.is_error() && server.is_error())
        }
    }
}
