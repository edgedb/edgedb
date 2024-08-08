use crate::{
    auth::{self, generate_salted_password, ClientEnvironment, ClientTransaction, Sha256Out},
    protocol::{
        builder, match_message, AuthenticationMessage, AuthenticationOk, AuthenticationSASL,
        AuthenticationSASLContinue, AuthenticationSASLFinal, BackendKeyData, ErrorResponse,
        Message, ParameterStatus, ReadyForQuery,
    },
};
use base64::Engine;
use rand::Rng;
use std::future::poll_fn;
use std::{
    cell::RefCell,
    task::{ready, Poll},
};
use tokio::io::ReadBuf;

pub trait Stream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin {}

impl<T> Stream for T where T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin {}

#[derive(Debug, thiserror::Error)]
pub enum PGError {
    #[error("Invalid state")]
    InvalidState,
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("SCRAM: {0}")]
    SCRAM(#[from] auth::SCRAMError),
}

pub struct PGConn<S: Stream> {
    stm: RefCell<S>,
    state: RefCell<ConnState>,
}

#[derive(Clone)]
struct Credentials {
    username: String,
    password: String,
    database: String,
}

enum ConnState {
    Connecting(Credentials),
    SCRAM(ClientTransaction, ClientEnvironmentImpl),
    Connected,
    Ready,
}

struct ClientEnvironmentImpl {
    credentials: Credentials,
}

impl ClientEnvironment for ClientEnvironmentImpl {
    fn generate_nonce(&self) -> String {
        let nonce: [u8; 32] = rand::thread_rng().r#gen();
        base64::engine::general_purpose::STANDARD.encode(nonce)
    }
    fn get_salted_password(&self, username: &str, salt: &[u8], iterations: usize) -> Sha256Out {
        generate_salted_password(&self.credentials.password, salt, iterations)
    }
}

impl<S: Stream> PGConn<S> {
    pub fn new(stm: S, username: String, password: String, database: String) -> Self {
        Self {
            stm: stm.into(),
            state: ConnState::Connecting(Credentials {
                username,
                password,
                database,
            })
            .into(),
        }
    }

    async fn write(&self, mut buf: &[u8]) -> Result<(), PGError> {
        println!("Write:");
        hexdump::hexdump(buf);
        loop {
            let n = poll_fn(|cx| {
                let mut stm = self.stm.borrow_mut();
                let stm = std::pin::Pin::new(&mut *stm);
                let n = match ready!(stm.poll_write(cx, buf)) {
                    Ok(n) => n,
                    Err(e) => return Poll::Ready(Err(e)),
                };
                Poll::Ready(Ok(n))
            })
            .await?;
            if n == buf.len() {
                break;
            }
            buf = &buf[n..];
        }
        Ok(())
    }

    fn process_message(&self, message: &[u8]) -> Result<Vec<u8>, PGError> {
        let state = &mut *self.state.borrow_mut();
        let mut send = vec![];
        match state {
            ConnState::Connecting(credentials) => {
                match_message!(message, Backend {
                    (AuthenticationOk) => {
                        eprintln!("auth ok");
                        *state = ConnState::Connected;
                    },
                    (AuthenticationSASL as sasl) => {
                        for mech in sasl.mechanisms() {
                            eprintln!("sasl: {:?}", mech);
                        }
                        let credentials = credentials.clone();
                        let mut tx = ClientTransaction::new("".into());
                        let mut env = ClientEnvironmentImpl { credentials };
                        let Some(initial_message) = tx.process_message(&[], &mut env)? else {
                            return Err(auth::SCRAMError::ProtocolError.into());
                        };
                        send = builder::SASLInitialResponse {
                            mechanism: "SCRAM-SHA-256",
                            response: &initial_message,
                        }.to_vec();
                        *state = ConnState::SCRAM(tx, env);
                    },
                    (Message as message) => {
                        let mlen = message.mlen();
                        eprintln!("Connecting Unknown message: {} (len {mlen})", message.mtype() as char)
                    },
                });
            }
            ConnState::SCRAM(tx, env) => {
                match_message!(message, Backend {
                    (AuthenticationSASLContinue as sasl) => {
                        let Some(message) = tx.process_message(&sasl.data(), env)? else {
                            return Err(auth::SCRAMError::ProtocolError.into());
                        };
                        send = builder::SASLResponse {
                            response: &message,
                        }.to_vec();
                    },
                    (AuthenticationSASLFinal as sasl) => {
                        let None = tx.process_message(&sasl.data(), env)? else {
                            return Err(auth::SCRAMError::ProtocolError.into());
                        };
                    },
                    (AuthenticationOk) => {
                        eprintln!("auth ok");
                        *state = ConnState::Connected;
                    },
                    (AuthenticationMessage as auth) => {
                        eprintln!("SCRAM Unknown auth message: {}", auth.status())
                    },
                    (ErrorResponse as error) => {
                        for field in error.fields() {
                            eprintln!("error: {} {:?}", field.etype(), field.value());
                        }
                    },
                    (Message as message) => {
                        let mlen = message.mlen();
                        eprintln!("SCRAM Unknown message: {} (len {mlen})", message.mtype() as char)
                    },
                });
            }
            ConnState::Connected => {
                match_message!(message, Backend {
                    (ParameterStatus as param) => {
                        eprintln!("param: {:?}={:?}", param.name(), param.value());
                    },
                    (BackendKeyData as key_data) => {
                        eprintln!("key={:?} pid={:?}", key_data.key(), key_data.pid());
                    },
                    (ReadyForQuery as ready) => {
                        eprintln!("ready: {:?}", ready.status());
                        *state = ConnState::Ready;
                    },
                    (Message as message) => {
                        let mlen = message.mlen();
                        eprintln!("Connected Unknown message: {} (len {mlen})", message.mtype() as char)
                    },
                });
            }
            ConnState::Ready => {}
        }

        Ok(send)
    }

    pub async fn task(&self) -> Result<(), PGError> {
        // Only allow connection in the initial state
        let credentials = match &*self.state.borrow() {
            ConnState::Connecting(credentials) => credentials.clone(),
            _ => {
                return Err(PGError::InvalidState);
            }
        };

        let startup = builder::StartupMessage {
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
        }
        .to_vec();
        self.write(&startup).await?;

        let mut messages = vec![];

        loop {
            let mut buffer = [0; 1024];
            let n = poll_fn(|cx| {
                let mut stm = self.stm.borrow_mut();
                let stm = std::pin::Pin::new(&mut *stm);
                let mut buf = ReadBuf::new(&mut buffer);
                ready!(stm.poll_read(cx, &mut buf))
                    .map(|_| buf.filled().len())
                    .into()
            })
            .await?;
            println!("Read:");
            hexdump::hexdump(&buffer[..n]);
            messages.extend_from_slice(&buffer[..n]);
            while messages.len() > 5 {
                let message = Message::new(&messages);
                if message.mlen() as usize <= messages.len() + 1 {
                    let n = message.mlen() + 1;
                    let message = self.process_message(&messages[..n])?;
                    messages = messages[n..].to_vec();
                    if !message.is_empty() {
                        self.write(&message).await?;
                    }
                } else {
                    break;
                }
            }

            if n == 0 {
                break;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {}
