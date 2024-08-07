use crate::{auth::{self, generate_salted_password, ClientEnvironment, ClientTransaction, Sha256Out}, protocol::{
    builder, match_message, measure, AuthenticationOk, AuthenticationSASL, AuthenticationSASLContinue, AuthenticationSASLFinal, BackendKeyData, ErrorResponse, Message, ParameterStatus, ReadyForQuery
}};
use base64::Engine;
use rand::Rng;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

trait Stream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin {}

impl<T> Stream for T where T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin {}

#[derive(Debug, thiserror::Error)]
pub enum PGError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("SCRAM: {0}")]
    SCRAM(#[from] auth::SCRAMError),
}

pub struct PGConn<S: Stream> {
    stm: S,
    username: String,
    password: String,
    database: String,
}

impl<S: Stream> PGConn<S> {
    pub fn new(stm: S, username: String, password: String, database: String) -> Self {
        Self { stm, username, password, database }
    }

    pub async fn connect(&mut self) -> Result<(), PGError> {
        let mlen = measure::StartupMessage {
            params: &[
                measure::StartupNameValue {
                    name: "user",
                    value: &self.username,
                },
                measure::StartupNameValue {
                    name: "database",
                    value: &self.database,
                },
            ],
        }
        .measure() as _;
        let startup = builder::StartupMessage {
            mlen,
            params: &[
                builder::StartupNameValue {
                    name: "user",
                    value: &self.username,
                },
                builder::StartupNameValue {
                    name: "database",
                    value: &self.database,
                },
            ],
        }
        .to_vec();
        eprintln!("{startup:?}");
        self.stm.write_all(&startup).await?;
        self.stm.flush().await?;
        let mut buffer = [0; 65000];
        let r = self.stm.read(&mut buffer).await?;

        let mut buffer = &buffer[..r];
        let mut use_sasl = false;
        while !buffer.is_empty() {
            let message = Message::new(buffer);
            let message_buf = &buffer[..(message.mlen() + 1) as _];
            buffer = &buffer[((message.mlen() + 1) as _)..];

            match_message!(message_buf, Backend {
                (AuthenticationOk) => {
                    eprintln!("auth ok");
                },
                (AuthenticationSASL as sasl) => {
                    for mech in sasl.mechanisms() {
                        eprintln!("sasl: {:?}", mech);
                        use_sasl = true;
                    }
                },
                (ParameterStatus as param) => {
                    eprintln!("param: {:?}={:?}", param.name(), param.value());
                },
                (BackendKeyData as key_data) => {
                    eprintln!("key={:?} pid={:?}", key_data.key(), key_data.pid());
                },
                (ReadyForQuery as ready) => {
                    eprintln!("ready: {:?}", ready.status());
                },
                (Message) => {
                    let mlen = message.mlen();
                    eprintln!("Unknown message: {} (len {mlen})", message.mtype() as char)
                },
            });
        }
        if use_sasl {
            let mut client = ClientTransaction::new("".into());
            struct Env {
                username: String,
                password: String,
            }
            impl ClientEnvironment for Env {
                fn get_salted_password(&self, username: &str, salt: &[u8], iterations: usize) -> Sha256Out {
                    generate_salted_password(&self.password, salt, iterations)
                }
                fn generate_nonce(&self) -> String {
                    let nonce: [u8; 32] = rand::thread_rng().r#gen();
                    base64::engine::general_purpose::STANDARD.encode(nonce)
                }
            }

            let env = Env {
                username: self.username.clone(),
                password: self.password.clone(),
            };

            let mut buffer = [0; 1024];
            let mut n = 0;
            loop {
                let mut sasl_message = None;
                if n == 0 {
                    sasl_message = client.process_message(&[], &env)?;
                } else {
                    let message = Message::new(&buffer);
                    let message_buf = &buffer[..(message.mlen() + 1) as _];
        
                    match_message!(message_buf, Backend {
                        (AuthenticationSASLContinue as sasl) => {
                            sasl_message = client.process_message(&sasl.data(), &env)?;
                        },
                        (AuthenticationSASLFinal as sasl) => {
                            let None = client.process_message(&sasl.data(), &env)? else {
                                return Err(auth::SCRAMError::ProtocolError.into());
                            };
                            break;
                        },
                        (ErrorResponse as error) => {
                            for field in error.fields() {
                                eprintln!("error: {} {:?}", field.etype(), field.value());
                            }
                        }
                    });
                }
                if let Some(message) = sasl_message {
                    if n == 0 {
                        let mlen = (measure::SASLInitialResponse {
                            mechanism: "SCRAM-SHA-256",
                            response: &message,
                        }.measure() - 1) as _;
                        let message = builder::SASLInitialResponse {
                            mlen,
                            mechanism: "SCRAM-SHA-256",
                            response: &message,
                        }.to_vec();
                        self.stm.write_all(&message).await?;
                    } else {
                        let mlen = (measure::SASLResponse {
                            response: &message,
                        }.measure() - 1) as _;
                        let message = builder::SASLResponse {
                            mlen,
                            response: &message,
                        }.to_vec();
                        self.stm.write_all(&message).await?;
                    }
                }
                if client.success() {
                    break;
                }
                eprintln!("Reading");
                n = self.stm.read(&mut buffer).await?;
                eprintln!("{:?}", &buffer[..n]);
            }

        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {}
