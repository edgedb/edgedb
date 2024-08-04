use crate::protocol::{
    builder, measure, AuthenticationMessage, AuthenticationOk, BackendKeyData, Message,
    ParameterStatus, ReadyForQuery, Sync,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

trait Stream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin {}

impl<T> Stream for T where T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin {}

#[derive(Debug, thiserror::Error)]
pub enum PGError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub struct PGConn<S: Stream> {
    stm: S,
}

impl<S: Stream> PGConn<S> {
    pub fn new(stm: S) -> Self {
        Self { stm }
    }

    pub async fn connect(&mut self) -> Result<(), PGError> {
        let mlen = measure::StartupMessage {
            params: &[
                measure::StartupNameValue {
                    name: "user",
                    value: "postgres",
                },
                measure::StartupNameValue {
                    name: "database",
                    value: "postgres",
                },
            ],
        }
        .measure() as _;
        let startup = builder::StartupMessage {
            mlen,
            params: &[
                builder::StartupNameValue {
                    name: "user",
                    value: "postgres",
                },
                builder::StartupNameValue {
                    name: "database",
                    value: "postgres",
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
        while !buffer.is_empty() {
            let message = Message::new(buffer);
            eprintln!("{} {}", message.mtype() as char, message.mlen());
            let message_buf = &buffer[..(message.mlen() + 1) as _];
            buffer = &buffer[((message.mlen() + 1) as _)..];
            match (message.mtype(), message.mlen()) {
                (AuthenticationMessage::MTYPE, _) => {
                    let auth = AuthenticationMessage::new(message_buf);
                    if auth.status() == AuthenticationOk::STATUS {
                        eprintln!("auth ok");
                    } else {
                        eprintln!("status = {:?}", auth.status());
                    }
                }
                (Sync::MTYPE, Sync::MLEN) => {
                    eprintln!("sync");
                }
                (ParameterStatus::MTYPE, _) => {
                    let param = ParameterStatus::new(message_buf);
                    eprintln!("param: {:?}={:?}", param.name(), param.value());
                }
                (BackendKeyData::MTYPE, _) => {
                    let key = BackendKeyData::new(message_buf);
                    eprintln!("key={:?} pid={:?}", key.key(), key.pid());
                }
                (ReadyForQuery::MTYPE, _) => {
                    let ready = ReadyForQuery::new(message_buf);
                    eprintln!("ready: {:?}", ready.status());
                }
                (mtype, mlen) => eprintln!("Unknown message: {} (len {mlen})", mtype as char),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {}
