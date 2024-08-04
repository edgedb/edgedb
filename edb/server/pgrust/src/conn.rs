use crate::protocol::{
    builder, match_message, measure, AuthenticationOk, BackendKeyData, Message, ParameterStatus,
    ReadyForQuery,
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
            let message_buf = &buffer[..(message.mlen() + 1) as _];
            buffer = &buffer[((message.mlen() + 1) as _)..];

            match_message!(message_buf, Backend {
                (AuthenticationOk) => {
                    eprintln!("auth ok");
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
        Ok(())
    }
}

#[cfg(test)]
mod tests {}
