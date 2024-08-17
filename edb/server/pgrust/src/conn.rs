use crate::{
    connection::{
        ConnectionError, ConnectionState, ConnectionStateSend, ConnectionStateUpdate, Credentials,
    },
    protocol::{
        builder, match_message, CommandComplete, DataRow, ErrorResponse, Message, ReadyForQuery,
        RowDescription,
    },
};
use std::{
    cell::RefCell,
    task::{ready, Poll},
};
use std::{
    collections::VecDeque,
    future::{poll_fn, Future},
    rc::Rc,
    time::Duration,
};
use tokio::io::ReadBuf;

pub trait Stream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin {}

impl<T> Stream for T where T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin {}

#[derive(Debug, thiserror::Error)]
pub enum PGError {
    #[error("Invalid state")]
    InvalidState,
    #[error("Connection failed: {0}")]
    Connection(#[from] ConnectionError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

pub struct Client<S: Stream> {
    conn: Rc<PGConn<S>>,
}

impl<S: Stream> Client<S> {
    /// Create a new PostgreSQL client and a background task.
    pub fn new(
        parameters: ConnectionParameters,
        stm: S,
    ) -> (Self, impl Future<Output = Result<(), PGError>>) {
        let conn = Rc::new(PGConn::new(
            stm,
            parameters.username,
            parameters.password,
            parameters.database,
        ));
        let task = conn.clone().task();
        (Self { conn }, task)
    }

    pub async fn ready(&self) -> Result<(), PGError> {
        loop {
            if !self.conn.is_ready() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            } else {
                return Ok(());
            }
        }
    }

    pub fn query(
        &self,
        query: &str,
        f: impl QuerySink + 'static,
    ) -> impl Future<Output = Result<(), PGError>> {
        self.conn.clone().query(query.to_owned(), f)
    }
}

pub struct ConnectionParameters {
    pub username: String,
    pub password: String,
    pub database: String,
}

struct ErasedQuerySink<Q: QuerySink>(Q);

impl<Q, S> QuerySink for ErasedQuerySink<Q>
where
    Q: QuerySink<Output = S>,
    S: DataSink + 'static,
{
    type Output = Box<dyn DataSink>;
    fn error(&self, error: ErrorResponse) {
        self.0.error(error)
    }
    fn rows(&self, rows: RowDescription) -> Self::Output {
        Box::new(self.0.rows(rows))
    }
}

pub trait QuerySink {
    type Output: DataSink;
    fn rows(&self, rows: RowDescription) -> Self::Output;
    fn error(&self, error: ErrorResponse);
}

impl<Q, S> QuerySink for Box<Q>
where
    Q: QuerySink<Output = S> + 'static,
    S: DataSink + 'static,
{
    type Output = Box<dyn DataSink + 'static>;
    fn rows(&self, rows: RowDescription) -> Self::Output {
        Box::new(self.as_ref().rows(rows))
    }
    fn error(&self, error: ErrorResponse) {
        self.as_ref().error(error)
    }
}

impl<F1, F2, S> QuerySink for (F1, F2)
where
    F1: for<'a> Fn(RowDescription) -> S,
    F2: for<'a> Fn(ErrorResponse),
    S: DataSink,
{
    type Output = S;
    fn rows(&self, rows: RowDescription) -> S {
        (self.0)(rows)
    }
    fn error(&self, error: ErrorResponse) {
        (self.1)(error)
    }
}

pub trait DataSink {
    fn row(&self, values: Result<DataRow, ErrorResponse>);
}

impl DataSink for () {
    fn row(&self, _: Result<DataRow, ErrorResponse>) {}
}

impl<F> DataSink for F
where
    F: for<'a> Fn(Result<DataRow<'a>, ErrorResponse<'a>>),
{
    fn row(&self, values: Result<DataRow, ErrorResponse>) {
        (self)(values)
    }
}

impl DataSink for Box<dyn DataSink> {
    fn row(&self, values: Result<DataRow, ErrorResponse>) {
        self.as_ref().row(values)
    }
}

struct PGConn<S: Stream> {
    stm: RefCell<S>,
    state: RefCell<ConnState>,
}

struct QueryWaiter {
    #[allow(unused)]
    tx: tokio::sync::mpsc::UnboundedSender<()>,
    f: Box<dyn QuerySink<Output = Box<dyn DataSink>>>,
    data: RefCell<Option<Box<dyn DataSink>>>,
}

enum ConnState {
    Connecting(ConnectionState),
    Ready(VecDeque<QueryWaiter>),
}

impl<S: Stream> PGConn<S> {
    pub fn new(stm: S, username: String, password: String, database: String) -> Self {
        Self {
            stm: stm.into(),
            state: ConnState::Connecting(ConnectionState::new(Credentials {
                username,
                password,
                database,
            }))
            .into(),
        }
    }

    fn is_ready(&self) -> bool {
        matches!(&*self.state.borrow(), ConnState::Ready(..))
    }

    async fn write(&self, mut buf: &[u8]) -> Result<(), PGError> {
        if buf.is_empty() {
            return Ok(());
        }
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

    fn process_message(
        &self,
        message: Option<Message>,
        update: &mut impl ConnectionStateUpdate,
    ) -> Result<(), PGError> {
        let state = &mut *self.state.borrow_mut();
        match state {
            ConnState::Connecting(connection) => {
                let res = connection.drive(message, update);
                if connection.is_ready() {
                    *state = ConnState::Ready(Default::default());
                }
                res?;
            }
            ConnState::Ready(queue) => {
                let message = message.ok_or(PGError::InvalidState);
                match_message!(message?, Backend {
                    (RowDescription as row) => {
                        if let Some(qw) = queue.back() {
                            let qs = qw.f.rows(row);
                            *qw.data.borrow_mut() = Some(qs);
                        }
                    },
                    (DataRow as row) => {
                        if let Some(qw) = queue.back() {
                            if let Some(qs) = &*qw.data.borrow() {
                                qs.row(Ok(row))
                            }
                        }
                    },
                    (CommandComplete) => {
                        if let Some(qw) = queue.back() {
                            *qw.data.borrow_mut() = None;
                        }
                    },
                    (ReadyForQuery) => {
                        queue.pop_front();
                    },
                    (ErrorResponse as err) => {
                        if let Some(qw) = queue.back() {
                            qw.f.error(err);
                        }
                    },
                    unknown => {
                        eprintln!("Unknown message: {unknown:?}");
                    }
                });
            }
        }

        Ok(())
    }

    pub async fn task(self: Rc<Self>) -> Result<(), PGError> {
        // Only allow connection in the initial state

        struct Update(Vec<u8>);
        impl ConnectionStateSend for Update {
            fn send_initial(
                &mut self,
                message: crate::protocol::definition::InitialBuilder,
            ) -> Result<(), std::io::Error> {
                self.0.extend(message.to_vec());
                Ok(())
            }
            fn send(
                &mut self,
                message: crate::protocol::definition::FrontendBuilder,
            ) -> Result<(), std::io::Error> {
                self.0.extend(message.to_vec());
                Ok(())
            }
        }
        impl ConnectionStateUpdate for Update {}

        let mut update = Update(vec![]);
        self.process_message(None, &mut update)?;
        self.write(&std::mem::take(&mut update.0)).await?;

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
                if message.mlen() <= messages.len() + 1 {
                    let n = message.mlen() + 1;
                    let message = Message::new(&messages[..n]);
                    self.process_message(Some(message), &mut update)?;
                    messages = messages[n..].to_vec();
                    self.write(&std::mem::take(&mut update.0)).await?;
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

    pub async fn query(
        self: Rc<Self>,
        query: String,
        f: impl QuerySink + 'static,
    ) -> Result<(), PGError> {
        let mut rx = match &mut *self.state.borrow_mut() {
            ConnState::Ready(queue) => {
                let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                let f = Box::new(ErasedQuerySink(f)) as _;
                queue.push_back(QueryWaiter {
                    tx,
                    f,
                    data: None.into(),
                });
                rx
            }
            _ => return Err(PGError::InvalidState),
        };

        let message = builder::Query { query: &query }.to_vec();
        self.write(&message).await?;

        rx.recv().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {}
