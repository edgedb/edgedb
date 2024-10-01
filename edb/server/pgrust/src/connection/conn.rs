use super::{
    connect_raw_ssl,
    raw_conn::RawClient,
    stream::{Stream, StreamWithUpgrade},
    ConnectionSslRequirement, Credentials,
};
use crate::{
    connection::ConnectionError,
    protocol::{
        builder, match_message, meta, CommandComplete, DataRow, ErrorResponse, Message,
        ReadyForQuery, RowDescription, StructBuffer,
    },
};
use futures::FutureExt;
use std::{
    cell::RefCell,
    pin::Pin,
    sync::Arc,
    task::{ready, Poll},
};
use std::{
    collections::VecDeque,
    future::{poll_fn, Future},
    rc::Rc,
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tracing::{error, trace, warn};

#[derive(Debug, thiserror::Error)]
pub enum PGError {
    #[error("Invalid state")]
    InvalidState,
    #[error("Connection failed: {0}")]
    Connection(#[from] ConnectionError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Connection was closed")]
    Closed,
}

pub struct Client<B: Stream, C: Unpin>
where
    (B, C): StreamWithUpgrade,
{
    conn: Rc<PGConn<B, C>>,
}

impl<B: Stream, C: Unpin> Client<B, C>
where
    (B, C): StreamWithUpgrade,
    B: 'static,
    C: 'static,
{
    pub fn new(
        credentials: Credentials,
        socket: B,
        config: C,
    ) -> (Self, impl Future<Output = Result<(), PGError>>) {
        let conn = Rc::new(PGConn::new_connection(async move {
            let ssl_mode = ConnectionSslRequirement::Optional;
            let raw = connect_raw_ssl(credentials, ssl_mode, config, socket).await?;
            Ok(raw)
        }));
        let task = conn.clone().task();
        (Self { conn }, task)
    }

    /// Create a new PostgreSQL client and a background task.
    pub fn new_raw(stm: RawClient<B, C>) -> (Self, impl Future<Output = Result<(), PGError>>) {
        let conn = Rc::new(PGConn::new_raw(stm));
        let task = conn.clone().task();
        (Self { conn }, task)
    }

    pub async fn ready(&self) -> Result<(), PGError> {
        self.conn.ready().await
    }

    pub fn query(
        &self,
        query: &str,
        f: impl QuerySink + 'static,
    ) -> impl Future<Output = Result<(), PGError>> {
        self.conn.clone().query(query.to_owned(), f)
    }
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

struct QueryWaiter {
    #[allow(unused)]
    tx: tokio::sync::mpsc::UnboundedSender<()>,
    f: Box<dyn QuerySink<Output = Box<dyn DataSink>>>,
    data: RefCell<Option<Box<dyn DataSink>>>,
}

#[derive(derive_more::Debug)]
enum ConnState<B: Stream, C: Unpin>
where
    (B, C): StreamWithUpgrade,
{
    #[debug("Connecting(..)")]
    #[allow(clippy::type_complexity)]
    Connecting(Pin<Box<dyn Future<Output = Result<RawClient<B, C>, ConnectionError>>>>),
    #[debug("Ready(..)")]
    Ready(RawClient<B, C>, VecDeque<QueryWaiter>),
    Error(PGError),
    Closed,
}

struct PGConn<B: Stream, C: Unpin>
where
    (B, C): StreamWithUpgrade,
{
    state: RefCell<ConnState<B, C>>,
    write_lock: tokio::sync::Mutex<()>,
    ready_lock: Arc<tokio::sync::Mutex<()>>,
}

impl<B: Stream, C: Unpin> PGConn<B, C>
where
    (B, C): StreamWithUpgrade,
{
    pub fn new_connection(
        future: impl Future<Output = Result<RawClient<B, C>, ConnectionError>> + 'static,
    ) -> Self {
        Self {
            state: ConnState::Connecting(future.boxed_local()).into(),
            write_lock: Default::default(),
            ready_lock: Default::default(),
        }
    }

    pub fn new_raw(stm: RawClient<B, C>) -> Self {
        Self {
            state: ConnState::Ready(stm, Default::default()).into(),
            write_lock: Default::default(),
            ready_lock: Default::default(),
        }
    }

    fn check_error(&self) -> Result<(), PGError> {
        let state = &mut *self.state.borrow_mut();
        match state {
            ConnState::Error(..) => {
                let ConnState::Error(e) = std::mem::replace(state, ConnState::Closed) else {
                    unreachable!();
                };
                error!("Connection failed: {e:?}");
                Err(e)
            }
            ConnState::Closed => Err(PGError::Closed),
            _ => Ok(()),
        }
    }

    #[inline(always)]
    async fn ready(&self) -> Result<(), PGError> {
        let _ = self.ready_lock.lock().await;
        self.check_error()
    }

    fn with_stream<T, F>(&self, f: F) -> Result<T, PGError>
    where
        F: FnOnce(Pin<&mut RawClient<B, C>>) -> T,
    {
        match &mut *self.state.borrow_mut() {
            ConnState::Ready(ref mut raw_client, _) => Ok(f(Pin::new(raw_client))),
            _ => Err(PGError::InvalidState),
        }
    }

    async fn write(&self, mut buf: &[u8]) -> Result<(), PGError> {
        let _lock = self.write_lock.lock().await;

        if buf.is_empty() {
            return Ok(());
        }
        println!("Write:");
        hexdump::hexdump(buf);
        loop {
            let n = poll_fn(|cx| {
                self.with_stream(|stm| {
                    let n = match ready!(stm.poll_write(cx, buf)) {
                        Ok(n) => n,
                        Err(e) => return Poll::Ready(Err(PGError::Io(e))),
                    };
                    Poll::Ready(Ok(n))
                })?
            })
            .await?;
            if n == buf.len() {
                break;
            }
            buf = &buf[n..];
        }
        Ok(())
    }

    fn process_message(&self, message: Option<Message>) -> Result<(), PGError> {
        let state = &mut *self.state.borrow_mut();
        match state {
            ConnState::Ready(_, queue) => {
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
            ConnState::Connecting(..) => {
                return Err(PGError::InvalidState);
            }
            ConnState::Error(..) | ConnState::Closed => self.check_error()?,
        }

        Ok(())
    }

    pub fn task(self: Rc<Self>) -> impl Future<Output = Result<(), PGError>> {
        let ready_lock = self.ready_lock.clone().try_lock_owned().unwrap();

        async move {
            poll_fn(|cx| {
                let mut state = self.state.borrow_mut();
                match &mut *state {
                    ConnState::Connecting(fut) => match fut.poll_unpin(cx) {
                        Poll::Ready(result) => {
                            let raw = match result {
                                Ok(raw) => raw,
                                Err(e) => {
                                    let error = PGError::Connection(e);
                                    *state = ConnState::Error(error);
                                    return Poll::Ready(Ok::<_, PGError>(()));
                                }
                            };
                            *state = ConnState::Ready(raw, VecDeque::new());
                            Poll::Ready(Ok::<_, PGError>(()))
                        }
                        Poll::Pending => Poll::Pending,
                    },
                    ConnState::Ready(..) => Poll::Ready(Ok(())),
                    ConnState::Error(..) | ConnState::Closed => Poll::Ready(self.check_error()),
                }
            })
            .await?;

            drop(ready_lock);

            let mut buffer = StructBuffer::<meta::Message>::default();
            loop {
                let mut read_buffer = [0; 1024];
                let n = poll_fn(|cx| {
                    self.with_stream(|stm| {
                        let mut buf = ReadBuf::new(&mut read_buffer);
                        let res = ready!(stm.poll_read(cx, &mut buf));
                        Poll::Ready(res.map(|_| buf.filled().len())).map_err(PGError::Io)
                    })?
                })
                .await?;

                println!("Read:");
                hexdump::hexdump(&read_buffer[..n]);

                buffer.push_fallible(&read_buffer[..n], |message| {
                    self.process_message(Some(message))
                })?;

                if n == 0 {
                    break;
                }
            }
            Ok(())
        }
    }

    pub async fn query(
        self: Rc<Self>,
        query: String,
        f: impl QuerySink + 'static,
    ) -> Result<(), PGError> {
        trace!("Query task started: {query}");
        let mut rx = match &mut *self.state.borrow_mut() {
            ConnState::Ready(_, queue) => {
                let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                let f = Box::new(ErasedQuerySink(f)) as _;
                queue.push_back(QueryWaiter {
                    tx,
                    f,
                    data: None.into(),
                });
                rx
            }
            x => {
                warn!("Connection state was not ready: {x:?}");
                return Err(PGError::InvalidState);
            }
        };

        let message = builder::Query { query: &query }.to_vec();
        self.write(&message).await?;
        rx.recv().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {}
