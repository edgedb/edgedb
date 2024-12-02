use super::{
    connect_raw_ssl,
    flow::{MessageHandler, MessageResult, Pipeline, QuerySink, SyncMessageHandler},
    raw_conn::RawClient,
    stream::{Stream, StreamWithUpgrade},
    Credentials,
};
use crate::{
    connection::{flow::QueryMessageHandler, ConnectionError},
    handshake::ConnectionSslRequirement,
    protocol::{
        postgres::{builder, data::Message, meta},
        StructBuffer,
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
use tracing::{error, trace, warn, Level};

#[derive(Debug, thiserror::Error)]
pub enum PGConnError {
    #[error("Invalid state")]
    InvalidState,
    #[error("Postgres error: {0}")]
    PgError(#[from] crate::errors::PgServerError),
    #[error("Connection failed: {0}")]
    Connection(#[from] ConnectionError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// If an operation in a pipeline group fails, all operations up to
    /// the next sync are skipped.
    #[error("Operation skipped because of previous pipeline failure: {0}")]
    Skipped(crate::errors::PgServerError),
    #[error("Connection was closed")]
    Closed,
}

/// A client for a PostgreSQL connection.
///
/// ```
/// client = Client::new(credentials, socket, config);
/// client.query("SELECT 1", logging_sink()).await;
/// if client.pipeline(Pipeline::Flush, &[
///     Flow::Parse("stmt1", "SELECT 1", &[Oid::Unspecified]),
///     Flow::Bind("portal1", "stmt1", &[Param::Null], &[Format::Text]),
///     Flow::Execute("portal1", MaxRows::Limited(NonZeroU32::new(1).unwrap())),
/// ]).await.is_ok() {
///     client.pipeline(Pipeline::Sync, &[
///         Flow::Query("..."),
///     ]).await;
/// }
/// ```
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
    ) -> (Self, impl Future<Output = Result<(), PGConnError>>) {
        let conn = Rc::new(PGConn::new_connection(async move {
            let ssl_mode = ConnectionSslRequirement::Optional;
            let raw = connect_raw_ssl(credentials, ssl_mode, config, socket).await?;
            Ok(raw)
        }));
        let task = conn.clone().task();
        (Self { conn }, task)
    }

    /// Create a new PostgreSQL client and a background task.
    pub fn new_raw(stm: RawClient<B, C>) -> (Self, impl Future<Output = Result<(), PGConnError>>) {
        let conn = Rc::new(PGConn::new_raw(stm));
        let task = conn.clone().task();
        (Self { conn }, task)
    }

    pub async fn ready(&self) -> Result<(), PGConnError> {
        self.conn.ready().await
    }

    /// Performs a bare `Query` operation. The sink handles the following messages:
    ///
    ///  - `RowDescription`
    ///  - `DataRow`
    ///  - `CopyOutResponse`
    ///  - `CopyData`
    ///  - `ErrorResponse`
    ///
    /// `CopyInResponse` is not currently supported and will result in a `CopyFail` being
    /// sent to the server.
    pub fn query(
        &self,
        query: &str,
        f: impl QuerySink + 'static,
    ) -> impl Future<Output = Result<(), PGConnError>> {
        self.conn.clone().query(query, f)
    }

    /// Performs a set of pipelined steps as a `Sync` group.
    pub fn pipeline_sync(
        &self,
        pipeline: Pipeline,
    ) -> impl Future<Output = Result<(), PGConnError>> {
        self.conn.clone().pipeline_sync(pipeline)
    }
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
    Ready(
        RawClient<B, C>,
        VecDeque<(
            &'static str,
            Box<dyn MessageHandler>,
            Option<tokio::sync::oneshot::Sender<()>>,
        )>,
    ),
    Error(PGConnError),
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

    fn check_error(&self) -> Result<(), PGConnError> {
        let state = &mut *self.state.borrow_mut();
        match state {
            ConnState::Error(..) => {
                let ConnState::Error(e) = std::mem::replace(state, ConnState::Closed) else {
                    unreachable!();
                };
                error!("Connection failed: {e:?}");
                Err(e)
            }
            ConnState::Closed => Err(PGConnError::Closed),
            _ => Ok(()),
        }
    }

    #[inline(always)]
    async fn ready(&self) -> Result<(), PGConnError> {
        let _ = self.ready_lock.lock().await;
        self.check_error()
    }

    fn with_stream<T, F>(&self, f: F) -> Result<T, PGConnError>
    where
        F: FnOnce(Pin<&mut RawClient<B, C>>) -> T,
    {
        match &mut *self.state.borrow_mut() {
            ConnState::Ready(ref mut raw_client, _) => Ok(f(Pin::new(raw_client))),
            _ => Err(PGConnError::InvalidState),
        }
    }

    fn enqueue_handler(
        &self,
        name: &'static str,
        handler: Box<dyn MessageHandler>,
        tx: Option<tokio::sync::oneshot::Sender<()>>,
    ) -> Result<(), PGConnError> {
        match &mut *self.state.borrow_mut() {
            ConnState::Ready(_, queue) => {
                queue.push_back((name, handler, tx));
            }
            x => {
                warn!("Connection state was not ready: {x:?}");
                return Err(PGConnError::InvalidState);
            }
        }
        Ok(())
    }

    async fn write(&self, mut buf: &[u8]) -> Result<(), PGConnError> {
        let _lock = self.write_lock.lock().await;

        if buf.is_empty() {
            return Ok(());
        }
        if tracing::enabled!(Level::TRACE) {
            trace!("Write:");
            for s in hexdump::hexdump_iter(buf) {
                trace!("{}", s);
            }
        }
        loop {
            let n = poll_fn(|cx| {
                self.with_stream(|stm| {
                    let n = match ready!(stm.poll_write(cx, buf)) {
                        Ok(n) => n,
                        Err(e) => return Poll::Ready(Err(PGConnError::Io(e))),
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

    fn process_message(&self, message: Option<Message>) -> Result<(), PGConnError> {
        let state = &mut *self.state.borrow_mut();
        match state {
            ConnState::Ready(_, queue) => {
                let message = message.ok_or(PGConnError::InvalidState);
                if let Some((name, handler, _tx)) = queue.front_mut() {
                    match handler.handle(message?) {
                        MessageResult::SkipUntilSync => {
                            let mut found_sync = false;
                            while let Some((name, handler, _)) = queue.front() {
                                if handler.is_sync() {
                                    found_sync = true;
                                    break;
                                }
                                trace!("skipping {name}");
                                queue.pop_front();
                            }
                            if !found_sync {
                                warn!("No sync handler found");
                            }
                        }
                        MessageResult::Continue => {}
                        MessageResult::Done => {
                            queue.pop_front();
                        }
                        MessageResult::Unknown => {
                            // TODO
                            warn!("Unknown message in {name}");
                        }
                    };
                };
            }
            ConnState::Connecting(..) => {
                return Err(PGConnError::InvalidState);
            }
            ConnState::Error(..) | ConnState::Closed => self.check_error()?,
        }

        Ok(())
    }

    pub fn task(self: Rc<Self>) -> impl Future<Output = Result<(), PGConnError>> {
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
                                    let error = PGConnError::Connection(e);
                                    *state = ConnState::Error(error);
                                    return Poll::Ready(Ok::<_, PGConnError>(()));
                                }
                            };
                            *state = ConnState::Ready(raw, VecDeque::new());
                            Poll::Ready(Ok::<_, PGConnError>(()))
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
                        Poll::Ready(res.map(|_| buf.filled().len())).map_err(PGConnError::Io)
                    })?
                })
                .await?;

                if tracing::enabled!(Level::TRACE) {
                    trace!("Read:");
                    for s in hexdump::hexdump_iter(&read_buffer[..n]) {
                        trace!("{}", s);
                    }
                }

                buffer.push_fallible(&read_buffer[..n], |message| {
                    if let Ok(message) = &message {
                        if tracing::enabled!(Level::TRACE) {
                            trace!("Message ({:?})", message.mtype() as char);
                            for s in hexdump::hexdump_iter(message.__buf) {
                                trace!("{}", s);
                            }
                        }
                    };
                    self.process_message(Some(message.map_err(ConnectionError::ParseError)?))
                })?;

                if n == 0 {
                    break;
                }
            }
            Ok(())
        }
    }

    pub fn query(
        self: Rc<Self>,
        query: &str,
        f: impl QuerySink + 'static,
    ) -> impl Future<Output = Result<(), PGConnError>> {
        trace!("Query task started: {query}");
        let message = builder::Query { query: &query }.to_vec();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let handler = QueryMessageHandler {
            sink: f,
            data: None.into(),
            copy: None.into(),
        };
        async move {
            self.enqueue_handler("query", Box::new(handler), Some(tx))?;
            self.write(&message).await?;
            _ = rx.await;
            Ok(())
        }
    }

    pub fn pipeline_sync(
        self: Rc<Self>,
        pipeline: Pipeline,
    ) -> impl Future<Output = Result<(), PGConnError>> {
        async move {
            let (tx, rx) = tokio::sync::oneshot::channel();
            for handler in pipeline.handlers {
                self.enqueue_handler("pipeline", handler, None)?;
            }
            self.enqueue_handler("sync", Box::new(SyncMessageHandler), Some(tx))?;
            self.write(&pipeline.messages).await?;
            self.write(&builder::Sync::default().to_vec()).await?;
            _ = rx.await;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use hex_literal::hex;
    use std::{fmt::Write, time::Duration};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt, DuplexStream},
        task::LocalSet,
        time::timeout,
    };

    use crate::connection::{
        flow::{CopyDataSink, DataSink, DoneHandling},
        raw_conn::ConnectionParams,
    };
    use crate::protocol::postgres::data::*;

    use super::*;

    impl QuerySink for Rc<RefCell<String>> {
        type Output = Self;
        type CopyOutput = Self;
        fn rows(&mut self, rows: RowDescription) -> Self {
            write!(self.borrow_mut(), "[table=[").unwrap();
            for field in rows.fields() {
                write!(self.borrow_mut(), "{},", field.name().to_string_lossy()).unwrap();
            }
            write!(self.borrow_mut(), "]").unwrap();
            self.clone()
        }
        fn copy(&mut self, copy: CopyOutResponse) -> Self {
            write!(
                self.borrow_mut(),
                "[copy={:?} {:?}",
                copy.format(),
                copy.format_codes()
            )
            .unwrap();
            self.clone()
        }
        fn error(&mut self, error: ErrorResponse) {
            for field in error.fields() {
                if field.etype() as char == 'C' {
                    write!(
                        self.borrow_mut(),
                        "[error {}]",
                        field.value().to_string_lossy()
                    )
                    .unwrap();
                    return;
                }
            }
            write!(self.borrow_mut(), "[error ??? {:?}]", error).unwrap();
        }
        fn protocol_violation(&mut self, message: Message, hint: &'static str) {
            // This won't happen during these tests
            panic!(
                "protocol error {}: {:?} ({hint})",
                message.mtype() as char,
                message
            );
        }
    }

    impl DataSink for Rc<RefCell<String>> {
        fn row(&mut self, row: DataRow) {
            write!(self.borrow_mut(), "[").unwrap();
            for value in row.values() {
                write!(self.borrow_mut(), "{},", value.to_string_lossy()).unwrap();
            }
            write!(self.borrow_mut(), "]").unwrap();
        }
        fn done(&mut self, result: Result<CommandComplete, ErrorResponse>) -> DoneHandling {
            match result {
                Ok(complete) => {
                    write!(
                        self.borrow_mut(),
                        " done={}]",
                        complete.tag().to_string_lossy()
                    )
                    .unwrap();
                }
                Err(error) => {
                    for field in error.fields() {
                        if field.etype() as char == 'C' {
                            write!(
                                self.borrow_mut(),
                                "[error {}]]",
                                field.value().to_string_lossy()
                            )
                            .unwrap();
                            return DoneHandling::Handled;
                        }
                    }
                    write!(self.borrow_mut(), "[error ??? {:?}]]", error).unwrap();
                }
            }
            DoneHandling::Handled
        }
    }

    impl CopyDataSink for Rc<RefCell<String>> {
        fn data(&mut self, data: CopyData) {
            write!(
                self.borrow_mut(),
                "[{}]",
                String::from_utf8_lossy(data.data().as_ref())
            )
            .unwrap();
        }
        fn done(&mut self, result: Result<CommandComplete, ErrorResponse>) -> DoneHandling {
            match result {
                Ok(complete) => {
                    write!(
                        self.borrow_mut(),
                        " done={}]",
                        complete.tag().to_string_lossy()
                    )
                    .unwrap();
                }
                Err(error) => {
                    for field in error.fields() {
                        if field.etype() as char == 'C' {
                            write!(
                                self.borrow_mut(),
                                "[error {}]]",
                                field.value().to_string_lossy()
                            )
                            .unwrap();
                            return DoneHandling::Handled;
                        }
                    }
                    write!(self.borrow_mut(), "[error ??? {:?}]]", error).unwrap();
                }
            }
            DoneHandling::Handled
        }
    }

    async fn read_expect<S: AsyncReadExt + Unpin>(stream: &mut S, expected: &[u8]) {
        let mut buf = vec![0u8; expected.len()];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, expected);
    }

    /// Perform a test using captured binary protocol data from a real server.
    async fn run_expect<F: Future>(
        query_task: impl FnOnce(Client<DuplexStream, ()>, Rc<RefCell<String>>) -> F + 'static,
        expect: &'static [(&[u8], &[u8], &str)],
    ) {
        let f = async move {
            let (mut s1, s2) = tokio::io::duplex(1024 * 1024);

            let (client, task) = Client::new_raw(RawClient::new(s2, ConnectionParams::default()));
            let task_handle = tokio::task::spawn_local(task);

            let handle = tokio::task::spawn_local(async move {
                let log = Rc::new(RefCell::new(String::new()));
                query_task(client, log.clone()).await;
                Rc::try_unwrap(log).unwrap().into_inner()
            });

            let mut log_expect = String::new();
            for (read, write, expect) in expect {
                // Query[text=""]
                eprintln!("read {read:?}");
                read_expect(&mut s1, read).await;
                eprintln!("write {write:?}");
                s1.write_all(write).await.unwrap();
                log_expect.push_str(expect);
            }

            let log = handle.await.unwrap();

            assert_eq!(log, log_expect);

            // EOF to trigger the task to exit
            drop(s1);

            task_handle.await.unwrap().unwrap();
        };

        let local = LocalSet::new();
        let task = local.spawn_local(f);

        timeout(Duration::from_secs(1), local).await.unwrap();

        // Ensure we detect panics inside the task
        task.await.unwrap();
    }

    #[test_log::test(tokio::test)]
    async fn query_select_1() {
        run_expect(
            |client, log| async move {
                client.query("SELECT 1", log.clone()).await.unwrap();
            },
            &[(
                &hex!("51000000 0d53454c 45435420 3100"),
                // T, D, C, Z
                &hex!("54000000 2100013f 636f6c75 6d6e3f00 00000000 00000000 00170004 ffffffff 00004400 00000b00 01000000 01314300 00000d53 454c4543 54203100 5a000000 0549"),
                "[table=[?column?,][1,] done=SELECT 1]",
            )],
        )
        .await;
    }

    #[test_log::test(tokio::test)]
    async fn query_select_1_limit_0() {
        run_expect(
            |client, log| async move {
                client.query("SELECT 1 LIMIT 0", log.clone()).await.unwrap();
            },
            &[(
                &hex!("51000000 1553454c 45435420 31204c49 4d495420 3000"),
                // T, C, Z
                &hex!("54000000 2100013f 636f6c75 6d6e3f00 00000000 00000000 00170004 ffffffff 00004300 00000d53 454c4543 54203000 5a000000 0549"),
                "[table=[?column?,] done=SELECT 0]",
            )],
        )
        .await;
    }

    #[test_log::test(tokio::test)]
    async fn query_copy_1() {
        run_expect(
            |client, log| async move {
                client.query("copy (select 1) to stdout;", log.clone()).await.unwrap();
            },
            &[(
                &hex!("51000000 1f636f70 79202873 656c6563 74203129 20746f20 7374646f 75743b00"),
                // H, d, c, C, Z
                &hex!("48000000 09000001 00006400 00000631 0a630000 00044300 00000b43 4f505920 31005a00 00000549"),
                "[copy=0 [0][1\n] done=COPY 1]",
            )],
        )
        .await;
    }

    #[test_log::test(tokio::test)]
    async fn query_copy_1_limit_0() {
        run_expect(
            |client, log| async move {
                client.query("copy (select 1 limit 0) to stdout;", log.clone()).await.unwrap();
            },
            &[(
                &hex!("51000000 27636f70 79202873 656c6563 74203120 6c696d69 74203029 20746f20 7374646f 75743b00"),
                // H, c, C, Z
                &hex!("48000000 09000001 00006300 00000443 0000000b 434f5059 2030005a 00000005 49"),
                "[copy=0 [0] done=COPY 0]",
            )],
        )
        .await;
    }

    #[test_log::test(tokio::test)]
    async fn query_copy_with_error_rows() {
        run_expect(
            |client, log| async move {
                client.query("copy (select case when id = 2 then id/(id-2) else id end from (select generate_series(1,2) as id)) to stdout;", log.clone()).await.unwrap();
            },
            &[(
                &hex!("""
                    51000000 72636f70 79202873 656c6563
                    74206361 73652077 68656e20 6964203d
                    20322074 68656e20 69642f28 69642d32
                    2920656c 73652069 6420656e 64206672
                    6f6d2028 73656c65 63742067 656e6572
                    6174655f 73657269 65732831 2c322920
                    61732069 64292920 746f2073 74646f75
                    743b00
                """),
                // H, d, E, Z
                &hex!("""
                    48000000 09000001 00006400 00000631
                    0a450000 00415345 52524f52 00564552
                    524f5200 43323230 3132004d 64697669
                    73696f6e 20627920 7a65726f 0046696e
                    742e6300 4c383431 0052696e 74346469
                    7600005a 00000005 49
                """),
                "[copy=0 [0][1\n][error 22012]]",
            )],
        )
        .await;
    }

    #[test_log::test(tokio::test)]
    async fn query_error() {
        run_expect(
            |client, log| async move {
                client.query("do $$begin raise exception 'hi'; end$$;", log.clone()).await.unwrap();
            },
            &[(
                &hex!("51000000 2c646f20 24246265 67696e20 72616973 65206578 63657074 696f6e20 27686927 3b20656e 6424243b 00"),
                // E, Z
                &hex!("""
                    45000000 75534552 524f5200 56455252
                    4f520043 50303030 31004d68 69005750
                    4c2f7067 53514c20 66756e63 74696f6e
                    20696e6c 696e655f 636f6465 5f626c6f
                    636b206c 696e6520 31206174 20524149
                    53450046 706c5f65 7865632e 63004c33
                    39313100 52657865 635f7374 6d745f72
                    61697365 00005a00 00000549
                """),
                "[error P0001]",
            )],
        )
        .await;
    }

    #[test_log::test(tokio::test)]
    async fn query_empty_do() {
        run_expect(
            |client, log| async move {
                client
                    .query("do $$begin end$$;", log.clone())
                    .await
                    .unwrap();
            },
            &[(
                &hex!("51000000 16646f20 24246265 67696e20 656e6424 243b00"),
                // C, Z
                &hex!("""
                    43000000 07444f00 5a000000 0549
                """),
                "",
            )],
        )
        .await;
    }

    #[test_log::test(tokio::test)]
    async fn query_error_with_rows() {
        run_expect(
            |client, log| async move {
                client.query("select case when id = 2 then id/(id-2) else 1 end from (select 1 as id union all select 2 as id);", log.clone()).await.unwrap();
            },
            &[(
                &hex!("""
                    51000000 6673656c 65637420 63617365
                    20776865 6e206964 203d2032 20746865
                    6e206964 2f286964 2d322920 656c7365
                    20312065 6e642066 726f6d20 2873656c
                    65637420 31206173 20696420 756e696f
                    6e20616c 6c207365 6c656374 20322061
                    73206964 293b00
                """),
                // T, D, E, Z
                &hex!("""
                    54000000 1d000163 61736500 00000000
                    00000000 00170004 ffffffff 00004400
                    00000b00 01000000 01314500 00004153
                    4552524f 52005645 52524f52 00433232
                    30313200 4d646976 6973696f 6e206279
                    207a6572 6f004669 6e742e63 004c3834
                    31005269 6e743464 69760000 5a000000
                    0549
                """),
                "[table=[case,][1,][error 22012]]",
            )],
        )
        .await;
    }

    #[test_log::test(tokio::test)]
    async fn query_second_errors() {
        run_expect(
            |client, log| async move {
                client
                    .query("select; select 1/0;", log.clone())
                    .await
                    .unwrap();
            },
            &[(
                &hex!("51000000 1873656c 6563743b 2073656c 65637420 312f303b 00"),
                // T, D, C, E, Z
                &hex!("""
                        54000000 06000044 00000006 00004300
                        00000d53 454c4543 54203100 45000000
                        41534552 524f5200 56455252 4f520043
                        32323031 32004d64 69766973 696f6e20
                        6279207a 65726f00 46696e74 2e63004c
                        38343100 52696e74 34646976 00005a00
                        00000549
                    """),
                "[table=[][] done=SELECT 1][error 22012]",
            )],
        )
        .await;
    }

    #[test_log::test(tokio::test)]
    async fn query_two_empty() {
        run_expect(
            |client, log| async move {
                client.query("", log.clone()).await.unwrap();
                client.query("", log.clone()).await.unwrap();
            },
            &[
                (
                    &hex!("51000000 0500"),
                    // I, Z
                    &hex!("49000000 045a0000 000549"),
                    "",
                ),
                (
                    &hex!("51000000 0500"),
                    // I, Z
                    &hex!("49000000 045a0000 000549"),
                    "",
                ),
            ],
        )
        .await;
    }

    #[test_log::test(tokio::test)]
    async fn query_two_error() {
        run_expect(
            |client, log| async move {
                client
                    .query(".", log.clone())
                    .await
                    .expect_err("should fail");
                client
                    .query(".", log.clone())
                    .await
                    .expect_err("should fail");
            },
            &[
                (
                    &hex!("51000000 062e00"),
                    // E, Z
                    &hex!("""
                        45000000 59534552 524f5200 56455252
                        4f520043 34323630 31004d73 796e7461
                        78206572 726f7220 6174206f 72206e65
                        61722022 2e220050 31004673 63616e2e
                        6c004c31 32343400 52736361 6e6e6572
                        5f797965 72726f72 0000
                    """),
                    "",
                ),
                (
                    &hex!("51000000 062e00"),
                    // E, Z
                    &hex!("""
                        45000000 59534552 524f5200 56455252
                        4f520043 34323630 31004d73 796e7461
                        78206572 726f7220 6174206f 72206e65
                        61722022 2e220050 31004673 63616e2e
                        6c004c31 32343400 52736361 6e6e6572
                        5f797965 72726f72 00005a00 00000549
                    """),
                    "",
                ),
            ],
        )
        .await;
    }
}
