//! Postgres flow notes:
//!
//! https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-PIPELINING
//!
//! Extended query messages Parse, Bind, Describe, Execute, Close put the server
//! into a "skip-til-sync" mode when erroring. All messages other than Terminate (including
//! those not part of the extended query protocol) are skipped until an explicit Sync message is received.
//!
//! Sync closes _implicit_ but not _explicit_ transactions.
//!
//! Both Query and Execute may return COPY responses rather than rows. In the case of Query,
//! RowDescription + DataRow is replaced by CopyOutResponse + CopyData + CopyDone. In the case
//! of Execute, describing the portal will return NoData, but Execute will return CopyOutResponse +
//! CopyData + CopyDone.

use std::num::NonZeroU32;

use tracing::warn;

use crate::protocol::{
    match_message,
    postgres::{
        builder,
        data::{
            BindComplete, CloseComplete, CommandComplete, CopyData, CopyDone, CopyOutResponse,
            DataRow, EmptyQueryResponse, ErrorResponse, Message, ParseComplete, PortalSuspended,
            ReadyForQuery, RowDescription,
        },
    },
    Encoded,
};

#[derive(Debug, Clone, Copy)]
pub enum Param<'a> {
    Null,
    Text(&'a str),
    Binary(&'a [u8]),
}

#[derive(Debug, Clone, Copy, zerocopy_derive::KnownLayout)]
#[repr(u32)]
pub enum Oid {
    Unspecified,
    Oid(NonZeroU32),
}

#[derive(Debug, Clone, Copy)]
#[repr(i16)]
pub enum Format {
    Text = 0,
    Binary = 1,
}

#[derive(Debug, Clone, Copy)]
#[repr(i32)]
pub enum MaxRows {
    Unlimited,
    Limited(NonZeroU32),
}

#[derive(Debug, Clone, Copy)]
pub struct Portal<'a>(pub &'a str);

#[derive(Debug, Clone, Copy)]
pub struct Statement<'a>(pub &'a str);

pub trait Flow {
    fn to_vec(&self) -> Vec<u8>;
}

/// Performs a prepared statement parse operation.
///
/// Handles:
///  - `ParseComplete`
///  - `ErrorResponse`
#[derive(Debug, Clone, Copy)]
struct ParseFlow<'a> {
    pub name: Statement<'a>,
    pub query: &'a str,
    pub param_types: &'a [Oid],
}

/// Performs a prepared statement bind operation.
///
/// Handles:
///  - `BindComplete`
///  - `ErrorResponse`
#[derive(Debug, Clone, Copy)]
struct BindFlow<'a> {
    pub portal: Portal<'a>,
    pub statement: Statement<'a>,
    pub params: &'a [Param<'a>],
    pub result_format_codes: &'a [Format],
}

/// Performs a prepared statement execute operation.
///
/// Handles:
///  - `CommandComplete`
///  - `DataRow`
///  - `PortalSuspended`
///  - `CopyOutResponse`
///  - `CopyData`
///  - `CopyDone`
///  - `ErrorResponse`
#[derive(Debug, Clone, Copy)]
struct ExecuteFlow<'a> {
    pub portal: Portal<'a>,
    pub max_rows: MaxRows,
}

/// Performs a portal describe operation.
///
/// Handles:
///  - `RowDescription`
///  - `NoData`
///  - `ErrorResponse`
#[derive(Debug, Clone, Copy)]
struct DescribePortalFlow<'a> {
    pub name: Portal<'a>,
}

/// Performs a statement describe operation.
///
/// Handles:
///  - `RowDescription`
///  - `NoData`
///  - `ParameterDescription`
///  - `ErrorResponse`
#[derive(Debug, Clone, Copy)]
struct DescribeStatementFlow<'a> {
    pub name: Statement<'a>,
}

/// Performs a portal close operation.
///
/// Handles:
///  - `CloseComplete`
///  - `ErrorResponse`
#[derive(Debug, Clone, Copy)]
struct ClosePortalFlow<'a> {
    pub name: Portal<'a>,
}

/// Performs a statement close operation.
///
/// Handles:
///  - `CloseComplete`
///  - `ErrorResponse`
#[derive(Debug, Clone, Copy)]
struct CloseStatementFlow<'a> {
    pub name: Statement<'a>,
}

/// Performs a query operation.
///
/// Handles:
///  - `EmptyQueryResponse`: If no queries were specified in the text
///  - `CommandComplete`: For each fully-completed query
///  - `RowDescription`: For each query that returns data
///  - `DataRow`: For each row returned by a query
///  - `CopyOutResponse`: For each query that returns copy data
///  - `CopyData`: For each chunk of copy data returned by a query
///  - `CopyDone`: For each query that returns copy data
///  - `ErrorResponse`: For the first failed query
#[derive(Debug, Clone, Copy)]
struct QueryFlow<'a> {
    pub query: &'a str,
}

impl<'a> Flow for ParseFlow<'a> {
    fn to_vec(&self) -> Vec<u8> {
        let param_types: Vec<i32> = self
            .param_types
            .iter()
            .map(|oid| match oid {
                Oid::Unspecified => 0,
                Oid::Oid(n) => n.get() as i32,
            })
            .collect();
        builder::Parse {
            statement: self.name.0,
            query: self.query,
            param_types: &param_types,
        }
        .to_vec()
    }
}

impl<'a> Flow for BindFlow<'a> {
    fn to_vec(&self) -> Vec<u8> {
        let mut format_codes = Vec::with_capacity(self.params.len());
        let mut values = Vec::with_capacity(self.params.len());

        for param in self.params {
            match param {
                Param::Null => {
                    format_codes.push(0);
                    values.push(Encoded::Null);
                }
                Param::Text(value) => {
                    format_codes.push(0);
                    values.push(Encoded::Value(value.as_bytes()));
                }
                Param::Binary(value) => {
                    format_codes.push(1);
                    values.push(Encoded::Value(value));
                }
            }
        }

        let result_format_codes: Vec<i16> =
            self.result_format_codes.iter().map(|f| *f as i16).collect();

        builder::Bind {
            portal: self.portal.0,
            statement: self.statement.0,
            format_codes: &format_codes,
            values: &values,
            result_format_codes: &result_format_codes,
        }
        .to_vec()
    }
}

impl<'a> Flow for ExecuteFlow<'a> {
    fn to_vec(&self) -> Vec<u8> {
        let max_rows = match self.max_rows {
            MaxRows::Unlimited => 0,
            MaxRows::Limited(n) => n.get() as i32,
        };
        builder::Execute {
            portal: self.portal.0,
            max_rows,
        }
        .to_vec()
    }
}

impl<'a> Flow for DescribePortalFlow<'a> {
    fn to_vec(&self) -> Vec<u8> {
        builder::Describe {
            name: self.name.0,
            dtype: 'P' as _,
        }
        .to_vec()
    }
}

impl<'a> Flow for DescribeStatementFlow<'a> {
    fn to_vec(&self) -> Vec<u8> {
        builder::Describe {
            name: self.name.0,
            dtype: 'S' as _,
        }
        .to_vec()
    }
}

impl<'a> Flow for ClosePortalFlow<'a> {
    fn to_vec(&self) -> Vec<u8> {
        builder::Close {
            name: self.name.0,
            ctype: 'P' as _,
        }
        .to_vec()
    }
}

impl<'a> Flow for CloseStatementFlow<'a> {
    fn to_vec(&self) -> Vec<u8> {
        builder::Close {
            name: self.name.0,
            ctype: 'S' as _,
        }
        .to_vec()
    }
}

impl<'a> Flow for QueryFlow<'a> {
    fn to_vec(&self) -> Vec<u8> {
        builder::Query { query: self.query }.to_vec()
    }
}

pub(crate) enum MessageResult {
    Continue,
    Done,
    SkipUntilSync,
    Unknown,
}

pub(crate) trait MessageHandler {
    fn handle(&mut self, message: Message) -> MessageResult;
}

pub(crate) struct SyncMessageHandler;

impl MessageHandler for SyncMessageHandler {
    fn handle(&mut self, _: Message) -> MessageResult {
        MessageResult::Done
    }
}

impl<F> MessageHandler for F
where
    F: for<'a> FnMut(Message<'a>) -> MessageResult,
{
    fn handle(&mut self, message: Message) -> MessageResult {
        (self)(message)
    }
}

impl MessageHandler for Box<dyn MessageHandler> {
    fn handle(&mut self, message: Message) -> MessageResult {
        self.as_mut().handle(message)
    }
}

pub trait FlowWithSink {
    fn visit_flow(&self, f: impl FnMut(&dyn Flow));
    fn make_handler(self) -> Box<dyn MessageHandler>;
}

pub trait SimpleFlowSink {
    fn handle(&mut self, result: Result<(), ErrorResponse>);
}

impl SimpleFlowSink for () {
    fn handle(&mut self, _: Result<(), ErrorResponse>) {}
}

impl<F: for<'a> FnMut(Result<(), ErrorResponse>)> SimpleFlowSink for F {
    fn handle(&mut self, result: Result<(), ErrorResponse>) {
        (self)(result)
    }
}

impl<S: SimpleFlowSink + 'static> FlowWithSink for (ParseFlow<'_>, S) {
    fn visit_flow(&self, mut f: impl FnMut(&dyn Flow)) {
        f(&self.0);
    }
    fn make_handler(mut self) -> Box<dyn MessageHandler> {
        Box::new(move |message: Message<'_>| {
            if let Some(_) = ParseComplete::try_new(&message) {
                self.1.handle(Ok(()));
                return MessageResult::Done;
            }
            if let Some(msg) = ErrorResponse::try_new(&message) {
                self.1.handle(Err(msg));
                return MessageResult::SkipUntilSync;
            }
            return MessageResult::Unknown;
        })
    }
}

impl<S: SimpleFlowSink + 'static> FlowWithSink for (BindFlow<'_>, S) {
    fn visit_flow(&self, mut f: impl FnMut(&dyn Flow)) {
        f(&self.0);
    }
    fn make_handler(mut self) -> Box<dyn MessageHandler> {
        Box::new(move |message: Message<'_>| {
            if let Some(_) = BindComplete::try_new(&message) {
                self.1.handle(Ok(()));
                return MessageResult::Done;
            }
            if let Some(msg) = ErrorResponse::try_new(&message) {
                self.1.handle(Err(msg));
                return MessageResult::SkipUntilSync;
            }
            return MessageResult::Unknown;
        })
    }
}

impl<S: SimpleFlowSink + 'static> FlowWithSink for (ClosePortalFlow<'_>, S) {
    fn visit_flow(&self, mut f: impl FnMut(&dyn Flow)) {
        f(&self.0);
    }
    fn make_handler(mut self) -> Box<dyn MessageHandler> {
        Box::new(move |message: Message<'_>| {
            if let Some(_) = CloseComplete::try_new(&message) {
                self.1.handle(Ok(()));
                return MessageResult::Done;
            }
            if let Some(msg) = ErrorResponse::try_new(&message) {
                self.1.handle(Err(msg));
                return MessageResult::SkipUntilSync;
            }
            return MessageResult::Unknown;
        })
    }
}

impl<S: SimpleFlowSink + 'static> FlowWithSink for (CloseStatementFlow<'_>, S) {
    fn visit_flow(&self, mut f: impl FnMut(&dyn Flow)) {
        f(&self.0);
    }
    fn make_handler(mut self) -> Box<dyn MessageHandler> {
        Box::new(move |message: Message<'_>| {
            if let Some(_) = CloseComplete::try_new(&message) {
                self.1.handle(Ok(()));
                return MessageResult::Done;
            }
            if let Some(msg) = ErrorResponse::try_new(&message) {
                self.1.handle(Err(msg));
                return MessageResult::Done;
            }
            return MessageResult::Unknown;
        })
    }
}

impl<S: ExecuteSink + 'static> FlowWithSink for (ExecuteFlow<'_>, S) {
    fn visit_flow(&self, mut f: impl FnMut(&dyn Flow)) {
        f(&self.0);
    }
    fn make_handler(self) -> Box<dyn MessageHandler> {
        Box::new(ExecuteMessageHandler {
            sink: self.1,
            data: None,
            copy: None,
        })
    }
}

impl<S: QuerySink + 'static> FlowWithSink for (QueryFlow<'_>, S) {
    fn visit_flow(&self, mut f: impl FnMut(&dyn Flow)) {
        f(&self.0);
    }
    fn make_handler(self) -> Box<dyn MessageHandler> {
        Box::new(QueryMessageHandler {
            sink: self.1,
            data: None,
            copy: None,
        })
    }
}

pub trait ExecuteSink {
    type Output: DataSink;
    type CopyOutput: CopyDataSink;

    fn rows(&mut self) -> Self::Output;
    fn copy(&mut self, copy: CopyOutResponse) -> Self::CopyOutput;
    fn suspended(&mut self, _suspended: PortalSuspended) {}
    fn complete(&mut self, _complete: CommandComplete) {}
    fn error(&mut self, error: ErrorResponse);

    fn protocol_violation(&mut self, message: Message, hint: &'static str) {
        warn!("Protocol violation: {message:?} ({hint})");
    }
}

impl ExecuteSink for () {
    type Output = ();
    type CopyOutput = ();
    fn rows(&mut self) -> () {}
    fn copy(&mut self, _: CopyOutResponse) -> () {}
    fn error(&mut self, _: ErrorResponse) {}
}

/// A sink capable of handling standard query and COPY (out direction) messages.
pub trait QuerySink {
    type Output: DataSink;
    type CopyOutput: CopyDataSink;

    fn rows(&mut self, rows: RowDescription) -> Self::Output;
    fn copy(&mut self, copy: CopyOutResponse) -> Self::CopyOutput;
    fn complete(&mut self, _complete: CommandComplete) {}
    fn error(&mut self, error: ErrorResponse);

    fn protocol_violation(&mut self, message: Message, hint: &'static str) {
        warn!("Protocol violation: {message:?} ({hint})");
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DoneHandling {
    Handled,
    RedirectToParent,
}

pub trait DataSink {
    /// Sink a row of data.
    fn row(&mut self, values: DataRow);
    /// Handle the completion of a command. If unimplemented, will be redirected to the parent.
    #[must_use]
    fn done(&mut self, _result: Result<CommandComplete, ErrorResponse>) -> DoneHandling {
        DoneHandling::RedirectToParent
    }
}

pub trait CopyDataSink {
    /// Sink a chunk of COPY data.
    fn data(&mut self, values: CopyData);
    /// Handle the completion of a COPY operation. If unimplemented, will be redirected to the parent.
    #[must_use]
    fn done(&mut self, _result: Result<CommandComplete, ErrorResponse>) -> DoneHandling {
        DoneHandling::RedirectToParent
    }
}

impl<Q> QuerySink for Box<Q>
where
    Q: QuerySink + 'static,
{
    type Output = Box<dyn DataSink + 'static>;
    type CopyOutput = Box<dyn CopyDataSink + 'static>;
    fn rows(&mut self, rows: RowDescription) -> Self::Output {
        Box::new(self.as_mut().rows(rows))
    }
    fn copy(&mut self, copy: CopyOutResponse) -> Self::CopyOutput {
        Box::new(self.as_mut().copy(copy))
    }
    fn complete(&mut self, _complete: CommandComplete) {
        self.as_mut().complete(_complete)
    }
    fn protocol_violation(&mut self, message: Message, hint: &'static str) {
        self.as_mut().protocol_violation(message, hint)
    }
    fn error(&mut self, error: ErrorResponse) {
        self.as_mut().error(error)
    }
}

impl QuerySink for () {
    type Output = ();
    type CopyOutput = ();
    fn rows(&mut self, _: RowDescription) -> () {}
    fn copy(&mut self, _: CopyOutResponse) -> () {}
    fn error(&mut self, _: ErrorResponse) {}
}

impl<F1, F2, S> QuerySink for (F1, F2)
where
    F1: for<'a> FnMut(RowDescription<'a>) -> S,
    F2: for<'a> FnMut(ErrorResponse<'a>),
    S: DataSink,
{
    type Output = S;
    type CopyOutput = ();
    fn rows(&mut self, rows: RowDescription) -> S {
        (self.0)(rows)
    }
    fn copy(&mut self, _: CopyOutResponse) -> () {
        ()
    }
    fn error(&mut self, error: ErrorResponse) {
        (self.1)(error)
    }
}

impl<F1, F2, F3, S, T> QuerySink for (F1, F2, F3)
where
    F1: for<'a> FnMut(RowDescription<'a>) -> S,
    F2: for<'a> FnMut(CopyOutResponse<'a>) -> T,
    F3: for<'a> FnMut(ErrorResponse<'a>),
    S: DataSink,
    T: CopyDataSink,
{
    type Output = S;
    type CopyOutput = T;
    fn rows(&mut self, rows: RowDescription) -> S {
        (self.0)(rows)
    }
    fn copy(&mut self, copy: CopyOutResponse) -> T {
        (self.1)(copy)
    }
    fn error(&mut self, error: ErrorResponse) {
        (self.2)(error)
    }
}

impl DataSink for () {
    fn row(&mut self, _: DataRow) {}
}

impl<F> DataSink for F
where
    F: for<'a> Fn(DataRow<'a>),
{
    fn row(&mut self, values: DataRow) {
        (self)(values)
    }
}

impl DataSink for Box<dyn DataSink> {
    fn row(&mut self, values: DataRow) {
        self.as_mut().row(values)
    }
    fn done(&mut self, result: Result<CommandComplete, ErrorResponse>) -> DoneHandling {
        self.as_mut().done(result)
    }
}

impl CopyDataSink for () {
    fn data(&mut self, _: CopyData) {}
}

impl<F> CopyDataSink for F
where
    F: for<'a> FnMut(CopyData<'a>),
{
    fn data(&mut self, values: CopyData) {
        (self)(values)
    }
}

impl CopyDataSink for Box<dyn CopyDataSink> {
    fn data(&mut self, values: CopyData) {
        self.as_mut().data(values)
    }
    fn done(&mut self, result: Result<CommandComplete, ErrorResponse>) -> DoneHandling {
        self.as_mut().done(result)
    }
}

pub(crate) struct ExecuteMessageHandler<Q: ExecuteSink> {
    pub sink: Q,
    pub data: Option<Q::Output>,
    pub copy: Option<Q::CopyOutput>,
}

impl<Q: ExecuteSink> MessageHandler for ExecuteMessageHandler<Q> {
    fn handle(&mut self, message: Message) -> MessageResult {
        match_message!(Ok(message), Backend {
            (CopyOutResponse as copy) => {
                let sink = std::mem::replace(&mut self.copy, Some(self.sink.copy(copy)));
                if sink.is_some() {
                    self.sink.protocol_violation(message, "copy sink exists");
                }
            },
            (CopyData as data) => {
                if let Some(sink) = &mut self.copy {
                    sink.data(data);
                } else {
                    self.sink.protocol_violation(message, "copy sink does not exist");
                }
            },
            (CopyDone) => {
                if self.copy.is_none() {
                    self.sink.protocol_violation(message, "copy sink does not exist");
                }
            },

            (DataRow as row) => {
                if self.data.is_none() {
                    self.data = Some(self.sink.rows());
                }
                let Some(sink) = &mut self.data else {
                    unreachable!()
                };
                sink.row(row)
            },
            (PortalSuspended) => {
                // self.sink.suspended(message);
                return MessageResult::Done;
            },
            (CommandComplete as complete) => {
                let sink = std::mem::take(&mut self.data);
                if let Some(mut sink) = sink {
                    if sink.done(Ok(complete)) == DoneHandling::RedirectToParent {
                        self.sink.complete(complete);
                    }
                } else {
                    let sink = std::mem::take(&mut self.copy);
                    if let Some(mut sink) = sink {
                        if sink.done(Ok(complete)) == DoneHandling::RedirectToParent {
                            self.sink.complete(complete);
                        }
                    } else {
                        self.sink.complete(complete);
                    }
                }
                return MessageResult::Done;
            },

            (ErrorResponse as err) => {
                return MessageResult::SkipUntilSync;
            },

            _unknown => {
                self.sink.protocol_violation(message, "unknown message");
            }
        });
        MessageResult::Continue
    }
}

pub(crate) struct QueryMessageHandler<Q: QuerySink> {
    pub sink: Q,
    pub data: Option<Q::Output>,
    pub copy: Option<Q::CopyOutput>,
}

impl<Q: QuerySink> MessageHandler for QueryMessageHandler<Q> {
    fn handle(&mut self, message: Message) -> MessageResult {
        match_message!(Ok(message), Backend {
            (CopyOutResponse as copy) => {
                let sink = std::mem::replace(&mut self.copy, Some(self.sink.copy(copy)));
                if sink.is_some() {
                    self.sink.protocol_violation(message, "copy sink exists");
                }
            },
            (CopyData as data) => {
                if let Some(sink) = &mut self.copy {
                    sink.data(data);
                } else {
                    self.sink.protocol_violation(message, "copy sink does not exist");
                }
            },
            (CopyDone) => {
                if self.copy.is_none() {
                    self.sink.protocol_violation(message, "copy sink does not exist");
                }
            },

            (RowDescription as row) => {
                let sink = std::mem::replace(&mut self.data, Some(self.sink.rows(row)));
                if sink.is_some() {
                    self.sink.protocol_violation(message, "data sink exists");
                }
            },
            (DataRow as row) => {
                if let Some(sink) = &mut self.data {
                    sink.row(row)
                } else {
                    self.sink.protocol_violation(message, "data sink does not exist");
                }
            },
            (CommandComplete as complete) => {
                let sink = std::mem::take(&mut self.data);
                if let Some(mut sink) = sink {
                    if sink.done(Ok(complete)) == DoneHandling::RedirectToParent {
                        self.sink.complete(complete);
                    }
                } else {
                    let sink = std::mem::take(&mut self.copy);
                    if let Some(mut sink) = sink {
                        if sink.done(Ok(complete)) == DoneHandling::RedirectToParent {
                            self.sink.complete(complete);
                        }
                    } else {
                        self.sink.complete(complete);
                    }
                }
            },

            (EmptyQueryResponse) => {
                // Equivalent to CommandComplete, but no data was provided
                let sink = std::mem::take(&mut self.data);
                if sink.is_some() {
                    self.sink.protocol_violation(message, "data sink exists");
                } else {
                    let sink = std::mem::take(&mut self.copy);
                    if sink.is_some() {
                        self.sink.protocol_violation(message, "copy sink exists");
                    }
                }
            },

            (ErrorResponse as err) => {
                // Depending on the state of the sink, we direct the error to
                // the appropriate handler.
                if let Some(mut sink) = std::mem::take(&mut self.data) {
                    if sink.done(Err(err)) == DoneHandling::RedirectToParent {
                        self.sink.error(err);
                    }
                } else if let Some(mut sink) = std::mem::take(&mut self.copy) {
                    if sink.done(Err(err)) == DoneHandling::RedirectToParent {
                        self.sink.error(err);
                    }
                } else {
                    self.sink.error(err);
                }
            },

            (ReadyForQuery) => {
                // All operations are complete at this point.
                if std::mem::take(&mut self.data).is_some() || std::mem::take(&mut self.copy).is_some() {
                    self.sink.protocol_violation(message, "sink exists");
                }
                return MessageResult::Done;
            },

            _unknown => {
                self.sink.protocol_violation(message, "unknown message");
            }
        });
        MessageResult::Continue
    }
}

#[derive(Default)]
pub struct PipelineBuilder {
    handlers: Vec<Box<dyn MessageHandler>>,
    messages: Vec<u8>,
}

impl PipelineBuilder {
    fn push_flow_with_sink(mut self, flow: impl FlowWithSink) -> Self {
        flow.visit_flow(|flow| self.messages.extend_from_slice(&flow.to_vec()));
        self.handlers.push(flow.make_handler());
        self
    }

    pub fn bind(
        mut self,
        portal: Portal,
        statement: Statement,
        params: &[Param],
        result_format_codes: &[Format],
        handler: impl SimpleFlowSink + 'static,
    ) -> Self {
        self.push_flow_with_sink((
            BindFlow {
                portal,
                statement,
                params,
                result_format_codes,
            },
            handler,
        ))
    }

    pub fn parse(
        mut self,
        name: Statement,
        query: &str,
        param_types: &[Oid],
        handler: impl SimpleFlowSink + 'static,
    ) -> Self {
        self.push_flow_with_sink((
            ParseFlow {
                name,
                query,
                param_types,
            },
            handler,
        ))
    }

    pub fn execute(
        mut self,
        portal: Portal,
        max_rows: MaxRows,
        handler: impl ExecuteSink + 'static,
    ) -> Self {
        self.push_flow_with_sink((ExecuteFlow { portal, max_rows }, handler))
    }

    pub fn close_portal(mut self, name: Portal, handler: impl SimpleFlowSink + 'static) -> Self {
        self.push_flow_with_sink((ClosePortalFlow { name }, handler))
    }

    pub fn close_statement(
        mut self,
        name: Statement,
        handler: impl SimpleFlowSink + 'static,
    ) -> Self {
        self.push_flow_with_sink((CloseStatementFlow { name }, handler))
    }

    /// Add a query flow to the pipeline.
    ///
    /// Note that if a query fails, the pipeline will continue executing until it
    /// completes or a non-query pipeline element fails. If a previous non-query
    /// element of this pipeline failed, the query will not be executed.
    pub fn query(mut self, query: &str, handler: impl QuerySink + 'static) -> Self {
        self.push_flow_with_sink((QueryFlow { query }, handler))
    }

    pub fn build(self) -> Pipeline {
        Pipeline {
            handlers: self.handlers,
            messages: self.messages,
        }
    }
}

pub struct Pipeline {
    pub(crate) handlers: Vec<Box<dyn MessageHandler>>,
    pub(crate) messages: Vec<u8>,
}
