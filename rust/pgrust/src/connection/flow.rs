//! Postgres flow notes:
//!
//! <https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-PIPELINING>
//!
//! <https://segmentfault.com/a/1190000017136059>
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

use std::{cell::RefCell, num::NonZeroU32, rc::Rc};

use crate::protocol::postgres::{
    builder,
    data::{
        BindComplete, CloseComplete, CommandComplete, CopyData, CopyDone, CopyOutResponse, DataRow,
        EmptyQueryResponse, ErrorResponse, Message, NoData, NoticeResponse, ParameterDescription,
        ParseComplete, PortalSuspended, ReadyForQuery, RowDescription,
    },
};
use db_proto::{match_message, Encoded};

#[derive(Debug, Clone, Copy)]
pub enum Param<'a> {
    Null,
    Text(&'a str),
    Binary(&'a [u8]),
}

#[derive(Debug, Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
#[repr(transparent)]
pub struct Oid(u32);

impl Oid {
    pub fn unspecified() -> Self {
        Self(0)
    }

    pub fn from(oid: NonZeroU32) -> Self {
        Self(oid.get())
    }
}

#[derive(Debug, Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
#[repr(transparent)]
pub struct Format(i16);

impl Format {
    pub fn text() -> Self {
        Self(0)
    }

    pub fn binary() -> Self {
        Self(1)
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(i32)]
pub enum MaxRows {
    Unlimited,
    Limited(NonZeroU32),
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Portal<'a>(pub &'a str);

#[derive(Debug, Clone, Copy, Default)]
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
        let param_types = bytemuck::cast_slice(self.param_types);
        builder::Parse {
            statement: self.name.0,
            query: self.query,
            param_types,
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

        let result_format_codes = bytemuck::cast_slice(self.result_format_codes);

        builder::Bind {
            portal: self.portal.0,
            statement: self.statement.0,
            format_codes: &format_codes,
            values: &values,
            result_format_codes,
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
            dtype: b'P',
        }
        .to_vec()
    }
}

impl<'a> Flow for DescribeStatementFlow<'a> {
    fn to_vec(&self) -> Vec<u8> {
        builder::Describe {
            name: self.name.0,
            dtype: b'S',
        }
        .to_vec()
    }
}

impl<'a> Flow for ClosePortalFlow<'a> {
    fn to_vec(&self) -> Vec<u8> {
        builder::Close {
            name: self.name.0,
            ctype: b'P',
        }
        .to_vec()
    }
}

impl<'a> Flow for CloseStatementFlow<'a> {
    fn to_vec(&self) -> Vec<u8> {
        builder::Close {
            name: self.name.0,
            ctype: b'S',
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
    UnexpectedState { complaint: &'static str },
}

pub(crate) trait MessageHandler {
    fn handle(&mut self, message: Message) -> MessageResult;
    fn name(&self) -> &'static str;
    fn is_sync(&self) -> bool {
        false
    }
}

pub(crate) struct SyncMessageHandler;

impl MessageHandler for SyncMessageHandler {
    fn handle(&mut self, message: Message) -> MessageResult {
        if ReadyForQuery::try_new(&message).is_some() {
            return MessageResult::Done;
        }
        MessageResult::Unknown
    }
    fn name(&self) -> &'static str {
        "Sync"
    }
    fn is_sync(&self) -> bool {
        true
    }
}

impl<F> MessageHandler for (&'static str, F)
where
    F: for<'a> FnMut(Message<'a>) -> MessageResult,
{
    fn handle(&mut self, message: Message) -> MessageResult {
        (self.1)(message)
    }
    fn name(&self) -> &'static str {
        self.0
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
        Box::new(("Parse", move |message: Message<'_>| {
            if ParseComplete::try_new(&message).is_some() {
                self.1.handle(Ok(()));
                return MessageResult::Done;
            }
            if let Some(msg) = ErrorResponse::try_new(&message) {
                self.1.handle(Err(msg));
                return MessageResult::SkipUntilSync;
            }
            MessageResult::Unknown
        }))
    }
}

impl<S: SimpleFlowSink + 'static> FlowWithSink for (BindFlow<'_>, S) {
    fn visit_flow(&self, mut f: impl FnMut(&dyn Flow)) {
        f(&self.0);
    }
    fn make_handler(mut self) -> Box<dyn MessageHandler> {
        Box::new(("Bind", move |message: Message<'_>| {
            if BindComplete::try_new(&message).is_some() {
                self.1.handle(Ok(()));
                return MessageResult::Done;
            }
            if let Some(msg) = ErrorResponse::try_new(&message) {
                self.1.handle(Err(msg));
                return MessageResult::SkipUntilSync;
            }
            MessageResult::Unknown
        }))
    }
}

impl<S: SimpleFlowSink + 'static> FlowWithSink for (ClosePortalFlow<'_>, S) {
    fn visit_flow(&self, mut f: impl FnMut(&dyn Flow)) {
        f(&self.0);
    }
    fn make_handler(mut self) -> Box<dyn MessageHandler> {
        Box::new(("ClosePortal", move |message: Message<'_>| {
            if CloseComplete::try_new(&message).is_some() {
                self.1.handle(Ok(()));
                return MessageResult::Done;
            }
            if let Some(msg) = ErrorResponse::try_new(&message) {
                self.1.handle(Err(msg));
                return MessageResult::SkipUntilSync;
            }
            MessageResult::Unknown
        }))
    }
}

impl<S: SimpleFlowSink + 'static> FlowWithSink for (CloseStatementFlow<'_>, S) {
    fn visit_flow(&self, mut f: impl FnMut(&dyn Flow)) {
        f(&self.0);
    }
    fn make_handler(mut self) -> Box<dyn MessageHandler> {
        Box::new(("CloseStatement", move |message: Message<'_>| {
            if CloseComplete::try_new(&message).is_some() {
                self.1.handle(Ok(()));
                return MessageResult::Done;
            }
            if let Some(msg) = ErrorResponse::try_new(&message) {
                self.1.handle(Err(msg));
                return MessageResult::Done;
            }
            MessageResult::Unknown
        }))
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

impl<S: DescribeSink + 'static> FlowWithSink for (DescribePortalFlow<'_>, S) {
    fn visit_flow(&self, mut f: impl FnMut(&dyn Flow)) {
        f(&self.0);
    }
    fn make_handler(self) -> Box<dyn MessageHandler> {
        Box::new(DescribeMessageHandler { sink: self.1 })
    }
}

impl<S: DescribeSink + 'static> FlowWithSink for (DescribeStatementFlow<'_>, S) {
    fn visit_flow(&self, mut f: impl FnMut(&dyn Flow)) {
        f(&self.0);
    }
    fn make_handler(self) -> Box<dyn MessageHandler> {
        Box::new(DescribeMessageHandler { sink: self.1 })
    }
}

pub trait DescribeSink {
    fn params(&mut self, params: ParameterDescription);
    fn rows(&mut self, rows: RowDescription);
    fn error(&mut self, error: ErrorResponse);
}

impl DescribeSink for () {
    fn params(&mut self, _: ParameterDescription) {}
    fn rows(&mut self, _: RowDescription) {}
    fn error(&mut self, _: ErrorResponse) {}
}

impl<F> DescribeSink for F
where
    F: for<'a> FnMut(RowDescription<'a>),
{
    fn rows(&mut self, rows: RowDescription) {
        (self)(rows)
    }
    fn params(&mut self, _params: ParameterDescription) {}
    fn error(&mut self, _error: ErrorResponse) {}
}

impl<F1, F2> DescribeSink for (F1, F2)
where
    F1: for<'a> FnMut(ParameterDescription<'a>),
    F2: for<'a> FnMut(RowDescription<'a>),
{
    fn params(&mut self, params: ParameterDescription) {
        (self.0)(params)
    }
    fn rows(&mut self, rows: RowDescription) {
        (self.1)(rows)
    }
    fn error(&mut self, _error: ErrorResponse) {}
}

struct DescribeMessageHandler<S: DescribeSink> {
    sink: S,
}

impl<S: DescribeSink> MessageHandler for DescribeMessageHandler<S> {
    fn name(&self) -> &'static str {
        "Describe"
    }
    fn handle(&mut self, message: Message) -> MessageResult {
        match_message!(Ok(message), Backend {
            (ParameterDescription as params) => {
                self.sink.params(params);
                return MessageResult::Continue;
            },
            (RowDescription as rows) => {
                self.sink.rows(rows);
                return MessageResult::Done;
            },
            (NoData) => {
                return MessageResult::Done;
            },
            (ErrorResponse as err) => {
                self.sink.error(err);
                return MessageResult::SkipUntilSync;
            },
            _unknown => {
                return MessageResult::Unknown;
            }
        })
    }
}

pub trait ExecuteSink {
    type Output: ExecuteDataSink;
    type CopyOutput: CopyDataSink;

    fn rows(&mut self) -> Self::Output;
    fn copy(&mut self, copy: CopyOutResponse) -> Self::CopyOutput;
    fn complete(&mut self, _complete: ExecuteCompletion) {}
    fn notice(&mut self, _: NoticeResponse) {}
    fn error(&mut self, error: ErrorResponse);
}

pub enum ExecuteCompletion<'a> {
    PortalSuspended(PortalSuspended<'a>),
    CommandComplete(CommandComplete<'a>),
}

impl ExecuteSink for () {
    type Output = ();
    type CopyOutput = ();
    fn rows(&mut self) {}
    fn copy(&mut self, _: CopyOutResponse) {}
    fn error(&mut self, _: ErrorResponse) {}
}

impl<F1, F2, S> ExecuteSink for (F1, F2)
where
    F1: for<'a> FnMut() -> S,
    F2: for<'a> FnMut(ErrorResponse<'a>),
    S: ExecuteDataSink,
{
    type Output = S;
    type CopyOutput = ();
    fn rows(&mut self) -> S {
        (self.0)()
    }
    fn copy(&mut self, _: CopyOutResponse) {}
    fn error(&mut self, error: ErrorResponse) {
        (self.1)(error)
    }
}

impl<F1, F2, F3, S, T> ExecuteSink for (F1, F2, F3)
where
    F1: for<'a> FnMut() -> S,
    F2: for<'a> FnMut(CopyOutResponse<'a>) -> T,
    F3: for<'a> FnMut(ErrorResponse<'a>),
    S: ExecuteDataSink,
    T: CopyDataSink,
{
    type Output = S;
    type CopyOutput = T;
    fn rows(&mut self) -> S {
        (self.0)()
    }
    fn copy(&mut self, copy: CopyOutResponse) -> T {
        (self.1)(copy)
    }
    fn error(&mut self, error: ErrorResponse) {
        (self.2)(error)
    }
}

pub trait ExecuteDataSink {
    /// Sink a row of data.
    fn row(&mut self, values: DataRow);
    /// Handle the completion of a command. If unimplemented, will be redirected to the parent.
    #[must_use]
    fn done(&mut self, _result: Result<ExecuteCompletion, ErrorResponse>) -> DoneHandling {
        DoneHandling::RedirectToParent
    }
}

impl ExecuteDataSink for () {
    fn row(&mut self, _: DataRow) {}
}

impl<F> ExecuteDataSink for F
where
    F: for<'a> Fn(DataRow<'a>),
{
    fn row(&mut self, values: DataRow) {
        (self)(values)
    }
}

/// A sink capable of handling standard query and COPY (out direction) messages.
pub trait QuerySink {
    type Output: DataSink;
    type CopyOutput: CopyDataSink;

    fn rows(&mut self, rows: RowDescription) -> Self::Output;
    fn copy(&mut self, copy: CopyOutResponse) -> Self::CopyOutput;
    fn complete(&mut self, _complete: CommandComplete) {}
    fn notice(&mut self, _: NoticeResponse) {}
    fn error(&mut self, error: ErrorResponse);
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
    fn error(&mut self, error: ErrorResponse) {
        self.as_mut().error(error)
    }
}

impl QuerySink for () {
    type Output = ();
    type CopyOutput = ();
    fn rows(&mut self, _: RowDescription) {}
    fn copy(&mut self, _: CopyOutResponse) {}
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
    fn copy(&mut self, _: CopyOutResponse) {}
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
    fn name(&self) -> &'static str {
        "Execute"
    }
    fn handle(&mut self, message: Message) -> MessageResult {
        match_message!(Ok(message), Backend {
            (CopyOutResponse as copy) => {
                let sink = std::mem::replace(&mut self.copy, Some(self.sink.copy(copy)));
                if sink.is_some() {
                    return MessageResult::UnexpectedState { complaint: "copy sink exists" };
                }
            },
            (CopyData as data) => {
                if let Some(sink) = &mut self.copy {
                    sink.data(data);
                } else {
                    return MessageResult::UnexpectedState { complaint: "copy sink does not exist" };
                }
            },
            (CopyDone) => {
                if self.copy.is_none() {
                    return MessageResult::UnexpectedState { complaint: "copy sink does not exist" };
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
            (PortalSuspended as complete) => {
                if let Some(mut sink) = std::mem::take(&mut self.data) {
                    if sink.done(Ok(ExecuteCompletion::PortalSuspended(complete))) == DoneHandling::RedirectToParent {
                        self.sink.complete(ExecuteCompletion::PortalSuspended(complete));
                    }
                } else {
                    return MessageResult::UnexpectedState { complaint: "data sink does not exist" };
                }
                return MessageResult::Done;
            },
            (CommandComplete as complete) => {
                if let Some(mut sink) = std::mem::take(&mut self.copy) {
                    // If COPY has started, route this to the COPY sink.
                    if sink.done(Ok(complete)) == DoneHandling::RedirectToParent {
                        self.sink.complete(ExecuteCompletion::CommandComplete(complete));
                    }
                } else if let Some(mut sink) = std::mem::take(&mut self.data) {
                    // If data has started, route this to the data sink.
                    if sink.done(Ok(ExecuteCompletion::CommandComplete(complete))) == DoneHandling::RedirectToParent {
                        self.sink.complete(ExecuteCompletion::CommandComplete(complete));
                    }
                } else {
                    // Otherwise, create a new data sink and route to there.
                    if self.sink.rows().done(Ok(ExecuteCompletion::CommandComplete(complete))) == DoneHandling::RedirectToParent {
                        self.sink.complete(ExecuteCompletion::CommandComplete(complete));
                    }
                }
                return MessageResult::Done;
            },
            (EmptyQueryResponse) => {
                // TODO: This should be exposed to the sink
                return MessageResult::Done;
            },

            (ErrorResponse as err) => {
                if let Some(mut sink) = std::mem::take(&mut self.copy) {
                    // If COPY has started, route this to the COPY sink.
                    if sink.done(Err(err)) == DoneHandling::RedirectToParent {
                        self.sink.error(err);
                    }
                } else if let Some(mut sink) = std::mem::take(&mut self.data) {
                    // If data has started, route this to the data sink.
                    if sink.done(Err(err)) == DoneHandling::RedirectToParent {
                        self.sink.error(err);
                    }
                } else {
                    // Otherwise, create a new data sink and route to there.
                    if self.sink.rows().done(Err(err)) == DoneHandling::RedirectToParent {
                        self.sink.error(err);
                    }
                }

                return MessageResult::SkipUntilSync;
            },
            (NoticeResponse as notice) => {
                self.sink.notice(notice);
            },

            _unknown => {
                return MessageResult::Unknown;
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
    fn name(&self) -> &'static str {
        "Query"
    }
    fn handle(&mut self, message: Message) -> MessageResult {
        match_message!(Ok(message), Backend {
            (CopyOutResponse as copy) => {
                let sink = std::mem::replace(&mut self.copy, Some(self.sink.copy(copy)));
                if sink.is_some() {
                    return MessageResult::UnexpectedState { complaint: "copy sink exists" };
                }
            },
            (CopyData as data) => {
                if let Some(sink) = &mut self.copy {
                    sink.data(data);
                } else {
                    return MessageResult::UnexpectedState { complaint: "copy sink does not exist" };
                }
            },
            (CopyDone) => {
                if self.copy.is_none() {
                    return MessageResult::UnexpectedState { complaint: "copy sink does not exist" };
                }
            },

            (RowDescription as row) => {
                let sink = std::mem::replace(&mut self.data, Some(self.sink.rows(row)));
                if sink.is_some() {
                    return MessageResult::UnexpectedState { complaint: "data sink exists" };
                }
            },
            (DataRow as row) => {
                if let Some(sink) = &mut self.data {
                    sink.row(row)
                } else {
                    return MessageResult::UnexpectedState { complaint: "data sink does not exist" };
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
                    return MessageResult::UnexpectedState { complaint: "data sink exists" };
                } else {
                    let sink = std::mem::take(&mut self.copy);
                    if sink.is_some() {
                        return MessageResult::UnexpectedState { complaint: "copy sink exists" };
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
                    // Top level errors must complete this operation
                    self.sink.error(err);
                }
            },
            (NoticeResponse as notice) => {
                self.sink.notice(notice);
            },

            (ReadyForQuery) => {
                // All operations are complete at this point.
                if std::mem::take(&mut self.data).is_some() || std::mem::take(&mut self.copy).is_some() {
                    return MessageResult::UnexpectedState { complaint: "sink exists" };
                }
                return MessageResult::Done;
            },

            _unknown => {
                return MessageResult::Unknown;
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

    /// Add a bind flow to the pipeline.
    pub fn bind(
        self,
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

    /// Add a parse flow to the pipeline.
    pub fn parse(
        self,
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

    /// Add an execute flow to the pipeline.
    ///
    /// Note that this may be a COPY statement. In that case, the description of the portal
    /// will not show any data returned, and this will use the `CopySink` of the provided
    /// sink. In addition, COPY operations do not respect the `max_rows` parameter.
    pub fn execute(
        self,
        portal: Portal,
        max_rows: MaxRows,
        handler: impl ExecuteSink + 'static,
    ) -> Self {
        self.push_flow_with_sink((ExecuteFlow { portal, max_rows }, handler))
    }

    /// Add a close portal flow to the pipeline.
    pub fn close_portal(self, name: Portal, handler: impl SimpleFlowSink + 'static) -> Self {
        self.push_flow_with_sink((ClosePortalFlow { name }, handler))
    }

    /// Add a close statement flow to the pipeline.
    pub fn close_statement(self, name: Statement, handler: impl SimpleFlowSink + 'static) -> Self {
        self.push_flow_with_sink((CloseStatementFlow { name }, handler))
    }

    /// Add a describe portal flow to the pipeline. Note that this will describe
    /// both parameters and rows.
    pub fn describe_portal(self, name: Portal, handler: impl DescribeSink + 'static) -> Self {
        self.push_flow_with_sink((DescribePortalFlow { name }, handler))
    }

    /// Add a describe statement flow to the pipeline. Note that this will describe
    /// only the rows of the portal.
    pub fn describe_statement(self, name: Statement, handler: impl DescribeSink + 'static) -> Self {
        self.push_flow_with_sink((DescribeStatementFlow { name }, handler))
    }

    /// Add a query flow to the pipeline.
    ///
    /// Note that if a query fails, the pipeline will continue executing until it
    /// completes or a non-query pipeline element fails. If a previous non-query
    /// element of this pipeline failed, the query will not be executed.
    pub fn query(self, query: &str, handler: impl QuerySink + 'static) -> Self {
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

#[derive(Default)]
/// Accumulate raw messages from a flow. Useful mainly for testing.
pub struct FlowAccumulator {
    data: Vec<u8>,
    messages: Vec<usize>,
}

impl FlowAccumulator {
    pub fn push(&mut self, message: impl AsRef<[u8]>) {
        self.messages.push(self.data.len());
        self.data.extend_from_slice(message.as_ref());
    }

    pub fn with_messages(&self, mut f: impl FnMut(Message)) {
        for &offset in &self.messages {
            // First get the message header
            let message = Message::new(&self.data[offset..]).unwrap();
            let len = message.mlen();
            // Then resize the message to the correct length
            let message = Message::new(&self.data[offset..offset + len + 1]).unwrap();
            f(message);
        }
    }
}

impl QuerySink for Rc<RefCell<FlowAccumulator>> {
    type Output = Self;
    type CopyOutput = Self;
    fn rows(&mut self, message: RowDescription) -> Self {
        self.borrow_mut().push(message);
        self.clone()
    }
    fn copy(&mut self, message: CopyOutResponse) -> Self {
        self.borrow_mut().push(message);
        self.clone()
    }
    fn error(&mut self, message: ErrorResponse) {
        self.borrow_mut().push(message);
    }
    fn complete(&mut self, complete: CommandComplete) {
        self.borrow_mut().push(complete);
    }
    fn notice(&mut self, message: NoticeResponse) {
        self.borrow_mut().push(message);
    }
}

impl ExecuteSink for Rc<RefCell<FlowAccumulator>> {
    type Output = Self;
    type CopyOutput = Self;

    fn rows(&mut self) -> Self {
        self.clone()
    }
    fn copy(&mut self, message: CopyOutResponse) -> Self {
        self.borrow_mut().push(message);
        self.clone()
    }
    fn error(&mut self, message: ErrorResponse) {
        self.borrow_mut().push(message);
    }
    fn complete(&mut self, complete: ExecuteCompletion) {
        match complete {
            ExecuteCompletion::PortalSuspended(suspended) => self.borrow_mut().push(suspended),
            ExecuteCompletion::CommandComplete(complete) => self.borrow_mut().push(complete),
        }
    }
    fn notice(&mut self, message: NoticeResponse) {
        self.borrow_mut().push(message);
    }
}

impl DataSink for Rc<RefCell<FlowAccumulator>> {
    fn row(&mut self, message: DataRow) {
        self.borrow_mut().push(message);
    }
    fn done(&mut self, result: Result<CommandComplete, ErrorResponse>) -> DoneHandling {
        match result {
            Ok(complete) => self.borrow_mut().push(complete),
            Err(err) => self.borrow_mut().push(err),
        };
        DoneHandling::Handled
    }
}

impl ExecuteDataSink for Rc<RefCell<FlowAccumulator>> {
    fn row(&mut self, message: DataRow) {
        self.borrow_mut().push(message);
    }
    fn done(&mut self, result: Result<ExecuteCompletion, ErrorResponse>) -> DoneHandling {
        match result {
            Ok(ExecuteCompletion::PortalSuspended(suspended)) => self.borrow_mut().push(suspended),
            Ok(ExecuteCompletion::CommandComplete(complete)) => self.borrow_mut().push(complete),
            Err(err) => self.borrow_mut().push(err),
        };
        DoneHandling::Handled
    }
}

impl CopyDataSink for Rc<RefCell<FlowAccumulator>> {
    fn data(&mut self, message: CopyData) {
        self.borrow_mut().push(message);
    }
    fn done(&mut self, result: Result<CommandComplete, ErrorResponse>) -> DoneHandling {
        match result {
            Ok(complete) => self.borrow_mut().push(complete),
            Err(err) => self.borrow_mut().push(err),
        };
        DoneHandling::Handled
    }
}

impl SimpleFlowSink for Rc<RefCell<FlowAccumulator>> {
    fn handle(&mut self, result: Result<(), ErrorResponse>) {
        match result {
            Ok(()) => (),
            Err(err) => self.borrow_mut().push(err),
        }
    }
}

impl DescribeSink for Rc<RefCell<FlowAccumulator>> {
    fn params(&mut self, params: ParameterDescription) {
        self.borrow_mut().push(params);
    }
    fn rows(&mut self, rows: RowDescription) {
        self.borrow_mut().push(rows);
    }
    fn error(&mut self, error: ErrorResponse) {
        self.borrow_mut().push(error);
    }
}
