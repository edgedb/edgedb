use crate::{
    conn::{ConnError, ConnResult, Connector},
    metrics::MetricVariant,
    pool::{Pool, PoolConfig},
    PoolHandle,
};
use derive_more::{Add, AddAssign};
use futures::future::poll_fn;
use pyo3::{exceptions::PyException, prelude::*, types::PyByteArray};
use serde_pickle::SerOptions;
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    os::fd::IntoRawFd,
    pin::Pin,
    rc::Rc,
    thread,
    time::{Duration, Instant},
};
use strum::IntoEnumIterator;
use tokio::{io::AsyncWrite, task::LocalSet};
use tracing::{error, info, subscriber::DefaultGuard, trace};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

pyo3::create_exception!(_conn_pool, InternalError, PyException);

#[derive(Debug)]
enum RustToPythonMessage {
    Acquired(PythonConnId, ConnHandleId),
    Pruned(PythonConnId),

    PerformConnect(ConnHandleId, String),
    PerformDisconnect(ConnHandleId),
    PerformReconnect(ConnHandleId, String),

    Failed(PythonConnId, ConnHandleId),
    Metrics(Vec<u8>),
}

impl ToPyObject for RustToPythonMessage {
    fn to_object(&self, py: Python<'_>) -> PyObject {
        use RustToPythonMessage::*;
        match self {
            Acquired(a, b) => (0, a, b.0).to_object(py),
            PerformConnect(conn, s) => (1, conn.0, s).to_object(py),
            PerformDisconnect(conn) => (2, conn.0).to_object(py),
            PerformReconnect(conn, s) => (3, conn.0, s).to_object(py),
            Pruned(conn) => (4, conn).to_object(py),
            Failed(conn, error) => (5, conn, error.0).to_object(py),
            Metrics(metrics) => {
                // This is not really fast but it should not be happening very often
                (6, PyByteArray::new_bound(py, &metrics)).to_object(py)
            }
        }
    }
}

#[derive(Debug)]
enum PythonToRustMessage {
    /// Acquire a connection.
    Acquire(PythonConnId, String),
    /// Release a connection.
    Release(PythonConnId),
    /// Discard a connection.
    Discard(PythonConnId),
    /// Prune connections from a database.
    Prune(PythonConnId, String),
    /// Completed an async request made by Rust.
    CompletedAsync(ConnHandleId),
    /// Failed an async request made by Rust.
    FailedAsync(ConnHandleId),
}

type PipeSender = tokio::net::unix::pipe::Sender;

type PythonConnId = u64;
#[derive(Debug, Default, Clone, Copy, Add, AddAssign, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct ConnHandleId(u64);

impl Into<Box<(dyn derive_more::Error + std::marker::Send + Sync + 'static)>> for ConnHandleId {
    fn into(self) -> Box<(dyn derive_more::Error + std::marker::Send + Sync + 'static)> {
        Box::new(ConnError::Underlying(format!("{self:?}")))
    }
}

struct RpcPipe {
    rust_to_python_notify: RefCell<PipeSender>,
    rust_to_python: std::sync::mpsc::Sender<RustToPythonMessage>,
    python_to_rust: RefCell<tokio::sync::mpsc::UnboundedReceiver<PythonToRustMessage>>,
    handles: RefCell<BTreeMap<PythonConnId, PoolHandle<Rc<RpcPipe>>>>,
    next_id: Cell<ConnHandleId>,
    async_ops: RefCell<BTreeMap<ConnHandleId, tokio::sync::oneshot::Sender<()>>>,
}

impl std::fmt::Debug for RpcPipe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RpcPipe")
    }
}

impl RpcPipe {
    async fn write(&self, msg: RustToPythonMessage) -> ConnResult<(), String> {
        self.rust_to_python
            .send(msg)
            .map_err(|_| ConnError::Shutdown)?;
        // If we're shutting down, this may fail (but that's OK)
        poll_fn(|cx| {
            let pipe = &mut *self.rust_to_python_notify.borrow_mut();
            let this = Pin::new(pipe);
            this.poll_write(cx, &[0])
        })
        .await
        .map_err(|_| ConnError::Shutdown)?;
        Ok(())
    }

    async fn call<T>(
        self: Rc<Self>,
        conn_id: ConnHandleId,
        ok: T,
        msg: RustToPythonMessage,
    ) -> ConnResult<T, ConnHandleId> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.async_ops.borrow_mut().insert(conn_id, tx);
        self.write(msg)
            .await
            .map_err(|_| ConnError::Underlying(conn_id))?;
        if let Ok(_) = rx.await {
            Err(ConnError::Underlying(conn_id))
        } else {
            Ok(ok)
        }
    }
}

impl Connector for Rc<RpcPipe> {
    type Conn = ConnHandleId;
    type Error = ConnHandleId;

    fn connect(
        &self,
        db: &str,
    ) -> impl futures::Future<
        Output = ConnResult<<Self as Connector>::Conn, <Self as Connector>::Error>,
    > + 'static {
        let id = self.next_id.get();
        self.next_id.set(id + ConnHandleId(1));
        let msg = RustToPythonMessage::PerformConnect(id, db.to_owned());
        self.clone().call(id, id, msg)
    }

    fn disconnect(
        &self,
        conn: Self::Conn,
    ) -> impl futures::Future<Output = ConnResult<(), <Self as Connector>::Error>> + 'static {
        self.clone()
            .call(conn, (), RustToPythonMessage::PerformDisconnect(conn))
    }

    fn reconnect(
        &self,
        conn: Self::Conn,
        db: &str,
    ) -> impl futures::Future<
        Output = ConnResult<<Self as Connector>::Conn, <Self as Connector>::Error>,
    > + 'static {
        self.clone().call(
            conn,
            conn,
            RustToPythonMessage::PerformReconnect(conn, db.to_owned()),
        )
    }
}

#[pyclass]
struct ConnPool {
    python_to_rust: tokio::sync::mpsc::UnboundedSender<PythonToRustMessage>,
    rust_to_python: std::sync::mpsc::Receiver<RustToPythonMessage>,
    notify_fd: u64,
}

impl Drop for ConnPool {
    fn drop(&mut self) {
        info!("ConnPool dropped");
    }
}

fn internal_error(message: &str) -> PyErr {
    error!("{message}");
    InternalError::new_err(())
}

async fn run_and_block(config: PoolConfig, rpc_pipe: RpcPipe, stats_interval: f64) {
    let rpc_pipe = Rc::new(rpc_pipe);

    let pool = Pool::new(config, rpc_pipe.clone());

    let pool_task = {
        let pool = pool.clone();
        let rpc_pipe = rpc_pipe.clone();
        tokio::task::spawn_local(async move {
            let stats_interval = Duration::from_secs_f64(stats_interval);
            let mut last_stats = Instant::now();
            loop {
                pool.run_once();
                tokio::time::sleep(Duration::from_millis(10)).await;
                if last_stats.elapsed() > stats_interval {
                    last_stats = Instant::now();
                    if rpc_pipe
                        .write(RustToPythonMessage::Metrics(
                            serde_pickle::to_vec(&pool.metrics(), SerOptions::new())
                                .unwrap_or_default(),
                        ))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }
        })
    };

    loop {
        let Some(rpc) = poll_fn(|cx| rpc_pipe.python_to_rust.borrow_mut().poll_recv(cx)).await
        else {
            info!("ConnPool shutting down");
            pool_task.abort();
            pool.shutdown().await;
            break;
        };
        let pool = pool.clone();
        trace!("Received RPC: {rpc:?}");
        let rpc_pipe = rpc_pipe.clone();
        tokio::task::spawn_local(async move {
            use PythonToRustMessage::*;
            match rpc {
                Acquire(conn_id, db) => {
                    let conn = match pool.acquire(&db).await {
                        Ok(conn) => conn,
                        Err(ConnError::Underlying(err)) => {
                            _ = rpc_pipe
                                .write(RustToPythonMessage::Failed(conn_id, err))
                                .await;
                            return;
                        }
                        Err(_) => {
                            // TODO
                            return;
                        }
                    };
                    let handle = conn.handle();
                    rpc_pipe.handles.borrow_mut().insert(conn_id, conn);
                    _ = rpc_pipe
                        .write(RustToPythonMessage::Acquired(conn_id, handle))
                        .await;
                }
                Release(conn_id) => {
                    rpc_pipe.handles.borrow_mut().remove(&conn_id);
                }
                Discard(conn_id) => {
                    rpc_pipe
                        .handles
                        .borrow_mut()
                        .remove(&conn_id)
                        .unwrap()
                        .poison();
                }
                Prune(conn_id, db) => {
                    pool.drain_idle(&db).await;
                    _ = rpc_pipe.write(RustToPythonMessage::Pruned(conn_id)).await;
                }
                CompletedAsync(handle_id) => {
                    rpc_pipe.async_ops.borrow_mut().remove(&handle_id);
                }
                FailedAsync(handle_id) => {
                    _ = rpc_pipe
                        .async_ops
                        .borrow_mut()
                        .remove(&handle_id)
                        .unwrap()
                        .send(());
                }
            }
        });
    }
}

#[pymethods]
impl ConnPool {
    /// Create the connection pool and automatically boot a tokio runtime on a
    /// new thread. When this [`ConnPool`] is GC'd, the thread will be torn down.
    #[new]
    fn new(max_capacity: usize, min_idle_time_before_gc: f64, stats_interval: f64) -> Self {
        let min_idle_time_before_gc = min_idle_time_before_gc as usize;
        info!("ConnPool::new(max_capacity={max_capacity}, min_idle_time_before_gc={min_idle_time_before_gc})");
        let (txrp, rxrp) = std::sync::mpsc::channel();
        let (txpr, rxpr) = tokio::sync::mpsc::unbounded_channel();
        let (txfd, rxfd) = std::sync::mpsc::channel();

        thread::spawn(move || {
            info!("Rust-side ConnPool thread booted");
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .enable_io()
                .build()
                .unwrap();
            let _guard = rt.enter();
            let (txn, rxn) = tokio::net::unix::pipe::pipe().unwrap();
            let fd = rxn.into_nonblocking_fd().unwrap().into_raw_fd() as u64;
            txfd.send(fd).unwrap();
            let local = LocalSet::new();

            let rpc_pipe = RpcPipe {
                python_to_rust: rxpr.into(),
                rust_to_python: txrp,
                rust_to_python_notify: txn.into(),
                next_id: Default::default(),
                handles: Default::default(),
                async_ops: Default::default(),
            };

            let config = PoolConfig::suggested_default_for(max_capacity)
                .with_min_idle_time_for_gc(Duration::from_secs(min_idle_time_before_gc as _));
            local.block_on(&rt, run_and_block(config, rpc_pipe, stats_interval));
        });

        let notify_fd = rxfd.recv().unwrap();
        ConnPool {
            python_to_rust: txpr,
            rust_to_python: rxrp,
            notify_fd,
        }
    }

    #[getter]
    fn _fd(&self) -> u64 {
        self.notify_fd
    }

    fn _acquire(&self, id: u64, db: &str) -> PyResult<()> {
        self.python_to_rust
            .send(PythonToRustMessage::Acquire(id, db.to_owned()))
            .map_err(|_| internal_error("In shutdown"))
    }

    fn _release(&self, id: u64) -> PyResult<()> {
        self.python_to_rust
            .send(PythonToRustMessage::Release(id))
            .map_err(|_| internal_error("In shutdown"))
    }

    fn _discard(&self, id: u64) -> PyResult<()> {
        self.python_to_rust
            .send(PythonToRustMessage::Discard(id))
            .map_err(|_| internal_error("In shutdown"))
    }

    fn _completed(&self, id: u64) -> PyResult<()> {
        self.python_to_rust
            .send(PythonToRustMessage::CompletedAsync(ConnHandleId(id)))
            .map_err(|_| internal_error("In shutdown"))
    }

    fn _failed(&self, id: u64, _error: PyObject) -> PyResult<()> {
        self.python_to_rust
            .send(PythonToRustMessage::FailedAsync(ConnHandleId(id)))
            .map_err(|_| internal_error("In shutdown"))
    }

    fn _prune(&self, id: u64, db: &str) -> PyResult<()> {
        self.python_to_rust
            .send(PythonToRustMessage::Prune(id, db.to_owned()))
            .map_err(|_| internal_error("In shutdown"))
    }

    fn _read(&self, py: Python<'_>) -> Py<PyAny> {
        let Ok(msg) = self.rust_to_python.recv() else {
            return py.None();
        };
        msg.to_object(py)
    }

    fn _try_read(&self, py: Python<'_>) -> Py<PyAny> {
        let Ok(msg) = self.rust_to_python.try_recv() else {
            return py.None();
        };
        msg.to_object(py)
    }

    fn _close_pipe(&mut self) {
        // Replace the channel with a dummy, closed one which will also
        // signal the other side to exit.
        (_, self.rust_to_python) = std::sync::mpsc::channel();
    }
}

/// Ensure that logging does not outlive the Python runtime.
#[pyclass]
struct LoggingGuard {
    #[allow(unused)]
    guard: DefaultGuard,
}

#[pymethods]
impl LoggingGuard {
    #[new]
    fn init_logging(py: Python) -> PyResult<LoggingGuard> {
        let logging = py.import_bound("logging")?;
        let logger = logging.getattr("getLogger")?.call(("edb.server",), None)?;
        let level = logger
            .getattr("getEffectiveLevel")?
            .call((), None)?
            .extract::<i32>()?;
        let logger = logger.to_object(py);

        struct PythonSubscriber {
            logger: Py<PyAny>,
        }

        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for PythonSubscriber {
            fn on_event(
                &self,
                event: &tracing::Event,
                _ctx: tracing_subscriber::layer::Context<S>,
            ) {
                let mut message = format!("[{}] ", event.metadata().target());
                #[derive(Default)]
                struct Visitor(String);
                impl tracing::field::Visit for Visitor {
                    fn record_debug(
                        &mut self,
                        field: &tracing::field::Field,
                        value: &dyn std::fmt::Debug,
                    ) {
                        if field.name() == "message" {
                            self.0 += &format!("{value:?} ");
                        } else {
                            self.0 += &format!("{}={:?} ", field.name(), value)
                        }
                    }
                }

                let mut visitor = Visitor::default();
                event.record(&mut visitor);
                message += &visitor.0;

                Python::with_gil(|py| {
                    let Ok(log) = (match *event.metadata().level() {
                        tracing::Level::TRACE => self.logger.getattr(py, "debug"),
                        tracing::Level::DEBUG => self.logger.getattr(py, "warning"),
                        tracing::Level::INFO => self.logger.getattr(py, "info"),
                        tracing::Level::WARN => self.logger.getattr(py, "warning"),
                        tracing::Level::ERROR => self.logger.getattr(py, "error"),
                    }) else {
                        return;
                    };
                    // This may fail
                    _ = log.call1(py, (message,));
                });
            }
        }

        let level = if level < 10 {
            tracing_subscriber::filter::LevelFilter::TRACE
        } else if level <= 10 {
            tracing_subscriber::filter::LevelFilter::DEBUG
        } else if level <= 20 {
            tracing_subscriber::filter::LevelFilter::INFO
        } else if level <= 30 {
            tracing_subscriber::filter::LevelFilter::WARN
        } else if level <= 40 {
            tracing_subscriber::filter::LevelFilter::ERROR
        } else {
            tracing_subscriber::filter::LevelFilter::OFF
        };

        let subscriber = PythonSubscriber { logger };
        let guard = tracing_subscriber::registry()
            .with(level)
            .with(subscriber)
            .set_default();

        tracing::info!("ConnPool initialized (level = {level})");
        Ok(LoggingGuard { guard })
    }
}

#[pymodule]
fn _conn_pool(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<ConnPool>()?;
    m.add_class::<LoggingGuard>()?;
    m.add("InternalError", py.get_type_bound::<InternalError>())?;

    // Add each metric variant as a constant
    for variant in MetricVariant::iter() {
        m.add(
            format!("METRIC_{}", variant.as_ref().to_ascii_uppercase()).as_str(),
            variant as u32,
        )?;
    }

    Ok(())
}
