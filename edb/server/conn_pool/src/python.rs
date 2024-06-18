use crate::{
    algo::PoolConstraints,
    conn::{ConnError, Connector},
    pool::{Pool, PoolConfig},
    PoolHandle,
};
use futures::TryFutureExt;
use pyo3::{
    exceptions::PyException,
    prelude::*,
    types::{PyDict, PyTuple},
};
use std::{
    cell::RefCell,
    collections::HashMap,
    rc::Rc,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock,
    },
};
use tokio::task::LocalSet;
use tracing::{error, trace};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

pyo3::create_exception!(_conn_pool, InternalError, PyException);

#[derive(Debug)]
#[repr(u8)]
enum ConnectOp {
    Connect,
    Disconnect,
    Reconnect,
}

#[derive(Debug, Default)]
struct PythonConnectionMap {
    /// Connection : [`PoolHandle`] (to keep the handle alive)
    handle: HashMap<usize, PoolHandle<PythonConnectionFactory>>,
    py_dict: Option<Py<PyDict>>,
    next_id: usize,
}

impl PythonConnectionMap {
    pub fn insert(&mut self, py: Python, handle: PoolHandle<PythonConnectionFactory>) {
        let py_dict = self
            .py_dict
            .get_or_insert_with(|| PyDict::new(py).into())
            .as_ref(py);
        _ = handle.with_handle(|conn| py_dict.set_item(conn, self.next_id));
        self.handle.insert(self.next_id, handle);
        self.next_id += 1;
    }

    pub fn remove(
        &mut self,
        py: Python,
        conn: PyObject,
    ) -> Option<PoolHandle<PythonConnectionFactory>> {
        let Some(py_dict) = &mut self.py_dict else {
            return None;
        };
        let py_dict = py_dict.as_ref(py);
        let item = py_dict.get_item(conn.clone_ref(py)).ok()??;
        _ = py_dict.del_item(conn);
        let key = item.extract::<usize>().ok()?;
        self.handle.remove(&key)
    }
}

/// Implementation of the [`Connector`] interface. We don't pass the pool or Python objects
/// between threads, but rather use a usize ID that allows us to keep two maps in sync on
/// both sides of this interface.
#[derive(Debug)]
struct PythonConnectionFactory {
    /// The _callback method that triggers the correctly-threaded task for the
    /// connection operation.
    callback: PyObject,
    /// RPC callbacks.
    responses: Arc<RwLock<HashMap<usize, tokio::sync::oneshot::Sender<PyObject>>>>,
    /// Next RPC ID.
    next_response_id: Arc<AtomicUsize>,
}

impl PythonConnectionFactory {
    fn send(
        &self,
        op: ConnectOp,
        args: impl IntoPy<Py<PyTuple>>,
    ) -> impl futures::Future<Output = crate::conn::ConnResult<PyObject>> + 'static {
        let (sender, receiver) = tokio::sync::oneshot::channel::<PyObject>();
        let response_id = self.next_response_id.fetch_add(1, Ordering::SeqCst);
        self.responses.write().unwrap().insert(response_id, sender);
        let success = Python::with_gil(|py| {
            let args0: Py<PyTuple> = (op as u8, response_id).into_py(py);
            let args = args.into_py(py);

            let Ok(result) = self.callback.call(py, (args0, args), None) else {
                error!("Unexpected failure in _callback");
                return false;
            };
            let Ok(result) = result.is_true(py) else {
                error!("Unexpected return value from _callback");
                return false;
            };
            if !result {
                return false;
            }
            true
        });
        async move {
            if success {
                let conn = receiver.await.unwrap();
                let conn = Python::with_gil(|py| conn.to_object(py));
                trace!("Thread received {response_id} {}", conn);
                Ok(conn)
            } else {
                Err(ConnError::Shutdown)
            }
        }
    }
}

impl Connector for PythonConnectionFactory {
    type Conn = PyObject;

    fn connect(
        &self,
        db: &str,
    ) -> impl futures::Future<Output = crate::conn::ConnResult<Self::Conn>> + 'static {
        self.send(ConnectOp::Connect, (db,))
    }

    fn disconnect(
        &self,
        conn: Self::Conn,
    ) -> impl futures::Future<Output = crate::conn::ConnResult<()>> + 'static {
        self.send(ConnectOp::Disconnect, (conn,)).map_ok(|_| ())
    }

    fn reconnect(
        &self,
        conn: Self::Conn,
        db: &str,
    ) -> impl futures::Future<Output = crate::conn::ConnResult<Self::Conn>> + 'static {
        self.send(ConnectOp::Reconnect, (conn, db))
    }
}

impl PythonConnectionFactory {
    fn new(callback: PyObject) -> Self {
        Self {
            callback,
            responses: Default::default(),
            next_response_id: Default::default(),
        }
    }
}

#[derive(Debug)]
enum PoolRPC {
    Acquire(String, PyObject),
    Release(PyObject, bool),
}

#[pyclass]
struct ConnPool {
    connector: RwLock<Option<PythonConnectionFactory>>,
    responses: Arc<RwLock<HashMap<usize, tokio::sync::oneshot::Sender<PyObject>>>>,
    rpc_tx: RwLock<Option<tokio::sync::mpsc::UnboundedSender<PoolRPC>>>,
}

fn internal_error(py: Python, message: &str) {
    error!("{message}");
    InternalError::new_err(()).restore(py);
}

async fn run_and_block(
    connector: PythonConnectionFactory,
    mut rpc_rx: tokio::sync::mpsc::UnboundedReceiver<PoolRPC>,
) {
    let pool = Rc::new(Pool::<PythonConnectionFactory>::new(
        PoolConfig {
            constraints: PoolConstraints {
                max: 10,
                max_per_target: 10,
            },
        },
        connector,
    ));
    let conns = Rc::new(RefCell::new(PythonConnectionMap::default()));
    loop {
        let Some(rpc) = rpc_rx.recv().await else {
            break;
        };
        let pool = pool.clone();
        let conns = conns.clone();
        trace!("Received RPC: {rpc:?}");
        tokio::task::spawn_local(async move {
            match rpc {
                PoolRPC::Acquire(db, callback) => {
                    let conn = pool.acquire(&db).await.unwrap();
                    trace!("Acquired a handle to return to Python!");
                    Python::with_gil(|py| {
                        let handle = conn.handle_clone();
                        conns.borrow_mut().insert(py, conn);
                        callback.call1(py, (handle,)).unwrap();
                    });
                }
                PoolRPC::Release(conn, dispose) => {
                    Python::with_gil(|py| {
                        let Some(conn) = conns.borrow_mut().remove(py, conn) else {
                            error!("Attempted to dispose a connection that does not exist");
                            return;
                        };

                        if dispose {
                            conn.poison();
                        }

                        drop(conn);
                    });
                }
            }
        });
    }
}

#[pymethods]
impl ConnPool {
    #[new]
    fn new(callback: PyObject) -> Self {
        let connector = PythonConnectionFactory::new(callback);
        let responses = connector.responses.clone();
        ConnPool {
            connector: RwLock::new(Some(connector)),
            responses,
            rpc_tx: Default::default(),
        }
    }

    fn _respond(&self, py: Python, response_id: usize, object: PyObject) {
        trace!("_respond({response_id}, {object})");
        let response = self.responses.write().unwrap().remove(&response_id);
        if let Some(response) = response {
            response.send(object).unwrap();
        } else {
            internal_error(py, "Missing response sender");
        }
    }

    fn halt(&self, py: Python) {}

    /// Asynchronously acquires a connection, returning it to the callback
    fn acquire(&self, db: &str, callback: PyObject) {
        self.rpc_tx
            .read()
            .unwrap()
            .as_ref()
            .unwrap()
            .send(PoolRPC::Acquire(db.to_owned(), callback))
            .unwrap();
    }

    /// Releases a connection when possible, potentially discarding it
    fn release(&self, conn: PyObject, discard: bool) {
        self.rpc_tx
            .read()
            .unwrap()
            .as_ref()
            .unwrap()
            .send(PoolRPC::Release(conn, discard))
            .unwrap();
    }

    /// Boot the connection pool on this thread.
    fn run_and_block(&self, py: Python) {
        let connector = self.connector.write().unwrap().take().unwrap();
        let (rpc_tx, rpc_rx) = tokio::sync::mpsc::unbounded_channel();
        *self.rpc_tx.write().unwrap() = Some(rpc_tx);
        py.allow_threads(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap();
            let local = LocalSet::new();
            local.block_on(&rt, run_and_block(connector, rpc_rx));
        })
    }
}

#[pymodule]
fn _conn_pool(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<ConnPool>()?;
    m.add("InternalError", py.get_type::<InternalError>())?;

    let logging = py.import("logging")?;
    let logger = logging
        .getattr("getLogger")?
        .call(("edb.server.connpool",), None)?;
    let level = logger
        .getattr("getEffectiveLevel")?
        .call((), None)?
        .extract::<i32>()?;
    let logger = logger.to_object(py);

    struct PythonSubscriber {
        logger: Py<PyAny>,
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for PythonSubscriber {
        fn on_event(&self, event: &tracing::Event, _ctx: tracing_subscriber::layer::Context<S>) {
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
                let log = match event.metadata().level() {
                    &tracing::Level::TRACE => self.logger.getattr(py, "debug").unwrap(),
                    &tracing::Level::DEBUG => self.logger.getattr(py, "warning").unwrap(),
                    &tracing::Level::INFO => self.logger.getattr(py, "info").unwrap(),
                    &tracing::Level::WARN => self.logger.getattr(py, "warning").unwrap(),
                    &tracing::Level::ERROR => self.logger.getattr(py, "error").unwrap(),
                };
                log.call1(py, (message,)).unwrap();
            })
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
    tracing_subscriber::registry()
        .with(level)
        .with(subscriber)
        .init();

    tracing::info!("ConnPool initialized (level = {level})");

    Ok(())
}
