use std::{
    collections::HashMap,
    rc::Rc,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock,
    },
};

use pyo3::{
    prelude::*,
    types::{PyFunction, PyString},
};
use tokio::task::LocalSet;

use crate::{
    algo::PoolConstraints,
    conn::Connector,
    pool::{Pool, PoolConfig, PoolHandle},
};

/// Implementation of the [`Connector`] interface. We don't pass the pool or Python objects
/// between threads, but rather use a usize ID that allows us to keep two maps in sync on
/// both sides of this interface.
#[derive(Debug)]
struct PythonConnectionFactory {
    callback: PyObject,

    /// Our RPC-like response callbacks.
    responses: Arc<RwLock<HashMap<usize, tokio::sync::oneshot::Sender<PyObject>>>>,

    id: Arc<AtomicUsize>,

    /// Python connections
    conns: Arc<RwLock<HashMap<usize, PyObject>>>,
}

impl Connector for PythonConnectionFactory {
    type Conn = usize;

    fn connect(
        &self,
        db: &str,
    ) -> impl futures::Future<Output = crate::conn::ConnResult<Self::Conn>> + 'static {
        let (sender, receiver) = tokio::sync::oneshot::channel::<PyObject>();
        let response_id = self.id.fetch_add(1, Ordering::SeqCst);
        self.responses.write().unwrap().insert(response_id, sender);
        Python::with_gil(|py| {
            let db = PyString::new(py, db);
            let Ok(result) = self.callback.call(py, (0, response_id, 1), None) else {
                println!("Error?");
                return false;
            };
            let Ok(result) = result.is_true(py) else {
                println!("Error?");
                return false;
            };
            if !result {
                return false;
            }
            true
        });
        let conns = self.conns.clone();
        async move {
            let conn = receiver.await.unwrap();
            let conn = Python::with_gil(|py| conn.to_object(py));
            eprintln!("Received {response_id} {}", conn);
            conns.write().unwrap().insert(response_id, conn);
            Ok(response_id)
        }
    }

    fn disconnect(
        &self,
        conn: Self::Conn,
    ) -> impl futures::Future<Output = crate::conn::ConnResult<()>> + 'static {
        async { todo!() }
    }

    fn reconnect(
        &self,
        conn: Self::Conn,
        db: &str,
    ) -> impl futures::Future<Output = crate::conn::ConnResult<Self::Conn>> + 'static {
        async { todo!() }
    }
}

impl PythonConnectionFactory {
    fn new(callback: PyObject) -> Self {
        Self {
            callback,
            responses: Default::default(),
            id: Default::default(),
            conns: Default::default(),
        }
    }
}

#[pyclass]
struct ConnPool {
    connector: RwLock<Option<PythonConnectionFactory>>,
    responses: Arc<RwLock<HashMap<usize, tokio::sync::oneshot::Sender<PyObject>>>>,
    rpc_tx: RwLock<Option<tokio::sync::mpsc::UnboundedSender<(String, PyObject)>>>,
    conns: Arc<RwLock<HashMap<usize, PyObject>>>,
}

#[pymethods]
impl ConnPool {
    #[new]
    fn new(callback: PyObject) -> Self {
        let connector = PythonConnectionFactory::new(callback);
        let responses = connector.responses.clone();
        let conns = connector.conns.clone();
        ConnPool {
            connector: RwLock::new(Some(connector)),
            responses,
            rpc_tx: Default::default(),
            conns,
        }
    }

    fn _respond(&self, _py: Python, response_id: usize, object: PyObject) {
        println!(" - Sending!");
        let response = self.responses.write().unwrap().remove(&response_id);
        if let Some(response) = response {
            response.send(object).unwrap();
        } else {
            println!("Missing?");
        }
        println!(" - Sent!");
    }

    fn halt(&self, py: Python) {}

    /// Asynchronously acquires a connection, returning it to the callback
    fn acquire(&self, py: Python, db: &str, callback: PyObject) {
        self.rpc_tx
            .read()
            .unwrap()
            .as_ref()
            .unwrap()
            .send((db.to_owned(), callback));
    }

    /// Releases a connection when possible, potentially discarding it
    fn release(&self, py: Python, conn: usize, discard: bool) {}

    /// Boot the connection pool on this thread.
    fn run_and_block(&self, py: Python) {
        let connector = self.connector.write().unwrap().take().unwrap();
        let conns = self.conns.clone();
        let (rpc_tx, mut rpc_rx) = tokio::sync::mpsc::unbounded_channel();
        *self.rpc_tx.write().unwrap() = Some(rpc_tx);
        py.allow_threads(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap();
            let local = LocalSet::new();
            local.block_on(&rt, async {
                let pool = Rc::new(Pool::<PythonConnectionFactory>::new(
                    PoolConfig {
                        constraints: PoolConstraints {
                            max: 10,
                            max_per_target: 10,
                        },
                    },
                    connector,
                ));
                loop {
                    let Some((db, callback)) = rpc_rx.recv().await else {
                        break;
                    };
                    let pool = pool.clone();
                    let conns = conns.clone();
                    tokio::task::spawn_local(async move {
                        eprintln!("task");
                        let conn = pool.acquire(&db).await.unwrap();
                        eprintln!("acq'd");
                        let handle = conn.handle();
                        Python::with_gil(|py| {
                            let conn = conns.read().unwrap().get(&handle).unwrap().clone();
                            callback.call1(py, (conn,)).unwrap();
                        });
                    });
                }
            });
        })
    }
}

#[pymodule]
fn _conn_pool(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<ConnPool>().unwrap();
    Ok(())
}
