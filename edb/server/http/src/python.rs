use futures::future::poll_fn;
use pyo3::{exceptions::PyException, prelude::*, types::PyByteArray};
use reqwest::Method;
use scopeguard::defer;
use serde_pickle::SerOptions;
use std::{
    cell::RefCell, collections::HashMap, os::fd::IntoRawFd, pin::Pin, rc::Rc, sync::Mutex, thread,
};
use tokio::{
    io::AsyncWrite,
    sync::{AcquireError, Semaphore, SemaphorePermit},
    task::LocalSet,
};
use tracing::{error, info, trace};

pyo3::create_exception!(_http, InternalError, PyException);

type PythonConnId = u64;

#[derive(Debug)]
enum RustToPythonMessage {
    Response(PythonConnId, Vec<u8>),
    Error(PythonConnId, String),
}

impl ToPyObject for RustToPythonMessage {
    fn to_object(&self, py: Python<'_>) -> PyObject {
        use RustToPythonMessage::*;
        match self {
            Error(conn, error) => (0, *conn, error).to_object(py),
            Response(conn, response) => {
                (1, conn, PyByteArray::new_bound(py, &response)).to_object(py)
            }
        }
    }
}

#[derive(Debug)]
enum PythonToRustMessage {
    UpdateLimit(usize),
    /// Acquire a connection.
    Request(u64, String, String, Vec<u8>, Vec<(String, String)>),
}

type PipeSender = tokio::net::unix::pipe::Sender;

struct RpcPipe {
    rust_to_python_notify: RefCell<PipeSender>,
    rust_to_python: std::sync::mpsc::Sender<RustToPythonMessage>,
    python_to_rust: RefCell<tokio::sync::mpsc::UnboundedReceiver<PythonToRustMessage>>,
}

impl std::fmt::Debug for RpcPipe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RpcPipe")
    }
}

impl RpcPipe {
    async fn write(&self, msg: RustToPythonMessage) -> Result<(), String> {
        self.rust_to_python.send(msg).map_err(|_| "Shutdown")?;
        // If we're shutting down, this may fail (but that's OK)
        poll_fn(|cx| {
            let pipe = &mut *self.rust_to_python_notify.borrow_mut();
            let this = Pin::new(pipe);
            this.poll_write(cx, &[0])
        })
        .await
        .map_err(|_| "Shutdown")?;
        Ok(())
    }
}

#[pyclass]
struct Http {
    python_to_rust: tokio::sync::mpsc::UnboundedSender<PythonToRustMessage>,
    rust_to_python: std::sync::mpsc::Receiver<RustToPythonMessage>,
    notify_fd: u64,
}

impl Drop for Http {
    fn drop(&mut self) {
        info!("Http dropped");
    }
}

fn internal_error(message: &str) -> PyErr {
    error!("{message}");
    InternalError::new_err(())
}

async fn request(
    client: reqwest::Client,
    url: String,
    method: String,
    body: Vec<u8>,
    headers: Vec<(String, String)>,
) -> Result<(reqwest::StatusCode, Vec<u8>, HashMap<String, String>), String> {
    let method =
        Method::from_bytes(method.as_bytes()).map_err(|e| format!("Invalid HTTP method: {}", e))?;

    let mut req = client.request(method, url);

    for (key, value) in headers {
        req = req.header(key, value);
    }

    if !body.is_empty() {
        req = req.body(body);
    }

    let resp = req
        .send()
        .await
        .map_err(|e| format!("Request failed: {}", e))?;

    let status = resp.status();

    let headers = resp
        .headers()
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
        .collect();

    let body = resp
        .bytes()
        .await
        .map_err(|e| format!("Failed to read response body: {}", e))?
        .to_vec();

    Ok((status, body, headers))
}

struct PermitCount {
    active: usize,
    capacity: usize,
}

/// By default, the [`Semaphore`] does not allow for releasing permits that are currently
/// outstanding. This allows us to keep a count of what the limit _should_ be, which we'll
/// target if we've taken too many permits out by forgetting outstanding permits.
struct PermitManager {
    counts: Mutex<PermitCount>,
    semaphore: Semaphore,
}

impl PermitManager {
    fn new(capacity: usize) -> Self {
        Self {
            counts: Mutex::new(PermitCount {
                active: 0,
                capacity,
            }),
            semaphore: Semaphore::new(capacity),
        }
    }

    async fn acquire(&self) -> Result<SemaphorePermit<'_>, AcquireError> {
        let permit = self.semaphore.acquire().await?;
        let mut counts = self.counts.lock().unwrap();
        counts.active += 1;
        Ok(permit)
    }

    fn release(&self, permit: SemaphorePermit<'_>) {
        let mut counts = self.counts.lock().unwrap();
        if counts.active > counts.capacity {
            // We have too many permits, so forget this one instead of releasing
            permit.forget();
        } else {
            // Normal release
            drop(permit);
        }
        counts.active -= 1;
    }

    fn update_limit(&self, new_limit: usize) {
        let mut counts = self.counts.lock().unwrap();
        let old_capacity = counts.capacity;
        counts.capacity = new_limit;

        if new_limit > old_capacity {
            let added = new_limit - old_capacity;
            self.semaphore.add_permits(added);
        } else if new_limit < old_capacity {
            let removed = old_capacity - new_limit;
            self.semaphore.forget_permits(removed);
        }
    }

    #[cfg(test)]
    fn active(&self) -> usize {
        let counts = self.counts.lock().unwrap();
        counts.active
    }

    #[cfg(test)]
    fn capacity(&self) -> usize {
        let counts = self.counts.lock().unwrap();
        counts.capacity
    }
}

async fn run_and_block(capacity: usize, rpc_pipe: RpcPipe) {
    let rpc_pipe = Rc::new(rpc_pipe);

    let client = reqwest::Client::new();
    let permit_manager = Rc::new(PermitManager::new(capacity));

    loop {
        let Some(rpc) = poll_fn(|cx| rpc_pipe.python_to_rust.borrow_mut().poll_recv(cx)).await
        else {
            info!("Http shutting down");
            break;
        };
        let client = client.clone();
        trace!("Received RPC: {rpc:?}");
        let rpc_pipe = rpc_pipe.clone();
        let permit_manager = permit_manager.clone();
        tokio::task::spawn_local(async move {
            use PythonToRustMessage::*;
            match rpc {
                UpdateLimit(limit) => {
                    permit_manager.update_limit(limit);
                }
                Request(id, url, method, body, headers) => {
                    let Ok(permit) = permit_manager.acquire().await else {
                        return;
                    };
                    defer!(permit_manager.release(permit));
                    match request(client, url, method, body, headers).await {
                        Ok((status, body, headers)) => {
                            _ = rpc_pipe
                                .write(RustToPythonMessage::Response(
                                    id,
                                    serde_pickle::to_vec(
                                        &(status.as_u16(), body, headers),
                                        SerOptions::default(),
                                    )
                                    .unwrap(),
                                ))
                                .await;
                        }
                        Err(err) => {
                            _ = rpc_pipe.write(RustToPythonMessage::Error(id, err)).await;
                        }
                    }
                }
            }
        });
    }
}

#[pymethods]
impl Http {
    /// Create the connection pool and automatically boot a tokio runtime on a
    /// new thread. When this class is GC'd, the thread will be torn down.
    #[new]
    fn new(max_capacity: usize) -> Self {
        info!("Http::new(max_capacity={max_capacity})");
        let (txrp, rxrp) = std::sync::mpsc::channel();
        let (txpr, rxpr) = tokio::sync::mpsc::unbounded_channel();
        let (txfd, rxfd) = std::sync::mpsc::channel();

        thread::spawn(move || {
            info!("Rust-side Http thread booted");
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
            };

            local.block_on(&rt, run_and_block(max_capacity, rpc_pipe));
        });

        let notify_fd = rxfd.recv().unwrap();
        Http {
            python_to_rust: txpr,
            rust_to_python: rxrp,
            notify_fd,
        }
    }

    #[getter]
    fn _fd(&self) -> u64 {
        self.notify_fd
    }

    fn _request(
        &self,
        id: u64,
        url: String,
        method: String,
        body: Vec<u8>,
        headers: Vec<(String, String)>,
    ) -> PyResult<()> {
        self.python_to_rust
            .send(PythonToRustMessage::Request(id, url, method, body, headers))
            .map_err(|_| internal_error("In shutdown"))
    }

    fn _update_limit(&self, limit: usize) -> PyResult<()> {
        self.python_to_rust
            .send(PythonToRustMessage::UpdateLimit(limit))
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
}

#[pymodule]
fn _http(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<Http>()?;
    m.add("InternalError", py.get_type_bound::<InternalError>())?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    };

    use super::*;

    #[tokio::test]
    async fn test_permit_manager() {
        let manager = Arc::new(PermitManager::new(5));

        // Shared done flag
        let done = Arc::new(AtomicBool::new(false));

        // Create 100 tasks
        let tasks = (0..100)
            .map(|_| {
                let manager = manager.clone();
                let done = done.clone();
                tokio::spawn(async move {
                    loop {
                        if done.load(Ordering::Relaxed) {
                            break;
                        }
                        let permit = manager.acquire().await.unwrap();
                        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                        manager.release(permit);
                    }
                })
            })
            .collect::<Vec<_>>();

        // Create a task that changes the limit between 1..10 every 1ms
        {
            let manager = manager.clone();
            tokio::spawn(async move {
                for i in 1..100 {
                    manager.update_limit(i % 10 + 1);
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
            })
            .await
            .unwrap();
        }

        done.store(true, Ordering::Relaxed);

        for task in tasks {
            task.await.unwrap();
        }

        assert_eq!(manager.active(), 0);
        assert_eq!(manager.capacity(), 10);
    }

    #[tokio::test]
    async fn test_permit_manager_zero_to_five() {
        let manager = Arc::new(PermitManager::new(0));
        let permit_count = Arc::new(AtomicUsize::new(0));

        // Create 5 tasks that try to acquire permits
        let tasks: Vec<_> = (0..5)
            .map(|_| {
                let manager = manager.clone();
                let permit_count = permit_count.clone();
                tokio::spawn(async move {
                    let _ = manager.acquire().await.unwrap();
                    permit_count.fetch_add(1, Ordering::SeqCst);
                })
            })
            .collect();

        // Wait a bit to ensure no permits are issued
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(permit_count.load(Ordering::SeqCst), 0);

        // Update the permit count to 5
        manager.update_limit(5);

        // Wait for all tasks to complete
        for task in tasks {
            task.await.unwrap();
        }

        // Check that all 5 permits were issued
        assert_eq!(permit_count.load(Ordering::SeqCst), 5);
        assert_eq!(manager.active(), 5);
        assert_eq!(manager.capacity(), 5);
    }
}
