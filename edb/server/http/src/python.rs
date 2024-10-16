use futures::future::poll_fn;
use pyo3::{exceptions::PyException, prelude::*, types::PyByteArray};
use reqwest::Method;
use scopeguard::ScopeGuard;
use std::{
    cell::RefCell, collections::HashMap, os::fd::IntoRawFd, pin::Pin, rc::Rc, sync::Mutex, thread,
    time::Duration,
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
    Response(PythonConnId, (u16, Vec<u8>, HashMap<String, String>)),
    Error(PythonConnId, String),
}

impl ToPyObject for RustToPythonMessage {
    fn to_object(&self, py: Python<'_>) -> PyObject {
        use RustToPythonMessage::*;
        match self {
            Error(conn, error) => (0, *conn, error).to_object(py),
            Response(conn, (status, body, headers)) => (
                1,
                conn,
                (*status, PyByteArray::new_bound(py, &body), headers),
            )
                .to_object(py),
        }
    }
}

#[derive(Debug)]
enum PythonToRustMessage {
    /// Update the inflight limit
    UpdateLimit(usize),
    /// Perform a request
    Request(PythonConnId, String, String, Vec<u8>, Vec<(String, String)>),
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

#[derive(Debug, Clone, Copy)]
struct PermitCount {
    active: usize,
    capacity: usize,
    #[cfg(debug_assertions)]
    waiting: usize,
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
                #[cfg(debug_assertions)]
                waiting: 0,
            }),
            semaphore: Semaphore::new(capacity),
        }
    }

    async fn acquire<'a>(
        &'a self,
    ) -> Result<ScopeGuard<SemaphorePermit<'a>, impl FnOnce(SemaphorePermit<'a>) + 'a>, AcquireError>
    {
        #[cfg(debug_assertions)]
        {
            let mut counts = self.counts.lock().unwrap();
            counts.waiting += 1;
            drop(counts);
        }
        let permit = self.semaphore.acquire().await?;
        let mut counts = self.counts.lock().unwrap();
        counts.active += 1;
        #[cfg(debug_assertions)]
        {
            counts.waiting -= 1;
        }
        drop(counts);
        self.assert_valid();
        Ok(scopeguard::guard(permit, |permit| self.release(permit)))
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
        drop(counts);
        self.assert_valid();
    }

    fn update_limit(&self, new_limit: usize) {
        let mut counts = self.counts.lock().unwrap();
        let old_capacity = counts.capacity;
        counts.capacity = new_limit;

        if new_limit > old_capacity {
            // We may be oversubscribed on permits right now. If so, we want to add
            // enough permits to cover the current deficit only.
            let added = new_limit.saturating_sub(counts.active.max(old_capacity));
            self.semaphore.add_permits(added);
        } else if new_limit < old_capacity {
            // This may not be able to forget all of the permits, as some may be
            // active. That's OK, because we'll fix it in `release`.
            let removed = old_capacity - new_limit;
            self.semaphore.forget_permits(removed);
        }

        drop(counts);
        self.assert_valid();
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

    fn assert_valid(&self) {
        #[cfg(debug_assertions)]
        {
            let count_lock = self.counts.lock().unwrap();
            let available = self.semaphore.available_permits();
            let counts = *count_lock;
            drop(count_lock);

            if counts.active > counts.capacity {
                // Oversubscribed
                assert!(available == 0);
            } else {
                // Not oversubscribed, but we can only validate these if nothing is waiting
                if counts.waiting == 0 {
                    assert!(
                        available + counts.waiting == counts.capacity - counts.active,
                        "{} + {} == {} - {}",
                        available,
                        counts.waiting,
                        counts.capacity,
                        counts.active
                    );
                    assert!(counts.active <= counts.capacity);
                }
            }
        }
    }
}

async fn run_and_block(capacity: usize, rpc_pipe: RpcPipe) {
    let rpc_pipe = Rc::new(rpc_pipe);

    // Set some reasonable defaults for timeouts
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(30))
        .timeout(Duration::from_secs(120))
        .read_timeout(Duration::from_secs(10))
        .pool_idle_timeout(Duration::from_secs(30));
    let client = client.build().unwrap();

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
                    match request(client, url, method, body, headers).await {
                        Ok((status, body, headers)) => {
                            _ = rpc_pipe
                                .write(RustToPythonMessage::Response(
                                    id,
                                    (status.as_u16(), body, headers),
                                ))
                                .await;
                        }
                        Err(err) => {
                            _ = rpc_pipe.write(RustToPythonMessage::Error(id, err)).await;
                        }
                    }
                    drop(permit);
                }
            }
        });
    }
}

#[pymethods]
impl Http {
    /// Create the HTTP pool and automatically boot a tokio runtime on a
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
        id: PythonConnId,
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
    use super::*;
    use rstest::rstest;
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    };

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
                        drop(permit);
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
        assert_eq!(manager.active(), 0);
        assert_eq!(manager.capacity(), 5);
    }

    #[rstest]
    // Up-down-up
    #[case(20, 5, 20, 8)]
    #[case(10, 3, 15, 5)]
    #[case(5, 2, 4, 3)]
    #[case(8, 3, 6, 4)]
    #[case(12, 4, 9, 6)]
    #[case(7, 2, 5, 3)]
    #[case(9, 4, 7, 5)]
    // Down-up-down
    #[case(5, 10, 5, 4)]
    #[case(8, 15, 6, 4)]
    // Down-up-up
    #[case(10, 15, 20, 4)]
    #[case(10, 15, 20, 10)]
    #[tokio::test]
    async fn test_overrelease(
        #[case] initial_capacity: usize,
        #[case] new_capacity: usize,
        #[case] final_capacity: usize,
        #[case] acquire_count: usize,
    ) {
        let manager = Arc::new(PermitManager::new(initial_capacity));
        let mut permits = vec![];

        for _ in 0..acquire_count {
            permits.push(manager.acquire().await.unwrap());
        }

        assert_eq!(manager.active(), acquire_count);
        assert_eq!(manager.capacity(), initial_capacity);
        assert_eq!(
            manager.semaphore.available_permits(),
            initial_capacity - acquire_count
        );

        manager.update_limit(new_capacity);

        assert_eq!(manager.active(), acquire_count);
        assert_eq!(manager.capacity(), new_capacity);
        assert_eq!(
            manager.semaphore.available_permits(),
            new_capacity.saturating_sub(acquire_count)
        );

        manager.update_limit(final_capacity);

        assert_eq!(manager.active(), acquire_count);
        assert_eq!(manager.capacity(), final_capacity);
        assert_eq!(
            manager.semaphore.available_permits(),
            final_capacity.saturating_sub(acquire_count)
        );

        for permit in permits {
            drop(permit);
            assert_eq!(
                manager.semaphore.available_permits(),
                final_capacity.saturating_sub(manager.active())
            );
            eprintln!(
                "{} {} {}",
                manager.active(),
                manager.capacity(),
                manager.semaphore.available_permits()
            );
        }

        assert_eq!(manager.active(), 0);
        assert_eq!(manager.capacity(), final_capacity);
        assert_eq!(manager.semaphore.available_permits(), final_capacity);
    }
}
