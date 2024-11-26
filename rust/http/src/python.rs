use eventsource_stream::Eventsource;
use futures::{future::poll_fn, TryStreamExt};
use pyo3::{exceptions::PyException, prelude::*, types::PyByteArray};
use pyo3_util::logging::{get_python_logger_level, initialize_logging_in_thread};
use reqwest::Method;
use scopeguard::{defer, guard, ScopeGuard};
use std::{
    cell::RefCell,
    collections::HashMap,
    os::fd::IntoRawFd,
    pin::Pin,
    rc::Rc,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};
use tokio::{
    io::AsyncWrite,
    sync::{AcquireError, Semaphore, SemaphorePermit},
    task::{JoinHandle, LocalSet},
};
use tracing::{error, info, trace};

pyo3::create_exception!(_http, InternalError, PyException);

/// The backlog for SSE message
const SSE_QUEUE_SIZE: usize = 100;

type PythonConnId = u64;

#[derive(Debug)]
enum RustToPythonMessage {
    Response(PythonConnId, (u16, Vec<u8>, HashMap<String, String>)),
    SSEStart(PythonConnId, (u16, HashMap<String, String>)),
    SSEEvent(PythonConnId, eventsource_stream::Event),
    SSEEnd(PythonConnId),
    Error(PythonConnId, String),
}
impl RustToPythonMessage {
    fn to_object(&self, py: Python<'_>) -> PyResult<PyObject> {
        use RustToPythonMessage::*;
        trace!("Read: {self:?}");
        match self {
            Error(conn, error) => (0, conn, error).into_pyobject(py),
            Response(conn, (status, body, headers)) => {
                (1, conn, (status, PyByteArray::new(py, body), headers)).into_pyobject(py)
            }
            SSEStart(conn, (status, headers)) => (2, conn, (status, headers)).into_pyobject(py),
            SSEEvent(conn, message) => {
                (3, conn, (&message.id, &message.data, &message.event)).into_pyobject(py)
            }
            SSEEnd(conn) => (4, conn, ()).into_pyobject(py),
        }
        .map(|e| e.into())
    }
}

#[derive(Debug)]
enum PythonToRustMessage {
    /// Update the inflight limit
    UpdateLimit(usize),
    /// Perform a request
    Request(PythonConnId, String, String, Vec<u8>, Vec<(String, String)>),
    /// Perform a request with SSE
    RequestSse(PythonConnId, String, String, Vec<u8>, Vec<(String, String)>),
    /// Close an SSE connection
    Close(PythonConnId),
    /// Acknowledge an SSE message
    Ack(PythonConnId),
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
        trace!("Rust -> Python: {msg:?}");
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
    rust_to_python: Mutex<std::sync::mpsc::Receiver<RustToPythonMessage>>,
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
) -> Result<reqwest::Response, String> {
    let method =
        Method::from_bytes(method.as_bytes()).map_err(|e| format!("Invalid HTTP method: {e:?}"))?;

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
        .map_err(|e| format!("Request failed: {e:?}"))?;

    Ok(resp)
}

async fn request_bytes(
    client: reqwest::Client,
    url: String,
    method: String,
    body: Vec<u8>,
    headers: Vec<(String, String)>,
) -> Result<(reqwest::StatusCode, Vec<u8>, HashMap<String, String>), String> {
    let resp = request(client, url, method, body, headers).await?;
    let status = resp.status();
    let headers = process_headers(resp.headers());
    let body = resp
        .bytes()
        .await
        .map_err(|e| format!("Failed to read response body: {e:?}"))?
        .to_vec();

    Ok((status, body, headers))
}

#[allow(clippy::too_many_arguments)]
async fn request_sse(
    client: reqwest::Client,
    id: PythonConnId,
    backpressure: Arc<Semaphore>,
    url: String,
    method: String,
    body: Vec<u8>,
    headers: Vec<(String, String)>,
    rpc_pipe: Rc<RpcPipe>,
) -> Result<(), String> {
    trace!("Entering SSE");
    let guard = guard((), |_| trace!("Exiting SSE due to cancellation"));
    let response = request(client, url, method, body, headers).await?;

    let content_type = response.headers().get("content-type");
    let is_event_stream = content_type
        .and_then(|v| v.to_str().ok())
        .map(|s| s.starts_with("text/event-stream"))
        .unwrap_or(false);

    if !is_event_stream {
        let headers = process_headers(response.headers());
        let status = response.status();
        let body = match response.bytes().await {
            Ok(bytes) => bytes.to_vec(),
            Err(e) => {
                return Err(format!("Failed to read response body: {e:?}"));
            }
        };
        _ = rpc_pipe
            .write(RustToPythonMessage::Response(
                id,
                (status.as_u16(), body, headers),
            ))
            .await;

        trace!("Exiting SSE due to non-SSE response");
        ScopeGuard::into_inner(guard);
        return Ok(());
    }

    let headers = process_headers(response.headers());
    let status = response.status();
    _ = rpc_pipe
        .write(RustToPythonMessage::SSEStart(
            id,
            (status.as_u16(), headers.clone()),
        ))
        .await;

    let mut stream = response.bytes_stream().eventsource();
    loop {
        let chunk = match stream.try_next().await {
            Ok(None) => break,
            Ok(Some(chunk)) => chunk,
            Err(e) => {
                return Err(format!("Failed to read response body: {e:?}"));
            }
        };

        // Note that we use semaphores here in a strange way, but basically we
        // want to have per-stream backpressure to avoid buffering messages
        // indefinitely.
        let Ok(permit) = backpressure.acquire().await else {
            break;
        };
        permit.forget();

        if rpc_pipe
            .write(RustToPythonMessage::SSEEvent(id, chunk))
            .await
            .is_err()
        {
            break;
        }
    }

    trace!("Exiting SSE");
    ScopeGuard::into_inner(guard);
    Ok(())
}

fn process_headers(headers: &reqwest::header::HeaderMap) -> HashMap<String, String> {
    headers
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
        .collect()
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

    #[allow(clippy::comparison_chain)]
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

struct HttpTask {
    task: JoinHandle<()>,
    backpressure: Arc<Semaphore>,
}

async fn run_and_block(capacity: usize, rpc_pipe: RpcPipe) {
    let rpc_pipe = Rc::new(rpc_pipe);

    const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
    const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
    const STANDARD_READ_TIMEOUT: Duration = Duration::from_secs(10);
    const STANDARD_TOTAL_TIMEOUT: Duration = Duration::from_secs(120);
    const SSE_READ_TIMEOUT: Duration = Duration::from_secs(60 * 60); // 1 hour

    // Set some reasonable defaults for timeouts
    let client = reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(STANDARD_TOTAL_TIMEOUT)
        .read_timeout(STANDARD_READ_TIMEOUT)
        .pool_idle_timeout(POOL_IDLE_TIMEOUT);
    let client = client.build().unwrap();

    // SSE requests should have a very long read timeout and no general timeout
    let client_sse = reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .read_timeout(SSE_READ_TIMEOUT)
        .pool_idle_timeout(POOL_IDLE_TIMEOUT);
    let client_sse = client_sse.build().unwrap();

    let permit_manager = Rc::new(PermitManager::new(capacity));
    let tasks = Arc::new(Mutex::new(HashMap::<PythonConnId, HttpTask>::new()));

    loop {
        let Some(rpc) = poll_fn(|cx| rpc_pipe.python_to_rust.borrow_mut().poll_recv(cx)).await
        else {
            info!("Http shutting down");
            break;
        };
        let client = client.clone();
        let client_sse = client_sse.clone();
        trace!("Received RPC: {rpc:?}");
        let rpc_pipe = rpc_pipe.clone();
        // Allocate a task ID and backpressure object if we're initiating a
        // request. This would be less awkward if we allocated in the Rust side
        // of the code rather than the Python side.
        let (id, backpressure) = match rpc {
            PythonToRustMessage::Request(id, ..) | PythonToRustMessage::RequestSse(id, ..) => {
                (Some(id), Some(Semaphore::new(SSE_QUEUE_SIZE).into()))
            }
            _ => (None, None),
        };
        let task = tokio::task::spawn_local(execute(
            id,
            backpressure.clone(),
            tasks.clone(),
            rpc,
            permit_manager.clone(),
            client,
            client_sse,
            rpc_pipe,
        ));
        if let (Some(id), Some(backpressure)) = (id, backpressure) {
            tasks
                .lock()
                .unwrap()
                .insert(id, HttpTask { task, backpressure });
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute(
    id: Option<u64>,
    backpressure: Option<Arc<Semaphore>>,
    tasks_clone: Arc<Mutex<HashMap<u64, HttpTask>>>,
    rpc: PythonToRustMessage,
    permit_manager: Rc<PermitManager>,
    client: reqwest::Client,
    client_sse: reqwest::Client,
    rpc_pipe: Rc<RpcPipe>,
) {
    // If a request task was booted by this request, remove it from the list of
    // tasks when we exit.
    if let Some(id) = id {
        defer!(_ = tasks_clone.lock().unwrap().remove(&id));
    }

    use PythonToRustMessage::*;
    match rpc {
        UpdateLimit(limit) => {
            permit_manager.update_limit(limit);
        }
        Request(id, url, method, body, headers) => {
            let Ok(permit) = permit_manager.acquire().await else {
                return;
            };
            match request_bytes(client, url, method, body, headers).await {
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
        RequestSse(id, url, method, body, headers) => {
            // Ensure we send the end message whenever this block exits (though
            // we need to spawn a task to do so)
            defer!({
                let rpc_pipe = rpc_pipe.clone();
                let future = async move { rpc_pipe.write(RustToPythonMessage::SSEEnd(id)).await };
                tokio::task::spawn_local(future);
            });
            let Ok(permit) = permit_manager.acquire().await else {
                return;
            };
            match request_sse(
                client_sse,
                id,
                backpressure.unwrap(),
                url,
                method,
                body,
                headers,
                rpc_pipe.clone(),
            )
            .await
            {
                Ok(..) => {}
                Err(err) => {
                    _ = rpc_pipe
                        .write(RustToPythonMessage::Error(id, format!("SSE error: {err}")))
                        .await;
                }
            }
            drop(permit);
        }
        Ack(id) => {
            let lock = tasks_clone.lock().unwrap();
            if let Some(task) = lock.get(&id) {
                task.backpressure.add_permits(1);
            }
        }
        Close(id) => {
            let Some(task) = tasks_clone.lock().unwrap().remove(&id) else {
                return;
            };
            task.task.abort();
        }
    }
}

impl Http {
    fn send(&self, msg: PythonToRustMessage) -> PyResult<()> {
        trace!("Python -> Rust: {msg:?}");
        self.python_to_rust
            .send(msg)
            .map_err(|_| internal_error("In shutdown"))
    }
}

#[pymethods]
impl Http {
    /// Create the HTTP pool and automatically boot a tokio runtime on a
    /// new thread. When this class is GC'd, the thread will be torn down.
    #[new]
    fn new(py: Python, max_capacity: usize) -> PyResult<Self> {
        let level = get_python_logger_level(py, "edgedb.server.http")?;

        info!("Http::new(max_capacity={max_capacity})");
        let (txrp, rxrp) = std::sync::mpsc::channel();
        let (txpr, rxpr) = tokio::sync::mpsc::unbounded_channel();
        let (txfd, rxfd) = std::sync::mpsc::channel();

        thread::Builder::new()
            .name("edgedb-http".to_string())
            .spawn(move || {
                initialize_logging_in_thread("edgedb.server.http", level);
                defer!(info!("Rust-side Http thread exiting"));
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
            })
            .expect("Failed to create HTTP thread");

        let notify_fd = rxfd.recv().unwrap();
        Ok(Http {
            python_to_rust: txpr,
            rust_to_python: Mutex::new(rxrp),
            notify_fd,
        })
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
        self.send(PythonToRustMessage::Request(id, url, method, body, headers))
    }

    fn _request_sse(
        &self,
        id: PythonConnId,
        url: String,
        method: String,
        body: Vec<u8>,
        headers: Vec<(String, String)>,
    ) -> PyResult<()> {
        self.send(PythonToRustMessage::RequestSse(
            id, url, method, body, headers,
        ))
    }

    fn _close(&self, id: PythonConnId) -> PyResult<()> {
        self.send(PythonToRustMessage::Close(id))
    }

    fn _ack_sse(&self, id: PythonConnId) -> PyResult<()> {
        self.send(PythonToRustMessage::Ack(id))
    }

    fn _update_limit(&self, limit: usize) -> PyResult<()> {
        self.send(PythonToRustMessage::UpdateLimit(limit))
    }

    fn _read(&self, py: Python<'_>) -> PyResult<PyObject> {
        let Ok(msg) = self
            .rust_to_python
            .try_lock()
            .expect("Unsafe thread access")
            .recv()
        else {
            return Ok(py.None());
        };
        msg.to_object(py)
    }

    fn _try_read(&self, py: Python<'_>) -> PyResult<PyObject> {
        let Ok(msg) = self
            .rust_to_python
            .try_lock()
            .expect("Unsafe thread access")
            .try_recv()
        else {
            return Ok(py.None());
        };
        msg.to_object(py)
    }

    fn _close_pipe(&mut self) {
        trace!("Closing pipe");
        // Replace the channel with a dummy, closed one which will also
        // signal the other side to exit.
        self.rust_to_python = Mutex::new(std::sync::mpsc::channel().1);
    }
}

#[pymodule]
pub fn _http(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<Http>()?;
    m.add("InternalError", py.get_type::<InternalError>())?;

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
