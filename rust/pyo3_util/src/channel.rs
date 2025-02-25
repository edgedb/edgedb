use std::{
    cell::RefCell,
    future::poll_fn,
    os::fd::IntoRawFd,
    pin::Pin,
    sync::{Arc, Mutex},
};

use pyo3::{
    exceptions::PyException, prelude::*, BoundObject, FromPyObject, IntoPyObject, PyAny, PyResult,
};
use tokio::io::AsyncWrite;
use tracing::{error, trace};

pyo3::create_exception!(_channel, InternalError, PyException);

fn internal_error(message: &str) -> PyErr {
    error!("{message}");
    InternalError::new_err(())
}

pub trait RustToPython: for<'py> IntoPyObject<'py> + Send + std::fmt::Debug {}
pub trait PythonToRust: for<'py> FromPyObject<'py> + Send + std::fmt::Debug {}

impl<T> RustToPython for T where T: for<'py> IntoPyObject<'py> + Send + std::fmt::Debug {}
impl<T> PythonToRust for T where T: for<'py> FromPyObject<'py> + Send + std::fmt::Debug {}

/// A channel that can be used to send and receive messages between Rust and Python.
pub struct RustChannel<RX: for<'py> FromPyObject<'py>, TX: for<'py> IntoPyObject<'py> + Send> {
    rust_to_python_notify: RefCell<tokio::net::unix::pipe::Sender>,
    rust_to_python: std::sync::mpsc::Sender<TX>,
    python_to_rust: RefCell<tokio::sync::mpsc::UnboundedReceiver<RX>>,
}

impl<RX: PythonToRust, TX: RustToPython> RustChannel<RX, TX> {
    pub async fn recv(&self) -> Option<RX> {
        // Don't hold the lock across the await point
        poll_fn(|cx| {
            let pipe = &mut *self.python_to_rust.borrow_mut();
            let mut this = Pin::new(pipe);
            this.poll_recv(cx)
        })
        .await
    }

    pub async fn write(&self, msg: TX) -> Result<(), String> {
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

pub struct PythonChannelImpl<RX: PythonToRust, TX: RustToPython> {
    python_to_rust: tokio::sync::mpsc::UnboundedSender<RX>,
    rust_to_python: Mutex<std::sync::mpsc::Receiver<TX>>,
    notify_fd: u64,
}

impl<RX: PythonToRust, TX: RustToPython> PythonChannelImpl<RX, TX> {
    pub fn send(&self, msg: RX) -> Result<(), RX> {
        self.python_to_rust.send(msg).map_err(|e| e.0)
    }

    pub fn send_err(&self, msg: RX) -> PyResult<()> {
        self.python_to_rust
            .send(msg)
            .map_err(|_| internal_error("In shutdown"))
    }
}

pub trait PythonChannelProtocol: Send + Sync {
    fn _write(&self, py: Python<'_>, msg: Py<PyAny>) -> PyResult<()>;
    fn _read<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>>;
    fn _try_read<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>>;
    fn _close_pipe(&mut self);
    fn _fd(&self) -> u64;
}

impl<RX: PythonToRust, TX: RustToPython> PythonChannelProtocol for Arc<PythonChannelImpl<RX, TX>> {
    fn _write(&self, py: Python<'_>, msg: Py<PyAny>) -> PyResult<()> {
        let msg = msg.extract(py)?;
        trace!("Python -> Rust: {msg:?}");
        self.python_to_rust
            .send(msg)
            .map_err(|_| internal_error("In shutdown"))
    }

    fn _read<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let Ok(msg) = self
            .rust_to_python
            .try_lock()
            .expect("Unsafe thread access")
            .try_recv()
        else {
            return Ok(py.None().into_bound(py));
        };
        Ok(msg
            .into_pyobject(py)
            .map_err(|e| e.into())?
            .into_bound()
            .into_any())
    }

    fn _try_read<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let Ok(msg) = self
            .rust_to_python
            .try_lock()
            .expect("Unsafe thread access")
            .try_recv()
        else {
            return Ok(py.None().into_bound(py));
        };

        Ok(msg
            .into_pyobject(py)
            .map_err(|e| e.into())?
            .into_bound()
            .into_any())
    }

    fn _close_pipe(&mut self) {
        *self
            .rust_to_python
            .try_lock()
            .expect("Unsafe thread access") = std::sync::mpsc::channel().1;
    }

    fn _fd(&self) -> u64 {
        self.notify_fd
    }
}

#[pyclass]
pub struct PythonChannel {
    _impl: Box<dyn PythonChannelProtocol>,
}

impl PythonChannel {
    pub fn new<T: PythonChannelProtocol + 'static>(imp: T) -> Self {
        Self {
            _impl: Box::new(imp),
        }
    }
}

#[pymethods]
impl PythonChannel {
    fn _write(&self, py: Python<'_>, msg: Py<PyAny>) -> PyResult<()> {
        self._impl._write(py, msg)
    }

    fn _read<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self._impl._read(py)
    }

    fn _try_read<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self._impl._try_read(py)
    }

    fn _close_pipe(&mut self) {
        // Replace the channel with a dummy, closed one which will also
        // signal the other side to exit.
        self._impl._close_pipe()
    }

    #[getter]
    fn _fd(&self) -> u64 {
        self._impl._fd()
    }
}

/// Create a new Python <-> Rust channel from within a tokio runtime.
pub fn new_python_channel<RX: PythonToRust, TX: RustToPython>(
) -> (RustChannel<RX, TX>, PythonChannelImpl<RX, TX>) {
    let (tx_sync, rx_sync) = std::sync::mpsc::channel();
    let (tx_async, rx_async) = tokio::sync::mpsc::unbounded_channel();
    let (tx_pipe, rx_pipe) = tokio::net::unix::pipe::pipe().unwrap();
    let notify_fd = rx_pipe.into_nonblocking_fd().unwrap().into_raw_fd() as u64;
    let rust = RustChannel {
        rust_to_python_notify: RefCell::new(tx_pipe),
        rust_to_python: tx_sync,
        python_to_rust: RefCell::new(rx_async),
    };
    let python = PythonChannelImpl {
        python_to_rust: tx_async,
        rust_to_python: Mutex::new(rx_sync),
        notify_fd,
    };
    (rust, python)
}
