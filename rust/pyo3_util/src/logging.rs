use std::cell::RefCell;
use std::os::fd::IntoRawFd;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use pyo3::{
    prelude::*,
    types::{PyAnyMethods, PyDict},
    PyResult, Python,
};
use scopeguard::defer;
use tracing::{subscriber::DefaultGuard, Dispatch, Level};
use tracing_subscriber::{filter::LevelFilter, layer::SubscriberExt};

/// A useful tool for debugging logging.
#[macro_export]
macro_rules! debug_log_method {
    ($method_name:expr, $($arg:tt)*) => {
        if is_debug_enabled() {
            debug_log!($($arg)*);
            defer! {
                debug_log!("{} exited", $method_name);
            }
        }
    };
}

/// A simple debug logging macro that prints to stderr if debug logging is enabled.
#[macro_export]
macro_rules! debug_log {
    ($($arg:tt)*) => {
        if is_debug_enabled() {
            eprint!("LOGGING [{}]: ", std::process::id());
            eprintln!($($arg)*);
        }
    };
}

/// Initializes logging for the current thread. This function should be called
/// at the start of any new thread that needs to use logging.
///
/// Important: logging from threads involves a write to a socket and taking the GIL,
/// any may have performance impacts when logging is enabled. Disabled logging is
/// nearly free, however.
pub fn initialize_logging_in_thread(python_package: &'static str, level: LevelFilter) {
    debug_log_method!(
        "initialize_logging_in_thread",
        "Initializing logging in thread {python_package:?}"
    );
    thread_local! {
        static GUARD: RefCell<Option<DefaultGuard>> = const { RefCell::new(None) };
    }
    GUARD.with(|g| {
        debug_log_method!("initialize_logging_in_thread", "Initializing logger bridge");
        let unix_socket = get_logging_socket();

        debug_log!("Got logging socket");

        let logger_bridge = LoggerBridge {
            unix_socket,
            buffer: Mutex::new([0; 65536]),
            python_package,
        };

        let dispatch = Dispatch::new(
            tracing_subscriber::registry()
                .with(level)
                .with(logger_bridge),
        );
        *g.borrow_mut() = Some(tracing::dispatcher::set_default(&dispatch));
    });
}

static EDGEDB_RUST_PYTHON_LOGGER_DEBUG: OnceLock<bool> = OnceLock::new();

fn is_debug_enabled() -> bool {
    *EDGEDB_RUST_PYTHON_LOGGER_DEBUG.get_or_init(|| {
        std::env::var("EDGEDB_RUST_PYTHON_LOGGER_DEBUG")
            .map(|v| v == "1")
            .unwrap_or(false)
    })
}

/// The Python script that spawns the log reader thread. This runs within a blocking I/O thread
/// to avoid interaction with the asyncio event loop.
const THREAD_SCRIPT: &std::ffi::CStr = cr#"
def spawn_log_reader(fd):
    import socket
    import threading

    def _log_reader(sock):
        import socket
        import struct
        import logging

        log_cache = {}

        while True:
            try:
                # Receive entire datagram at once
                data, _ = sock.recvfrom(65536) # Max UDP datagram size
                if not data:
                    break
                    
                # Parse level (first 4 bytes) in big-endian
                level = struct.unpack('>I', data[:4])[0]
                
                # Parse first string length and data in big-endian
                str1_len = struct.unpack('>I', data[4:8])[0]
                logger_name = data[8:8+str1_len].decode('utf-8')
                
                # Parse second string length and data in big-endian
                str2_start = 8 + str1_len
                str2_len = struct.unpack('>I', data[str2_start:str2_start+4])[0]
                msg = data[str2_start+4:str2_start+4+str2_len].decode('utf-8')
                
                logger = log_cache.get(logger_name, None);
                if logger is None:
                    log_cache[logger_name] = logger = logging.getLogger(logger_name)
                logger.log(level, msg)
                
            except socket.error:
                break
        sock.close()

    sock = socket.fromfd(fd, socket.AF_UNIX, socket.SOCK_DGRAM)
    thread = threading.Thread(target=_log_reader, args=(sock,), daemon=True, name="Python/Rust logging bridge")
    thread.start()

try:
    spawn_log_reader(fd)
except Exception as e:
    import traceback
    traceback.print_exc()
    raise
"#;

fn python_to_rust_level(level: i32) -> LevelFilter {
    match level {
        ..10 => LevelFilter::TRACE,
        10 => LevelFilter::DEBUG,
        11..=20 => LevelFilter::INFO,
        21..=30 => LevelFilter::WARN,
        31..=40 => LevelFilter::ERROR,
        _ => LevelFilter::OFF,
    }
}

/// Call this from the thread that is running the Python interpreter.
pub fn get_python_logger_level(py: Python, python_package: &'static str) -> PyResult<LevelFilter> {
    let logging = py.import("logging")?;
    let logger = logging.call_method("getLogger", (python_package,), None)?;
    let level = logger.call_method("getEffectiveLevel", (), None)?;
    debug_log!("Python logger '{python_package}' level = {level:?}");
    Ok(python_to_rust_level(level.extract::<i32>()?))
}

struct LoggerBridge {
    unix_socket: std::os::unix::net::UnixDatagram,
    buffer: Mutex<[u8; 65536]>,
    python_package: &'static str,
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for LoggerBridge {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        debug_log_method!("on_event", "LoggerBridge on_event called: {event:?}");
        let mut message = format!("[{}] ", event.metadata().target());
        #[derive(Default)]
        struct Visitor(String);
        impl tracing::field::Visit for Visitor {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
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

        // Clamp the length of message to 4kB
        let message = &message[..message.len().min(4096)];

        let level = match *event.metadata().level() {
            Level::TRACE => 5,  // NOTSET
            Level::DEBUG => 10, // DEBUG
            Level::INFO => 20,  // INFO
            Level::WARN => 30,  // WARNING
            Level::ERROR => 40, // ERROR
        };

        let logger = self.python_package;
        let mut lock = self.buffer.lock().unwrap();

        // Write the level, logger, and message to the buffer
        let buf = lock.as_mut_slice();
        buf[..4].copy_from_slice(&(level as u32).to_be_bytes());
        buf[4..8].copy_from_slice(&(logger.len() as u32).to_be_bytes());
        buf[8..8 + logger.len()].copy_from_slice(logger.as_bytes());
        let str2_start = 8 + logger.len();
        buf[str2_start..str2_start + 4].copy_from_slice(&(message.len() as u32).to_be_bytes());
        buf[str2_start + 4..str2_start + 4 + message.len()].copy_from_slice(message.as_bytes());

        let total_len = str2_start + 4 + message.len();

        _ = self.unix_socket.send(&buf[..total_len]);
    }
}

static LOGGING_WRITER: OnceLock<std::os::unix::net::UnixDatagram> = OnceLock::new();

fn get_logging_socket() -> std::os::unix::net::UnixDatagram {
    debug_log_method!("get_logging_socket", "Getting logging socket");
    let tx = LOGGING_WRITER
        .get_or_init(|| {
            debug_log_method!("get_logging_socket", "Creating logging socket");
            let (tx, rx) =
                std::os::unix::net::UnixDatagram::pair().expect("Failed to create logging socket");
            let rx = rx.into_raw_fd();
            Python::with_gil(|py| {
                debug_log_method!("get_logging_socket", "Running thread script");
                let locals = PyDict::new(py);
                locals
                    .set_item("fd", rx)
                    .expect("Failed to set fd in locals");
                py.run(THREAD_SCRIPT, None, Some(&locals))
                    .expect("Failed to run thread script");
            });
            tx
        })
        .try_clone()
        .expect("Failed to clone logging socket");

    tx.set_write_timeout(Some(Duration::from_secs(10)))
        .expect("Failed to set write timeout");
    tx
}
