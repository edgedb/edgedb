use crate::{
    connection::{
        dsn::{ConnectionParameters, RawConnectionParameters, SslMode, *},
        Credentials, PGConnectionError,
    },
    errors::PgServerError,
    handshake::{
        client::{
            ConnectionDrive, ConnectionState, ConnectionStateSend, ConnectionStateType,
            ConnectionStateUpdate,
        },
        ConnectionSslRequirement,
    },
    protocol::postgres::{data::SSLResponse, meta, FrontendBuilder, InitialBuilder},
};
use db_proto::StructBuffer;
use gel_stream::client::ResolvedTarget;
use pyo3::{
    buffer::PyBuffer,
    exceptions::{PyException, PyRuntimeError},
    prelude::*,
    pymodule,
    types::{PyAnyMethods, PyByteArray, PyBytes, PyMemoryView, PyModule, PyModuleMethods},
    Bound, PyAny, PyResult, Python,
};
use std::collections::HashMap;
use std::{borrow::Cow, path::Path};
use tracing::warn;

#[derive(Clone, Copy, PartialEq, Eq)]
#[pyclass(eq, eq_int)]
pub enum SSLMode {
    Disable,
    Allow,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

impl From<PGConnectionError> for PyErr {
    fn from(err: PGConnectionError) -> PyErr {
        PyRuntimeError::new_err(err.to_string())
    }
}

impl From<ParseError> for PyErr {
    fn from(err: ParseError) -> PyErr {
        PyRuntimeError::new_err(err.to_string())
    }
}

impl EnvVar for (String, Bound<'_, PyAny>) {
    fn read(&self, name: &'static str) -> Option<std::borrow::Cow<str>> {
        // os.environ[name], or the default user if not
        let py_str = self.1.get_item(name).ok();
        if name == "PGUSER" && py_str.is_none() {
            Some((&self.0).into())
        } else {
            py_str.map(|s| s.to_string().into())
        }
    }
}

#[pyclass]
struct PyConnectionParams {
    inner: RawConnectionParameters<'static>,
}

#[pymethods]
impl PyConnectionParams {
    #[new]
    #[pyo3(signature = (dsn=None))]
    fn new(dsn: Option<String>) -> PyResult<Self> {
        if let Some(dsn_str) = dsn {
            match parse_postgres_dsn(&dsn_str) {
                Ok(params) => Ok(PyConnectionParams {
                    inner: params.to_static(),
                }),
                Err(err) => Err(PyException::new_err(err.to_string())),
            }
        } else {
            Ok(PyConnectionParams {
                inner: RawConnectionParameters::default(),
            })
        }
    }

    #[getter]
    #[allow(clippy::type_complexity)]
    pub fn host_candidates(
        &self,
        py: Python,
    ) -> PyResult<Vec<(&'static str, Py<PyAny>, String, u16)>> {
        // As this might be blocking, drop the GIL while we allow for
        // resolution to take place.
        let hosts = self.inner.hosts()?;
        let hosts = py.allow_threads(|| hosts.to_addrs_sync());
        let mut errors = Vec::new();
        let mut resolved_hosts = Vec::new();

        for (host, resolved) in hosts {
            let hostname = host.0.to_string();
            let port = host.1;
            match resolved {
                Ok(addrs) => {
                    for addr in addrs {
                        match addr {
                            ResolvedTarget::SocketAddr(addr) => {
                                resolved_hosts.push((
                                    if addr.ip().is_ipv4() { "v4" } else { "v6" },
                                    addr.ip().to_string().into_pyobject(py)?.into(),
                                    hostname.clone(),
                                    addr.port(),
                                ));
                            }
                            #[cfg(unix)]
                            ResolvedTarget::UnixSocketAddr(path) => {
                                if let Some(path) = path.as_pathname() {
                                    resolved_hosts.push((
                                        "unix",
                                        path.to_string_lossy().into_pyobject(py)?.into(),
                                        hostname.clone(),
                                        port,
                                    ));
                                    continue;
                                }

                                #[cfg(target_os = "linux")]
                                {
                                    use std::os::linux::net::SocketAddrExt;
                                    if let Some(name) = path.as_abstract_name() {
                                        let mut name = name.to_vec();
                                        name.insert(0, 0);
                                        resolved_hosts.push((
                                            "unix",
                                            PyBytes::new(py, &name).as_any().clone().unbind(),
                                            hostname.clone(),
                                            port,
                                        ));
                                        continue;
                                    }
                                }

                                unreachable!()
                            }
                        }
                    }
                }
                Err(err) => errors.push(err),
            }
        }

        if resolved_hosts.is_empty() {
            return Err(PGConnectionError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Could not resolve addresses: {errors:?}"),
            ))
            .into());
        }

        Ok(resolved_hosts)
    }

    #[getter]
    pub fn keys(&self) -> Vec<&str> {
        RawConnectionParameters::field_names()
    }

    pub fn to_dict(&self) -> HashMap<String, String> {
        self.inner.clone().into()
    }

    pub fn update_server_settings(&mut self, key: &str, value: &str) -> PyResult<()> {
        self.inner
            .server_settings
            .get_or_insert_with(HashMap::new)
            .insert(key.to_string().into(), value.to_string().into());
        Ok(())
    }

    pub fn clear_server_settings(&mut self) -> PyResult<()> {
        if let Some(server_settings) = &mut self.inner.server_settings {
            server_settings.clear();
        }
        Ok(())
    }

    pub fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }

    pub fn resolve(&self, py: Python, username: String, home_dir: String) -> PyResult<Self> {
        let os = py.import("os")?;
        let environ = os.getattr("environ")?;

        let mut params = self.inner.clone();
        params
            .apply_env((username.clone(), environ))
            .map_err(|err| PyException::new_err(err.to_string()))?;
        let mut params = ConnectionParameters::try_from(params)
            .map_err(|err| PyException::new_err(err.to_string()))?;
        if let Some(warning) = params.password.resolve(
            Path::new(&home_dir),
            &params.hosts,
            &params.database,
            &params.user,
        )? {
            let warnings = py.import("warnings")?;
            warnings.call_method1("warn", (warning.to_string(),))?;
        }

        params
            .ssl
            .resolve(Path::new(&home_dir))
            .map_err(|err| PyException::new_err(err.to_string()))?;
        Ok(Self {
            inner: params.into(),
        })
    }

    pub fn to_dsn(&self) -> String {
        self.inner.to_url()
    }

    fn __repr__(&self) -> String {
        let field_names = RawConnectionParameters::field_names();
        let mut repr = "<ConnectionParams".to_owned();
        for field_name in field_names {
            let value = self.inner.get_by_name(field_name);
            let Some(value) = value else {
                continue;
            };
            repr.push_str(&format!(" {}={}", field_name, value));
        }
        if let Some(server_settings) = &self.inner.server_settings {
            repr.push_str(&format!(" server_settings={server_settings:?}"));
        }
        repr.push('>');
        repr
    }

    pub fn __getitem__(&self, name: &str) -> Option<Cow<'_, str>> {
        self.inner.get_by_name(name)
    }

    pub fn __setitem__(&mut self, name: &str, value: &str) -> PyResult<()> {
        self.inner
            .set_by_name(name, value.to_string().into())
            .map_err(|e| PyException::new_err(e.to_string()))?;
        Ok(())
    }
}

#[pymodule]
pub fn _pg_rust(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<PyConnectionParams>()?;
    m.add_class::<PyConnectionState>()?;
    m.add_class::<SSLMode>()?;
    Ok(())
}

#[pyclass]
struct PyConnectionState {
    inner: ConnectionState,
    parsed_dsn: Py<PyConnectionParams>,
    update: PyConnectionStateUpdate,
    message_buffer: StructBuffer<meta::Message>,
}

#[pymethods]
impl PyConnectionState {
    #[new]
    fn new(
        py: Python,
        dsn: &PyConnectionParams,
        username: String,
        home_dir: String,
    ) -> PyResult<Self> {
        let os = py.import("os")?;
        let environ = os.getattr("environ")?;

        let mut params = dsn.inner.clone();
        params
            .apply_env((username.clone(), environ))
            .map_err(|err| PyException::new_err(err.to_string()))?;
        let mut params = ConnectionParameters::try_from(params)
            .map_err(|err| PyException::new_err(err.to_string()))?;
        if let Some(warning) = params.password.resolve(
            Path::new(&home_dir),
            &params.hosts,
            &params.database,
            &params.user,
        )? {
            let warnings = py.import("warnings")?;
            warnings.call_method1("warn", (warning.to_string(),))?;
        }

        params
            .ssl
            .resolve(Path::new(&home_dir))
            .map_err(|err| PyException::new_err(err.to_string()))?;
        let credentials = Credentials {
            username: params.user.clone(),
            password: params.password.password().unwrap_or_default().to_string(),
            database: params.database.clone(),
            server_settings: params.server_settings.clone(),
        };
        let ssl_mode = match params.ssl {
            Ssl::Disable => ConnectionSslRequirement::Disable,
            Ssl::Enable(SslMode::Allow | SslMode::Prefer, ..) => ConnectionSslRequirement::Optional,
            _ => ConnectionSslRequirement::Required,
        };
        let params = params.into();
        Ok(PyConnectionState {
            inner: ConnectionState::new(credentials, ssl_mode),
            parsed_dsn: Py::new(py, PyConnectionParams { inner: params })?,
            update: PyConnectionStateUpdate {
                py_update: py.None(),
            },
            message_buffer: Default::default(),
        })
    }

    #[setter]
    fn update(&mut self, update: &Bound<PyAny>) {
        self.update.py_update = update.clone().unbind();
    }

    fn is_ready(&self) -> bool {
        self.inner.is_ready()
    }

    fn read_ssl_response(&self) -> bool {
        self.inner.read_ssl_response()
    }

    fn drive_initial(&mut self) -> PyResult<()> {
        self.inner
            .drive(ConnectionDrive::Initial, &mut self.update)?;
        Ok(())
    }

    fn drive_message(&mut self, py: Python, data: &Bound<PyMemoryView>) -> PyResult<()> {
        let buffer = PyBuffer::<u8>::get(data)?;
        if self.inner.read_ssl_response() {
            // SSL responses are always one character
            let response = [buffer.as_slice(py).unwrap().first().unwrap().get()];
            let response =
                SSLResponse::new(&response).map_err(|e| PyException::new_err(e.to_string()))?;
            self.inner
                .drive(ConnectionDrive::SslResponse(response), &mut self.update)?;
        } else {
            with_python_buffer(py, buffer, |buf| {
                self.message_buffer.push_fallible(buf, |message| {
                    self.inner
                        .drive(ConnectionDrive::Message(message), &mut self.update)
                })
            })?;
        }
        Ok(())
    }

    fn drive_ssl_ready(&mut self) -> PyResult<()> {
        self.inner
            .drive(ConnectionDrive::SslReady, &mut self.update)?;
        Ok(())
    }

    #[getter]
    fn config(&self, py: Python) -> PyResult<Py<PyConnectionParams>> {
        Ok(self.parsed_dsn.clone_ref(py))
    }
}

/// Attempt to stack-copy the data from a `PyBuffer`.
#[inline(always)]
fn with_python_buffer<T>(py: Python, data: PyBuffer<u8>, mut f: impl FnMut(&[u8]) -> T) -> T {
    let len = data.item_count();
    if len <= 128 {
        let mut slice = [0; 128];
        data.copy_to_slice(py, &mut slice[..len]).unwrap();
        f(&slice[..len])
    } else if len <= 1024 {
        let mut slice = [0; 1024];
        data.copy_to_slice(py, &mut slice[..len]).unwrap();
        f(&slice[..len])
    } else {
        f(&data.to_vec(py).unwrap())
    }
}

struct PyConnectionStateUpdate {
    py_update: Py<PyAny>,
}

impl ConnectionStateSend for PyConnectionStateUpdate {
    fn send_initial(&mut self, message: InitialBuilder) -> Result<(), std::io::Error> {
        Python::with_gil(|py| {
            let bytes = PyByteArray::new(py, &message.to_vec());
            if let Err(e) = self.py_update.call_method1(py, "send", (bytes,)) {
                eprintln!("Error in send_initial: {:?}", e);
                e.print(py);
            }
        });
        Ok(())
    }

    fn send(&mut self, message: FrontendBuilder) -> Result<(), std::io::Error> {
        Python::with_gil(|py| {
            let bytes = PyBytes::new(py, &message.to_vec());
            if let Err(e) = self.py_update.call_method1(py, "send", (bytes,)) {
                eprintln!("Error in send: {:?}", e);
                e.print(py);
            }
        });
        Ok(())
    }

    fn upgrade(&mut self) -> Result<(), std::io::Error> {
        Python::with_gil(|py| {
            if let Err(e) = self.py_update.call_method0(py, "upgrade") {
                eprintln!("Error in upgrade: {:?}", e);
                e.print(py);
            }
        });
        Ok(())
    }
}

impl ConnectionStateUpdate for PyConnectionStateUpdate {
    fn parameter(&mut self, name: &str, value: &str) {
        Python::with_gil(|py| {
            if let Err(e) = self.py_update.call_method1(py, "parameter", (name, value)) {
                eprintln!("Error in parameter: {:?}", e);
                e.print(py);
            }
        });
    }

    fn cancellation_key(&mut self, pid: i32, key: i32) {
        Python::with_gil(|py| {
            if let Err(e) = self
                .py_update
                .call_method1(py, "cancellation_key", (pid, key))
            {
                eprintln!("Error in cancellation_key: {:?}", e);
                e.print(py);
            }
        });
    }

    fn state_changed(&mut self, state: ConnectionStateType) {
        Python::with_gil(|py| {
            if let Err(e) = self
                .py_update
                .call_method1(py, "state_changed", (state as u8,))
            {
                eprintln!("Error in state_changed: {:?}", e);
                e.print(py);
            }
        });
    }

    fn auth(&mut self, auth: gel_auth::AuthType) {
        Python::with_gil(|py| {
            if let Err(e) = self.py_update.call_method1(py, "auth", (auth as u8,)) {
                eprintln!("Error in auth: {:?}", e);
                e.print(py);
            }
        });
    }

    fn server_notice(&mut self, notice: &PgServerError) {
        warn!("Unexpected server notice during handshake: {:?}", notice);
    }

    fn server_error(&mut self, error: &PgServerError) {
        Python::with_gil(|py| {
            let mut fields = vec![];
            for (field, value) in error.fields() {
                let etype = field as u8 as char;
                let message = value.to_string();
                fields.push((etype, message));
            }
            if let Err(e) = self.py_update.call_method1(py, "server_error", (fields,)) {
                eprintln!("Error in server_error: {:?}", e);
                e.print(py);
            }
        });
    }
}
