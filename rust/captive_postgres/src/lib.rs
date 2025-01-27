use ephemeral_port::EphemeralPort;
use gel_auth::AuthType;
use openssl::ssl::{Ssl, SslContext, SslMethod};
use std::io::{BufReader, Write};
use std::net::{Ipv4Addr, SocketAddr};
use std::num::NonZeroUsize;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use stdio_reader::StdioReader;
use tempfile::TempDir;

mod ephemeral_port;
mod stdio_reader;

// Constants
pub const STARTUP_TIMEOUT_DURATION: Duration = Duration::from_secs(30);
pub const PORT_RELEASE_TIMEOUT: Duration = Duration::from_secs(30);
pub const LINGER_DURATION: Duration = Duration::from_secs(1);
pub const HOT_LOOP_INTERVAL: Duration = Duration::from_millis(100);
pub const DEFAULT_USERNAME: &str = "username";
pub const DEFAULT_PASSWORD: &str = "password";
pub const DEFAULT_DATABASE: &str = "postgres";

use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
pub enum PostgresBinPath {
    #[default]
    Path,
    Specified(PathBuf),
}

#[derive(Debug, Clone)]
pub struct PostgresBuilder {
    auth: AuthType,
    bin_path: PostgresBinPath,
    data_dir: Option<PathBuf>,
    server_options: HashMap<String, String>,
    ssl_cert_and_key: Option<(PathBuf, PathBuf)>,
    unix_enabled: bool,
    debug_level: Option<u8>,
    standby_of_port: Option<u16>,
}

impl Default for PostgresBuilder {
    fn default() -> Self {
        Self {
            auth: AuthType::Trust,
            bin_path: PostgresBinPath::default(),
            data_dir: None,
            server_options: HashMap::new(),
            ssl_cert_and_key: None,
            unix_enabled: false,
            debug_level: None,
            standby_of_port: None,
        }
    }
}

impl PostgresBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Attempt to configure the builder to use the default postgres binaries.
    /// Returns an error if the binaries are not found.
    pub fn with_automatic_bin_path(mut self) -> std::io::Result<Self> {
        let bindir = postgres_bin_dir()?;
        self.bin_path = PostgresBinPath::Specified(bindir);
        Ok(self)
    }

    /// Configures the builder with a quick networking mode.
    pub fn with_automatic_mode(mut self, mode: Mode) -> Self {
        match mode {
            Mode::Tcp => {
                // No special configuration needed for TCP mode
            }
            Mode::TcpSsl => {
                let certs_dir = test_data_dir().join("certs");
                let cert = certs_dir.join("server.cert.pem");
                let key = certs_dir.join("server.key.pem");
                self.ssl_cert_and_key = Some((cert, key));
            }
            Mode::Unix => {
                self.unix_enabled = true;
            }
        }
        self
    }

    pub fn auth(mut self, auth: AuthType) -> Self {
        self.auth = auth;
        self
    }

    pub fn bin_path(mut self, bin_path: impl AsRef<Path>) -> Self {
        self.bin_path = PostgresBinPath::Specified(bin_path.as_ref().to_path_buf());
        self
    }

    pub fn data_dir(mut self, data_dir: PathBuf) -> Self {
        self.data_dir = Some(data_dir);
        self
    }

    pub fn debug_level(mut self, debug_level: u8) -> Self {
        self.debug_level = Some(debug_level);
        self
    }

    pub fn server_option(mut self, key: impl AsRef<str>, value: impl AsRef<str>) -> Self {
        self.server_options
            .insert(key.as_ref().to_string(), value.as_ref().to_string());
        self
    }

    pub fn server_options(
        mut self,
        server_options: impl IntoIterator<Item = (impl AsRef<str>, impl AsRef<str>)>,
    ) -> Self {
        for (key, value) in server_options {
            self.server_options
                .insert(key.as_ref().to_string(), value.as_ref().to_string());
        }
        self
    }

    pub fn enable_ssl(mut self, cert_path: PathBuf, key_path: PathBuf) -> Self {
        self.ssl_cert_and_key = Some((cert_path, key_path));
        self
    }

    pub fn enable_unix(mut self) -> Self {
        self.unix_enabled = true;
        self
    }

    pub fn enable_standby_of(mut self, port: u16) -> Self {
        self.standby_of_port = Some(port);
        self
    }

    pub fn build(self) -> std::io::Result<PostgresProcess> {
        let initdb = match &self.bin_path {
            PostgresBinPath::Path => "initdb".into(),
            PostgresBinPath::Specified(path) => path.join("initdb"),
        };
        let postgres = match &self.bin_path {
            PostgresBinPath::Path => "postgres".into(),
            PostgresBinPath::Specified(path) => path.join("postgres"),
        };
        let pg_basebackup = match &self.bin_path {
            PostgresBinPath::Path => "pg_basebackup".into(),
            PostgresBinPath::Specified(path) => path.join("pg_basebackup"),
        };

        if !initdb.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("initdb executable not found at {}", initdb.display()),
            ));
        }
        if !postgres.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("postgres executable not found at {}", postgres.display()),
            ));
        }
        if !pg_basebackup.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "pg_basebackup executable not found at {}",
                    pg_basebackup.display()
                ),
            ));
        }

        let temp_dir = TempDir::new()?;
        let port = EphemeralPort::allocate()?;
        let data_dir = self
            .data_dir
            .unwrap_or_else(|| temp_dir.path().join("data"));

        // Create a standby signal file if requested
        if let Some(standby_of_port) = self.standby_of_port {
            run_pgbasebackup(&pg_basebackup, &data_dir, "localhost", standby_of_port)?;
            let standby_signal_path = data_dir.join("standby.signal");
            std::fs::write(&standby_signal_path, "")?;
        } else {
            init_postgres(&initdb, &data_dir, self.auth)?;
        }

        let port = port.take();

        let ssl_config = self.ssl_cert_and_key;

        let (socket_address, socket_path) = if self.unix_enabled {
            (
                ListenAddress::Unix(get_unix_socket_path(&data_dir, port)),
                Some(&data_dir),
            )
        } else {
            (
                ListenAddress::Tcp(SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port)),
                None,
            )
        };

        let tcp_address = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port);

        let mut command = Command::new(postgres);
        command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .arg("-D")
            .arg(&data_dir)
            .arg("-h")
            .arg(Ipv4Addr::LOCALHOST.to_string())
            .arg("-F")
            .arg("-p")
            .arg(port.to_string());

        if let Some(socket_path) = &socket_path {
            command.arg("-k").arg(socket_path);
        }

        for (key, value) in self.server_options {
            command.arg("-c").arg(format!("{}={}", key, value));
        }

        if let Some(debug_level) = self.debug_level {
            command.arg("-d").arg(debug_level.to_string());
        }

        let child = run_postgres(command, &data_dir, socket_path, ssl_config, port)?;

        Ok(PostgresProcess {
            child: Some(child),
            socket_address,
            tcp_address,
            temp_dir,
        })
    }
}

#[derive(Debug, Clone)]
pub enum ListenAddress {
    Tcp(SocketAddr),
    #[cfg(unix)]
    Unix(PathBuf),
}

fn spawn(command: &mut Command) -> std::io::Result<()> {
    let program = Path::new(command.get_program())
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    eprintln!("{program} command:\n  {:?}", command);
    let command = command.spawn()?;
    let output = std::thread::scope(|s| {
        use nix::sys::signal::{self, Signal};
        use nix::unistd::Pid;

        let pid = Pid::from_raw(command.id() as _);
        let handle = s.spawn(|| command.wait_with_output());
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(30) {
            if handle.is_finished() {
                let handle = handle.join().map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::Other, format!("{e:?}"))
                })??;
                return Ok(handle);
            }
            std::thread::sleep(HOT_LOOP_INTERVAL);
        }
        eprintln!("Command timed out after 30 seconds. Sending SIGKILL.");
        signal::kill(pid, Signal::SIGKILL)?;
        handle
            .join()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{e:?}")))?
    })?;
    eprintln!("{program}: {}", output.status);
    let status = output.status;
    let output_str = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let error_str = String::from_utf8_lossy(&output.stderr).trim().to_string();

    if !output_str.is_empty() {
        eprintln!("=== begin {} stdout:===", program);
        eprintln!("{}", output_str);
        if !output_str.ends_with('\n') {
            eprintln!();
        }
        eprintln!("=== end {} stdout ===", program);
    }
    if !error_str.is_empty() {
        eprintln!("=== begin {} stderr:===", program);
        eprintln!("{}", error_str);
        if !error_str.ends_with('\n') {
            eprintln!();
        }
        eprintln!("=== end {} stderr ===", program);
    }
    if output_str.is_empty() && error_str.is_empty() {
        eprintln!("{program}: No output\n");
    }
    if !status.success() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("{program} failed with: {}", status),
        ));
    }

    Ok(())
}

fn init_postgres(initdb: &Path, data_dir: &Path, auth: AuthType) -> std::io::Result<()> {
    let mut pwfile = tempfile::NamedTempFile::new()?;
    writeln!(pwfile, "{}", DEFAULT_PASSWORD)?;
    let mut command = Command::new(initdb);
    command
        .arg("-D")
        .arg(data_dir)
        .arg("-A")
        .arg(match auth {
            AuthType::Deny => "reject",
            AuthType::Trust => "trust",
            AuthType::Plain => "password",
            AuthType::Md5 => "md5",
            AuthType::ScramSha256 => "scram-sha-256",
        })
        .arg("--pwfile")
        .arg(pwfile.path())
        .arg("-U")
        .arg(DEFAULT_USERNAME)
        .arg("--no-instructions");

    spawn(&mut command)?;

    Ok(())
}

fn run_pgbasebackup(
    pg_basebackup: &Path,
    data_dir: &Path,
    host: &str,
    port: u16,
) -> std::io::Result<()> {
    let mut command = Command::new(pg_basebackup);
    // This works for testing purposes but putting passwords in the environment
    // is usually bad practice.
    //
    // "Use of this environment variable is not recommended for security
    // reasons" <https://www.postgresql.org/docs/current/libpq-envars.html>
    command.env("PGPASSWORD", DEFAULT_PASSWORD);
    command
        .arg("-D")
        .arg(data_dir)
        .arg("-h")
        .arg(host)
        .arg("-p")
        .arg(port.to_string())
        .arg("-U")
        .arg(DEFAULT_USERNAME)
        .arg("-X")
        .arg("stream")
        .arg("-w");

    spawn(&mut command)?;
    Ok(())
}

fn run_postgres(
    mut command: Command,
    data_dir: &Path,
    socket_path: Option<impl AsRef<Path>>,
    ssl: Option<(PathBuf, PathBuf)>,
    port: u16,
) -> std::io::Result<std::process::Child> {
    let socket_path = socket_path.map(|path| path.as_ref().to_owned());

    if let Some((cert_path, key_path)) = ssl {
        let postgres_cert_path = data_dir.join("server.crt");
        let postgres_key_path = data_dir.join("server.key");
        std::fs::copy(cert_path, &postgres_cert_path)?;
        std::fs::copy(key_path, &postgres_key_path)?;
        // Set permissions for the certificate and key files
        std::fs::set_permissions(&postgres_cert_path, std::fs::Permissions::from_mode(0o600))?;
        std::fs::set_permissions(&postgres_key_path, std::fs::Permissions::from_mode(0o600))?;

        // Edit pg_hba.conf to change all "host" line prefixes to "hostssl"
        let pg_hba_path = data_dir.join("pg_hba.conf");
        let content = std::fs::read_to_string(&pg_hba_path)?;
        let modified_content = content
            .lines()
            .filter(|line| !line.starts_with("#") && !line.is_empty())
            .map(|line| {
                if line.trim_start().starts_with("host") {
                    line.replacen("host", "hostssl", 1)
                } else {
                    line.to_string()
                }
            })
            .collect::<Vec<String>>()
            .join("\n");
        eprintln!("pg_hba.conf:\n==========\n{modified_content}\n==========");
        std::fs::write(&pg_hba_path, modified_content)?;

        command.arg("-l");
    }

    eprintln!("postgres command:\n  {:?}", command);
    let mut child = command.spawn()?;

    let stdout_reader = BufReader::new(child.stdout.take().expect("Failed to capture stdout"));
    let _ = StdioReader::spawn(stdout_reader, format!("pg_stdout {}", child.id()));
    let stderr_reader = BufReader::new(child.stderr.take().expect("Failed to capture stderr"));
    let stderr_reader = StdioReader::spawn(stderr_reader, format!("pg_stderr {}", child.id()));

    let start_time = Instant::now();

    let mut tcp_socket: Option<std::net::TcpStream> = None;
    let mut unix_socket: Option<std::os::unix::net::UnixStream> = None;

    let unix_socket_path = socket_path.map(|path| get_unix_socket_path(path, port));
    let tcp_socket_addr = std::net::SocketAddr::from((Ipv4Addr::LOCALHOST, port));

    let mut db_ready = false;
    let mut network_ready = false;

    while start_time.elapsed() < STARTUP_TIMEOUT_DURATION && !network_ready {
        std::thread::sleep(HOT_LOOP_INTERVAL);
        match child.try_wait() {
            Ok(Some(status)) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("PostgreSQL exited with status: {}", status),
                ))
            }
            Err(e) => return Err(e),
            _ => {}
        }
        if !db_ready && stderr_reader.contains("database system is ready to accept ") {
            eprintln!("Database is ready");
            db_ready = true;
        } else {
            continue;
        }
        if let Some(unix_socket_path) = &unix_socket_path {
            if unix_socket.is_none() {
                unix_socket = std::os::unix::net::UnixStream::connect(unix_socket_path).ok();
            }
        }
        if tcp_socket.is_none() {
            tcp_socket = std::net::TcpStream::connect(tcp_socket_addr).ok();
        }

        network_ready =
            (unix_socket_path.is_none() || unix_socket.is_some()) && tcp_socket.is_some();
    }

    // Print status for TCP/unix sockets
    if let Some(tcp) = &tcp_socket {
        eprintln!(
            "TCP socket at {tcp_socket_addr:?} bound successfully (local address was {})",
            tcp.local_addr()?
        );
    } else {
        eprintln!("TCP socket at {tcp_socket_addr:?} binding failed");
    }

    if let Some(unix_socket_path) = &unix_socket_path {
        if unix_socket.is_some() {
            eprintln!("Unix socket at {unix_socket_path:?} connected successfully");
        } else {
            eprintln!("Unix socket at {unix_socket_path:?} connection failed");
        }
    }

    if network_ready {
        return Ok(child);
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "PostgreSQL failed to start within 30 seconds",
    ))
}

fn test_data_dir() -> std::path::PathBuf {
    let cargo_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests");
    if cargo_path.exists() {
        cargo_path
    } else {
        Path::new("../../tests")
            .canonicalize()
            .expect("Failed to canonicalize tests directory path")
    }
}

fn postgres_bin_dir() -> std::io::Result<std::path::PathBuf> {
    let cargo_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../build/postgres/install/bin");
    if cargo_path.exists() {
        cargo_path.canonicalize()
    } else {
        Path::new("../../build/postgres/install/bin").canonicalize()
    }
}

fn get_unix_socket_path(socket_path: impl AsRef<Path>, port: u16) -> PathBuf {
    socket_path.as_ref().join(format!(".s.PGSQL.{}", port))
}

#[derive(Debug, Clone, Copy)]
pub enum Mode {
    Tcp,
    TcpSsl,
    Unix,
}

pub fn create_ssl_client() -> Result<Ssl, Box<dyn std::error::Error>> {
    let ssl_context = SslContext::builder(SslMethod::tls_client())?.build();
    let mut ssl = Ssl::new(&ssl_context)?;
    ssl.set_connect_state();
    Ok(ssl)
}

/// The signal to send to the server to shut it down.
///
/// <https://www.postgresql.org/docs/8.1/postmaster-shutdown.html>
#[derive(Debug, Clone, Copy)]
pub enum ShutdownSignal {
    /// "After receiving SIGTERM, the server disallows new connections, but lets
    /// existing sessions end their work normally. It shuts down only after all
    /// of the sessions terminate normally. This is the Smart Shutdown."
    Smart,
    /// "The server disallows new connections and sends all existing server
    /// processes SIGTERM, which will cause them to abort their current
    /// transactions and exit promptly. It then waits for the server processes
    /// to exit and finally shuts down. This is the Fast Shutdown."
    Fast,
    /// "This is the Immediate Shutdown, which will cause the postmaster process
    /// to send a SIGQUIT to all child processes and exit immediately, without
    /// properly shutting itself down. The child processes likewise exit
    /// immediately upon receiving SIGQUIT. This will lead to recovery (by
    /// replaying the WAL log) upon next start-up. This is recommended only in
    /// emergencies."
    Immediate,
    /// "It is best not to use SIGKILL to shut down the server. Doing so will
    /// prevent the server from releasing shared memory and semaphores, which
    /// may then have to be done manually before a new server can be started.
    /// Furthermore, SIGKILL kills the postmaster process without letting it
    /// relay the signal to its subprocesses, so it will be necessary to kill
    /// the individual subprocesses by hand as well."
    Forceful,
}

#[derive(Debug)]
pub struct PostgresCluster {
    primary: PostgresProcess,
    standbys: Vec<PostgresProcess>,
}

impl PostgresCluster {
    pub fn shutdown_timeout(
        self,
        timeout: Duration,
        signal: ShutdownSignal,
    ) -> Result<(), Vec<PostgresProcess>> {
        let mut failed = Vec::new();
        for standby in self.standbys {
            if let Err(e) = standby.shutdown_timeout(timeout, signal) {
                failed.push(e);
            }
        }
        if let Err(e) = self.primary.shutdown_timeout(timeout, signal) {
            failed.push(e);
        }
        if failed.is_empty() {
            Ok(())
        } else {
            Err(failed)
        }
    }
}

#[derive(Debug)]
pub struct PostgresProcess {
    child: Option<std::process::Child>,
    pub socket_address: ListenAddress,
    pub tcp_address: SocketAddr,
    #[allow(unused)]
    temp_dir: TempDir,
}

impl PostgresProcess {
    fn child(&self) -> &std::process::Child {
        self.child.as_ref().unwrap()
    }

    fn child_mut(&mut self) -> &mut std::process::Child {
        self.child.as_mut().unwrap()
    }

    pub fn notify_shutdown(&mut self, signal: ShutdownSignal) -> std::io::Result<()> {
        use nix::sys::signal::{self, Signal};
        use nix::unistd::Pid;

        let id = Pid::from_raw(self.child().id() as _);
        // https://www.postgresql.org/docs/8.1/postmaster-shutdown.html
        match signal {
            ShutdownSignal::Smart => signal::kill(id, Signal::SIGTERM)?,
            ShutdownSignal::Fast => signal::kill(id, Signal::SIGINT)?,
            ShutdownSignal::Immediate => signal::kill(id, Signal::SIGQUIT)?,
            ShutdownSignal::Forceful => signal::kill(id, Signal::SIGKILL)?,
        }
        Ok(())
    }

    pub fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        self.child_mut().try_wait()
    }

    /// Try to shut down, waiting up to `timeout` for the process to exit.
    pub fn shutdown_timeout(
        mut self,
        timeout: Duration,
        signal: ShutdownSignal,
    ) -> Result<std::process::ExitStatus, Self> {
        _ = self.notify_shutdown(signal);

        let id = self.child().id();

        let start = Instant::now();
        while start.elapsed() < timeout {
            if let Ok(Some(exit)) = self.child_mut().try_wait() {
                self.child = None;
                eprintln!("Process {id} died gracefully. ({exit:?})");
                return Ok(exit);
            }
            std::thread::sleep(HOT_LOOP_INTERVAL);
        }
        Err(self)
    }
}

impl Drop for PostgresProcess {
    fn drop(&mut self) {
        use nix::sys::signal::{self, Signal};
        use nix::unistd::Pid;

        let Some(mut child) = self.child.take() else {
            return;
        };

        // Create a thread to send SIGQUIT to the child process. The thread will not block
        // process exit.

        let id = Pid::from_raw(child.id() as _);
        if let Ok(Some(_)) = child.try_wait() {
            eprintln!("Process {id} already exited (crashed?).");
            return;
        }
        if let Err(e) = signal::kill(id, Signal::SIGQUIT) {
            eprintln!("Failed to send SIGQUIT to process {id}: {e:?}");
        }

        let builder = std::thread::Builder::new().name("postgres-shutdown-signal".into());
        builder
            .spawn(move || {
                // Instead of sleeping, loop and check if the child process has exited every 100ms for up to 10 seconds.
                let start = Instant::now();
                while start.elapsed() < std::time::Duration::from_secs(10) {
                    if let Ok(Some(_)) = child.try_wait() {
                        eprintln!("Process {id} died gracefully.");
                        return;
                    }
                    std::thread::sleep(HOT_LOOP_INTERVAL);
                }
                eprintln!("Process {id} did not die gracefully. Sending SIGKILL.");
                _ = signal::kill(id, Signal::SIGKILL);
            })
            .unwrap();
    }
}

/// Creates and runs a new Postgres server process in a temporary directory.
pub fn setup_postgres(auth: AuthType, mode: Mode) -> std::io::Result<Option<PostgresProcess>> {
    let builder: PostgresBuilder = PostgresBuilder::new();

    let Ok(mut builder) = builder.with_automatic_bin_path() else {
        eprintln!("Skipping test: postgres bin dir not found");
        return Ok(None);
    };

    builder = builder.auth(auth).with_automatic_mode(mode);

    let process = builder.build()?;
    Ok(Some(process))
}

pub fn create_cluster(
    auth: AuthType,
    size: NonZeroUsize,
) -> std::io::Result<Option<PostgresCluster>> {
    let builder: PostgresBuilder = PostgresBuilder::new();

    let Ok(mut builder) = builder.with_automatic_bin_path() else {
        eprintln!("Skipping test: postgres bin dir not found");
        return Ok(None);
    };

    builder = builder.auth(auth).with_automatic_mode(Mode::Tcp);

    // Primary requires the following postgres settings:
    // - wal_level = replica

    let primary = builder
        .clone()
        .server_option("wal_level", "replica")
        .build()?;
    let primary_port = primary.tcp_address.port();

    let mut cluster = PostgresCluster {
        primary,
        standbys: vec![],
    };

    // Standby requires the following postgres settings:
    // - primary_conninfo = 'host=localhost port=<port> user=postgres password=password'
    // - hot_standby = on

    for _ in 0..size.get() - 1 {
        let builder = builder.clone()
            .server_option("primary_conninfo", format!("host=localhost port={primary_port} user={DEFAULT_USERNAME} password={DEFAULT_PASSWORD}"))
            .server_option("hot_standby", "on")
            .enable_standby_of(primary_port);
        let standby = builder.build()?;
        cluster.standbys.push(standby);
    }

    Ok(Some(cluster))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{num::NonZeroUsize, path::PathBuf};

    #[test]
    fn test_builder_defaults() {
        let builder = PostgresBuilder::new();
        assert!(matches!(builder.auth, AuthType::Trust));
        assert!(matches!(builder.bin_path, PostgresBinPath::Path));
        assert!(builder.data_dir.is_none());
        assert_eq!(builder.server_options.len(), 0);
    }

    #[test]
    fn test_builder_customization() {
        let mut options = HashMap::new();
        options.insert("max_connections", "100");

        let data_dir = PathBuf::from("/tmp/pg_data");
        let bin_path = PathBuf::from("/usr/local/pgsql/bin");

        let builder = PostgresBuilder::new()
            .auth(AuthType::Md5)
            .bin_path(bin_path)
            .data_dir(data_dir.clone())
            .server_options(options);

        assert!(matches!(builder.auth, AuthType::Md5));
        assert!(matches!(builder.bin_path, PostgresBinPath::Specified(_)));
        assert_eq!(builder.data_dir.unwrap(), data_dir);
        assert_eq!(
            builder.server_options.get("max_connections").unwrap(),
            "100"
        );
    }

    #[test]
    fn test_create_cluster() {
        let Some(cluster) = create_cluster(AuthType::Md5, NonZeroUsize::new(2).unwrap()).unwrap()
        else {
            return;
        };
        assert_eq!(cluster.standbys.len(), 1);
        cluster
            .shutdown_timeout(Duration::from_secs(10), ShutdownSignal::Smart)
            .unwrap();
    }
}
