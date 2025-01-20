// Constants
use gel_auth::AuthType;
use openssl::ssl::{Ssl, SslContext, SslMethod};
use std::io::{BufRead, BufReader, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

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

#[derive(Debug)]
pub struct PostgresBuilder {
    auth: AuthType,
    bin_path: PostgresBinPath,
    data_dir: Option<PathBuf>,
    server_options: HashMap<String, String>,
    ssl_cert_and_key: Option<(PathBuf, PathBuf)>,
    unix_enabled: bool,
    debug_level: Option<u8>,
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

    pub fn build(self) -> std::io::Result<PostgresProcess> {
        let initdb = match &self.bin_path {
            PostgresBinPath::Path => "initdb".into(),
            PostgresBinPath::Specified(path) => path.join("initdb"),
        };
        let postgres = match &self.bin_path {
            PostgresBinPath::Path => "postgres".into(),
            PostgresBinPath::Specified(path) => path.join("postgres"),
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

        let temp_dir = TempDir::new()?;
        let port = EphemeralPort::allocate()?;
        let data_dir = self
            .data_dir
            .unwrap_or_else(|| temp_dir.path().join("data"));

        init_postgres(&initdb, &data_dir, self.auth)?;
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
            child,
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

/// Represents an ephemeral port that can be allocated and released for immediate re-use by another process.
struct EphemeralPort {
    port: u16,
    listener: Option<TcpListener>,
}

impl EphemeralPort {
    /// Allocates a new ephemeral port.
    ///
    /// Returns a Result containing the EphemeralPort if successful,
    /// or an IO error if the allocation fails.
    fn allocate() -> std::io::Result<Self> {
        let socket = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None)?;
        socket.set_reuse_address(true)?;
        socket.set_reuse_port(true)?;
        socket.set_linger(Some(LINGER_DURATION))?;
        socket.bind(&std::net::SocketAddr::from((Ipv4Addr::LOCALHOST, 0)).into())?;
        socket.listen(1)?;
        let listener = TcpListener::from(socket);
        let port = listener.local_addr()?.port();
        Ok(EphemeralPort {
            port,
            listener: Some(listener),
        })
    }

    /// Consumes the EphemeralPort and returns the allocated port number.
    fn take(self) -> u16 {
        // Drop the listener to free up the port
        drop(self.listener);

        // Loop until the port is free
        let start = Instant::now();

        // If we can successfully connect to the port, it's not fully closed
        while start.elapsed() < PORT_RELEASE_TIMEOUT {
            let res = std::net::TcpStream::connect((Ipv4Addr::LOCALHOST, self.port));
            if res.is_err() {
                // If connection fails, the port is released
                break;
            }
            std::thread::sleep(HOT_LOOP_INTERVAL);
        }

        self.port
    }
}

struct StdioReader {
    output: Arc<RwLock<String>>,
}

impl StdioReader {
    fn spawn<R: BufRead + Send + 'static>(reader: R, prefix: &'static str) -> Self {
        let output = Arc::new(RwLock::new(String::new()));
        let output_clone = Arc::clone(&output);

        thread::spawn(move || {
            let mut buf_reader = std::io::BufReader::new(reader);
            loop {
                let mut line = String::new();
                match buf_reader.read_line(&mut line) {
                    Ok(0) => break,
                    Ok(_) => {
                        if let Ok(mut output) = output_clone.write() {
                            output.push_str(&line);
                        }
                        eprint!("[{}]: {}", prefix, line);
                    }
                    Err(e) => {
                        let error_line = format!("Error reading {}: {}\n", prefix, e);
                        if let Ok(mut output) = output_clone.write() {
                            output.push_str(&error_line);
                        }
                        eprintln!("{}", error_line);
                    }
                }
            }
        });

        StdioReader { output }
    }

    fn contains(&self, s: &str) -> bool {
        if let Ok(output) = self.output.read() {
            output.contains(s)
        } else {
            false
        }
    }
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
        .arg(DEFAULT_USERNAME);

    eprintln!("initdb command: {:?}", command);
    let output = command.output()?;

    let status = output.status;
    let output_str = String::from_utf8_lossy(&output.stdout).to_string();
    let error_str = String::from_utf8_lossy(&output.stderr).to_string();

    eprintln!("initdb stdout:\n{}", output_str);
    eprintln!("initdb stderr:\n{}", error_str);

    if !status.success() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "initdb command failed",
        ));
    }

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

    let mut child = command.spawn()?;

    let stdout_reader = BufReader::new(child.stdout.take().expect("Failed to capture stdout"));
    let _ = StdioReader::spawn(stdout_reader, "stdout");
    let stderr_reader = BufReader::new(child.stderr.take().expect("Failed to capture stderr"));
    let stderr_reader = StdioReader::spawn(stderr_reader, "stderr");

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
        if !db_ready && stderr_reader.contains("database system is ready to accept connections") {
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
            "TCP socket at {tcp_socket_addr:?} bound successfully on {}",
            tcp.local_addr()?
        );
    } else {
        eprintln!("TCP socket at {tcp_socket_addr:?} binding failed");
    }

    if unix_socket.is_some() {
        eprintln!("Unix socket at {unix_socket_path:?} connected successfully");
    } else {
        eprintln!("Unix socket at {unix_socket_path:?} connection failed");
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

pub struct PostgresProcess {
    child: std::process::Child,
    pub socket_address: ListenAddress,
    pub tcp_address: SocketAddr,
    #[allow(unused)]
    temp_dir: TempDir,
}

impl Drop for PostgresProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

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
}
