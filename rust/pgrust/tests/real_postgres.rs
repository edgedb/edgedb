// Constants
use gel_auth::AuthType;
use openssl::ssl::{Ssl, SslContext, SslMethod};
use pgrust::connection::dsn::{Host, HostType};
use pgrust::connection::{connect_raw_ssl, ConnectionError, Credentials, ResolvedTarget};
use pgrust::errors::PgServerError;
use pgrust::handshake::ConnectionSslRequirement;
use rstest::rstest;
use std::io::{BufRead, BufReader, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

const STARTUP_TIMEOUT_DURATION: Duration = Duration::from_secs(30);
const PORT_RELEASE_TIMEOUT: Duration = Duration::from_secs(30);
const LINGER_DURATION: Duration = Duration::from_secs(1);
const HOT_LOOP_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_USERNAME: &str = "username";
const DEFAULT_PASSWORD: &str = "password";
const DEFAULT_DATABASE: &str = "postgres";

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
    postgres_bin: &Path,
    data_dir: &Path,
    socket_path: &Path,
    ssl: Option<(PathBuf, PathBuf)>,
    port: u16,
) -> std::io::Result<std::process::Child> {
    let mut command = Command::new(postgres_bin);
    command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .arg("-D")
        .arg(data_dir)
        .arg("-k")
        .arg(socket_path)
        .arg("-h")
        .arg(Ipv4Addr::LOCALHOST.to_string())
        .arg("-F")
        // Useful for debugging
        // .arg("-d")
        // .arg("5")
        .arg("-p")
        .arg(port.to_string());

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

    let unix_socket_path = get_unix_socket_path(socket_path, port);
    let tcp_socket_addr = std::net::SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let mut db_ready = false;

    while start_time.elapsed() < STARTUP_TIMEOUT_DURATION {
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
        if unix_socket.is_none() {
            unix_socket = std::os::unix::net::UnixStream::connect(&unix_socket_path).ok();
        }
        if tcp_socket.is_none() {
            tcp_socket = std::net::TcpStream::connect(tcp_socket_addr).ok();
        }
        if unix_socket.is_some() && tcp_socket.is_some() {
            break;
        }
    }

    if unix_socket.is_some() && tcp_socket.is_some() {
        return Ok(child);
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

    Err(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "PostgreSQL failed to start within 30 seconds",
    ))
}

fn test_data_dir() -> std::path::PathBuf {
    Path::new("../../../tests")
        .canonicalize()
        .expect("Failed to canonicalize tests directory path")
}

fn postgres_bin_dir() -> std::io::Result<std::path::PathBuf> {
    Path::new("../../../build/postgres/install/bin").canonicalize()
}

fn get_unix_socket_path(socket_path: &Path, port: u16) -> PathBuf {
    socket_path.join(format!(".s.PGSQL.{}", port))
}

#[derive(Debug, Clone, Copy)]
enum Mode {
    Tcp,
    TcpSsl,
    Unix,
}

fn create_ssl_client() -> Result<Ssl, Box<dyn std::error::Error>> {
    let ssl_context = SslContext::builder(SslMethod::tls_client())?.build();
    let mut ssl = Ssl::new(&ssl_context)?;
    ssl.set_connect_state();
    Ok(ssl)
}
struct PostgresProcess {
    child: std::process::Child,
    socket_address: ResolvedTarget,
    #[allow(unused)]
    temp_dir: TempDir,
}

impl Drop for PostgresProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
    }
}

fn setup_postgres(
    auth: AuthType,
    mode: Mode,
) -> Result<Option<PostgresProcess>, Box<dyn std::error::Error>> {
    let Ok(bindir) = postgres_bin_dir() else {
        println!("Skipping test: postgres bin dir not found");
        return Ok(None);
    };

    let initdb = bindir.join("initdb");
    let postgres = bindir.join("postgres");

    if !initdb.exists() || !postgres.exists() {
        println!("Skipping test: initdb or postgres not found");
        return Ok(None);
    }

    let temp_dir = TempDir::new()?;
    let port = EphemeralPort::allocate()?;
    let data_dir = temp_dir.path().join("data");

    init_postgres(&initdb, &data_dir, auth)?;
    let ssl_key = match mode {
        Mode::TcpSsl => {
            let certs_dir = test_data_dir().join("certs");
            let cert = certs_dir.join("server.cert.pem");
            let key = certs_dir.join("server.key.pem");
            Some((cert, key))
        }
        _ => None,
    };

    let port = port.take();
    let child = run_postgres(&postgres, &data_dir, &data_dir, ssl_key, port)?;

    let socket_address = match mode {
        Mode::Unix => ResolvedTarget::to_addrs_sync(&Host(
            HostType::Path(data_dir.to_string_lossy().to_string()),
            port,
        ))?
        .remove(0),
        Mode::Tcp | Mode::TcpSsl => {
            ResolvedTarget::SocketAddr(SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port))
        }
    };

    Ok(Some(PostgresProcess {
        child,
        socket_address,
        temp_dir,
    }))
}

#[rstest]
#[tokio::test]
async fn test_auth_real(
    #[values(AuthType::Trust, AuthType::Plain, AuthType::Md5, AuthType::ScramSha256)]
    auth: AuthType,
    #[values(Mode::Tcp, Mode::TcpSsl, Mode::Unix)] mode: Mode,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(postgres_process) = setup_postgres(auth, mode)? else {
        return Ok(());
    };

    let credentials = Credentials {
        username: DEFAULT_USERNAME.to_string(),
        password: DEFAULT_PASSWORD.to_string(),
        database: DEFAULT_DATABASE.to_string(),
        server_settings: Default::default(),
    };

    let client = postgres_process.socket_address.connect().await?;

    let ssl_requirement = match mode {
        Mode::TcpSsl => ConnectionSslRequirement::Required,
        _ => ConnectionSslRequirement::Optional,
    };

    let params = connect_raw_ssl(credentials, ssl_requirement, create_ssl_client()?, client)
        .await?
        .params()
        .clone();

    assert_eq!(matches!(mode, Mode::TcpSsl), params.ssl);
    assert_eq!(auth, params.auth);

    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_bad_password(
    #[values(AuthType::Plain, AuthType::Md5, AuthType::ScramSha256)] auth: AuthType,
    #[values(Mode::Tcp, Mode::TcpSsl, Mode::Unix)] mode: Mode,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(postgres_process) = setup_postgres(auth, mode)? else {
        return Ok(());
    };

    let credentials = Credentials {
        username: DEFAULT_USERNAME.to_string(),
        password: "badpassword".to_string(),
        database: DEFAULT_DATABASE.to_string(),
        server_settings: Default::default(),
    };

    let client = postgres_process.socket_address.connect().await?;

    let ssl_requirement = match mode {
        Mode::TcpSsl => ConnectionSslRequirement::Required,
        _ => ConnectionSslRequirement::Optional,
    };

    let params = connect_raw_ssl(credentials, ssl_requirement, create_ssl_client()?, client).await;
    assert!(
        matches!(params, Err(ConnectionError::ServerError(PgServerError { code, .. })) if &code.to_code() == b"28P01")
    );

    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_bad_username(
    #[values(AuthType::Plain, AuthType::Md5, AuthType::ScramSha256)] auth: AuthType,
    #[values(Mode::Tcp, Mode::TcpSsl, Mode::Unix)] mode: Mode,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(postgres_process) = setup_postgres(auth, mode)? else {
        return Ok(());
    };

    let credentials = Credentials {
        username: "badusername".to_string(),
        password: DEFAULT_PASSWORD.to_string(),
        database: DEFAULT_DATABASE.to_string(),
        server_settings: Default::default(),
    };

    let client = postgres_process.socket_address.connect().await?;

    let ssl_requirement = match mode {
        Mode::TcpSsl => ConnectionSslRequirement::Required,
        _ => ConnectionSslRequirement::Optional,
    };

    let params = connect_raw_ssl(credentials, ssl_requirement, create_ssl_client()?, client).await;
    assert!(
        matches!(params, Err(ConnectionError::ServerError(PgServerError { code, .. })) if &code.to_code() == b"28P01")
    );

    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_bad_database(
    #[values(AuthType::Plain, AuthType::Md5, AuthType::ScramSha256)] auth: AuthType,
    #[values(Mode::Tcp, Mode::TcpSsl, Mode::Unix)] mode: Mode,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(postgres_process) = setup_postgres(auth, mode)? else {
        return Ok(());
    };

    let credentials = Credentials {
        username: DEFAULT_USERNAME.to_string(),
        password: DEFAULT_PASSWORD.to_string(),
        database: "baddatabase".to_string(),
        server_settings: Default::default(),
    };

    let client = postgres_process.socket_address.connect().await?;

    let ssl_requirement = match mode {
        Mode::TcpSsl => ConnectionSslRequirement::Required,
        _ => ConnectionSslRequirement::Optional,
    };

    let params = connect_raw_ssl(credentials, ssl_requirement, create_ssl_client()?, client).await;
    assert!(
        matches!(params, Err(ConnectionError::ServerError(PgServerError { code, .. })) if &code.to_code() == b"3D000")
    );

    Ok(())
}
