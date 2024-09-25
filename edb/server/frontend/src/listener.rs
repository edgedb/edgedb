#![doc = "../README.md"]

use crate::{
    config::{ListenerAddress, SslConfig},
    hyper::HyperUpgradedStream,
    service::{
        AuthResult, AuthTarget, BabelfishService, BranchDB, ConnectionIdentityBuilder,
        StreamLanguage,
    },
    stream::{ListenerStream, StreamProperties, TransportType},
    stream_type::{
        identify_stream, negotiate_alpn, negotiate_ws_protocol, PostgresInitialMessage, StreamType,
        UnknownStreamType,
    },
};
use futures::StreamExt;
use hyper::{upgrade::OnUpgrade, Request, Response, StatusCode, Version};
use openssl::ssl::{AlpnError, NameType, SniError, Ssl, SslAlert, SslContext, SslMethod};
use pgrust::protocol::InitialMessage;
use scopeguard::defer;
use std::sync::OnceLock;
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    hash::RandomState,
    io::ErrorKind,
    iter::FromIterator,
    pin::{pin, Pin},
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UnixListener, UnixStream},
};
use tracing::{error, info, trace, warn};

use crate::config::ListenerConfig;

struct SSLExData {
    identity: ConnectionIdentityBuilder,
    stream_props: Arc<StreamProperties>,
}

static SSL_EX_DATA_INDEX: OnceLock<openssl::ex_data::Index<Ssl, SSLExData>> = OnceLock::new();

fn get_ssl_ex_data_index() -> openssl::ex_data::Index<Ssl, SSLExData> {
    *SSL_EX_DATA_INDEX
        .get_or_init(|| Ssl::new_ex_index().expect("Failed to create SSL ex_data index"))
}

async fn handle_ws_upgrade_http1(
    stream_props: Arc<StreamProperties>,
    identity: ConnectionIdentityBuilder,
    mut req: Request<hyper::body::Incoming>,
    bound_config: impl IsBoundConfig,
) -> Result<Response<String>, std::io::Error> {
    let mut stream_props = StreamProperties {
        parent: Some(stream_props),
        http_version: Some(req.version()),
        ..StreamProperties::new(TransportType::WebSocket)
    };

    let mut ws_key = None;
    let mut ws_version = None;
    let mut ws_protocol = None;

    if let Some(upgrade) = req.headers().get(hyper::header::UPGRADE) {
        if upgrade.as_bytes().eq_ignore_ascii_case(b"websocket") {
            ws_key = req
                .headers()
                .get(hyper::header::SEC_WEBSOCKET_KEY)
                .map(|v| v.to_str().unwrap_or("").to_string());
            ws_version = req
                .headers()
                .get(hyper::header::SEC_WEBSOCKET_VERSION)
                .map(|v| v.to_str().unwrap_or("").to_string());
            ws_protocol = req
                .headers()
                .get(hyper::header::SEC_WEBSOCKET_PROTOCOL)
                .map(|v| v.to_str().unwrap_or("").to_string());
        }
    }

    stream_props.request_headers = Some(std::mem::take(req.headers_mut()));

    if let (Some(key), Some(version)) = (ws_key, ws_version) {
        trace!("WebSocket upgrade request detected:");
        trace!("  Key: {}", key);
        trace!("  Version: {}", version);
        if let Some(protocol) = &ws_protocol {
            trace!("  Protocol: {}", protocol);
            stream_props.protocol =
                negotiate_ws_protocol(bound_config.config().as_ref(), protocol, &stream_props);
        }

        if stream_props.protocol.is_none() {
            return Ok(Response::builder()
                .status(StatusCode::FORBIDDEN)
                .body("Invalid WebSocket upgrade request".to_string())
                .unwrap());
        }

        tokio::task::spawn(async move {
            if let Ok(upgraded) = hyper::upgrade::on(req).await {
                let stream =
                    ListenerStream::new_websocket(stream_props, HyperUpgradedStream::new(upgraded));
                if let Err(err) = handle_ws_upgrade(stream, identity, bound_config).await {
                    error!("WebSocket task failed {err:?}");
                }
            }
        });

        Ok(Response::builder()
            .status(StatusCode::SWITCHING_PROTOCOLS)
            .header(hyper::header::UPGRADE, "websocket")
            .header(hyper::header::CONNECTION, "Upgrade")
            .header(
                hyper::header::SEC_WEBSOCKET_ACCEPT,
                generate_ws_accept(&key),
            )
            .body("Switching to WebSocket".to_string())
            .unwrap())
    } else {
        Ok(Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body("Invalid WebSocket upgrade request".to_string())
            .unwrap())
    }
}

fn generate_ws_accept(key: &str) -> String {
    use base64::{engine::general_purpose, Engine as _};
    use sha1::{Digest, Sha1};

    let mut sha1 = Sha1::new();
    sha1.update(key.as_bytes());
    sha1.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    let result = sha1.finalize();
    general_purpose::STANDARD.encode(result)
}

async fn handle_ws_upgrade_http2(
    stream_props: Arc<StreamProperties>,
    identity: ConnectionIdentityBuilder,
    mut req: Request<hyper::body::Incoming>,
    bound_config: impl IsBoundConfig,
) -> Result<Response<String>, std::io::Error> {
    let mut stream_props = StreamProperties {
        parent: Some(stream_props),
        http_version: Some(req.version()),
        ..StreamProperties::new(TransportType::WebSocket)
    };
    if let Some(protocol) = req.extensions().get::<hyper::ext::Protocol>() {
        if protocol.as_str().eq_ignore_ascii_case("websocket") {
            let ws_version = req
                .headers()
                .get(hyper::header::SEC_WEBSOCKET_VERSION)
                .map(|v| v.to_str().unwrap_or("").to_string());
            let ws_protocol = req
                .headers()
                .get(hyper::header::SEC_WEBSOCKET_PROTOCOL)
                .map(|v| v.to_str().unwrap_or("").to_string());
            stream_props.request_headers = Some(std::mem::take(req.headers_mut()));

            if let Some(version) = ws_version {
                trace!("HTTP/2 WebSocket upgrade request detected:");
                trace!("  Version: {}", version);
                if let Some(protocol) = &ws_protocol {
                    trace!("  Protocol: {}", protocol);
                    stream_props.protocol = negotiate_ws_protocol(
                        bound_config.config().as_ref(),
                        protocol,
                        &stream_props,
                    );
                }
            }

            if stream_props.protocol.is_none() {
                return Ok(Response::builder()
                    .status(StatusCode::FORBIDDEN)
                    .body("Invalid WebSocket upgrade request".to_string())
                    .unwrap());
            }

            tokio::task::spawn(async move {
                if let Ok(upgraded) = hyper::upgrade::on(req).await {
                    let stream = ListenerStream::new_websocket(
                        stream_props,
                        HyperUpgradedStream::new(upgraded),
                    );
                    handle_ws_upgrade(stream, identity, bound_config).await;
                }
            });

            Ok(Response::builder()
                .status(StatusCode::OK)
                .body("Switching to WebSocket".to_string())
                .unwrap())
        } else {
            Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Invalid WebSocket protocol".to_string())
                .unwrap())
        }
    } else {
        Ok(Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body("Missing protocol extension".to_string())
            .unwrap())
    }
}

async fn handle_ws_upgrade(
    stream: ListenerStream,
    identity: ConnectionIdentityBuilder,
    bound_config: impl IsBoundConfig,
) -> Result<(), std::io::Error> {
    handle_connection_inner(stream, identity, bound_config).await
}

struct HttpService<T: IsBoundConfig> {
    bound_config: T,
    stream_props: Arc<StreamProperties>,
    identity: ConnectionIdentityBuilder,
}

impl<T: IsBoundConfig> HttpService<T> {
    pub fn new(
        bound_config: T,
        stream_props: Arc<StreamProperties>,
        identity: ConnectionIdentityBuilder,
    ) -> Self {
        Self {
            bound_config,
            stream_props,
            identity,
        }
    }
}

impl<T: IsBoundConfig> hyper::service::Service<Request<hyper::body::Incoming>> for HttpService<T> {
    type Error = std::io::Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<Response<String>, std::io::Error>> + Send + Sync>>;
    type Response = Response<String>;

    fn call(&self, mut req: Request<hyper::body::Incoming>) -> Self::Future {
        let bound_config = self.bound_config.clone();
        let stream_props = self.stream_props.clone();
        let identity = self.identity.new_builder();
        Box::pin(async move {
            // First, check for invalid URI segments. The server will require fully-normalized paths.
            let uri = req.uri();
            if uri.path()[1..]
                .split('/')
                .any(|segment| segment == "." || segment == ".." || segment.is_empty())
            {
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body("Invalid request: URI contains invalid segments".to_string())
                    .unwrap());
            }

            req.headers().get("x-edgedb-user");

            if req.extensions().get::<OnUpgrade>().is_some() {
                match req.version() {
                    hyper::Version::HTTP_11 => {
                        return handle_ws_upgrade_http1(stream_props, identity, req, bound_config)
                            .await;
                    }
                    hyper::Version::HTTP_2 => {
                        return handle_ws_upgrade_http2(stream_props, identity, req, bound_config)
                            .await;
                    }
                    _ => {
                        return Ok(Response::builder()
                            .status(StatusCode::HTTP_VERSION_NOT_SUPPORTED)
                            .body("Unsupported HTTP version".to_string())
                            .unwrap());
                    }
                }
            }

            if uri.path().starts_with("/db/") || uri.path().starts_with("/branch/") {
                let mut split = uri.path().split('/');
                split.next();
                if let Some(branch_or_db) = split.next() {
                    if split.next().is_none() {}
                }
            }

            Ok::<_, std::io::Error>(Response::new("Hello!".to_owned()))
        })
    }
}

/// Handles a connection from the listener. This method will not return until the connection is closed.
async fn handle_connection_inner(
    mut socket: ListenerStream,
    identity: ConnectionIdentityBuilder,
    bound_config: impl IsBoundConfig,
) -> Result<(), std::io::Error> {
    let res = identify_stream(&mut socket).await;
    let stream_type = match res {
        Ok(stream_type) => stream_type,
        Err(unknown_type) => return handle_stream_unknown(unknown_type, socket).await,
    };

    let transport = socket.transport_type();
    if !bound_config.config().is_supported(
        Some(stream_type),
        socket.transport_type(),
        socket.props(),
    ) {
        warn!("{stream_type:?} on {transport:?} disabled");
        _ = socket.write_all(stream_type.go_away_message()).await;
        _ = socket.shutdown().await;
        return Ok(());
    }

    match stream_type {
        StreamType::EdgeDBBinary => {
            handle_stream_edgedb_binary(socket, identity, bound_config).await
        }
        StreamType::HTTP1x => handle_stream_http1x(socket, identity, bound_config).await,
        StreamType::HTTP2 => handle_stream_http2(socket, identity, bound_config).await,
        StreamType::SSLTLS => handle_stream_ssltls(socket, identity, bound_config).await,
        StreamType::PostgresInitial(PostgresInitialMessage::SSLRequest) => {
            handle_stream_postgres_ssl(socket, identity, bound_config).await
        }
        StreamType::PostgresInitial(..) => {
            handle_stream_postgres_initial(socket, identity, bound_config).await
        }
    }
}

async fn handle_stream_unknown(
    unknown_type: UnknownStreamType,
    mut socket: ListenerStream,
) -> Result<(), std::io::Error> {
    _ = socket.write_all(unknown_type.go_away_message()).await;
    _ = socket.shutdown().await;
    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("Invalid protocol ({unknown_type:?})"),
    ))
}

async fn handle_stream_edgedb_binary(
    mut socket: ListenerStream,
    identity: ConnectionIdentityBuilder,
    bound_config: impl IsBoundConfig,
) -> Result<(), std::io::Error> {
    socket.read_u8().await?;
    let mut length_bytes = [0; 4];
    socket.read_exact(&mut length_bytes).await?;
    let length = u32::from_be_bytes(length_bytes) - 4;
    let mut handshake = vec![0; length as usize];
    socket.read_exact(&mut handshake).await?;
    println!("Handshake: {:?}", handshake);
    _ = socket
        .write_all(StreamType::EdgeDBBinary.go_away_message())
        .await;
    _ = socket.shutdown().await;
    Ok(())
}

async fn handle_stream_http1x(
    mut socket: ListenerStream,
    identity: ConnectionIdentityBuilder,
    bound_config: impl IsBoundConfig,
) -> Result<(), std::io::Error> {
    let http1 = hyper::server::conn::http1::Builder::new();
    let mut props = socket.props_clone();
    let conn = http1.serve_connection(
        hyper_util::rt::TokioIo::new(socket),
        HttpService::new(bound_config, props, identity),
    );
    conn.with_upgrades()
        .await
        .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))
}

async fn handle_stream_http2(
    mut socket: ListenerStream,
    identity: ConnectionIdentityBuilder,
    bound_config: impl IsBoundConfig,
) -> Result<(), std::io::Error> {
    let mut http2 = hyper::server::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new());
    http2.enable_connect_protocol();
    let mut props = socket.props_clone();
    let conn = http2.serve_connection(
        hyper_util::rt::TokioIo::new(socket),
        HttpService::new(bound_config, props, identity),
    );
    tokio::task::spawn(conn)
        .await?
        .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))
}

async fn handle_stream_ssltls(
    socket: ListenerStream,
    identity: ConnectionIdentityBuilder,
    bound_config: impl IsBoundConfig,
) -> Result<(), std::io::Error> {
    let mut ssl = bound_config.ssl()?;
    ssl.set_ex_data(
        get_ssl_ex_data_index(),
        SSLExData {
            identity: identity.clone(),
            stream_props: socket.props_clone(),
        },
    );
    let ssl_socket = socket.start_ssl(ssl).await?;
    Box::pin(handle_connection_inner(ssl_socket, identity, bound_config)).await
}

async fn handle_stream_postgres_ssl(
    mut socket: ListenerStream,
    identity: ConnectionIdentityBuilder,
    bound_config: impl IsBoundConfig,
) -> Result<(), std::io::Error> {
    let mut rewind = [0_u8; 8];
    socket.read_exact(&mut rewind).await?;

    // Postgres checks to see if the socket is readable and fails here
    let mut peek = [0; 1];
    // let len = socket.peek(&mut peek).await?;
    // if len != 0 {
    //     return Err(std::io::Error::new(
    //         std::io::ErrorKind::InvalidData,
    //         "Invalid SSL handshake",
    //     ));
    // }

    if !bound_config.config().is_supported(
        Some(StreamType::PostgresInitial(
            PostgresInitialMessage::StartupMessage,
        )),
        TransportType::Ssl,
        socket.props(),
    ) {
        socket.write_all(b"N").await?;
        return Box::pin(handle_connection_inner(socket, identity, bound_config)).await;
    }

    eprintln!("Booting postgres SSL");
    socket.write_all(b"S").await?;
    let ssl = bound_config.ssl()?;
    let ssl_socket = socket.start_ssl(ssl).await?;
    Box::pin(handle_connection_inner(ssl_socket, identity, bound_config)).await
}

async fn handle_stream_postgres_initial(
    mut socket: ListenerStream,
    identity: ConnectionIdentityBuilder,
    bound_config: impl IsBoundConfig,
) -> Result<(), std::io::Error> {
    use pgrust::protocol::{
        match_message, messages::Initial, meta::InitialMessage, StartupMessage, StructBuffer,
    };

    let mut buf = StructBuffer::<InitialMessage>::default();
    let mut done = false;
    let mut startup_params = HashMap::with_capacity(16);
    while !done {
        let mut b = [0; 512];
        let n = socket.read(&mut b).await?;
        if n == 0 {
            return Err(ErrorKind::UnexpectedEof.into());
        }
        buf.push_fallible(&b[..n], |msg| {
            match_message!(msg, Initial {
                (StartupMessage as startup) => {
                    for param in startup.params() {
                        if param.name() == "database" {
                            let db = param.value().to_str().map_err(|_| std::io::Error::new(ErrorKind::InvalidData, "Invalid database name"))?.to_owned();
                            identity.set_branch(BranchDB::Branch(db));
                        } else if param.name() == "username" {
                            let username = param.value().to_str().map_err(|_| std::io::Error::new(ErrorKind::InvalidData, "Invalid username"))?.to_owned();
                            identity.set_user(username);
                        } else {
                            let name = param.name().to_str().map_err(|_| std::io::Error::new(ErrorKind::InvalidData, "Invalid startup parameter"))?.to_owned();
                            let value = param.value().to_str().map_err(|_| std::io::Error::new(ErrorKind::InvalidData, "Invalid startup parameter"))?.to_owned();
                            if startup_params.contains_key(&name) {
                                return Err(std::io::Error::new(ErrorKind::InvalidData, "Invalid startup parameter"));
                            }
                            startup_params.insert(name, value);
                        }
                    }
                    done = true;
                },
                message => {
                    return Err(std::io::Error::new(ErrorKind::InvalidData, "Unexpected message"));
                }
            });
            Ok(())
        })?;
    }

    let bytes = buf.into_inner();

    // Do some auth
    let auth = bound_config
        .service()
        .lookup_auth(
            identity.build(),
            AuthTarget::Stream(StreamLanguage::Postgres),
        )
        .await?;
    match auth {
        AuthResult::Trust => {}
        AuthResult::MTLS => {}
        AuthResult::Deny => {}
        AuthResult::MD5(..) => {}
        AuthResult::ScramSHA256(..) => {}
    }

    _ = socket
        .write_all(
            StreamType::PostgresInitial(PostgresInitialMessage::StartupMessage).go_away_message(),
        )
        .await;
    _ = socket.shutdown().await;
    Ok(())
}

#[derive(Debug)]
pub struct BoundConfig<C: ListenerConfig, S: BabelfishService> {
    config: Arc<C>,
    service: Arc<S>,
    ssl: SslContext,
}

impl<C: ListenerConfig, S: BabelfishService> BoundConfig<C, S> {
    pub fn new(config: C, service: S) -> std::io::Result<Self> {
        let config = Arc::new(config);
        let ssl = create_ssl_for_listener_config(config.clone())?;
        Ok(Self {
            config,
            service: service.into(),
            ssl,
        })
    }
}

impl<C: ListenerConfig, S: BabelfishService> Clone for BoundConfig<C, S> {
    fn clone(&self) -> Self {
        Self {
            config: Arc::clone(&self.config),
            service: Arc::clone(&self.service),
            ssl: self.ssl.clone(),
        }
    }
}

trait IsBoundConfig: Clone + Send + Sync + 'static {
    type Config: ListenerConfig;
    type Service: BabelfishService;

    fn config(&self) -> &Arc<Self::Config>;
    fn service(&self) -> &Arc<Self::Service>;
    fn ssl(&self) -> std::io::Result<Ssl>;
}

impl<C: ListenerConfig, S: BabelfishService> IsBoundConfig for BoundConfig<C, S> {
    type Config = C;
    type Service = S;

    #[inline(always)]
    fn config(&self) -> &Arc<Self::Config> {
        &self.config
    }

    #[inline(always)]
    fn service(&self) -> &Arc<Self::Service> {
        &self.service
    }

    fn ssl(&self) -> std::io::Result<Ssl> {
        Ok(Ssl::new(&self.ssl)?)
    }
}

pub struct BoundServer {
    task: tokio::task::JoinHandle<std::io::Result<()>>,
    addresses: tokio::sync::Mutex<tokio::sync::watch::Receiver<Option<Vec<ListenerAddress>>>>,
}

impl BoundServer {
    pub fn bind(
        config: impl ListenerConfig,
        service: impl BabelfishService,
    ) -> std::io::Result<Self> {
        let config = BoundConfig::new(config, service)?;

        trace!("Booting bound server with {config:#?}");

        let (tx, rx) = tokio::sync::watch::channel(None);
        let task = tokio::task::spawn(bind_task(tx, config.config().clone(), move |_, stm| {
            let config = config.clone();
            let identity = ConnectionIdentityBuilder::new();
            if !config
                .config
                .is_supported(None, stm.transport_type(), stm.props())
            {
                return;
            }
            tokio::task::spawn(async move { handle_connection_inner(stm, identity, config).await });
        }));
        Ok(Self {
            task,
            addresses: rx.into(),
        })
    }

    pub async fn addresses(&self) -> Vec<ListenerAddress> {
        let mut lock = self.addresses.lock().await;
        let Ok(res) = lock.wait_for(|t| t.is_some()).await else {
            return vec![];
        };
        res.clone().unwrap_or_default()
    }

    pub fn shutdown(self) -> impl Future<Output = ()> {
        self.task.abort();
        async {
            _ = self.task.await;
        }
    }
}

fn create_ssl_for_listener_config(
    config: Arc<impl ListenerConfig>,
) -> Result<openssl::ssl::SslContext, std::io::Error> {
    let mut ssl = openssl::ssl::SslContext::builder(SslMethod::tls_server())?;
    {
        ssl.set_servername_callback(move |ssl, alert| {
            let Some(ex_data) = ssl.ex_data(get_ssl_ex_data_index()) else {
                error!("Missing SSLExData");
                *alert = SslAlert::ILLEGAL_PARAMETER;
                return Err(SniError::ALERT_FATAL);
            };
            let hostname = ssl.servername(NameType::HOST_NAME);
            eprintln!("SNI: {hostname:?}");
            let (ssl_new, tenant) = config.ssl_config_sni(hostname).unwrap();
            if let Some(tenant) = tenant {
                ex_data.identity.set_tenant(tenant);
            }
            let config = config.clone();
            let stream_props = ex_data.stream_props.clone();
            let ssl_new = ssl_new.maybe_configure(move |ctx| {
                ctx.set_alpn_select_callback(move |_, alpn| {
                    trace!("Server ALPN callback: {:?}", alpn);
                    let protocol = negotiate_alpn(config.as_ref(), alpn, &stream_props);
                    if !alpn.is_empty() && protocol.is_none() {
                        return Err(AlpnError::ALERT_FATAL);
                    }
                    protocol.map(|s| s.as_bytes()).ok_or(AlpnError::NOACK)
                });
            });
            ssl.set_ssl_context(&ssl_new).unwrap();
            Ok(())
        });
    };
    let ssl = ssl.build();
    Ok(ssl)
}

/// Bind on the stream of addresses provided by this listener.
fn bind_task<C: ListenerConfig>(
    tx: tokio::sync::watch::Sender<Option<Vec<ListenerAddress>>>,
    config: Arc<C>,
    callback: impl FnMut(Arc<C>, ListenerStream) + Send + Sync + 'static,
) -> impl Future<Output = std::io::Result<()>> {
    let callback = Arc::new(Mutex::new(callback));
    async move {
        let mut stm = pin!(config.listen_address());
        let listeners = Mutex::new(HashMap::<
            _,
            (
                ListenerAddress,
                tokio::task::JoinHandle<std::io::Result<()>>,
            ),
        >::new());
        defer!({
            _ = tx.send(Some(vec![]));
            for (_, (_, listener)) in listeners.lock().unwrap().drain() {
                listener.abort()
            }
        });
        while let Some(addresses) = stm.next().await.transpose()? {
            info!("Requested to listen on {addresses:?}");
            let new_listeners = HashSet::<_, RandomState>::from_iter(addresses);
            listeners.lock().unwrap().retain(|k, (_, v)| {
                // Remove any crashed tasks
                if v.is_finished() {
                    return false;
                }
                let res = new_listeners.contains(k);
                if !res {
                    v.abort();
                }
                res
            });

            for addr in new_listeners {
                if listeners.lock().unwrap().contains_key(&addr) {
                    continue;
                }

                let (resolved_addr, task) = match addr.clone() {
                    ListenerAddress::Tcp(x) => {
                        let config = config.clone();
                        let listener = TcpListener::bind(x).await?;
                        let local_addr = listener.local_addr()?.into();
                        info!("Listening on {local_addr:?}");
                        let callback = callback.clone();
                        (
                            local_addr,
                            tokio::task::spawn(async move {
                                loop {
                                    let tcp = listener.accept().await?;
                                    (callback.lock().unwrap())(
                                        config.clone(),
                                        ListenerStream::new_tcp(tcp.0),
                                    );
                                }
                                #[allow(unreachable_code)]
                                Ok::<_, std::io::Error>(())
                            }),
                        )
                    }
                    ListenerAddress::Unix(x) => {
                        let config = config.clone();
                        let listener = std::os::unix::net::UnixListener::bind_addr(&x)?;
                        let local_addr = ListenerAddress::from(Arc::new(listener.local_addr()?));
                        info!("Listening on {local_addr:?}");
                        let listener = UnixListener::from_std(listener)?;
                        let callback = callback.clone();
                        (
                            local_addr.clone(),
                            tokio::task::spawn(async move {
                                loop {
                                    let (stream, _) = listener.accept().await?;
                                    let stream = stream.into_std()?;
                                    let peer_addr =
                                        stream.peer_addr().ok().map(|s| Arc::new(s).into());
                                    let stream = UnixStream::from_std(stream)?;
                                    let peer_cred = stream.peer_cred().ok();
                                    (callback.lock().unwrap())(
                                        config.clone(),
                                        ListenerStream::new_unix(
                                            stream,
                                            Some(local_addr.clone()),
                                            peer_addr,
                                            peer_cred,
                                        ),
                                    );
                                }
                                #[allow(unreachable_code)]
                                Ok::<_, std::io::Error>(())
                            }),
                        )
                    }
                };

                listeners
                    .lock()
                    .unwrap()
                    .insert(addr, (resolved_addr, task));
                let addresses = listeners
                    .lock()
                    .unwrap()
                    .values()
                    .map(|(addr, _)| addr.clone())
                    .collect();
                _ = tx.send(Some(addresses));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use hyper::Uri;
    use hyper_util::rt::TokioIo;
    use openssl::ssl::{Ssl, SslContext, SslMethod};
    use rstest::rstest;

    use crate::{
        config::TestListenerConfig,
        service::{AuthResult, AuthTarget, ConnectionIdentity, StreamLanguage},
    };

    use super::*;
    use std::sync::{Arc, Mutex};

    /// Captured from PostgreSQL 7.x
    const LEGACY_POSTGRES: &[u8] = &[
        0x00, 0x00, 0x01, 0x28, 0x00, 0x02, 0x00, 0x00, 0x6d, 0x61, 0x74, 0x74, 0x00, 0x00, 0x00,
        0x00,
    ];
    /// Captured from OpenSSL 1.0.2k (using -ssl2)
    const LEGACY_SSL2: &[u8] = &[
        0x80, 0x25, 0x01, 0x00, 0x02, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x10, 0x05, 0x00, 0x80, 0x03,
        0x00,
    ];
    /// Captured from a modern PostgreSQL client (version 16+)
    const MODERN_POSTGRES: &[u8] = &[
        0x00, 0x00, 0x00, 0x4c, 0x00, 0x03, 0x00, 0x00, 0x75, 0x73, 0x65, 0x72, 0x00, 0x6d, 0x61,
        0x74,
    ];
    /// Captured from a modern EdgeDB client
    const MODERN_EDGEDB: &[u8] = &[
        0x56, 0x00, 0x00, 0x00, 0x4d, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x08,
        0x64,
    ];

    #[derive(Clone, Debug, Default)]
    struct TestService {
        log: Arc<Mutex<Vec<String>>>,
    }

    impl TestService {
        fn log(&self, msg: String) {
            eprintln!("{msg:?}");
            self.log.lock().unwrap().push(msg);
        }
    }

    enum TestMode {
        Tcp,
        Ssl,
        SslAlpn(&'static str),
    }

    fn create_test_ssl_client(alpn: Option<&'static str>) -> openssl::ssl::SslContext {
        let mut ctx = SslContext::builder(SslMethod::tls_client()).unwrap();
        if let Some(alpn) = alpn {
            let mut alpn = alpn.as_bytes().to_vec();
            alpn.insert(0, alpn.len() as _);
            ctx.set_alpn_protos(&alpn).unwrap();
        }
        ctx.build()
    }

    impl BabelfishService for TestService {
        fn lookup_auth(
            &self,
            identity: ConnectionIdentity,
            target: AuthTarget,
        ) -> impl Future<Output = Result<AuthResult, std::io::Error>> {
            self.log(format!("lookup_auth: {:?}", identity));
            async { Ok(Default::default()) }
        }

        fn accept_stream(
            &self,
            identity: ConnectionIdentity,
            language: StreamLanguage,
            stream: ListenerStream,
        ) -> impl Future<Output = Result<(), std::io::Error>> {
            self.log(format!(
                "accept_stream: {:?}, {:?}, {:?}",
                identity, language, stream
            ));
            async { Ok(()) }
        }

        fn accept_http(
            &self,
            identity: ConnectionIdentity,
            req: hyper::http::Request<hyper::body::Incoming>,
        ) -> impl Future<Output = Result<hyper::http::Response<String>, std::io::Error>> {
            self.log(format!("accept_http: {:?}, {:?}", identity, req));
            async { Ok(Default::default()) }
        }
    }

    /// Run a test server and connect to it.
    fn run_test_service<F: Future<Output = Result<(), std::io::Error>> + Send + 'static>(
        mode: TestMode,
        f: impl Fn(ListenerStream) -> F + Send + 'static,
    ) {
        let svc = TestService::default();
        let config = TestListenerConfig::new("localhost:0");

        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async move {
                let server = BoundServer::bind(config, svc).unwrap();
                let addr = server
                    .addresses()
                    .await
                    .first()
                    .cloned()
                    .unwrap()
                    .tcp_addr()
                    .unwrap();

                let t2 = tokio::spawn(async move {
                    let socket = ListenerStream::new_tcp(TcpStream::connect(addr).await.unwrap());
                    let socket = match mode {
                        TestMode::Tcp => socket,
                        TestMode::Ssl => {
                            let mut ssl = Ssl::new(&create_test_ssl_client(None)).unwrap();
                            ssl.set_hostname("localhost").unwrap();
                            socket.start_ssl(ssl).await.unwrap()
                        }
                        TestMode::SslAlpn(alpn) => {
                            let mut ssl = Ssl::new(&create_test_ssl_client(Some(alpn))).unwrap();
                            ssl.set_hostname("localhost").unwrap();
                            socket.start_ssl(ssl).await.unwrap()
                        }
                    };
                    f(socket).await.unwrap();
                });

                t2.await.unwrap();
                server.shutdown().await;
            });
    }

    /// Closes the connection with an error starting with "E" and ending in NUL.
    #[rstest]
    #[test_log::test]
    fn test_legacy_postgres(#[values(TestMode::Tcp, TestMode::Ssl)] mode: TestMode) {
        run_test_service(mode, |mut stm| async move {
            stm.write_all(LEGACY_POSTGRES).await.unwrap();
            let mut buf = vec![];
            stm.read_to_end(&mut buf).await.unwrap();
            assert_eq!(buf[0], b'E');
            assert_eq!(buf[buf.len() - 1], 0);
            Ok(())
        });
    }

    /// Closes the connection with an SSLv2 error.
    #[test_log::test]
    fn test_legacy_ssl() {
        run_test_service(TestMode::Tcp, |mut stm| async move {
            stm.write_all(LEGACY_SSL2).await.unwrap();
            let mut buf = vec![];
            stm.read_to_end(&mut buf).await.unwrap();
            assert_eq!(buf, vec![0x80, 3, 0, 0, 1]);
            Ok(())
        });
    }

    #[test]
    fn test_raw_postgres() {
        use pgrust::protocol::builder::{StartupMessage, StartupNameValue};
        run_test_service(TestMode::Tcp, |mut stm| async move {
            let msg = StartupMessage {
                params: &[
                    StartupNameValue {
                        name: "database",
                        value: "name",
                    },
                    StartupNameValue {
                        name: "username",
                        value: "me",
                    },
                ],
            }
            .to_vec();
            stm.write_all(&msg).await.unwrap();
            assert_eq!(stm.read_u8().await.unwrap(), b'S');
            Ok(())
        });
    }

    #[rstest]
    #[test_log::test]
    fn test_http_manual(
        #[values(TestMode::Tcp, TestMode::Ssl, TestMode::SslAlpn("http/1.1"))] mode: TestMode,
    ) {
        run_test_service(mode, |mut stm| async move {
            stm.write_all(b"GET /\r\n\r\n").await.unwrap();
            let mut buf = vec![];
            stm.read_to_end(&mut buf).await.unwrap();
            let result = String::from_utf8(buf).unwrap();
            assert_eq!(&result[..12], "HTTP/1.1 400");
            Ok(())
        });
    }

    #[rstest]
    #[test_log::test]
    fn test_http_1(
        #[values(TestMode::Tcp, TestMode::Ssl, TestMode::SslAlpn("http/1.1"))] mode: TestMode,
    ) {
        run_test_service(mode, |stm| async move {
            let http1 = hyper::client::conn::http1::Builder::new();
            let (mut send, conn) = http1
                .handshake::<_, String>(TokioIo::new(stm))
                .await
                .unwrap();
            tokio::task::spawn(conn);
            let req = hyper::Request::new("x".to_string());
            let resp = send.send_request(req).await.unwrap();
            eprintln!("{resp:?}");
            Ok(())
        });
    }

    #[rstest]
    #[test_log::test]
    fn test_http_2(
        #[values(TestMode::Tcp, TestMode::Ssl, TestMode::SslAlpn("h2"))] mode: TestMode,
    ) {
        run_test_service(mode, |stm| {
            async move {
                let http2 =
                    hyper::client::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new());
                let (mut send, conn) = http2
                    .handshake::<_, String>(TokioIo::new(stm))
                    .await
                    .unwrap();
                tokio::task::spawn(conn);
                let req = hyper::Request::new("x".to_string());
                let resp = send.send_request(req).await.unwrap();
                eprintln!("{resp:?}");

                // assert_eq!(stm.read_u8().await.unwrap(), b'S');
                Ok(())
            }
        });
    }

    #[rstest]
    #[test_log::test]
    fn test_tunneled_edgedb(
        #[values(TestMode::Tcp, TestMode::Ssl, TestMode::SslAlpn("h2"))] mode: TestMode,
    ) {
        run_test_service(mode, |stm| {
            async move {
                let http2 =
                    hyper::client::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new());
                let (mut send, conn) = http2.handshake::<_, _>(TokioIo::new(stm)).await.unwrap();
                tokio::task::spawn(conn);
                let mut body = vec![];
                body.extend_from_slice(b"O\x00\x00\x00\xef\x00\x00\xff\xff\xff\xff\xff\xff\xff\xd9\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00bo\x00\x00\x00\x93");
                body.extend_from_slice(b"\n      select {\n        instanceName := sys::get_instance_name(),\n        databases := sys::Database.name,\n        roles := sys::Role.name,\n      }");
                body.extend_from_slice(b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00");
                body.extend_from_slice(b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00S\x00\x00\x00\x04");
                let mut req =
                    hyper::Request::new(http_body_util::Full::new(hyper::body::Bytes::from(body)));
                *req.uri_mut() = Uri::from_static("/db/./mydb");
                let resp = send.send_request(req).await.unwrap();
                eprintln!("{resp:?}");

                // assert_eq!(stm.read_u8().await.unwrap(), b'S');
                Ok(())
            }
        });
    }
}
