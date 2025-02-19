use tokio::net::UnixSocket;

struct AsyncConnector {
    unix_socket: UnixSocket,
}

impl AsyncConnector {}
