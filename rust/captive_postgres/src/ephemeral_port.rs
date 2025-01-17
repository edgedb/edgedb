use std::net::{Ipv4Addr, TcpListener};
use std::time::Instant;

use crate::{HOT_LOOP_INTERVAL, LINGER_DURATION, PORT_RELEASE_TIMEOUT};

/// Represents an ephemeral port that can be allocated and released for immediate re-use by another process.
pub struct EphemeralPort {
    port: u16,
    listener: Option<TcpListener>,
}

impl EphemeralPort {
    /// Allocates a new ephemeral port.
    ///
    /// Returns a Result containing the EphemeralPort if successful,
    /// or an IO error if the allocation fails.
    pub fn allocate() -> std::io::Result<Self> {
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
    pub fn take(self) -> u16 {
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
