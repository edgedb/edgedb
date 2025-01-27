use std::net::SocketAddr;

use super::stream::UpgradableStream;
use super::target::{MaybeResolvedTarget, ResolvedTarget};
use super::tokio_stream::Resolver;
use super::{ConnectionError, Ssl, Target, TlsInit};

type Connection = UpgradableStream<super::Stream, Option<super::Ssl>>;

/// A connector can be used to connect multiple times to the same target.
pub struct Connector {
    target: Target,
    resolver: Resolver,
}

impl Connector {
    pub fn new(target: Target) -> Result<Self, std::io::Error> {
        Ok(Self {
            target,
            resolver: Resolver::new()?,
        })
    }

    pub async fn connect(&self) -> Result<Connection, ConnectionError> {
        let stream = match self.target.maybe_resolved() {
            MaybeResolvedTarget::Resolved(target) => target.connect().await?,
            MaybeResolvedTarget::Unresolved(host, port, _) => {
                let ip = self
                    .resolver
                    .resolve_remote(host.clone().into_owned())
                    .await?;
                ResolvedTarget::SocketAddr(SocketAddr::new(ip, *port))
                    .connect()
                    .await?
            }
        };

        if let Some(ssl) = self.target.maybe_ssl() {
            let mut stm = UpgradableStream::new(stream, Some(Ssl::init(ssl, self.target.name())?));
            if !self.target.is_starttls() {
                stm.secure_upgrade().await?;
            }
            Ok(stm)
        } else {
            Ok(UpgradableStream::new(stream, None))
        }
    }
}
