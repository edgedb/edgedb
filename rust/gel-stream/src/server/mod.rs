use crate::{RewindStream, Ssl, UpgradableStream};

mod acceptor;
pub use acceptor::Acceptor;

type Connection<D = Ssl> = UpgradableStream<RewindStream<crate::BaseStream>, D>;
