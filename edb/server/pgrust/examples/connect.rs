use clap::{builder, Parser};
use clap_derive::Parser;
use pgrust::PGConn;
use std::net::SocketAddr;
use tokio::net::{TcpStream, UnixSocket};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Network socket address and port
    #[clap(short = 't', long = "tcp", value_parser, conflicts_with = "unix")]
    tcp: Option<SocketAddr>,

    /// Unix socket path
    #[clap(short = 'u', long = "unix", value_parser, conflicts_with = "tcp")]
    unix: Option<String>,

    /// Username to use for the connection
    #[clap(
        short = 'U',
        long = "username",
        value_parser,
        default_value = "postgres"
    )]
    username: String,

    /// Username to use for the connection
    #[clap(short = 'P', long = "password", value_parser, default_value = "")]
    password: String,

    /// Database to use for the connection
    #[clap(
        short = 'd',
        long = "database",
        value_parser,
        default_value = "postgres"
    )]
    database: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    match (args.tcp, args.unix) {
        (Some(addr), None) => {
            // Connect to the port with tokio
            let mut client = TcpStream::connect(addr).await?;
        }
        (None, Some(path)) => {
            // Connect to the unix stream socket
            let socket = UnixSocket::new_stream()?;
            let client = socket.connect(path).await?;
            let mut conn = PGConn::new(client, args.username, args.password, args.database);
            conn.task().await?;
        }
        _ => return Err("Must specify either a TCP address or a Unix socket path".into()),
    }
    Ok(())
}
