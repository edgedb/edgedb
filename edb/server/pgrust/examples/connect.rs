use clap::Parser;
use clap_derive::Parser;
use openssl::ssl::{Ssl, SslContext, SslMethod};
use pgrust::{
    connection::{dsn::parse_postgres_dsn_env, Client, Credentials, ResolvedTarget},
    protocol::postgres::data::{DataRow, ErrorResponse, RowDescription},
};
use std::net::SocketAddr;
use tokio::task::LocalSet;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short = 'D', long = "dsn", value_parser, conflicts_with_all = &["unix", "tcp", "username", "password", "database"])]
    dsn: Option<String>,

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

    /// SQL statements to run
    #[clap(
        name = "statements",
        trailing_var_arg = true,
        allow_hyphen_values = true,
        help = "Zero or more SQL statements to run (defaults to 'select 1')"
    )]
    statements: Option<Vec<String>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let mut args = Args::parse();
    eprintln!("{args:?}");

    let mut socket_address: Option<ResolvedTarget> = None;
    if let Some(dsn) = args.dsn {
        let mut conn = parse_postgres_dsn_env(&dsn, std::env::vars())?;
        #[allow(deprecated)]
        let home = std::env::home_dir().unwrap();
        conn.password
            .resolve(&home, &conn.hosts, &conn.database, &conn.database)?;
        args.database = conn.database;
        args.username = conn.user;
        args.password = conn.password.password().unwrap_or_default().to_string();
        if let Some(host) = conn.hosts.first() {
            socket_address = ResolvedTarget::to_addrs_sync(host)?.into_iter().next();
        }
    }

    let socket_address = socket_address.unwrap_or_else(|| match (args.tcp, args.unix) {
        (Some(addr), None) => ResolvedTarget::SocketAddr(addr),
        (None, Some(path)) => ResolvedTarget::UnixSocketAddr(
            std::os::unix::net::SocketAddr::from_pathname(path).unwrap(),
        ),
        _ => panic!("Must specify either a TCP address or a Unix socket path"),
    });

    eprintln!("Connecting to {socket_address:?}");

    let credentials = Credentials {
        username: args.username,
        password: args.password,
        database: args.database,
        server_settings: Default::default(),
    };

    let statements = args
        .statements
        .unwrap_or_else(|| vec!["select 1;".to_string()]);
    let local = LocalSet::new();
    local
        .run_until(run_queries(socket_address, credentials, statements))
        .await?;

    Ok(())
}

async fn run_queries(
    socket_address: ResolvedTarget,
    credentials: Credentials,
    statements: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = socket_address.connect().await?;
    let ssl = SslContext::builder(SslMethod::tls_client())?.build();
    let ssl = Ssl::new(&ssl)?;

    let (conn, task) = Client::new(credentials, client, ssl);
    tokio::task::spawn_local(task);
    conn.ready().await?;

    let local = LocalSet::new();
    eprintln!("Statements: {statements:?}");
    for statement in statements {
        let sink = (
            |rows: RowDescription<'_>| {
                eprintln!("\nFields:");
                for field in rows.fields() {
                    eprint!(" {:?}", field.name());
                }
                eprintln!();
                let guard = scopeguard::guard((), |_| {
                    eprintln!("Done");
                });
                move |row: Result<DataRow<'_>, ErrorResponse<'_>>| {
                    let _ = &guard;
                    if let Ok(row) = row {
                        eprintln!("Row:");
                        for field in row.values() {
                            eprint!(" {:?}", field);
                        }
                        eprintln!();
                    }
                }
            },
            |error: ErrorResponse<'_>| {
                eprintln!("\nError:\n {:?}", error);
            },
        );
        local.spawn_local(conn.query(&statement, sink));
    }
    local.await;

    Ok(())
}
