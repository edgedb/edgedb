use captive_postgres::{
    setup_postgres, ListenAddress, Mode, DEFAULT_DATABASE, DEFAULT_PASSWORD, DEFAULT_USERNAME,
};
use clap::Parser;
use clap_derive::Parser;
use gel_auth::AuthType;
use gel_stream::client::{Connector, ResolvedTarget, Target};
use pgrust::{
    connection::{
        dsn::parse_postgres_dsn_env, Client, Credentials, ExecuteSink, Format, MaxRows,
        PipelineBuilder, Portal, QuerySink, Statement,
    },
    protocol::postgres::data::{CopyData, CopyOutResponse, DataRow, ErrorResponse, RowDescription},
};
use std::net::SocketAddr;
use tokio::task::LocalSet;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Use an ephemeral database
    #[clap(short = 'e', long = "ephemeral", conflicts_with_all = &["dsn", "unix", "tcp", "username", "password", "database"])]
    ephemeral: bool,

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

    /// Use extended query syntax
    #[clap(short = 'x', long = "extended")]
    extended: bool,

    /// SQL statements to run
    #[clap(
        name = "statements",
        trailing_var_arg = true,
        allow_hyphen_values = true,
        help = "Zero or more SQL statements to run (defaults to 'select 1')"
    )]
    statements: Option<Vec<String>>,
}

fn address(address: &ListenAddress) -> ResolvedTarget {
    match address {
        ListenAddress::Tcp(addr) => ResolvedTarget::SocketAddr(*addr),
        #[cfg(unix)]
        ListenAddress::Unix(path) => ResolvedTarget::UnixSocketAddr(
            std::os::unix::net::SocketAddr::from_pathname(path).unwrap(),
        ),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let mut args = Args::parse();
    eprintln!("{args:?}");

    let mut socket_address: Option<ResolvedTarget> = None;

    let _ephemeral = if args.ephemeral {
        let process = setup_postgres(AuthType::Trust, Mode::Unix)?;
        let Some(process) = process else {
            eprintln!("Failed to start ephemeral database");
            return Err("Failed to start ephemeral database".into());
        };
        socket_address = Some(address(&process.socket_address));
        args.username = DEFAULT_USERNAME.to_string();
        args.password = DEFAULT_PASSWORD.to_string();
        args.database = DEFAULT_DATABASE.to_string();
        Some(process)
    } else {
        None
    };

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
            socket_address = host.target_name()?.to_addrs_sync()?.into_iter().next();
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
    let socket_address = Target::new_resolved(socket_address);

    let local = LocalSet::new();
    local
        .run_until(run_queries(
            socket_address,
            credentials,
            statements,
            args.extended,
        ))
        .await?;

    Ok(())
}

fn logging_sink() -> impl QuerySink {
    (
        |rows: RowDescription<'_>| {
            eprintln!("\nFields:");
            for field in rows.fields() {
                eprint!(" {:?}", field.name());
            }
            eprintln!();
            let guard = scopeguard::guard((), |_| {
                eprintln!("Done");
            });
            move |row: DataRow<'_>| {
                let _ = &guard;
                eprintln!("Row:");
                for field in row.values() {
                    eprint!(" {:?}", field);
                }
                eprintln!();
            }
        },
        |_: CopyOutResponse<'_>| {
            eprintln!("\nCopy:");
            let guard = scopeguard::guard((), |_| {
                eprintln!("Done");
            });
            move |data: CopyData<'_>| {
                let _ = &guard;
                eprintln!("Chunk:");
                for line in hexdump::hexdump_iter(data.data().as_ref()) {
                    eprintln!("{line}");
                }
            }
        },
        |error: ErrorResponse<'_>| {
            eprintln!("\nError:\n {:?}", error);
        },
    )
}

fn logging_sink_execute() -> impl ExecuteSink {
    (
        || {
            eprintln!();
            let guard = scopeguard::guard((), |_| {
                eprintln!("Done");
            });
            move |row: DataRow<'_>| {
                let _ = &guard;
                eprintln!("Row:");
                for field in row.values() {
                    eprint!(" {:?}", field);
                }
                eprintln!();
            }
        },
        |_: CopyOutResponse<'_>| {
            eprintln!("\nCopy:");
            let guard = scopeguard::guard((), |_| {
                eprintln!("Done");
            });
            move |data: CopyData<'_>| {
                let _ = &guard;
                eprintln!("Chunk:");
                for line in hexdump::hexdump_iter(data.data().as_ref()) {
                    eprintln!("{line}");
                }
            }
        },
        |error: ErrorResponse<'_>| {
            eprintln!("\nError:\n {:?}", error);
        },
    )
}

async fn run_queries(
    target: Target,
    credentials: Credentials,
    statements: Vec<String>,
    extended: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let connector = Connector::new(target)?;
    let (conn, task) = Client::new(credentials, connector);
    tokio::task::spawn_local(task);
    conn.ready().await?;

    eprintln!("Statements: {statements:?}");

    for statement in statements {
        if extended {
            let conn = conn.clone();
            tokio::task::spawn_local(async move {
                let pipeline = PipelineBuilder::default()
                    .parse(Statement::default(), &statement, &[], ())
                    .describe_statement(Statement::default(), ())
                    .bind(
                        Portal::default(),
                        Statement::default(),
                        &[],
                        &[Format::text()],
                        (),
                    )
                    .describe_portal(Portal::default(), ())
                    .execute(
                        Portal::default(),
                        MaxRows::Unlimited,
                        logging_sink_execute(),
                    )
                    .build();
                conn.pipeline_sync(pipeline).await
            })
            .await??;
        } else {
            tokio::task::spawn_local(conn.query(&statement, logging_sink())).await??;
        }
    }

    Ok(())
}
