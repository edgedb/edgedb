use std::{cell::RefCell, collections::HashMap, future::Future, rc::Rc};

use db_proto::StructBuffer;
use gel_stream::{client::Connector, client::Target, TlsParameters};
use openssl::ssl::{Ssl, SslContext, SslMethod};
use pgrust::{
    connection::{Client, Credentials},
    protocol::edgedb::data::{CommandComplete, ParameterStatus, StateDataDescription},
};
use std::pin::Pin;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpSocket,
    task::LocalSet,
};

#[derive(Debug, Clone)]
struct TestSetup {
    addr: std::net::SocketAddr,
    username: String,
    password: String,
    database: String,
}

trait SmokeTest {
    fn name(&self) -> String;
    async fn run(&self, setup: &TestSetup) -> Result<(), Box<dyn std::error::Error>>;
}

struct PostgresSelect {
    query: String,
    expected: String,
}

impl SmokeTest for PostgresSelect {
    fn name(&self) -> String {
        format!("PostgresSelect [{}]", self.query)
    }

    async fn run(&self, setup: &TestSetup) -> Result<(), Box<dyn std::error::Error>> {
        use pgrust::protocol::postgres::data::{DataRow, ErrorResponse, RowDescription};

        let target = Target::new_tcp_tls(setup.addr, TlsParameters::default());
        let connector = Connector::new(target)?;

        let credentials = Credentials {
            username: setup.username.clone(),
            password: setup.password.clone(),
            database: setup.database.clone(),
            server_settings: HashMap::new(),
        };
        let (client, task) = Client::new(credentials, connector);
        tokio::task::spawn_local(task);
        client.ready().await?;

        let out = Rc::new(RefCell::new(String::new()));
        let out_clone = out.clone();
        client
            .query(
                &self.query,
                (
                    move |rows: RowDescription<'_>| {
                        let cols = rows
                            .fields()
                            .into_iter()
                            .map(|field| field.name().to_string_lossy().to_string())
                            .collect::<Vec<_>>();
                        out.borrow_mut().push_str(&format!("{}\n", cols.join(",")));
                        let out = out.clone();
                        move |row: DataRow<'_>| {
                            let values: Vec<_> = row
                                .values()
                                .into_iter()
                                .map(|v| v.to_string_lossy().to_string())
                                .collect();
                            out.borrow_mut()
                                .push_str(&format!("{}\n", values.join(",")));
                        }
                    },
                    |_: ErrorResponse<'_>| {},
                ),
            )
            .await?;

        let out = out_clone.borrow().clone();
        if out == self.expected {
            Ok(())
        } else {
            Err(format!("Expected `{}` but got `{}`", self.expected, out).into())
        }
    }
}

struct EdgeQLSelect {
    query: String,
    expected: String,
}

impl SmokeTest for EdgeQLSelect {
    fn name(&self) -> String {
        format!("EdgeQLSelect [{}]", self.query)
    }

    async fn run(&self, setup: &TestSetup) -> Result<(), Box<dyn std::error::Error>> {
        use pgrust::protocol::edgedb::{builder, data::Data, meta};

        let socket = TcpSocket::new_v4()?.connect(setup.addr).await?;
        let mut ssl = SslContext::builder(SslMethod::tls_client())?;
        ssl.set_alpn_protos(b"\x0dedgedb-binary")?;
        let ssl = ssl.build();
        let mut ssl = Ssl::new(&ssl)?;
        ssl.set_connect_state();

        let mut stream = tokio_openssl::SslStream::new(ssl, socket)?;
        Pin::new(&mut stream).do_handshake().await?;

        let handshake = builder::ClientHandshake {
            major_ver: 2,
            minor_ver: 0,
            params: &[
                builder::ConnectionParam {
                    name: "user",
                    value: &setup.username,
                },
                builder::ConnectionParam {
                    name: "database",
                    value: &setup.database,
                },
            ],
            extensions: &[],
        };
        stream.write_all(&handshake.to_vec()).await?;

        let execute = builder::Execute {
            command_text: &self.query,
            output_format: b'j',
            expected_cardinality: b'o', // AT_MOST_ONE
            ..Default::default()
        };
        stream.write_all(&execute.to_vec()).await?;

        let mut buf = StructBuffer::<meta::Message>::default();

        let mut done = false;
        while !done {
            let mut bytes = vec![0; 1024];
            let n = stream.read(&mut bytes).await?;
            if n == 0 {
                break;
            }
            buf.push(&bytes[..n], |msg| match msg {
                Ok(msg) => {
                    if let Some(msg) = StateDataDescription::try_new(&msg) {
                        eprintln!("{:?}", String::from_utf8_lossy(msg.typedesc().as_ref()));
                    } else if let Some(msg) = ParameterStatus::try_new(&msg) {
                        eprintln!(
                            "{:?} {:?}",
                            String::from_utf8_lossy(msg.name().as_ref()),
                            String::from_utf8_lossy(msg.value().as_ref())
                        );
                    } else if let Some(data) = Data::try_new(&msg) {
                        for data in data.data() {
                            eprintln!("{:?}", data.data());
                        }
                    } else if CommandComplete::try_new(&msg).is_some() {
                        done = true;
                        return;
                    } else {
                        eprintln!("{} {:?}", msg.mtype() as char, msg);
                    }
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                }
            });
        }

        Ok(())
    }
}

#[tokio::main]
pub async fn main() {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = std::env::args().collect();
    if args.len() != 5 {
        println!(
            "Usage: {} <addr:port> <username> <password> <database>",
            args[0]
        );
        return;
    }

    let addr = &args[1];
    let username = &args[2];
    let password = &args[3];
    let database = &args[4];

    let addr = match addr.parse::<std::net::SocketAddr>() {
        Ok(addr) => addr,
        Err(e) => {
            eprintln!("Invalid address format: {}", e);
            return;
        }
    };

    let setup = TestSetup {
        addr,
        username: username.to_string(),
        password: password.to_string(),
        database: database.to_string(),
    };

    LocalSet::new()
        .run_until(async {
            let mut tests: Vec<Pin<Box<dyn Future<Output = ()> + 'static>>> = vec![];

            fn test(
                setup: &TestSetup,
                test: impl SmokeTest + 'static,
            ) -> Pin<Box<dyn Future<Output = ()> + 'static>> {
                let setup = setup.clone();
                Box::pin(async move {
                    let name = test.name();
                    let res = test.run(&setup).await;
                    match res {
                        Ok(_) => println!("✅ {name} passed"),
                        Err(e) => println!("❌ {name} failed: {}", e),
                    };
                })
            }

            tests.push(test(
                &setup,
                PostgresSelect {
                    query: "SELECT".to_string(),
                    expected: "\n\n".to_string(),
                },
            ));
            tests.push(test(
                &setup,
                PostgresSelect {
                    query: "SELECT 1 as x".to_string(),
                    expected: "x\n1\n".to_string(),
                },
            ));
            tests.push(test(
                &setup,
                PostgresSelect {
                    query: "SELECT LIMIT 0".to_string(),
                    expected: "\n".to_string(),
                },
            ));
            tests.push(test(
                &setup,
                EdgeQLSelect {
                    query: "select 1".to_string(),
                    expected: "1\n".to_string(),
                },
            ));

            for test in tests {
                test.await;
            }
        })
        .await;
}
