use std::{future::Future, time::Duration};

use edb_frontend::config::*;
use edb_frontend::listener::*;
use edb_frontend::service::*;
use edb_frontend::stream::*;
use hyper::Response;
use openssl::ssl::{SslContext, SslMethod};

#[derive(Clone, Debug, Default)]
struct ExampleService {}

impl BabelfishService for ExampleService {
    fn lookup_auth(
        &self,
        identity: ConnectionIdentity,
        target: AuthTarget,
    ) -> impl Future<Output = Result<AuthResult, std::io::Error>> {
        eprintln!("lookup_auth: {:?}", identity);
        async { Ok(Default::default()) }
    }

    fn accept_stream(
        &self,
        identity: ConnectionIdentity,
        language: StreamLanguage,
        stream: ListenerStream,
    ) -> impl Future<Output = Result<(), std::io::Error>> {
        eprintln!(
            "accept_stream: {:?}, {:?}, {:?}",
            identity, language, stream
        );
        async { Ok(()) }
    }

    fn accept_http(
        &self,
        identity: ConnectionIdentity,
        req: hyper::http::Request<hyper::body::Incoming>,
    ) -> impl Future<Output = Result<hyper::http::Response<String>, std::io::Error>> {
        eprintln!("accept_http: {:?}, {:?}", identity, req);
        async { Ok(Response::new("Hello!".to_string())) }
    }
}

/// Run a test server and cconnect to it.
fn run_test_service() {
    let server = ExampleService::default();

    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async move {
            BoundServer::bind(TestListenerConfig::new("localhost:2134"), server).unwrap();
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
}

pub fn main() {
    tracing_subscriber::fmt::init();
    run_test_service();
}
