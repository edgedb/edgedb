use std::future::Future;
use std::num::NonZero;

// Constants
use gel_auth::AuthType;
use pgrust::connection::tokio::TokioStream;
use pgrust::connection::{
    Client, Credentials, MaxRows, PipelineBuilder, Portal, ResolvedTarget, Statement,
};
use tokio::task::LocalSet;

use captive_postgres::*;

fn address(address: &ListenAddress) -> ResolvedTarget {
    match address {
        ListenAddress::Tcp(addr) => ResolvedTarget::SocketAddr(*addr),
        #[cfg(unix)]
        ListenAddress::Unix(path) => ResolvedTarget::UnixSocketAddr(
            std::os::unix::net::SocketAddr::from_pathname(path).unwrap(),
        ),
    }
}

async fn with_postgres<F, R>(callback: F) -> Result<(), Box<dyn std::error::Error>>
where
    F: FnOnce(Client<TokioStream, ()>) -> R,
    R: Future<Output = Result<(), Box<dyn std::error::Error>>>,
{
    let Some(postgres_process) = setup_postgres(AuthType::Trust, Mode::Tcp)? else {
        return Ok(());
    };

    let credentials = Credentials {
        username: DEFAULT_USERNAME.to_string(),
        password: DEFAULT_PASSWORD.to_string(),
        database: DEFAULT_DATABASE.to_string(),
        server_settings: Default::default(),
    };

    let socket = address(&postgres_process.socket_address).connect().await?;
    let (client, task) = Client::new(credentials, socket, ());

    LocalSet::new()
        .run_until(async move {
            tokio::task::spawn_local(task);
            client.ready().await?;
            callback(client).await?;
            Result::<(), Box<dyn std::error::Error>>::Ok(())
        })
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_query() -> Result<(), Box<dyn std::error::Error>> {
    with_postgres(|client| async move {
        client.query("SELECT 1", ()).await?;
        Ok(())
    })
    .await
}

#[test_log::test(tokio::test)]
async fn test_extended_query_success() -> Result<(), Box<dyn std::error::Error>> {
    with_postgres(|client| async move {
        client
            .pipeline_sync(
                PipelineBuilder::default()
                    .parse(Statement("test"), "SELECT generate_series(1, 10)", &[], ())
                    .bind(Portal("test"), Statement("test"), &[], &[], ())
                    .execute(
                        Portal("test"),
                        MaxRows::Limited(NonZero::new(1).unwrap()),
                        (),
                    )
                    .build(),
            )
            .await?;
        Ok(())
    })
    .await
}

#[test_log::test(tokio::test)]
async fn test_extended_query_parse_error() -> Result<(), Box<dyn std::error::Error>> {
    with_postgres(|client| async move {
        client
            .pipeline_sync(
                PipelineBuilder::default()
                    .parse(Statement("test"), ".", &[], ())
                    .bind(Portal("test"), Statement("test"), &[], &[], ())
                    .query("SELECT 1", ())
                    .build(),
            )
            .await?;
        Ok(())
    })
    .await
}

// test: execute portal suspended
// test: execute COPY
