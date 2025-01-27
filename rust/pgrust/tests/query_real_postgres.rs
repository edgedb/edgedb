use std::cell::RefCell;
use std::future::Future;
use std::num::NonZero;
use std::rc::Rc;

// Constants
use db_proto::match_message;
use gel_auth::AuthType;
use gel_stream::client::{Connector, ResolvedTarget, Target};
use pgrust::connection::{
    Client, Credentials, FlowAccumulator, MaxRows, Oid, Param, PipelineBuilder, Portal, Statement,
};
use pgrust::protocol::postgres::data::*;
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

async fn with_postgres<F, R>(callback: F) -> Result<Option<String>, Box<dyn std::error::Error>>
where
    F: FnOnce(Client, Rc<RefCell<FlowAccumulator>>) -> R,
    R: Future<Output = Result<(), Box<dyn std::error::Error>>>,
{
    let Some(postgres_process) = setup_postgres(AuthType::Trust, Mode::Tcp)? else {
        return Ok(None);
    };

    let credentials = Credentials {
        username: DEFAULT_USERNAME.to_string(),
        password: DEFAULT_PASSWORD.to_string(),
        database: DEFAULT_DATABASE.to_string(),
        server_settings: Default::default(),
    };

    let connector = Connector::new(Target::new_resolved(address(
        &postgres_process.socket_address,
    )))?;
    let (client, task) = Client::new(credentials, connector);
    let accumulator = Rc::new(RefCell::new(FlowAccumulator::default()));

    let accumulator2 = accumulator.clone();
    LocalSet::new()
        .run_until(async move {
            tokio::task::spawn_local(task);
            client.ready().await?;
            callback(client, accumulator2.clone()).await?;
            Result::<(), Box<dyn std::error::Error>>::Ok(())
        })
        .await?;

    let mut s = String::new();
    accumulator.borrow().with_messages(|message| {
        match_message!(Ok(message), Backend {
            (ParameterDescription as params) => {
                // OID values are not guaranteed to be stable, so we just print "..." instead.
                s.push_str(&format!("ParameterDescription {:?}\n", params.param_types().into_iter().map(|_| "...").collect::<Vec<_>>()));
            },
            (RowDescription as rows) => {
                s.push_str(&format!("RowDescription {}\n", rows.fields().into_iter().map(|f| f.name().to_string_lossy().into_owned()).collect::<Vec<_>>().join(", ")));
            },
            (PortalSuspended) => {
                s.push_str("PortalSuspended\n");
            },
            (ErrorResponse as err) => {
                for field in err.fields() {
                    if field.etype() as char == 'C' {
                        s.push_str(&format!("ErrorResponse {}\n", field.value().to_string_lossy()));
                        return;
                    }
                }
                s.push_str(&format!("ErrorResponse {:?}\n", err));
            },
            (NoticeResponse as notice) => {
                for field in notice.fields() {
                    if field.ntype() as char == 'M' {
                        s.push_str(&format!("NoticeResponse {}\n", field.value().to_string_lossy()));
                        return;
                    }
                }
                s.push_str(&format!("NoticeResponse {:?}\n", notice));
            },
            (CommandComplete as cmd) => {
                s.push_str(&format!("CommandComplete {:?}\n", cmd.tag()));
            },
            (DataRow as row) => {
                s.push_str(&format!("DataRow {}\n", row.values().into_iter().map(|v| v.to_string_lossy().into_owned()).collect::<Vec<_>>().join(", ")));
            },
            (CopyData as copy_data) => {
                s.push_str(&format!("CopyData {:?}\n", String::from_utf8_lossy(&copy_data.data())));
            },
            (CopyOutResponse as copy_out) => {
                s.push_str(&format!("CopyOutResponse {}\n", copy_out.format()));
            },
            _unknown => {
                s.push_str("Unknown\n");
            }
        })
    });

    Ok(Some(s))
}

#[test_log::test(tokio::test)]
async fn test_query() -> Result<(), Box<dyn std::error::Error>> {
    if let Some(s) = with_postgres(|client, accumulator| async move {
        client.query("SELECT 1", accumulator.clone()).await?;
        Ok(())
    })
    .await?
    {
        assert_eq!(
            s,
            "RowDescription ?column?\nDataRow 1\nCommandComplete \"SELECT 1\"\n"
        );
    }
    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_extended_query_success() -> Result<(), Box<dyn std::error::Error>> {
    if let Some(s) = with_postgres(|client, accumulator| async move {
        client
            .pipeline_sync(
                PipelineBuilder::default()
                    .parse(
                        Statement("test"),
                        "SELECT $1",
                        &[Oid::unspecified()],
                        accumulator.clone(),
                    )
                    .describe_statement(Statement("test"), accumulator.clone())
                    .bind(
                        Portal("test"),
                        Statement("test"),
                        &[Param::Text("1")],
                        &[],
                        accumulator.clone(),
                    )
                    .describe_portal(Portal("test"), accumulator.clone())
                    .execute(
                        Portal("test"),
                        MaxRows::Limited(NonZero::new(1).unwrap()),
                        accumulator.clone(),
                    )
                    .build(),
            )
            .await?;
        Ok(())
    })
    .await?
    {
        assert_eq!(s, "ParameterDescription [\"...\"]\nRowDescription ?column?\nRowDescription ?column?\nDataRow 1\nPortalSuspended\n");
    }
    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_extended_query_parse_error() -> Result<(), Box<dyn std::error::Error>> {
    if let Some(s) = with_postgres(|client, accumulator| async move {
        client
            .pipeline_sync(
                PipelineBuilder::default()
                    .parse(Statement("test"), ".", &[], accumulator.clone())
                    .bind(
                        Portal("test"),
                        Statement("test"),
                        &[],
                        &[],
                        accumulator.clone(),
                    )
                    .query("SELECT 1", accumulator.clone())
                    .build(),
            )
            .await?;
        Ok(())
    })
    .await?
    {
        assert_eq!(s, "ErrorResponse 42601\n");
    }
    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_extended_query_portal_suspended() -> Result<(), Box<dyn std::error::Error>> {
    if let Some(s) = with_postgres(|client, accumulator| async move {
        client
            .pipeline_sync(
                PipelineBuilder::default()
                    .parse(
                        Statement("test"),
                        "SELECT generate_series(1,3)",
                        &[],
                        accumulator.clone(),
                    )
                    .bind(
                        Portal("test"),
                        Statement("test"),
                        &[],
                        &[],
                        accumulator.clone(),
                    )
                    .execute(
                        Portal("test"),
                        MaxRows::Limited(NonZero::new(2).unwrap()),
                        accumulator.clone(),
                    )
                    .execute(
                        Portal("test"),
                        MaxRows::Limited(NonZero::new(2).unwrap()),
                        accumulator.clone(),
                    )
                    .build(),
            )
            .await?;
        Ok(())
    })
    .await?
    {
        assert_eq!(
            s,
            "DataRow 1\nDataRow 2\nPortalSuspended\nDataRow 3\nCommandComplete \"SELECT 1\"\n"
        );
    }
    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_extended_query_copy() -> Result<(), Box<dyn std::error::Error>> {
    if let Some(s) = with_postgres(|client, accumulator| async move {
        client
            .pipeline_sync(
                PipelineBuilder::default()
                    .parse(
                        Statement("test"),
                        "COPY (SELECT 1) TO STDOUT",
                        &[],
                        accumulator.clone(),
                    )
                    .bind(
                        Portal("test"),
                        Statement("test"),
                        &[],
                        &[],
                        accumulator.clone(),
                    )
                    .execute(Portal("test"), MaxRows::Unlimited, accumulator.clone())
                    .build(),
            )
            .await?;
        Ok(())
    })
    .await?
    {
        assert_eq!(
            s,
            "CopyOutResponse 0\nCopyData \"1\\n\"\nCommandComplete \"COPY 1\"\n"
        );
    }
    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_extended_query_empty() -> Result<(), Box<dyn std::error::Error>> {
    if let Some(s) = with_postgres(|client, accumulator| async move {
        client
            .pipeline_sync(
                PipelineBuilder::default()
                    .parse(Statement("test"), "", &[], accumulator.clone())
                    .bind(
                        Portal("test"),
                        Statement("test"),
                        &[],
                        &[],
                        accumulator.clone(),
                    )
                    .execute(Portal("test"), MaxRows::Unlimited, accumulator.clone())
                    .build(),
            )
            .await?;
        Ok(())
    })
    .await?
    {
        assert_eq!(s, "");
    }
    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_query_notice() -> Result<(), Box<dyn std::error::Error>> {
    if let Some(s) = with_postgres(|client, accumulator| async move {
        // DO block with NOTICE RAISE generates a notice
        client
            .query(
                "DO $$ BEGIN RAISE NOTICE 'test notice'; END $$;",
                accumulator.clone(),
            )
            .await?;
        Ok(())
    })
    .await?
    {
        assert_eq!(s, "NoticeResponse test notice\nCommandComplete \"DO\"\n");
    }
    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_query_warning() -> Result<(), Box<dyn std::error::Error>> {
    if let Some(s) = with_postgres(|client, accumulator| async move {
        // DO block with WARNING RAISE generates a warning
        client
            .query(
                "DO $$ BEGIN RAISE WARNING 'test warning'; END $$;",
                accumulator.clone(),
            )
            .await?;
        Ok(())
    })
    .await?
    {
        assert_eq!(s, "NoticeResponse test warning\nCommandComplete \"DO\"\n");
    }
    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_double_begin_transaction() -> Result<(), Box<dyn std::error::Error>> {
    if let Some(s) = with_postgres(|client, accumulator| async move {
        client
            .pipeline_sync(
                PipelineBuilder::default()
                    .query("BEGIN TRANSACTION", accumulator.clone())
                    .query("BEGIN TRANSACTION", accumulator.clone())
                    .build(),
            )
            .await?;
        Ok(())
    })
    .await?
    {
        assert_eq!(s, "CommandComplete \"BEGIN\"\nNoticeResponse there is already a transaction in progress\nCommandComplete \"BEGIN\"\n");
    }
    Ok(())
}
