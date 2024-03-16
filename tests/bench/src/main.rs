#![allow(unused_imports, unused)]

mod helpers;
use std::{cell::OnceCell, error::Error, sync::Arc, thread, time::Duration};

use edgedb_protocol::{
    codec::{ObjectShape, ShapeElement},
    common::Cardinality as Cd,
    value::{EnumValue, Value as EValue},
};
use tokio::{
    sync::{Mutex, RwLock},
    task::JoinSet,
    time::{error::Elapsed, Instant},
};

static THREADS: usize = 4;
static DURATION_MS: u128 = 25_000;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let edb = edgedb_tokio::Client::new(
        &edgedb_tokio::Builder::new()
            .client_security(edgedb_tokio::ClientSecurity::InsecureDevMode)
            .dsn(std::env::var("EDGEDB_DSN").unwrap().as_str())
            .unwrap()
            .build_env()
            .await
            .unwrap(),
    );
    edb.ensure_connected().await?;

    println!("Threads: {THREADS}, Test duraiton: {DURATION_MS}");

    print!("simple query: ");
    test_it(&edb, "select 1 + 1".to_string(), EValue::Nothing).await;

    print!("normal query: ");
    test_it(
        &edb,
        include_str!("../dumps/query2.edgeql").to_string(),
        edgedb_args! {
            "me_num" => (Some(EValue::Int64(1)), Cd::One),
            "q" =>  (None, Cd::AtMostOne),
            "nickname" => (Some(EValue::Str("red".to_string())), Cd::AtMostOne)
        },
    )
    .await;

    print!("complex query: ");
    test_it(
        &edb,
        include_str!("../dumps/query1.edgeql").to_string(),
        edgedb_args! {
            "me" => (Some(EValue::Int64(1)), Cd::One),
            "e" => (None, Cd::AtMostOne),
            "q" => (None, Cd::AtMostOne),
            "sphere" => (None, Cd::AtMostOne),
            "lower" => (None, Cd::AtMostOne),
            "upper" => (None, Cd::AtMostOne),
            "geoarea" => (None, Cd::AtMostOne)
        },
    )
    .await;

    Ok(())
}

async fn test_it(edb: &edgedb_tokio::Client, query: String, args: EValue) {
    let query = Arc::new(query);
    let args = Arc::new(args);

    let mut latencies = Arc::new(Mutex::new(Vec::with_capacity(THREADS * 10_000)));

    let started_at = Instant::now();
    let threads: Vec<tokio::task::JoinHandle<()>> = (0..THREADS)
        .map(|thread_id| {
            let handle = tokio::runtime::Handle::current();

            let query = Arc::clone(&query);
            let args = Arc::clone(&args);
            let latencies = Arc::clone(&latencies);
            let edb = edb.clone();

            handle.spawn(async move {
                loop {
                    if started_at.elapsed().as_millis() >= DURATION_MS {
                        break;
                    }
                    let n = Instant::now();
                    edb.query_json(&query, &*args).await.unwrap();
                    let latency = n.elapsed().as_millis();
                    let mut latencies = latencies.lock().await;
                    latencies.push(latency as f32);
                }
            })
        })
        .collect();

    for t in threads {
        t.await.unwrap();
    }

    let latencies = latencies.lock().await;
    let count = latencies.len() as f32;

    let rps = (count / DURATION_MS as f32) * 1000.0;
    let avg_query_latency = latencies.iter().sum::<f32>() / count as f32;

    println!("rps: {rps:.0}, avg query latency: {avg_query_latency:.0}ms");
}
