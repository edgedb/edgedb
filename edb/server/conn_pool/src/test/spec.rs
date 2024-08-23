//! Test utilities.
use anyhow::{Error, Ok, Result};
use rand::{thread_rng, Rng};
use std::time::Duration;
use tokio::task::LocalSet;
use tracing::{error, info, trace};

use crate::{
    test::{BasicConnector, Latencies, WeightedScored},
    time::Instant,
    Pool, PoolConfig,
};

use super::{
    AbsoluteLatency, ConnectionOverhead, DBSpec, EndingCapacity, LatencyDistribution, LatencyRatio,
    QoS, Score, Spec, SuiteQoS, Triangle,
};

pub async fn run(spec: Spec) -> Result<QoS> {
    let local = LocalSet::new();
    let res = local.run_until(run_local(spec)).await?;
    local.await;
    Ok(res)
}

/// This is the general spec-running function used by all spec paths.
async fn run_local(spec: Spec) -> std::result::Result<QoS, Error> {
    let start = Instant::now();
    let real_time = std::time::Instant::now();
    let config = PoolConfig::suggested_default_for(spec.capacity)
        .with_min_idle_time_for_gc(Duration::from_secs_f64(spec.duration / 10.0));
    let disconnect_cost = spec.disconn_cost;
    let connect_cost = spec.disconn_cost;
    let conn_failure_percentage = spec.conn_failure_percentage;
    let connector = BasicConnector::delay(move |disconnect| {
        if conn_failure_percentage > 0 && thread_rng().gen_range(0..100) > conn_failure_percentage {
            return std::result::Result::Err(());
        }
        std::result::Result::Ok(if disconnect {
            disconnect_cost.random_duration()
        } else {
            connect_cost.random_duration()
        })
    });
    let pool = Pool::new(config, connector);
    let mut tasks = vec![];
    let latencies = Latencies::default();

    // Boot a task for each DBSpec in the Spec
    for (i, db_spec) in spec.dbs.into_iter().enumerate() {
        let interval = 1.0 / (db_spec.qps as f64);
        info!("[{i:-2}] db {db_spec:?}");
        let db = format!("t{}", db_spec.db);
        let pool = pool.clone();
        let latencies = latencies.clone();
        let local = async move {
            let now = Instant::now();
            let count = ((db_spec.end_at - db_spec.start_at) * (db_spec.qps as f64)) as usize;
            tokio::time::sleep(Duration::from_secs_f64(db_spec.start_at)).await;
            info!(
                "+[{i:-2}] Starting db {db} at {}qps (approx {}q·s/s from {}..{})...",
                db_spec.qps,
                db_spec.qps as f64 * db_spec.query_cost.0,
                db_spec.start_at,
                db_spec.end_at,
            );
            let start_time = now.elapsed().as_secs_f64();
            // Boot one task for each expected query in a localset, with a
            // sleep that schedules it for the appropriate time.
            let local = LocalSet::new();
            for i in 0..count {
                let pool = pool.clone();
                let latencies = latencies.clone();
                let duration = db_spec.query_cost.random_duration();
                let db = db.clone();
                local.spawn_local(async move {
                    tokio::time::sleep(Duration::from_secs_f64(i as f64 * interval)).await;
                    let now = Instant::now();
                    let conn = pool.acquire(&db).await?;
                    let latency = now.elapsed();
                    latencies.mark(&db, latency.as_secs_f64());
                    tokio::time::sleep(duration).await;
                    drop(conn);
                    Ok(())
                });
            }
            tokio::time::timeout(Duration::from_secs(120), local)
                .await
                .unwrap_or_else(move |_| error!("*[{i:-2}] DBSpec {i} for {db} timed out"));
            let end_time = now.elapsed().as_secs_f64();
            info!("-[{i:-2}] Finished db t{} at {}qps. Load generated from {}..{}, processed from {}..{}",
                    db_spec.db, db_spec.qps, db_spec.start_at, db_spec.end_at, start_time, end_time);
        };
        tasks.push(tokio::task::spawn_local(local));
    }

    // Boot the monitor the runs the pool algorithm and prints the current
    // block connection stats.
    let monitor = {
        let pool = pool.clone();
        tokio::task::spawn_local(async move {
            let mut orig = "".to_owned();
            loop {
                pool.run_once();
                let mut s = "".to_owned();
                for (name, block) in pool.metrics().blocks {
                    s += &format!("{name}={} ", block.total);
                }
                if !s.is_empty() && s != orig {
                    trace!(
                        "Blocks: {}/{} {s}",
                        pool.metrics().pool.total,
                        pool.config.constraints.max
                    );
                    orig = s;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
    };

    info!("Starting...");
    tokio::time::sleep(Duration::from_secs_f64(spec.duration)).await;

    for task in tasks {
        _ = task.await;
    }

    info!(
        "Took {:?} of virtual time ({:?} real time)",
        start.elapsed(),
        real_time.elapsed()
    );

    monitor.abort();
    _ = monitor.await;
    let metrics = pool.metrics();
    info!("{metrics:#?}");
    info!("{latencies:#?}");

    let metrics = pool.metrics();
    let mut qos = 0.0;
    let mut scores = vec![];
    for score in spec.score {
        let scored = score.method.score(&latencies, &metrics, &pool.config);

        let score_component = score.calculate(scored.raw_value);
        info!(
            "[QoS: {}] {} = {:.2} -> {:.2} (weight {:.2})",
            spec.name, scored.description, scored.raw_value, score_component, score.weight
        );
        trace!(
            "[QoS: {}] {} [detail]: {} = {:.3}",
            spec.name,
            scored.description,
            (scored.detailed_calculation)(3),
            scored.raw_value
        );
        scores.push(WeightedScored {
            scored,
            weight: score.weight,
            score: score_component,
        });
        qos += score_component * score.weight;
    }
    info!("[QoS: {}] Score = {qos:0.02}", spec.name);

    info!("Shutting down...");
    pool.shutdown().await;

    Ok(QoS { scores, qos })
}

fn test_connpool_1() -> Spec {
    let mut dbs = vec![];
    for i in 0..6 {
        dbs.push(DBSpec {
            db: i,
            start_at: 0.0,
            end_at: 0.5,
            qps: 50,
            query_cost: Triangle(0.03, 0.005),
        })
    }
    for i in 6..12 {
        dbs.push(DBSpec {
            db: i,
            start_at: 0.3,
            end_at: 0.7,
            qps: 50,
            query_cost: Triangle(0.03, 0.005),
        })
    }
    for i in 0..6 {
        dbs.push(DBSpec {
            db: i,
            start_at: 0.6,
            end_at: 0.8,
            qps: 50,
            query_cost: Triangle(0.03, 0.005),
        })
    }

    Spec {
        name: "test_connpool_1".into(),
        desc: r#"
            This is a test for Mode D, where 2 groups of blocks race for connections
            in the pool with max capacity set to 6. The first group (0-5) has more
            dedicated time with the pool, so it should have relatively lower latency
            than the second group (6-11). But the QoS is focusing on the latency
            distribution similarity, as we don't want to starve only a few blocks
            because of the lack of capacity. Therefore, reconnection is a necessary
            cost for QoS.
        "#,
        capacity: 6,
        conn_cost: Triangle(0.05, 0.01),
        score: vec![
            Score::new(
                0.18,
                [2.0, 0.5, 0.25, 0.0],
                LatencyDistribution { group: 0..6 },
            ),
            Score::new(
                0.28,
                [2.0, 0.3, 0.1, 0.0],
                LatencyDistribution { group: 6..12 },
            ),
            Score::new(
                0.48,
                [2.0, 0.7, 0.45, 0.2],
                LatencyDistribution { group: 0..12 },
            ),
            Score::new(0.06, [0.5, 0.2, 0.1, 0.0], ConnectionOverhead {}),
        ],
        dbs,
        ..Default::default()
    }
}

fn test_connpool_2() -> Spec {
    let mut dbs = vec![];
    for i in 0..6 {
        dbs.push(DBSpec {
            db: i,
            start_at: 0.0,
            end_at: 0.5,
            qps: 1500,
            query_cost: Triangle(0.001, 0.005),
        })
    }
    for i in 6..12 {
        dbs.push(DBSpec {
            db: i,
            start_at: 0.3,
            end_at: 0.7,
            qps: 700,
            query_cost: Triangle(0.03, 0.001),
        })
    }
    for i in 0..6 {
        dbs.push(DBSpec {
            db: i,
            start_at: 0.6,
            end_at: 0.8,
            qps: 700,
            query_cost: Triangle(0.06, 0.01),
        })
    }

    Spec {
        name: "test_connpool_2".into(),
        desc: r#"
            In this test, we have 6x1500qps connections that simulate fast
            queries (0.001..0.006s), and 6x700qps connections that simulate
            slow queries (~0.03s). The algorithm allocates connections
            fairly to both groups, essentially using the
            "demand = avg_query_time * avg_num_of_connection_waiters"
            formula. The QoS is at the same level for all DBs. (Mode B / C)
        "#,
        capacity: 100,
        conn_cost: Triangle(0.04, 0.011),
        score: vec![
            Score::new(
                0.18,
                [2.0, 0.5, 0.25, 0.0],
                LatencyDistribution { group: 0..6 },
            ),
            Score::new(
                0.28,
                [2.0, 0.3, 0.1, 0.0],
                LatencyDistribution { group: 6..12 },
            ),
            Score::new(
                0.48,
                [2.0, 0.7, 0.45, 0.2],
                LatencyDistribution { group: 0..12 },
            ),
            Score::new(0.06, [0.5, 0.2, 0.1, 0.0], ConnectionOverhead {}),
        ],
        dbs,
        ..Default::default()
    }
}

fn test_connpool_3() -> Spec {
    let mut dbs = vec![];
    for i in 0..6 {
        dbs.push(DBSpec {
            db: i,
            start_at: 0.0,
            end_at: 0.8,
            qps: 5000,
            query_cost: Triangle(0.01, 0.005),
        })
    }

    Spec {
        name: "test_connpool_3".into(),
        desc: r#"
            This test simply starts 6 same crazy requesters for 6 databases to
            test the pool fairness in Mode C with max capacity of 100.
        "#,
        capacity: 100,
        conn_cost: Triangle(0.04, 0.011),
        score: vec![
            Score::new(
                0.85,
                [1.0, 0.2, 0.1, 0.0],
                LatencyDistribution { group: 0..6 },
            ),
            Score::new(0.15, [0.5, 0.2, 0.1, 0.0], ConnectionOverhead {}),
        ],
        dbs,
        ..Default::default()
    }
}

fn test_connpool_4() -> Spec {
    let mut dbs = vec![];
    for i in 0..6 {
        dbs.push(DBSpec {
            db: i,
            start_at: 0.0,
            end_at: 0.8,
            qps: 1000,
            query_cost: Triangle(0.01 * (i as f64 + 1.0), 0.005 * (i as f64 + 1.0)),
        })
    }

    Spec {
        name: "test_connpool_4".into(),
        desc: r#"
            Similar to test 3, this test also has 6 requesters for 6 databases,
            they have the same Q/s but with different query cost. In Mode C,
            we should observe equal connection acquisition latency, fair and
            stable connection distribution and reasonable reconnection cost.
        "#,
        capacity: 50,
        conn_cost: Triangle(0.04, 0.011),
        score: vec![
            Score::new(
                0.9,
                [1.0, 0.2, 0.1, 0.0],
                LatencyDistribution { group: 0..6 },
            ),
            Score::new(0.1, [0.5, 0.2, 0.1, 0.0], ConnectionOverhead {}),
        ],
        dbs,
        ..Default::default()
    }
}

fn test_connpool_5() -> Spec {
    let mut dbs = vec![];

    for i in 0..6 {
        dbs.push(DBSpec {
            db: i,
            start_at: 0.0 + i as f64 / 10.0,
            end_at: 0.5 + i as f64 / 10.0,
            qps: 150,
            query_cost: Triangle(0.020, 0.005),
        });
    }
    for i in 6..12 {
        dbs.push(DBSpec {
            db: i,
            start_at: 0.3,
            end_at: 0.7,
            qps: 50,
            query_cost: Triangle(0.008, 0.003),
        });
    }
    for i in 0..6 {
        dbs.push(DBSpec {
            db: i,
            start_at: 0.6,
            end_at: 0.8,
            qps: 50,
            query_cost: Triangle(0.003, 0.002),
        });
    }

    Spec {
        name: "test_connpool_5".into(),
        desc: r#"
            This is a mixed test with pool max capacity set to 6. Requests in
            the first group (0-5) come and go alternatively as time goes on,
            even with different query cost, so its latency similarity doesn't
            matter much, as far as the latency distribution is not too crazy
            and unstable. However the second group (6-11) has a stable
            environment - pressure from the first group is quite even at the
            time the second group works. So we should observe a high similarity
            in the second group. Also due to a low query cost, the second group
            should have a higher priority in connection acquisition, therefore
            a much lower latency distribution comparing to the first group.
            Pool Mode wise, we should observe a transition from Mode A to C,
            then D and eventually back to C. One regression to be aware of is
            that, the last D->C transition should keep the pool running at
            a full capacity.
        "#,
        capacity: 6,
        conn_cost: Triangle(0.15, 0.05),
        score: vec![
            Score::new(
                0.05,
                [2.0, 0.8, 0.4, 0.0],
                LatencyDistribution { group: 0..6 },
            ),
            Score::new(
                0.25,
                [2.0, 0.8, 0.4, 0.0],
                LatencyDistribution { group: 6..12 },
            ),
            Score::new(
                0.45,
                [1.0, 2.0, 5.0, 30.0],
                LatencyRatio {
                    percentile: 75,
                    dividend: 0..6,
                    divisor: 6..12,
                },
            ),
            Score::new(0.15, [0.5, 0.2, 0.1, 0.0], ConnectionOverhead {}),
            Score::new(0.10, [3.0, 4.0, 5.0, 6.0], EndingCapacity {}),
        ],
        dbs,
        ..Default::default()
    }
}

fn test_connpool_6() -> Spec {
    let mut dbs = vec![];

    for i in 0..6 {
        dbs.push(DBSpec {
            db: 0,
            start_at: 0.0 + i as f64 / 10.0,
            end_at: 0.5 + i as f64 / 10.0,
            qps: 150,
            query_cost: Triangle(0.020, 0.005),
        });
    }

    Spec {
        name: "test_connpool_6".into(),
        desc: r#"
            This is a simple test for Mode A. In this case, we don't want to
            have lots of reconnection overhead.
        "#,
        capacity: 6,
        conn_cost: Triangle(0.15, 0.05),
        score: vec![Score::new(1.0, [0.5, 0.2, 0.1, 0.0], ConnectionOverhead {})],
        dbs,
        ..Default::default()
    }
}

fn test_connpool_7() -> Spec {
    Spec {
        name: "test_connpool_7".into(),
        desc: r#"
            The point of this test is to have one connection "t1" that
            just has crazy demand for connections.  Then the "t2" connections
            are infrequent -- so they have a miniscule quota.

            Our goal is to make sure that "t2" has good QoS and gets
            its queries processed as soon as they're submitted. Therefore,
            "t2" should have way lower connection acquisition cost than "t1".
        "#,
        capacity: 6,
        conn_cost: Triangle(0.15, 0.05),
        score: vec![
            Score::new(
                0.2,
                [1.0, 10.0, 50.0, 100.0],
                LatencyRatio {
                    percentile: 99,
                    dividend: 1..2,
                    divisor: 2..3,
                },
            ),
            Score::new(
                0.4,
                [1.0, 20.0, 100.0, 200.0],
                LatencyRatio {
                    percentile: 75,
                    dividend: 1..2,
                    divisor: 2..3,
                },
            ),
            Score::new(0.4, [0.5, 0.2, 0.1, 0.0], ConnectionOverhead {}),
        ],
        dbs: vec![
            DBSpec {
                db: 1,
                start_at: 0.0,
                end_at: 1.0,
                qps: 500,
                query_cost: Triangle(0.040, 0.005),
            },
            DBSpec {
                db: 2,
                start_at: 0.1,
                end_at: 0.3,
                qps: 30,
                query_cost: Triangle(0.030, 0.005),
            },
            DBSpec {
                db: 2,
                start_at: 0.6,
                end_at: 0.9,
                qps: 30,
                query_cost: Triangle(0.010, 0.005),
            },
        ],
        ..Default::default()
    }
}

fn test_connpool_8() -> Spec {
    let base_load = 200;

    Spec {
        name: "test_connpool_8".into(),
        desc: r#"
            This test spec is to check the pool connection reusability with a
            single block before the pool reaches its full capacity in Mode A.
            We should observe just enough number of connects to serve the load,
            while there can be very few disconnects because of GC.
        "#,
        capacity: 100,
        conn_cost: Triangle(0.0, 0.0),
        score: vec![Score::new(1.0, [0.5, 0.2, 0.1, 0.0], ConnectionOverhead {})],
        dbs: vec![
            DBSpec {
                db: 1,
                start_at: 0.0,
                end_at: 0.1,
                qps: base_load / 4,
                query_cost: Triangle(0.01, 0.0),
            },
            DBSpec {
                db: 1,
                start_at: 0.1,
                end_at: 0.2,
                qps: base_load / 2,
                query_cost: Triangle(0.01, 0.0),
            },
            DBSpec {
                db: 1,
                start_at: 0.2,
                end_at: 0.6,
                qps: base_load,
                query_cost: Triangle(0.01, 0.0),
            },
        ],
        ..Default::default()
    }
}

fn test_connpool_9() -> Spec {
    let full_qps = 20000;

    Spec {
        name: "test_connpool_9".into(),
        desc: r#"
            This test spec is to check the pool performance with low traffic
            between 3 pre-heated blocks in Mode B. t1 is a reference block,
            t2 has the same qps as t1, but t3 with doubled qps came in while t2
            is active. As the total throughput is low enough, we shouldn't have
            a lot of connects and disconnects, nor a high acquire waiting time.
        "#,
        capacity: 100,
        conn_cost: Triangle(0.01, 0.005),
        score: vec![
            Score::new(
                0.1,
                [2.0, 1.0, 0.5, 0.2],
                LatencyDistribution { group: 1..4 },
            ),
            Score::new(
                0.1,
                [0.05, 0.004, 0.002, 0.001],
                AbsoluteLatency {
                    group: 1..4,
                    percentile: 99,
                },
            ),
            Score::new(
                0.2,
                [0.005, 0.0004, 0.0002, 0.0001],
                AbsoluteLatency {
                    group: 1..4,
                    percentile: 75,
                },
            ),
            Score::new(0.6, [0.5, 0.2, 0.1, 0.0], ConnectionOverhead {}),
        ],
        dbs: vec![
            DBSpec {
                db: 1,
                start_at: 0.0,
                end_at: 0.1,
                qps: (full_qps / 32),
                query_cost: Triangle(0.01, 0.005),
            },
            DBSpec {
                db: 1,
                start_at: 0.1,
                end_at: 0.4,
                qps: (full_qps / 16),
                query_cost: Triangle(0.01, 0.005),
            },
            DBSpec {
                db: 2,
                start_at: 0.5,
                end_at: 0.6,
                qps: (full_qps / 32),
                query_cost: Triangle(0.01, 0.005),
            },
            DBSpec {
                db: 2,
                start_at: 0.6,
                end_at: 1.0,
                qps: (full_qps / 16),
                query_cost: Triangle(0.01, 0.005),
            },
            DBSpec {
                db: 3,
                start_at: 0.7,
                end_at: 0.8,
                qps: (full_qps / 16),
                query_cost: Triangle(0.01, 0.005),
            },
            DBSpec {
                db: 3,
                start_at: 0.8,
                end_at: 0.9,
                qps: (full_qps / 8),
                query_cost: Triangle(0.01, 0.005),
            },
        ],
        ..Default::default()
    }
}

fn test_connpool_10() -> Spec {
    let full_qps = 2000;

    Spec {
        name: "test_connpool_10".into(),
        desc: r#"
            This test spec is to check the pool garbage collection feature.
            t1 is a constantly-running reference block, t2 starts in the middle
            with a full qps and ends early to leave enough time for the pool to
            execute garbage collection.
        "#,
        timeout: 10,
        duration: 2.0,
        capacity: 100,
        conn_cost: Triangle(0.01, 0.005),
        score: vec![Score::new(
            1.0,
            [100.0, 40.0, 20.0, 10.0],
            EndingCapacity {},
        )],
        dbs: vec![
            DBSpec {
                db: 1,
                start_at: 0.0,
                end_at: 1.0,
                qps: (full_qps / 32),
                query_cost: Triangle(0.01, 0.005),
            },
            DBSpec {
                db: 2,
                start_at: 0.4,
                end_at: 0.6,
                qps: ((full_qps / 32) * 31),
                query_cost: Triangle(0.01, 0.005),
            },
        ],
        ..Default::default()
    }
}

#[cfg(test)]
#[test_log::test(tokio::test(flavor = "current_thread", start_paused = true))]
async fn run_spec_tests() -> Result<()> {
    let qos = spec_tests(None, &|_| true).await?;
    eprintln!("QoS = {qos:?}");
    // assert!(qos.qos() > 85.0, "Avg QoS failed: {}", qos.qos());
    // assert!(qos.qos_min() > 70.0, "Min QoS failed: {}", qos.qos_min());
    Ok(())
}

#[cfg(test)]
async fn spec_tests(
    scale: Option<f64>,
    spec_predicate: &impl Fn(&'static str) -> bool,
) -> Result<SuiteQoS> {
    let mut results = SuiteQoS::default();
    for (name, spec) in SPEC_FUNCTIONS {
        if !spec_predicate(name) {
            continue;
        }
        let mut spec = spec();
        if let Some(scale) = scale {
            spec.scale(scale);
        }
        let name = spec.name.clone();
        let res = run(spec).await?;
        results.insert(name, res);
    }
    for (name, QoS { qos, .. }) in &results {
        info!("QoS[{name}] = [{qos:.02}]");
    }
    info!(
        "QoS = [{:.02}] (rms={:.02})",
        results.qos(),
        results.qos_rms_error()
    );
    Ok(results)
}

/// Runs the specs `count` times, returning the median run.
pub fn run_specs_tests_in_runtime(
    count: usize,
    scale: Option<f64>,
    spec_predicate: &impl Fn(&'static str) -> bool,
) -> Result<SuiteQoS> {
    let mut handles = vec![];
    for _ in 0..count {
        let mut suite_handles = vec![];
        for (name, spec) in SPEC_FUNCTIONS {
            if !spec_predicate(name) {
                continue;
            }
            let mut spec = spec();
            if let Some(scale) = scale {
                spec.scale(scale);
            }
            let h = std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build()
                    .unwrap();
                let _guard = runtime.enter();
                tokio::time::pause();
                let qos = runtime.block_on(run(spec))?;
                Ok((name, qos))
            });
            suite_handles.push(h);
        }
        handles.push(suite_handles);
    }
    let mut runs = vec![];
    for suite_handles in handles {
        let mut suite = SuiteQoS::default();
        for handle in suite_handles {
            let (name, qos) = handle
                .join()
                .map_err(|e| anyhow::anyhow!("Thread failed: {e:?}"))??;
            suite.insert(name.into(), qos);
        }
        runs.push(suite)
    }
    runs.sort_by_cached_key(|run| (run.qos_rms_error() * 1_000_000.0) as usize);
    let ret = runs.drain(count / 2..).next().unwrap();
    Ok(ret)
}

macro_rules! run_spec {
    ($($spec:ident),* $(,)?) => {
        const SPEC_FUNCTIONS: [(&'static str, fn() -> Spec); [$( $spec ),*].len()] = [
            $(
                (stringify!($spec), $spec),
            )*
        ];

        #[cfg(test)]
        mod spec {
            use super::*;
            $(
                #[::test_log::test]
                fn $spec() -> Result<()> {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_time()
                        .build()
                        .unwrap();
                    let _guard = runtime.enter();
                    tokio::time::pause();
                    let qos = runtime.block_on(run(super::$spec()))?;
                    eprintln!("QoS = {qos:?}");
                    // assert!(qos.qos > 80.0, "QoS failed: {}", qos.qos);
                    Ok(())
                }
            )*
        }
    };
}

run_spec!(
    test_connpool_1,
    test_connpool_2,
    test_connpool_3,
    test_connpool_4,
    test_connpool_5,
    test_connpool_6,
    test_connpool_7,
    test_connpool_8,
    test_connpool_9,
    test_connpool_10,
);
