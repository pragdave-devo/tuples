#![deny(warnings)]

use anyhow::Result;
use clap::Parser;
use proto::tuples::{
    tuples_client::TuplesClient, PutTupleRequest, RegisterPlaybookRequest, RegisterSchemaRequest,
};
use rand::Rng;
use std::time::{Duration, Instant};

const DEFAULT_SERVER: &str = "http://[::1]:50051";

#[derive(Parser)]
#[command(name = "tuples-bench", about = "Benchmark put_tuple throughput")]
struct Args {
    /// gRPC server address
    #[arg(long, default_value = DEFAULT_SERVER)]
    server: String,
    /// Number of distinct tuple types to generate
    #[arg(long, default_value_t = 10)]
    tuple_count: usize,
    /// Number of triggers in the playbook
    #[arg(long, default_value_t = 50)]
    trigger_count: usize,
    /// Total number of puts in the timed run
    #[arg(long, default_value_t = 1000)]
    run_size: usize,
}

/// (type_name, data_json)
type Pool = Vec<(String, String)>;

async fn setup(
    client: &mut TuplesClient<tonic::transport::Channel>,
    args: &Args,
    run_id: &str,
) -> Result<(Pool, usize)> {
    let schema_def =
        r#"{"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}"#;

    for i in 0..args.tuple_count {
        let name = format!("perf_{run_id}_type_{i}");
        client
            .register_schema(RegisterSchemaRequest {
                name,
                definition: schema_def.to_string(),
            })
            .await?;
    }

    let matching_count = std::cmp::min(
        std::cmp::max(1, (args.trigger_count as f64 * 0.2).round() as usize),
        args.trigger_count,
    );

    let agents = serde_json::json!([{
        "id": "perf_agent",
        "description": "",
        "schema": format!("perf_{run_id}_type_0")
    }]);

    let mut triggers = Vec::with_capacity(args.trigger_count);
    for i in 0..matching_count {
        let type_name = format!("perf_{run_id}_type_{}", i % args.tuple_count);
        triggers.push(serde_json::json!({
            "filter": {
                "id": format!("t_{i}"),
                "exact": { "type": type_name }
            },
            "agent": "perf_agent",
            "mapping": {}
        }));
    }
    for j in 0..(args.trigger_count - matching_count) {
        triggers.push(serde_json::json!({
            "filter": {
                "id": format!("t_miss_{j}"),
                "exact": { "type": format!("perf_{run_id}_miss_{j}") }
            },
            "agent": "perf_agent",
            "mapping": {}
        }));
    }

    let playbook = serde_json::json!({
        "name": format!("perf_{run_id}"),
        "description": "",
        "conductor": "perf_agent",
        "agents": agents,
        "triggers": triggers
    });

    client
        .register_playbook(RegisterPlaybookRequest {
            definition: serde_json::to_string(&playbook)?,
        })
        .await?;

    let pool: Pool = (0..args.tuple_count)
        .map(|i| {
            let type_name = format!("perf_{run_id}_type_{i}");
            let data = serde_json::json!({ "id": format!("perf-{i}") }).to_string();
            (type_name, data)
        })
        .collect();

    Ok((pool, matching_count))
}

async fn timed_run(
    client: &mut TuplesClient<tonic::transport::Channel>,
    pool: &Pool,
    run_size: usize,
) -> Result<Vec<Duration>> {
    let mut rng = rand::thread_rng();
    let mut latencies = Vec::with_capacity(run_size);

    for _ in 0..run_size {
        let idx = rng.gen_range(0..pool.len());
        let (type_name, data) = &pool[idx];
        let t0 = Instant::now();
        client
            .put_tuple(PutTupleRequest {
                r#type: type_name.clone(),
                trace_id: "perf".to_string(),
                data: data.clone(),
            })
            .await?;
        latencies.push(t0.elapsed());
    }

    Ok(latencies)
}

fn fmt_ms(d: Duration) -> String {
    format!("{:.1}ms", d.as_secs_f64() * 1000.0)
}

fn print_stats(mut latencies: Vec<Duration>, total: Duration, args: &Args, matching_count: usize) {
    let n = latencies.len();
    let puts_per_sec = n as f64 / total.as_secs_f64();

    println!(
        "Setup:   {} schemas, 1 playbook ({} triggers, {} matching)",
        args.tuple_count, args.trigger_count, matching_count
    );
    println!(
        "Run:     {} puts in {:.2}s  \u{2192}  {:.0} puts/sec",
        n,
        total.as_secs_f64(),
        puts_per_sec
    );
    println!();

    latencies.sort();
    let min = latencies[0];
    let p50 = latencies[n / 2];
    let p99 = latencies[(n * 99 / 100).min(n - 1)];
    let max = latencies[n - 1];

    println!("Latency  {:<7}{:<7}{:<7}{}", "min", "p50", "p99", "max");
    println!(
        "         {:<7}{:<7}{:<7}{}",
        fmt_ms(min),
        fmt_ms(p50),
        fmt_ms(p99),
        fmt_ms(max)
    );
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let run_id = format!("{:08x}", rand::random::<u32>());
    let mut client = TuplesClient::connect(args.server.clone()).await?;

    let (pool, matching_count) = setup(&mut client, &args, &run_id).await?;

    let start = Instant::now();
    let latencies = timed_run(&mut client, &pool, args.run_size).await?;
    let total = start.elapsed();

    print_stats(latencies, total, &args, matching_count);

    Ok(())
}
