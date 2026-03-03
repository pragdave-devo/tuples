#![deny(warnings)]

use anyhow::Result;
use clap::Parser;
use proto::tuples::{
    agents_client::AgentsClient, health_client::HealthClient, playbooks_client::PlaybooksClient,
    schemas_client::SchemasClient, tuple_store_client::TupleStoreClient, Empty, PutTupleRequest,
    RegisterAgentRequest, RegisterPlaybookRequest, RegisterSchemaRequest,
};
use rand::Rng;
use std::time::{Duration, Instant};
use tonic::transport::Channel;

const DEFAULT_SERVER: &str = "http://[::1]:50051";

#[derive(Parser)]
#[command(name = "tuples-bench", about = "Benchmark put_tuple throughput")]
struct Args {
    /// gRPC server address
    #[arg(long, default_value = DEFAULT_SERVER)]
    server: String,
    /// Number of schemas to create
    #[arg(long, default_value_t = 10)]
    schemas: usize,
    /// Number of triggers to create
    #[arg(long, default_value_t = 20)]
    triggers: usize,
    /// Tuples per batch write
    #[arg(long, default_value_t = 100)]
    batch_size: usize,
    /// Total tuples to write
    #[arg(long, default_value_t = 10000)]
    total_tuples: usize,
    /// Percentage of tuples that should fire triggers (0-100)
    #[arg(long, default_value_t = 50)]
    trigger_pct: u8,
    /// Use guaranteed_write=true for every put
    #[arg(long)]
    guaranteed: bool,
}

/// (type_name, data_json)
type Pool = Vec<(String, String)>;

/// Property types we randomly assign to schemas.
#[derive(Clone, Copy)]
enum PropType {
    String,
    Number,
    Integer,
    Boolean,
}

impl PropType {
    fn json_type(self) -> &'static str {
        match self {
            PropType::String => "string",
            PropType::Number => "number",
            PropType::Integer => "integer",
            PropType::Boolean => "boolean",
        }
    }

    fn random_value(self, rng: &mut impl Rng) -> serde_json::Value {
        match self {
            PropType::String => {
                let s: String = (0..8)
                    .map(|_| {
                        let idx = rng.gen_range(0..36);
                        if idx < 10 {
                            (b'0' + idx) as char
                        } else {
                            (b'a' + idx - 10) as char
                        }
                    })
                    .collect();
                serde_json::Value::String(s)
            }
            PropType::Number => {
                let v: f64 = rng.gen_range(0.0..1000.0);
                serde_json::json!(v)
            }
            PropType::Integer => {
                let v: i64 = rng.gen_range(0..1000);
                serde_json::json!(v)
            }
            PropType::Boolean => serde_json::json!(rng.gen_bool(0.5)),
        }
    }

    fn random(rng: &mut impl Rng) -> Self {
        match rng.gen_range(0..4) {
            0 => PropType::String,
            1 => PropType::Number,
            2 => PropType::Integer,
            _ => PropType::Boolean,
        }
    }
}

/// Schema definition: name + extra properties (beyond "status").
struct SchemaDef {
    name: String,
    extra_props: Vec<(String, PropType)>,
}

fn generate_schema_defs(rng: &mut impl Rng, count: usize) -> Vec<SchemaDef> {
    (0..count)
        .map(|i| {
            let n_extra = rng.gen_range(1..=3usize);
            let extra_props: Vec<(String, PropType)> = (0..n_extra)
                .map(|j| (format!("field_{j}"), PropType::random(rng)))
                .collect();
            SchemaDef {
                name: format!("perf_type_{i}"),
                extra_props,
            }
        })
        .collect()
}

fn schema_definition_json(def: &SchemaDef) -> String {
    let mut props = serde_json::Map::new();
    props.insert(
        "status".to_string(),
        serde_json::json!({ "type": "string" }),
    );
    for (name, ptype) in &def.extra_props {
        props.insert(
            name.clone(),
            serde_json::json!({ "type": ptype.json_type() }),
        );
    }
    serde_json::json!({
        "type": "object",
        "properties": props,
        "required": ["status"]
    })
    .to_string()
}

async fn setup(
    health: &mut HealthClient<Channel>,
    schemas_client: &mut SchemasClient<Channel>,
    playbooks_client: &mut PlaybooksClient<Channel>,
    agents_client: &mut AgentsClient<Channel>,
    args: &Args,
    schema_defs: &[SchemaDef],
) -> Result<()> {
    // Clear all existing data.
    health.clear_all(Empty {}).await?;

    // Register schemas.
    for def in schema_defs {
        schemas_client
            .register_schema(RegisterSchemaRequest {
                name: def.name.clone(),
                definition: schema_definition_json(def),
            })
            .await?;
    }

    // Register agent.
    agents_client
        .register_agent(RegisterAgentRequest {
            name: "perf_agent".to_string(),
            description: "Perf test agent".to_string(),
            schema: schema_defs[0].name.clone(),
        })
        .await?;

    // Build triggers.
    let mut triggers = Vec::with_capacity(args.triggers);
    for i in 0..args.triggers {
        let schema_idx = if i < args.schemas {
            i
        } else {
            rand::thread_rng().gen_range(0..args.schemas)
        };
        let type_name = &schema_defs[schema_idx].name;
        triggers.push(serde_json::json!({
            "id": format!("t_{i}"),
            "match": {
                "tuple_type": type_name,
                "filter": {
                    "id": format!("f_{i}"),
                    "exact": { "status": "fire" }
                }
            },
            "execution": {
                "agent": "perf_agent",
                "params": {}
            }
        }));
    }

    let playbook = serde_json::json!({
        "name": "perf_playbook",
        "description": "Perf test playbook",
        "conductor": "perf_agent",
        "agents": ["perf_agent"],
        "triggers": triggers
    });

    playbooks_client
        .register_playbook(RegisterPlaybookRequest {
            definition: serde_json::to_string(&playbook)?,
        })
        .await?;

    Ok(())
}

fn generate_pool(
    rng: &mut impl Rng,
    schema_defs: &[SchemaDef],
    pool_size: usize,
    trigger_pct: u8,
) -> Pool {
    (0..pool_size)
        .map(|_| {
            let def = &schema_defs[rng.gen_range(0..schema_defs.len())];
            let fires = rng.gen_range(0..100) < trigger_pct;
            let status = if fires { "fire" } else { "idle" };

            let mut data = serde_json::Map::new();
            data.insert("status".to_string(), serde_json::json!(status));
            for (name, ptype) in &def.extra_props {
                data.insert(name.clone(), ptype.random_value(rng));
            }

            (def.name.clone(), serde_json::Value::Object(data).to_string())
        })
        .collect()
}

struct RunStats {
    batch_durations: Vec<Duration>,
    put_latencies: Vec<Duration>,
}

async fn timed_run(
    client: &mut TupleStoreClient<Channel>,
    pool: &Pool,
    args: &Args,
) -> Result<RunStats> {
    let mut rng = rand::thread_rng();
    let batch_count = args.total_tuples / args.batch_size;
    let pool_size = pool.len();

    let mut batch_durations = Vec::with_capacity(batch_count);
    let mut put_latencies = Vec::with_capacity(args.total_tuples);

    for _ in 0..batch_count {
        let start_idx = rng.gen_range(0..args.batch_size);
        let batch_start = Instant::now();

        for j in 0..args.batch_size {
            let idx = (start_idx + j) % pool_size;
            let (type_name, data) = &pool[idx];
            let t0 = Instant::now();
            client
                .put_tuple(PutTupleRequest {
                    r#type: type_name.clone(),
                    trace_id: "perf".to_string(),
                    data: data.clone(),
                    guaranteed_write: args.guaranteed,
                })
                .await?;
            put_latencies.push(t0.elapsed());
        }

        batch_durations.push(batch_start.elapsed());
    }

    Ok(RunStats {
        batch_durations,
        put_latencies,
    })
}

fn percentile(sorted: &[Duration], pct: f64) -> Duration {
    let idx = ((sorted.len() as f64 * pct / 100.0) as usize).min(sorted.len() - 1);
    sorted[idx]
}

fn fmt_ms(d: Duration) -> String {
    format!("{:.1}", d.as_secs_f64() * 1000.0)
}

fn print_stats(stats: RunStats, total: Duration, args: &Args) {
    let batch_count = args.total_tuples / args.batch_size;
    let actual_tuples = batch_count * args.batch_size;
    let tuples_per_sec = actual_tuples as f64 / total.as_secs_f64();

    println!(
        "Config:  {} schemas, {} triggers, {}% trigger rate",
        args.schemas, args.triggers, args.trigger_pct
    );
    println!(
        "         batch_size={}, total={} ({} batches), guaranteed={}",
        args.batch_size, actual_tuples, batch_count, args.guaranteed
    );
    println!();
    println!(
        "Batches: {} batches in {:.2}s \u{2192} {:.0} tuples/sec",
        batch_count,
        total.as_secs_f64(),
        tuples_per_sec
    );
    println!();

    // Batch latency
    let mut batch_sorted = stats.batch_durations;
    batch_sorted.sort();
    if !batch_sorted.is_empty() {
        println!("Batch latency (ms):");
        println!(
            "         {:<10}{:<10}{:<10}{}",
            "min", "p50", "p99", "max"
        );
        println!(
            "         {:<10}{:<10}{:<10}{}",
            fmt_ms(batch_sorted[0]),
            fmt_ms(percentile(&batch_sorted, 50.0)),
            fmt_ms(percentile(&batch_sorted, 99.0)),
            fmt_ms(*batch_sorted.last().unwrap())
        );
        println!();
    }

    // Put latency
    let mut put_sorted = stats.put_latencies;
    put_sorted.sort();
    if !put_sorted.is_empty() {
        println!("Put latency (ms):");
        println!(
            "         {:<10}{:<10}{:<10}{}",
            "min", "p50", "p99", "max"
        );
        println!(
            "         {:<10}{:<10}{:<10}{}",
            fmt_ms(put_sorted[0]),
            fmt_ms(percentile(&put_sorted, 50.0)),
            fmt_ms(percentile(&put_sorted, 99.0)),
            fmt_ms(*put_sorted.last().unwrap())
        );
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    assert!(args.schemas > 0, "need at least 1 schema");
    assert!(args.batch_size > 0, "batch_size must be > 0");
    assert!(args.total_tuples >= args.batch_size, "total_tuples must be >= batch_size");
    assert!(args.trigger_pct <= 100, "trigger_pct must be 0-100");

    let channel = Channel::from_shared(args.server.clone())?.connect().await?;
    let mut health = HealthClient::new(channel.clone());
    let mut schemas_client = SchemasClient::new(channel.clone());
    let mut playbooks_client = PlaybooksClient::new(channel.clone());
    let mut agents_client = AgentsClient::new(channel.clone());
    let mut tuples_client = TupleStoreClient::new(channel);

    let mut rng = rand::thread_rng();
    let schema_defs = generate_schema_defs(&mut rng, args.schemas);

    println!("Setting up {} schemas, {} triggers...", args.schemas, args.triggers);
    setup(
        &mut health,
        &mut schemas_client,
        &mut playbooks_client,
        &mut agents_client,
        &args,
        &schema_defs,
    )
    .await?;

    let pool_size = 2 * args.batch_size;
    let pool = generate_pool(&mut rng, &schema_defs, pool_size, args.trigger_pct);
    println!("Pool: {} tuples, running benchmark...", pool_size);
    println!();

    let start = Instant::now();
    let stats = timed_run(&mut tuples_client, &pool, &args).await?;
    let total = start.elapsed();

    print_stats(stats, total, &args);

    Ok(())
}
