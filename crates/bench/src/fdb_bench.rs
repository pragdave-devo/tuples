#![deny(warnings)]

use anyhow::Result;
use clap::Parser;
use foundationdb::tuple::pack;
use std::time::{Duration, Instant};

#[derive(Parser)]
#[command(name = "fdb-bench", about = "FDB raw write benchmark")]
struct Args {
    /// Total tuples to write per repeat
    #[arg(long, default_value_t = 1000)]
    tuples: usize,
    /// Keys per FDB transaction
    #[arg(long, default_value_t = 10)]
    batch_size: usize,
    /// Number of timed runs
    #[arg(long, default_value_t = 5)]
    repeats: usize,
}

fn fmt_ms(d: Duration) -> String {
    format!("{:.2}ms", d.as_secs_f64() * 1000.0)
}

fn percentile(sorted: &[Duration], pct: usize) -> Duration {
    let idx = ((sorted.len() * pct) / 100).min(sorted.len() - 1);
    sorted[idx]
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Boot the FDB network thread.
    let network = unsafe { foundationdb::boot() };

    let db = foundationdb::Database::default()?;

    let fixed_value = {
        let v = serde_json::json!({"id": "bench", "data": "x".repeat(64)});
        serde_json::to_vec(&v)?
    };

    println!(
        "FDB raw write benchmark  tuples={}  batch_size={}  repeats={}\n",
        args.tuples, args.batch_size, args.repeats
    );
    println!("{:<10} {:<10} {:<10} {:<10} {:<10}", "", "total", "puts/sec", "p50", "p99");

    let mut all_totals: Vec<Duration> = Vec::with_capacity(args.repeats);
    let mut all_p50: Vec<Duration> = Vec::with_capacity(args.repeats);
    let mut all_p99: Vec<Duration> = Vec::with_capacity(args.repeats);

    for run in 0..args.repeats {
        let mut commit_latencies: Vec<Duration> = Vec::new();
        let run_start = Instant::now();

        let mut idx: usize = 0;
        while idx < args.tuples {
            let chunk_end = (idx + args.batch_size).min(args.tuples);
            let t0 = Instant::now();
            let trx = db.create_trx()?;
            for i in idx..chunk_end {
                let key = pack(&("bench", i as i64));
                trx.set(&key, &fixed_value);
            }
            trx.commit().await.map_err(|e| anyhow::anyhow!("fdb commit: {e}"))?;
            commit_latencies.push(t0.elapsed());
            idx = chunk_end;
        }

        let total = run_start.elapsed();
        let puts_per_sec = args.tuples as f64 / total.as_secs_f64();

        commit_latencies.sort();
        let p50 = percentile(&commit_latencies, 50);
        let p99 = percentile(&commit_latencies, 99);

        println!(
            "{:<10} {:<10} {:<10} {:<10} {:<10}",
            format!("run {}:", run + 1),
            fmt_ms(total),
            format!("{:.0}", puts_per_sec),
            fmt_ms(p50),
            fmt_ms(p99),
        );

        all_totals.push(total);
        all_p50.push(p50);
        all_p99.push(p99);
    }

    // Aggregate stats.
    let avg_total = all_totals.iter().sum::<Duration>() / args.repeats as u32;
    let avg_puts_per_sec = args.tuples as f64 / avg_total.as_secs_f64();
    let avg_p50 = all_p50.iter().sum::<Duration>() / args.repeats as u32;
    let avg_p99 = all_p99.iter().sum::<Duration>() / args.repeats as u32;

    println!("{}", "\u{2500}".repeat(60));
    println!(
        "{:<10} {:<10} {:<10} {:<10} {:<10}",
        "avg:",
        fmt_ms(avg_total),
        format!("{:.0}", avg_puts_per_sec),
        fmt_ms(avg_p50),
        fmt_ms(avg_p99),
    );

    drop(network);
    Ok(())
}
