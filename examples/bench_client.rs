//! Benchmark load generator.
//!
//! Connects N clients to a running `bench_server`, joins them into rooms via
//! the server's round-robin, and fires the three-event workload for a fixed
//! duration. Reports throughput. It prints its own RSS so the memory spent on
//! the client side is also visible and can be subtracted from the total.
//!
//! ```bash
//! cargo run --release --example bench_client -- --clients 5000 --rooms 50 \
//!     --addr 127.0.0.1:8080 --duration 10
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use magic_ws::{Client, Event};

const PLAYER_MOVE: Event = Event::new("player_move");
const CHAT_MESSAGE: Event = Event::new("chat_message");
const PLAYER_ACTION: Event = Event::new("player_action");

#[derive(Parser, Debug)]
#[command(about, version)]
struct Args {
    /// Number of concurrent clients to open.
    #[arg(long, default_value_t = 5_000)]
    clients: usize,

    /// Server address.
    #[arg(long, default_value = "127.0.0.1:8080")]
    addr: String,

    /// How long the measurement phase runs, in seconds.
    #[arg(long, default_value_t = 10)]
    duration: u64,

    /// Milliseconds between sends per client.
    #[arg(long, default_value_t = 50)]
    tick_ms: u64,

    /// Print the client process RSS at start and end.
    #[arg(long, default_value_t = true)]
    rss: bool,
}

#[cfg(target_os = "linux")]
fn process_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

#[cfg(not(target_os = "linux"))]
fn process_rss_bytes() -> Option<u64> {
    None
}

fn fmt_mib(bytes: u64) -> String {
    format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let url = format!("ws://{}/ws", args.addr);
    let start = Instant::now();

    let mut clients = Vec::with_capacity(args.clients);
    for i in 0..args.clients {
        match Client::connect(&url).await {
            Ok(client) => clients.push(client),
            Err(err) => {
                eprintln!("client {i} failed to connect: {err}");
                break;
            }
        }
    }
    println!(
        "connected {} clients in {:?}",
        clients.len(),
        start.elapsed()
    );

    let rss_after_connect = if args.rss { process_rss_bytes() } else { None };

    let received = Arc::new(AtomicU64::new(0));
    let sent = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    for client in &clients {
        let client = client.clone();
        let received = Arc::clone(&received);
        let sent = Arc::clone(&sent);

        let events = client.events();
        for event in [PLAYER_MOVE, CHAT_MESSAGE, PLAYER_ACTION] {
            let received = Arc::clone(&received);
            events.on(event, move |_payload| {
                received.fetch_add(1, Ordering::Relaxed);
            });
        }

        let tick = Duration::from_millis(args.tick_ms);
        handles.push(tokio::spawn(async move {
            let mut step = 0u64;
            let mut ticker = tokio::time::interval(tick);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                match step % 3 {
                    0 => {
                        client.emit(PLAYER_MOVE, b"pos_x:123.45,pos_y:678.90");
                    }
                    1 => {
                        client.emit(CHAT_MESSAGE, b"Hello world from client!");
                    }
                    _ => {
                        client.emit(PLAYER_ACTION, b"attack_skill_id:99");
                    }
                }
                step += 1;
                sent.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    let duration = Duration::from_secs(args.duration);
    tokio::time::sleep(duration).await;

    let total_sent = sent.load(Ordering::Relaxed);
    let total_received = received.load(Ordering::Relaxed);

    println!("\n=== client-side results ===");
    println!("duration:         {}s", args.duration);
    println!("total sent:       {total_sent} packets");
    println!("client received:  {total_received} packets");
    println!(
        "throughput:       {:.2} msg/sec",
        total_received as f64 / args.duration as f64
    );
    if let Some(bytes) = rss_after_connect {
        println!(
            "client_rss:       {} (this process, incl. all clients)",
            fmt_mib(bytes)
        );
    }

    for handle in handles {
        handle.abort();
    }
    for client in &clients {
        client.close().await;
    }

    Ok(())
}
