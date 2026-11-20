//! Standalone benchmark server.
//!
//! Runs a MagicWS server with the same three-event workload as
//! `events_benchmark`, and prints its own resident memory (RSS) every second
//! so the server footprint can be measured independently of the clients.
//!
//! Run the load generator against it:
//!
//! ```bash
//! # terminal 1: the server
//! cargo run --release --example bench_server -- --addr 127.0.0.1:8080
//!
//! # terminal 2: the load
//! cargo run --release --example bench_client -- --clients 5000 --rooms 50
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use magic_ws::{Event, Server};

const PLAYER_MOVE: Event = Event::new("player_move");
const CHAT_MESSAGE: Event = Event::new("chat_message");
const PLAYER_ACTION: Event = Event::new("player_action");

#[derive(Parser, Debug)]
#[command(about, version)]
struct Args {
    /// Address to bind the server to.
    #[arg(long, default_value = "127.0.0.1:8080")]
    addr: String,

    /// Number of rooms clients are spread over (clients self-assign).
    #[arg(long, default_value_t = 50)]
    rooms: u32,

    /// How often to print the server's RSS, in seconds.
    #[arg(long, default_value_t = 1)]
    rss_interval_secs: u64,

    /// Print the RSS line only (no other output), useful for piping.
    #[arg(long, default_value_t = false)]
    quiet: bool,
}

/// Reads the current process RSS (resident set size) in bytes on Linux.
#[cfg(target_os = "linux")]
fn process_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            // VmRSS is reported in KiB.
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

    let room_counter = Arc::new(AtomicU64::new(0));
    let rooms = args.rooms;

    let server = Arc::new(
        Server::builder()
            // Accept as many connections as the load generator opens.
            .max_connections(0)
            .on_connect(move |user| {
                let next = room_counter.fetch_add(1, Ordering::Relaxed) % u64::from(rooms) + 1;
                user.set_room(next as u32);
            })
            .build(),
    );

    // Same workload as events_benchmark:
    //   player_move   -> fan out to the sender's room
    //   chat_message  -> fan out to the sender's room
    //   player_action -> echo back to the sender only
    let move_server = Arc::clone(&server);
    server.on_event(PLAYER_MOVE, move |user, payload| {
        if let Some(room) = user.room() {
            move_server.emit_to_room(room, PLAYER_MOVE, payload);
        }
    });
    let chat_server = Arc::clone(&server);
    server.on_event(CHAT_MESSAGE, move |user, payload| {
        if let Some(room) = user.room() {
            chat_server.emit_to_room(room, CHAT_MESSAGE, payload);
        }
    });
    server.on_event(PLAYER_ACTION, |user, payload| {
        user.send_event(PLAYER_ACTION, payload);
    });

    let addr = args.addr.clone();
    let server_task = {
        let server = Arc::clone(&server);
        tokio::spawn(async move { server.listen(addr).await })
    };

    if !args.quiet {
        println!(
            "bench_server listening on {} ({} rooms), printing RSS every {}s",
            args.addr, args.rooms, args.rss_interval_secs
        );
    }

    // RSS monitor: print the server process's own footprint every interval.
    let rss_interval_secs = args.rss_interval_secs.max(1);
    let monitor_server = Arc::clone(&server);
    let monitor = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(rss_interval_secs));
        ticker.tick().await; // consume the immediate first tick
        loop {
            ticker.tick().await;
            match process_rss_bytes() {
                Some(bytes) => {
                    println!(
                        "rss_server={} users={}",
                        fmt_mib(bytes),
                        monitor_server.user_count()
                    );
                }
                None => {
                    println!("rss_server=unavailable");
                }
            }
        }
    });

    // Wait for shutdown (Ctrl-C).
    tokio::signal::ctrl_c().await?;
    if !args.quiet {
        println!("shutting down");
    }
    server.shutdown();
    server_task.abort();
    monitor.abort();

    Ok(())
}
