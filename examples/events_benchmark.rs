//! Stress test: many clients, many rooms, one event fan-out.
//!
//! ```bash
//! cargo run --release --example events_benchmark -- --clients 5000 --rooms 50
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use magic_ws::{Client, Event, Server};

const PLAYER_MOVE: Event = Event::new("player_move");
const CHAT_MESSAGE: Event = Event::new("chat_message");
const PLAYER_ACTION: Event = Event::new("player_action");

/// MagicWS multi-event stress test.
#[derive(Parser, Debug)]
#[command(about, version)]
struct Args {
    /// Number of concurrent clients.
    #[arg(long, default_value_t = 5_000)]
    clients: usize,

    /// Number of rooms to spread the clients over.
    #[arg(long, default_value_t = 50)]
    rooms: u32,

    /// How long the measurement phase runs, in seconds.
    #[arg(long, default_value_t = 10)]
    duration: u64,

    /// Milliseconds between sends per client.
    #[arg(long, default_value_t = 50)]
    tick_ms: u64,

    /// Address the server binds to.
    #[arg(long, default_value = "127.0.0.1:8080")]
    addr: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let room_counter = Arc::new(AtomicU64::new(0));
    let rooms = args.rooms;

    let server = Arc::new(
        Server::builder()
            .max_connections(args.clients)
            .on_connect(move |user| {
                // Round-robin each client into a room.
                let next = room_counter.fetch_add(1, Ordering::Relaxed) % u64::from(rooms) + 1;
                user.set_room(next as u32);
            })
            .build(),
    );

    // Mirrors the Go `events_benchmark` example so the two are comparable:
    //   player_move  -> fan out to the sender's room
    //   chat_message -> fan out to the sender's room
    //   player_action-> echo back to the sender only
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

    tokio::time::sleep(Duration::from_millis(200)).await;
    println!(
        "=== starting stress test: {} clients, {} rooms ===",
        args.clients, args.rooms
    );

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

    println!("\n=== stress test results ===");
    println!("duration:         {}s", args.duration);
    println!("total sent:       {total_sent} packets");
    println!("client received:  {total_received} packets");
    println!(
        "throughput:       {:.2} msg/sec",
        total_received as f64 / args.duration as f64
    );

    for handle in handles {
        handle.abort();
    }
    for client in &clients {
        client.close().await;
    }
    server.shutdown();
    server_task.abort();

    Ok(())
}
