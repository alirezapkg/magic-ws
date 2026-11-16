//! Minimal client that connects, sends one event and waits for the reply.
//!
//! Run a server first (`cargo run --example test_server`), then:
//!
//! ```bash
//! cargo run --example basic
//! ```

use std::sync::{Arc, Mutex};

use magic_ws::{Client, Event};

const ECHO: Event = Event::new("echo");

#[tokio::main]
async fn main() -> magic_ws::Result<()> {
    tracing_subscriber::fmt::init();

    let client = Client::connect("ws://127.0.0.1:8080/ws").await?;
    println!("connected to ws://127.0.0.1:8080/ws");

    // `oneshot::Sender::send` consumes the sender, so guard it in a `Mutex`.
    let (tx, rx) = tokio::sync::oneshot::channel();
    let tx = Arc::new(Mutex::new(Some(tx)));

    let events = client.events();
    let reply = Arc::clone(&tx);
    events.on(ECHO, move |payload| {
        if let Some(tx) = reply.lock().unwrap().take() {
            let _ = tx.send(payload.to_vec());
        }
    });

    client.emit(ECHO, b"hello from rust");
    println!("sent: hello from rust");

    match tokio::time::timeout(std::time::Duration::from_secs(5), rx).await {
        Ok(Ok(reply)) => println!("reply: {}", String::from_utf8_lossy(&reply)),
        _ => println!("no reply within 5s"),
    }

    client.close().await;
    Ok(())
}
