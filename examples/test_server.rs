//! Minimal echo server.
//!
//! Run with:
//!
//! ```bash
//! cargo run --example test_server
//! ```

use std::sync::Arc;

use magic_ws::{Event, Server};

const ECHO: Event = Event::new("echo");
const CHAT: Event = Event::new("chat_message");

#[tokio::main]
async fn main() -> magic_ws::Result<()> {
    tracing_subscriber::fmt::init();

    let server = Arc::new(Server::builder().build());

    // Room assignment: put every client into room 101.
    let connect_server = Arc::clone(&server);
    let server = Arc::new(
        Server::builder()
            .on_connect(move |user| {
                println!("[+] connected: {}", user.id());
                connect_server.set_user_room(user, 101);
            })
            .on_event(ECHO, |user, payload| {
                println!(
                    "[echo] {} -> {:?}",
                    user.id(),
                    String::from_utf8_lossy(payload)
                );
                user.send_event(ECHO, payload);
            })
            .on_disconnect(|user| println!("[-] disconnected: {}", user.id()))
            .build(),
    );

    // The chat handler needs the server handle, so register it after building.
    let chat_server = Arc::clone(&server);
    server.on_event(CHAT, move |user, payload| {
        println!(
            "[chat] {} said {:?}",
            user.id(),
            String::from_utf8_lossy(payload)
        );
        if let Some(room) = user.room() {
            let sent = chat_server.emit_to_room(room, CHAT, payload);
            println!("[chat] fanned out to {sent} users");
        }
    });

    println!("magic-ws server listening on 127.0.0.1:8080");
    server.listen("127.0.0.1:8080").await
}
