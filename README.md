# MagicWS

Lightweight binary WebSocket framework for real-time games, in Rust.

MagicWS keeps the wire format deliberately tiny: a 32-bit event id followed by
an opaque payload. No JSON, no base64, no per-message type tags.

```text
+----------------------+-------------------+
| event id (u32, BE)   | payload (n bytes) |
+----------------------+-------------------+
```

- **Zero-copy dispatch** — event ids are the FNV-1a hash of a name, computed at
  compile time in a `const`.
- **Room primitives** built in — join, leave, fan-out, broadcast.
- **Non-blocking sends** — a full queue drops the packet instead of stalling the
  network task, which is what a game server wants.
- **Plain TCP or TLS** — the server works on any `AsyncRead + AsyncWrite`, and
  the client speaks `ws://` and `wss://`.
- **Wire-compatible** with the existing Unity client and the original Go
  server. Same handshake, same framing, same event hashes.

## Install

```bash
cargo add magic-ws
```

Or add it by hand:

```toml
[dependencies]
magic-ws = "1.1"
tokio = { version = "1", features = ["full"] }
```

Requires a Rust toolchain of **1.80** or newer.

### Feature flags

| Feature | Default | What it does |
|---|---|---|
| `tls` | yes | `wss://` client support (rustls + webpki roots). Disable for a smaller build: `default-features = false` |

## Quick start

### Server

```rust
use magic_ws::{Event, Server};

const CHAT: Event = Event::new("chat_message");

#[tokio::main]
async fn main() -> magic_ws::Result<()> {
    let server = Server::builder()
        .on_connect(|user| println!("{} connected", user.id()))
        .on_disconnect(|user| println!("{} left", user.id()))
        .on_event(CHAT, |user, payload| {
            // Echo back to the sender.
            user.send_event(CHAT, payload);
        })
        .build();

    server.listen("127.0.0.1:8080").await
}
```

### Client

```rust
use magic_ws::{Client, Event};

const CHAT: Event = Event::new("chat_message");

#[tokio::main]
async fn main() -> magic_ws::Result<()> {
    let client = Client::connect("ws://127.0.0.1:8080/ws").await?;

    client.events().on(CHAT, |payload| {
        println!("received: {}", String::from_utf8_lossy(payload));
    });

    client.emit(CHAT, b"hello server");
    Ok(())
}
```

Both sides open the connection, and MagicWS sends the one-byte protocol version
for you. There is no extra handshake step.

## Usage

### Events

Events are `const` values, so the id is computed at compile time and a dispatch
is a hash-map lookup:

```rust
use magic_ws::Event;

const PLAYER_MOVE: Event = Event::new("player_move");
const CHAT_MESSAGE: Event = Event::new("chat_message");

assert_eq!(PLAYER_MOVE.id(), 0x4c7f_d63c);
assert_eq!(CHAT_MESSAGE.id(), 0x7805_ec3b);
```

The name is what makes the id stable across languages, so keep event names in
sync with the client. Registering a handler twice for the same event replaces
the first one.

### Rooms

Every user belongs to at most one room. `0` is not a valid room id.

```rust
use magic_ws::Server;

// Move a user (also keeps the room index in sync).
server.set_user_room(&user, 42);
server.set_user_room(&user, 0);          // leave the room

// Fan out an event to the whole room, sender included.
let delivered = server.emit_to_room(42, CHAT, b"payload");

// ...or to everyone except one user.
let delivered = server.to_room(42).emit_except(CHAT, &user.id(), b"payload");
```

`emit_to_room` and `emit_except` return how many users the message was queued
for — useful for metrics.

### Talking to the server from inside a handler

Handlers receive a `&User` but not the server. Two patterns work well:

**Register after building**, since `Server` is a cheap `Arc` handle:

```rust
use std::sync::Arc;
use magic_ws::{Event, Server};

let server = Arc::new(Server::builder().build());

let fanout = Arc::clone(&server);
server.on_event(CHAT, move |user, payload| {
    if let Some(room) = user.room() {
        fanout.emit_to_room(room, CHAT, payload);
    }
});
```

**Or use `User` directly**, which is all you need for a reply:

```rust
server.on_event(CHAT, |user, payload| {
    user.send_event(CHAT, payload);   // echo
});
```

### Connection lifecycle

```rust
use magic_ws::{Event, Server, UserState};

let server = Server::builder()
    .on_connect(|user| {
        assert_eq!(user.state(), UserState::Connected);
        user.set_room(7);              // assign a room immediately
    })
    .max_connections(10_000)           // reject beyond this many
    .max_queue(512)                    // per-connection outbound queue
    .build();

// Graceful shutdown: stops accepting and closes every connection.
server.shutdown();

// Or await it from a signal handler.
tokio::select! {
    _ = tokio::signal::ctrl_c() => server.shutdown(),
}
```

`on_connect` runs *after* the user is registered, so `set_room` there is safe
and immediately visible to room fan-out.

### Sending without a room

```rust
// Encoded message straight to one user.
server.emit(&user, CHAT, b"payload");

// Already-encoded bytes.
server.send_to(&user, message_bytes);

// Everyone.
let delivered = server.broadcast(&message_bytes);
```

All sends are **non-blocking**: if the per-connection queue is full the message
is dropped and `false` is returned. Nothing ever waits on a slow client.

### TLS

`wss://` works out of the box on the client:

```rust
let client = Client::connect("wss://example.com/ws").await?;
```

To terminate TLS on the server, accept the stream yourself and hand it to
MagicWS:

```rust
use tokio_rustls::TlsAcceptor;

let acceptor = TlsAcceptor::from(config);

loop {
    let (tcp, addr) = listener.accept().await?;
    let tls = acceptor.accept(tcp).await?;

    let server = server.clone();
    tokio::spawn(async move {
        server.accept_stream(tls, addr).await
    });
}
```

For a private CA or a self-signed certificate, build a trust store and use
`Client::connect_with_tls_config`:

```rust
use std::sync::Arc;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};

let mut roots = RootCertStore::empty();
roots.add(my_cert_der)?;

let config = ClientConfig::builder()
    .with_root_certificates(roots)
    .with_no_client_auth();

let client = Client::connect_with_tls_config(
    "wss://127.0.0.1:8443/ws",
    std::time::Duration::from_secs(5),
    Arc::new(config),
).await?;
```

### Custom transports

`Server::accept_stream` accepts any `AsyncRead + AsyncWrite`, so the same
connection loop can serve plain TCP, TLS, or a proxied stream:

```rust
pub async fn accept_stream<S>(&self, stream: S, addr: SocketAddr) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
```

`Client::start` does the same on the client side for an already-upgraded
socket.

## Protocol

### Handshake

1. The client opens a WebSocket connection.
2. The client sends **one binary frame containing a single byte**: `0x01`.
3. The server replies with a close frame (`1002 protocol error`) if that byte is
   not `0x01`; otherwise the connection is live.

MagicWS does this for you on both sides. Raw clients must do it themselves.

### Messages

Every subsequent message is:

```text
offset  size  field
0       4     event id, unsigned 32-bit, big-endian
4       n     payload, opaque bytes
```

A payload may be empty. Messages shorter than 4 bytes are dropped.

### Event ids

Event ids are the **32-bit FNV-1a** hash of the event name:

```text
hash = 0x811c9dc5
for each byte b of name:
    hash = hash XOR b
    hash = hash * 0x01000193   (wrapping)
```

Names are ASCII in practice, so the Rust, Go and Unity implementations all
agree. `Event::new` computes this in a `const` context:

```rust
const CHAT: Event = Event::new("chat_message");
```

To interoperate with a peer that assigned its own ids, build the event from
parts:

```rust
let custom = Event::from_parts(0xdead_beef, "my_event");
```

### Compatibility

| | Rust | Go | Unity |
|---|---|---|---|
| Version byte | `0x01` | `0x01` | `0x01` |
| Frame | `[u32 BE][payload]` | same | same |
| Hash | FNV-1a 32 | same | same |

## Performance

Measured on a 4-core machine, 5000 clients spread over 50 rooms, 10 second
run, 50 ms tick — identical workload to the Go benchmark (three events;
two broadcast to the room, one echoed to the sender):

| | MagicWS (Rust) | Original (Go) |
|---|---|---|
| Throughput | **~1.1–1.2 M msg/s** | ~0.6–0.8 M msg/s |
| Peak RSS | **~315 MiB** | ~390 MiB |
| CPU | **~2.5 cores** | ~2.9 cores |
| GC pauses | none | 2 per run |

Memory is flat per connection: roughly **14 KiB** of server-side state per
connection, and the process stays at ~1.3 MiB when idle. 10 000 idle
connections sit at ~140 MiB.

The main win comes from batching writes: `SinkExt::send` flushes after every
message, which costs one syscall per packet. MagicWS drains the outbound queue
and flushes once, which took the same workload from 236 k msg/s to 1.16 M
msg/s.

Run it yourself:

```bash
cargo run --release --example events_benchmark -- --clients 5000 --rooms 50 --duration 10
```

## Examples

| Example | What it shows |
|---|---|
| `test_server` | echo + chat server, with a room |
| `basic` | one-shot client that waits for a reply |
| `tls_test` | `wss://` round-trip with a self-signed certificate |
| `events_benchmark` | multi-client stress test with flags |

```bash
cargo run --example test_server
cargo run --example basic
cargo run --example tls_test
```

The certificate in `examples/certs/` is a throwaway self-signed localhost
certificate that exists only so the TLS example runs. Do not use it anywhere.

## Testing

```bash
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check
```

The suite covers protocol framing, FNV-1a vectors, room bookkeeping, the
registry under concurrency, and six end-to-end tests over real TCP sockets
(handshake, echo, rooms, rejection of a bad version byte, shutdown).

## License

MIT — see [LICENSE](LICENSE).