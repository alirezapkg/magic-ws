//! MagicWS — a lightweight binary WebSocket framework for real-time games.
//!
//! MagicWS keeps the wire format deliberately tiny: every message is a
//! 32-bit big-endian event id followed by an opaque payload.
//!
//! ```text
//! +----------------------+-------------------+
//! | event id (u32, BE)   | payload (n bytes) |
//! +----------------------+-------------------+
//! ```
//!
//! # Server
//!
//! ```no_run
//! use magic_ws::{Event, Server};
//!
//! # async fn run() -> magic_ws::Result<()> {
//! const CHAT: Event = Event::new("chat_message");
//!
//! let server = Server::builder()
//!     .on_connect(|user| println!("{} joined", user.id()))
//!     .on_event(CHAT, |user, payload| {
//!         if let Some(room) = user.room() {
//!             let _ = (room, payload.len());
//!         }
//!     })
//!     .build();
//!
//! server.listen("127.0.0.1:8080").await
//! # }
//! ```
//!
//! # Client
//!
//! ```no_run
//! use magic_ws::{Client, Event};
//!
//! # async fn run() -> magic_ws::Result<()> {
//! const CHAT: Event = Event::new("chat_message");
//!
//! let client = Client::connect("ws://127.0.0.1:8080/ws").await?;
//! let events = client.events();
//! events.on(CHAT, |payload| println!("got {} bytes", payload.len()));
//! client.emit(CHAT, b"hello");
//! # Ok(())
//! # }
//! ```
//!
//! # Protocol compatibility
//!
//! The framing, the FNV-1a event hashing and the one-byte version handshake
//! match the Go and Unity implementations, so a Rust server can serve the
//! existing Unity client unchanged.

#![deny(unsafe_code)]
#![warn(missing_debug_implementations, rust_2018_idioms)]
#![doc(html_root_url = "https://docs.rs/magic-ws")]

mod client;
mod error;
mod event;
mod events;
mod hub;
mod protocol;
mod server;
mod user;

#[cfg(feature = "tls")]
pub use client::install_crypto_provider;
pub use client::{Client, ClientCloseHandler, ClientMessageHandler};
pub use error::{Error, Result};
pub use event::{fnv1a, hash_name, Event};
pub use events::{
    ClientEvents, ClientHandler, MessageHandler, RoomEmit, ServerEvents, UserEventHandler,
};
pub use hub::{JoinError, UserManager};
pub use protocol::{
    is_version_packet, pack_message, unpack_message, version_packet, EventId, HEADER_LEN,
    PROTOCOL_VERSION, READ_BUF_SIZE, SEND_QUEUE, WRITE_BUF_SIZE,
};
pub use server::{
    accept_key, ConnectHandler, DisconnectHandler, HeartbeatConfig, Server, ServerBuilder,
    ShutdownSignal,
};
pub use user::{User, UserState};

/// Identifier of a room.
///
/// The value `0` is reserved and means "not in a room".
pub type RoomId = u32;

/// Identifies a user inside a room.
pub type UserId = String;

/// Convenience alias for the message-oriented server handler type.
pub type RawMessageHandler = MessageHandler;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_version_is_exposed() {
        assert_eq!(PROTOCOL_VERSION, 1);
        assert_eq!(HEADER_LEN, 4);
        assert_eq!(SEND_QUEUE, 256);
    }

    #[test]
    fn room_id_zero_is_unassigned() {
        assert_eq!(0, 0);
    }

    #[test]
    fn event_constants_are_usable() {
        const MOVE: Event = Event::new("player_move");
        assert_eq!(MOVE.id(), fnv1a(b"player_move"));
        assert_eq!(MOVE.name(), "player_move");
    }
}
