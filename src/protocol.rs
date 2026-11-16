//! Wire protocol constants and helpers.
//!
//! MagicWS frames every message as:
//!
//! ```text
//! +----------------------+-------------------+
//! | event id (u32, BE)   | payload (n bytes) |
//! +----------------------+-------------------+
//! ```
//!
//! Right after the WebSocket upgrade succeeds the client sends a single
//! binary frame containing one byte: the protocol version. The server drops
//! the connection when that byte does not match [`PROTOCOL_VERSION`].

/// Protocol version understood by this implementation.
pub const PROTOCOL_VERSION: u8 = 1;

/// Size of the event-id prefix in bytes.
pub const HEADER_LEN: usize = 4;

/// Default capacity of the per-connection read buffer.
pub const READ_BUF_SIZE: usize = 4096;

/// Default capacity of the per-connection write buffer.
pub const WRITE_BUF_SIZE: usize = 4096;

/// Default number of messages a send channel can hold before dropping.
pub const SEND_QUEUE: usize = 256;

/// Convenience alias for a MagicWS event id.
pub type EventId = u32;

/// Prefixes `payload` with its `event_id` and returns the encoded message.
///
/// The returned buffer has exactly `4 + payload.len()` bytes of capacity.
#[must_use]
pub fn pack_message(event_id: EventId, payload: &[u8]) -> Vec<u8> {
    let mut msg = Vec::with_capacity(HEADER_LEN + payload.len());
    msg.extend_from_slice(&event_id.to_be_bytes());
    msg.extend_from_slice(payload);
    msg
}

/// Splits an encoded message into `(event_id, payload)`.
///
/// Returns `None` when `msg` is shorter than the event-id prefix.
#[must_use]
pub fn unpack_message(msg: &[u8]) -> Option<(EventId, &[u8])> {
    let (head, payload) = msg.split_at_checked(HEADER_LEN)?;
    Some((EventId::from_be_bytes(head.try_into().ok()?), payload))
}

/// Byte slice holding the encoded protocol version, as sent on connect.
#[must_use]
pub const fn version_packet() -> [u8; 1] {
    [PROTOCOL_VERSION]
}

/// Reports whether `data` is a valid MagicWS version handshake.
#[must_use]
pub fn is_version_packet(data: &[u8]) -> bool {
    matches!(data, [PROTOCOL_VERSION])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_unpack_roundtrip() {
        let msg = pack_message(0xDEAD_BEEF, b"hello");
        assert_eq!(msg.len(), HEADER_LEN + 5);
        let (id, payload) = unpack_message(&msg).expect("valid frame");
        assert_eq!(id, 0xDEAD_BEEF);
        assert_eq!(payload, b"hello");
    }

    #[test]
    fn empty_payload_roundtrip() {
        let msg = pack_message(7, b"");
        assert_eq!(msg.len(), HEADER_LEN);
        assert_eq!(unpack_message(&msg), Some((7, &[][..])));
    }

    #[test]
    fn short_message_is_rejected() {
        assert_eq!(unpack_message(&[0, 0, 0]), None);
        assert_eq!(unpack_message(&[]), None);
    }

    #[test]
    fn version_handshake() {
        assert!(is_version_packet(&version_packet()));
        assert!(!is_version_packet(&[2]));
        assert!(!is_version_packet(&[]));
        assert!(!is_version_packet(&[1, 1]));
    }
}
