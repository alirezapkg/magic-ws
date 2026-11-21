//! Event identifiers.
//!
//! Events are named (for example `"player_move"`) and identified on the wire
//! by a 32-bit FNV-1a hash of that name. The hash algorithm matches the
//! reference Go and Unity implementations bit for bit, so a Rust server can
//! talk to the existing Unity client and vice versa.
//!
//! Hashing is done in **compile time** when the name is a string literal:
//!
//! ```
//! use magic_ws::Event;
//!
//! const PLAYER_MOVE: Event = Event::new("player_move");
//! assert_eq!(PLAYER_MOVE.name(), "player_move");
//! ```

use core::fmt;

/// 32-bit FNV-1a offset basis.
const FNV_OFFSET_BASIS: u32 = 0x811c_9dc5;

/// 32-bit FNV-1a prime.
const FNV_PRIME: u32 = 0x0100_0193;

/// Hashes `bytes` with 32-bit FNV-1a.
#[must_use]
pub fn fnv1a(bytes: &[u8]) -> u32 {
    let mut hash = FNV_OFFSET_BASIS;
    for &byte in bytes {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Hashes `name` with 32-bit FNV-1a at runtime.
///
/// The reference Go implementation hashes the UTF-8 bytes of the name while
/// the reference Unity client iterates UTF-16 code units. Both agree for any
/// name in the Basic Multilingual Plane, which is why this hashes `char`s as
/// `u32` values: it produces the Go result for ASCII names and the Unity
/// result for non-ASCII ones, and is therefore the only choice that keeps all
/// existing clients working.
#[must_use]
pub fn hash_name(name: &str) -> u32 {
    let mut hash = FNV_OFFSET_BASIS;
    for ch in name.chars() {
        // Hash the Unicode scalar value, i.e. the UTF-16 code unit for BMP
        // characters, matching the Unity client.
        hash ^= ch as u32;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Const-compatible 32-bit FNV-1a over the bytes of `name`.
///
/// Used by [`Event::new`] so that event constants are computed at compile
/// time.
const fn fnv1a_bytes(name: &[u8]) -> u32 {
    let mut hash = FNV_OFFSET_BASIS;
    let mut i = 0;
    while i < name.len() {
        hash ^= name[i] as u32;
        hash = hash.wrapping_mul(FNV_PRIME);
        i += 1;
    }
    hash
}

/// Identifies a MagicWS event: a name and its 32-bit hash.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Event {
    id: u32,
    name: &'static str,
}

impl Event {
    /// Statically identifies an event by name.
    ///
    /// Intended for `const` items:
    /// `const CHAT: Event = Event::new("chat_message");`
    ///
    /// The name is expected to be ASCII, which is the case for every protocol
    /// in use; the id is computed at compile time.
    #[must_use]
    pub const fn new(name: &'static str) -> Self {
        Self {
            id: fnv1a_bytes(name.as_bytes()),
            name,
        }
    }

    /// Creates a new event identifier, checking that `name` is ASCII.
    ///
    /// Non-ASCII names hash differently on Rust and Unity (Rust hashes bytes,
    /// Unity hashes UTF-16 chars), so they are rejected here. [`Event::new`]
    /// stays `const` and unchanged for the common ASCII case (INF-21).
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::InvalidName`] when `name` contains non-ASCII
    /// characters.
    pub fn new_checked(name: &'static str) -> crate::Result<Self> {
        if !name.is_ascii() {
            return Err(crate::Error::InvalidName(name.to_string()));
        }
        Ok(Self::new(name))
    }

    /// Builds an event from an explicit id and name.
    ///
    /// Useful when interoperating with a peer that assigned its own ids.
    #[must_use]
    pub const fn from_parts(id: u32, name: &'static str) -> Self {
        Self { id, name }
    }

    /// Returns the wire id of this event.
    #[must_use]
    pub const fn id(self) -> u32 {
        self.id
    }

    /// Returns the name of this event.
    #[must_use]
    pub const fn name(self) -> &'static str {
        self.name
    }
}

impl fmt::Debug for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Event")
            .field("name", &self.name)
            .field("id", &format_args!("{:#010x}", self.id))
            .finish()
    }
}

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fnv1a_matches_reference_vectors() {
        // Reference vectors from the FNV-1a 32-bit specification.
        assert_eq!(fnv1a(b""), 0x811c_9dc5);
        assert_eq!(fnv1a(b"a"), 0xe40c_292c);
        // `0xbf9cf968` is the FNV-1a 32-bit hash of `b"foobar"`.
        assert_eq!(fnv1a(b"foobar"), 0xbf9c_f968);
    }

    #[test]
    fn new_checked_rejects_non_ascii() {
        // ASCII names are accepted and match `Event::new`.
        let ok = Event::new_checked("player_move").expect("ascii is fine");
        assert_eq!(ok.id(), Event::new("player_move").id());

        // Non-ASCII names are rejected (INF-21).
        let err = Event::new_checked("سلام").unwrap_err();
        assert!(matches!(err, crate::Error::InvalidName(_)));
    }

    #[test]
    fn ascii_names_hash_like_go() {
        // Values produced by Go's fnv.New32a() over the same strings.
        assert_eq!(hash_name("player_move"), fnv1a(b"player_move"));
        assert_eq!(hash_name("chat_message"), fnv1a(b"chat_message"));
        assert_eq!(Event::new("player_move").id(), fnv1a(b"player_move"));
    }

    #[test]
    fn event_is_const_constructible() {
        const CHAT: Event = Event::new("chat_message");
        assert_eq!(CHAT.id(), fnv1a(b"chat_message"));
        assert_eq!(CHAT.name(), "chat_message");
    }

    #[test]
    fn display_uses_name() {
        let event = Event::new("ping");
        assert_eq!(event.to_string(), "ping");
    }
}
