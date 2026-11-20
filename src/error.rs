//! Error types.

use std::fmt;

/// Errors produced by the MagicWS runtime.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The peer sent a version handshake this build does not understand.
    #[error("unsupported protocol version: {found} (expected {expected})")]
    UnsupportedVersion {
        /// The version byte received from the peer.
        found: u8,
        /// The version this build implements.
        expected: u8,
    },

    /// The WebSocket upgrade request was malformed or not a WebSocket request.
    ///
    /// Boxed because `tungstenite::Error` is large (136+ bytes) and would
    /// otherwise dominate the size of every `Result` in this crate.
    #[error("websocket upgrade failed: {0}")]
    Upgrade(#[from] Box<tokio_tungstenite::tungstenite::Error>),

    /// An I/O error occurred while reading from or writing to the network.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// The address (or address string) could not be parsed or bound.
    #[error("invalid address: {0}")]
    InvalidAddress(String),

    /// The URL passed to [`crate::Client::connect`] was invalid.
    #[error("invalid url: {0}")]
    InvalidUrl(String),

    /// The requested room does not exist.
    #[error("room {0} does not exist")]
    NoSuchRoom(crate::RoomId),

    /// The server is already running.
    #[error("server is already running")]
    AlreadyRunning,

    /// The server has been shut down.
    #[error("server stopped")]
    Stopped,

    /// A room was already at capacity when a user tried to join it.
    #[error(transparent)]
    RoomFull(#[from] crate::hub::JoinError),
}

impl Error {
    /// Returns an [`Error::InvalidAddress`] for `addr`.
    pub(crate) fn address(addr: impl fmt::Display) -> Self {
        Self::InvalidAddress(addr.to_string())
    }

    /// Wraps a WebSocket error into [`Error::Upgrade`].
    pub(crate) fn upgrade(err: tokio_tungstenite::tungstenite::Error) -> Self {
        Self::Upgrade(Box::new(err))
    }
}

/// Convenience result type used throughout this crate.
pub type Result<T> = std::result::Result<T, Error>;
