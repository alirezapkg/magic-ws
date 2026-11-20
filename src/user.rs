//! Users and rooms.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU8, Ordering};
use std::sync::{Arc, OnceLock, Weak};

use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

use crate::hub::HubInner;
use crate::protocol::SEND_QUEUE;
use crate::RoomId;

/// Lifecycle state of a connected user.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum UserState {
    /// The connection is open but the MagicWS version handshake has not been
    /// accepted yet.
    Pending = 0,
    /// The handshake succeeded and the user is fully connected.
    Connected = 1,
    /// The connection is gone.
    Disconnected = 2,
}

impl UserState {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Connected,
            2 => Self::Disconnected,
            _ => Self::Pending,
        }
    }
}

/// A connected client.
///
/// Handles are cheap to clone (`Arc` inside) and are the unit of addressing
/// for every server operation: sending messages, moving between rooms or
/// disconnecting.
///
/// Sending is non-blocking: when the outbound queue is full the message is
/// dropped, mirroring the "drop packet on full channel" behaviour of the Go
/// implementation. This keeps a slow consumer from ever stalling the
/// networking tasks.
#[derive(Debug)]
pub struct User {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    id: String,
    player_id: OnceLock<String>,
    addr: std::net::SocketAddr,
    state: AtomicU8,
    room: AtomicU32,
    closed: AtomicBool,
    tx: Mutex<Option<mpsc::Sender<Vec<u8>>>>,
    /// Registry that owns this user, so [`User::set_room`] can keep the room
    /// index in sync. `Weak` because the registry also holds the user.
    hub: Mutex<Option<Weak<HubInner>>>,
}

impl User {
    pub(crate) fn new(id: String, addr: std::net::SocketAddr, tx: mpsc::Sender<Vec<u8>>) -> Self {
        Self {
            inner: Arc::new(Inner {
                id,
                player_id: OnceLock::new(),
                addr,
                state: AtomicU8::new(UserState::Pending as u8),
                room: AtomicU32::new(0),
                closed: AtomicBool::new(false),
                tx: Mutex::new(Some(tx)),
                hub: Mutex::new(None),
            }),
        }
    }

    /// Attaches this user to `hub` so room changes keep the index in sync.
    pub(crate) fn attach_hub(&self, hub: &Arc<HubInner>) {
        *self.inner.hub.lock() = Some(Arc::downgrade(hub));
    }

    /// Returns the unique id of this user, derived from its remote address.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.inner.id
    }

    /// Sets a stable player id for this connection.
    ///
    /// Returns `true` when the id was stored, `false` when one was already
    /// set. The id is application-assigned (for example a session token or a
    /// database id) and stays constant across reconnects, unlike
    /// [`User::id`] which is derived from the socket address.
    pub fn set_player_id(&self, id: &str) -> bool {
        if self.inner.player_id.set(id.to_string()).is_err() {
            return false;
        }
        // If this user is already registered, expose the new id through the
        // registry's secondary index.
        let hub = self.inner.hub.lock().as_ref().and_then(Weak::upgrade);
        if let Some(hub) = hub {
            hub.index_player_id(self);
        }
        true
    }

    /// Returns the stable player id, if one was set.
    #[must_use]
    pub fn player_id(&self) -> Option<&str> {
        self.inner.player_id.get().map(String::as_str)
    }

    /// Returns the remote address of this user.
    #[must_use]
    pub fn addr(&self) -> std::net::SocketAddr {
        self.inner.addr
    }

    /// Returns the current lifecycle state.
    #[must_use]
    pub fn state(&self) -> UserState {
        UserState::from_u8(self.inner.state.load(Ordering::Acquire))
    }

    pub(crate) fn set_state(&self, state: UserState) {
        self.inner.state.store(state as u8, Ordering::Release);
    }

    /// Returns the room this user currently belongs to, or `None`.
    #[must_use]
    pub fn room(&self) -> Option<RoomId> {
        let id = self.inner.room.load(Ordering::Acquire);
        (id != 0).then_some(id)
    }

    /// Moves this user into `room`, leaving any previous room.
    ///
    /// Passing `0` (or `None`) removes the user from its room. When the user
    /// belongs to a [`crate::UserManager`] — which is the case for every user
    /// created by a [`crate::Server`] — the room index is updated too, so
    /// room fan-out works immediately.
    pub fn set_room(&self, room: impl Into<Option<RoomId>>) {
        let room = room.into();

        let hub = self.inner.hub.lock().as_ref().and_then(Weak::upgrade);
        if let Some(hub) = hub {
            hub.set_room(self, room);
        } else {
            // Standalone user (tests, custom transports): just record it.
            self.store_room(room);
        }
    }

    /// Stores the room id without touching the registry index.
    pub(crate) fn store_room(&self, room: Option<RoomId>) {
        self.inner.room.store(room.unwrap_or(0), Ordering::Release);
    }

    /// Returns `true` when the connection has been torn down.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    /// Queues `data` for delivery.
    ///
    /// Returns `true` when the message was queued. Returns `false` when the
    /// connection is gone or its queue is full, in which case the message is
    /// silently dropped.
    pub fn send(&self, data: Vec<u8>) -> bool {
        if self.is_closed() || self.state() != UserState::Connected {
            return false;
        }
        let guard = self.inner.tx.lock();
        let Some(tx) = guard.as_ref() else {
            return false;
        };
        match tx.try_send(data) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) => {
                tracing::trace!(user = %self.inner.id, "send queue full, dropping packet");
                false
            }
            Err(TrySendError::Closed(_)) => false,
        }
    }

    /// Queues an already encoded MagicWS message for this event.
    pub fn send_event(&self, event: crate::Event, payload: &[u8]) -> bool {
        self.send(crate::pack_message(event.id(), payload))
    }

    /// Returns the outbound queue capacity.
    #[must_use]
    pub fn queue_capacity(&self) -> usize {
        self.max_capacity()
    }

    /// Returns the outbound queue capacity.
    pub(crate) fn max_capacity(&self) -> usize {
        self.inner
            .tx
            .lock()
            .as_ref()
            .map_or(SEND_QUEUE, mpsc::Sender::max_capacity)
    }

    /// Requests a graceful shutdown of this user's connection.
    ///
    /// Dropping the sender closes the channel, so the writer task drains the
    /// queued messages and then finishes.
    pub fn close(&self) {
        if !self.inner.closed.swap(true, Ordering::AcqRel) {
            *self.inner.tx.lock() = None;
        }
    }
}

impl Clone for User {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl PartialEq for User {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl Eq for User {}

impl std::hash::Hash for User {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::ptr::hash(Arc::as_ptr(&self.inner), state);
    }
}

/// Creates the outbound channel used by a new connection.
#[cfg(test)]
pub(crate) fn channel() -> (mpsc::Sender<Vec<u8>>, mpsc::Receiver<Vec<u8>>) {
    mpsc::channel(SEND_QUEUE)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user() -> (User, mpsc::Receiver<Vec<u8>>) {
        let (tx, rx) = channel();
        (
            User::new(
                "127.0.0.1:1234".to_string(),
                "127.0.0.1:1234".parse().unwrap(),
                tx,
            ),
            rx,
        )
    }

    #[tokio::test]
    async fn send_requires_connected_state() {
        let (u, mut rx) = user();
        assert!(!u.send(vec![1]));
        assert!(rx.try_recv().is_err());

        u.set_state(UserState::Connected);
        assert!(u.send(vec![1, 2, 3]));
        assert_eq!(rx.recv().await, Some(vec![1, 2, 3]));
    }

    #[tokio::test]
    async fn close_prevents_further_sends() {
        let (u, mut rx) = user();
        u.set_state(UserState::Connected);
        assert!(u.send(vec![7]));
        u.close();
        assert!(!u.send(vec![8]));
        assert!(u.is_closed());
        assert_eq!(rx.recv().await, Some(vec![7]));
        assert_eq!(rx.recv().await, None);
    }

    #[test]
    fn room_is_optional() {
        let (u, _rx) = user();
        assert_eq!(u.room(), None);
        u.set_room(Some(42));
        assert_eq!(u.room(), Some(42));
        u.set_room(None);
        assert_eq!(u.room(), None);
    }
}
