//! Users and rooms.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, watch};

use crate::hub::HubInner;
use crate::protocol::SEND_QUEUE;
use crate::RoomId;

/// Error returned by [`User::try_send`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendError {
    /// The user's connection is gone.
    Closed,
    /// The outbound queue is full.
    QueueFull,
    /// The user is not in the connected state.
    NotConnected,
}

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closed => write!(f, "connection closed"),
            Self::QueueFull => write!(f, "send queue full"),
            Self::NotConnected => write!(f, "user not connected"),
        }
    }
}

impl std::error::Error for SendError {}

/// Rate-limit policy applied to incoming messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViolationPolicy {
    /// Silently drop messages that exceed the limit.
    Drop,
    /// Close the connection after `n` violations.
    DisconnectAfter(u32),
}

/// Token-bucket rate limit configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimit {
    /// Sustained messages per second.
    pub messages_per_second: u32,
    /// Maximum burst size.
    pub burst: u32,
    /// What to do when a message arrives with no tokens left.
    pub on_violation: ViolationPolicy,
}

impl Default for RateLimit {
    fn default() -> Self {
        Self {
            messages_per_second: 60,
            burst: 60,
            on_violation: ViolationPolicy::Drop,
        }
    }
}

/// Per-user token bucket.
#[derive(Debug)]
struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(capacity: u32) -> Self {
        Self {
            tokens: f64::from(capacity),
            last_refill: Instant::now(),
        }
    }

    /// Consumes one token, refilling based on elapsed time.
    ///
    /// Returns `true` when a token was available.
    fn consume(&mut self, rate: u32, burst: u32, now: Instant) -> bool {
        let elapsed = now.saturating_duration_since(self.last_refill);
        self.last_refill = now;
        let refill = elapsed.as_secs_f64() * f64::from(rate);
        let cap = f64::from(burst);
        self.tokens = (self.tokens + refill).min(cap);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

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
    pub(crate) inner: Arc<Inner>,
}

#[derive(Debug)]
pub(crate) struct Inner {
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
    /// Last time a Pong was received from this user, used by the heartbeat
    /// watchdog to detect dead connections.
    last_pong: Mutex<Option<Instant>>,
    /// Last time a Ping was sent to this user, used to compute RTT.
    last_ping_sent: Mutex<Option<Instant>>,
    /// Number of consecutive Pings sent without an intervening Pong.
    pub(crate) missed_pongs: AtomicU32,
    /// Last measured round-trip time in microseconds. `0` means unknown.
    rtt_us: AtomicU64,
    /// Fires when the connection is closed so the read loop can stop waiting
    /// on the socket even when the peer has gone silent.
    closed_tx: watch::Sender<bool>,
    /// Optional rate limit. `OnceLock` so it can be set right after the user is
    /// created (before the read loop starts) and is then immutable.
    rate_limit: OnceLock<RateLimit>,
    /// Per-user token bucket.
    bucket: Mutex<TokenBucket>,
    /// Number of rate-limit violations observed so far.
    violations: AtomicU32,
    /// Close code and reason requested by the application. The writer task
    /// reads this when it finishes.
    close_code: Mutex<Option<(u16, String)>>,
}

impl User {
    pub(crate) fn new(id: String, addr: std::net::SocketAddr, tx: mpsc::Sender<Vec<u8>>) -> Self {
        let (closed_tx, _) = watch::channel(false);
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
                last_pong: Mutex::new(None),
                last_ping_sent: Mutex::new(None),
                missed_pongs: AtomicU32::new(0),
                rtt_us: AtomicU64::new(0),
                closed_tx,
                rate_limit: OnceLock::new(),
                bucket: Mutex::new(TokenBucket::new(0)),
                violations: AtomicU32::new(0),
                close_code: Mutex::new(None),
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

    /// Records that a Ping was sent at `now`.
    pub(crate) fn note_ping_sent(&self, now: Instant) {
        *self.inner.last_ping_sent.lock() = Some(now);
        self.inner.missed_pongs.fetch_add(1, Ordering::AcqRel);
    }

    /// Records a received Pong, updating the RTT estimate and the missed-pong
    /// counter.
    pub(crate) fn record_pong(&self, now: Instant) {
        *self.inner.last_pong.lock() = Some(now);
        self.inner.missed_pongs.store(0, Ordering::Release);
        if let Some(sent) = *self.inner.last_ping_sent.lock() {
            let elapsed = now.saturating_duration_since(sent);
            self.inner
                .rtt_us
                .store(elapsed.as_micros() as u64, Ordering::Release);
        }
    }

    /// Returns the last measured round-trip time, if any.
    ///
    /// The value is `Some` once at least one Ping/Pong exchange completed.
    #[must_use]
    pub fn rtt(&self) -> Option<Duration> {
        let us = self.inner.rtt_us.load(Ordering::Acquire);
        (us > 0).then(|| Duration::from_micros(us))
    }

    /// Returns the last time a Pong was received, if any.
    #[must_use]
    pub fn last_seen(&self) -> Option<Instant> {
        *self.inner.last_pong.lock()
    }

    /// Attaches `limit` to this user. Must be called before the read loop
    /// starts; later calls are ignored.
    pub(crate) fn set_rate_limit(&self, limit: RateLimit) {
        if self.inner.rate_limit.set(limit).is_ok() {
            *self.inner.bucket.lock() = TokenBucket::new(limit.burst);
        }
    }

    /// Returns the number of rate-limit violations observed so far.
    #[must_use]
    pub fn violations(&self) -> u32 {
        self.inner.violations.load(Ordering::Acquire)
    }

    /// Consumes one token for an incoming message.
    ///
    /// Returns `true` when the message is allowed. When the bucket is empty the
    /// violation counter is incremented and `false` is returned; the caller
    /// applies [`ViolationPolicy`].
    pub(crate) fn check_rate_limit(&self) -> bool {
        let Some(limit) = self.inner.rate_limit.get() else {
            return true;
        };
        let mut bucket = self.inner.bucket.lock();
        if bucket.consume(limit.messages_per_second, limit.burst, Instant::now()) {
            true
        } else {
            self.inner.violations.fetch_add(1, Ordering::AcqRel);
            false
        }
    }

    /// Returns `true` when the user missed more than `max_missed` Pings.
    pub(crate) fn heartbeat_expired(&self, max_missed: u32) -> bool {
        self.inner.missed_pongs.load(Ordering::Acquire) > max_missed
    }

    /// Returns `true` when the last Pong is older than `timeout`.
    #[allow(dead_code)]
    pub(crate) fn heartbeat_timed_out(&self, timeout: Duration, now: Instant) -> bool {
        let last_pong = *self.inner.last_pong.lock();
        let last_ping = *self.inner.last_ping_sent.lock();
        match (last_pong, last_ping) {
            (Some(pong), _) => now.saturating_duration_since(pong) > timeout,
            (None, Some(ping)) => now.saturating_duration_since(ping) > timeout,
            (None, None) => false,
        }
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

    /// Queues `data` for delivery, returning a precise error on failure.
    ///
    /// Returns [`SendError::NotConnected`] when the user has not finished the
    /// handshake, [`SendError::Closed`] when the connection is gone, and
    /// [`SendError::QueueFull`] when the outbound queue is full.
    pub fn try_send(&self, data: Vec<u8>) -> std::result::Result<(), SendError> {
        if self.is_closed() {
            return Err(SendError::Closed);
        }
        if self.state() != UserState::Connected {
            return Err(SendError::NotConnected);
        }
        let guard = self.inner.tx.lock();
        let Some(tx) = guard.as_ref() else {
            return Err(SendError::Closed);
        };
        match tx.try_send(data) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                tracing::trace!(user = %self.inner.id, "send queue full, dropping packet");
                Err(SendError::QueueFull)
            }
            Err(TrySendError::Closed(_)) => Err(SendError::Closed),
        }
    }

    /// Queues `data` for delivery.
    ///
    /// Returns `true` when the message was queued. Returns `false` when the
    /// connection is gone or its queue is full, in which case the message is
    /// silently dropped. See [`User::try_send`] for a precise error.
    pub fn send(&self, data: Vec<u8>) -> bool {
        self.try_send(data).is_ok()
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
    /// queued messages and then finishes. The read loop is also woken so it
    /// stops waiting on the socket.
    pub fn close(&self) {
        if !self.inner.closed.swap(true, Ordering::AcqRel) {
            *self.inner.tx.lock() = None;
            let _ = self.inner.closed_tx.send(true);
        }
    }

    /// Closes the connection with a custom WebSocket close code and reason.
    ///
    /// The writer task sends the code once the outbound queue drains. If the
    /// connection is already closed this is a no-op.
    pub fn close_with(&self, code: u16, reason: &str) {
        *self.inner.close_code.lock() = Some((code, reason.to_string()));
        self.close();
    }

    /// Rejects the connection with close code `1008` (policy violation).
    pub fn reject(&self, reason: &str) {
        self.close_with(1008, reason);
    }

    /// Returns the requested close code and reason, if any.
    pub(crate) fn close_code(&self) -> Option<(u16, String)> {
        self.inner.close_code.lock().clone()
    }

    /// Returns a receiver that resolves once the connection is closed.
    pub(crate) fn closed_receiver(&self) -> watch::Receiver<bool> {
        self.inner.closed_tx.subscribe()
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

    #[tokio::test]
    async fn try_send_reports_precise_errors() {
        let (u, mut rx) = user();

        // Pending -> NotConnected.
        assert_eq!(u.try_send(vec![1]), Err(SendError::NotConnected));

        u.set_state(UserState::Connected);
        assert_eq!(u.try_send(vec![1]), Ok(()));

        // Fill the queue.
        let mut got_full = false;
        for _ in 0..1000 {
            if u.try_send(vec![0u8; 8]) == Err(SendError::QueueFull) {
                got_full = true;
                break;
            }
        }
        assert!(got_full, "queue should eventually be full");

        // Drain one to make room, then close.
        assert!(rx.try_recv().is_ok());
        u.close();
        assert_eq!(u.try_send(vec![2]), Err(SendError::Closed));
    }

    #[test]
    fn close_with_records_code() {
        let (u, _rx) = user();
        u.close_with(4001, "bye");
        assert_eq!(u.close_code(), Some((4001, "bye".to_string())));
        assert!(u.is_closed());
    }

    #[test]
    fn reject_uses_1008() {
        let (u, _rx) = user();
        u.reject("banned");
        assert_eq!(u.close_code(), Some((1008, "banned".to_string())));
    }

    #[tokio::test]
    async fn heartbeat_expires_after_max_missed() {
        let (u, _rx) = user();
        assert!(!u.heartbeat_expired(3));
        for _ in 0..3 {
            u.note_ping_sent(Instant::now());
        }
        assert!(!u.heartbeat_expired(3), "equal to max is allowed");
        u.note_ping_sent(Instant::now());
        assert!(u.heartbeat_expired(3), "over max is expired");

        // A pong after a small delay resets the counter and records an RTT.
        tokio::time::sleep(Duration::from_millis(2)).await;
        u.record_pong(Instant::now());
        assert!(!u.heartbeat_expired(3), "pong resets the counter");
        assert!(u.rtt().is_some(), "rtt is set after a pong");
    }
}
