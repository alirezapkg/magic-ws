//! Event routing.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::user::User;
use crate::{Event, EventId, RoomId};

/// Callback invoked for every message that reaches the server.
pub type MessageHandler = Arc<dyn Fn(&User, &[u8]) + Send + Sync + 'static>;

/// Callback invoked for an event delivered to a client.
pub type ClientHandler = Arc<dyn Fn(&[u8]) + Send + Sync + 'static>;

/// Per-user event callback.
pub type UserEventHandler = Arc<dyn Fn(&User, &[u8]) + Send + Sync + 'static>;

/// Dispatches incoming server messages to handlers registered per event id.
#[derive(Clone, Default)]
pub struct ServerEvents {
    handlers: Arc<RwLock<HashMap<EventId, UserEventHandler>>>,
}

impl std::fmt::Debug for ServerEvents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerEvents")
            .field("handlers", &self.handlers.read().unwrap().len())
            .finish()
    }
}

impl ServerEvents {
    /// Creates an empty router.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `handler` for `event`.
    ///
    /// Registering the same event again replaces the previous handler.
    pub fn on<F>(&self, event: Event, handler: F)
    where
        F: Fn(&User, &[u8]) + Send + Sync + 'static,
    {
        debug_assert!(
            event.name().is_ascii(),
            "event name must be ASCII: {:?}",
            event.name()
        );
        let mut handlers = self.handlers.write().unwrap();
        handlers.insert(event.id(), Arc::new(handler));
    }

    /// Removes the handler registered for `event`.
    pub fn remove(&self, event: Event) {
        let mut handlers = self.handlers.write().unwrap();
        handlers.remove(&event.id());
    }

    /// Returns the handler registered for `event`, if any.
    #[must_use]
    pub fn get(&self, event: Event) -> Option<UserEventHandler> {
        let handlers = self.handlers.read().unwrap();
        handlers.get(&event.id()).cloned()
    }

    /// Routes `data` to the handler registered for its event id.
    ///
    /// Returns `true` when a handler ran. Messages shorter than the event-id
    /// prefix are ignored and also report `true` (they are malformed, not
    /// unrouted).
    pub fn dispatch(&self, user: &User, data: &[u8]) -> bool {
        let Some((event_id, payload)) = crate::unpack_message(data) else {
            tracing::trace!(user = %user.id(), "dropping malformed message");
            return true;
        };

        let handler = self.get(Event::from_parts(event_id, ""));
        match handler {
            Some(handler) => {
                handler(user, payload);
                true
            }
            None => {
                tracing::trace!(user = %user.id(), event = event_id, "no handler");
                false
            }
        }
    }

    /// Number of registered handlers.
    #[must_use]
    pub fn len(&self) -> usize {
        self.handlers.read().unwrap().len()
    }

    /// Returns `true` when no handler is registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Dispatches incoming client messages to per-event handlers.
#[derive(Clone, Default)]
pub struct ClientEvents {
    handlers: Arc<RwLock<HashMap<EventId, ClientHandler>>>,
}

impl std::fmt::Debug for ClientEvents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientEvents")
            .field("handlers", &self.handlers.read().unwrap().len())
            .finish()
    }
}

impl ClientEvents {
    /// Creates an empty client-side router.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `handler` for `event`.
    ///
    /// Registering the same event again replaces the previous handler.
    pub fn on<F>(&self, event: Event, handler: F)
    where
        F: Fn(&[u8]) + Send + Sync + 'static,
    {
        debug_assert!(
            event.name().is_ascii(),
            "event name must be ASCII: {:?}",
            event.name()
        );
        let mut handlers = self.handlers.write().unwrap();
        handlers.insert(event.id(), Arc::new(handler));
    }

    /// Removes the handler registered for `event`.
    pub fn remove(&self, event: Event) {
        let mut handlers = self.handlers.write().unwrap();
        handlers.remove(&event.id());
    }

    /// Routes `data` to the handler registered for its event id.
    ///
    /// Returns `true` when a handler ran.
    pub(crate) fn dispatch_checked(&self, data: &[u8]) -> bool {
        let Some((event_id, payload)) = crate::unpack_message(data) else {
            return true;
        };

        let handler = self.handlers.read().unwrap().get(&event_id).cloned();
        match handler {
            Some(handler) => {
                handler(payload);
                true
            }
            None => false,
        }
    }

    /// Routes `data` to the handler registered for its event id.
    pub fn dispatch(&self, data: &[u8]) {
        let _ = self.dispatch_checked(data);
    }

    /// Number of registered handlers.
    #[must_use]
    pub fn len(&self) -> usize {
        self.handlers.read().unwrap().len()
    }

    /// Returns `true` when no handler is registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Builder that emits an event to a whole room.
#[derive(Debug, Clone, Copy)]
pub struct RoomEmit<'a> {
    manager: &'a crate::UserManager,
    room: RoomId,
}

impl<'a> RoomEmit<'a> {
    /// Targets `room` through `manager`.
    #[must_use]
    pub fn new(manager: &'a crate::UserManager, room: RoomId) -> Self {
        Self { manager, room }
    }

    /// Returns the targeted room.
    #[must_use]
    pub fn room(&self) -> RoomId {
        self.room
    }

    /// Sends `event` with `payload` to every member of the room.
    ///
    /// Returns the number of users the message was queued for.
    pub fn emit(&self, event: Event, payload: &[u8]) -> usize {
        let msg = crate::pack_message(event.id(), payload);
        self.manager.send_to_room(self.room, &msg)
    }

    /// Sends `event` with `payload` to everyone in the room but `except`.
    ///
    /// Returns the number of users the message was queued for.
    pub fn emit_except(&self, event: Event, except: &str, payload: &[u8]) -> usize {
        let msg = crate::pack_message(event.id(), payload);
        self.manager.send_to_room_except(self.room, except, &msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{User, UserState};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn user() -> (User, tokio::sync::mpsc::Receiver<Vec<u8>>) {
        let (tx, rx) = crate::user::channel();
        let u = User::new("test".into(), "127.0.0.1:1".parse().unwrap(), tx);
        u.set_state(UserState::Connected);
        (u, rx)
    }

    #[test]
    fn server_events_dispatch_by_id() {
        const PING: Event = Event::new("ping");
        const PONG: Event = Event::new("pong");

        let router = ServerEvents::new();
        let seen = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&seen);
        router.on(PING, move |_u, payload| {
            counter.fetch_add(payload.len(), Ordering::SeqCst);
        });

        let (user, _rx) = user();
        assert!(router.dispatch(&user, &crate::pack_message(PING.id(), b"abc")));
        assert!(!router.dispatch(&user, &crate::pack_message(PONG.id(), b"ignored")));
        assert!(router.dispatch(&user, &[0, 0]));

        assert_eq!(seen.load(Ordering::SeqCst), 3);
        assert_eq!(router.len(), 1);
    }

    #[test]
    fn replacing_a_handler_keeps_one_entry() {
        const PING: Event = Event::new("ping");
        let router = ServerEvents::new();
        router.on(PING, |_u, _p| {});
        router.on(PING, |_u, _p| {});
        assert_eq!(router.len(), 1);

        router.remove(PING);
        assert!(router.is_empty());
    }

    #[test]
    fn client_events_dispatch() {
        const CHAT: Event = Event::new("chat");
        let router = ClientEvents::new();
        let seen = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&seen);
        router.on(CHAT, move |payload| {
            counter.fetch_add(payload.len(), Ordering::SeqCst);
        });

        assert!(router.dispatch_checked(&crate::pack_message(CHAT.id(), b"hello")));
        assert_eq!(seen.load(Ordering::SeqCst), 5);
    }
}
