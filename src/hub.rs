//! Connection registry: users, rooms and broadcast helpers.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};

use crate::user::User;
use crate::RoomId;

/// Tracks every connected user and their room membership.
///
/// The registry is cheap to clone and safe to share across tasks.
#[derive(Debug, Clone)]
pub struct UserManager {
    inner: Arc<HubInner>,
}

/// Shared state of the registry.
///
/// `pub(crate)` so [`User::set_room`] can keep the room index in sync.
#[derive(Debug, Default)]
pub(crate) struct HubInner {
    pub(crate) users: RwLock<HashMap<String, User>>,
    /// Secondary index keyed by the application-assigned player id. Only users
    /// that called [`User::set_player_id`] appear here.
    pub(crate) player_ids: RwLock<HashMap<String, User>>,
    /// Per-room queues of users. A queue preserves insertion order and lets
    /// removal be O(1) on average instead of O(n) per call.
    pub(crate) rooms: RwLock<HashMap<RoomId, VecDeque<User>>>,
}

impl HubInner {
    /// Moves `user` to `room`, updating the room index.
    ///
    /// Called from [`User::set_room`]; `User` reaches this through the
    /// `Weak<HubInner>` it holds.
    pub(crate) fn set_room(&self, user: &User, room: Option<RoomId>) {
        let current = user.room();
        if current == room {
            return;
        }

        if let Some(old) = current {
            self.remove_from_room(user, old);
        }

        // Record the new room without going through `User::set_room`, which
        // would re-enter this method.
        user.store_room(room);

        if let Some(room_id) = room {
            self.rooms
                .write()
                .unwrap()
                .entry(room_id)
                .or_default()
                .push_back(user.clone());
        }
    }

    /// Drops `user` out of `room`'s queue.
    pub(crate) fn remove_from_room(&self, user: &User, room: RoomId) {
        let mut rooms = self.rooms.write().unwrap();
        let Entry::Occupied(mut entry) = rooms.entry(room) else {
            return;
        };
        let queue = entry.get_mut();
        if let Some(pos) = queue.iter().position(|u| u.id() == user.id()) {
            queue.remove(pos);
        }
        if queue.is_empty() {
            entry.remove();
        }
    }

    /// Indexes `user` under its player id, if it has one.
    ///
    /// Called from [`User::set_player_id`] so ids assigned after the user was
    /// added (for example in `on_connect`) are reachable through
    /// [`UserManager::get_by_player_id`].
    pub(crate) fn index_player_id(&self, user: &User) {
        if let Some(player_id) = user.player_id() {
            self.player_ids
                .write()
                .unwrap()
                .insert(player_id.to_string(), user.clone());
        }
    }
}

impl Default for UserManager {
    fn default() -> Self {
        Self::new()
    }
}

impl UserManager {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(HubInner::default()),
        }
    }

    /// Registers `user` and returns the handle that was stored.
    ///
    /// The user is linked back to this registry, so later room changes through
    /// [`User::set_room`] keep the room index up to date. If the user already
    /// carries a player id it is indexed too.
    pub fn add(&self, user: User) -> User {
        user.attach_hub(&self.inner);
        if let Some(player_id) = user.player_id() {
            self.inner
                .player_ids
                .write()
                .unwrap()
                .insert(player_id.to_string(), user.clone());
        }
        self.inner
            .users
            .write()
            .unwrap()
            .insert(user.id().to_string(), user.clone());
        user
    }

    /// Looks a user up by id.
    #[must_use]
    pub fn get(&self, id: &str) -> Option<User> {
        self.inner.users.read().unwrap().get(id).cloned()
    }

    /// Looks a user up by its application-assigned player id.
    ///
    /// Only users that called [`User::set_player_id`] are indexed.
    #[must_use]
    pub fn get_by_player_id(&self, id: &str) -> Option<User> {
        self.inner.player_ids.read().unwrap().get(id).cloned()
    }

    /// Removes `id` from the registry and from its room.
    ///
    /// Returns the removed user, if any.
    pub fn remove(&self, id: &str) -> Option<User> {
        let removed = self.inner.users.write().unwrap().remove(id)?;
        removed.close();

        if let Some(player_id) = removed.player_id() {
            self.inner.player_ids.write().unwrap().remove(player_id);
        }
        if let Some(room) = removed.room() {
            self.inner.remove_from_room(&removed, room);
        }
        removed.store_room(None);

        Some(removed)
    }

    /// Removes the user identified by its application-assigned player id.
    ///
    /// The connection is closed. Returns the removed user, if any.
    pub fn remove_by_player_id(&self, id: &str) -> Option<User> {
        let user = self.get_by_player_id(id)?;
        self.remove(user.id())
    }

    /// Moves `user` to `room`, leaving its previous room first.
    ///
    /// Passing `None` removes the user from any room.
    pub fn set_user_room(&self, user: &User, room: Option<RoomId>) {
        self.inner.set_room(user, room);
    }

    /// Returns the users in `room`.
    #[must_use]
    pub fn room_members(&self, room: RoomId) -> Vec<User> {
        self.inner
            .rooms
            .read()
            .unwrap()
            .get(&room)
            .map(|q| q.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Returns the number of users in `room`.
    #[must_use]
    pub fn room_len(&self, room: RoomId) -> usize {
        self.inner
            .rooms
            .read()
            .unwrap()
            .get(&room)
            .map_or(0, VecDeque::len)
    }

    /// Sends `data` to everyone in `room`.
    ///
    /// Returns the number of users the message was queued for.
    pub fn send_to_room(&self, room: RoomId, data: &[u8]) -> usize {
        self.send_to_room_impl(room, None, data)
    }

    /// Sends `data` to everyone in `room` except `except`.
    pub fn send_to_room_except(&self, room: RoomId, except: &str, data: &[u8]) -> usize {
        self.send_to_room_impl(room, Some(except), data)
    }

    fn send_to_room_impl(&self, room: RoomId, except: Option<&str>, data: &[u8]) -> usize {
        let queue = self.inner.rooms.read().unwrap();
        let Some(members) = queue.get(&room) else {
            return 0;
        };
        let mut sent = 0;
        for user in members.iter() {
            if except.is_some_and(|id| id == user.id()) {
                continue;
            }
            if user.send(data.to_vec()) {
                sent += 1;
            }
        }
        sent
    }

    /// Sends `data` to every connected user.
    ///
    /// Returns the number of users the message was queued for.
    pub fn broadcast(&self, data: &[u8]) -> usize {
        let users: Vec<User> = self
            .inner
            .users
            .read()
            .unwrap()
            .values()
            .filter(|u| u.state() == crate::UserState::Connected)
            .cloned()
            .collect();

        users.iter().filter(|u| u.send(data.to_vec())).count()
    }

    /// Drops every member of `room` from it.
    ///
    /// Returns the users that were removed from the room.
    pub fn clear_room(&self, room: RoomId) -> Vec<User> {
        let queue = self.inner.rooms.write().unwrap().remove(&room);
        let Some(members) = queue else {
            return Vec::new();
        };
        // The queue is already gone from the index, so store the room change
        // directly instead of going through `User::set_room`.
        for user in &members {
            user.store_room(None);
        }
        members.into_iter().collect()
    }

    /// Number of connected users.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.users.read().unwrap().len()
    }

    /// Returns `true` when no user is connected.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.users.read().unwrap().is_empty()
    }

    /// Number of active rooms.
    #[must_use]
    pub fn room_count(&self) -> usize {
        self.inner.rooms.read().unwrap().len()
    }

    /// Snapshot of every connected user.
    #[must_use]
    pub fn users(&self) -> Vec<User> {
        self.inner.users.read().unwrap().values().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::UserState;

    fn connected(id: &str) -> User {
        let (tx, _rx) = crate::user::channel();
        let user = User::new(id.to_string(), "127.0.0.1:1".parse().unwrap(), tx);
        user.set_state(UserState::Connected);
        user
    }

    #[test]
    fn add_get_remove() {
        let manager = UserManager::new();
        let user = manager.add(connected("a"));

        assert_eq!(manager.len(), 1);
        assert_eq!(
            manager.get("a").map(|u| u.id().to_string()),
            Some("a".into())
        );

        manager.remove("a");
        assert!(manager.is_empty());
        assert!(user.is_closed());
        assert!(manager.remove("a").is_none());
    }

    #[test]
    fn room_membership_follows_user() {
        let manager = UserManager::new();
        let a = manager.add(connected("a"));
        let b = manager.add(connected("b"));

        manager.set_user_room(&a, Some(1));
        manager.set_user_room(&b, Some(1));
        assert_eq!(manager.room_len(1), 2);
        assert_eq!(a.room(), Some(1));

        manager.set_user_room(&a, Some(2));
        assert_eq!(manager.room_len(1), 1);
        assert_eq!(manager.room_len(2), 1);
        assert_eq!(a.room(), Some(2));

        manager.set_user_room(&a, None);
        assert_eq!(manager.room_len(2), 0);
        assert_eq!(a.room(), None);

        manager.set_user_room(&b, Some(1));
        manager.set_user_room(&b, Some(1));
        assert_eq!(
            manager.room_len(1),
            1,
            "re-joining the same room is a no-op"
        );
    }

    #[test]
    fn remove_leaves_room() {
        let manager = UserManager::new();
        let a = manager.add(connected("a"));
        manager.set_user_room(&a, Some(9));
        manager.remove("a");

        assert_eq!(manager.room_len(9), 0);
        assert_eq!(manager.room_count(), 0);
    }

    #[tokio::test]
    async fn send_to_room_except_skips_sender() {
        let manager = UserManager::new();
        let (tx_a, mut rx_a) = crate::user::channel();
        let (tx_b, mut rx_b) = crate::user::channel();
        let a = manager.add(User::new("a".into(), "127.0.0.1:1".parse().unwrap(), tx_a));
        let b = manager.add(User::new("b".into(), "127.0.0.1:2".parse().unwrap(), tx_b));
        a.set_state(UserState::Connected);
        b.set_state(UserState::Connected);
        manager.set_user_room(&a, Some(3));
        manager.set_user_room(&b, Some(3));

        assert_eq!(manager.send_to_room(3, b"hi"), 2);
        assert_eq!(rx_a.recv().await, Some(b"hi".to_vec()));
        assert_eq!(rx_b.recv().await, Some(b"hi".to_vec()));

        assert_eq!(manager.send_to_room_except(3, "a", b"yo"), 1);
        assert_eq!(rx_b.recv().await, Some(b"yo".to_vec()));
        assert!(rx_a.try_recv().is_err(), "sender is skipped");
    }

    #[tokio::test]
    async fn clear_room_detaches_members() {
        let manager = UserManager::new();
        let a = manager.add(connected("a"));
        manager.set_user_room(&a, Some(5));

        let cleared = manager.clear_room(5);
        assert_eq!(cleared.len(), 1);
        assert_eq!(a.room(), None);
        assert_eq!(manager.room_count(), 0);
    }

    #[tokio::test]
    async fn broadcast_only_hits_connected_users() {
        let manager = UserManager::new();
        let pending = manager.add(connected("pending"));
        pending.set_state(UserState::Pending);
        let (tx, mut rx) = crate::user::channel();
        let live = manager.add(User::new("live".into(), "127.0.0.1:3".parse().unwrap(), tx));
        live.set_state(UserState::Connected);

        assert_eq!(manager.broadcast(b"x"), 1);
        assert_eq!(rx.recv().await, Some(b"x".to_vec()));
    }

    #[test]
    fn player_id_lookup_and_removal() {
        let manager = UserManager::new();
        let user = manager.add(connected("a"));

        assert!(user.set_player_id("player:123"));
        assert_eq!(
            manager
                .get_by_player_id("player:123")
                .map(|u| u.id().to_string()),
            Some("a".into())
        );

        // Second set is rejected and keeps the first value.
        assert!(!user.set_player_id("player:456"));
        assert_eq!(user.player_id(), Some("player:123"));
        assert!(manager.get_by_player_id("player:456").is_none());

        // Removing by player id drops the connection and both indexes.
        let removed = manager.remove_by_player_id("player:123");
        assert_eq!(removed.map(|u| u.id().to_string()), Some("a".into()));
        assert!(manager.get_by_player_id("player:123").is_none());
        assert!(manager.get("a").is_none());
        assert!(user.is_closed());
    }

    #[tokio::test]
    async fn player_id_survives_reindex_on_add() {
        // A user that already carries a player id is indexed on add.
        let manager = UserManager::new();
        let (tx, _rx) = crate::user::channel();
        let user = User::new("a".into(), "127.0.0.1:1".parse().unwrap(), tx);
        user.set_player_id("p:1");
        let user = manager.add(user);
        assert!(manager.get_by_player_id("p:1").is_some());
        assert_eq!(user.player_id(), Some("p:1"));
    }
}
