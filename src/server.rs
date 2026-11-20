//! Binary WebSocket server.
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
//!             let _ = (room, payload);
//!         }
//!     })
//!     .build();
//!
//! server.listen("127.0.0.1:8080").await
//! # }
//! ```

use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_util::{SinkExt, Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch, Mutex};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::frame::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};
use tokio_tungstenite::{accept_async_with_config, WebSocketStream};

use crate::error::{Error, Result};
use crate::events::{MessageHandler, ServerEvents};
use crate::protocol::{self, WRITE_BUF_SIZE};
use crate::user::User;
use crate::UserManager;
use crate::{Event, RoomId, UserState};

/// WebSocket configuration applied to every accepted connection.
pub(crate) fn ws_config() -> WebSocketConfig {
    WebSocketConfig {
        write_buffer_size: WRITE_BUF_SIZE,
        max_message_size: Some(16 << 20),
        max_frame_size: Some(16 << 20),
        accept_unmasked_frames: false,
        ..WebSocketConfig::default()
    }
}

/// Callback fired right after the version handshake succeeded.
pub type ConnectHandler = Arc<dyn Fn(&User) + Send + Sync + 'static>;

/// Callback fired when a connection ends.
pub type DisconnectHandler = Arc<dyn Fn(&User) + Send + Sync + 'static>;

type WsError = tokio_tungstenite::tungstenite::Error;

fn close_frame(code: CloseCode, reason: &str) -> CloseFrame<'static> {
    CloseFrame {
        code,
        reason: reason.to_string().into(),
    }
}

/// Transport-independent view of an accepted WebSocket connection.
pub(crate) struct Connection<S> {
    socket: WebSocketStream<S>,
}

impl<S> Connection<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn new(socket: WebSocketStream<S>) -> Self {
        Self { socket }
    }

    async fn next_message(&mut self) -> Option<std::result::Result<Message, WsError>> {
        self.socket.next().await
    }

    async fn send(&mut self, message: Message) -> Result<()> {
        self.socket.send(message).await.map_err(Error::upgrade)
    }

    async fn flush(&mut self) -> Result<()> {
        self.socket.flush().await.map_err(Error::upgrade)
    }

    fn split(self) -> (ConnectionSink<S>, ConnectionStream<S>) {
        let (sink, stream) = self.socket.split();
        (
            ConnectionSink {
                sink: Box::pin(sink),
            },
            ConnectionStream {
                stream: Box::pin(stream),
            },
        )
    }
}

pub(crate) struct ConnectionSink<S> {
    sink: Pin<Box<futures_util::stream::SplitSink<WebSocketStream<S>, Message>>>,
}

impl<S> ConnectionSink<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Queues a message without flushing, so a batch can share one write.
    async fn feed(&mut self, message: Message) -> Result<()> {
        self.sink.feed(message).await.map_err(Error::upgrade)
    }

    /// Sends a message and flushes it immediately.
    async fn send(&mut self, message: Message) -> Result<()> {
        self.sink.send(message).await.map_err(Error::upgrade)
    }

    async fn flush(&mut self) -> Result<()> {
        self.sink.flush().await.map_err(Error::upgrade)
    }
}

pub(crate) struct ConnectionStream<S> {
    stream: Pin<Box<futures_util::stream::SplitStream<WebSocketStream<S>>>>,
}

impl<S> Stream for ConnectionStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    type Item = std::result::Result<Message, WsError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(cx)
    }
}

/// Builder for [`Server`].
#[derive(Default)]
pub struct ServerBuilder {
    on_connect: Option<ConnectHandler>,
    on_disconnect: Option<DisconnectHandler>,
    on_message: Option<MessageHandler>,
    events: ServerEvents,
    max_connections: Option<usize>,
    max_queue: Option<usize>,
}

impl std::fmt::Debug for ServerBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerBuilder")
            .field("events", &self.events)
            .field("max_connections", &self.max_connections)
            .field("max_queue", &self.max_queue)
            .finish_non_exhaustive()
    }
}

impl ServerBuilder {
    /// Sets the callback fired right after the version handshake.
    #[must_use]
    pub fn on_connect<F>(mut self, handler: F) -> Self
    where
        F: Fn(&User) + Send + Sync + 'static,
    {
        self.on_connect = Some(Arc::new(handler));
        self
    }

    /// Sets the callback fired when a connection ends.
    #[must_use]
    pub fn on_disconnect<F>(mut self, handler: F) -> Self
    where
        F: Fn(&User) + Send + Sync + 'static,
    {
        self.on_disconnect = Some(Arc::new(handler));
        self
    }

    /// Sets the fallback callback for messages without a registered handler.
    #[must_use]
    pub fn on_message<F>(mut self, handler: F) -> Self
    where
        F: Fn(&User, &[u8]) + Send + Sync + 'static,
    {
        self.on_message = Some(Arc::new(handler));
        self
    }

    /// Registers `handler` for `event`.
    ///
    /// `ServerEvents` is shared (`Arc` inside), so registering a handler does
    /// not need a `mut` binding.
    #[must_use]
    pub fn on_event<F>(self, event: Event, handler: F) -> Self
    where
        F: Fn(&User, &[u8]) + Send + Sync + 'static,
    {
        self.events.on(event, handler);
        self
    }

    /// Registers a handler that does not need the user context.
    #[must_use]
    pub fn on_event_simple<F>(self, event: Event, handler: F) -> Self
    where
        F: Fn(&[u8]) + Send + Sync + 'static,
    {
        self.on_event(event, move |_user, payload| handler(payload))
    }

    /// Caps the number of simultaneous connections.
    #[must_use]
    pub fn max_connections(mut self, limit: usize) -> Self {
        self.max_connections = Some(limit);
        self
    }

    /// Sets the outbound queue length per connection (default 256).
    #[must_use]
    pub fn max_queue(mut self, len: usize) -> Self {
        self.max_queue = Some(len.max(1));
        self
    }

    /// Builds the server.
    #[must_use]
    pub fn build(self) -> Server {
        Server(Arc::new(ServerInner {
            users: UserManager::new(),
            on_connect: self.on_connect,
            on_disconnect: self.on_disconnect,
            on_message: self.on_message,
            events: self.events,
            max_connections: self.max_connections,
            max_queue: self.max_queue.unwrap_or(protocol::SEND_QUEUE),
            connections: AtomicUsize::new(0),
            shutdown: watch::channel(false).0,
            tasks: Mutex::new(Vec::new()),
        }))
    }
}

/// Binary WebSocket server with room and event support.
///
/// The server is an `Arc` handle: clone it freely and share it with callbacks.
#[derive(Clone)]
pub struct Server(Arc<ServerInner>);

struct ServerInner {
    users: UserManager,
    on_connect: Option<ConnectHandler>,
    on_disconnect: Option<DisconnectHandler>,
    on_message: Option<MessageHandler>,
    events: ServerEvents,
    max_connections: Option<usize>,
    max_queue: usize,
    connections: AtomicUsize,
    shutdown: watch::Sender<bool>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl std::fmt::Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server")
            .field("users", &self.users().len())
            .field("connections", &self.connection_count())
            .finish()
    }
}

impl Default for Server {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl Server {
    /// Returns a fresh builder.
    #[must_use]
    pub fn builder() -> ServerBuilder {
        ServerBuilder::default()
    }

    /// Creates a server with default settings.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the user registry.
    #[must_use]
    pub fn users(&self) -> &UserManager {
        &self.0.users
    }

    /// Registers an event handler after construction.
    pub fn on_event<F>(&self, event: Event, handler: F)
    where
        F: Fn(&User, &[u8]) + Send + Sync + 'static,
    {
        self.0.events.on(event, handler);
    }

    /// Number of currently accepted connections.
    #[must_use]
    pub fn connection_count(&self) -> usize {
        self.0.connections.load(Ordering::Acquire)
    }

    /// Number of connected users.
    #[must_use]
    pub fn user_count(&self) -> usize {
        self.0.users.len()
    }

    /// Returns a future that resolves once [`Server::shutdown`] is called.
    pub fn shutdown_signal(&self) -> ShutdownSignal {
        ShutdownSignal {
            rx: self.0.shutdown.subscribe(),
        }
    }

    /// Binds `addr` and serves until shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error when the address cannot be bound or the accept loop
    /// fails.
    pub async fn listen(&self, addr: impl tokio::net::ToSocketAddrs) -> Result<()> {
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|err| Error::InvalidAddress(format!("{err} (while binding)")))?;
        let local = listener.local_addr().map_err(Error::Io)?;
        tracing::info!(%local, "magic-ws server listening");
        self.serve(listener).await
    }

    /// Serves connections arriving on `listener` until shutdown.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Stopped`] when the server was already shut down, or an
    /// I/O error when accepting fails.
    pub async fn serve(&self, listener: TcpListener) -> Result<()> {
        if *self.0.shutdown.borrow() {
            return Err(Error::Stopped);
        }

        let mut shutdown = self.0.shutdown.subscribe();

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        break;
                    }
                }
                accepted = listener.accept() => {
                    let (stream, addr) = accepted.map_err(Error::Io)?;
                    if self.at_capacity() {
                        tracing::warn!(%addr, "connection rejected: server at capacity");
                        drop(stream);
                        continue;
                    }
                    if let Err(err) = stream.set_nodelay(true) {
                        tracing::trace!(%err, "failed to set nodelay");
                    }
                    let this = self.clone();
                    let handle = tokio::spawn(async move {
                        if let Err(err) = this.accept(stream, addr).await {
                            tracing::debug!(%addr, %err, "connection ended with error");
                        }
                    });
                    self.0.tasks.lock().await.push(handle);
                }
            }
        }

        let tasks: Vec<JoinHandle<()>> = self.0.tasks.lock().await.drain(..).collect();
        for task in tasks {
            task.abort();
        }
        Ok(())
    }

    /// Stops accepting connections and disconnects every client.
    ///
    /// Uses `send_replace` rather than `send`: `send` is a no-op when no
    /// [`ShutdownSignal`] exists yet, which would silently swallow the
    /// shutdown request.
    pub fn shutdown(&self) {
        self.0.shutdown.send_replace(true);
        for user in self.0.users.users() {
            user.close();
        }
    }

    /// Disconnects a single user.
    pub fn disconnect(&self, user: &User) {
        self.0.users.remove(user.id());
    }

    /// Disconnects the user with the given application-assigned player id.
    ///
    /// Returns `true` when a matching user was found and disconnected.
    pub fn disconnect_by_player_id(&self, id: &str) -> bool {
        self.0.users.remove_by_player_id(id).is_some()
    }

    /// Queues `msg` for `user`.
    ///
    /// Returns `false` when the user is gone or its queue is full.
    pub fn send_to(&self, user: &User, msg: Vec<u8>) -> bool {
        user.send(msg)
    }

    /// Encodes and queues `event` for `user`.
    pub fn emit(&self, user: &User, event: Event, payload: &[u8]) -> bool {
        user.send_event(event, payload)
    }

    /// Returns a room emitter for `room`.
    #[must_use]
    pub fn to_room(&self, room: RoomId) -> crate::RoomEmit<'_> {
        crate::RoomEmit::new(self.users(), room)
    }

    /// Moves `user` into `room`, leaving any previous room.
    ///
    /// Passing `0` removes the user from its room.
    pub fn set_user_room(&self, user: &User, room: RoomId) {
        let room = (room != 0).then_some(room);
        self.0.users.set_user_room(user, room);
    }

    /// Atomically checks the room capacity and moves `user` into `room`.
    ///
    /// `cap == 0` means unlimited. Returns [`JoinError::RoomFull`] when the
    /// room already holds `cap` users. Unlike [`Server::set_user_room`] the
    /// check and the move are atomic, so concurrent joins cannot exceed the
    /// capacity.
    pub fn join_room(&self, user: &User, room: RoomId, cap: usize) -> Result<()> {
        if room == 0 {
            return Err(Error::InvalidAddress(
                "room id 0 is not a valid room".into(),
            ));
        }
        self.0
            .users
            .join_room(user, room, cap)
            .map_err(Error::RoomFull)
    }

    /// Removes `user` from its current room, returning the old room id.
    pub fn leave_room(&self, user: &User) -> Option<RoomId> {
        let old = user.room();
        user.set_room(None);
        old
    }

    /// Returns the users currently in `room`.
    #[must_use]
    pub fn room_members(&self, room: RoomId) -> Vec<User> {
        self.0.users.room_members(room)
    }

    /// Emits `event` to every member of `room`.
    ///
    /// Returns the number of users the message was queued for.
    pub fn emit_to_room(&self, room: RoomId, event: Event, payload: &[u8]) -> usize {
        self.to_room(room).emit(event, payload)
    }

    /// Broadcasts `msg` to every connected user.
    ///
    /// Returns the number of users the message was queued for.
    pub fn broadcast(&self, msg: &[u8]) -> usize {
        self.0.users.broadcast(msg)
    }

    fn at_capacity(&self) -> bool {
        self.0
            .max_connections
            .is_some_and(|limit| self.connection_count() >= limit)
    }

    async fn accept(&self, stream: TcpStream, addr: std::net::SocketAddr) -> Result<()> {
        self.accept_stream(stream, addr).await
    }

    /// Accepts an already-established stream (plain TCP or TLS).
    ///
    /// This is the entry point for TLS listeners: run the TLS handshake
    /// yourself, then hand the stream over.
    ///
    /// # Errors
    ///
    /// Returns an error when the WebSocket upgrade fails.
    pub async fn accept_stream<S>(&self, stream: S, addr: std::net::SocketAddr) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let socket = accept_async_with_config(stream, Some(ws_config()))
            .await
            .map_err(Error::upgrade)?;

        self.run(Connection::new(socket), addr).await
    }

    async fn run<S>(&self, connection: Connection<S>, addr: std::net::SocketAddr) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let mut connection = connection;
        self.0.connections.fetch_add(1, Ordering::AcqRel);
        let guard = ConnectionGuard(Arc::clone(&self.0));

        // --- version handshake ------------------------------------------------
        let version_ok = match connection.next_message().await {
            Some(Ok(Message::Binary(data))) => protocol::is_version_packet(&data),
            _ => false,
        };

        if !version_ok {
            tracing::debug!(%addr, "rejected connection: bad version handshake");
            let _ = connection
                .send(Message::Close(Some(close_frame(
                    CloseCode::Protocol,
                    "unsupported protocol version",
                ))))
                .await;
            let _ = connection.flush().await;
            drop(guard);
            return Ok(());
        }

        let (tx, rx) = mpsc::channel(self.0.max_queue);
        let user = User::new(addr.to_string(), addr, tx);
        user.set_state(UserState::Connected);
        let user = self.0.users.add(user);

        if let Some(handler) = &self.0.on_connect {
            handler(&user);
        }

        // Pings are answered inline; outbound messages go through a channel
        // that carries both binary payloads and control frames.
        let (ctrl_tx, ctrl_rx) = mpsc::unbounded_channel::<Message>();
        let (sink, mut stream) = connection.split();
        let writer = tokio::spawn(writer_task(sink, rx, ctrl_rx));

        // --- read loop ---------------------------------------------------------
        while let Some(item) = stream.next().await {
            match item {
                Ok(Message::Binary(data)) => {
                    let bytes = data.to_vec();
                    if !self.route(&user, &bytes) {
                        if let Some(handler) = &self.0.on_message {
                            handler(&user, &bytes);
                        }
                    }
                }
                Ok(Message::Close(_)) => break,
                Ok(Message::Ping(payload)) => {
                    if ctrl_tx.send(Message::Pong(payload)).is_err() {
                        break;
                    }
                }
                Ok(Message::Pong(_)) | Ok(Message::Text(_)) | Ok(Message::Frame(_)) => {}
                Err(err) => {
                    tracing::debug!(%addr, %err, "read error");
                    break;
                }
            }
        }

        writer.abort();
        self.0.users.remove(user.id());
        drop(guard);

        if let Some(handler) = &self.0.on_disconnect {
            handler(&user);
        }

        Ok(())
    }

    /// Routes `data` to its handler, reporting whether one ran.
    fn route(&self, user: &User, data: &[u8]) -> bool {
        self.0.events.dispatch(user, data)
    }
}

/// Writes queued messages to the socket until the channel closes.
/// Number of messages to coalesce into one socket write.
///
/// `SinkExt::send` flushes after every message, which costs one syscall per
/// packet. Draining the queue first and flushing once turns a burst of 100
/// queued messages into a single write.
const WRITE_BATCH: usize = 256;

async fn writer_task<S>(
    mut sink: ConnectionSink<S>,
    mut rx: mpsc::Receiver<Vec<u8>>,
    mut ctrl: mpsc::UnboundedReceiver<Message>,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let mut wrote_any = false;

        // Drain everything that is already queued, then flush once. `send`
        // would flush per message and cost a syscall each time.
        for _ in 0..WRITE_BATCH {
            match rx.try_recv() {
                Ok(message) => {
                    if sink.feed(Message::Binary(message)).await.is_err() {
                        return finish(&mut sink).await;
                    }
                    wrote_any = true;
                }
                Err(_) => break,
            }
        }

        // Control frames (pongs, closes) must not sit behind a large batch.
        while let Ok(message) = ctrl.try_recv() {
            if sink.feed(message).await.is_err() {
                return finish(&mut sink).await;
            }
            wrote_any = true;
        }

        if wrote_any {
            if sink.flush().await.is_err() {
                return finish(&mut sink).await;
            }
            continue;
        }

        // Nothing buffered: wait for either side.
        let next = tokio::select! {
            biased;
            Some(message) = rx.recv() => Some(Message::Binary(message)),
            ctrl_message = ctrl.recv() => ctrl_message,
            else => break,
        };

        let Some(message) = next else { break };
        if sink.send(message).await.is_err() {
            break;
        }
    }

    finish(&mut sink).await;
}

/// Sends the closing frame after the writer loop ends.
async fn finish<S>(sink: &mut ConnectionSink<S>)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let _ = sink
        .send(Message::Close(Some(close_frame(
            CloseCode::Away,
            "server closed connection",
        ))))
        .await;
    let _ = sink.flush().await;
}

struct ConnectionGuard(Arc<ServerInner>);

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.0.connections.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Future resolving once the server shuts down.
#[derive(Debug)]
#[must_use = "futures do nothing unless polled"]
pub struct ShutdownSignal {
    rx: watch::Receiver<bool>,
}

impl std::future::Future for ShutdownSignal {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if *self.rx.borrow_and_update() {
            return Poll::Ready(());
        }
        // `Receiver::changed` is `!Unpin`; pin it on the stack for this poll.
        let changed = std::pin::pin!(self.rx.changed());
        let mut changed = changed;
        match changed.as_mut().poll(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(()),
            Poll::Ready(Err(_)) => Poll::Ready(()),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Computes the `Sec-WebSocket-Accept` value for `key`.
///
/// Exposed for integrators that implement their own upgrade path.
#[must_use]
pub fn accept_key(key: &str) -> String {
    derive_accept_key(key.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accept_key_matches_rfc_example() {
        // RFC 6455 section 1.3 example.
        assert_eq!(
            accept_key("dGhlIHNhbXBsZSBub25jZQ=="),
            "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
        );
    }

    #[test]
    fn shutdown_signal_resolves() {
        use std::future::Future;

        let server = Server::new();
        let signal = server.shutdown_signal();
        server.shutdown();
        // The signalled receiver resolves immediately.
        let waker = futures_util::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        let mut signal = std::pin::pin!(signal);
        assert!(signal.as_mut().poll(&mut cx).is_ready());
    }

    #[tokio::test]
    async fn serve_reports_already_stopped() {
        let server = Server::new();
        server.shutdown();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        assert!(matches!(server.serve(listener).await, Err(Error::Stopped)));
    }

    #[test]
    fn zero_connection_limit_is_not_a_limit() {
        let server = Server::builder().max_connections(0).build();
        assert_eq!(server.connection_count(), 0);
        assert_eq!(server.user_count(), 0);
    }
}
