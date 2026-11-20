//! WebSocket client.
//!
//! The client speaks the same binary framing as the server: it sends the
//! one-byte protocol version right after the upgrade and then exchanges
//! `[event id][payload]` messages.
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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use parking_lot::Mutex;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::frame::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::Message;
#[cfg(not(feature = "tls"))]
use tokio_tungstenite::WebSocketStream;
#[cfg(feature = "tls")]
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::error::{Error, Result};
use crate::events::ClientEvents;
use crate::protocol;

/// Callback invoked for every message received by the client.
pub type ClientMessageHandler = Arc<dyn Fn(&[u8]) + Send + Sync + 'static>;

/// Callback invoked when the client connection ends.
pub type ClientCloseHandler = Arc<dyn Fn() + Send + Sync + 'static>;

/// Default timeout applied to the TCP connect and the WebSocket upgrade.
pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// The concrete stream type produced by [`Client::connect`].
#[cfg(feature = "tls")]
pub type DefaultSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;
/// The concrete stream type produced by [`Client::connect`] without `tls`.
#[cfg(not(feature = "tls"))]
pub type DefaultSocket = WebSocketStream<TcpStream>;

struct ClientInner {
    tx: Mutex<Option<mpsc::Sender<Vec<u8>>>>,
    closed: AtomicBool,
    message_handler: RwLock<Option<ClientMessageHandler>>,
    close_handler: RwLock<Option<ClientCloseHandler>>,
    events: ClientEvents,
    shutdown: watch::Sender<bool>,
}

/// A WebSocket client connection.
///
/// Cloning is cheap (`Arc` inside); all clones share the same connection.
#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
    writer: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
    reader: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("closed", &self.is_closed())
            .field("queue_capacity", &self.queue_capacity())
            .finish()
    }
}

impl Client {
    /// Opens a `ws://` connection and performs the version handshake.
    ///
    /// # Errors
    ///
    /// Returns an error when the URL is invalid, the TCP connection fails, the
    /// WebSocket upgrade fails, the handshake times out or the server rejects
    /// the protocol version.
    pub async fn connect(url: &str) -> Result<Self> {
        Self::connect_with_timeout(url, DEFAULT_HANDSHAKE_TIMEOUT).await
    }

    /// Like [`Client::connect`] but with an explicit handshake timeout.
    ///
    /// # Errors
    ///
    /// Same as [`Client::connect`].
    pub async fn connect_with_timeout(url: &str, timeout: Duration) -> Result<Self> {
        #[cfg(feature = "tls")]
        {
            Self::connect_with_connector(url, timeout, None).await
        }
        #[cfg(not(feature = "tls"))]
        {
            Self::connect_with_connector(url, timeout).await
        }
    }

    /// Connects with a caller-supplied rustls client configuration.
    ///
    /// This is how you talk to a server with a self-signed or private
    /// certificate: build a `ClientConfig` that trusts it and pass it here.
    /// When `tls_config` is `None` the behaviour matches
    /// [`Client::connect_with_timeout`].
    ///
    /// # Errors
    ///
    /// Same as [`Client::connect`].
    #[cfg(feature = "tls")]
    pub async fn connect_with_tls_config(
        url: &str,
        timeout: Duration,
        tls_config: std::sync::Arc<rustls::ClientConfig>,
    ) -> Result<Self> {
        Self::connect_with_connector(url, timeout, Some(tls_config)).await
    }

    /// Shared implementation behind the `connect*` constructors.
    async fn connect_with_connector(
        url: &str,
        timeout: Duration,
        #[cfg(feature = "tls")] tls_config: Option<std::sync::Arc<rustls::ClientConfig>>,
    ) -> Result<Self> {
        // rustls needs a crypto provider; `tungstenite` does not pick one.
        #[cfg(feature = "tls")]
        let _ = install_crypto_provider();

        let request = url
            .parse::<http::Uri>()
            .map_err(|err| Error::InvalidUrl(format!("{url}: {err}")))?
            .into_client_request()
            .map_err(|err| Error::InvalidUrl(format!("{url}: {err}")))?;

        let (host, port) = split_authority(&request)?;

        let tcp = tokio::time::timeout(timeout, TcpStream::connect((host.as_str(), port)))
            .await
            .map_err(|_| Error::address(format!("{host}:{port} (timeout)")))?
            .map_err(|err| Error::address(format!("{host}:{port}: {err}")))?;
        tcp.set_nodelay(true).ok();

        #[cfg(feature = "tls")]
        let socket = {
            let connector = tls_config.map(tokio_tungstenite::Connector::Rustls);
            tokio::time::timeout(
                timeout,
                tokio_tungstenite::client_async_tls_with_config(request, tcp, None, connector),
            )
            .await
            .map_err(|_| {
                Error::upgrade(tokio_tungstenite::tungstenite::Error::Io(
                    std::io::Error::new(std::io::ErrorKind::TimedOut, "handshake timeout"),
                ))
            })?
            .map_err(Error::upgrade)?
            .0
        };

        // Plain `ws://` without the `tls` feature. `wss://` is rejected because
        // there is no TLS implementation compiled in.
        #[cfg(not(feature = "tls"))]
        let socket = {
            if request.uri().scheme_str() == Some("wss") {
                return Err(Error::InvalidUrl(
                    "wss:// requires the `tls` feature".to_string(),
                ));
            }
            tokio::time::timeout(timeout, tokio_tungstenite::client_async(request, tcp))
                .await
                .map_err(|_| {
                    Error::upgrade(tokio_tungstenite::tungstenite::Error::Io(
                        std::io::Error::new(std::io::ErrorKind::TimedOut, "handshake timeout"),
                    ))
                })?
                .map_err(Error::upgrade)?
                .0
        };

        Self::start(socket).await
    }

    /// Wraps an already upgraded WebSocket stream.
    ///
    /// The protocol version byte is sent immediately, mirroring the behaviour
    /// of the reference client implementations.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket dies during the version handshake.
    pub async fn start(socket: DefaultSocket) -> Result<Self> {
        let mut socket = socket;
        socket
            .send(Message::Binary(protocol::version_packet().to_vec()))
            .await
            .map_err(Error::upgrade)?;
        socket.flush().await.map_err(Error::upgrade)?;

        let (sink, stream) = socket.split();
        let (tx, rx) = mpsc::channel::<Vec<u8>>(protocol::SEND_QUEUE);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let inner = Arc::new(ClientInner {
            tx: Mutex::new(Some(tx)),
            closed: AtomicBool::new(false),
            message_handler: RwLock::new(None),
            close_handler: RwLock::new(None),
            events: ClientEvents::new(),
            shutdown: shutdown_tx,
        });

        let client = Self {
            inner: Arc::clone(&inner),
            writer: Arc::new(tokio::sync::Mutex::new(None)),
            reader: Arc::new(tokio::sync::Mutex::new(None)),
        };

        let writer = spawn_writer(sink, rx, Arc::clone(&inner), shutdown_rx);
        let reader = spawn_reader(stream, Arc::clone(&inner));

        *client.writer.lock().await = Some(writer);
        *client.reader.lock().await = Some(reader);

        Ok(client)
    }

    /// Installs the callback fired for every received message.
    pub fn on_message<F>(&self, handler: F)
    where
        F: Fn(&[u8]) + Send + Sync + 'static,
    {
        if let Ok(mut slot) = self.inner.message_handler.write() {
            *slot = Some(Arc::new(handler));
        }
    }

    /// Installs the callback fired when the connection closes.
    pub fn on_close<F>(&self, handler: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        if let Ok(mut slot) = self.inner.close_handler.write() {
            *slot = Some(Arc::new(handler));
        }
    }

    /// Returns the event router bound to this connection.
    #[must_use]
    pub fn events(&self) -> ClientEvents {
        self.inner.events.clone()
    }

    /// Returns `true` when the connection has been closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    /// Returns `true` while the connection is open.
    #[must_use]
    pub fn is_open(&self) -> bool {
        !self.is_closed()
    }

    /// Queues an encoded message for delivery.
    ///
    /// Returns `false` when the connection is gone or the queue is full, in
    /// which case the message is silently dropped.
    pub fn send(&self, data: Vec<u8>) -> bool {
        if self.is_closed() {
            return false;
        }
        let guard = self.inner.tx.lock();
        let Some(tx) = guard.as_ref() else {
            return false;
        };
        match tx.try_send(data) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                tracing::trace!("client send queue full, dropping packet");
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }
    }

    /// Encodes and queues `event` with `payload`.
    pub fn emit(&self, event: crate::Event, payload: &[u8]) -> bool {
        self.send(crate::pack_message(event.id(), payload))
    }

    /// Returns the outbound queue capacity.
    #[must_use]
    pub fn queue_capacity(&self) -> usize {
        self.inner
            .tx
            .lock()
            .as_ref()
            .map_or(crate::protocol::SEND_QUEUE, mpsc::Sender::max_capacity)
    }

    /// Closes the connection and waits for both tasks to finish.
    pub async fn close(&self) {
        self.begin_close();
        if let Some(handle) = self.reader.lock().await.take() {
            handle.abort();
        }
        if let Some(handle) = self.writer.lock().await.take() {
            handle.abort();
        }
        self.notify_close();
    }

    /// Starts closing the connection without waiting for the tasks.
    pub fn close_now(&self) {
        self.begin_close();
        if let Ok(mut slot) = self.reader.try_lock() {
            if let Some(handle) = slot.take() {
                handle.abort();
            }
        }
        if let Ok(mut slot) = self.writer.try_lock() {
            if let Some(handle) = slot.take() {
                handle.abort();
            }
        }
        self.notify_close();
    }

    /// Resolves once the connection is fully torn down.
    pub async fn closed(&self) {
        let mut rx = self.inner.shutdown.subscribe();
        if *rx.borrow() {
            return;
        }
        let _ = rx.changed().await;
    }

    fn begin_close(&self) {
        if !self.inner.closed.swap(true, Ordering::AcqRel) {
            *self.inner.tx.lock() = None;
            // `send_replace` stores the value even with no receivers, unlike
            // `send` which would silently drop the signal.
            self.inner.shutdown.send_replace(true);
        }
    }

    fn notify_close(&self) {
        let handler = self
            .inner
            .close_handler
            .read()
            .ok()
            .and_then(|slot| slot.clone());
        if let Some(handler) = handler {
            handler();
        }
    }
}

/// Writes queued messages to the socket until the channel closes.
/// Number of messages to coalesce into one socket write.
///
/// `SinkExt::send` flushes after every message, which costs one syscall per
/// packet. Draining the queue first and flushing once turns a burst into a
/// single write.
const WRITE_BATCH: usize = 256;

fn spawn_writer<S>(
    mut sink: futures_util::stream::SplitSink<WebSocketStream<S>, Message>,
    mut rx: mpsc::Receiver<Vec<u8>>,
    state: Arc<ClientInner>,
    mut shutdown: watch::Receiver<bool>,
) -> JoinHandle<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        loop {
            let mut wrote_any = false;

            // Coalesce everything already queued into one flush.
            for _ in 0..WRITE_BATCH {
                match rx.try_recv() {
                    Ok(message) => {
                        if sink.feed(Message::Binary(message)).await.is_err() {
                            break;
                        }
                        wrote_any = true;
                    }
                    Err(_) => break,
                }
            }
            if wrote_any {
                if sink.flush().await.is_err() {
                    break;
                }
                if *shutdown.borrow() {
                    break;
                }
                continue;
            }

            let next = tokio::select! {
                biased;
                _ = shutdown.changed() => None,
                msg = rx.recv() => msg,
            };
            let Some(message) = next else { break };
            if sink.send(Message::Binary(message)).await.is_err() {
                break;
            }
        }

        let _ = sink
            .send(Message::Close(Some(close_frame(
                CloseCode::Normal,
                "client closed",
            ))))
            .await;
        let _ = sink.flush().await;
        state.closed.store(true, Ordering::Release);
    })
}

/// Reads messages and feeds them to the installed handlers.
fn spawn_reader<S>(
    mut stream: futures_util::stream::SplitStream<WebSocketStream<S>>,
    state: Arc<ClientInner>,
) -> JoinHandle<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        while let Some(item) = stream.next().await {
            match item {
                Ok(Message::Binary(data)) => {
                    let payload = data.to_vec();
                    if !state.events.dispatch_checked(&payload) {
                        let handler = state
                            .message_handler
                            .read()
                            .ok()
                            .and_then(|slot| slot.clone());
                        if let Some(handler) = handler {
                            handler(&payload);
                        }
                    }
                }
                Ok(Message::Close(_)) => break,
                Ok(Message::Ping(payload)) => {
                    // tungstenite automatically queues a Pong when a Ping is
                    // read through `StreamExt::next`, so the connection stays
                    // open. Do NOT set `closed` here: doing so would kill a
                    // healthy client whenever the server sends a heartbeat.
                    let _ = payload;
                }
                Ok(Message::Pong(_)) | Ok(Message::Text(_)) | Ok(Message::Frame(_)) => {}
                Err(err) => {
                    tracing::debug!(%err, "client read error");
                    break;
                }
            }
        }

        state.closed.store(true, Ordering::Release);
        *state.tx.lock() = None;
        // Signal `Client::closed()` too, otherwise a server-initiated close
        // would leave it waiting forever.
        state.shutdown.send_replace(true);

        let handler = state
            .close_handler
            .read()
            .ok()
            .and_then(|slot| slot.clone());
        if let Some(handler) = handler {
            handler();
        }
    })
}

fn close_frame(code: CloseCode, reason: &str) -> CloseFrame<'static> {
    CloseFrame {
        code,
        reason: reason.to_string().into(),
    }
}

/// Installs the process-wide rustls crypto provider.
///
/// `tungstenite` builds rustls with `default-features = false`, so no provider
/// is selected automatically and rustls panics on first use. Calling this once
/// (it is idempotent) fixes that. `Client::connect` calls it for you, so you
/// only need it when driving `tokio-rustls` yourself.
///
/// # Errors
///
/// Returns an error when another provider was already installed with a
/// different configuration.
#[cfg(feature = "tls")]
pub fn install_crypto_provider() -> std::result::Result<(), String> {
    use std::sync::OnceLock;

    static RESULT: OnceLock<std::result::Result<(), String>> = OnceLock::new();
    RESULT
        .get_or_init(|| {
            rustls::crypto::ring::default_provider()
                .install_default()
                .map_err(|_| "a rustls crypto provider is already installed".to_string())
        })
        .clone()
}

/// Parses the host and port out of an upgrade request.
fn split_authority(request: &http::Request<()>) -> Result<(String, u16)> {
    let uri = request.uri();
    let host = uri
        .host()
        .ok_or_else(|| Error::InvalidUrl(format!("missing host in `{uri}`")))?;

    let default_port = if uri.scheme_str() == Some("wss") {
        443
    } else {
        80
    };
    let port = uri.port_u16().unwrap_or(default_port);

    // `Uri::host()` keeps the brackets around IPv6 literals.
    Ok((host.to_string(), port))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authority_parsing() {
        let case = |url: &str| -> (String, u16) {
            let req = url
                .parse::<http::Uri>()
                .unwrap()
                .into_client_request()
                .unwrap();
            split_authority(&req).unwrap()
        };

        assert_eq!(
            case("ws://example.com:9000/ws"),
            ("example.com".into(), 9000)
        );
        assert_eq!(case("ws://example.com/ws"), ("example.com".into(), 80));
        assert_eq!(case("wss://example.com/ws"), ("example.com".into(), 443));
        // `Uri::host()` keeps the brackets around IPv6 literals.
        assert_eq!(case("ws://[::1]:8080/ws"), ("[::1]".into(), 8080));
    }

    #[test]
    fn missing_host_is_rejected() {
        let req = http::Request::builder()
            .uri("/ws")
            .header("host", "example.com")
            .body(())
            .unwrap();
        // A request without a scheme/authority has no host to dial.
        assert!(split_authority(&req).is_err());
    }
}
