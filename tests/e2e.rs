//! End-to-end tests over a real TCP socket.
//!
//! These exercise the full path: accept loop, version handshake, event
//! routing, rooms and shutdown.

use std::time::Duration;

use magic_ws::{Client, Event, Server, UserState};

const ECHO: Event = Event::new("echo");
const CHAT: Event = Event::new("chat");

/// Spawns a server on an ephemeral port and returns it with its address.
///
/// The builder is consumed, so tests configure handlers before the server
/// starts accepting.
async fn spawn_server(
    build: impl FnOnce(magic_ws::ServerBuilder) -> magic_ws::ServerBuilder,
) -> (Server, std::net::SocketAddr) {
    let server = build(Server::builder()).build();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let task_server = server.clone();
    tokio::spawn(async move {
        let _ = task_server.serve(listener).await;
    });

    // Let the accept loop start.
    tokio::time::sleep(Duration::from_millis(50)).await;
    (server, addr)
}

#[tokio::test]
async fn echo_roundtrip() {
    let (server, addr) = spawn_server(|builder| {
        builder.on_event(ECHO, |user, payload| {
            user.send_event(ECHO, payload);
        })
    })
    .await;

    let client = Client::connect(&format!("ws://{addr}/ws")).await.unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let tx = std::sync::Mutex::new(Some(tx));

    let events = client.events();
    events.on(ECHO, move |payload| {
        if let Some(tx) = tx.lock().unwrap().take() {
            let _ = tx.send(payload.to_vec());
        }
    });

    assert!(client.emit(ECHO, b"ping"));

    let reply = tokio::time::timeout(Duration::from_secs(5), rx)
        .await
        .expect("reply arrives")
        .expect("sender alive");
    assert_eq!(reply, b"ping");

    client.close().await;
    server.shutdown();
}

#[tokio::test]
async fn user_is_registered_then_removed() {
    let (server, addr) = spawn_server(|builder| builder).await;

    assert_eq!(server.user_count(), 0);

    let client = Client::connect(&format!("ws://{addr}/ws")).await.unwrap();

    // The connection is registered once the version handshake passes.
    let mut deadline = 20;
    while server.user_count() == 0 && deadline > 0 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        deadline -= 1;
    }
    assert_eq!(server.user_count(), 1);

    let user = server.users().users().remove(0);
    assert_eq!(user.state(), UserState::Connected);
    assert_eq!(user.room(), None);

    client.close().await;

    // Removal happens once the read loop observes the close.
    let mut deadline = 50;
    while server.user_count() != 0 && deadline > 0 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        deadline -= 1;
    }
    assert_eq!(server.user_count(), 0, "user removed after disconnect");

    server.shutdown();
}

#[tokio::test]
async fn room_fanout_reaches_every_member() {
    let (server, addr) = spawn_server(|builder| builder).await;

    // Fan out from a handler registered after construction, where the handle
    // is available.
    let fanout = server.clone();
    server.on_event(CHAT, move |user, payload| {
        if let Some(room) = user.room() {
            fanout.emit_to_room(room, CHAT, payload);
        }
    });

    let a = Client::connect(&format!("ws://{addr}/ws")).await.unwrap();
    let b = Client::connect(&format!("ws://{addr}/ws")).await.unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    for client in [&a, &b] {
        let tx = tx.clone();
        let events = client.events();
        events.on(CHAT, move |payload| {
            let _ = tx.send(payload.to_vec());
        });
    }

    // Wait until both clients are registered, then move them into room 7.
    let mut deadline = 50;
    while server.user_count() < 2 && deadline > 0 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        deadline -= 1;
    }
    assert_eq!(server.user_count(), 2);

    for user in server.users().users() {
        server.set_user_room(&user, 7);
    }
    assert_eq!(server.users().room_len(7), 2);

    assert!(a.emit(CHAT, b"hello room"));

    let first = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("first reply")
        .expect("channel alive");
    let second = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("second reply")
        .expect("channel alive");
    assert_eq!(first, b"hello room");
    assert_eq!(second, b"hello room");

    a.close().await;
    b.close().await;
    server.shutdown();
}

#[tokio::test]
async fn bad_version_handshake_is_rejected() {
    let (server, addr) =
        spawn_server(|builder| builder.on_connect(|_user| panic!("must not connect"))).await;

    // Speak raw WebSocket: complete the upgrade, then send the wrong version.
    let (mut socket, _) = tokio_tungstenite::client_async(
        format!("ws://{addr}/ws").parse::<http::Uri>().unwrap(),
        tokio::net::TcpStream::connect(addr).await.unwrap(),
    )
    .await
    .unwrap();

    use futures_util::SinkExt;
    use tokio_tungstenite::tungstenite::Message;

    socket.send(Message::Binary(vec![2])).await.unwrap();

    // The server closes the connection right after the bad handshake.
    let mut saw_close = false;
    while let Ok(Some(Ok(msg))) = tokio::time::timeout(
        Duration::from_secs(5),
        futures_util::StreamExt::next(&mut socket),
    )
    .await
    {
        if matches!(msg, Message::Close(_)) {
            saw_close = true;
            break;
        }
    }
    assert!(saw_close, "server closed the connection");
    assert_eq!(server.user_count(), 0);

    server.shutdown();
}

#[tokio::test]
async fn shutdown_disconnects_everyone() {
    let (server, addr) = spawn_server(|builder| builder.on_connect(|_user| {})).await;

    let client = Client::connect(&format!("ws://{addr}/ws")).await.unwrap();

    let mut deadline = 50;
    while server.user_count() == 0 && deadline > 0 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        deadline -= 1;
    }
    assert_eq!(server.user_count(), 1);

    server.shutdown();
    client.closed().await;

    assert!(client.is_closed());
}

#[tokio::test]
async fn events_beyond_the_prefix_are_routed() {
    let (server, addr) = spawn_server(|builder| {
        builder.on_event(ECHO, |user, payload| {
            // Verify the payload excludes the 4-byte event id.
            assert_eq!(payload, b"payload");
            user.send_event(ECHO, payload);
        })
    })
    .await;

    let client = Client::connect(&format!("ws://{addr}/ws")).await.unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let tx = std::sync::Mutex::new(Some(tx));

    let events = client.events();
    events.on(ECHO, move |payload| {
        if let Some(tx) = tx.lock().unwrap().take() {
            let _ = tx.send(payload.to_vec());
        }
    });

    assert!(client.emit(ECHO, b"payload"));
    let reply = tokio::time::timeout(Duration::from_secs(5), rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply, b"payload");

    client.close().await;
    server.shutdown();
}

#[tokio::test]
async fn heartbeat_disconnects_silent_client() {
    use magic_ws::HeartbeatConfig;

    // A heartbeat with a very short timeout so the test stays fast.
    let (server, addr) = spawn_server(|builder| {
        builder.heartbeat(HeartbeatConfig {
            interval: Duration::from_millis(30),
            timeout: Duration::from_millis(200),
            max_missed: 3,
        })
    })
    .await;

    // Raw client that completes the upgrade and the version handshake but then
    // never reads. tungstenite queues an automatic Pong when it reads a Ping,
    // but this client never calls `next()`, so no Pong is ever written back.
    let (socket, _) = tokio_tungstenite::client_async(
        format!("ws://{addr}/ws").parse::<http::Uri>().unwrap(),
        tokio::net::TcpStream::connect(addr).await.unwrap(),
    )
    .await
    .unwrap();

    use futures_util::SinkExt;
    use tokio_tungstenite::tungstenite::Message;

    let mut socket = socket;
    socket
        .send(Message::Binary(vec![magic_ws::PROTOCOL_VERSION]))
        .await
        .unwrap();

    // Wait until the server has registered us.
    let mut deadline = 50;
    while server.user_count() == 0 && deadline > 0 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        deadline -= 1;
    }
    assert_eq!(server.user_count(), 1, "user registered");

    // Never read from the socket: the server's pings go unanswered, so after
    // `max_missed` the user should be reaped from the registry.
    let mut deadline = 100;
    while server.user_count() != 0 && deadline > 0 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        deadline -= 1;
    }
    assert_eq!(
        server.user_count(),
        0,
        "silent client should be disconnected by the heartbeat"
    );

    // Keep the socket alive until the end so the OS does not reset the
    // connection before the heartbeat fires.
    drop(socket);
    server.shutdown();
}

#[tokio::test]
async fn rtt_is_measured_after_pong() {
    use magic_ws::HeartbeatConfig;

    let (server, addr) = spawn_server(|builder| {
        builder.heartbeat(HeartbeatConfig {
            interval: Duration::from_millis(30),
            timeout: Duration::from_secs(1),
            max_missed: 3,
        })
    })
    .await;

    // Connect the Rust client, which answers Pings automatically (after INF-11).
    let client = Client::connect(&format!("ws://{addr}/ws")).await.unwrap();

    // Give the heartbeat time to fire a ping and receive the pong.
    let mut observed_rtt = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(25)).await;
        if server
            .users()
            .users()
            .into_iter()
            .any(|u| u.rtt().is_some())
        {
            observed_rtt = true;
            break;
        }
    }
    assert!(
        observed_rtt,
        "server should measure an RTT from the client's pong"
    );
    // The client must stay open even though it received Pings.
    assert!(!client.is_closed(), "client must survive server pings");

    client.close().await;
    server.shutdown();
}

#[tokio::test]
async fn client_survives_server_ping() {
    use magic_ws::HeartbeatConfig;

    let (server, addr) = spawn_server(|builder| {
        builder.heartbeat(HeartbeatConfig {
            interval: Duration::from_millis(20),
            timeout: Duration::from_secs(2),
            max_missed: 3,
        })
    })
    .await;

    let client = Client::connect(&format!("ws://{addr}/ws")).await.unwrap();

    // Wait through several ping intervals; the client must not close.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        !client.is_closed(),
        "receiving a Ping must not close the client"
    );
    assert!(client.is_open());

    client.close().await;
    server.shutdown();
}

#[tokio::test]
async fn zero_connection_limit_means_unlimited() {
    // `max_connections(0)` must behave like no limit at all.
    let (server, addr) =
        spawn_server(|builder| builder.max_connections(0).on_connect(|_| {})).await;

    let client = Client::connect(&format!("ws://{addr}/ws")).await.unwrap();

    // The user must be registered, not rejected.
    let mut deadline = 50;
    while server.user_count() == 0 && deadline > 0 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        deadline -= 1;
    }
    assert_eq!(server.user_count(), 1, "zero limit must not reject");

    client.close().await;
    server.shutdown();
}

#[tokio::test]
async fn graceful_shutdown_flushes_queued_messages() {
    let (server, addr) = spawn_server(|builder| builder).await;

    let client = Client::connect(&format!("ws://{addr}/ws")).await.unwrap();

    // Wait for registration.
    let mut deadline = 50;
    while server.user_count() == 0 && deadline > 0 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        deadline -= 1;
    }
    assert_eq!(server.user_count(), 1);

    // Queue a message for the user, then start a graceful shutdown.
    let user = server.users().users().remove(0);
    let events = client.events();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let tx = std::sync::Mutex::new(Some(tx));
    events.on(ECHO, move |payload| {
        if let Some(tx) = tx.lock().unwrap().take() {
            let _ = tx.send(payload.to_vec());
        }
    });

    assert!(server.emit(&user, ECHO, b"bye"));

    // The message should still arrive even though we shut down right after.
    let shutdown = tokio::spawn({
        let server = server.clone();
        async move { server.shutdown_graceful(Duration::from_secs(2)).await }
    });

    let reply = tokio::time::timeout(Duration::from_secs(3), rx)
        .await
        .expect("queued message should arrive before the connection closes")
        .expect("sender alive");
    assert_eq!(reply, b"bye");

    let _ = shutdown.await;
}

#[tokio::test]
async fn on_shutdown_fires_once() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let (server, _addr) = spawn_server(|builder| builder).await;
    let counter = Arc::new(AtomicUsize::new(0));
    let c = Arc::clone(&counter);
    server.on_shutdown(move || {
        c.fetch_add(1, Ordering::SeqCst);
    });

    server.shutdown_graceful(Duration::from_millis(100)).await;
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[cfg(feature = "tls")]
#[tokio::test]
async fn tls_listener_roundtrip() {
    use magic_ws::{Client, TlsConfig};
    use std::sync::Arc;
    use tokio_rustls::rustls::{ClientConfig, RootCertStore};

    let cert = std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/examples/certs/cert.pem"
    ))
    .expect("cert.pem");
    let key = std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/examples/certs/key.pem"
    ))
    .expect("key.pem");

    let server = Server::builder()
        .on_event(ECHO, |user, payload| {
            user.send_event(ECHO, payload);
        })
        .build();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let task_server = server.clone();
    let tls_cfg = TlsConfig {
        cert: cert.clone(),
        key,
    };
    tokio::spawn(async move {
        let _ = task_server
            .listen_tls(format!("127.0.0.1:{}", addr.port()), tls_cfg)
            .await;
    });

    // Give the listener time to start.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Trust the self-signed cert.
    let mut roots = RootCertStore::empty();
    let mut certs = std::io::Cursor::new(cert);
    for cert in rustls_pemfile::certs(&mut certs) {
        roots.add(cert.unwrap()).unwrap();
    }
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    let client = Client::connect_with_tls_config(
        &format!("wss://127.0.0.1:{}/ws", addr.port()),
        Duration::from_secs(5),
        Arc::new(config),
    )
    .await
    .expect("tls connect");

    let (tx, rx) = tokio::sync::oneshot::channel();
    let tx = std::sync::Mutex::new(Some(tx));
    let events = client.events();
    events.on(ECHO, move |payload| {
        if let Some(tx) = tx.lock().unwrap().take() {
            let _ = tx.send(payload.to_vec());
        }
    });

    client.emit(ECHO, b"tls hello");
    let reply = tokio::time::timeout(Duration::from_secs(5), rx)
        .await
        .expect("reply over tls")
        .expect("sender alive");
    assert_eq!(reply, b"tls hello");

    client.close().await;
    server.shutdown();
}

#[cfg(feature = "tls")]
#[tokio::test]
async fn tls_listener_rejects_invalid_pem() {
    use magic_ws::TlsConfig;

    let server = Server::builder().build();
    let result = server
        .listen_tls(
            "127.0.0.1:0",
            TlsConfig {
                cert: b"not a cert".to_vec(),
                key: b"not a key".to_vec(),
            },
        )
        .await;
    assert!(result.is_err(), "invalid PEM should fail");
}

#[tokio::test]
async fn metrics_counters_increase() {
    let (server, addr) = spawn_server(|builder| {
        builder.on_event(ECHO, |user, payload| {
            user.send_event(ECHO, payload);
        })
    })
    .await;

    let client = Client::connect(&format!("ws://{addr}/ws")).await.unwrap();

    let mut deadline = 50;
    while server.user_count() == 0 && deadline > 0 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        deadline -= 1;
    }

    // Send a few messages and let the echo come back.
    let events = client.events();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    events.on(ECHO, move |_payload| {
        let _ = tx.send(());
    });
    for _ in 0..5 {
        client.emit(ECHO, b"ping");
    }
    for _ in 0..5 {
        let _ = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;
    }

    let m = server.metrics();
    assert!(
        m.messages_received >= 5,
        "received >= 5: {}",
        m.messages_received
    );
    assert!(m.messages_sent >= 5, "sent >= 5: {}", m.messages_sent);
    assert!(m.bytes_received > 0);
    assert!(m.bytes_sent > 0);
    assert!(m.connections >= 1);
    assert!(m.users >= 1);

    client.close().await;
    server.shutdown();
}

#[tokio::test]
async fn per_ip_limit_rejects_third_connection() {
    // 2 connections from localhost are allowed, the 3rd is rejected.
    let (server, addr) = spawn_server(|builder| builder.max_connections_per_ip(2)).await;

    let c1 = Client::connect(&format!("ws://{addr}/ws")).await.unwrap();
    let c2 = Client::connect(&format!("ws://{addr}/ws")).await.unwrap();

    let mut deadline = 50;
    while server.user_count() < 2 && deadline > 0 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        deadline -= 1;
    }
    assert_eq!(server.user_count(), 2, "two connections admitted");

    // Third is rejected: the server drops the TCP stream before the WS
    // handshake, so the client connect fails.
    let c3 = Client::connect(&format!("ws://{addr}/ws")).await;
    assert!(c3.is_err(), "third connection should be rejected");

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(server.user_count(), 2, "still only two users");

    c1.close().await;
    c2.close().await;
    server.shutdown();
}

#[tokio::test]
async fn ban_ip_disconnects_and_blocks() {
    let (server, addr) = spawn_server(|builder| builder).await;

    let client = Client::connect(&format!("ws://{addr}/ws")).await.unwrap();

    let mut deadline = 50;
    while server.user_count() == 0 && deadline > 0 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        deadline -= 1;
    }
    assert_eq!(server.user_count(), 1);

    let loopback: std::net::IpAddr = "127.0.0.1".parse().unwrap();
    assert!(!server.is_banned(loopback));

    // Ban localhost: the connected user should be dropped.
    server.ban_ip(loopback);
    assert!(server.is_banned(loopback));

    tokio::time::timeout(Duration::from_secs(2), client.closed())
        .await
        .expect("client should be disconnected by the ban");

    // A new connection from the banned IP is rejected.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let c2 = Client::connect(&format!("ws://{addr}/ws")).await;
    assert!(c2.is_err(), "banned ip cannot connect");

    // Unban allows it again.
    server.unban_ip(loopback);
    assert!(!server.is_banned(loopback));
    let c3 = Client::connect(&format!("ws://{addr}/ws")).await;
    assert!(c3.is_ok(), "after unban the ip can connect");
    if let Ok(c3) = c3 {
        c3.close().await;
    }

    server.shutdown();
}

#[tokio::test]
async fn rate_limit_drops_overflow() {
    use magic_ws::{RateLimit, ViolationPolicy};

    let (server, addr) = spawn_server(|builder| {
        builder
            .on_event(ECHO, |user, payload| {
                user.send_event(ECHO, payload);
            })
            .rate_limit(RateLimit {
                messages_per_second: 60,
                burst: 10,
                on_violation: ViolationPolicy::Drop,
            })
    })
    .await;

    let client = Client::connect(&format!("ws://{addr}/ws")).await.unwrap();

    // Register the reply handler before sending so we count every echo.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let events = client.events();
    events.on(ECHO, move |_payload| {
        let _ = tx.send(());
    });

    // Wait for registration.
    let mut deadline = 50;
    while server.user_count() == 0 && deadline > 0 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        deadline -= 1;
    }

    // Burst 1000 messages as fast as possible; only ~burst (10) should get
    // through, the rest are dropped by the token bucket.
    for _ in 0..1000 {
        client.emit(ECHO, b"x");
    }

    // Count replies within a short window.
    let mut received = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(50), rx.recv()).await {
            Ok(Some(())) => received += 1,
            _ => break,
        }
    }
    assert!(
        received <= 20,
        "rate limit should drop most of the burst, got {received}"
    );

    client.close().await;
    server.shutdown();
}

#[tokio::test]
async fn rate_limit_disconnects_after_violations() {
    use magic_ws::{RateLimit, ViolationPolicy};

    let (server, addr) = spawn_server(|builder| {
        builder.rate_limit(RateLimit {
            messages_per_second: 60,
            burst: 5,
            on_violation: ViolationPolicy::DisconnectAfter(3),
        })
    })
    .await;

    let client = Client::connect(&format!("ws://{addr}/ws")).await.unwrap();

    let mut deadline = 50;
    while server.user_count() == 0 && deadline > 0 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        deadline -= 1;
    }

    // Flood past the burst; after 3 violations the server closes us.
    for _ in 0..200 {
        client.emit(ECHO, b"x");
    }

    // The client should observe the close within a reasonable window.
    tokio::time::timeout(Duration::from_secs(3), client.closed())
        .await
        .expect("client should be closed by the rate limiter");
    assert!(client.is_closed());

    server.shutdown();
}

#[tokio::test]
async fn close_with_custom_code_reaches_client() {
    let (server, addr) = spawn_server(|builder| {
        builder.on_connect(move |user| {
            // Disconnect immediately with a custom code so the test is fast.
            user.close_with(4001, "auth failed");
        })
    })
    .await;

    // Raw client so we can inspect the close code.
    let (socket, _) = tokio_tungstenite::client_async(
        format!("ws://{addr}/ws").parse::<http::Uri>().unwrap(),
        tokio::net::TcpStream::connect(addr).await.unwrap(),
    )
    .await
    .unwrap();

    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let mut socket = socket;
    socket
        .send(Message::Binary(vec![magic_ws::PROTOCOL_VERSION]))
        .await
        .unwrap();

    // Expect a Close frame with code 4001.
    let mut code = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), socket.next()).await {
            Ok(Some(Ok(Message::Close(Some(frame))))) => {
                code = Some(frame.code);
                break;
            }
            Ok(Some(Ok(_))) => {}
            _ => break,
        }
    }
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
    assert_eq!(code, Some(CloseCode::from(4001)));

    server.shutdown();
}
