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

    client.close().await;
    server.shutdown();
}
