//! TLS server + client round-trip.
//!
//! Serves `wss://` on `127.0.0.1:8443` using the self-signed certificate in
//! `certs/`, then connects a client to it.
//!
//! ```bash
//! cargo run --example tls_test
//! ```

use std::sync::Arc;

use magic_ws::{Client, Event, Server};
use tokio::net::TcpListener;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

const ECHO: Event = Event::new("ping_pong");

/// Self-signed certificate used by the example.
mod certs {
    /// PEM-encoded private key (PKCS#8, ECDSA P-256).
    pub const KEY_PEM: &str = include_str!("certs/key.pem");
    /// PEM-encoded self-signed certificate for `localhost`.
    pub const CERT_PEM: &str = include_str!("certs/cert.pem");
}

fn server_config() -> Arc<ServerConfig> {
    // The server terminates TLS itself, so install the provider here too.
    magic_ws::install_crypto_provider().expect("crypto provider");

    let key = rustls_pemfile::pkcs8_private_keys(&mut certs::KEY_PEM.as_bytes())
        .next()
        .expect("readable key PEM")
        .expect("one key");
    let certs: Vec<_> = rustls_pemfile::certs(&mut certs::CERT_PEM.as_bytes())
        .collect::<std::result::Result<_, _>>()
        .expect("readable certificate PEM");

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key.into())
        .expect("usable certificate");

    Arc::new(config)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let acceptor = TlsAcceptor::from(server_config());
    let listener = TcpListener::bind("127.0.0.1:8443").await?;
    println!("wss server listening on 127.0.0.1:8443");

    let server = Arc::new(
        Server::builder()
            .on_event(ECHO, |user, payload| {
                println!("[server] {} bytes from {}", payload.len(), user.id());
                user.send_event(ECHO, b"PONG_SECURE");
            })
            .build(),
    );

    // Accept loop: terminate TLS, then hand the stream to MagicWS.
    let task = {
        let server = Arc::clone(&server);
        tokio::spawn(async move {
            loop {
                let Ok((stream, addr)) = listener.accept().await else {
                    break;
                };
                let acceptor = acceptor.clone();
                let server = Arc::clone(&server);
                tokio::spawn(async move {
                    let tls = match acceptor.accept(stream).await {
                        Ok(tls) => tls,
                        Err(err) => {
                            tracing::debug!(%addr, %err, "tls handshake failed");
                            return;
                        }
                    };
                    if let Err(err) = server.accept_stream(tls, addr).await {
                        tracing::debug!(%addr, %err, "connection ended");
                    }
                });
            }
        })
    };

    // Give the server a moment, then connect.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Trust the self-signed certificate instead of the webpki roots.
    let cert = rustls_pemfile::certs(&mut certs::CERT_PEM.as_bytes())
        .next()
        .expect("readable certificate PEM")
        .expect("one certificate");

    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    roots.add(cert).expect("valid root certificate");

    let tls_config = tokio_rustls::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    let client = Client::connect_with_tls_config(
        "wss://127.0.0.1:8443/ws",
        std::time::Duration::from_secs(5),
        std::sync::Arc::new(tls_config),
    )
    .await?;
    println!("[client] connected over wss");

    let (tx, rx) = tokio::sync::oneshot::channel();
    let tx = std::sync::Mutex::new(Some(tx));

    let events = client.events();
    events.on(ECHO, move |payload| {
        if let Some(tx) = tx.lock().unwrap().take() {
            let _ = tx.send(payload.to_vec());
        }
    });

    client.emit(ECHO, b"PING_SECURE");

    match tokio::time::timeout(std::time::Duration::from_secs(5), rx).await {
        Ok(Ok(reply)) => println!("[client] reply: {}", String::from_utf8_lossy(&reply)),
        _ => println!("[client] no reply"),
    }

    client.close().await;
    task.abort();
    Ok(())
}
