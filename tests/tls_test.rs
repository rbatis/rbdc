//! End-to-end tests for the TLS wrapper using a local, in-process TLS server.
//!
//! The certificate/key pair in `tests/fixtures` is a throwaway self-signed
//! pair generated solely for these tests.

use rbdc::net::{CertificateInput, MaybeTlsStream};
use rustls::pki_types::CertificateDer;
use std::io::Cursor;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;

const CA_PEM: &str = include_str!("fixtures/test_ca.pem");
const SERVER_CERT_PEM: &str = include_str!("fixtures/test_server_cert.pem");
const SERVER_KEY_PEM: &str = include_str!("fixtures/test_server_key.pem");

fn acceptor() -> TlsAcceptor {
    let certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut Cursor::new(SERVER_CERT_PEM.as_bytes()))
            .collect::<Result<_, _>>()
            .unwrap();
    let key = rustls_pemfile::private_key(&mut Cursor::new(SERVER_KEY_PEM.as_bytes()))
        .unwrap()
        .unwrap();
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .unwrap();
    TlsAcceptor::from(Arc::new(config))
}

async fn spawn_server() -> (u16, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let acceptor = acceptor();
    let handle = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut tls = acceptor.accept(tcp).await.unwrap();
        let mut buf = [0u8; 4];
        tls.read_exact(&mut buf).await.unwrap();
        tls.write_all(b"pong").await.unwrap();
        tls.flush().await.unwrap();
        tls.shutdown().await.unwrap();
    });
    (port, handle)
}

async fn assert_echo(stream: &mut MaybeTlsStream<TcpStream>) {
    stream.write_all(b"ping").await.unwrap();
    stream.flush().await.unwrap();
    let mut buf = [0u8; 4];
    stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"pong");
    stream.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_tls_upgrade_accept_invalid_certs() {
    let (port, server) = spawn_server().await;
    let tcp = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut stream = MaybeTlsStream::Raw(tcp);

    stream.upgrade("localhost", true, false, None).await.unwrap();
    assert!(stream.is_tls());
    // Deref / DerefMut to the underlying stream for the TLS variant.
    let _ = &*stream;
    let _ = &mut *stream;
    assert_echo(&mut stream).await;

    server.await.unwrap();
}

#[tokio::test]
async fn test_tls_upgrade_with_custom_root_and_wrong_hostname() {
    let (port, server) = spawn_server().await;
    let tcp = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut stream = MaybeTlsStream::Raw(tcp);

    let ca: CertificateInput = CA_PEM.to_string().into();
    // The certificate is trusted via the custom root, so only the hostname
    // mismatch is handled by NoHostnameTlsVerifier.
    stream
        .upgrade("wrong.example", false, true, Some(&ca))
        .await
        .unwrap();
    assert!(stream.is_tls());
    assert_echo(&mut stream).await;

    server.await.unwrap();
}

#[tokio::test]
async fn test_tls_upgrade_twice_is_idempotent() {
    let (port, server) = spawn_server().await;
    let tcp = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut stream = MaybeTlsStream::Raw(tcp);

    stream.upgrade("localhost", true, false, None).await.unwrap();
    // Upgrading an already-TLS stream is a no-op that preserves the stream.
    stream.upgrade("localhost", true, false, None).await.unwrap();
    assert!(stream.is_tls());
    assert_echo(&mut stream).await;

    server.await.unwrap();
}

#[tokio::test]
async fn test_upgrade_from_upgrading_state_errors() {
    let (_client, _server) = tokio::io::duplex(64);
    let mut stream: MaybeTlsStream<tokio::io::DuplexStream> = MaybeTlsStream::Upgrading;
    let err = stream
        .upgrade("localhost", true, false, None)
        .await
        .unwrap_err();
    assert!(!err.to_string().is_empty());
}
