use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use oto_testkit::{
    FakeVoiceGateway, FakeVoiceGatewayConfig, GatewayCommandError, GatewayRecord, TestTls,
    VoiceClose,
};
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, RootCertStore};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{WebSocketStream, client_async};

type TestClient = WebSocketStream<TlsStream<TcpStream>>;

async fn connect(
    gateway: &FakeVoiceGateway,
    config: Arc<ClientConfig>,
) -> Result<TestClient, Box<dyn std::error::Error>> {
    let tcp = TcpStream::connect(gateway.local_addr()).await?;
    let tls = TlsConnector::from(config)
        .connect(ServerName::try_from("localhost")?, tcp)
        .await?;
    let (websocket, _) = client_async(gateway.url(), tls).await?;
    Ok(websocket)
}

#[tokio::test]
async fn multiple_gateways_can_share_one_injected_test_root() {
    let tls = TestTls::generate().unwrap();
    let first = FakeVoiceGateway::start_with_tls(FakeVoiceGatewayConfig::local(), tls.clone())
        .await
        .unwrap();
    let second = FakeVoiceGateway::start_with_tls(FakeVoiceGatewayConfig::local(), tls.clone())
        .await
        .unwrap();

    let mut first_client = connect(&first, tls.client_config()).await.unwrap();
    let mut second_client = connect(&second, tls.client_config()).await.unwrap();
    assert_eq!(next_json(&mut first_client).await["op"], 8);
    assert_eq!(next_json(&mut second_client).await["op"], 8);

    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
}

async fn next_json(client: &mut TestClient) -> Value {
    let message = client.next().await.expect("message").expect("valid frame");
    let Message::Text(text) = message else {
        panic!("expected JSON text, got {message:?}");
    };
    serde_json::from_str(&text).expect("valid JSON")
}

#[tokio::test]
async fn tls_gateway_flow_buffered_resume_wrap_speaking_and_close_are_deterministic() {
    let mut config = FakeVoiceGatewayConfig::local();
    config.sequence_start = u16::MAX - 1;
    let gateway = FakeVoiceGateway::start(config).await.unwrap();

    let untrusted = Arc::new(
        ClientConfig::builder()
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth(),
    );
    assert!(connect(&gateway, untrusted).await.is_err());

    let mut client = connect(&gateway, gateway.tls().client_config())
        .await
        .unwrap();
    assert_eq!(next_json(&mut client).await["op"], 8);

    client
        .send(Message::Text(
            json!({
                "op": 0,
                "d": {
                    "server_id": "1",
                    "user_id": "2",
                    "session_id": "session",
                    "token": "local-test-token",
                    "max_dave_protocol_version": 0
                }
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
    let ready = next_json(&mut client).await;
    assert_eq!(ready["op"], 2);
    assert_eq!(ready["seq"], u16::MAX - 1);

    client
        .send(Message::Text(
            json!({"op": 3, "d": {"t": 1234, "seq_ack": u16::MAX - 1}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let heartbeat_ack = next_json(&mut client).await;
    assert_eq!(heartbeat_ack, json!({"op": 6, "d": {"t": 1234}}));

    client
        .send(Message::Text(
            json!({
                "op": 1,
                "d": {
                    "protocol": "udp",
                    "data": {
                        "address": "203.0.113.10",
                        "port": 50000,
                        "mode": "aead_aes256_gcm_rtpsize"
                    }
                }
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
    let session = next_json(&mut client).await;
    assert_eq!(session["op"], 4);
    assert_eq!(session["seq"], u16::MAX);

    client
        .send(Message::Text(
            json!({"op": 5, "d": {"speaking": 1, "delay": 0, "ssrc": 7}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();

    gateway
        .try_dispatch_json(11, json!({"user_ids": ["3"]}), true)
        .unwrap();
    let wrapped_json = next_json(&mut client).await;
    assert_eq!(wrapped_json["seq"], 0);

    gateway.try_dispatch_binary(25, vec![0xaa, 0xbb]).unwrap();
    let binary = client.next().await.unwrap().unwrap();
    let Message::Binary(binary) = binary else {
        panic!("expected binary message");
    };
    assert_eq!(&binary[..3], &[0, 1, 25]);
    assert_eq!(&binary[3..], &[0xaa, 0xbb]);

    client.close(None).await.unwrap();
    tokio::task::yield_now().await;

    let mut resumed = connect(&gateway, gateway.tls().client_config())
        .await
        .unwrap();
    assert_eq!(next_json(&mut resumed).await["op"], 8);
    resumed
        .send(Message::Text(
            json!({
                "op": 7,
                "d": {
                    "server_id": "1",
                    "session_id": "session",
                    "token": "local-test-token",
                    "seq_ack": u16::MAX
                }
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();

    let replayed_json = next_json(&mut resumed).await;
    assert_eq!(replayed_json["seq"], 0);
    let replayed_binary = resumed.next().await.unwrap().unwrap();
    assert!(matches!(replayed_binary, Message::Binary(_)));
    assert_eq!(next_json(&mut resumed).await["op"], 9);

    gateway
        .try_close(VoiceClose {
            code: 4015,
            reason: "voice server crashed".to_owned(),
        })
        .unwrap();
    let close = resumed.next().await.unwrap().unwrap();
    let Message::Close(Some(close)) = close else {
        panic!("expected close frame, got {close:?}");
    };
    assert_eq!(u16::from(close.code), 4015);

    let records = gateway.records();
    assert!(records.iter().any(|record| matches!(
        record,
        GatewayRecord::Heartbeat {
            nonce,
            seq_ack: Some(65534)
        } if nonce == &json!(1234)
    )));
    assert!(
        records
            .iter()
            .any(|record| matches!(record, GatewayRecord::Resume { seq_ack: 65535 }))
    );
    assert_eq!(gateway.speaking().len(), 1);

    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_messages_fail_closed_and_server_accepts_a_fresh_connection() {
    let gateway = FakeVoiceGateway::start(FakeVoiceGatewayConfig::local())
        .await
        .unwrap();
    let mut client = connect(&gateway, gateway.tls().client_config())
        .await
        .unwrap();
    assert_eq!(next_json(&mut client).await["op"], 8);
    client.send(Message::Text("{".into())).await.unwrap();
    let close = client.next().await.unwrap().unwrap();
    assert!(matches!(close, Message::Close(Some(_))));

    let mut second = connect(&gateway, gateway.tls().client_config())
        .await
        .unwrap();
    assert_eq!(next_json(&mut second).await["op"], 8);
    second
        .send(Message::Binary(Vec::new().into()))
        .await
        .unwrap();
    let close = second.next().await.unwrap().unwrap();
    assert!(matches!(close, Message::Close(Some(_))));

    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn command_and_message_bounds_have_explicit_full_policies() {
    let mut config = FakeVoiceGatewayConfig::local();
    config.command_capacity = 1;
    let maximum = config.max_message_bytes;
    let gateway = FakeVoiceGateway::start(config).await.unwrap();

    gateway
        .try_dispatch_binary(25, vec![0; maximum - 3])
        .unwrap();
    assert_eq!(
        gateway.try_dispatch_binary(25, vec![0; 1]),
        Err(GatewayCommandError::Full { capacity: 1 })
    );
    assert_eq!(
        gateway.try_dispatch_binary(25, vec![0; maximum - 2]),
        Err(GatewayCommandError::MessageTooLarge {
            actual: maximum + 1,
            maximum,
        })
    );

    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn oversized_input_missing_replay_and_handshake_shutdown_are_bounded() {
    let mut config = FakeVoiceGatewayConfig::local();
    config.max_message_bytes = 128;
    let gateway = FakeVoiceGateway::start(config).await.unwrap();
    let mut client = connect(&gateway, gateway.tls().client_config())
        .await
        .unwrap();
    assert_eq!(next_json(&mut client).await["op"], 8);
    client
        .send(Message::Binary(vec![0; 129].into()))
        .await
        .unwrap();
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(1), client.next())
        .await
        .expect("bounded disconnect after oversized input");
    assert!(!matches!(
        outcome,
        Some(Ok(Message::Text(_) | Message::Binary(_)))
    ));

    let mut resumed = connect(&gateway, gateway.tls().client_config())
        .await
        .unwrap();
    assert_eq!(next_json(&mut resumed).await["op"], 8);
    resumed
        .send(Message::Text(
            json!({"op": 7, "d": {"seq_ack": 123}}).to_string().into(),
        ))
        .await
        .unwrap();
    let close = resumed.next().await.unwrap().unwrap();
    let Message::Close(Some(close)) = close else {
        panic!("expected invalid-session close");
    };
    assert_eq!(u16::from(close.code), 4006);
    gateway.shutdown().await.unwrap();

    let stalled = FakeVoiceGateway::start(FakeVoiceGatewayConfig::local())
        .await
        .unwrap();
    let _raw_tcp = TcpStream::connect(stalled.local_addr()).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), stalled.shutdown())
        .await
        .expect("shutdown interrupts TLS handshake")
        .unwrap();
}

#[tokio::test]
async fn replay_history_evicts_oldest_and_rejects_an_unavailable_ack() {
    let mut config = FakeVoiceGatewayConfig::local();
    config.replay_capacity = 2;
    config.sequence_start = 10;
    let gateway = FakeVoiceGateway::start(config).await.unwrap();
    let mut client = connect(&gateway, gateway.tls().client_config())
        .await
        .unwrap();
    assert_eq!(next_json(&mut client).await["op"], 8);
    client
        .send(Message::Text(json!({"op": 0, "d": {}}).to_string().into()))
        .await
        .unwrap();
    assert_eq!(next_json(&mut client).await["seq"], 10);
    client
        .send(Message::Text(
            json!({
                "op": 1,
                "d": {
                    "protocol": "udp",
                    "data": {
                        "address": "203.0.113.10",
                        "port": 50000,
                        "mode": "aead_aes256_gcm_rtpsize"
                    }
                }
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
    assert_eq!(next_json(&mut client).await["seq"], 11);
    gateway
        .try_dispatch_json(11, json!({"user_ids": []}), true)
        .unwrap();
    assert_eq!(next_json(&mut client).await["seq"], 12);
    client.close(None).await.unwrap();
    tokio::task::yield_now().await;

    let mut resumed = connect(&gateway, gateway.tls().client_config())
        .await
        .unwrap();
    assert_eq!(next_json(&mut resumed).await["op"], 8);
    resumed
        .send(Message::Text(
            json!({"op": 7, "d": {"seq_ack": 10}}).to_string().into(),
        ))
        .await
        .unwrap();
    let close = resumed.next().await.unwrap().unwrap();
    let Message::Close(Some(close)) = close else {
        panic!("expected invalid-session close");
    };
    assert_eq!(u16::from(close.code), 4006);
    gateway.shutdown().await.unwrap();
}
