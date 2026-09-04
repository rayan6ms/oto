use std::time::Duration;

use oto_testkit::{
    DiscoveryError, DiscoveryResponseMutation, FakeUdpServer, FakeUdpServerConfig, FaultAction,
    ManualClock, TransportMode, TransportPacketEncoder, UdpTestkitError, build_discovery_request,
    parse_discovery_response,
};
use tokio::net::UdpSocket;

#[tokio::test]
async fn discovery_faults_capture_and_manual_delay_use_real_udp_without_live_discord() {
    let clock = ManualClock::new(Duration::from_secs(100));
    let mut config = FakeUdpServerConfig::localhost(clock.clone());
    config.max_datagram_bytes = 256;
    let server = FakeUdpServer::start(config).await.unwrap();
    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut response = [0_u8; 256];

    client
        .send_to(&build_discovery_request(1), server.local_addr())
        .await
        .unwrap();
    let length = client.recv(&mut response).await.unwrap();
    let parsed = parse_discovery_response(&response[..length], 1).unwrap();
    assert_eq!(parsed.port, 50_000);

    server
        .push_fault(FaultAction::Delay(Duration::from_secs(5)))
        .unwrap();
    client
        .send_to(&build_discovery_request(2), server.local_addr())
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), client.recv(&mut response))
            .await
            .is_err()
    );
    clock.advance(Duration::from_secs(5)).unwrap();
    let length = client.recv(&mut response).await.unwrap();
    assert_eq!(
        parse_discovery_response(&response[..length], 2)
            .unwrap()
            .ssrc,
        2
    );

    server.push_fault(FaultAction::Reorder).unwrap();
    server.push_fault(FaultAction::Pass).unwrap();
    client
        .send_to(&build_discovery_request(3), server.local_addr())
        .await
        .unwrap();
    client
        .send_to(&build_discovery_request(4), server.local_addr())
        .await
        .unwrap();
    let first_length = client.recv(&mut response).await.unwrap();
    assert_eq!(
        parse_discovery_response(&response[..first_length], 4)
            .unwrap()
            .ssrc,
        4
    );
    let second_length = client.recv(&mut response).await.unwrap();
    assert_eq!(
        parse_discovery_response(&response[..second_length], 3)
            .unwrap()
            .ssrc,
        3
    );

    server.push_fault(FaultAction::Drop).unwrap();
    client
        .send_to(&build_discovery_request(5), server.local_addr())
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), client.recv(&mut response))
            .await
            .is_err()
    );

    server
        .push_discovery_mutation(DiscoveryResponseMutation::MissingAddressTerminator)
        .unwrap();
    client
        .send_to(&build_discovery_request(6), server.local_addr())
        .await
        .unwrap();
    let length = client.recv(&mut response).await.unwrap();
    assert_eq!(
        parse_discovery_response(&response[..length], 6),
        Err(DiscoveryError::MissingAddressTerminator)
    );

    client
        .send_to(&[1, 2, 3], server.local_addr())
        .await
        .unwrap();
    client
        .send_to(&vec![0; 257], server.local_addr())
        .await
        .unwrap();
    for _ in 0..100 {
        if server.stats().malformed_discovery_dropped() == 1
            && server.stats().oversized_dropped() == 1
        {
            break;
        }
        tokio::task::yield_now().await;
    }

    assert_eq!(server.capture().len(), 7);
    assert_eq!(server.stats().malformed_discovery_dropped(), 1);
    assert_eq!(server.stats().oversized_dropped(), 1);
    assert_eq!(server.stats().responses_sent(), 5);

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_cancels_pending_delayed_delivery_cleanly() {
    let clock = ManualClock::new(Duration::ZERO);
    let server = FakeUdpServer::start(FakeUdpServerConfig::localhost(clock))
        .await
        .unwrap();
    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    server
        .push_fault(FaultAction::Delay(Duration::from_secs(1_000)))
        .unwrap();
    client
        .send_to(&build_discovery_request(9), server.local_addr())
        .await
        .unwrap();
    tokio::task::yield_now().await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn captured_current_transport_packets_are_decrypted_semantically() {
    let clock = ManualClock::new(Duration::ZERO);
    let server = FakeUdpServer::start(FakeUdpServerConfig::localhost(clock))
        .await
        .unwrap();
    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let key = [0x33; 32];

    for (index, mode) in [
        TransportMode::Aes256GcmRtpSize,
        TransportMode::XChaCha20Poly1305RtpSize,
    ]
    .into_iter()
    .enumerate()
    {
        let mut encoder = TransportPacketEncoder::new(mode, key, 0x0102_0304, 64).unwrap();
        let packet = encoder
            .encrypt(
                index as u16 + 10,
                960 * index as u32,
                0x5566_7788,
                b"encoded-opus",
            )
            .unwrap();
        client.send_to(&packet, server.local_addr()).await.unwrap();
    }

    for _ in 0..100 {
        if server.capture().len() == 2 {
            break;
        }
        tokio::task::yield_now().await;
    }
    for (index, mode) in [
        TransportMode::Aes256GcmRtpSize,
        TransportMode::XChaCha20Poly1305RtpSize,
    ]
    .into_iter()
    .enumerate()
    {
        let decoded = server
            .decrypt_captured_transport(index, mode, &key, 64)
            .unwrap();
        assert_eq!(decoded.sequence, index as u16 + 10);
        assert_eq!(decoded.timestamp, 960 * index as u32);
        assert_eq!(decoded.ssrc, 0x5566_7788);
        assert_eq!(decoded.nonce, 0x0102_0304);
        assert_eq!(decoded.payload, b"encoded-opus");
    }

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn mutation_and_pending_delivery_queues_reject_overload() {
    let clock = ManualClock::new(Duration::ZERO);
    let mut config = FakeUdpServerConfig::localhost(clock);
    config.mutation_capacity = 1;
    config.pending_delivery_capacity = 1;
    let server = FakeUdpServer::start(config).await.unwrap();
    server
        .push_discovery_mutation(DiscoveryResponseMutation::ZeroPort)
        .unwrap();
    assert!(matches!(
        server.push_discovery_mutation(DiscoveryResponseMutation::WrongType),
        Err(UdpTestkitError::MutationQueueFull { capacity: 1 })
    ));

    server
        .push_fault(FaultAction::Delay(Duration::from_secs(10)))
        .unwrap();
    server
        .push_fault(FaultAction::Delay(Duration::from_secs(10)))
        .unwrap();
    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client
        .send_to(&build_discovery_request(1), server.local_addr())
        .await
        .unwrap();
    client
        .send_to(&build_discovery_request(2), server.local_addr())
        .await
        .unwrap();
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    assert!(matches!(
        server.shutdown().await,
        Err(UdpTestkitError::PendingDeliveryFull { capacity: 1 })
    ));
}
