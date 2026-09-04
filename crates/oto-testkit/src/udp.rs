use std::collections::VecDeque;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use thiserror::Error;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::{
    CaptureError, DecodedTransportPacket, FaultAction, FaultError, FaultInjector, ManualClock,
    PacketCapture, PacketRecord, TransportCryptoError, TransportMode, decrypt_transport_packet,
};

const DISCOVERY_BYTES: usize = 74;
const DISCOVERY_LENGTH_FIELD: u16 = 70;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiscoveryResponse {
    pub ssrc: u32,
    pub address: IpAddr,
    pub port: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiscoveryResponseMutation {
    WrongType,
    WrongLength,
    WrongSsrc,
    MissingAddressTerminator,
    ZeroPort,
    Truncate(usize),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DiscoveryError {
    #[error("IP discovery packet must be exactly 74 bytes, got {actual}")]
    InvalidSize { actual: usize },
    #[error("unexpected IP discovery type {actual}")]
    InvalidType { actual: u16 },
    #[error("unexpected IP discovery length field {actual}")]
    InvalidLength { actual: u16 },
    #[error("unexpected IP discovery SSRC {actual}, expected {expected}")]
    InvalidSsrc { actual: u32, expected: u32 },
    #[error("IP discovery address is not null terminated")]
    MissingAddressTerminator,
    #[error("IP discovery address is not valid UTF-8")]
    InvalidAddressUtf8,
    #[error("IP discovery address is not a valid IP address")]
    InvalidAddress,
    #[error("IP discovery port must be nonzero")]
    ZeroPort,
    #[error("IP discovery request reserved fields must be zero")]
    NonzeroRequestReservedFields,
}

#[must_use]
pub fn build_discovery_request(ssrc: u32) -> [u8; DISCOVERY_BYTES] {
    let mut request = [0_u8; DISCOVERY_BYTES];
    request[..2].copy_from_slice(&1_u16.to_be_bytes());
    request[2..4].copy_from_slice(&DISCOVERY_LENGTH_FIELD.to_be_bytes());
    request[4..8].copy_from_slice(&ssrc.to_be_bytes());
    request
}

pub fn parse_discovery_response(
    packet: &[u8],
    expected_ssrc: u32,
) -> Result<DiscoveryResponse, DiscoveryError> {
    validate_common(packet, 2, expected_ssrc)?;
    let address_field = &packet[8..72];
    let terminator = address_field
        .iter()
        .position(|byte| *byte == 0)
        .ok_or(DiscoveryError::MissingAddressTerminator)?;
    let address_text = std::str::from_utf8(&address_field[..terminator])
        .map_err(|_| DiscoveryError::InvalidAddressUtf8)?;
    let address = address_text
        .parse()
        .map_err(|_| DiscoveryError::InvalidAddress)?;
    let port = u16::from_be_bytes(packet[72..74].try_into().expect("discovery port"));
    if port == 0 {
        return Err(DiscoveryError::ZeroPort);
    }
    Ok(DiscoveryResponse {
        ssrc: expected_ssrc,
        address,
        port,
    })
}

fn parse_discovery_request(packet: &[u8]) -> Result<u32, DiscoveryError> {
    if packet.len() != DISCOVERY_BYTES {
        return Err(DiscoveryError::InvalidSize {
            actual: packet.len(),
        });
    }
    let ssrc = u32::from_be_bytes(packet[4..8].try_into().expect("discovery SSRC"));
    validate_common(packet, 1, ssrc)?;
    if packet[8..].iter().any(|byte| *byte != 0) {
        return Err(DiscoveryError::NonzeroRequestReservedFields);
    }
    Ok(ssrc)
}

fn validate_common(
    packet: &[u8],
    expected_type: u16,
    expected_ssrc: u32,
) -> Result<(), DiscoveryError> {
    if packet.len() != DISCOVERY_BYTES {
        return Err(DiscoveryError::InvalidSize {
            actual: packet.len(),
        });
    }
    let packet_type = u16::from_be_bytes(packet[..2].try_into().expect("discovery type"));
    if packet_type != expected_type {
        return Err(DiscoveryError::InvalidType {
            actual: packet_type,
        });
    }
    let length = u16::from_be_bytes(packet[2..4].try_into().expect("discovery length"));
    if length != DISCOVERY_LENGTH_FIELD {
        return Err(DiscoveryError::InvalidLength { actual: length });
    }
    let ssrc = u32::from_be_bytes(packet[4..8].try_into().expect("discovery SSRC"));
    if ssrc != expected_ssrc {
        return Err(DiscoveryError::InvalidSsrc {
            actual: ssrc,
            expected: expected_ssrc,
        });
    }
    Ok(())
}

fn build_discovery_response(
    ssrc: u32,
    address: IpAddr,
    port: u16,
) -> Result<Vec<u8>, DiscoveryError> {
    let address = address.to_string();
    if address.len() >= 64 {
        return Err(DiscoveryError::InvalidAddress);
    }
    let mut response = vec![0_u8; DISCOVERY_BYTES];
    response[..2].copy_from_slice(&2_u16.to_be_bytes());
    response[2..4].copy_from_slice(&DISCOVERY_LENGTH_FIELD.to_be_bytes());
    response[4..8].copy_from_slice(&ssrc.to_be_bytes());
    response[8..8 + address.len()].copy_from_slice(address.as_bytes());
    response[72..74].copy_from_slice(&port.to_be_bytes());
    Ok(response)
}

#[derive(Clone, Debug)]
pub struct FakeUdpServerConfig {
    pub clock: ManualClock,
    pub public_address: IpAddr,
    pub public_port: u16,
    pub capture_capacity: usize,
    pub max_datagram_bytes: usize,
    pub fault_capacity: usize,
    pub mutation_capacity: usize,
    pub pending_delivery_capacity: usize,
    pub outbound_capacity: usize,
}

impl FakeUdpServerConfig {
    #[must_use]
    pub fn localhost(clock: ManualClock) -> Self {
        Self {
            clock,
            public_address: "203.0.113.10".parse().expect("documentation IPv4"),
            public_port: 50_000,
            capture_capacity: 64,
            max_datagram_bytes: 2_048,
            fault_capacity: 32,
            mutation_capacity: 16,
            pending_delivery_capacity: 32,
            outbound_capacity: 16,
        }
    }

    fn validate(&self) -> Result<(), UdpTestkitError> {
        if self.public_port == 0
            || self.capture_capacity == 0
            || self.max_datagram_bytes == 0
            || self.max_datagram_bytes > u16::MAX as usize
            || self.fault_capacity == 0
            || self.mutation_capacity == 0
            || self.pending_delivery_capacity == 0
            || self.outbound_capacity == 0
        {
            return Err(UdpTestkitError::InvalidConfig);
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct FakeUdpStatsInner {
    oversized_dropped: AtomicUsize,
    malformed_discovery_dropped: AtomicUsize,
    responses_sent: AtomicUsize,
}

#[derive(Clone, Debug)]
pub struct FakeUdpStats(Arc<FakeUdpStatsInner>);

impl FakeUdpStats {
    #[must_use]
    pub fn oversized_dropped(&self) -> usize {
        self.0.oversized_dropped.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn malformed_discovery_dropped(&self) -> usize {
        self.0.malformed_discovery_dropped.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn responses_sent(&self) -> usize {
        self.0.responses_sent.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Error)]
pub enum UdpTestkitError {
    #[error("invalid fake UDP server configuration")]
    InvalidConfig,
    #[error("UDP I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Capture(#[from] CaptureError),
    #[error(transparent)]
    Fault(#[from] FaultError),
    #[error(transparent)]
    Discovery(#[from] DiscoveryError),
    #[error("discovery mutation queue is full at capacity {capacity}")]
    MutationQueueFull { capacity: usize },
    #[error("pending UDP delivery queue is full at capacity {capacity}")]
    PendingDeliveryFull { capacity: usize },
    #[error("captured UDP packet index {index} does not exist")]
    CapturedPacketMissing { index: usize },
    #[error("no client UDP peer has been discovered")]
    ClientPeerMissing,
    #[error("fake UDP outbound queue is full at capacity {capacity}")]
    OutboundQueueFull { capacity: usize },
    #[error(transparent)]
    TransportCrypto(#[from] TransportCryptoError),
    #[error("fake UDP task failed to join: {0}")]
    Join(#[from] tokio::task::JoinError),
}

#[derive(Debug)]
struct PendingDelivery {
    due: Duration,
    peer: SocketAddr,
    bytes: Vec<u8>,
}

struct UdpServerShared {
    capture: PacketCapture,
    faults: Arc<Mutex<FaultInjector>>,
    mutations: Arc<Mutex<VecDeque<DiscoveryResponseMutation>>>,
    stats: FakeUdpStats,
    last_peer: Arc<Mutex<Option<SocketAddr>>>,
}

pub struct FakeUdpServer {
    local_addr: SocketAddr,
    capture: PacketCapture,
    faults: Arc<Mutex<FaultInjector>>,
    mutations: Arc<Mutex<VecDeque<DiscoveryResponseMutation>>>,
    mutation_capacity: usize,
    outbound: mpsc::Sender<Vec<u8>>,
    outbound_capacity: usize,
    last_peer: Arc<Mutex<Option<SocketAddr>>>,
    stats: FakeUdpStats,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<Result<(), UdpTestkitError>>,
}

impl std::fmt::Debug for FakeUdpServer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FakeUdpServer")
            .field("local_addr", &self.local_addr)
            .field("capture_len", &self.capture.len())
            .finish_non_exhaustive()
    }
}

impl FakeUdpServer {
    pub async fn start(config: FakeUdpServerConfig) -> Result<Self, UdpTestkitError> {
        config.validate()?;
        let socket = UdpSocket::bind("127.0.0.1:0").await?;
        let local_addr = socket.local_addr()?;
        let capture = PacketCapture::new(config.capture_capacity, config.max_datagram_bytes)?;
        let faults = Arc::new(Mutex::new(FaultInjector::new(
            config.fault_capacity,
            config.max_datagram_bytes,
        )?));
        let mutations = Arc::new(Mutex::new(VecDeque::with_capacity(
            config.mutation_capacity,
        )));
        let stats = FakeUdpStats(Arc::new(FakeUdpStatsInner::default()));
        let (shutdown, shutdown_rx) = watch::channel(false);
        let (outbound, outbound_rx) = mpsc::channel(config.outbound_capacity);
        let last_peer = Arc::new(Mutex::new(None));

        let shared = UdpServerShared {
            capture: capture.clone(),
            faults: faults.clone(),
            mutations: mutations.clone(),
            stats: stats.clone(),
            last_peer: last_peer.clone(),
        };

        let task = tokio::spawn(run_udp_server(
            socket,
            config.clone(),
            shared,
            outbound_rx,
            shutdown_rx,
        ));

        Ok(Self {
            local_addr,
            capture,
            faults,
            mutations,
            mutation_capacity: config.mutation_capacity,
            outbound,
            outbound_capacity: config.outbound_capacity,
            last_peer,
            stats,
            shutdown,
            task,
        })
    }

    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    #[must_use]
    pub fn capture(&self) -> PacketCapture {
        self.capture.clone()
    }

    #[must_use]
    pub fn stats(&self) -> FakeUdpStats {
        self.stats.clone()
    }

    pub fn push_fault(&self, action: FaultAction) -> Result<(), FaultError> {
        self.faults
            .lock()
            .expect("fake UDP fault mutex poisoned")
            .push(action)
    }

    pub fn push_discovery_mutation(
        &self,
        mutation: DiscoveryResponseMutation,
    ) -> Result<(), UdpTestkitError> {
        let mut mutations = self
            .mutations
            .lock()
            .expect("fake UDP mutation mutex poisoned");
        if mutations.len() == self.mutation_capacity {
            return Err(UdpTestkitError::MutationQueueFull {
                capacity: self.mutation_capacity,
            });
        }
        mutations.push_back(mutation);
        Ok(())
    }

    pub fn decrypt_captured_transport(
        &self,
        index: usize,
        mode: TransportMode,
        key: &[u8; 32],
        max_payload_bytes: usize,
    ) -> Result<DecodedTransportPacket, UdpTestkitError> {
        let packet = self
            .capture
            .snapshot()
            .into_iter()
            .nth(index)
            .ok_or(UdpTestkitError::CapturedPacketMissing { index })?;
        Ok(decrypt_transport_packet(
            mode,
            key,
            &packet.bytes,
            max_payload_bytes,
        )?)
    }

    pub fn try_send_to_client(&self, packet: Vec<u8>) -> Result<(), UdpTestkitError> {
        if packet.len() > self.capture.max_packet_bytes() {
            return Err(UdpTestkitError::Capture(CaptureError::PacketTooLarge {
                actual: packet.len(),
                maximum: self.capture.max_packet_bytes(),
            }));
        }
        if self
            .last_peer
            .lock()
            .expect("fake UDP peer mutex poisoned")
            .is_none()
        {
            return Err(UdpTestkitError::ClientPeerMissing);
        }
        self.outbound.try_send(packet).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => UdpTestkitError::OutboundQueueFull {
                capacity: self.outbound_capacity,
            },
            mpsc::error::TrySendError::Closed(_) => UdpTestkitError::Io(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "fake UDP task is closed",
            )),
        })
    }

    pub async fn shutdown(self) -> Result<(), UdpTestkitError> {
        self.shutdown.send_replace(true);
        self.task.await??;
        Ok(())
    }
}

async fn run_udp_server(
    socket: UdpSocket,
    config: FakeUdpServerConfig,
    shared: UdpServerShared,
    mut outbound: mpsc::Receiver<Vec<u8>>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), UdpTestkitError> {
    let mut clock_updates = config.clock.subscribe();
    let mut receive_buffer = vec![0_u8; config.max_datagram_bytes + 1];
    let mut pending = Vec::<PendingDelivery>::with_capacity(config.pending_delivery_capacity);

    loop {
        flush_due(&socket, config.clock.now(), &mut pending, &shared.stats).await?;
        tokio::select! {
            result = shutdown.changed() => {
                if result.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            result = clock_updates.changed() => {
                if result.is_err() {
                    return Ok(());
                }
            }
            packet = outbound.recv() => {
                let Some(packet) = packet else { return Ok(()); };
                let peer = *shared.last_peer
                    .lock()
                    .expect("fake UDP peer mutex poisoned");
                if let Some(peer) = peer {
                    socket.send_to(&packet, peer).await?;
                }
            }
            received = socket.recv_from(&mut receive_buffer) => {
                let (length, peer) = received?;
                *shared.last_peer
                    .lock()
                    .expect("fake UDP peer mutex poisoned") = Some(peer);
                if length > config.max_datagram_bytes {
                    shared.stats.0.oversized_dropped.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                let packet = receive_buffer[..length].to_vec();
                shared.capture.record(PacketRecord {
                    at: config.clock.now(),
                    peer,
                    bytes: packet.clone(),
                })?;

                let Ok(ssrc) = parse_discovery_request(&packet) else {
                    shared.stats.0.malformed_discovery_dropped.fetch_add(1, Ordering::Relaxed);
                    continue;
                };
                let mut response = build_discovery_response(
                    ssrc,
                    config.public_address,
                    config.public_port,
                )?;
                if let Some(mutation) = shared.mutations
                    .lock()
                    .expect("fake UDP mutation mutex poisoned")
                    .pop_front()
                {
                    apply_mutation(&mut response, mutation);
                }
                let scheduled = shared.faults
                    .lock()
                    .expect("fake UDP fault mutex poisoned")
                    .apply(config.clock.now(), response)?;
                for packet in scheduled {
                    if pending.len() == config.pending_delivery_capacity {
                        return Err(UdpTestkitError::PendingDeliveryFull {
                            capacity: config.pending_delivery_capacity,
                        });
                    }
                    pending.push(PendingDelivery {
                        due: packet.due,
                        peer,
                        bytes: packet.bytes,
                    });
                }
            }
        }
    }
}

async fn flush_due(
    socket: &UdpSocket,
    now: Duration,
    pending: &mut Vec<PendingDelivery>,
    stats: &FakeUdpStats,
) -> Result<(), std::io::Error> {
    let mut index = 0;
    while index < pending.len() {
        if pending[index].due <= now {
            let delivery = pending.remove(index);
            socket.send_to(&delivery.bytes, delivery.peer).await?;
            stats.0.responses_sent.fetch_add(1, Ordering::Relaxed);
        } else {
            index += 1;
        }
    }
    Ok(())
}

fn apply_mutation(response: &mut Vec<u8>, mutation: DiscoveryResponseMutation) {
    match mutation {
        DiscoveryResponseMutation::WrongType => response[..2].copy_from_slice(&3_u16.to_be_bytes()),
        DiscoveryResponseMutation::WrongLength => {
            response[2..4].copy_from_slice(&69_u16.to_be_bytes())
        }
        DiscoveryResponseMutation::WrongSsrc => {
            let ssrc = u32::from_be_bytes(response[4..8].try_into().expect("response SSRC"));
            response[4..8].copy_from_slice(&ssrc.wrapping_add(1).to_be_bytes());
        }
        DiscoveryResponseMutation::MissingAddressTerminator => response[8..72].fill(b'x'),
        DiscoveryResponseMutation::ZeroPort => response[72..74].fill(0),
        DiscoveryResponseMutation::Truncate(length) => {
            response.truncate(length.min(response.len()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_and_response_wire_format_is_exact() {
        let request = build_discovery_request(0x0102_0304);
        assert_eq!(request.len(), 74);
        assert_eq!(&request[..8], &[0, 1, 0, 70, 1, 2, 3, 4]);
        assert!(request[8..].iter().all(|byte| *byte == 0));

        let response =
            build_discovery_response(0x0102_0304, "203.0.113.10".parse().unwrap(), 50_000).unwrap();
        assert_eq!(
            parse_discovery_response(&response, 0x0102_0304).unwrap(),
            DiscoveryResponse {
                ssrc: 0x0102_0304,
                address: "203.0.113.10".parse().unwrap(),
                port: 50_000,
            }
        );
    }

    #[test]
    fn every_structural_response_error_is_rejected() {
        let ssrc = 7;
        for (mutation, expected) in [
            (
                DiscoveryResponseMutation::WrongType,
                DiscoveryError::InvalidType { actual: 3 },
            ),
            (
                DiscoveryResponseMutation::WrongLength,
                DiscoveryError::InvalidLength { actual: 69 },
            ),
            (
                DiscoveryResponseMutation::WrongSsrc,
                DiscoveryError::InvalidSsrc {
                    actual: 8,
                    expected: 7,
                },
            ),
            (
                DiscoveryResponseMutation::MissingAddressTerminator,
                DiscoveryError::MissingAddressTerminator,
            ),
            (
                DiscoveryResponseMutation::ZeroPort,
                DiscoveryError::ZeroPort,
            ),
            (
                DiscoveryResponseMutation::Truncate(20),
                DiscoveryError::InvalidSize { actual: 20 },
            ),
        ] {
            let mut response =
                build_discovery_response(ssrc, "203.0.113.10".parse().unwrap(), 9).unwrap();
            apply_mutation(&mut response, mutation);
            assert_eq!(parse_discovery_response(&response, ssrc), Err(expected));
        }
    }
}
