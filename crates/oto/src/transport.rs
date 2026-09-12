use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use aes_gcm::aead::{AeadInOut, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use tokio::net::UdpSocket;
use tokio::time::{Instant, timeout, timeout_at};

const DISCOVERY_BYTES: usize = 74;
const DISCOVERY_LENGTH: u16 = 70;
const DISCOVERY_ATTEMPTS: usize = 3;
const DISCOVERY_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(1);
const UDP_IO_TIMEOUT: Duration = Duration::from_secs(2);
const RTP_HEADER_BYTES: usize = 12;
const AEAD_TAG_BYTES: usize = 16;
const NONCE_SUFFIX_BYTES: usize = 4;
const RTP_OVERHEAD_BYTES: usize = RTP_HEADER_BYTES + AEAD_TAG_BYTES + NONCE_SUFFIX_BYTES;
const RTP_TIMESTAMP_STEP: u32 = 960;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TransportMode {
    Aes256GcmRtpSize,
    XChaCha20Poly1305RtpSize,
}

impl TransportMode {
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Aes256GcmRtpSize => "aead_aes256_gcm_rtpsize",
            Self::XChaCha20Poly1305RtpSize => "aead_xchacha20_poly1305_rtpsize",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "aead_aes256_gcm_rtpsize" => Some(Self::Aes256GcmRtpSize),
            "aead_xchacha20_poly1305_rtpsize" => Some(Self::XChaCha20Poly1305RtpSize),
            _ => None,
        }
    }
}

pub(crate) fn select_mode(modes: &[String]) -> Option<TransportMode> {
    // Preference is local policy, not the order advertised by the gateway.
    [
        TransportMode::Aes256GcmRtpSize,
        TransportMode::XChaCha20Poly1305RtpSize,
    ]
    .into_iter()
    .find(|candidate| modes.iter().any(|mode| mode == candidate.name()))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DiscoveredAddress {
    pub(crate) address: IpAddr,
    pub(crate) port: u16,
}

#[derive(Debug)]
pub(crate) struct DiscoveredSocket {
    pub(crate) socket: Arc<UdpSocket>,
    pub(crate) public: DiscoveredAddress,
}

#[derive(Debug)]
pub(crate) enum DiscoveryFailure {
    Io(io::Error),
    TimedOut,
}

impl std::fmt::Display for DiscoveryFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(_) => formatter.write_str("UDP discovery I/O failed"),
            Self::TimedOut => formatter.write_str("UDP discovery timed out"),
        }
    }
}

impl std::error::Error for DiscoveryFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::TimedOut => None,
        }
    }
}

pub(crate) async fn discover(
    remote: SocketAddr,
    ssrc: u32,
    max_datagram_bytes: usize,
) -> Result<DiscoveredSocket, DiscoveryFailure> {
    let bind_address = if remote.is_ipv4() {
        SocketAddr::from(([0, 0, 0, 0], 0))
    } else {
        SocketAddr::from(([0_u16; 8], 0))
    };
    let socket = timeout(UDP_IO_TIMEOUT, UdpSocket::bind(bind_address))
        .await
        .map_err(|_| DiscoveryFailure::TimedOut)?
        .map_err(DiscoveryFailure::Io)?;
    timeout(UDP_IO_TIMEOUT, socket.connect(remote))
        .await
        .map_err(|_| DiscoveryFailure::TimedOut)?
        .map_err(DiscoveryFailure::Io)?;
    apply_audio_qos(&socket);

    let request = build_discovery_request(ssrc);
    let mut response = vec![0_u8; max_datagram_bytes.saturating_add(1)];
    for _ in 0..DISCOVERY_ATTEMPTS {
        timeout(UDP_IO_TIMEOUT, socket.send(&request))
            .await
            .map_err(|_| DiscoveryFailure::TimedOut)?
            .map_err(DiscoveryFailure::Io)?;
        let deadline = Instant::now() + DISCOVERY_ATTEMPT_TIMEOUT;
        loop {
            let length = match timeout_at(deadline, socket.recv(&mut response)).await {
                Ok(Ok(length)) => length,
                Ok(Err(error)) => return Err(DiscoveryFailure::Io(error)),
                Err(_) => break,
            };
            if length > max_datagram_bytes {
                continue;
            }
            if let Some(public) = parse_discovery_response(&response[..length], ssrc) {
                return Ok(DiscoveredSocket {
                    socket: Arc::new(socket),
                    public,
                });
            }
        }
    }
    Err(DiscoveryFailure::TimedOut)
}

/// Optionally mark voice datagrams with an audio DSCP value. This is deliberately
/// opt-in: many networks ignore or rewrite DSCP, and changing the default would
/// make a transport experiment impossible to compare with existing runs. A
/// failed best-effort mark never prevents a voice connection.
#[cfg(target_os = "linux")]
fn apply_audio_qos(socket: &UdpSocket) {
    let Ok(raw) = std::env::var("RAYDIO_AUDIO_DSCP") else {
        return;
    };
    let Ok(dscp) = raw.parse::<u8>() else { return };
    if dscp > 63 {
        return;
    }
    let _ = rustix::net::sockopt::set_ip_tos(socket, dscp << 2);
}

#[cfg(not(target_os = "linux"))]
fn apply_audio_qos(_socket: &UdpSocket) {}

fn build_discovery_request(ssrc: u32) -> [u8; DISCOVERY_BYTES] {
    let mut request = [0_u8; DISCOVERY_BYTES];
    request[..2].copy_from_slice(&1_u16.to_be_bytes());
    request[2..4].copy_from_slice(&DISCOVERY_LENGTH.to_be_bytes());
    request[4..8].copy_from_slice(&ssrc.to_be_bytes());
    request
}

fn parse_discovery_response(packet: &[u8], expected_ssrc: u32) -> Option<DiscoveredAddress> {
    if packet.len() != DISCOVERY_BYTES
        || u16::from_be_bytes(packet[..2].try_into().ok()?) != 2
        || u16::from_be_bytes(packet[2..4].try_into().ok()?) != DISCOVERY_LENGTH
        || u32::from_be_bytes(packet[4..8].try_into().ok()?) != expected_ssrc
    {
        return None;
    }
    let address_field = &packet[8..72];
    let terminator = address_field.iter().position(|byte| *byte == 0)?;
    let address = std::str::from_utf8(&address_field[..terminator])
        .ok()?
        .parse()
        .ok()?;
    let port = u16::from_be_bytes(packet[72..74].try_into().ok()?);
    (port != 0).then_some(DiscoveredAddress { address, port })
}

#[cfg(fuzzing)]
pub(crate) fn fuzz_discovery_response(input: &[u8]) {
    let expected_ssrc = input
        .get(..4)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_be_bytes)
        .unwrap_or_default();
    if let Some(discovered) = parse_discovery_response(input, expected_ssrc) {
        assert_eq!(input.len(), DISCOVERY_BYTES);
        assert_ne!(discovered.port, 0);
    }
}

enum Cipher {
    Aes256Gcm(Box<Aes256Gcm>),
    XChaCha20Poly1305(XChaCha20Poly1305),
}

impl std::fmt::Debug for Cipher {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Cipher([REDACTED])")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TransportCryptoFailure {
    InvalidBounds,
    Randomness,
    NonceExhausted,
    Encryption,
}

#[derive(Debug)]
pub(crate) struct TransportEncoder {
    mode: TransportMode,
    cipher: Cipher,
    ssrc: u32,
    sequence: u16,
    timestamp: u32,
    next_nonce: Option<u32>,
    max_payload_bytes: usize,
    max_datagram_bytes: usize,
    ciphertext: Vec<u8>,
}

impl TransportEncoder {
    pub(crate) fn new(
        mode: TransportMode,
        key: &[u8; 32],
        ssrc: u32,
        max_payload_bytes: usize,
        max_datagram_bytes: usize,
    ) -> Result<Self, TransportCryptoFailure> {
        let mut random = [0_u8; 10];
        getrandom::fill(&mut random).map_err(|_| TransportCryptoFailure::Randomness)?;
        Self::new_with_starts(
            mode,
            key,
            ssrc,
            max_payload_bytes,
            max_datagram_bytes,
            u16::from_le_bytes(random[..2].try_into().expect("sequence bytes")),
            u32::from_le_bytes(random[2..6].try_into().expect("timestamp bytes")),
            u32::from_le_bytes(random[6..].try_into().expect("nonce bytes")),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_with_starts(
        mode: TransportMode,
        key: &[u8; 32],
        ssrc: u32,
        max_payload_bytes: usize,
        max_datagram_bytes: usize,
        sequence: u16,
        timestamp: u32,
        nonce: u32,
    ) -> Result<Self, TransportCryptoFailure> {
        if max_payload_bytes == 0
            || max_payload_bytes
                .checked_add(RTP_OVERHEAD_BYTES)
                .is_none_or(|packet| packet > max_datagram_bytes)
        {
            return Err(TransportCryptoFailure::InvalidBounds);
        }
        let cipher = match mode {
            TransportMode::Aes256GcmRtpSize => Cipher::Aes256Gcm(Box::new(
                Aes256Gcm::new_from_slice(key).map_err(|_| TransportCryptoFailure::Encryption)?,
            )),
            TransportMode::XChaCha20Poly1305RtpSize => Cipher::XChaCha20Poly1305(
                XChaCha20Poly1305::new_from_slice(key)
                    .map_err(|_| TransportCryptoFailure::Encryption)?,
            ),
        };
        Ok(Self {
            mode,
            cipher,
            ssrc,
            sequence,
            timestamp,
            next_nonce: Some(nonce),
            max_payload_bytes,
            max_datagram_bytes,
            ciphertext: Vec::with_capacity(max_payload_bytes + AEAD_TAG_BYTES),
        })
    }

    pub(crate) fn mode(&self) -> TransportMode {
        self.mode
    }

    #[cfg(test)]
    pub(crate) fn set_test_nonce_start(&mut self, nonce: u32) {
        self.next_nonce = Some(nonce);
    }

    // P07 wires this encoder to the paced sender. Keeping packet construction here
    // lets P06 prove the wire and nonce contracts without exposing a raw-send API.
    #[allow(dead_code)]
    pub(crate) fn encrypt_next(
        &mut self,
        opus: &[u8],
        packet: &mut Vec<u8>,
    ) -> Result<(), TransportCryptoFailure> {
        let packet_bytes = opus
            .len()
            .checked_add(RTP_OVERHEAD_BYTES)
            .ok_or(TransportCryptoFailure::InvalidBounds)?;
        if opus.len() > self.max_payload_bytes || packet_bytes > self.max_datagram_bytes {
            return Err(TransportCryptoFailure::InvalidBounds);
        }
        let nonce = self
            .next_nonce
            .ok_or(TransportCryptoFailure::NonceExhausted)?;
        // Consume before the encryption attempt so a failed attempt can never retry
        // with the same nonce under this key.
        self.next_nonce = nonce.checked_add(1);

        let mut header = [0_u8; RTP_HEADER_BYTES];
        header[0] = 0x80;
        header[1] = 0x78;
        header[2..4].copy_from_slice(&self.sequence.to_be_bytes());
        header[4..8].copy_from_slice(&self.timestamp.to_be_bytes());
        header[8..12].copy_from_slice(&self.ssrc.to_be_bytes());

        self.ciphertext.clear();
        self.ciphertext.extend_from_slice(opus);
        let suffix = nonce.to_le_bytes();
        match &self.cipher {
            Cipher::Aes256Gcm(cipher) => {
                let mut expanded = [0_u8; 12];
                expanded[..4].copy_from_slice(&suffix);
                let expanded = Nonce::from(expanded);
                cipher
                    .encrypt_in_place(&expanded, &header, &mut self.ciphertext)
                    .map_err(|_| TransportCryptoFailure::Encryption)?;
            }
            Cipher::XChaCha20Poly1305(cipher) => {
                let mut expanded = [0_u8; 24];
                expanded[..4].copy_from_slice(&suffix);
                let expanded = XNonce::from(expanded);
                cipher
                    .encrypt_in_place(&expanded, &header, &mut self.ciphertext)
                    .map_err(|_| TransportCryptoFailure::Encryption)?;
            }
        }

        packet.clear();
        packet.reserve(packet_bytes);
        packet.extend_from_slice(&header);
        packet.extend_from_slice(&self.ciphertext);
        packet.extend_from_slice(&suffix);
        self.sequence = self.sequence.wrapping_add(1);
        self.timestamp = self.timestamp.wrapping_add(RTP_TIMESTAMP_STEP);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use oto_testkit::{
        DiscoveryResponseMutation, FakeUdpServer, FakeUdpServerConfig, FaultAction, ManualClock,
        TransportMode as OracleMode,
    };

    use super::*;

    const KEY: [u8; 32] = [0x42; 32];

    fn oracle_mode(mode: TransportMode) -> OracleMode {
        match mode {
            TransportMode::Aes256GcmRtpSize => OracleMode::Aes256GcmRtpSize,
            TransportMode::XChaCha20Poly1305RtpSize => OracleMode::XChaCha20Poly1305RtpSize,
        }
    }

    #[test]
    fn mode_selection_is_preference_ordered_and_exact() {
        let reversed = vec![
            "aead_xchacha20_poly1305_rtpsize".to_owned(),
            "future_mode".to_owned(),
            "aead_aes256_gcm_rtpsize".to_owned(),
        ];
        assert_eq!(
            select_mode(&reversed),
            Some(TransportMode::Aes256GcmRtpSize)
        );
        assert_eq!(
            select_mode(&["aead_xchacha20_poly1305_rtpsize".to_owned()]),
            Some(TransportMode::XChaCha20Poly1305RtpSize)
        );
        assert_eq!(select_mode(&["xsalsa20_poly1305".to_owned()]), None);
    }

    #[test]
    fn discovery_wire_is_exact_and_parser_rejects_every_structural_error() {
        let request = build_discovery_request(0x0102_0304);
        assert_eq!(&request[..8], &[0, 1, 0, 70, 1, 2, 3, 4]);
        assert!(request[8..].iter().all(|byte| *byte == 0));

        let mut valid = [0_u8; DISCOVERY_BYTES];
        valid[..2].copy_from_slice(&2_u16.to_be_bytes());
        valid[2..4].copy_from_slice(&70_u16.to_be_bytes());
        valid[4..8].copy_from_slice(&0x0102_0304_u32.to_be_bytes());
        valid[8..20].copy_from_slice(b"203.0.113.1\0");
        valid[72..].copy_from_slice(&50_000_u16.to_be_bytes());
        assert_eq!(
            parse_discovery_response(&valid, 0x0102_0304),
            Some(DiscoveredAddress {
                address: "203.0.113.1".parse().unwrap(),
                port: 50_000,
            })
        );
        for broken in 0..8 {
            let mut packet = valid.to_vec();
            match broken {
                0 => packet.truncate(73),
                1 => packet[..2].copy_from_slice(&1_u16.to_be_bytes()),
                2 => packet[2..4].copy_from_slice(&69_u16.to_be_bytes()),
                3 => packet[4..8].copy_from_slice(&5_u32.to_be_bytes()),
                4 => packet[8..72].fill(b'x'),
                5 => packet[72..].fill(0),
                6 => {
                    packet[8..72].fill(0);
                    packet[8] = 0xff;
                }
                7 => {
                    packet[8..72].fill(0);
                    packet[8..18].copy_from_slice(b"not-an-ip\0");
                }
                _ => unreachable!(),
            }
            assert_eq!(parse_discovery_response(&packet, 0x0102_0304), None);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn discovery_ignores_malformed_and_oversized_datagrams_with_bounded_retries() {
        let clock = ManualClock::new(Duration::ZERO);
        let server = FakeUdpServer::start(FakeUdpServerConfig::localhost(clock))
            .await
            .unwrap();
        server
            .push_discovery_mutation(DiscoveryResponseMutation::WrongType)
            .unwrap();
        server
            .push_discovery_mutation(DiscoveryResponseMutation::WrongSsrc)
            .unwrap();
        let discovered = discover(server.local_addr(), 0x0102_0304, 2_048)
            .await
            .expect("third bounded attempt accepts a valid response");
        assert_eq!(
            discovered.public,
            DiscoveredAddress {
                address: "203.0.113.10".parse().unwrap(),
                port: 50_000,
            }
        );
        assert_eq!(server.capture().len(), 3);
        drop(discovered);
        server.shutdown().await.unwrap();

        let clock = ManualClock::new(Duration::ZERO);
        let mut config = FakeUdpServerConfig::localhost(clock);
        config.max_datagram_bytes = 4_096;
        let server = FakeUdpServer::start(config).await.unwrap();
        server
            .push_fault(FaultAction::Replace(vec![0; DISCOVERY_BYTES + 1]))
            .unwrap();
        let discovered = discover(server.local_addr(), 9, DISCOVERY_BYTES)
            .await
            .expect("oversized response is ignored and retried");
        assert_eq!(discovered.public.port, 50_000);
        assert_eq!(server.capture().len(), 2);
        drop(discovered);
        server.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn discovery_exhausts_exactly_three_attempts_without_accepting_malformed_input() {
        let clock = ManualClock::new(Duration::ZERO);
        let server = FakeUdpServer::start(FakeUdpServerConfig::localhost(clock))
            .await
            .unwrap();
        for _ in 0..DISCOVERY_ATTEMPTS {
            server
                .push_discovery_mutation(DiscoveryResponseMutation::ZeroPort)
                .unwrap();
        }
        assert!(matches!(
            discover(server.local_addr(), 7, 2_048).await,
            Err(DiscoveryFailure::TimedOut)
        ));
        assert_eq!(server.capture().len(), DISCOVERY_ATTEMPTS);
        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn both_modes_match_the_independent_oracle_and_reuse_buffers() {
        for mode in [
            TransportMode::Aes256GcmRtpSize,
            TransportMode::XChaCha20Poly1305RtpSize,
        ] {
            let clock = ManualClock::new(Duration::ZERO);
            let server = FakeUdpServer::start(FakeUdpServerConfig::localhost(clock))
                .await
                .unwrap();
            let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            socket.connect(server.local_addr()).await.unwrap();
            let mut encoder = TransportEncoder::new_with_starts(
                mode,
                &KEY,
                0x0102_0304,
                1_275,
                2_048,
                u16::MAX,
                u32::MAX - 959,
                0x0a0b_0c0d,
            )
            .unwrap();
            let mut packet = Vec::with_capacity(2_048);
            encoder.encrypt_next(&[1, 2, 3], &mut packet).unwrap();
            let capacity = packet.capacity();
            socket.send(&packet).await.unwrap();
            encoder.encrypt_next(&[4, 5], &mut packet).unwrap();
            assert_eq!(packet.capacity(), capacity);
            socket.send(&packet).await.unwrap();

            tokio::time::timeout(Duration::from_secs(1), async {
                while server.capture().len() != 2 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let first = server
                .decrypt_captured_transport(0, oracle_mode(mode), &KEY, 1_275)
                .unwrap();
            let second = server
                .decrypt_captured_transport(1, oracle_mode(mode), &KEY, 1_275)
                .unwrap();
            assert_eq!(
                (first.sequence, first.timestamp, first.nonce),
                (u16::MAX, u32::MAX - 959, 0x0a0b_0c0d)
            );
            assert_eq!(
                (second.sequence, second.timestamp, second.nonce),
                (0, 0, 0x0a0b_0c0e)
            );
            assert_eq!(first.payload, vec![1, 2, 3]);
            assert_eq!(second.payload, vec![4, 5]);
            server.shutdown().await.unwrap();
        }
    }

    #[test]
    fn production_encoder_matches_frozen_raw_golden_packets() {
        const GOLDEN_KEY: [u8; 32] = [0x11; 32];
        for (mode, expected) in [
            (
                TransportMode::Aes256GcmRtpSize,
                "8078112233445566778899aa3d32aa62bea8755ac10fbec5839ed2d6dd3eaf2004030201",
            ),
            (
                TransportMode::XChaCha20Poly1305RtpSize,
                "8078112233445566778899aa406f7c7259ab6e4e49338bc36e55dd7c9aa0b89b04030201",
            ),
        ] {
            let mut encoder = TransportEncoder::new_with_starts(
                mode,
                &GOLDEN_KEY,
                0x7788_99aa,
                32,
                64,
                0x1122,
                0x3344_5566,
                0x0102_0304,
            )
            .unwrap();
            let mut packet = Vec::new();
            encoder.encrypt_next(b"opus", &mut packet).unwrap();
            assert_eq!(to_hex(&packet), expected);
        }
    }

    #[test]
    fn nonce_maximum_is_used_once_then_refused_for_both_modes() {
        for mode in [
            TransportMode::Aes256GcmRtpSize,
            TransportMode::XChaCha20Poly1305RtpSize,
        ] {
            let mut encoder =
                TransportEncoder::new_with_starts(mode, &KEY, 7, 1_275, 2_048, 1, 2, u32::MAX - 1)
                    .unwrap();
            let mut packet = Vec::new();
            encoder.encrypt_next(&[1], &mut packet).unwrap();
            assert_eq!(&packet[packet.len() - 4..], &(u32::MAX - 1).to_le_bytes());
            encoder.encrypt_next(&[1], &mut packet).unwrap();
            assert_eq!(&packet[packet.len() - 4..], &u32::MAX.to_le_bytes());
            assert_eq!(
                encoder.encrypt_next(&[1], &mut packet),
                Err(TransportCryptoFailure::NonceExhausted)
            );
        }
    }

    fn to_hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}
