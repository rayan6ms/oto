use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use thiserror::Error;

pub const RTP_HEADER_BYTES: usize = 12;
const AEAD_TAG_BYTES: usize = 16;
const NONCE_SUFFIX_BYTES: usize = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportMode {
    Aes256GcmRtpSize,
    XChaCha20Poly1305RtpSize,
}

impl TransportMode {
    #[must_use]
    pub const fn discord_name(self) -> &'static str {
        match self {
            Self::Aes256GcmRtpSize => "aead_aes256_gcm_rtpsize",
            Self::XChaCha20Poly1305RtpSize => "aead_xchacha20_poly1305_rtpsize",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedTransportPacket {
    pub sequence: u16,
    pub timestamp: u32,
    pub ssrc: u32,
    pub nonce: u32,
    pub payload: Vec<u8>,
}

pub struct TransportPacketEncoder {
    mode: TransportMode,
    key: [u8; 32],
    next_nonce: Option<u32>,
    max_payload_bytes: usize,
}

impl std::fmt::Debug for TransportPacketEncoder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TransportPacketEncoder")
            .field("mode", &self.mode)
            .field("key", &"[REDACTED]")
            .field("next_nonce", &self.next_nonce)
            .field("max_payload_bytes", &self.max_payload_bytes)
            .finish()
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TransportCryptoError {
    #[error("maximum payload size must be nonzero")]
    InvalidMaximum,
    #[error("payload has {actual} bytes, exceeding configured maximum {maximum}")]
    PayloadTooLarge { actual: usize, maximum: usize },
    #[error("transport packet is too short: {actual} bytes")]
    PacketTooShort { actual: usize },
    #[error("invalid RTP version/flags byte {actual:#04x}")]
    InvalidRtpFlags { actual: u8 },
    #[error("invalid Discord Opus payload type {actual:#04x}")]
    InvalidPayloadType { actual: u8 },
    #[error("transport nonce exhausted; reuse under this key is forbidden")]
    NonceExhausted,
    #[error("transport AEAD operation failed")]
    Aead,
}

impl TransportPacketEncoder {
    pub fn new(
        mode: TransportMode,
        key: [u8; 32],
        starting_nonce: u32,
        max_payload_bytes: usize,
    ) -> Result<Self, TransportCryptoError> {
        if max_payload_bytes == 0 {
            return Err(TransportCryptoError::InvalidMaximum);
        }
        Ok(Self {
            mode,
            key,
            next_nonce: Some(starting_nonce),
            max_payload_bytes,
        })
    }

    pub fn encrypt(
        &mut self,
        sequence: u16,
        timestamp: u32,
        ssrc: u32,
        payload: &[u8],
    ) -> Result<Vec<u8>, TransportCryptoError> {
        if payload.len() > self.max_payload_bytes {
            return Err(TransportCryptoError::PayloadTooLarge {
                actual: payload.len(),
                maximum: self.max_payload_bytes,
            });
        }
        let nonce = self
            .next_nonce
            .ok_or(TransportCryptoError::NonceExhausted)?;

        let mut header = [0_u8; RTP_HEADER_BYTES];
        header[0] = 0x80;
        header[1] = 0x78;
        header[2..4].copy_from_slice(&sequence.to_be_bytes());
        header[4..8].copy_from_slice(&timestamp.to_be_bytes());
        header[8..12].copy_from_slice(&ssrc.to_be_bytes());

        let ciphertext = encrypt_payload(self.mode, &self.key, nonce, &header, payload)?;
        let mut packet =
            Vec::with_capacity(RTP_HEADER_BYTES + ciphertext.len() + NONCE_SUFFIX_BYTES);
        packet.extend_from_slice(&header);
        packet.extend_from_slice(&ciphertext);
        packet.extend_from_slice(&nonce.to_le_bytes());

        self.next_nonce = nonce.checked_add(1);
        Ok(packet)
    }
}

pub fn decrypt_transport_packet(
    mode: TransportMode,
    key: &[u8; 32],
    packet: &[u8],
    max_payload_bytes: usize,
) -> Result<DecodedTransportPacket, TransportCryptoError> {
    if max_payload_bytes == 0 {
        return Err(TransportCryptoError::InvalidMaximum);
    }
    let minimum = RTP_HEADER_BYTES + AEAD_TAG_BYTES + NONCE_SUFFIX_BYTES;
    if packet.len() < minimum {
        return Err(TransportCryptoError::PacketTooShort {
            actual: packet.len(),
        });
    }
    if packet[0] != 0x80 {
        return Err(TransportCryptoError::InvalidRtpFlags { actual: packet[0] });
    }
    if packet[1] != 0x78 {
        return Err(TransportCryptoError::InvalidPayloadType { actual: packet[1] });
    }

    let suffix_start = packet.len() - NONCE_SUFFIX_BYTES;
    let nonce = u32::from_le_bytes(
        packet[suffix_start..]
            .try_into()
            .expect("four-byte nonce suffix"),
    );
    let payload = decrypt_payload(
        mode,
        key,
        nonce,
        &packet[..RTP_HEADER_BYTES],
        &packet[RTP_HEADER_BYTES..suffix_start],
    )?;
    if payload.len() > max_payload_bytes {
        return Err(TransportCryptoError::PayloadTooLarge {
            actual: payload.len(),
            maximum: max_payload_bytes,
        });
    }

    Ok(DecodedTransportPacket {
        sequence: u16::from_be_bytes(packet[2..4].try_into().expect("RTP sequence")),
        timestamp: u32::from_be_bytes(packet[4..8].try_into().expect("RTP timestamp")),
        ssrc: u32::from_be_bytes(packet[8..12].try_into().expect("RTP SSRC")),
        nonce,
        payload,
    })
}

fn encrypt_payload(
    mode: TransportMode,
    key: &[u8; 32],
    nonce: u32,
    header: &[u8],
    payload: &[u8],
) -> Result<Vec<u8>, TransportCryptoError> {
    let suffix = nonce.to_le_bytes();
    match mode {
        TransportMode::Aes256GcmRtpSize => {
            let mut expanded = [0_u8; 12];
            expanded[..4].copy_from_slice(&suffix);
            let nonce = Nonce::from(expanded);
            Aes256Gcm::new_from_slice(key)
                .expect("32-byte AES key")
                .encrypt(
                    &nonce,
                    Payload {
                        msg: payload,
                        aad: header,
                    },
                )
                .map_err(|_| TransportCryptoError::Aead)
        }
        TransportMode::XChaCha20Poly1305RtpSize => {
            let mut expanded = [0_u8; 24];
            expanded[..4].copy_from_slice(&suffix);
            let nonce = XNonce::from(expanded);
            XChaCha20Poly1305::new_from_slice(key)
                .expect("32-byte XChaCha key")
                .encrypt(
                    &nonce,
                    Payload {
                        msg: payload,
                        aad: header,
                    },
                )
                .map_err(|_| TransportCryptoError::Aead)
        }
    }
}

fn decrypt_payload(
    mode: TransportMode,
    key: &[u8; 32],
    nonce: u32,
    header: &[u8],
    ciphertext: &[u8],
) -> Result<Vec<u8>, TransportCryptoError> {
    let suffix = nonce.to_le_bytes();
    match mode {
        TransportMode::Aes256GcmRtpSize => {
            let mut expanded = [0_u8; 12];
            expanded[..4].copy_from_slice(&suffix);
            let nonce = Nonce::from(expanded);
            Aes256Gcm::new_from_slice(key)
                .expect("32-byte AES key")
                .decrypt(
                    &nonce,
                    Payload {
                        msg: ciphertext,
                        aad: header,
                    },
                )
                .map_err(|_| TransportCryptoError::Aead)
        }
        TransportMode::XChaCha20Poly1305RtpSize => {
            let mut expanded = [0_u8; 24];
            expanded[..4].copy_from_slice(&suffix);
            let nonce = XNonce::from(expanded);
            XChaCha20Poly1305::new_from_slice(key)
                .expect("32-byte XChaCha key")
                .decrypt(
                    &nonce,
                    Payload {
                        msg: ciphertext,
                        aad: header,
                    },
                )
                .map_err(|_| TransportCryptoError::Aead)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: [u8; 32] = [0x11; 32];

    #[test]
    fn both_modes_round_trip_exact_header_and_little_endian_suffix() {
        for mode in [
            TransportMode::Aes256GcmRtpSize,
            TransportMode::XChaCha20Poly1305RtpSize,
        ] {
            let mut encoder = TransportPacketEncoder::new(mode, KEY, 0x0102_0304, 32).unwrap();
            let packet = encoder
                .encrypt(0x1122, 0x3344_5566, 0x7788_99aa, b"opus")
                .unwrap();
            let expected = match mode {
                TransportMode::Aes256GcmRtpSize => {
                    "8078112233445566778899aa3d32aa62bea8755ac10fbec5839ed2d6dd3eaf2004030201"
                }
                TransportMode::XChaCha20Poly1305RtpSize => {
                    "8078112233445566778899aa406f7c7259ab6e4e49338bc36e55dd7c9aa0b89b04030201"
                }
            };
            assert_eq!(to_hex(&packet), expected);
            assert_eq!(&packet[..12], &hex_header());
            assert_eq!(&packet[packet.len() - 4..], &[4, 3, 2, 1]);

            let decoded = decrypt_transport_packet(mode, &KEY, &packet, 32).unwrap();
            assert_eq!(decoded.sequence, 0x1122);
            assert_eq!(decoded.timestamp, 0x3344_5566);
            assert_eq!(decoded.ssrc, 0x7788_99aa);
            assert_eq!(decoded.nonce, 0x0102_0304);
            assert_eq!(decoded.payload, b"opus");
        }
    }

    #[test]
    fn nonce_maximum_is_used_once_then_rejected() {
        let mut encoder =
            TransportPacketEncoder::new(TransportMode::Aes256GcmRtpSize, KEY, u32::MAX, 8).unwrap();
        let packet = encoder.encrypt(1, 2, 3, b"x").unwrap();
        assert_eq!(&packet[packet.len() - 4..], &u32::MAX.to_le_bytes());
        assert_eq!(
            encoder.encrypt(2, 3, 4, b"y"),
            Err(TransportCryptoError::NonceExhausted)
        );
    }

    #[test]
    fn authentication_and_bounds_fail_closed() {
        let mut encoder =
            TransportPacketEncoder::new(TransportMode::Aes256GcmRtpSize, KEY, 7, 4).unwrap();
        assert!(matches!(
            encoder.encrypt(1, 2, 3, b"large"),
            Err(TransportCryptoError::PayloadTooLarge { .. })
        ));
        let mut packet = encoder.encrypt(1, 2, 3, b"ok").unwrap();
        packet[12] ^= 1;
        assert_eq!(
            decrypt_transport_packet(TransportMode::Aes256GcmRtpSize, &KEY, &packet, 4),
            Err(TransportCryptoError::Aead)
        );
    }

    fn hex_header() -> [u8; 12] {
        [
            0x80, 0x78, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa,
        ]
    }

    fn to_hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}
