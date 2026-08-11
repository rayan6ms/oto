//! Deterministic, bounded Discord Voice test peers and packet oracles.
//!
//! This crate is test infrastructure. It intentionally contains no production Oto client.

#![forbid(unsafe_code)]

mod capture;
mod clock;
mod crypto;
mod fault;
mod gateway;
mod rng;
mod udp;

pub use capture::{CaptureError, PacketCapture, PacketRecord};
pub use clock::{ManualClock, ManualClockError};
pub use crypto::{
    DecodedTransportPacket, RTP_HEADER_BYTES, TransportCryptoError, TransportMode,
    TransportPacketEncoder, decrypt_transport_packet,
};
pub use fault::{FaultAction, FaultError, FaultInjector, ScheduledPacket};
pub use gateway::{
    FakeVoiceGateway, FakeVoiceGatewayConfig, GatewayCommandError, GatewayError, GatewayRecord,
    TestTls, VoiceClose,
};
pub use rng::{DeterministicRng, DeterministicRngError};
pub use udp::{
    DiscoveryError, DiscoveryResponse, DiscoveryResponseMutation, FakeUdpServer,
    FakeUdpServerConfig, FakeUdpStats, UdpTestkitError, build_discovery_request,
    parse_discovery_response,
};
