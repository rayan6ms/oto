#![forbid(unsafe_code)]
#![doc = "Oto's transport-only Discord voice connection core."]
#![warn(missing_docs)]

#[cfg(test)]
#[global_allocator]
static TEST_ALLOCATOR: &stats_alloc::StatsAlloc<std::alloc::System> =
    &stats_alloc::INSTRUMENTED_SYSTEM;

mod audio;
mod config;
mod connection;
mod dave;
mod error;
mod gateway;
mod model;
mod pacer;
mod transport;

#[cfg(fuzzing)]
#[doc(hidden)]
pub mod __fuzzing {
    pub fn dave_binary_envelope(input: &[u8]) {
        crate::gateway::fuzz_dave_binary_envelope(input);
    }

    pub fn dave_control_sequence(input: &[u8]) {
        crate::dave::fuzz_control_sequence(input);
    }

    pub fn gateway_json_dispatch(input: &[u8]) {
        crate::gateway::fuzz_gateway_json_dispatch(input);
    }

    pub fn ip_discovery_response(input: &[u8]) {
        crate::transport::fuzz_discovery_response(input);
    }
}

pub use audio::{FrameSource, FrameStatus, PacedAudioSender};
pub use config::{Oto, OtoBuilder, ResourceLimits};
pub use connection::{EventSubscriber, VoiceConnection};
pub use error::{Error, ErrorKind, Operation, RetryDisposition};
pub use model::{
    AudioPhase, AudioSnapshot, AudioStats, CloseReason, ConnectionEvent, ConnectionGeneration,
    ConnectionPhase, ConnectionSnapshot, ConnectionStats, EventReceiveError, FailureSnapshot,
    SourceGeneration, VoiceConnectInfo, VoiceToken,
};
