#![forbid(unsafe_code)]
#![doc = "Oto's transport-only Discord voice connection core."]

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

pub use audio::{FrameSource, FrameStatus, PacedAudioSender};
pub use config::{Oto, OtoBuilder, ResourceLimits};
pub use connection::{EventSubscriber, VoiceConnection};
pub use error::{Error, ErrorKind, Operation, RetryDisposition};
pub use model::{
    AudioPhase, AudioSnapshot, AudioStats, CloseReason, ConnectionEvent, ConnectionGeneration,
    ConnectionPhase, ConnectionSnapshot, ConnectionStats, EventReceiveError, FailureSnapshot,
    SourceGeneration, VoiceConnectInfo, VoiceToken,
};
