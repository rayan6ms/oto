#![forbid(unsafe_code)]
#![doc = "Oto's transport-only Discord voice connection core."]

mod config;
mod connection;
mod error;
mod gateway;
mod model;
mod transport;

pub use config::{Oto, OtoBuilder, ResourceLimits};
pub use connection::{EventSubscriber, VoiceConnection};
pub use error::{Error, ErrorKind, Operation, RetryDisposition};
pub use model::{
    AudioPhase, CloseReason, ConnectionEvent, ConnectionGeneration, ConnectionPhase,
    ConnectionSnapshot, ConnectionStats, EventReceiveError, FailureSnapshot, VoiceConnectInfo,
    VoiceToken,
};
