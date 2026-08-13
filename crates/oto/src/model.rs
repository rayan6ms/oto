use std::fmt;
use std::num::NonZeroU64;
use std::time::Duration;

use crate::{ErrorKind, Operation, RetryDisposition};

#[derive(Clone)]
pub struct VoiceToken(Box<str>);

impl VoiceToken {
    #[must_use]
    pub fn new(value: impl Into<Box<str>>) -> Self {
        Self(value.into())
    }

    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for VoiceToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("VoiceToken([REDACTED])")
    }
}

#[derive(Clone)]
pub struct VoiceConnectInfo {
    server_id: u64,
    user_id: u64,
    channel_id: u64,
    session_id: Box<str>,
    endpoint: Box<str>,
    token: VoiceToken,
}

impl VoiceConnectInfo {
    #[must_use]
    pub fn new(
        server_id: u64,
        user_id: u64,
        channel_id: u64,
        session_id: impl Into<Box<str>>,
        endpoint: impl Into<Box<str>>,
        token: VoiceToken,
    ) -> Self {
        Self {
            server_id,
            user_id,
            channel_id,
            session_id: session_id.into(),
            endpoint: endpoint.into(),
            token,
        }
    }

    #[must_use]
    pub fn server_id(&self) -> u64 {
        self.server_id
    }

    #[must_use]
    pub fn user_id(&self) -> u64 {
        self.user_id
    }

    #[must_use]
    pub fn channel_id(&self) -> u64 {
        self.channel_id
    }

    #[must_use]
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub(crate) fn token(&self) -> &str {
        self.token.expose()
    }
}

impl fmt::Debug for VoiceConnectInfo {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VoiceConnectInfo")
            .field("server_id", &self.server_id)
            .field("user_id", &self.user_id)
            .field("channel_id", &self.channel_id)
            .field("session_id", &self.session_id)
            .field("endpoint", &self.endpoint)
            .field("token", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConnectionGeneration(NonZeroU64);

impl ConnectionGeneration {
    pub(crate) const FIRST: Self = Self(NonZeroU64::MIN);

    pub(crate) fn next(self) -> Option<Self> {
        self.0
            .get()
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .map(Self)
    }

    #[must_use]
    pub fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConnectionPhase {
    Connecting,
    Handshaking,
    EstablishingTransport,
    EstablishingDave,
    Connected,
    Resuming,
    Reconnecting,
    NeedsFreshVoiceInfo,
    Closing,
    Closed,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AudioPhase {
    Detached,
    WaitingForSource,
    Starting,
    Sending,
    DrainingSilence,
    Stopped,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CloseReason {
    ExplicitShutdown,
    CleanRemote,
    RemoteCode(u16),
    VoiceInfoReplaced,
    TerminalFailure,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ConnectionStats {
    reconnect_attempts: u64,
    resume_attempts: u64,
    resume_successes: u64,
    heartbeat_timeouts: u64,
    unknown_opcodes: u64,
    event_lagged: u64,
    discarded_udp_datagrams: u64,
}

impl ConnectionStats {
    #[must_use]
    pub fn reconnect_attempts(self) -> u64 {
        self.reconnect_attempts
    }
    #[must_use]
    pub fn resume_attempts(self) -> u64 {
        self.resume_attempts
    }
    #[must_use]
    pub fn resume_successes(self) -> u64 {
        self.resume_successes
    }
    #[must_use]
    pub fn heartbeat_timeouts(self) -> u64 {
        self.heartbeat_timeouts
    }
    #[must_use]
    pub fn unknown_opcodes(self) -> u64 {
        self.unknown_opcodes
    }
    #[must_use]
    pub fn event_lagged(self) -> u64 {
        self.event_lagged
    }
    #[must_use]
    pub fn discarded_udp_datagrams(self) -> u64 {
        self.discarded_udp_datagrams
    }

    pub(crate) fn reconnecting(&mut self) {
        self.reconnect_attempts += 1;
    }
    pub(crate) fn resuming(&mut self) {
        self.resume_attempts += 1;
    }
    pub(crate) fn resumed(&mut self) {
        self.resume_successes += 1;
    }
    pub(crate) fn heartbeat_timeout(&mut self) {
        self.heartbeat_timeouts += 1;
    }
    pub(crate) fn unknown_opcode(&mut self) {
        self.unknown_opcodes += 1;
    }
    pub(crate) fn set_event_lagged(&mut self, count: u64) {
        self.event_lagged = count;
    }
    pub(crate) fn add_discarded_udp_datagrams(&mut self, count: u64) {
        self.discarded_udp_datagrams = self.discarded_udp_datagrams.saturating_add(count);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailureSnapshot {
    kind: ErrorKind,
    operation: Operation,
    generation: ConnectionGeneration,
    retry: RetryDisposition,
    safe_code: Option<u32>,
}

impl FailureSnapshot {
    pub(crate) fn new(
        kind: ErrorKind,
        operation: Operation,
        generation: ConnectionGeneration,
        retry: RetryDisposition,
        safe_code: Option<u32>,
    ) -> Self {
        Self {
            kind,
            operation,
            generation,
            retry,
            safe_code,
        }
    }

    #[must_use]
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }
    #[must_use]
    pub fn operation(&self) -> Operation {
        self.operation
    }
    #[must_use]
    pub fn generation(&self) -> ConnectionGeneration {
        self.generation
    }
    #[must_use]
    pub fn retry_disposition(&self) -> RetryDisposition {
        self.retry
    }
    #[must_use]
    pub fn safe_code(&self) -> Option<u32> {
        self.safe_code
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectionSnapshot {
    generation: ConnectionGeneration,
    phase: ConnectionPhase,
    failure: Option<FailureSnapshot>,
    close_reason: Option<CloseReason>,
    gateway_rtt: Option<Duration>,
    stats: ConnectionStats,
}

impl ConnectionSnapshot {
    pub(crate) fn initial() -> Self {
        Self {
            generation: ConnectionGeneration::FIRST,
            phase: ConnectionPhase::Connecting,
            failure: None,
            close_reason: None,
            gateway_rtt: None,
            stats: ConnectionStats::default(),
        }
    }

    #[must_use]
    pub fn generation(&self) -> ConnectionGeneration {
        self.generation
    }
    #[must_use]
    pub fn phase(&self) -> ConnectionPhase {
        self.phase
    }
    #[must_use]
    pub fn failure(&self) -> Option<&FailureSnapshot> {
        self.failure.as_ref()
    }
    #[must_use]
    pub fn close_reason(&self) -> Option<CloseReason> {
        self.close_reason
    }
    #[must_use]
    pub fn gateway_rtt(&self) -> Option<Duration> {
        self.gateway_rtt
    }
    #[must_use]
    pub fn stats(&self) -> ConnectionStats {
        self.stats
    }

    pub(crate) fn set_generation(&mut self, generation: ConnectionGeneration) {
        self.generation = generation;
        self.failure = None;
        self.close_reason = None;
        self.gateway_rtt = None;
    }
    pub(crate) fn set_phase(&mut self, phase: ConnectionPhase) {
        self.phase = phase;
    }
    pub(crate) fn set_failure(&mut self, failure: FailureSnapshot) {
        self.failure = Some(failure);
    }
    pub(crate) fn set_close_reason(&mut self, reason: CloseReason) {
        self.close_reason = Some(reason);
    }
    pub(crate) fn set_rtt(&mut self, rtt: Duration) {
        self.gateway_rtt = Some(rtt);
    }
    pub(crate) fn stats_mut(&mut self) -> &mut ConnectionStats {
        &mut self.stats
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConnectionEvent {
    StateChanged {
        generation: ConnectionGeneration,
        phase: ConnectionPhase,
    },
    ResumeStarted {
        generation: ConnectionGeneration,
    },
    ResumeSucceeded {
        generation: ConnectionGeneration,
    },
    VoiceInfoReplaced {
        old: ConnectionGeneration,
        new: ConnectionGeneration,
    },
    Failure(FailureSnapshot),
    Closed(CloseReason),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventReceiveError {
    Lagged { skipped: u64 },
    Closed,
}
