use std::fmt;
use std::num::NonZeroU64;
use std::time::Duration;

use crate::{DaveContext, DaveFailure, ErrorKind, Operation, RetryDisposition};

/// A Discord voice token whose debug representation is always redacted.
#[derive(Clone)]
pub struct VoiceToken(Box<str>);

impl VoiceToken {
    /// Wraps a voice token without exposing a public plaintext accessor.
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

/// Complete per-generation Discord voice connection information.
///
/// The token and voice session identifier are redacted from `Debug`. Supplying
/// a new value through [`crate::VoiceConnection::replace_voice_info`] always
/// creates a fresh connection generation.
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
    /// Creates complete voice connection information.
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

    /// Returns the Discord guild/server snowflake.
    #[must_use]
    pub fn server_id(&self) -> u64 {
        self.server_id
    }

    /// Returns the bot user's Discord snowflake.
    #[must_use]
    pub fn user_id(&self) -> u64 {
        self.user_id
    }

    /// Returns the target voice-channel snowflake.
    #[must_use]
    pub fn channel_id(&self) -> u64 {
        self.channel_id
    }

    /// Returns the Discord gateway voice session identifier.
    #[must_use]
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Returns the Discord Voice Gateway endpoint.
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
            .field("session_id", &"[REDACTED]")
            .field("endpoint", &self.endpoint)
            .field("token", &"[REDACTED]")
            .finish()
    }
}

/// A nonzero monotonically increasing voice connection generation.
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

    /// Returns the generation as a nonzero integer value.
    #[must_use]
    pub fn get(self) -> u64 {
        self.0.get()
    }
}

/// A nonzero monotonically increasing audio-source generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SourceGeneration(NonZeroU64);

impl SourceGeneration {
    pub(crate) const FIRST: Self = Self(NonZeroU64::MIN);

    pub(crate) fn next(self) -> Option<Self> {
        self.0
            .get()
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .map(Self)
    }

    /// Returns the generation as a nonzero integer value.
    #[must_use]
    pub fn get(self) -> u64 {
        self.0.get()
    }
}

/// Durable phases of a voice connection generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConnectionPhase {
    /// The control owner and initial connection are being created.
    Connecting,
    /// The Voice WebSocket handshake and Identify are in progress.
    Handshaking,
    /// UDP discovery and transport-mode selection are in progress.
    EstablishingTransport,
    /// DAVE membership and sender establishment are in progress.
    EstablishingDave,
    /// The generation is ready for idle or paced media use.
    Connected,
    /// A buffered Voice Gateway Resume is in progress.
    Resuming,
    /// The current generation is reconnecting after network loss.
    Reconnecting,
    /// Recovery requires fresh externally supplied voice information.
    NeedsFreshVoiceInfo,
    /// Explicit or terminal cleanup is in progress.
    Closing,
    /// Cleanup completed without an active control owner.
    Closed,
    /// A terminal failure closed the generation.
    Failed,
}

/// Durable phases of an attached paced audio sender.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AudioPhase {
    /// No source is attached to the connection.
    Detached,
    /// A source is attached but currently has no frame available.
    WaitingForSource,
    /// The first frame is waiting for the Speaking write barrier.
    Starting,
    /// Encoded frames are being sent on paced deadlines.
    Sending,
    /// The sender is emitting the bounded five-frame terminal silence drain.
    DrainingSilence,
    /// The sender stopped and released its source/transport ownership.
    Stopped,
    /// A terminal source or send failure stopped the sender.
    Failed,
}

/// Cumulative low-cost counters for one paced sender.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AudioStats {
    frames_sent: u64,
    silence_frames_sent: u64,
    frames_unavailable: u64,
    skipped_deadlines: u64,
    send_failures: u64,
    source_overruns: u64,
    max_lateness: Duration,
    last_source_overrun_wall: Duration,
    last_source_overrun_cpu: Duration,
}

impl AudioStats {
    pub(crate) fn from_values(
        frames_sent: u64,
        silence_frames_sent: u64,
        frames_unavailable: u64,
        skipped_deadlines: u64,
        send_failures: u64,
        source_overruns: u64,
        max_lateness: Duration,
    ) -> Self {
        Self {
            frames_sent,
            silence_frames_sent,
            frames_unavailable,
            skipped_deadlines,
            send_failures,
            source_overruns,
            max_lateness,
            ..Self::default()
        }
    }

    pub(crate) fn with_source_overrun(mut self, wall: Duration, cpu: Duration) -> Self {
        self.last_source_overrun_wall = wall;
        self.last_source_overrun_cpu = cpu;
        self
    }
    /// Returns the number of caller-supplied frames sent.
    #[must_use]
    pub fn frames_sent(self) -> u64 {
        self.frames_sent
    }
    /// Returns the number of terminal silence frames sent.
    #[must_use]
    pub fn silence_frames_sent(self) -> u64 {
        self.silence_frames_sent
    }
    /// Returns the number of paced opportunities without a ready source frame.
    #[must_use]
    pub fn frames_unavailable(self) -> u64 {
        self.frames_unavailable
    }
    /// Returns the number of stale deadlines intentionally skipped.
    #[must_use]
    pub fn skipped_deadlines(self) -> u64 {
        self.skipped_deadlines
    }
    /// Returns the number of typed UDP/media send failures.
    #[must_use]
    pub fn send_failures(self) -> u64 {
        self.send_failures
    }
    /// Returns source polls exceeding the elapsed-time budget, including
    /// descheduling. On Linux an elapsed overrun alone is not a fatal failure.
    #[must_use]
    pub fn source_overruns(self) -> u64 {
        self.source_overruns
    }
    /// Returns the elapsed duration of the latest source poll overrun, or zero
    /// if none occurred. Retained after failure for diagnosis without tracing
    /// every frame.
    #[must_use]
    pub fn last_source_overrun_wall(self) -> Duration {
        self.last_source_overrun_wall
    }
    /// Returns the thread CPU duration measured for the latest source poll
    /// overrun on Linux. Returns `None` elsewhere or before the first overrun.
    #[must_use]
    pub fn last_source_overrun_cpu(self) -> Option<Duration> {
        (cfg!(target_os = "linux") && self.source_overruns != 0)
            .then_some(self.last_source_overrun_cpu)
    }
    /// Returns the greatest observed sender deadline lateness.
    #[must_use]
    pub fn max_lateness(self) -> Duration {
        self.max_lateness
    }
}

/// Durable state and counters for one audio-source generation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AudioSnapshot {
    generation: SourceGeneration,
    phase: AudioPhase,
    failure: Option<ErrorKind>,
    dave_failure: Option<DaveFailure>,
    dave_context: Option<DaveContext>,
    stats: AudioStats,
}

impl AudioSnapshot {
    pub(crate) fn initial() -> Self {
        Self {
            generation: SourceGeneration::FIRST,
            phase: AudioPhase::WaitingForSource,
            failure: None,
            dave_failure: None,
            dave_context: None,
            stats: AudioStats::default(),
        }
    }

    /// Returns the current source generation.
    #[must_use]
    pub fn generation(&self) -> SourceGeneration {
        self.generation
    }
    /// Returns the current audio phase.
    #[must_use]
    pub fn phase(&self) -> AudioPhase {
        self.phase
    }
    /// Returns the terminal failure classification, if any.
    #[must_use]
    pub fn failure(&self) -> Option<ErrorKind> {
        self.failure
    }
    /// Returns the terminal DAVE cause, if applicable.
    #[must_use]
    pub fn dave_failure(&self) -> Option<DaveFailure> {
        self.dave_failure
    }
    /// Returns the DAVE lifecycle state observed after the failure.
    #[must_use]
    pub fn dave_context(&self) -> Option<DaveContext> {
        self.dave_context
    }

    /// Returns cumulative sender counters.
    #[must_use]
    pub fn stats(&self) -> AudioStats {
        self.stats
    }

    pub(crate) fn set_generation(&mut self, generation: SourceGeneration) {
        self.generation = generation;
        self.failure = None;
        self.dave_failure = None;
        self.dave_context = None;
    }
    pub(crate) fn set_phase(&mut self, phase: AudioPhase) {
        self.phase = phase;
    }
    pub(crate) fn set_failure(&mut self, failure: &crate::Error) {
        self.failure = Some(failure.kind());
        self.dave_failure = failure.dave_failure();
        self.dave_context = failure.dave_context();
        self.phase = AudioPhase::Failed;
    }
    pub(crate) fn set_stats(&mut self, stats: AudioStats) {
        self.stats = stats;
    }
}

/// A durable reason for a connection reaching its closed state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CloseReason {
    /// The caller requested explicit shutdown.
    ExplicitShutdown,
    /// Discord closed the Voice WebSocket cleanly without a code.
    CleanRemote,
    /// Discord closed the Voice WebSocket with the contained safe code.
    RemoteCode(u16),
    /// Fresh voice information replaced this generation.
    VoiceInfoReplaced,
    /// A terminal typed failure closed the connection.
    TerminalFailure,
}

/// Cumulative low-cost counters for one voice connection handle.
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
    /// Returns the number of full reconnect attempts.
    #[must_use]
    pub fn reconnect_attempts(self) -> u64 {
        self.reconnect_attempts
    }
    /// Returns the number of buffered Resume attempts.
    #[must_use]
    pub fn resume_attempts(self) -> u64 {
        self.resume_attempts
    }
    /// Returns the number of successful buffered Resumes.
    #[must_use]
    pub fn resume_successes(self) -> u64 {
        self.resume_successes
    }
    /// Returns the number of heartbeat acknowledgement timeouts.
    #[must_use]
    pub fn heartbeat_timeouts(self) -> u64 {
        self.heartbeat_timeouts
    }
    /// Returns the number of ignored unknown Voice Gateway opcodes.
    #[must_use]
    pub fn unknown_opcodes(self) -> u64 {
        self.unknown_opcodes
    }
    /// Returns the exact cumulative event entries skipped by subscribers.
    #[must_use]
    pub fn event_lagged(self) -> u64 {
        self.event_lagged
    }
    /// Returns the number of inbound UDP datagrams discarded after discovery.
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

/// Redaction-safe durable metadata for the last connection failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailureSnapshot {
    kind: ErrorKind,
    operation: Operation,
    generation: ConnectionGeneration,
    retry: RetryDisposition,
    safe_code: Option<u32>,
    dave_failure: Option<DaveFailure>,
    dave_context: Option<DaveContext>,
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
            dave_failure: None,
            dave_context: None,
        }
    }

    pub(crate) fn with_dave_context(mut self, context: Option<DaveContext>) -> Self {
        self.dave_context = context;
        self
    }

    pub(crate) fn with_dave_failure(mut self, failure: Option<DaveFailure>) -> Self {
        self.dave_failure = failure;
        self
    }

    /// Returns the DAVE cause, if applicable, without backend error text.
    #[must_use]
    pub fn dave_failure(&self) -> Option<DaveFailure> {
        self.dave_failure
    }
    /// Returns the DAVE lifecycle state observed after the failure.
    #[must_use]
    pub fn dave_context(&self) -> Option<DaveContext> {
        self.dave_context
    }

    /// Returns the stable failure classification.
    #[must_use]
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }
    /// Returns the operation that observed the failure.
    #[must_use]
    pub fn operation(&self) -> Operation {
        self.operation
    }
    /// Returns the affected connection generation.
    #[must_use]
    pub fn generation(&self) -> ConnectionGeneration {
        self.generation
    }
    /// Returns the retry ownership/disposition.
    #[must_use]
    pub fn retry_disposition(&self) -> RetryDisposition {
        self.retry
    }
    /// Returns a redaction-safe protocol/status code when applicable.
    #[must_use]
    pub fn safe_code(&self) -> Option<u32> {
        self.safe_code
    }
}

/// Durable connection state independent of transient event delivery.
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

    /// Returns the active or final connection generation.
    #[must_use]
    pub fn generation(&self) -> ConnectionGeneration {
        self.generation
    }
    /// Returns the current lifecycle phase.
    #[must_use]
    pub fn phase(&self) -> ConnectionPhase {
        self.phase
    }
    /// Returns the last retained failure metadata, if any.
    #[must_use]
    pub fn failure(&self) -> Option<&FailureSnapshot> {
        self.failure.as_ref()
    }
    /// Returns the durable close reason, if the connection closed.
    #[must_use]
    pub fn close_reason(&self) -> Option<CloseReason> {
        self.close_reason
    }
    /// Returns the most recent numbered heartbeat round-trip time.
    #[must_use]
    pub fn gateway_rtt(&self) -> Option<Duration> {
        self.gateway_rtt
    }
    /// Returns cumulative connection counters.
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

/// A transient bounded connection lifecycle notification.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConnectionEvent {
    /// The durable connection lifecycle phase changed.
    StateChanged {
        /// Connection generation that changed.
        generation: ConnectionGeneration,
        /// Newly committed lifecycle phase.
        phase: ConnectionPhase,
    },
    /// Buffered Resume began for the generation.
    ResumeStarted {
        /// Generation attempting Resume.
        generation: ConnectionGeneration,
    },
    /// Buffered Resume completed for the generation.
    ResumeSucceeded {
        /// Generation that retained its transport session.
        generation: ConnectionGeneration,
    },
    /// Fresh external voice information replaced a generation.
    VoiceInfoReplaced {
        /// Superseded generation.
        old: ConnectionGeneration,
        /// Newly admitted generation.
        new: ConnectionGeneration,
    },
    /// The attached audio source generation changed phase.
    AudioChanged {
        /// Source generation that changed.
        generation: SourceGeneration,
        /// Newly committed audio phase.
        phase: AudioPhase,
    },
    /// A typed connection failure was committed.
    Failure(FailureSnapshot),
    /// A durable connection close reason was committed.
    Closed(CloseReason),
}

/// Errors returned while receiving transient connection events.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventReceiveError {
    /// The subscriber fell behind the bounded ring.
    Lagged {
        /// Exact number of overwritten events skipped by this receive.
        skipped: u64,
    },
    /// The connection event publisher is closed.
    Closed,
}
