use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;

use crate::ConnectionGeneration;

/// Stable classifications for Oto-owned failures.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// Builder inputs are zero, inconsistent, or unsafe.
    InvalidConfiguration,
    /// Voice connection information is malformed or out of bounds.
    InvalidVoiceInfo,
    /// The operation requires a running caller-owned Tokio runtime.
    RuntimeUnavailable,
    /// Endpoint parsing, DNS, WebSocket, or TLS establishment failed.
    EndpointOrTls,
    /// Discord rejected the supplied voice credentials.
    CredentialsRejected,
    /// A Voice Gateway message or state transition violated the protocol.
    GatewayProtocol,
    /// The Voice Gateway heartbeat acknowledgement deadline expired.
    HeartbeatTimeout,
    /// Discord rejected or could not satisfy buffered Resume.
    ResumeRejected,
    /// Recovery requires a fresh external voice-state/server update.
    NeedsFreshVoiceInfo,
    /// UDP IP discovery did not produce a valid route.
    UdpDiscovery,
    /// Discord offered no current supported transport encryption mode.
    UnsupportedTransport,
    /// RTP transport encryption, key, nonce, or packet processing failed.
    TransportCrypto,
    /// The call requires DAVE but no ready encrypted sender is available.
    DaveRequired,
    /// Discord selected a DAVE version Oto cannot support.
    DaveUnsupported,
    /// DAVE setup, membership, transition, or media transformation failed.
    DaveTransition,
    /// A source blocked, returned an invalid length, or violated its frame contract.
    FrameSourceContract,
    /// Sending a UDP media packet failed.
    SendIo,
    /// A configured bounded resource limit rejected an operation.
    ResourceLimit,
    /// A bounded queue or operation deadline remained saturated.
    Overloaded,
    /// A newer lifecycle operation replaced the requested work.
    Superseded,
    /// Explicit shutdown or owner cancellation ended the operation.
    Shutdown,
}

/// Credential-free DAVE failure classifications, retained by durable snapshots.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DaveFailure {
    /// The selected protocol version is unsupported.
    UnsupportedVersion,
    /// A required encrypted call attempted a plaintext downgrade.
    RequiredDowngrade,
    /// A control payload or frame length was malformed.
    Malformed,
    /// The lifecycle state does not permit the operation.
    InvalidState,
    /// The encryption or MLS backend rejected the operation.
    Backend,
    /// The backend panicked.
    BackendPanic,
    /// The owner command could not be queued before its deadline.
    QueueTimeout,
    /// The owner did not respond before its deadline.
    ResponseTimeout,
    /// The owner command or response lane closed.
    Closed,
}

impl std::fmt::Display for DaveFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::UnsupportedVersion => "unsupported DAVE protocol version",
            Self::RequiredDowngrade => "DAVE-required call attempted a plaintext downgrade",
            Self::Malformed => "malformed DAVE control payload",
            Self::InvalidState => "invalid DAVE lifecycle transition",
            Self::Backend => "DAVE backend rejected the operation",
            Self::BackendPanic => "DAVE backend panicked while rejecting untrusted input",
            Self::QueueTimeout => "DAVE command queue remained full",
            Self::ResponseTimeout => "DAVE owner response deadline expired",
            Self::Closed => "DAVE owner task stopped",
        })
    }
}

impl std::error::Error for DaveFailure {}

/// Redaction-safe DAVE state observed when an operation failed.
/// This is a lifecycle observation, not a copy of keys or control payloads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DaveContext {
    /// Active protocol version observed by the DAVE owner.
    pub active_version: u16,
    /// Whether a transition was pending at the observation.
    pub transition_pending: bool,
    /// Whether the owner reported a ready encrypted sender.
    pub ready: bool,
}

impl From<crate::dave::Snapshot> for DaveContext {
    fn from(state: crate::dave::Snapshot) -> Self {
        Self {
            active_version: state.active_version,
            transition_pending: state.transition_id.is_some(),
            ready: state.ready,
        }
    }
}

/// What the caller should assume about retry ownership after a failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RetryDisposition {
    /// Oto is already retrying within the current connection generation.
    RetryingInternally,
    /// The caller must supply a fresh complete [`crate::VoiceConnectInfo`].
    NeedsFreshVoiceInfo,
    /// Oto will not retry this failure automatically.
    Fatal,
    /// The operation ended because the connection is shutting down.
    Shutdown,
}

/// Public operation labels attached to failures and snapshots.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Operation {
    /// Connector configuration validation.
    Build,
    /// Initial voice connection establishment.
    Connect,
    /// Fresh voice information replacement.
    ReplaceVoiceInfo,
    /// Internal buffered Resume processing.
    Resume,
    /// Voice Gateway heartbeat round-trip measurement.
    Ping,
    /// Paced sender attachment.
    StartAudio,
    /// Attached source replacement.
    ReplaceSource,
    /// Graceful paced audio stop.
    StopAudio,
    /// Explicit connection shutdown.
    Shutdown,
}

/// An Oto-owned typed failure with redaction-safe metadata.
///
/// Dependency error enums are retained only as an optional source and are not
/// part of the stable classification contract.
#[derive(Clone)]
pub struct Error {
    kind: ErrorKind,
    operation: Operation,
    generation: Option<ConnectionGeneration>,
    retry: RetryDisposition,
    safe_code: Option<u32>,
    message: &'static str,
    source: Option<Arc<dyn StdError + Send + Sync>>,
    dave_context: Option<DaveContext>,
}

impl Error {
    pub(crate) fn new(
        kind: ErrorKind,
        operation: Operation,
        generation: Option<ConnectionGeneration>,
        retry: RetryDisposition,
        safe_code: Option<u32>,
        message: &'static str,
    ) -> Self {
        Self {
            kind,
            operation,
            generation,
            retry,
            safe_code,
            message,
            source: None,
            dave_context: None,
        }
    }

    pub(crate) fn with_dave_context(mut self, context: DaveContext) -> Self {
        self.dave_context = Some(context);
        self
    }

    /// Returns the lifecycle state observed after a DAVE failure, if available.
    #[must_use]
    pub fn dave_context(&self) -> Option<DaveContext> {
        self.dave_context
    }

    pub(crate) fn with_source(mut self, source: impl StdError + Send + Sync + 'static) -> Self {
        self.source = Some(Arc::new(source));
        self
    }

    pub(crate) fn for_operation_generation(
        mut self,
        operation: Operation,
        generation: ConnectionGeneration,
    ) -> Self {
        self.operation = operation;
        self.generation = Some(generation);
        self
    }

    /// Returns the stable failure classification.
    #[must_use]
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Returns only the allowlisted DAVE cause, never arbitrary source text.
    #[must_use]
    pub fn dave_failure(&self) -> Option<DaveFailure> {
        self.source
            .as_deref()?
            .downcast_ref::<DaveFailure>()
            .copied()
    }

    /// Returns the operation that observed the failure.
    #[must_use]
    pub fn operation(&self) -> Operation {
        self.operation
    }

    /// Returns the affected connection generation when one exists.
    #[must_use]
    pub fn generation(&self) -> Option<ConnectionGeneration> {
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

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Error")
            .field("kind", &self.kind)
            .field("operation", &self.operation)
            .field("generation", &self.generation)
            .field("retry", &self.retry)
            .field("safe_code", &self.safe_code)
            .field("dave_failure", &self.dave_failure())
            .field("dave_context", &self.dave_context)
            .field("message", &self.message)
            .field("has_source", &self.source.is_some())
            .finish()
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn StdError + 'static))
    }
}

#[cfg(test)]
mod diagnostic_tests {
    use super::*;

    #[test]
    fn durable_dave_cause_survives_failure_and_resets_with_source_generation() {
        let error = Error::new(
            ErrorKind::DaveTransition,
            Operation::StartAudio,
            None,
            RetryDisposition::Fatal,
            None,
            "DAVE media encryption failed",
        )
        .with_source(DaveFailure::ResponseTimeout)
        .with_dave_context(DaveContext {
            active_version: 1,
            transition_pending: true,
            ready: false,
        });
        let mut audio = crate::AudioSnapshot::initial();
        audio.set_failure(&error);
        assert_eq!(
            audio.clone().dave_failure(),
            Some(DaveFailure::ResponseTimeout)
        );
        assert_eq!(audio.failure(), Some(ErrorKind::DaveTransition));
        assert_eq!(audio.clone().dave_context(), error.dave_context());
        let connection = crate::FailureSnapshot::new(
            error.kind(),
            error.operation(),
            ConnectionGeneration::FIRST,
            error.retry_disposition(),
            None,
        )
        .with_dave_failure(error.dave_failure())
        .with_dave_context(error.dave_context());
        assert_eq!(connection.clone().dave_failure(), audio.dave_failure());
        assert_eq!(connection.clone().dave_context(), audio.dave_context());
        audio.set_generation(crate::SourceGeneration::FIRST);
        assert_eq!(audio.dave_failure(), None);
        assert_eq!(audio.dave_context(), None);
        assert_eq!(audio.failure(), None);
    }

    #[test]
    fn arbitrary_backend_source_is_not_exposed_by_diagnostics() {
        let error = Error::new(
            ErrorKind::DaveTransition,
            Operation::Connect,
            None,
            RetryDisposition::Fatal,
            None,
            "DAVE failed",
        )
        .with_source(std::io::Error::other("SECRET_TOKEN_AND_PAYLOAD"));
        assert_eq!(error.dave_failure(), None);
        assert!(!format!("{error:?} {error}").contains("SECRET_TOKEN_AND_PAYLOAD"));
    }
}
