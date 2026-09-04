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
        }
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
