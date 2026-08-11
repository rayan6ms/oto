use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;

use crate::ConnectionGeneration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    InvalidConfiguration,
    InvalidVoiceInfo,
    RuntimeUnavailable,
    EndpointOrTls,
    CredentialsRejected,
    GatewayProtocol,
    HeartbeatTimeout,
    ResumeRejected,
    NeedsFreshVoiceInfo,
    UdpDiscovery,
    UnsupportedTransport,
    TransportCrypto,
    DaveRequired,
    DaveUnsupported,
    DaveTransition,
    FrameSourceContract,
    SendIo,
    ResourceLimit,
    Overloaded,
    Superseded,
    Shutdown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RetryDisposition {
    RetryingInternally,
    NeedsFreshVoiceInfo,
    Fatal,
    Shutdown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Operation {
    Build,
    Connect,
    ReplaceVoiceInfo,
    Resume,
    Ping,
    StartAudio,
    ReplaceSource,
    StopAudio,
    Shutdown,
}

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

    #[must_use]
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    #[must_use]
    pub fn operation(&self) -> Operation {
        self.operation
    }

    #[must_use]
    pub fn generation(&self) -> Option<ConnectionGeneration> {
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
