use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::timeout;

use crate::audio::{AudioControl, AudioSource, FrameSource, PacedAudioSender, SpawnAudio};
use crate::config::Config;
use crate::error::{Error, ErrorKind, Operation, RetryDisposition};
use crate::gateway::{self, Command};
use crate::model::{
    CloseReason, ConnectionEvent, ConnectionGeneration, ConnectionPhase, ConnectionSnapshot,
    EventReceiveError, FailureSnapshot, VoiceConnectInfo,
};

const COMMAND_TIMEOUT: Duration = Duration::from_secs(1);

/// A caller-owned Discord voice connection handle.
///
/// Clones refer to the same connection generation. Use [`Self::shutdown`] for
/// explicit asynchronous cleanup; dropping the last handle only requests
/// best-effort cancellation.
#[derive(Clone)]
pub struct VoiceConnection {
    inner: Arc<Inner>,
}

struct Inner {
    config: Arc<Config>,
    commands: mpsc::Sender<Command>,
    shutdown: watch::Sender<bool>,
    state: watch::Receiver<ConnectionSnapshot>,
    events: broadcast::Sender<ConnectionEvent>,
    subscribers: Arc<AtomicUsize>,
    event_lagged: Arc<AtomicU64>,
    subscriber_capacity: usize,
    task: Mutex<Option<JoinHandle<()>>>,
    audio_active: Arc<AtomicBool>,
    next_audio_id: AtomicU64,
    audio: Mutex<Option<Arc<AudioControl>>>,
}

struct AudioReservation {
    active: Arc<AtomicBool>,
    committed: bool,
}

impl AudioReservation {
    fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for AudioReservation {
    fn drop(&mut self) {
        if !self.committed {
            self.active.store(false, Ordering::Release);
        }
    }
}

impl std::fmt::Debug for VoiceConnection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VoiceConnection")
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.shutdown.send_replace(true);
        if let Some(task) = self
            .task
            .get_mut()
            .expect("connection task mutex poisoned")
            .take()
        {
            task.abort();
        }
    }
}

impl VoiceConnection {
    pub(crate) async fn connect(
        config: Arc<Config>,
        info: VoiceConnectInfo,
    ) -> Result<Self, Error> {
        if tokio::runtime::Handle::try_current().is_err() {
            return Err(Error::new(
                ErrorKind::RuntimeUnavailable,
                Operation::Connect,
                None,
                RetryDisposition::Fatal,
                None,
                "a running Tokio runtime is required",
            ));
        }
        let info = gateway::ValidatedInfo::new(info, Operation::Connect, None)?;
        let initial = ConnectionSnapshot::initial();
        let (state_tx, state_rx) = watch::channel(initial.clone());
        let (events, _) = broadcast::channel(config.limits.event_capacity());
        let subscriber_capacity = config.limits.event_subscriber_capacity();
        let (commands, command_rx) = mpsc::channel(config.limits.gateway_command_capacity());
        let (shutdown, shutdown_rx) = watch::channel(false);
        let (initial_tx, initial_rx) = oneshot::channel();
        let store = StateStore {
            current: initial,
            state: state_tx,
            events: events.clone(),
        };
        let task = tokio::spawn(gateway::run(
            config.clone(),
            info,
            command_rx,
            shutdown_rx,
            store,
            initial_tx,
        ));
        let connection = Self {
            inner: Arc::new(Inner {
                config,
                commands,
                shutdown,
                state: state_rx,
                events,
                subscribers: Arc::new(AtomicUsize::new(0)),
                event_lagged: Arc::new(AtomicU64::new(0)),
                subscriber_capacity,
                task: Mutex::new(Some(task)),
                audio_active: Arc::new(AtomicBool::new(false)),
                next_audio_id: AtomicU64::new(1),
                audio: Mutex::new(None),
            }),
        };

        match initial_rx.await {
            Ok(Ok(())) => Ok(connection),
            Ok(Err(error)) => {
                connection.stop_task().await;
                Err(error)
            }
            Err(_) => {
                let generation = connection.state().generation();
                connection.stop_task().await;
                Err(Error::new(
                    ErrorKind::Shutdown,
                    Operation::Connect,
                    Some(generation),
                    RetryDisposition::Shutdown,
                    None,
                    "connection control task stopped during connect",
                ))
            }
        }
    }

    /// Returns the latest durable connection state and counters.
    #[must_use]
    pub fn state(&self) -> ConnectionSnapshot {
        let mut snapshot = self.inner.state.borrow().clone();
        snapshot
            .stats_mut()
            .set_event_lagged(self.inner.event_lagged.load(Ordering::Acquire));
        snapshot
    }

    /// Subscribes to bounded transient lifecycle events.
    ///
    /// Admission fails with [`ErrorKind::ResourceLimit`] when the configured
    /// subscriber limit has been reached. Durable final state remains available
    /// through [`Self::state`] even if the subscriber later lags.
    pub fn subscribe_events(&self) -> Result<EventSubscriber, Error> {
        let mut current = self.inner.subscribers.load(Ordering::Acquire);
        loop {
            if current >= self.inner.subscriber_capacity {
                return Err(Error::new(
                    ErrorKind::ResourceLimit,
                    Operation::Connect,
                    Some(self.state().generation()),
                    RetryDisposition::Fatal,
                    None,
                    "event subscriber limit reached",
                ));
            }
            match self.inner.subscribers.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
        Ok(EventSubscriber {
            receiver: self.inner.events.subscribe(),
            subscribers: self.inner.subscribers.clone(),
            event_lagged: self.inner.event_lagged.clone(),
        })
    }

    /// Measures a numbered Voice Gateway heartbeat round trip.
    pub async fn ping(&self) -> Result<Duration, Error> {
        let (reply, response) = oneshot::channel();
        self.send_command(Command::Ping { reply }, Operation::Ping)
            .await?;
        response.await.unwrap_or_else(|_| {
            Err(Error::new(
                ErrorKind::Shutdown,
                Operation::Ping,
                Some(self.state().generation()),
                RetryDisposition::Shutdown,
                None,
                "connection closed before ping completed",
            ))
        })
    }

    /// Starts a fresh connection generation from new external voice information.
    ///
    /// The old transport is invalidated and its admitted sends are drained
    /// before the replacement is acknowledged.
    pub async fn replace_voice_info(
        &self,
        info: VoiceConnectInfo,
    ) -> Result<ConnectionGeneration, Error> {
        let generation = self.state().generation();
        let info =
            gateway::ValidatedInfo::new(info, Operation::ReplaceVoiceInfo, Some(generation))?;
        let (reply, response) = oneshot::channel();
        self.send_command(
            Command::Replace { info, reply },
            Operation::ReplaceVoiceInfo,
        )
        .await?;
        response.await.unwrap_or_else(|_| {
            Err(Error::new(
                ErrorKind::Shutdown,
                Operation::ReplaceVoiceInfo,
                Some(self.state().generation()),
                RetryDisposition::Shutdown,
                None,
                "connection closed before voice info replacement completed",
            ))
        })
    }

    /// Attaches the connection's single paced encoded-Opus sender.
    ///
    /// A source may initially be pending; Speaking is written and acknowledged
    /// internally before its first UDP media packet.
    pub async fn start_audio<S: FrameSource>(&self, source: S) -> Result<PacedAudioSender, Error> {
        self.start_audio_source(AudioSource::Callback(Box::new(source)))
            .await
    }

    /// Attaches an Oto-owned bounded channel without executing caller callbacks.
    pub async fn start_audio_channel(
        &self,
        source: crate::FrameReader,
    ) -> Result<PacedAudioSender, Error> {
        self.start_audio_source(AudioSource::Channel(source)).await
    }

    async fn start_audio_source(&self, source: AudioSource) -> Result<PacedAudioSender, Error> {
        if self
            .inner
            .audio_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(Error::new(
                ErrorKind::ResourceLimit,
                Operation::StartAudio,
                Some(self.state().generation()),
                RetryDisposition::Fatal,
                None,
                "a paced audio sender is already attached",
            ));
        }
        let reservation = AudioReservation {
            active: self.inner.audio_active.clone(),
            committed: false,
        };
        let sender = self.start_audio_reserved(source).await?;
        reservation.commit();
        Ok(sender)
    }

    async fn start_audio_reserved(&self, source: AudioSource) -> Result<PacedAudioSender, Error> {
        let audio_id = self
            .inner
            .next_audio_id
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map_err(|_| {
                Error::new(
                    ErrorKind::ResourceLimit,
                    Operation::StartAudio,
                    Some(self.state().generation()),
                    RetryDisposition::Fatal,
                    None,
                    "audio attachment identifier exhausted",
                )
            })?;
        let pacer = self
            .inner
            .config
            .pacer
            .register()
            .await
            .map_err(|failure| {
                Error::new(
                    if failure == crate::pacer::PacerFailure::Overloaded {
                        ErrorKind::Overloaded
                    } else {
                        ErrorKind::Shutdown
                    },
                    Operation::StartAudio,
                    Some(self.state().generation()),
                    RetryDisposition::Fatal,
                    None,
                    "shared pacing coordinator is unavailable",
                )
            })?;
        let (updates, transport_updates) = mpsc::channel(1);
        let (reply, response) = oneshot::channel();
        self.send_command(
            Command::AttachAudio {
                audio_id,
                updates,
                reply,
            },
            Operation::StartAudio,
        )
        .await?;
        let transport = response.await.unwrap_or_else(|_| {
            Err(Error::new(
                ErrorKind::Shutdown,
                Operation::StartAudio,
                Some(self.state().generation()),
                RetryDisposition::Shutdown,
                None,
                "gateway owner stopped before audio attachment completed",
            ))
        })?;
        let control = AudioControl::spawn(SpawnAudio {
            id: audio_id,
            source,
            transport,
            transport_updates,
            pacer,
            gateway_commands: self.inner.commands.clone(),
            connection_state: self.inner.state.clone(),
            connection_shutdown: self.inner.shutdown.subscribe(),
            events: self.inner.events.clone(),
            command_capacity: self.inner.config.limits.sender_command_capacity(),
            max_frame_bytes: self.inner.config.limits.encoded_opus_frame_bytes(),
            max_datagram_bytes: self.inner.config.limits.udp_datagram_bytes(),
            active: self.inner.audio_active.clone(),
            #[cfg(test)]
            fail_udp_sends: self.inner.config.fail_udp_sends.clone(),
        });
        *self
            .inner
            .audio
            .lock()
            .expect("audio control mutex poisoned") = Some(control.clone());
        Ok(control.sender())
    }

    /// Explicitly and idempotently shuts down audio and connection tasks.
    pub async fn shutdown(&self) -> Result<ConnectionSnapshot, Error> {
        let audio = self
            .inner
            .audio
            .lock()
            .expect("audio control mutex poisoned")
            .clone();
        if let Some(audio) = audio {
            let _ = audio.stop().await;
        }
        self.inner.shutdown.send_replace(true);
        self.stop_task().await;
        Ok(self.state())
    }

    async fn send_command(&self, command: Command, operation: Operation) -> Result<(), Error> {
        timeout(COMMAND_TIMEOUT, self.inner.commands.send(command))
            .await
            .map_err(|_| {
                Error::new(
                    ErrorKind::Overloaded,
                    operation,
                    Some(self.state().generation()),
                    RetryDisposition::Fatal,
                    None,
                    "gateway command queue remained full",
                )
            })?
            .map_err(|_| {
                Error::new(
                    ErrorKind::Shutdown,
                    operation,
                    Some(self.state().generation()),
                    RetryDisposition::Shutdown,
                    None,
                    "connection is closed",
                )
            })
    }

    async fn stop_task(&self) {
        self.inner.shutdown.send_replace(true);
        let task = self
            .inner
            .task
            .lock()
            .expect("connection task mutex poisoned")
            .take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }
}

/// A bounded receiver for transient [`ConnectionEvent`] values.
pub struct EventSubscriber {
    receiver: broadcast::Receiver<ConnectionEvent>,
    subscribers: Arc<AtomicUsize>,
    event_lagged: Arc<AtomicU64>,
}

impl std::fmt::Debug for EventSubscriber {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EventSubscriber")
            .finish_non_exhaustive()
    }
}

impl Drop for EventSubscriber {
    fn drop(&mut self) {
        self.subscribers.fetch_sub(1, Ordering::AcqRel);
    }
}

impl EventSubscriber {
    /// Waits for the next event or reports exact lag/closure.
    pub async fn recv(&mut self) -> Result<ConnectionEvent, EventReceiveError> {
        match self.receiver.recv().await {
            Ok(event) => Ok(event),
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                self.event_lagged.fetch_add(skipped, Ordering::AcqRel);
                Err(EventReceiveError::Lagged { skipped })
            }
            Err(broadcast::error::RecvError::Closed) => Err(EventReceiveError::Closed),
        }
    }
}

pub(crate) struct StateStore {
    current: ConnectionSnapshot,
    state: watch::Sender<ConnectionSnapshot>,
    events: broadcast::Sender<ConnectionEvent>,
}

impl StateStore {
    pub(crate) fn generation(&self) -> ConnectionGeneration {
        self.current.generation()
    }
    pub(crate) fn phase(&self) -> ConnectionPhase {
        self.current.phase()
    }

    pub(crate) fn phase_to(&mut self, phase: ConnectionPhase) {
        if self.current.phase() == phase {
            return;
        }
        self.current.set_phase(phase);
        self.commit();
        let _ = self.events.send(ConnectionEvent::StateChanged {
            generation: self.current.generation(),
            phase,
        });
    }

    pub(crate) fn replace_generation(&mut self, generation: ConnectionGeneration) {
        let old = self.current.generation();
        self.current.set_generation(generation);
        self.current.set_phase(ConnectionPhase::Connecting);
        self.commit();
        let _ = self.events.send(ConnectionEvent::VoiceInfoReplaced {
            old,
            new: generation,
        });
        let _ = self.events.send(ConnectionEvent::StateChanged {
            generation,
            phase: ConnectionPhase::Connecting,
        });
    }

    pub(crate) fn resume_started(&mut self) {
        self.current.stats_mut().resuming();
        self.current.set_phase(ConnectionPhase::Resuming);
        self.commit();
        let generation = self.current.generation();
        let _ = self
            .events
            .send(ConnectionEvent::ResumeStarted { generation });
        let _ = self.events.send(ConnectionEvent::StateChanged {
            generation,
            phase: ConnectionPhase::Resuming,
        });
    }

    pub(crate) fn resume_succeeded(&mut self, phase: ConnectionPhase) {
        self.current.stats_mut().resumed();
        self.current.set_phase(phase);
        self.commit();
        let generation = self.current.generation();
        let _ = self
            .events
            .send(ConnectionEvent::ResumeSucceeded { generation });
        let _ = self
            .events
            .send(ConnectionEvent::StateChanged { generation, phase });
    }

    pub(crate) fn reconnecting(&mut self) {
        self.current.stats_mut().reconnecting();
        self.phase_to(ConnectionPhase::Reconnecting);
    }

    pub(crate) fn heartbeat_timeout(&mut self) {
        self.current.stats_mut().heartbeat_timeout();
        self.commit();
    }

    pub(crate) fn unknown_opcode(&mut self) {
        self.current.stats_mut().unknown_opcode();
        self.commit();
    }

    pub(crate) fn discarded_udp_datagrams(&mut self, count: u64) {
        self.current.stats_mut().add_discarded_udp_datagrams(count);
        self.commit();
    }

    pub(crate) fn rtt(&mut self, rtt: Duration) {
        self.current.set_rtt(rtt);
        self.commit();
    }

    pub(crate) fn fail(&mut self, error: &Error, terminal_phase: ConnectionPhase) {
        let failure = FailureSnapshot::new(
            error.kind(),
            error.operation(),
            self.current.generation(),
            error.retry_disposition(),
            error.safe_code(),
        );
        self.current.set_failure(failure.clone());
        self.current.set_phase(terminal_phase);
        if terminal_phase == ConnectionPhase::Failed {
            self.current.set_close_reason(CloseReason::TerminalFailure);
        }
        self.commit();
        let _ = self.events.send(ConnectionEvent::Failure(failure));
        let _ = self.events.send(ConnectionEvent::StateChanged {
            generation: self.current.generation(),
            phase: terminal_phase,
        });
    }

    pub(crate) fn close(&mut self, reason: CloseReason) {
        self.current.set_close_reason(reason);
        self.current.set_phase(ConnectionPhase::Closed);
        self.commit();
        let _ = self.events.send(ConnectionEvent::Closed(reason));
        let _ = self.events.send(ConnectionEvent::StateChanged {
            generation: self.current.generation(),
            phase: ConnectionPhase::Closed,
        });
    }

    fn commit(&self) {
        self.state.send_replace(self.current.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancelled_audio_admission_releases_its_reservation() {
        let active = Arc::new(AtomicBool::new(true));
        drop(AudioReservation {
            active: active.clone(),
            committed: false,
        });
        assert!(!active.load(Ordering::Acquire));

        active.store(true, Ordering::Release);
        AudioReservation {
            active: active.clone(),
            committed: false,
        }
        .commit();
        assert!(active.load(Ordering::Acquire));
    }
}
