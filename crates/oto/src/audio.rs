use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant as StdInstant};

use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout};

use crate::dave;
use crate::error::{Error, ErrorKind, Operation, RetryDisposition};
use crate::gateway::Command as GatewayCommand;
use crate::model::{
    AudioPhase, AudioSnapshot, AudioStats, ConnectionEvent, ConnectionGeneration, ConnectionPhase,
    ConnectionSnapshot, SourceGeneration,
};
use crate::pacer::{FRAME_PERIOD, PacerFailure, PacerRegistration};
use crate::transport::{TransportCryptoFailure, TransportEncoder};

const COMMAND_TIMEOUT: Duration = Duration::from_secs(1);
const SOURCE_POLL_LIMIT: Duration = Duration::from_millis(2);
const SILENCE: [u8; 3] = [0xF8, 0xFF, 0xFE];
const SILENCE_FRAMES: u8 = 5;

pub trait FrameSource: Send + 'static {
    fn poll_frame(&mut self, cx: &mut Context<'_>, output: &mut [u8]) -> Poll<FrameStatus>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum FrameStatus {
    Frame { len: usize },
    Ended,
}

pub struct PacedAudioSender {
    control: Arc<AudioControl>,
}

impl Clone for PacedAudioSender {
    fn clone(&self) -> Self {
        Self {
            control: self.control.clone(),
        }
    }
}

impl std::fmt::Debug for PacedAudioSender {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PacedAudioSender")
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}

impl PacedAudioSender {
    #[must_use]
    pub fn state(&self) -> AudioSnapshot {
        let mut state = self.control.state.borrow().clone();
        state.set_stats(self.control.counters.snapshot());
        state
    }

    pub async fn replace_source<S: FrameSource>(
        &self,
        source: S,
    ) -> Result<SourceGeneration, Error> {
        let (reply, response) = oneshot::channel();
        self.control
            .send(
                AudioCommand::Replace {
                    source: Box::new(source),
                    reply,
                },
                Operation::ReplaceSource,
            )
            .await?;
        response.await.unwrap_or_else(|_| {
            Err(audio_error(
                ErrorKind::Shutdown,
                Operation::ReplaceSource,
                RetryDisposition::Shutdown,
                "audio sender stopped before source replacement completed",
            ))
        })
    }

    pub async fn stop(&self) -> Result<AudioSnapshot, Error> {
        self.control.stop().await
    }
}

pub(crate) struct InstalledTransport {
    pub(crate) generation: ConnectionGeneration,
    pub(crate) socket: Arc<tokio::net::UdpSocket>,
    pub(crate) encoder: TransportEncoder,
    pub(crate) dave_protocol_version: u16,
    pub(crate) dave: Option<dave::Handle>,
}

impl std::fmt::Debug for InstalledTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InstalledTransport")
            .field("generation", &self.generation)
            .field("dave_protocol_version", &self.dave_protocol_version)
            .finish_non_exhaustive()
    }
}

pub(crate) struct AudioControl {
    commands: mpsc::Sender<AudioCommand>,
    state: watch::Receiver<AudioSnapshot>,
    cancel: watch::Sender<bool>,
    task: Mutex<Option<JoinHandle<()>>>,
    active: Arc<AtomicBool>,
    counters: Arc<AudioCounters>,
}

impl Drop for AudioControl {
    fn drop(&mut self) {
        self.cancel.send_replace(true);
        if let Some(task) = self
            .task
            .get_mut()
            .expect("audio task mutex poisoned")
            .take()
        {
            task.abort();
        }
    }
}

pub(crate) struct SpawnAudio {
    pub(crate) id: u64,
    pub(crate) source: Box<dyn FrameSource>,
    pub(crate) transport: InstalledTransport,
    pub(crate) transport_updates: mpsc::Receiver<InstalledTransport>,
    pub(crate) pacer: PacerRegistration,
    pub(crate) gateway_commands: mpsc::Sender<GatewayCommand>,
    pub(crate) connection_state: watch::Receiver<ConnectionSnapshot>,
    pub(crate) connection_shutdown: watch::Receiver<bool>,
    pub(crate) events: broadcast::Sender<ConnectionEvent>,
    pub(crate) command_capacity: usize,
    pub(crate) max_frame_bytes: usize,
    pub(crate) max_datagram_bytes: usize,
    pub(crate) active: Arc<AtomicBool>,
}

impl AudioControl {
    pub(crate) fn spawn(input: SpawnAudio) -> Arc<Self> {
        let (commands, command_rx) = mpsc::channel(input.command_capacity);
        let (state_tx, state) = watch::channel(AudioSnapshot::initial());
        let (cancel, cancel_rx) = watch::channel(false);
        let active = input.active.clone();
        let counters = Arc::new(AudioCounters::default());
        let task = tokio::spawn(run_audio(
            input,
            command_rx,
            state_tx,
            cancel_rx,
            counters.clone(),
        ));
        Arc::new(Self {
            commands,
            state,
            cancel,
            task: Mutex::new(Some(task)),
            active,
            counters,
        })
    }

    pub(crate) fn sender(self: &Arc<Self>) -> PacedAudioSender {
        PacedAudioSender {
            control: self.clone(),
        }
    }

    pub(crate) async fn stop(&self) -> Result<AudioSnapshot, Error> {
        if !self.active.load(Ordering::Acquire) {
            return Ok(self.snapshot());
        }
        let (reply, response) = oneshot::channel();
        self.send(AudioCommand::Stop { reply }, Operation::StopAudio)
            .await?;
        let result = response.await.unwrap_or_else(|_| {
            Err(audio_error(
                ErrorKind::Shutdown,
                Operation::StopAudio,
                RetryDisposition::Shutdown,
                "audio sender stopped before graceful stop completed",
            ))
        });
        let task = self.task.lock().expect("audio task mutex poisoned").take();
        if let Some(task) = task {
            let _ = task.await;
        }
        result
    }

    fn snapshot(&self) -> AudioSnapshot {
        let mut snapshot = self.state.borrow().clone();
        snapshot.set_stats(self.counters.snapshot());
        snapshot
    }

    async fn send(&self, command: AudioCommand, operation: Operation) -> Result<(), Error> {
        match timeout(COMMAND_TIMEOUT, self.commands.send(command)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(audio_error(
                ErrorKind::Shutdown,
                operation,
                RetryDisposition::Shutdown,
                "audio sender is closed",
            )),
            Err(_) => Err(audio_error(
                ErrorKind::Overloaded,
                operation,
                RetryDisposition::Fatal,
                "audio sender command queue remained full",
            )),
        }
    }
}

enum AudioCommand {
    Replace {
        source: Box<dyn FrameSource>,
        reply: oneshot::Sender<Result<SourceGeneration, Error>>,
    },
    Stop {
        reply: oneshot::Sender<Result<AudioSnapshot, Error>>,
    },
}

struct WakeShared {
    latest_generation: AtomicU64,
    notify: tokio::sync::Notify,
}

struct WakeToken {
    generation: SourceGeneration,
    shared: Arc<WakeShared>,
}

impl Wake for WakeToken {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.shared
            .latest_generation
            .fetch_max(self.generation.get(), Ordering::AcqRel);
        self.shared.notify.notify_one();
    }
}

struct SourceSlot {
    source: Box<dyn FrameSource>,
    generation: SourceGeneration,
    waker: Waker,
}

impl SourceSlot {
    fn new(
        source: Box<dyn FrameSource>,
        generation: SourceGeneration,
        wake: &Arc<WakeShared>,
    ) -> Self {
        Self {
            source,
            generation,
            waker: Waker::from(Arc::new(WakeToken {
                generation,
                shared: wake.clone(),
            })),
        }
    }
}

struct AudioStore {
    current: AudioSnapshot,
    state: watch::Sender<AudioSnapshot>,
    events: broadcast::Sender<ConnectionEvent>,
    counters: Arc<AudioCounters>,
}

impl AudioStore {
    fn phase(&mut self, phase: AudioPhase) {
        if self.current.phase() == phase {
            return;
        }
        self.current.set_phase(phase);
        self.commit();
        let _ = self.events.send(ConnectionEvent::AudioChanged {
            generation: self.current.generation(),
            phase,
        });
    }

    fn replace_generation(&mut self, generation: SourceGeneration) {
        self.current.set_generation(generation);
        self.phase(AudioPhase::WaitingForSource);
    }

    fn invalidate_generation(&mut self, generation: SourceGeneration) {
        self.current.set_generation(generation);
        self.commit();
    }

    fn fail(&mut self, kind: ErrorKind) {
        self.current.set_failure(kind);
        self.commit();
        let _ = self.events.send(ConnectionEvent::AudioChanged {
            generation: self.current.generation(),
            phase: AudioPhase::Failed,
        });
    }

    fn commit(&self) {
        let mut snapshot = self.current.clone();
        snapshot.set_stats(self.counters.snapshot());
        self.state.send_replace(snapshot);
    }
}

#[derive(Default)]
struct AudioCounters {
    frames_sent: AtomicU64,
    silence_frames_sent: AtomicU64,
    frames_unavailable: AtomicU64,
    skipped_deadlines: AtomicU64,
    send_failures: AtomicU64,
    source_overruns: AtomicU64,
    max_lateness_nanos: AtomicU64,
}

impl AudioCounters {
    fn snapshot(&self) -> AudioStats {
        AudioStats::from_values(
            self.frames_sent.load(Ordering::Relaxed),
            self.silence_frames_sent.load(Ordering::Relaxed),
            self.frames_unavailable.load(Ordering::Relaxed),
            self.skipped_deadlines.load(Ordering::Relaxed),
            self.send_failures.load(Ordering::Relaxed),
            self.source_overruns.load(Ordering::Relaxed),
            Duration::from_nanos(self.max_lateness_nanos.load(Ordering::Relaxed)),
        )
    }

    fn observe_lateness(&self, lateness: Duration) {
        let nanos = lateness.as_nanos().min(u128::from(u64::MAX)) as u64;
        self.max_lateness_nanos.fetch_max(nanos, Ordering::Relaxed);
    }

    fn increment(counter: &AtomicU64) {
        let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            Some(value.saturating_add(1))
        });
    }
}

struct Executor {
    id: u64,
    source: SourceSlot,
    wake: Arc<WakeShared>,
    transport: Option<InstalledTransport>,
    transport_updates: mpsc::Receiver<InstalledTransport>,
    pacer: PacerRegistration,
    deadlines: mpsc::Receiver<Instant>,
    gateway: mpsc::Sender<GatewayCommand>,
    connection: watch::Receiver<ConnectionSnapshot>,
    connection_shutdown: watch::Receiver<bool>,
    cancel: watch::Receiver<bool>,
    commands: mpsc::Receiver<AudioCommand>,
    store: AudioStore,
    frame: Vec<u8>,
    packet: Vec<u8>,
    speaking: bool,
    active_timeline: bool,
    silence_sent: u8,
    source_ended: bool,
    stop_reply: Option<oneshot::Sender<Result<AudioSnapshot, Error>>>,
}

async fn run_audio(
    input: SpawnAudio,
    commands: mpsc::Receiver<AudioCommand>,
    state: watch::Sender<AudioSnapshot>,
    cancel: watch::Receiver<bool>,
    counters: Arc<AudioCounters>,
) {
    let wake = Arc::new(WakeShared {
        latest_generation: AtomicU64::new(0),
        notify: tokio::sync::Notify::new(),
    });
    let mut pacer = input.pacer;
    let deadlines = pacer.take_deadlines();
    let mut executor = Executor {
        id: input.id,
        source: SourceSlot::new(input.source, SourceGeneration::FIRST, &wake),
        wake,
        transport: Some(input.transport),
        transport_updates: input.transport_updates,
        pacer,
        deadlines,
        gateway: input.gateway_commands,
        connection: input.connection_state,
        connection_shutdown: input.connection_shutdown,
        cancel,
        commands,
        store: AudioStore {
            current: AudioSnapshot::initial(),
            state,
            events: input.events,
            counters,
        },
        frame: vec![0; input.max_frame_bytes],
        packet: Vec::with_capacity(input.max_datagram_bytes),
        speaking: false,
        active_timeline: false,
        silence_sent: 0,
        source_ended: false,
        stop_reply: None,
    };
    let failure = executor.run().await.err();
    if let Some(error) = &failure {
        executor.store.fail(error.kind());
    } else if executor.store.current.phase() != AudioPhase::Failed {
        executor.store.phase(AudioPhase::Stopped);
    }
    let _ = executor.pacer.unregister().await;
    executor.detach_transport().await;
    input.active.store(false, Ordering::Release);
    let result = match failure {
        Some(error) => Err(error),
        None => Ok(executor.store.current.clone()),
    };
    if let Some(reply) = executor.stop_reply.take() {
        let _ = reply.send(result);
    }
}

impl Executor {
    async fn run(&mut self) -> Result<(), Error> {
        self.try_start().await?;
        loop {
            if self.stop_reply.is_some() && !self.active_timeline {
                return Ok(());
            }
            if self.active_timeline {
                tokio::select! {
                    biased;
                    changed = self.cancel.changed() => {
                        if changed.is_err() || *self.cancel.borrow() { return Ok(()); }
                    }
                    changed = self.connection_shutdown.changed() => {
                        if changed.is_err() || *self.connection_shutdown.borrow() { return Ok(()); }
                    }
                    command = self.commands.recv() => {
                        if !self.handle_command(command).await? { return Ok(()); }
                    }
                    update = self.transport_updates.recv() => {
                        self.handle_transport(update).await?;
                    }
                    changed = self.connection.changed() => {
                        if changed.is_err() { return Ok(()); }
                        self.handle_connection_change().await?;
                    }
                    deadline = self.deadlines.recv() => {
                        let Some(deadline) = deadline else { return Err(pacer_error(PacerFailure::Closed)); };
                        self.handle_deadline(deadline).await?;
                        if self.stop_reply.is_some() && !self.active_timeline { return Ok(()); }
                    }
                }
            } else {
                tokio::select! {
                    biased;
                    changed = self.cancel.changed() => {
                        if changed.is_err() || *self.cancel.borrow() { return Ok(()); }
                    }
                    changed = self.connection_shutdown.changed() => {
                        if changed.is_err() || *self.connection_shutdown.borrow() { return Ok(()); }
                    }
                    command = self.commands.recv() => {
                        if !self.handle_command(command).await? { return Ok(()); }
                    }
                    update = self.transport_updates.recv() => {
                        self.handle_transport(update).await?;
                    }
                    changed = self.connection.changed() => {
                        if changed.is_err() { return Ok(()); }
                        self.handle_connection_change().await?;
                    }
                    _ = self.wake.notify.notified() => {
                        if self.wake.latest_generation.load(Ordering::Acquire) == self.source.generation.get() {
                            self.try_start().await?;
                        }
                    }
                }
            }
        }
    }

    async fn try_start(&mut self) -> Result<(), Error> {
        if self.stop_reply.is_some() || !self.connection_ready() || self.transport.is_none() {
            return Ok(());
        }
        match self.poll_source()? {
            Polled::Frame(len) => {
                let staged_generation = self.source.generation;
                self.store.phase(AudioPhase::Starting);
                self.set_speaking(true).await?;
                if !self.connection_ready() {
                    self.speaking = false;
                    return Ok(());
                }
                if let Ok(command) = self.commands.try_recv() {
                    self.set_speaking(false).await?;
                    let _ = self.handle_command(Some(command)).await?;
                    return Ok(());
                }
                if staged_generation != self.source.generation {
                    self.set_speaking(false).await?;
                    return Ok(());
                }
                match self.send_payload(len, false).await? {
                    SendOutcome::Sent => {
                        self.active_timeline = true;
                        self.silence_sent = 0;
                        self.source_ended = false;
                        self.store.phase(AudioPhase::Sending);
                        self.pacer
                            .activate(Instant::now() + FRAME_PERIOD)
                            .await
                            .map_err(pacer_error)?;
                    }
                    SendOutcome::Renewing => self.store.phase(AudioPhase::Starting),
                }
            }
            Polled::Pending => self.store.phase(AudioPhase::WaitingForSource),
            Polled::Ended => {
                self.source_ended = true;
                self.store.phase(AudioPhase::Stopped);
            }
        }
        Ok(())
    }

    async fn handle_deadline(&mut self, deadline: Instant) -> Result<(), Error> {
        let lateness = Instant::now().saturating_duration_since(deadline);
        self.store
            .counters
            .skipped_deadlines
            .store(self.pacer.skipped(), Ordering::Relaxed);
        self.store.counters.observe_lateness(lateness);
        if lateness >= FRAME_PERIOD || !self.connection_ready() {
            return Ok(());
        }

        let polled = if self.stop_reply.is_some() {
            Polled::Ended
        } else {
            self.poll_source()?
        };
        match polled {
            Polled::Frame(len) => {
                self.silence_sent = 0;
                self.source_ended = false;
                self.store.phase(AudioPhase::Sending);
                if self.send_payload(len, false).await? == SendOutcome::Renewing {
                    self.pause_timeline().await?;
                    self.store.phase(AudioPhase::Starting);
                }
            }
            Polled::Pending | Polled::Ended => {
                AudioCounters::increment(&self.store.counters.frames_unavailable);
                self.source_ended |= matches!(polled, Polled::Ended);
                self.store.phase(AudioPhase::DrainingSilence);
                if self.send_silence().await? == SendOutcome::Renewing {
                    self.pause_timeline().await?;
                    self.store.phase(AudioPhase::Starting);
                    return Ok(());
                }
                self.silence_sent = self.silence_sent.saturating_add(1);
                if self.silence_sent == SILENCE_FRAMES {
                    self.pause_timeline().await?;
                    self.set_speaking(false).await?;
                    if self.stop_reply.is_some() || self.source_ended {
                        self.store.phase(AudioPhase::Stopped);
                    } else {
                        self.store.phase(AudioPhase::WaitingForSource);
                    }
                }
            }
        }
        Ok(())
    }

    async fn handle_command(&mut self, command: Option<AudioCommand>) -> Result<bool, Error> {
        match command {
            Some(AudioCommand::Replace { source, reply }) => {
                let Some(generation) = self.source.generation.next() else {
                    let error = audio_error(
                        ErrorKind::ResourceLimit,
                        Operation::ReplaceSource,
                        RetryDisposition::Fatal,
                        "audio source generation exhausted",
                    );
                    let _ = reply.send(Err(error.clone()));
                    return Err(error);
                };
                self.source = SourceSlot::new(source, generation, &self.wake);
                self.source_ended = false;
                self.silence_sent = 0;
                self.store.replace_generation(generation);
                let _ = reply.send(Ok(generation));
                if !self.active_timeline {
                    self.wake
                        .latest_generation
                        .fetch_max(generation.get(), Ordering::AcqRel);
                    self.wake.notify.notify_one();
                }
                Ok(true)
            }
            Some(AudioCommand::Stop { reply }) => {
                if self.stop_reply.is_some() {
                    let _ = reply.send(Err(audio_error(
                        ErrorKind::Overloaded,
                        Operation::StopAudio,
                        RetryDisposition::Fatal,
                        "audio stop is already in progress",
                    )));
                    return Ok(true);
                }
                let Some(generation) = self.source.generation.next() else {
                    let error = audio_error(
                        ErrorKind::ResourceLimit,
                        Operation::StopAudio,
                        RetryDisposition::Fatal,
                        "audio source generation exhausted during stop",
                    );
                    let _ = reply.send(Err(error.clone()));
                    return Err(error);
                };
                self.source.generation = generation;
                self.store.invalidate_generation(generation);
                self.stop_reply = Some(reply);
                self.source_ended = true;
                if !self.active_timeline {
                    if self.speaking {
                        self.set_speaking(false).await?;
                    }
                    self.store.phase(AudioPhase::Stopped);
                    return Ok(false);
                }
                Ok(true)
            }
            None => Ok(false),
        }
    }

    async fn handle_transport(&mut self, update: Option<InstalledTransport>) -> Result<(), Error> {
        let Some(update) = update else {
            return Err(audio_error(
                ErrorKind::Shutdown,
                Operation::StartAudio,
                RetryDisposition::Shutdown,
                "audio transport update channel closed",
            ));
        };
        self.pause_timeline().await?;
        self.speaking = false;
        if update.dave_protocol_version != 0
            && !update
                .dave
                .as_ref()
                .is_some_and(|dave| dave.snapshot().ready)
        {
            self.transport = Some(update);
            return Err(audio_error(
                ErrorKind::DaveRequired,
                Operation::StartAudio,
                RetryDisposition::Fatal,
                "DAVE is required before participant media can be sent",
            ));
        }
        self.transport = Some(update);
        self.try_start().await
    }

    async fn handle_connection_change(&mut self) -> Result<(), Error> {
        if !self.connection_ready() {
            self.pause_timeline().await?;
            self.speaking = false;
            self.store.phase(AudioPhase::Starting);
        } else if !self.active_timeline {
            self.try_start().await?;
        }
        Ok(())
    }

    fn connection_ready(&self) -> bool {
        let state = self.connection.borrow();
        let Some(transport) = &self.transport else {
            return false;
        };
        state.phase() == ConnectionPhase::Connected && state.generation() == transport.generation
    }

    fn poll_source(&mut self) -> Result<Polled, Error> {
        let mut cx = Context::from_waker(&self.source.waker);
        let started = StdInstant::now();
        let result = self.source.source.poll_frame(&mut cx, &mut self.frame);
        if started.elapsed() > SOURCE_POLL_LIMIT {
            AudioCounters::increment(&self.store.counters.source_overruns);
            return Err(audio_error(
                ErrorKind::FrameSourceContract,
                Operation::StartAudio,
                RetryDisposition::Fatal,
                "FrameSource poll exceeded the non-blocking time bound",
            ));
        }
        match result {
            Poll::Ready(FrameStatus::Frame { len }) if (1..=self.frame.len()).contains(&len) => {
                Ok(Polled::Frame(len))
            }
            Poll::Ready(FrameStatus::Frame { .. }) => Err(audio_error(
                ErrorKind::FrameSourceContract,
                Operation::StartAudio,
                RetryDisposition::Fatal,
                "FrameSource returned an invalid encoded frame length",
            )),
            Poll::Ready(FrameStatus::Ended) => Ok(Polled::Ended),
            Poll::Pending => Ok(Polled::Pending),
        }
    }

    async fn send_silence(&mut self) -> Result<SendOutcome, Error> {
        self.frame[..SILENCE.len()].copy_from_slice(&SILENCE);
        let outcome = self.send_payload(SILENCE.len(), true).await?;
        if outcome == SendOutcome::Sent {
            AudioCounters::increment(&self.store.counters.silence_frames_sent);
        }
        Ok(outcome)
    }

    async fn send_payload(&mut self, len: usize, silence: bool) -> Result<SendOutcome, Error> {
        let dave = self
            .transport
            .as_ref()
            .and_then(|transport| transport.dave.clone());
        let encrypted = if let Some(dave) = dave {
            Some(dave.encrypt(&self.frame[..len]).await.map_err(|source| {
                audio_error(
                    ErrorKind::DaveTransition,
                    Operation::StartAudio,
                    RetryDisposition::Fatal,
                    "DAVE media encryption failed",
                )
                .with_source(source)
            })?)
        } else {
            None
        };
        let payload = encrypted.as_deref().unwrap_or(&self.frame[..len]);
        let Some(transport) = self.transport.as_mut() else {
            return Ok(SendOutcome::Renewing);
        };
        match transport.encoder.encrypt_next(payload, &mut self.packet) {
            Ok(()) => {}
            Err(TransportCryptoFailure::NonceExhausted) => {
                self.request_renewal().await?;
                self.transport = None;
                return Ok(SendOutcome::Renewing);
            }
            Err(_) => {
                return Err(audio_error(
                    ErrorKind::TransportCrypto,
                    Operation::StartAudio,
                    RetryDisposition::Fatal,
                    "transport encryption failed",
                ));
            }
        }
        match timeout(FRAME_PERIOD, transport.socket.send(&self.packet)).await {
            Ok(Ok(written)) if written == self.packet.len() => {
                if !silence {
                    AudioCounters::increment(&self.store.counters.frames_sent);
                }
                Ok(SendOutcome::Sent)
            }
            _ => {
                AudioCounters::increment(&self.store.counters.send_failures);
                Err(audio_error(
                    ErrorKind::SendIo,
                    Operation::StartAudio,
                    RetryDisposition::RetryingInternally,
                    "UDP audio send failed or timed out",
                ))
            }
        }
    }

    async fn set_speaking(&mut self, speaking: bool) -> Result<(), Error> {
        if self.speaking == speaking {
            return Ok(());
        }
        let generation = self
            .transport
            .as_ref()
            .map(|transport| transport.generation)
            .ok_or_else(|| {
                audio_error(
                    ErrorKind::SendIo,
                    Operation::StartAudio,
                    RetryDisposition::RetryingInternally,
                    "audio transport is unavailable",
                )
            })?;
        let (reply, response) = oneshot::channel();
        timed_gateway_send(
            &self.gateway,
            GatewayCommand::Speaking {
                audio_id: self.id,
                generation,
                speaking,
                reply,
            },
        )
        .await?;
        response.await.map_err(|_| gateway_stopped())??;
        self.speaking = speaking;
        Ok(())
    }

    async fn request_renewal(&mut self) -> Result<(), Error> {
        let generation = self
            .transport
            .as_ref()
            .expect("renewal follows an installed transport")
            .generation;
        let (reply, response) = oneshot::channel();
        timed_gateway_send(
            &self.gateway,
            GatewayCommand::RenewTransport {
                audio_id: self.id,
                generation,
                reply,
            },
        )
        .await?;
        response.await.map_err(|_| gateway_stopped())??;
        Ok(())
    }

    async fn pause_timeline(&mut self) -> Result<(), Error> {
        if self.active_timeline {
            self.pacer.deactivate().await.map_err(pacer_error)?;
            self.active_timeline = false;
        }
        Ok(())
    }

    async fn detach_transport(&mut self) {
        let Some(transport) = self.transport.take() else {
            return;
        };
        let (reply, response) = oneshot::channel();
        if timed_gateway_send(
            &self.gateway,
            GatewayCommand::DetachAudio {
                audio_id: self.id,
                transport,
                reply,
            },
        )
        .await
        .is_ok()
        {
            let _ = response.await;
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Polled {
    Frame(usize),
    Pending,
    Ended,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SendOutcome {
    Sent,
    Renewing,
}

async fn timed_gateway_send(
    sender: &mpsc::Sender<GatewayCommand>,
    command: GatewayCommand,
) -> Result<(), Error> {
    match timeout(COMMAND_TIMEOUT, sender.send(command)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(gateway_stopped()),
        Err(_) => Err(audio_error(
            ErrorKind::Overloaded,
            Operation::StartAudio,
            RetryDisposition::Fatal,
            "gateway command queue remained full during audio lifecycle work",
        )),
    }
}

fn pacer_error(failure: PacerFailure) -> Error {
    let kind = if failure == PacerFailure::Overloaded {
        ErrorKind::Overloaded
    } else {
        ErrorKind::Shutdown
    };
    audio_error(
        kind,
        Operation::StartAudio,
        RetryDisposition::Fatal,
        "shared pacing coordinator is unavailable",
    )
}

fn gateway_stopped() -> Error {
    audio_error(
        ErrorKind::Shutdown,
        Operation::StartAudio,
        RetryDisposition::Shutdown,
        "gateway owner stopped during audio lifecycle work",
    )
}

fn audio_error(
    kind: ErrorKind,
    operation: Operation,
    retry: RetryDisposition,
    message: &'static str,
) -> Error {
    Error::new(kind, operation, None, retry, None, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pacer::Pacer;
    use crate::transport::TransportMode;

    struct ReadySource;

    impl FrameSource for ReadySource {
        fn poll_frame(&mut self, _cx: &mut Context<'_>, output: &mut [u8]) -> Poll<FrameStatus> {
            output[..4].copy_from_slice(&[1, 2, 3, 4]);
            Poll::Ready(FrameStatus::Frame { len: 4 })
        }
    }

    #[tokio::test]
    async fn failed_speaking_write_never_crosses_the_first_udp_packet_barrier() {
        let sink = tokio::net::UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("UDP sink binds");
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("UDP sender binds");
        socket
            .connect(sink.local_addr().expect("sink address"))
            .await
            .expect("UDP sender connects");
        let encoder = TransportEncoder::new(
            TransportMode::Aes256GcmRtpSize,
            &[0x42; 32],
            7,
            1_275,
            2_048,
        )
        .expect("transport encoder initializes");
        let pacer = Pacer::new().register().await.expect("pacer registers");
        let (gateway, gateway_rx) = mpsc::channel(1);
        drop(gateway_rx);
        let (_transport_updates, transport_updates) = mpsc::channel(1);
        let mut connection = ConnectionSnapshot::initial();
        connection.set_phase(ConnectionPhase::Connected);
        let (_connection_tx, connection_state) = watch::channel(connection);
        let (_connection_shutdown_tx, connection_shutdown) = watch::channel(false);
        let (events, _) = broadcast::channel(8);
        let active = Arc::new(AtomicBool::new(true));
        let control = AudioControl::spawn(SpawnAudio {
            id: 1,
            source: Box::new(ReadySource),
            transport: InstalledTransport {
                generation: ConnectionGeneration::FIRST,
                socket: Arc::new(socket),
                encoder,
                dave_protocol_version: 0,
                dave: None,
            },
            transport_updates,
            pacer,
            gateway_commands: gateway,
            connection_state,
            connection_shutdown,
            events,
            command_capacity: 8,
            max_frame_bytes: 1_275,
            max_datagram_bytes: 2_048,
            active,
        });

        timeout(Duration::from_secs(1), async {
            while control.snapshot().phase() != AudioPhase::Failed {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("sender fails after Speaking write failure");
        assert_eq!(control.snapshot().failure(), Some(ErrorKind::Shutdown));
        let mut packet = [0_u8; 2_048];
        assert!(
            timeout(Duration::from_millis(20), sink.recv(&mut packet))
                .await
                .is_err(),
            "no UDP packet may cross a failed Speaking barrier"
        );
    }
}
