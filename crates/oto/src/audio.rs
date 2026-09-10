use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
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
const TRANSPORT_INVALIDATED: usize = 1 << (usize::BITS - 1);

#[cfg(target_os = "linux")]
pub(crate) fn source_cpu_time() -> Duration {
    let now = rustix::time::clock_gettime(rustix::time::ClockId::ThreadCPUTime);
    Duration::new(now.tv_sec as u64, now.tv_nsec as u32)
}

/// A non-blocking source of encoded Discord Opus frames.
///
/// Each source is polled by one logical sender at a time. A ready frame must be
/// exactly 20 ms of 48 kHz stereo Opus. When returning [`Poll::Pending`], the
/// source must register or replace the supplied waker and wake it after a state
/// change that may make a frame or the end-of-stream marker available.
///
/// Polls must not block or do expensive work. On Linux, the sender isolates
/// polls consuming more than 2 ms of thread CPU time. Elapsed-time overruns
/// remain observable, but a descheduled thread does not permanently fail audio.
/// Sleeping or waiting in the callback still violates this caller contract;
/// the CPU guard cannot enforce it. Other platforms retain elapsed-time checks.
pub trait FrameSource: Send + 'static {
    /// Polls one encoded frame into `output` without blocking.
    fn poll_frame(&mut self, cx: &mut Context<'_>, output: &mut [u8]) -> Poll<FrameStatus>;
}

pub(crate) enum AudioSource {
    Callback(Box<dyn FrameSource>),
    Channel(crate::FrameReader),
}

impl AudioSource {
    fn poll_frame(&mut self, cx: &mut Context<'_>, output: &mut [u8]) -> Poll<FrameStatus> {
        match self {
            Self::Callback(source) => source.poll_frame(cx, output),
            Self::Channel(source) => source.poll_owned(cx, output),
        }
    }
}

/// The result of a ready [`FrameSource`] poll.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum FrameStatus {
    /// One complete encoded Opus frame was written into the output buffer.
    Frame {
        /// Number of initialized frame bytes at the start of the output buffer.
        len: usize,
    },
    /// The source has permanently ended.
    Ended,
}

/// An attached, restartable 20 ms audio scheduler for one voice connection.
///
/// Clones refer to the same sender. Call [`Self::stop`] for graceful silence
/// drain; dropping all handles only requests best-effort cancellation.
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
    /// Returns the latest durable audio state and low-cost counters.
    #[must_use]
    pub fn state(&self) -> AudioSnapshot {
        let mut state = self.control.state.borrow().clone();
        state.set_stats(self.control.counters.snapshot());
        state
    }

    /// Replaces the current source and returns the newly admitted generation.
    pub async fn replace_source<S: FrameSource>(
        &self,
        source: S,
    ) -> Result<SourceGeneration, Error> {
        self.replace_audio_source(AudioSource::Callback(Box::new(source)))
            .await
    }

    /// Replaces the source with an Oto-owned bounded encoded-frame channel.
    pub async fn replace_channel(
        &self,
        source: crate::FrameReader,
    ) -> Result<SourceGeneration, Error> {
        self.replace_audio_source(AudioSource::Channel(source))
            .await
    }

    async fn replace_audio_source(&self, source: AudioSource) -> Result<SourceGeneration, Error> {
        let (reply, response) = oneshot::channel();
        self.control
            .send(
                AudioCommand::Replace { source, reply },
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

    /// Gracefully drains terminal silence and stops audio without disconnecting.
    pub async fn stop(&self) -> Result<AudioSnapshot, Error> {
        self.control.stop().await
    }
}

pub(crate) struct InstalledTransport {
    pub(crate) generation: ConnectionGeneration,
    pub(crate) socket: Arc<tokio::net::UdpSocket>,
    pub(crate) encoder: TransportEncoder,
    pub(crate) validity: Arc<TransportValidity>,
    pub(crate) dave_protocol_version: u16,
    pub(crate) dave: Option<dave::Handle>,
    pub(crate) dave_media: Option<dave::MediaEncryptor>,
}

pub(crate) struct TransportValidity {
    state: AtomicUsize,
    drained: tokio::sync::Notify,
}

impl TransportValidity {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: AtomicUsize::new(0),
            drained: tokio::sync::Notify::new(),
        })
    }

    fn try_acquire(self: &Arc<Self>) -> Option<TransportPermit> {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            if state & TRANSPORT_INVALIDATED != 0 {
                return None;
            }
            state = match self.state.compare_exchange_weak(
                state,
                state.checked_add(1)?,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(TransportPermit {
                        validity: self.clone(),
                    });
                }
                Err(observed) => observed,
            };
        }
    }

    pub(crate) async fn invalidate(&self) {
        self.state.fetch_or(TRANSPORT_INVALIDATED, Ordering::AcqRel);
        loop {
            let drained = self.drained.notified();
            tokio::pin!(drained);
            drained.as_mut().enable();
            if self.state.load(Ordering::Acquire) == TRANSPORT_INVALIDATED {
                return;
            }
            drained.await;
        }
    }
}

struct TransportPermit {
    validity: Arc<TransportValidity>,
}

impl Drop for TransportPermit {
    fn drop(&mut self) {
        let previous = self.validity.state.fetch_sub(1, Ordering::AcqRel);
        if previous == TRANSPORT_INVALIDATED | 1 {
            self.validity.drained.notify_waiters();
        }
    }
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
    pub(crate) source: AudioSource,
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
    #[cfg(test)]
    pub(crate) fail_udp_sends: Arc<AtomicBool>,
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
        let mut result = response.await.unwrap_or_else(|_| {
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
        if let Ok(snapshot) = &mut result {
            snapshot.set_stats(self.counters.snapshot());
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
        source: AudioSource,
        reply: oneshot::Sender<Result<SourceGeneration, Error>>,
    },
    Stop {
        reply: oneshot::Sender<Result<AudioSnapshot, Error>>,
    },
}

const SOURCE_POLL_ACTIVE: u8 = 1;
const SOURCE_WAKE_PENDING: u8 = 2;

struct WakeShared {
    poll_state: AtomicU8,
    latest_generation: AtomicU64,
    notify: tokio::sync::Notify,
}

impl WakeShared {
    fn begin_poll(&self) {
        self.poll_state
            .fetch_or(SOURCE_POLL_ACTIVE, Ordering::AcqRel);
    }

    fn end_poll(&self) {
        // A wake races either before this exchange (we deliver it) or after
        // it (the waker delivers it). A single atomic prevents a missed wake.
        if self.poll_state.swap(0, Ordering::AcqRel) & SOURCE_WAKE_PENDING != 0 {
            self.notify.notify_one();
        }
    }
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
        // AtomicWaker registration may synchronously invoke this waker on
        // the source callback thread. Keep Oto's Notify/scheduler work outside
        // that callback; its execution budget belongs to the source itself.
        if self
            .shared
            .poll_state
            .fetch_or(SOURCE_WAKE_PENDING, Ordering::AcqRel)
            & SOURCE_POLL_ACTIVE
            == 0
        {
            self.shared.notify.notify_one();
        }
    }
}

struct SourceSlot {
    source: AudioSource,
    generation: SourceGeneration,
    waker: Waker,
}

impl SourceSlot {
    fn new(source: AudioSource, generation: SourceGeneration, wake: &Arc<WakeShared>) -> Self {
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

    fn fail(&mut self, error: &Error) {
        self.current.set_failure(error);
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
    last_source_overrun_wall_nanos: AtomicU64,
    last_source_overrun_cpu_nanos: AtomicU64,
    active_send_gap_counts: [AtomicU64; 3],
    max_active_send_gap_nanos: AtomicU64,
    last_active_send_gap_nanos: AtomicU64,
    last_active_send_gap_unix_ms: AtomicU64,
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
        .with_source_overrun(
            Duration::from_nanos(self.last_source_overrun_wall_nanos.load(Ordering::Relaxed)),
            Duration::from_nanos(self.last_source_overrun_cpu_nanos.load(Ordering::Relaxed)),
        )
        .with_send_gaps(
            self.active_send_gap_counts
                .each_ref()
                .map(|value| value.load(Ordering::Relaxed)),
            Duration::from_nanos(self.max_active_send_gap_nanos.load(Ordering::Relaxed)),
            Duration::from_nanos(self.last_active_send_gap_nanos.load(Ordering::Relaxed)),
            self.last_active_send_gap_unix_ms.load(Ordering::Relaxed),
        )
    }

    fn observe_send_gap(&self, gap: Duration) {
        if gap < Duration::from_millis(40) {
            return;
        }
        for (counter, threshold) in self.active_send_gap_counts.iter().zip([40, 100, 1000]) {
            if gap >= Duration::from_millis(threshold) {
                Self::increment(counter);
            }
        }
        let nanos = gap.as_nanos().min(u128::from(u64::MAX)) as u64;
        self.max_active_send_gap_nanos
            .fetch_max(nanos, Ordering::Relaxed);
        self.last_active_send_gap_nanos
            .store(nanos, Ordering::Relaxed);
        let unix_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        self.last_active_send_gap_unix_ms
            .store(unix_ms, Ordering::Relaxed);
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
    dave_state: Option<watch::Receiver<dave::Snapshot>>,
    connection_shutdown: watch::Receiver<bool>,
    cancel: watch::Receiver<bool>,
    commands: mpsc::Receiver<AudioCommand>,
    store: AudioStore,
    frame: Vec<u8>,
    // The already-polled Opus frame survives a DAVE reset or transport renewal.
    // Its bytes stay in `frame`; no second allocation or unbounded queue.
    staged_frame_len: Option<usize>,
    dave_frame: Vec<u8>,
    packet: Vec<u8>,
    connection_was_ready: bool,
    speaking: bool,
    active_timeline: bool,
    last_packet_sent: Option<Instant>,
    silence_sent: u8,
    source_ended: bool,
    stop_reply: Option<oneshot::Sender<Result<AudioSnapshot, Error>>>,
    #[cfg(test)]
    fail_udp_sends: Arc<AtomicBool>,
}

async fn run_audio(
    input: SpawnAudio,
    commands: mpsc::Receiver<AudioCommand>,
    state: watch::Sender<AudioSnapshot>,
    cancel: watch::Receiver<bool>,
    counters: Arc<AudioCounters>,
) {
    let wake = Arc::new(WakeShared {
        poll_state: AtomicU8::new(0),
        latest_generation: AtomicU64::new(0),
        notify: tokio::sync::Notify::new(),
    });
    let mut pacer = input.pacer;
    let deadlines = pacer.take_deadlines();
    let mut executor = Executor {
        id: input.id,
        source: SourceSlot::new(input.source, SourceGeneration::FIRST, &wake),
        wake,
        dave_state: input.transport.dave.as_ref().map(dave::Handle::subscribe),
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
        staged_frame_len: None,
        dave_frame: Vec::new(),
        packet: Vec::with_capacity(input.max_datagram_bytes),
        connection_was_ready: false,
        speaking: false,
        active_timeline: false,
        last_packet_sent: None,
        silence_sent: 0,
        source_ended: false,
        stop_reply: None,
        #[cfg(test)]
        fail_udp_sends: input.fail_udp_sends,
    };
    let failure = executor.run().await.err().map(|error| {
        if error.dave_failure().is_some()
            && let Some(dave) = executor.transport.as_ref().and_then(|t| t.dave.as_ref())
        {
            error.with_dave_context(dave.snapshot().into())
        } else {
            error
        }
    });
    if let Some(error) = &failure {
        executor.store.fail(error);
    } else if executor.store.current.phase() != AudioPhase::Failed {
        executor.store.phase(AudioPhase::Stopped);
    }
    let _ = executor.pacer.unregister().await;
    if executor.speaking {
        let _ = executor.set_speaking(false).await;
    }
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
        self.connection_was_ready = self.connection_ready();
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
                    changed = dave_changed(&mut self.dave_state) => {
                        changed?;
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
                    changed = dave_changed(&mut self.dave_state) => {
                        changed?;
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
                    self.connection_was_ready = false;
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
                    SendOutcome::Renewing | SendOutcome::AwaitingDave => {
                        self.connection_was_ready = false;
                        self.store.phase(AudioPhase::Starting);
                    }
                }
            }
            Polled::Pending if self.speaking => self.resume_silence_drain().await?,
            Polled::Pending => self.store.phase(AudioPhase::WaitingForSource),
            Polled::Ended => {
                self.source_ended = true;
                if self.speaking {
                    self.resume_silence_drain().await?;
                } else {
                    self.store.phase(AudioPhase::Stopped);
                }
            }
        }
        Ok(())
    }

    async fn resume_silence_drain(&mut self) -> Result<(), Error> {
        self.active_timeline = true;
        self.store.phase(AudioPhase::DrainingSilence);
        self.pacer
            .activate(Instant::now() + FRAME_PERIOD)
            .await
            .map_err(pacer_error)
    }

    async fn handle_deadline(&mut self, deadline: Instant) -> Result<(), Error> {
        let lateness = Instant::now().saturating_duration_since(deadline);
        self.store
            .counters
            .skipped_deadlines
            .store(self.pacer.skipped(), Ordering::Relaxed);
        self.store.counters.observe_lateness(lateness);
        if !self.connection_ready() {
            return Ok(());
        }

        // A delayed opportunity still permits one current frame. Dropping the
        // opportunity here adds another timer wait after a host scheduling
        // stall. The completion path below rebases the next deadline and
        // discards a queued old tick, so recovery cannot burst a backlog.

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
                if self.send_payload(len, false).await? != SendOutcome::Sent {
                    self.connection_was_ready = false;
                    self.pause_timeline().await?;
                    self.store.phase(AudioPhase::Starting);
                }
            }
            Polled::Pending | Polled::Ended => {
                AudioCounters::increment(&self.store.counters.frames_unavailable);
                self.source_ended |= matches!(polled, Polled::Ended);
                self.store.phase(AudioPhase::DrainingSilence);
                if self.send_silence().await? != SendOutcome::Sent {
                    self.connection_was_ready = false;
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
        // The opportunity itself, polling, or encryption may be late.
        // Preserve that valid frame, but start the next opportunity a
        // full period from completion instead of consuming a queued old tick.
        if self.active_timeline {
            let completed = Instant::now();
            let lateness = completed.saturating_duration_since(deadline);
            if lateness >= FRAME_PERIOD {
                self.store.counters.observe_lateness(lateness);
                self.pacer
                    .activate(completed + FRAME_PERIOD)
                    .await
                    .map_err(pacer_error)?;
                let _ = self.deadlines.try_recv();
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
                self.staged_frame_len = None;
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
                self.staged_frame_len = None;
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
        self.dave_state = update.dave.as_ref().map(dave::Handle::subscribe);
        if update.dave_protocol_version != 0
            && !update
                .dave
                .as_ref()
                .is_some_and(|dave| dave.snapshot().ready)
        {
            self.transport = Some(update);
            self.connection_was_ready = self.connection_ready();
            // A fresh connection generation can install its transport before
            // the DAVE control plane reaches Connected. Keep the sender
            // attached but paused; connection readiness below prevents any
            // plaintext or prematurely encrypted packet from escaping. The
            // connection watch will wake us once Execute Transition completes.
            self.store.phase(AudioPhase::Starting);
            return Ok(());
        }
        self.transport = Some(update);
        self.connection_was_ready = self.connection_ready();
        self.try_start().await
    }

    async fn handle_connection_change(&mut self) -> Result<(), Error> {
        let ready = self.connection_ready();
        let became_ready = ready && !self.connection_was_ready;
        self.connection_was_ready = ready;
        if !ready {
            self.pause_timeline().await?;
            // A same-transport DAVE rekey does not clear the gateway's Speaking
            // bit. Preserve our knowledge of it so Stop still clears it and
            // resumption needs no redundant Speaking round trip.
            let state = self.connection.borrow();
            if !matches!(
                state.phase(),
                ConnectionPhase::Connected | ConnectionPhase::EstablishingDave
            ) || !self
                .transport
                .as_ref()
                .is_some_and(|t| t.generation == state.generation())
            {
                self.speaking = false;
            }
            self.store.phase(AudioPhase::Starting);
        } else if became_ready && !self.active_timeline {
            self.try_start().await?;
        }
        Ok(())
    }

    fn connection_ready(&self) -> bool {
        let state = self.connection.borrow();
        let Some(transport) = &self.transport else {
            return false;
        };
        state.phase() == ConnectionPhase::Connected
            && state.generation() == transport.generation
            && (transport.dave_protocol_version == 0
                || self
                    .dave_state
                    .as_ref()
                    .is_some_and(|state| state.borrow().ready))
    }

    fn poll_source(&mut self) -> Result<Polled, Error> {
        if let Some(len) = self.staged_frame_len {
            return Ok(Polled::Frame(len));
        }
        let mut cx = Context::from_waker(&self.source.waker);
        self.wake.begin_poll();
        #[cfg(target_os = "linux")]
        let cpu_started =
            matches!(self.source.source, AudioSource::Callback(_)).then(source_cpu_time);
        let started = StdInstant::now();
        let result = self.source.source.poll_frame(&mut cx, &mut self.frame);
        let wall_elapsed = started.elapsed();
        if wall_elapsed > SOURCE_POLL_LIMIT
            && matches!(self.source.source, AudioSource::Callback(_))
        {
            // Wall time includes host/VM descheduling. Only attribute an
            // expensive callback to the source when the thread actually ran.
            // The second CPU-clock syscall is needed only on an overrun.
            #[cfg(target_os = "linux")]
            let cpu_elapsed =
                source_cpu_time().saturating_sub(cpu_started.expect("callback clock sampled"));
            #[cfg(not(target_os = "linux"))]
            let cpu_elapsed = Duration::ZERO;
            self.store.counters.last_source_overrun_wall_nanos.store(
                wall_elapsed.as_nanos().min(u128::from(u64::MAX)) as u64,
                Ordering::Relaxed,
            );
            self.store.counters.last_source_overrun_cpu_nanos.store(
                cpu_elapsed.as_nanos().min(u128::from(u64::MAX)) as u64,
                Ordering::Relaxed,
            );
            AudioCounters::increment(&self.store.counters.source_overruns);
            #[cfg(target_os = "linux")]
            let violated = cpu_elapsed > SOURCE_POLL_LIMIT;
            #[cfg(not(target_os = "linux"))]
            let violated = true;
            if violated {
                self.wake.end_poll();
                return Err(audio_error(
                    ErrorKind::FrameSourceContract,
                    Operation::StartAudio,
                    RetryDisposition::Fatal,
                    "FrameSource poll exceeded its execution time bound",
                ));
            }
        }
        self.wake.end_poll();
        match result {
            Poll::Ready(FrameStatus::Frame { len }) if (1..=self.frame.len()).contains(&len) => {
                self.staged_frame_len = Some(len);
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
        let Some(_transport_permit) = self
            .transport
            .as_ref()
            .and_then(|transport| transport.validity.try_acquire())
        else {
            self.transport = None;
            return Ok(SendOutcome::Renewing);
        };
        let dave_media = self
            .transport
            .as_mut()
            .and_then(|transport| transport.dave_media.as_mut());
        let dave_payload = if let Some(dave_media) = dave_media {
            let frame = std::mem::take(&mut self.frame);
            let output = std::mem::take(&mut self.dave_frame);
            let buffers = dave_media
                .encrypt_buffered(frame, len, output)
                .await
                .map_err(|source| {
                    audio_error(
                        ErrorKind::DaveTransition,
                        Operation::StartAudio,
                        RetryDisposition::Fatal,
                        "DAVE media encryption owner failed",
                    )
                    .with_source(source)
                })?;
            self.frame = buffers.frame;
            self.dave_frame = buffers.output;
            let outcome = buffers.result.map_err(|source| {
                audio_error(
                    ErrorKind::DaveTransition,
                    Operation::StartAudio,
                    RetryDisposition::Fatal,
                    "DAVE media encryption failed",
                )
                .with_source(source)
            })?;
            if outcome == dave::MediaOutcome::NotReady {
                return Ok(SendOutcome::AwaitingDave);
            }
            Some(self.dave_frame.as_slice())
        } else {
            None
        };
        let payload = dave_payload.unwrap_or(&self.frame[..len]);
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
        #[cfg(test)]
        if self.fail_udp_sends.load(Ordering::Acquire) {
            AudioCounters::increment(&self.store.counters.send_failures);
            return Err(audio_error(
                ErrorKind::SendIo,
                Operation::StartAudio,
                RetryDisposition::RetryingInternally,
                "UDP audio send failed or timed out",
            ));
        }
        match timeout(FRAME_PERIOD, transport.socket.send(&self.packet)).await {
            Ok(Ok(written)) if written == self.packet.len() => {
                self.staged_frame_len = None;
                let now = Instant::now();
                if let Some(previous) = self.last_packet_sent.replace(now) {
                    self.store
                        .counters
                        .observe_send_gap(now.saturating_duration_since(previous));
                }
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
        self.last_packet_sent = None;
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
    AwaitingDave,
}

async fn dave_changed(state: &mut Option<watch::Receiver<dave::Snapshot>>) -> Result<(), Error> {
    let Some(state) = state else {
        return std::future::pending().await;
    };
    state.changed().await.map_err(|_| {
        audio_error(
            ErrorKind::DaveTransition,
            Operation::StartAudio,
            RetryDisposition::Fatal,
            "DAVE readiness owner stopped",
        )
        .with_source(crate::DaveFailure::Closed)
    })
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
    #[test]
    fn send_gap_thresholds_retain_cumulative_counts_and_latest_time() {
        let counters = AudioCounters::default();
        for ms in [20, 39, 40, 99, 100, 999, 1000] {
            counters.observe_send_gap(Duration::from_millis(ms));
        }
        let stats = counters.snapshot();
        assert_eq!(stats.active_send_gap_counts(), [5, 3, 1]);
        assert_eq!(stats.last_active_send_gap().0, Duration::from_secs(1));
        assert!(stats.last_active_send_gap().1 > 0);
    }

    use super::*;
    use crate::pacer::Pacer;
    use crate::transport::TransportMode;
    use oto_testkit::{TransportMode as OracleMode, decrypt_transport_packet};

    struct ReadySource;

    impl FrameSource for ReadySource {
        fn poll_frame(&mut self, _cx: &mut Context<'_>, output: &mut [u8]) -> Poll<FrameStatus> {
            output[..4].copy_from_slice(&[1, 2, 3, 4]);
            Poll::Ready(FrameStatus::Frame { len: 4 })
        }
    }

    struct OneFrameSource {
        frame: Option<Vec<u8>>,
    }

    impl FrameSource for OneFrameSource {
        fn poll_frame(&mut self, _cx: &mut Context<'_>, output: &mut [u8]) -> Poll<FrameStatus> {
            let Some(frame) = self.frame.take() else {
                return Poll::Ready(FrameStatus::Ended);
            };
            output[..frame.len()].copy_from_slice(&frame);
            Poll::Ready(FrameStatus::Frame { len: frame.len() })
        }
    }

    struct MaximumFrameSource;

    impl FrameSource for MaximumFrameSource {
        fn poll_frame(&mut self, _cx: &mut Context<'_>, output: &mut [u8]) -> Poll<FrameStatus> {
            output.fill(0x55);
            Poll::Ready(FrameStatus::Frame { len: output.len() })
        }
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn source_waker_defers_notifier_work_until_callback_finishes() {
        use std::future::Future;
        struct SlowScheduler(AtomicUsize);
        impl Wake for SlowScheduler {
            fn wake(self: Arc<Self>) {
                self.wake_by_ref();
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
                let started = source_cpu_time();
                while source_cpu_time().saturating_sub(started) < Duration::from_millis(5) {
                    std::hint::spin_loop();
                }
            }
        }
        let shared = Arc::new(WakeShared {
            poll_state: AtomicU8::new(0),
            latest_generation: AtomicU64::new(0),
            notify: tokio::sync::Notify::new(),
        });
        let scheduler = Arc::new(SlowScheduler(AtomicUsize::new(0)));
        let scheduler_waker = Waker::from(scheduler.clone());
        let mut notified = std::pin::pin!(shared.notify.notified());
        assert!(
            notified
                .as_mut()
                .poll(&mut Context::from_waker(&scheduler_waker))
                .is_pending()
        );
        let source_waker = Waker::from(Arc::new(WakeToken {
            generation: SourceGeneration::FIRST,
            shared: shared.clone(),
        }));
        shared.begin_poll();
        let started = source_cpu_time();
        source_waker.wake_by_ref();
        let elapsed = source_cpu_time().saturating_sub(started);
        assert_eq!(
            scheduler.0.load(Ordering::Relaxed),
            0,
            "source wake must not execute Oto's scheduler inline"
        );
        assert!(elapsed < SOURCE_POLL_LIMIT);
        shared.end_poll();
        assert_eq!(
            scheduler.0.load(Ordering::Relaxed),
            1,
            "deferred wake must be delivered"
        );
        assert!(
            notified
                .as_mut()
                .poll(&mut Context::from_waker(&scheduler_waker))
                .is_ready()
        );
        println!("deferred source wake: {} us CPU", elapsed.as_micros());
    }

    #[test]
    fn source_wake_racing_poll_exit_is_never_lost() {
        use std::future::Future;
        let shared = Arc::new(WakeShared {
            poll_state: AtomicU8::new(0),
            latest_generation: AtomicU64::new(0),
            notify: tokio::sync::Notify::new(),
        });
        let token = Waker::from(Arc::new(WakeToken {
            generation: SourceGeneration::FIRST,
            shared: shared.clone(),
        }));
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                for _ in 0..1000 {
                    barrier.wait();
                    token.wake_by_ref();
                    barrier.wait();
                }
            });
            for _ in 0..1000 {
                shared.begin_poll();
                barrier.wait();
                shared.end_poll();
                barrier.wait();
                let mut notified = std::pin::pin!(shared.notify.notified());
                assert!(
                    notified
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_ready()
                );
            }
        });
    }

    async fn connected_udp_pair() -> (tokio::net::UdpSocket, tokio::net::UdpSocket) {
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
        (sink, socket)
    }

    fn connected_state() -> (
        watch::Sender<ConnectionSnapshot>,
        watch::Receiver<ConnectionSnapshot>,
    ) {
        let mut connection = ConnectionSnapshot::initial();
        connection.set_phase(ConnectionPhase::Connected);
        watch::channel(connection)
    }

    #[tokio::test(start_paused = true)]
    async fn overdue_opportunity_recovers_one_frame_without_waiting_or_catchup() {
        let (_sink, socket) = connected_udp_pair().await;
        let owner = Pacer::new();
        let mut pacer = owner.register().await.unwrap();
        let deadlines = pacer.take_deadlines();
        let wake = Arc::new(WakeShared {
            poll_state: AtomicU8::new(0),
            latest_generation: AtomicU64::new(0),
            notify: tokio::sync::Notify::new(),
        });
        let (_connection_tx, connection) = connected_state();
        let (state, _) = watch::channel(AudioSnapshot::initial());
        let (events, _) = broadcast::channel(8);
        let (gateway, _gateway_rx) = mpsc::channel(8);
        let (_transport_tx, transport_updates) = mpsc::channel(1);
        let (_command_tx, commands) = mpsc::channel(1);
        let (_cancel_tx, cancel) = watch::channel(false);
        let (_shutdown_tx, connection_shutdown) = watch::channel(false);
        let counters = Arc::new(AudioCounters::default());
        let mut executor = Executor {
            id: 1,
            source: SourceSlot::new(
                AudioSource::Callback(Box::new(ReadySource)),
                SourceGeneration::FIRST,
                &wake,
            ),
            wake,
            transport: Some(InstalledTransport {
                generation: ConnectionGeneration::FIRST,
                socket: Arc::new(socket),
                encoder: TransportEncoder::new(
                    TransportMode::Aes256GcmRtpSize,
                    &[0x42; 32],
                    7,
                    1_275,
                    2_048,
                )
                .unwrap(),
                validity: TransportValidity::new(),
                dave_protocol_version: 0,
                dave: None,
                dave_media: None,
            }),
            transport_updates,
            pacer,
            deadlines,
            gateway,
            connection,
            dave_state: None,
            connection_shutdown,
            cancel,
            commands,
            store: AudioStore {
                current: AudioSnapshot::initial(),
                state,
                events,
                counters: counters.clone(),
            },
            frame: vec![0; 1_275],
            staged_frame_len: None,
            dave_frame: Vec::new(),
            packet: Vec::with_capacity(2_048),
            connection_was_ready: true,
            speaking: true,
            active_timeline: true,
            last_packet_sent: None,
            silence_sent: 0,
            source_ended: false,
            stop_reply: None,
            fail_udp_sends: Arc::new(AtomicBool::new(false)),
        };
        // A ready source has no queued pacing work yet. Simulate waking after
        // the intended deadline, including a delay spanning multiple frames.
        for delay in [Duration::from_millis(37), Duration::from_millis(137)] {
            tokio::time::advance(delay + FRAME_PERIOD).await;
            let before = counters.frames_sent.load(Ordering::Relaxed);
            executor
                .handle_deadline(Instant::now() - delay)
                .await
                .unwrap();
            assert_eq!(
                counters.frames_sent.load(Ordering::Relaxed),
                before + 1,
                "recovery should send once now instead of adding another timer wait"
            );
            assert!(
                executor.deadlines.try_recv().is_err(),
                "no catch-up tick retained"
            );
            tokio::time::advance(FRAME_PERIOD - Duration::from_millis(1)).await;
            assert!(
                executor.deadlines.try_recv().is_err(),
                "next frame must wait a full period"
            );
            tokio::time::advance(Duration::from_millis(1)).await;
            tokio::task::yield_now().await;
            let next = executor
                .deadlines
                .try_recv()
                .expect("next normally spaced opportunity");
            executor.handle_deadline(next).await.unwrap();
            assert_eq!(counters.frames_sent.load(Ordering::Relaxed), before + 2);
        }
        executor.pacer.unregister().await.unwrap();
        drop(executor);
        drop(owner);
    }

    #[tokio::test]
    async fn failed_speaking_write_never_crosses_the_first_udp_packet_barrier() {
        let (sink, socket) = connected_udp_pair().await;
        let encoder = TransportEncoder::new(
            TransportMode::Aes256GcmRtpSize,
            &[0x42; 32],
            7,
            1_275,
            2_048,
        )
        .expect("transport encoder initializes");
        let pacer_owner = Pacer::new();
        let pacer = pacer_owner.register().await.expect("pacer registers");
        let (gateway, gateway_rx) = mpsc::channel(1);
        drop(gateway_rx);
        let (_transport_updates, transport_updates) = mpsc::channel(1);
        let (_connection_tx, connection_state) = connected_state();
        let (_connection_shutdown_tx, connection_shutdown) = watch::channel(false);
        let (events, _) = broadcast::channel(8);
        let active = Arc::new(AtomicBool::new(true));
        let control = AudioControl::spawn(SpawnAudio {
            id: 1,
            source: AudioSource::Callback(Box::new(ReadySource)),
            transport: InstalledTransport {
                generation: ConnectionGeneration::FIRST,
                socket: Arc::new(socket),
                encoder,
                validity: TransportValidity::new(),
                dave_protocol_version: 0,
                dave: None,
                dave_media: None,
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
            fail_udp_sends: Arc::new(AtomicBool::new(false)),
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

    #[tokio::test]
    async fn complete_paced_path_dave_encrypts_maximum_opus_and_all_terminal_silence() {
        verify_complete_paced_dave_path(false).await;
    }

    #[tokio::test]
    async fn owned_channel_preserves_dave_and_exact_terminal_silence() {
        verify_complete_paced_dave_path(true).await;
    }

    async fn verify_complete_paced_dave_path(channel: bool) {
        const KEY: [u8; 32] = [0x42; 32];
        let normal_frame = vec![0x55; 1_275];
        let (sink, socket) = connected_udp_pair().await;
        let encoder = TransportEncoder::new(
            TransportMode::Aes256GcmRtpSize,
            &KEY,
            7,
            1_275 + dave::OPUS_MAX_ENCRYPTION_OVERHEAD_BYTES,
            2_048,
        )
        .expect("transport encoder includes DAVE expansion");
        let dave = dave::Handle::spawn_ready_fixture(8);
        let dave_media = dave.media_encryptor();
        let pacer_owner = Pacer::new();
        let pacer = pacer_owner.register().await.expect("pacer registers");
        let (gateway, mut gateway_rx) = mpsc::channel(8);
        let speaking = Arc::new(Mutex::new(Vec::new()));
        let speaking_observer = speaking.clone();
        let gateway_task = tokio::spawn(async move {
            while let Some(command) = gateway_rx.recv().await {
                match command {
                    GatewayCommand::Speaking {
                        speaking, reply, ..
                    } => {
                        speaking_observer
                            .lock()
                            .expect("speaking record mutex")
                            .push(speaking);
                        let _ = reply.send(Ok(()));
                    }
                    GatewayCommand::DetachAudio { reply, .. } => {
                        let _ = reply.send(());
                        break;
                    }
                    _ => panic!("unexpected gateway command in paced DAVE test"),
                }
            }
        });
        let (transport_updates_tx, transport_updates) = mpsc::channel(1);
        let (connection_tx, connection_state) = connected_state();
        let (connection_shutdown_tx, connection_shutdown) = watch::channel(false);
        let (events, _) = broadcast::channel(8);
        let active = Arc::new(AtomicBool::new(true));
        let control = AudioControl::spawn(SpawnAudio {
            id: 1,
            source: if channel {
                let (mut writer, reader) = crate::frame_channel();
                // Publish then cancel the send and close: the queued maximum
                // frame must still precede EOF and the exact silence drain.
                let mut send = Box::pin(writer.send(&normal_frame));
                use std::future::Future;
                assert!(
                    send.as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                drop(send);
                drop(writer);
                AudioSource::Channel(reader)
            } else {
                AudioSource::Callback(Box::new(OneFrameSource {
                    frame: Some(normal_frame.clone()),
                }))
            },
            transport: InstalledTransport {
                generation: ConnectionGeneration::FIRST,
                socket: Arc::new(socket),
                encoder,
                validity: TransportValidity::new(),
                dave_protocol_version: 1,
                dave: Some(dave),
                dave_media: Some(dave_media),
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
            fail_udp_sends: Arc::new(AtomicBool::new(false)),
        });

        let mut observed = Vec::new();
        let mut packet = [0_u8; 2_048];
        for index in 0..1 + usize::from(SILENCE_FRAMES) {
            let length = match timeout(Duration::from_secs(2), sink.recv(&mut packet)).await {
                Ok(result) => result.expect("UDP packet is readable"),
                Err(_) => panic!(
                    "paced packet {index} did not arrive; sender state: {:?}",
                    control.snapshot()
                ),
            };
            let decoded = decrypt_transport_packet(
                OracleMode::Aes256GcmRtpSize,
                &KEY,
                &packet[..length],
                1_275 + dave::OPUS_MAX_ENCRYPTION_OVERHEAD_BYTES,
            )
            .expect("transport layer decrypts");
            observed.push(decoded.payload);
        }

        assert_ne!(observed[0], normal_frame);
        assert!(observed[0].len() > normal_frame.len());
        for encrypted_silence in &observed[1..] {
            assert_ne!(encrypted_silence.as_slice(), SILENCE);
            assert!(encrypted_silence.len() > SILENCE.len());
        }
        assert!(
            observed
                .iter()
                .all(|payload| payload.ends_with(&[0xFA, 0xFA]))
        );
        assert!(
            timeout(Duration::from_millis(40), sink.recv(&mut packet))
                .await
                .is_err(),
            "the terminal drain is exactly five silence frames"
        );
        timeout(Duration::from_secs(1), async {
            while control.snapshot().stats().silence_frames_sent() != u64::from(SILENCE_FRAMES) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("silence drain completes");
        control.stop().await.expect("sender stops cleanly");
        gateway_task.await.expect("gateway responder completes");
        drop((
            transport_updates_tx,
            connection_tx,
            connection_shutdown_tx,
            pacer_owner,
        ));
        assert_eq!(
            *speaking.lock().expect("speaking record mutex"),
            [true, false]
        );
    }

    #[tokio::test]
    async fn unready_dave_waits_without_plaintext_and_stop_remains_responsive() {
        let (sink, socket) = connected_udp_pair().await;
        let encoder = TransportEncoder::new(
            TransportMode::Aes256GcmRtpSize,
            &[0x42; 32],
            7,
            1_275 + dave::OPUS_MAX_ENCRYPTION_OVERHEAD_BYTES,
            2_048,
        )
        .expect("transport encoder initializes");
        let dave = dave::Handle::spawn(7, 9, 8).expect("unready DAVE owner starts");
        let dave_media = dave.media_encryptor();
        let pacer_owner = Pacer::new();
        let pacer = pacer_owner.register().await.expect("pacer registers");
        let (gateway, mut gateway_rx) = mpsc::channel(8);
        let speaking = Arc::new(Mutex::new(Vec::new()));
        let speaking_observer = speaking.clone();
        let gateway_task = tokio::spawn(async move {
            while let Some(command) = gateway_rx.recv().await {
                match command {
                    GatewayCommand::Speaking {
                        speaking, reply, ..
                    } => {
                        speaking_observer
                            .lock()
                            .expect("speaking record mutex")
                            .push(speaking);
                        let _ = reply.send(Ok(()));
                    }
                    GatewayCommand::DetachAudio { reply, .. } => {
                        let _ = reply.send(());
                        break;
                    }
                    _ => panic!("unexpected gateway command in DAVE failure test"),
                }
            }
        });
        let (transport_updates_tx, transport_updates) = mpsc::channel(1);
        let (connection_tx, connection_state) = connected_state();
        let (connection_shutdown_tx, connection_shutdown) = watch::channel(false);
        let (events, _) = broadcast::channel(8);
        let control = AudioControl::spawn(SpawnAudio {
            id: 2,
            source: AudioSource::Callback(Box::new(ReadySource)),
            transport: InstalledTransport {
                generation: ConnectionGeneration::FIRST,
                socket: Arc::new(socket),
                encoder,
                validity: TransportValidity::new(),
                dave_protocol_version: 1,
                dave: Some(dave),
                dave_media: Some(dave_media),
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
            active: Arc::new(AtomicBool::new(true)),
            fail_udp_sends: Arc::new(AtomicBool::new(false)),
        });

        tokio::time::sleep(Duration::from_millis(40)).await;
        assert_ne!(control.snapshot().phase(), AudioPhase::Failed);
        assert_eq!(control.snapshot().stats().frames_sent(), 0);
        timeout(Duration::from_millis(100), control.stop())
            .await
            .expect("Stop does not wait for an unready DAVE owner")
            .expect("Stop succeeds");
        let mut packet = [0_u8; 2_048];
        assert!(
            timeout(Duration::from_millis(40), sink.recv(&mut packet))
                .await
                .is_err(),
            "failed DAVE encryption cannot fall back to plaintext UDP"
        );
        gateway_task.await.expect("gateway responder completes");
        drop((
            transport_updates_tx,
            connection_tx,
            connection_shutdown_tx,
            pacer_owner,
        ));
        assert_eq!(
            *speaking.lock().expect("speaking record mutex"),
            Vec::<bool>::new()
        );
    }

    #[tokio::test]
    async fn dave_epoch_reset_during_speaking_preserves_sender_and_accepts_stop() {
        verify_epoch_reset(0, false).await;
    }

    #[tokio::test]
    async fn dave_epoch_reset_resumes_retained_frame_without_gateway_phase_update() {
        for owned in [false, true] {
            verify_epoch_reset(1, owned).await;
        }
    }

    #[tokio::test]
    async fn dave_epoch_reset_source_replacement_discards_old_staged_frame() {
        for owned in [false, true] {
            verify_epoch_reset(2, owned).await;
        }
    }

    #[tokio::test]
    async fn dave_epoch_reset_queued_ahead_of_inflight_media_retains_frame() {
        verify_epoch_reset(3, false).await;
    }

    #[tokio::test]
    async fn dave_epoch_reset_during_terminal_drain_finishes_exact_silence_tail() {
        verify_epoch_reset(4, false).await;
    }

    struct ResetSecondFrame {
        owner: dave::Handle,
        polls: usize,
    }

    impl FrameSource for ResetSecondFrame {
        fn poll_frame(&mut self, _cx: &mut Context<'_>, output: &mut [u8]) -> Poll<FrameStatus> {
            self.polls += 1;
            let len = match self.polls {
                1 => 64,
                2 => {
                    // FIFO owner commands put this reset ahead of the media
                    // request, after the sender's readiness check has passed.
                    self.owner.queue_fixture_epoch_reset();
                    128
                }
                _ => return Poll::Ready(FrameStatus::Ended),
            };
            output[..len].fill(0x55);
            Poll::Ready(FrameStatus::Frame { len })
        }
    }

    async fn verify_epoch_reset(action: u8, owned: bool) {
        let (sink, socket) = connected_udp_pair().await;
        let encoder = TransportEncoder::new(
            TransportMode::Aes256GcmRtpSize,
            &[0x42; 32],
            7,
            1_275 + dave::OPUS_MAX_ENCRYPTION_OVERHEAD_BYTES,
            2_048,
        )
        .expect("transport encoder initializes");
        let dave = dave::Handle::spawn_ready_fixture(8);
        let reset_owner = dave.clone();
        let recovery_owner = dave.clone();
        let dave_media = dave.media_encryptor();
        let pacer_owner = Pacer::new();
        let pacer = pacer_owner.register().await.expect("pacer registers");
        let (gateway, mut gateway_rx) = mpsc::channel(8);
        let speaking = Arc::new(Mutex::new(Vec::new()));
        let speaking_observer = speaking.clone();
        let gateway_task = tokio::spawn(async move {
            let mut reset = false;
            while let Some(command) = gateway_rx.recv().await {
                match command {
                    GatewayCommand::Speaking {
                        speaking, reply, ..
                    } => {
                        speaking_observer
                            .lock()
                            .expect("speaking record mutex")
                            .push(speaking);
                        if speaking && !reset && action < 3 {
                            reset_owner
                                .control(dave::Control::PrepareEpoch {
                                    protocol_version: 1,
                                    epoch: 1,
                                })
                                .await
                                .unwrap();
                            reset = true;
                        }
                        let _ = reply.send(Ok(()));
                    }
                    GatewayCommand::DetachAudio { reply, .. } => {
                        let _ = reply.send(());
                        break;
                    }
                    _ => panic!("unexpected gateway command in DAVE failure test"),
                }
            }
        });
        let (transport_updates_tx, transport_updates) = mpsc::channel(1);
        let (connection_tx, connection_state) = connected_state();
        let (connection_shutdown_tx, connection_shutdown) = watch::channel(false);
        let (events, _) = broadcast::channel(8);
        let control = AudioControl::spawn(SpawnAudio {
            id: 2,
            source: if action == 3 {
                AudioSource::Callback(Box::new(ResetSecondFrame {
                    owner: recovery_owner.clone(),
                    polls: 0,
                }))
            } else if owned {
                let (mut writer, reader) = crate::frame_channel();
                let mut send = Box::pin(writer.send(&[0x55; 128]));
                use std::future::Future;
                assert!(
                    send.as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                drop(send);
                drop(writer);
                AudioSource::Channel(reader)
            } else {
                AudioSource::Callback(Box::new(OneFrameSource {
                    frame: Some(vec![0x55; 128]),
                }))
            },
            transport: InstalledTransport {
                generation: ConnectionGeneration::FIRST,
                socket: Arc::new(socket),
                encoder,
                validity: TransportValidity::new(),
                dave_protocol_version: 1,
                dave: Some(dave),
                dave_media: Some(dave_media),
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
            active: Arc::new(AtomicBool::new(true)),
            fail_udp_sends: Arc::new(AtomicBool::new(false)),
        });

        if action == 4 {
            timeout(Duration::from_secs(1), async {
                while control.snapshot().stats().silence_frames_sent() < 2 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            recovery_owner
                .control(dave::Control::PrepareEpoch {
                    protocol_version: 1,
                    epoch: 1,
                })
                .await
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert_ne!(
            control.snapshot().phase(),
            AudioPhase::Failed,
            "an epoch reset is temporary, not a terminal encryption failure: {:?}",
            control.snapshot()
        );
        assert_eq!(
            control.snapshot().stats().frames_sent(),
            if action >= 3 { 1 } else { 0 }
        );
        let mut packet = [0_u8; 2_048];
        while sink.try_recv(&mut packet).is_ok() {}
        assert!(
            timeout(Duration::from_millis(40), sink.recv(&mut packet))
                .await
                .is_err(),
            "not-ready DAVE sends no plaintext or transport-only packet"
        );
        if action != 0 {
            if action == 2 {
                timeout(
                    Duration::from_millis(100),
                    control.sender().replace_source(OneFrameSource {
                        frame: Some(vec![0x77; 64]),
                    }),
                )
                .await
                .expect("replacement remains responsive during rekey")
                .unwrap();
            }
            recovery_owner.prepare_fixture_epoch().await;
            recovery_owner
                .control(dave::Control::ExecuteTransition { id: 7 })
                .await
                .unwrap();
            // The gateway phase deliberately stays Connected throughout. The
            // DAVE watch alone must wake and resume the retained source.
            let len = timeout(Duration::from_secs(1), sink.recv(&mut packet))
                .await
                .expect("pending frame resumes after key establishment")
                .unwrap();
            let decoded = decrypt_transport_packet(
                OracleMode::Aes256GcmRtpSize,
                &[0x42; 32],
                &packet[..len],
                2_048,
            )
            .unwrap();
            let expected_len = match action {
                2 => 64,
                4 => SILENCE.len(),
                _ => 128,
            };
            assert!(
                decoded.payload.len() > expected_len && decoded.payload.len() <= expected_len + 16,
                "exact pending source frame, not an EOF silence frame: {}",
                decoded.payload.len()
            );
            assert!(decoded.payload.ends_with(&[0xFA, 0xFA]));
            timeout(Duration::from_secs(1), async {
                while control.snapshot().stats().silence_frames_sent() != u64::from(SILENCE_FRAMES)
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("source finishes with its normal bounded silence drain");
            assert_eq!(
                control.snapshot().stats().frames_sent(),
                if action == 3 { 2 } else { 1 },
                "no dropped or duplicated source frame"
            );
        }
        timeout(Duration::from_millis(100), control.stop())
            .await
            .expect("Stop remains responsive during DAVE establishment")
            .expect("sender stops cleanly");
        gateway_task.await.expect("gateway responder completes");
        drop((
            transport_updates_tx,
            connection_tx,
            connection_shutdown_tx,
            pacer_owner,
        ));
        assert_eq!(
            *speaking.lock().expect("speaking record mutex"),
            [true, false]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "release-only P08 complete paced DAVE path performance evidence"]
    async fn p08_complete_paced_dave_path_benchmark() {
        let warmup = Duration::from_millis(
            std::env::var("OTO_P08_WARMUP_MS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(1_000),
        );
        let measurement = Duration::from_millis(
            std::env::var("OTO_P08_MEASURE_MS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(3_000),
        );
        let (sink, socket) = connected_udp_pair().await;
        let received = Arc::new(AtomicU64::new(0));
        let received_observer = received.clone();
        let sink_task = tokio::spawn(async move {
            let mut packet = [0_u8; 2_048];
            while sink.recv(&mut packet).await.is_ok() {
                AudioCounters::increment(&received_observer);
            }
        });
        let encoder = TransportEncoder::new(
            TransportMode::Aes256GcmRtpSize,
            &[0x42; 32],
            7,
            1_275 + dave::OPUS_MAX_ENCRYPTION_OVERHEAD_BYTES,
            2_048,
        )
        .expect("transport encoder initializes");
        let dave = dave::Handle::spawn_ready_fixture(8);
        let dave_media = dave.media_encryptor();
        let pacer_owner = Pacer::new();
        let pacer = pacer_owner.register().await.expect("pacer registers");
        let (gateway, mut gateway_rx) = mpsc::channel(8);
        let gateway_task = tokio::spawn(async move {
            while let Some(command) = gateway_rx.recv().await {
                match command {
                    GatewayCommand::Speaking { reply, .. } => {
                        let _ = reply.send(Ok(()));
                    }
                    GatewayCommand::DetachAudio { reply, .. } => {
                        let _ = reply.send(());
                        break;
                    }
                    _ => panic!("unexpected gateway command in paced DAVE benchmark"),
                }
            }
        });
        let (transport_updates_tx, transport_updates) = mpsc::channel(1);
        let (connection_tx, connection_state) = connected_state();
        let (connection_shutdown_tx, connection_shutdown) = watch::channel(false);
        let (events, _) = broadcast::channel(8);
        let channel_mode = std::env::var_os("OTO_P08_FRAME_CHANNEL").is_some();
        let (source, producer) = if channel_mode {
            let (mut writer, reader) = crate::frame_channel();
            let task = tokio::spawn(async move {
                let frame = [0x55; 1275];
                while writer.send(&frame).await.is_ok() {}
            });
            (AudioSource::Channel(reader), Some(task))
        } else {
            (AudioSource::Callback(Box::new(MaximumFrameSource)), None)
        };
        let control = AudioControl::spawn(SpawnAudio {
            id: 3,
            source,
            transport: InstalledTransport {
                generation: ConnectionGeneration::FIRST,
                socket: Arc::new(socket),
                encoder,
                validity: TransportValidity::new(),
                dave_protocol_version: 1,
                dave: Some(dave),
                dave_media: Some(dave_media),
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
            active: Arc::new(AtomicBool::new(true)),
            fail_udp_sends: Arc::new(AtomicBool::new(false)),
        });

        tokio::time::sleep(warmup).await;
        let frames_before = control.snapshot().stats().frames_sent();
        let udp_before = received.load(Ordering::Relaxed);
        let region = stats_alloc::Region::new(crate::TEST_ALLOCATOR);
        let started = StdInstant::now();
        tokio::time::sleep(measurement).await;
        let elapsed = started.elapsed();
        let allocation = region.change();
        let frames = control
            .snapshot()
            .stats()
            .frames_sent()
            .saturating_sub(frames_before);
        let udp_packets = received.load(Ordering::Relaxed).saturating_sub(udp_before);
        let result = serde_json::json!({
            "schemaVersion": 1,
            "benchmarkId": "oto-p08-complete-paced-dave",
            "source": if channel_mode { "owned-channel" } else { "callback" },
            "profile": "release",
            "warmupMs": warmup.as_millis(),
            "measurementMs": elapsed.as_millis(),
            "frames": frames,
            "udpPackets": udp_packets,
            "boundaryDifference": frames.abs_diff(udp_packets),
            "allocation": {
                "allocations": allocation.allocations,
                "reallocations": allocation.reallocations,
                "bytesAllocated": allocation.bytes_allocated,
                "allocationsPerFrame": allocation.allocations as f64 / frames.max(1) as f64,
                "reallocationsPerFrame": allocation.reallocations as f64 / frames.max(1) as f64,
                "bytesAllocatedPerFrame": allocation.bytes_allocated as f64 / frames.max(1) as f64,
            },
            "maxSenderLatenessNanos": control.snapshot().stats().max_lateness().as_nanos(),
        });
        println!("P08_COMPLETE_PATH_BENCHMARK={result}");
        assert!(frames > 0);
        assert!(frames.abs_diff(udp_packets) <= 1);

        control.stop().await.expect("benchmark sender stops");
        gateway_task.await.expect("gateway responder completes");
        if let Some(task) = producer {
            timeout(Duration::from_secs(1), task)
                .await
                .expect("channel producer closes")
                .expect("producer completes");
        }
        sink_task.abort();
        drop((
            transport_updates_tx,
            connection_tx,
            connection_shutdown_tx,
            pacer_owner,
        ));
    }

    #[tokio::test]
    async fn transport_invalidation_waits_for_inflight_send_and_refuses_new_sends() {
        let validity = TransportValidity::new();
        let permit = validity
            .try_acquire()
            .expect("valid transport admits an in-flight send");
        let invalidating = validity.clone();
        let invalidation = tokio::spawn(async move {
            invalidating.invalidate().await;
        });

        tokio::task::yield_now().await;
        assert!(
            !invalidation.is_finished(),
            "replacement waits for the admitted send to finish"
        );
        assert!(
            validity.try_acquire().is_none(),
            "no send starts once invalidation begins"
        );

        drop(permit);
        timeout(Duration::from_secs(1), invalidation)
            .await
            .expect("invalidation is notified")
            .expect("invalidation task does not panic");
        assert!(validity.try_acquire().is_none());
    }
}
