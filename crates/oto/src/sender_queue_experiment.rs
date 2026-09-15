//! Decision spike only, compiled exclusively into the Oto test executable.
//! Compares the real AudioControl path with a native paced payload queue.
//! It does not change VoiceConnection, gateway or production DAVE lifecycle.
#![cfg(target_os = "linux")]
use super::*;
use crate::pacer::Pacer;
use crate::transport::TransportMode;
use std::hash::{Hash, Hasher};
use std::net::UdpSocket as StdSocket;
use std::sync::atomic::AtomicBool;
use std::thread;

const DEPTH: usize = 8;
const MAX_OPUS: usize = 1275;
const MAX_PREPARED: usize = MAX_OPUS + dave::OPUS_MAX_ENCRYPTION_OVERHEAD_BYTES;
const MAX_PACKET: usize = 2048;
const KEY: [u8; 32] = [0x42; 32]; // Synthetic test key, never a Discord credential.
const PERIOD: Duration = Duration::from_millis(20);

#[derive(Clone, Debug)]
struct Prepared {
    source: u64,
    epoch: u64,
    raw_len: usize,
    raw: [u8; MAX_OPUS],
    len: usize,
    bytes: [u8; MAX_PREPARED],
    silence: bool,
}
impl Prepared {
    fn plain(sequence: usize) -> Self {
        let mut frame = Self {
            source: 1,
            epoch: 1,
            raw_len: MAX_OPUS,
            raw: [0x55; MAX_OPUS],
            len: MAX_OPUS,
            bytes: [0; MAX_PREPARED],
            silence: false,
        };
        frame.raw[..8].copy_from_slice(&(sequence as u64).to_be_bytes());
        frame.bytes[..MAX_OPUS].copy_from_slice(&frame.raw);
        frame
    }
}
fn digest(bytes: &[u8]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

struct Shared {
    cancel: AtomicBool,
    sent: AtomicUsize,
    credits: futures_util::task::AtomicWaker,
}

#[derive(Debug)]
enum Control {
    Pause(bool, oneshot::Sender<()>),
    // Return unsent original frames for re-encryption; never throw away music
    // for a DAVE transition. Caller stops admission before requesting the fence.
    Fence {
        source: u64,
        epoch: u64,
        reply: oneshot::Sender<Vec<Prepared>>,
    },
}

struct Native {
    producer: Option<rtrb::Producer<Prepared>>,
    controls: std::sync::mpsc::SyncSender<Control>,
    shared: Arc<Shared>,
    thread: Option<thread::JoinHandle<Result<(), String>>>,
}

impl Native {
    fn start(
        socket: StdSocket,
        encoder: TransportEncoder,
        capacity: usize,
        sender_stall_ms: u64,
    ) -> Self {
        Self::start_traced(socket, encoder, capacity, sender_stall_ms, None)
    }
    fn start_traced(
        socket: StdSocket,
        encoder: TransportEncoder,
        capacity: usize,
        sender_stall_ms: u64,
        mut trace: Option<crate::send_trace::TraceWriter>,
    ) -> Self {
        assert!((1..=32).contains(&capacity));
        socket.set_nonblocking(true).unwrap();
        let (producer, mut consumer) = rtrb::RingBuffer::new(capacity);
        let shared = Arc::new(Shared {
            cancel: AtomicBool::new(false),
            sent: AtomicUsize::new(0),
            credits: futures_util::task::AtomicWaker::new(),
        });
        let worker = shared.clone();
        let (controls, commands) = std::sync::mpsc::sync_channel(4);
        let thread = thread::Builder::new()
            .name("oto-queue-probe".into())
            .stack_size(256 * 1024)
            .spawn(move || {
                let mut encoder = encoder;
                let mut packet = Vec::with_capacity(MAX_PACKET);
                let mut deadline: Option<StdInstant> = None;
                let mut paused = false;
                let mut generation = (1, 1);
                let mut injected = false;
                let result = (|| -> Result<(), String> {
                    loop {
                        if worker.cancel.load(Ordering::Acquire) {
                            return Ok(());
                        }
                        while let Ok(command) = commands.try_recv() {
                            match command {
                                Control::Pause(value, reply) => {
                                    paused = value;
                                    // Keep deadline: pause/resume must not burst packets.
                                    let _ = reply.send(());
                                }
                                Control::Fence {
                                    source,
                                    epoch,
                                    reply,
                                } => {
                                    paused = true;
                                    let mut retained = Vec::with_capacity(capacity);
                                    while let Ok(frame) = consumer.pop() {
                                        retained.push(frame);
                                    }
                                    generation = (source, epoch);
                                    worker.credits.wake();
                                    let _ = reply.send(retained);
                                }
                            }
                        }
                        if paused {
                            thread::park_timeout(PERIOD);
                            continue;
                        }
                        if sender_stall_ms != 0
                            && !injected
                            && worker.sent.load(Ordering::Relaxed) == 50
                        {
                            injected = true;
                            thread::sleep(Duration::from_millis(sender_stall_ms));
                            continue; // Recheck controls after the injected delay.
                        }
                        if let Some(due) = deadline {
                            let now = StdInstant::now();
                            if now < due {
                                thread::park_timeout(due.saturating_duration_since(now));
                                continue;
                            }
                        }
                        let frame: Prepared = match consumer.pop() {
                            Ok(frame) => frame,
                            Err(_) if consumer.is_abandoned() => return Ok(()),
                            Err(_) => {
                                thread::park_timeout(PERIOD);
                                continue;
                            }
                        };
                        worker.credits.wake();
                        if (frame.source, frame.epoch) != generation {
                            return Err(
                                "stale prepared frame reached sender; admission must fence".into(),
                            );
                        }
                        encoder
                            .encrypt_next(&frame.bytes[..frame.len], &mut packet)
                            .map_err(|e| format!("transport: {e:?}"))?;
                        let written = socket.send(&packet).map_err(|e| format!("UDP: {e}"))?;
                        if written != packet.len() {
                            return Err("partial UDP write".into());
                        }
                        worker.sent.fetch_add(1, Ordering::Release);
                        // Same absolute-clock policy as Pacer: rebase after >=20ms
                        // lateness, never burst or discard frames to catch up.
                        let now = StdInstant::now();
                        if let Some(trace) = trace.as_mut() {
                            trace.record(
                                Instant::from_std(now),
                                &packet[..12],
                                1,
                                frame.source,
                                frame.silence,
                                Default::default(),
                            );
                        }
                        deadline = Some(match deadline {
                            Some(due) if now.saturating_duration_since(due) < PERIOD => {
                                due + PERIOD
                            }
                            _ => now + PERIOD,
                        });
                    }
                })();
                worker.cancel.store(true, Ordering::Release);
                worker.credits.wake();
                result
            })
            .unwrap();
        Self {
            producer: Some(producer),
            controls,
            shared,
            thread: Some(thread),
        }
    }

    async fn send(&mut self, frame: Prepared) {
        futures_util::future::poll_fn(|cx| {
            self.shared.credits.register(cx.waker());
            let producer = self
                .producer
                .as_mut()
                .expect("source producer is available");
            if producer.is_abandoned() || self.shared.cancel.load(Ordering::Acquire) {
                return Poll::Ready(false);
            }
            if producer.slots() > 0 {
                Poll::Ready(true)
            } else {
                Poll::Pending
            }
        })
        .await
        .then_some(())
        .expect("native sender alive");
        assert!(
            self.producer
                .as_mut()
                .expect("source producer is available")
                .push(frame)
                .is_ok()
        );
        self.thread.as_ref().unwrap().thread().unpark();
    }
    async fn pause(&self, paused: bool) {
        let (reply, response) = oneshot::channel();
        self.controls
            .try_send(Control::Pause(paused, reply))
            .expect("control capacity");
        self.thread.as_ref().unwrap().thread().unpark();
        timeout(Duration::from_secs(1), response)
            .await
            .unwrap()
            .unwrap();
    }
    async fn fence(&self, source: u64, epoch: u64) -> Vec<Prepared> {
        let (reply, response) = oneshot::channel();
        self.controls
            .try_send(Control::Fence {
                source,
                epoch,
                reply,
            })
            .expect("control capacity");
        self.thread.as_ref().unwrap().thread().unpark();
        timeout(Duration::from_secs(1), response)
            .await
            .unwrap()
            .unwrap()
    }
    fn shutdown(&mut self) {
        self.shared.cancel.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            thread.thread().unpark();
            thread.join().unwrap().unwrap();
        }
    }
}
impl Drop for Native {
    fn drop(&mut self) {
        self.shared.cancel.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            thread.thread().unpark();
            // All I/O is nonblocking and sleeps are <=20ms outside fault injection.
            let _ = thread.join();
        }
    }
}

struct Captured {
    at: StdInstant,
    len: usize,
    bytes: [u8; MAX_PACKET],
}
struct Receiver {
    stop: Arc<AtomicBool>,
    count: Arc<AtomicUsize>,
    task: Option<thread::JoinHandle<Vec<Captured>>>,
}
impl Receiver {
    fn start(capacity: usize) -> (Self, StdSocket) {
        let socket = StdSocket::bind("127.0.0.1:0").unwrap();
        socket
            .set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        let sender = StdSocket::bind("127.0.0.1:0").unwrap();
        sender.connect(socket.local_addr().unwrap()).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let count = Arc::new(AtomicUsize::new(0));
        let received = count.clone();
        let task = thread::spawn(move || {
            let mut captures = Vec::with_capacity(capacity);
            while !stopped.load(Ordering::Acquire) {
                let mut record = Captured {
                    at: StdInstant::now(),
                    len: 0,
                    bytes: [0; MAX_PACKET],
                };
                match socket.recv(&mut record.bytes) {
                    Ok(len) => {
                        record.at = StdInstant::now();
                        record.len = len;
                        assert!(captures.len() < capacity, "capture bound exceeded");
                        captures.push(record);
                        received.fetch_add(1, Ordering::Release);
                    }
                    Err(e)
                        if matches!(
                            e.kind(),
                            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                        ) => {}
                    Err(e) => panic!("UDP receiver: {e}"),
                }
            }
            captures
        });
        (
            Self {
                stop,
                count,
                task: Some(task),
            },
            sender,
        )
    }
    fn finish(&mut self) -> Vec<Captured> {
        self.stop.store(true, Ordering::Release);
        self.task.take().unwrap().join().unwrap()
    }
}
impl Drop for Receiver {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(task) = self.task.take() {
            let _ = task.join();
        }
    }
}

fn encoder() -> TransportEncoder {
    TransportEncoder::new(
        TransportMode::Aes256GcmRtpSize,
        &KEY,
        7,
        MAX_PREPARED,
        MAX_PACKET,
    )
    .unwrap()
}
async fn prepare(
    media: &mut dave::MediaEncryptor,
    frame: &mut Prepared,
    buffers: &mut (Vec<u8>, Vec<u8>),
) {
    buffers.0[..frame.raw_len].copy_from_slice(&frame.raw[..frame.raw_len]);
    let result = media
        .encrypt_buffered(
            std::mem::take(&mut buffers.0),
            frame.raw_len,
            std::mem::take(&mut buffers.1),
        )
        .await
        .unwrap();
    assert_eq!(result.result.unwrap(), dave::MediaOutcome::Encrypted);
    frame.len = result.output.len();
    frame.bytes[..frame.len].copy_from_slice(&result.output);
    *buffers = (result.frame, result.output);
}
async fn wait_count(count: &AtomicUsize, expected: usize) {
    timeout(Duration::from_secs(5) + PERIOD * expected as u32, async {
        while count.load(Ordering::Acquire) < expected {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("receiver/sender progress deadline");
}

// Both arms run source preparation and the real DAVE actor on this caller's
// Tokio runtime. Native fault coordination blocks the requested runtime workers,
// while the independent UDP receiver remains runnable in both arms.
async fn trial(
    native: bool,
    count: usize,
    runtime_stall_ms: u64,
    sender_stall_ms: u64,
    depth: usize,
    stalled_workers: usize,
) -> serde_json::Value {
    let (mut receiver, socket) = Receiver::start(count + 5);
    let owner = dave::Handle::spawn_ready_fixture(8);
    let mut baseline = None;
    let mut gateway_task = None;
    let mut keepalive = None;
    let mut worker = None;
    let mut expected_hashes: Option<Vec<u64>> = None;
    let mut trace;
    let (source_start, source_ready) = oneshot::channel();
    // Same capacity-one owned handoff as Crust's production bridge in both
    // arms. Producer runs on Tokio; generated frames cannot evade the fault.
    let (mut source_writer, source_reader) = crate::frame_channel();
    let mut source_reader = Some(source_reader);
    let produced = Arc::new(AtomicUsize::new(0));
    let producer_count = produced.clone();
    let source_task = tokio::spawn(async move {
        source_ready.await.unwrap();
        for index in 0..count {
            let frame = Prepared::plain(index);
            source_writer
                .send(&frame.raw[..frame.raw_len])
                .await
                .unwrap();
            producer_count.fetch_add(1, Ordering::Release);
        }
    });
    // At most depth prepared frames, one in preparation/admission, one native
    // in-flight frame and the capacity-one source bridge; no audio Vec grows
    // with duration. Digest/capture/trace storage is bounded test metadata.
    if native {
        let (writer, reader) = crate::send_trace::pair(count + 5);
        trace = reader;
        let sender = Native::start_traced(socket, encoder(), depth, sender_stall_ms, Some(writer));
        // The caller feeds after the fault task is installed below. This keeps
        // the runtime stall concurrent with source preparation, as it is in
        // production, instead of precomputing while the runtime is idle.
        worker = Some(sender);
    } else {
        assert_eq!(sender_stall_ms, 0);
        socket.set_nonblocking(true).unwrap();
        let socket = tokio::net::UdpSocket::from_std(socket).unwrap();
        let (gateway, mut commands) = mpsc::channel(8);
        gateway_task = Some(tokio::spawn(async move {
            while let Some(cmd) = commands.recv().await {
                match cmd {
                    GatewayCommand::Speaking { reply, .. } => {
                        let _ = reply.send(Ok(()));
                    }
                    GatewayCommand::DetachAudio { reply, .. } => {
                        let _ = reply.send(());
                        break;
                    }
                    _ => panic!("unexpected gateway control"),
                }
            }
        }));
        let (updates, transport_updates) = mpsc::channel(1);
        let mut connected = ConnectionSnapshot::initial();
        connected.set_phase(ConnectionPhase::Connected);
        let (connection, connection_state) = watch::channel(connected);
        let (shutdown, connection_shutdown) = watch::channel(false);
        let (events, _) = broadcast::channel(8);
        let pacer = Pacer::new().register().await.unwrap();
        baseline = Some(AudioControl::spawn(SpawnAudio {
            id: 1,
            source: AudioSource::Channel(source_reader.take().unwrap()),
            transport: InstalledTransport {
                generation: ConnectionGeneration::FIRST,
                socket: Arc::new(socket),
                encoder: encoder(),
                validity: TransportValidity::new(),
                dave_protocol_version: 1,
                dave: Some(owner.clone()),
                dave_media: Some(owner.media_encryptor()),
            },
            transport_updates,
            pacer,
            gateway_commands: gateway,
            connection_state,
            connection_shutdown,
            events,
            command_capacity: 8,
            max_frame_bytes: MAX_OPUS,
            max_datagram_bytes: MAX_PACKET,
            active: Arc::new(AtomicBool::new(true)),
            fail_udp_sends: Arc::new(AtomicBool::new(false)),
        }));
        trace = baseline
            .as_ref()
            .unwrap()
            .sender()
            .start_send_trace(count + 5)
            .await
            .unwrap();
        keepalive = Some((updates, connection, shutdown));
    }
    source_start.send(()).unwrap();
    let received = receiver.count.clone();
    let prepared_count = Arc::new(AtomicUsize::new(0));
    let prepared_fault = prepared_count.clone();
    let runtime = tokio::runtime::Handle::current();
    let fault = thread::spawn(move || {
        let limit = StdInstant::now() + Duration::from_secs(5);
        while received.load(Ordering::Acquire) < 50 {
            assert!(StdInstant::now() < limit, "fault trigger deadline");
            thread::sleep(Duration::from_millis(1));
        }
        let memory = process_memory();
        // External coordinator submits blockers through the global runtime
        // queue. No blocker awaits another task on the worker's LIFO slot.
        // Every Condvar wait is bounded so a broken injector cannot hang CI.
        let gate = Arc::new((Mutex::new((0usize, false)), std::sync::Condvar::new()));
        let mut blockers = Vec::new();
        if runtime_stall_ms != 0 {
            for _ in 0..stalled_workers {
                let gate = gate.clone();
                blockers.push(runtime.spawn(async move {
                    let mut state = gate.0.lock().unwrap();
                    state.0 += 1;
                    gate.1.notify_all();
                    let (state, _) = gate
                        .1
                        .wait_timeout_while(state, Duration::from_secs(2), |s| !s.1)
                        .unwrap();
                    assert!(state.1, "fault release deadline");
                }));
            }
            let state = gate.0.lock().unwrap();
            let (state, _) = gate
                .1
                .wait_timeout_while(state, Duration::from_secs(2), |s| s.0 != stalled_workers)
                .unwrap();
            assert_eq!(state.0, stalled_workers, "worker rendezvous deadline");
        }
        let before_produced = produced.load(Ordering::Acquire);
        let before_prepared = prepared_fault.load(Ordering::Acquire);
        if runtime_stall_ms != 0 {
            thread::sleep(Duration::from_millis(runtime_stall_ms));
        }
        let source_progress = produced.load(Ordering::Acquire) - before_produced;
        let preparation_progress = prepared_fault.load(Ordering::Acquire) - before_prepared;
        gate.0.lock().unwrap().1 = true;
        gate.1.notify_all();
        runtime.block_on(async {
            for blocker in blockers {
                blocker.await.unwrap();
            }
        });
        (source_progress, preparation_progress, memory)
    });
    let cpu_started = process_cpu();
    let wall_started = StdInstant::now();
    if native {
        let mut sender = worker.take().unwrap();
        let mut media = owner.media_encryptor();
        let mut buffers = (vec![0; MAX_OPUS], Vec::with_capacity(MAX_PREPARED));
        let mut hashes = Vec::with_capacity(count + 5);
        let mut reader = source_reader.take().unwrap();
        for index in 0..count + 5 {
            let mut frame = Prepared::plain(index);
            if index < count {
                let status =
                    futures_util::future::poll_fn(|cx| reader.poll_owned(cx, &mut frame.raw)).await;
                assert_eq!(status, FrameStatus::Frame { len: MAX_OPUS });
                assert_eq!(&frame.raw[..8], &(index as u64).to_be_bytes());
            } else {
                frame.silence = true;
                frame.raw_len = 3;
                frame.raw[..3].copy_from_slice(&SILENCE);
            }
            prepare(&mut media, &mut frame, &mut buffers).await;
            prepared_count.fetch_add(1, Ordering::Release);
            hashes.push(digest(&frame.bytes[..frame.len]));
            sender.send(frame).await;
        }
        expected_hashes = Some(hashes);
        worker = Some(sender);
    }
    wait_count(&receiver.count, count + 5).await;
    let cpu = process_cpu().saturating_sub(cpu_started);
    let elapsed = wall_started.elapsed();
    let fault_result = fault.join().unwrap();
    source_task.await.unwrap();
    if let Some(sender) = baseline {
        sender.stop().await.unwrap();
        assert_eq!(sender.snapshot().stats().frames_sent(), count as u64);
        assert_eq!(sender.snapshot().stats().silence_frames_sent(), 5);
        assert_eq!(sender.snapshot().stats().send_failures(), 0);
    }
    if let Some(mut sender) = worker {
        sender.shutdown();
    }
    if let Some(task) = gateway_task {
        task.await.unwrap();
    }
    drop(keepalive);
    let capture = receiver.finish();
    assert_eq!(capture.len(), count + 5);
    let mut previous = None;
    for (index, packet) in capture.iter().enumerate() {
        let decoded = oto_testkit::decrypt_transport_packet(
            oto_testkit::TransportMode::Aes256GcmRtpSize,
            &KEY,
            &packet.bytes[..packet.len],
            MAX_PREPARED,
        )
        .unwrap();
        if let Some((sequence, timestamp, nonce)) = previous {
            assert_eq!(decoded.sequence, u16::wrapping_add(sequence, 1));
            assert_eq!(decoded.timestamp, u32::wrapping_add(timestamp, 960));
            assert_eq!(decoded.nonce, nonce + 1);
        }
        previous = Some((decoded.sequence, decoded.timestamp, decoded.nonce));
        assert!(
            decoded.payload.ends_with(&[0xfa, 0xfa]),
            "DAVE trailer missing"
        );
        if let Some(expected) = expected_hashes.as_ref() {
            assert_eq!(
                digest(&decoded.payload),
                expected[index],
                "prepared payload altered"
            );
        }
    }
    let mut intervals: Vec<u64> = capture[..count]
        .windows(2)
        .map(|p| p[1].at.duration_since(p[0].at).as_micros() as u64)
        .collect();
    assert_eq!(trace.dropped(), 0);
    let mut sends = Vec::with_capacity(count + 5);
    while let Some(r) = trace.pop() {
        sends.push(r);
    }
    assert_eq!(
        sends.len(),
        count + 5,
        "all successful sends must be traced"
    );
    for (record, packet) in sends.iter().zip(&capture) {
        assert_eq!(
            record.sequence,
            u16::from_be_bytes(packet.bytes[2..4].try_into().unwrap())
        );
    }
    assert!(sends[..count].iter().all(|r| !r.silence));
    assert!(sends[count..].iter().all(|r| r.silence));
    let send_intervals_us: Vec<u64> = sends[..count]
        .windows(2)
        .map(|p| p[1].elapsed_micros - p[0].elapsed_micros)
        .collect();
    let arrival_intervals_us = intervals.clone();
    intervals.sort_unstable();
    serde_json::json!({ "send_interval_max_us": send_intervals_us.iter().max(),
        "send_gaps_ge_40ms": send_intervals_us.iter().filter(|&&n| n>=40000).count(),
        "send_intervals_us": send_intervals_us, "arrival_intervals_us": arrival_intervals_us, "native": native, "runtime_stall_ms": runtime_stall_ms,
        "sender_stall_ms": sender_stall_ms, "frames": count, "received": capture.len(),
        "gaps_ge_40ms": intervals.iter().filter(|&&v| v >= 40000).count(),
        "interval_min_us": intervals[0], "interval_max_us": intervals[intervals.len()-1],
        "interval_p999_us": intervals[((intervals.len()-1) as f64 * 0.999).ceil() as usize],
        "depth": depth, "stalled_workers": stalled_workers,
        "source_progress_during_fault": fault_result.0,
        "preparation_progress_during_fault": fault_result.1,
        "steady_process_memory": fault_result.2,
        "process_cpu_us": cpu.as_micros(), "elapsed_us": elapsed.as_micros(),
        "cpu_percent_one_core": 100.0 * cpu.as_secs_f64() / elapsed.as_secs_f64(),
        "receiver_storage_bytes": (count+5) * std::mem::size_of::<Captured>(),
        "digest_storage_bytes": if native { (count+5)*8 } else { 0 },
        "prepared_admission_frames_max": if native { 1 } else { 0 },
        "native_inflight_frames_max": if native { 1 } else { 0 },
        "source_channel_slots": 1,
        "trace_storage_bytes": (count+5) * std::mem::size_of::<crate::SendRecord>(),
        "preparation_buffer_bytes": if native { MAX_OPUS + MAX_PREPARED } else { 0 },
        "queue_slots": if native { depth } else { 0 },
        "queue_storage_bytes": if native { depth * std::mem::size_of::<Prepared>() } else { 0 },
        "native_stack_reserved_bytes": if native { 256 * 1024 } else { 0 } })
}

fn process_cpu() -> Duration {
    let t = rustix::time::clock_gettime(rustix::time::ClockId::ProcessCPUTime);
    Duration::new(t.tv_sec as u64, t.tv_nsec as u32)
}
fn process_memory() -> serde_json::Value {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let smaps = std::fs::read_to_string("/proc/self/smaps_rollup").unwrap();
    let value = |input: &str, key: &str| -> u64 {
        input
            .lines()
            .find_map(|l| l.strip_prefix(key))
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap()
            .parse()
            .unwrap()
    };
    serde_json::json!({"rss_kib":value(&status,"VmRSS:"), "pss_kib":value(&smaps,"Pss:"),
        "private_dirty_kib":value(&smaps,"Private_Dirty:"), "threads":value(&status,"Threads:")})
}

#[test]
#[ignore = "serial release benchmark; owned channel and actual DAVE runtime"]
fn compare_runtime_stalls() {
    let count: usize = std::env::var("OTO_QUEUE_FRAMES")
        .ok()
        .map(|v| v.parse().unwrap())
        .unwrap_or(180);
    let depth: usize = std::env::var("OTO_QUEUE_DEPTH")
        .ok()
        .map(|v| v.parse().unwrap())
        .unwrap_or(8);
    let workers: usize = std::env::var("OTO_QUEUE_WORKERS")
        .ok()
        .map(|v| v.parse().unwrap())
        .unwrap_or(2);
    assert!((120..=3000).contains(&count));
    assert!((1..=32).contains(&depth));
    assert!((1..=2).contains(&workers));
    let reverse = std::env::var_os("OTO_QUEUE_REVERSE").is_some();
    let only = std::env::var("OTO_QUEUE_ONLY").ok();
    assert!(matches!(only.as_deref(), None | Some("current" | "native")));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .unwrap();
    let cases = if only.is_some() {
        vec![(0, 1)]
    } else {
        let mut cases = vec![(0, 1), (100, workers), (250, workers), (400, workers)];
        if workers == 2 {
            cases.push((100, 1));
        }
        cases
    };
    for (delay, stalled_workers) in cases {
        for native in if reverse {
            [true, false]
        } else {
            [false, true]
        } {
            if only.as_deref().is_some_and(|s| (s == "native") != native) {
                continue;
            }
            // Spawn the trial on a worker so runtime saturation blocks source
            // preparation too. block_on alone is an additional runnable thread.
            let mut result = runtime.block_on(async {
                tokio::spawn(trial(native, count, delay, 0, depth, stalled_workers))
                    .await
                    .unwrap()
            });
            result["runtime_workers"] = workers.into();
            println!("QUEUE_TRIAL={result}");
        }
    }
    if only.is_none() {
        let mut result = runtime.block_on(async {
            tokio::spawn(trial(true, count, 0, 100, depth, 1))
                .await
                .unwrap()
        });
        result["runtime_workers"] = workers.into();
        println!("QUEUE_TRIAL={result}");
    }
}

#[tokio::test]
async fn native_pause_fence_rekey_stop_and_rtp_continuity() {
    let (mut receiver, socket) = Receiver::start(DEPTH + 1);
    let mut native = Native::start(socket, encoder(), DEPTH, 0);
    native.pause(true).await;
    let owner = dave::Handle::spawn_ready_fixture(8);
    let mut media = owner.media_encryptor();
    let mut buffers = (vec![0; MAX_OPUS], Vec::with_capacity(MAX_PREPARED));
    for index in 0..DEPTH {
        let mut frame = Prepared::plain(index);
        prepare(&mut media, &mut frame, &mut buffers).await;
        native.send(frame).await;
    }
    // A full lane is backpressured, not silently overwriting buffered music.
    assert!(futures_util::FutureExt::now_or_never(native.send(Prepared::plain(99))).is_none());
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert_eq!(receiver.count.load(Ordering::Acquire), 0);
    let originals = native.fence(1, 2).await;
    assert_eq!(originals.len(), DEPTH);
    owner.queue_fixture_epoch_reset();
    owner.prepare_fixture_epoch().await;
    owner
        .control(dave::Control::ExecuteTransition { id: 7 })
        .await
        .unwrap();
    for (index, mut frame) in originals.into_iter().enumerate() {
        assert_eq!(frame.raw, Prepared::plain(index).raw);
        frame.epoch = 2;
        prepare(&mut media, &mut frame, &mut buffers).await;
        native.send(frame).await;
    }
    native.pause(false).await;
    wait_count(&receiver.count, DEPTH).await;
    native.pause(true).await;
    // Fence returns originals. Replacing the source deliberately cancels its
    // unsent tail; rekey above instead preserved/re-encrypted every frame.
    native
        .send({
            let mut f = Prepared::plain(99);
            f.epoch = 2;
            f
        })
        .await;
    assert_eq!(native.fence(2, 2).await.len(), 1);
    let mut fresh = Prepared::plain(0);
    fresh.source = 2;
    fresh.epoch = 2;
    prepare(&mut media, &mut fresh, &mut buffers).await;
    native.send(fresh).await;
    native.pause(false).await;
    wait_count(&receiver.count, DEPTH + 1).await;
    native.shutdown();
    let captures = receiver.finish();
    assert_eq!(captures.len(), DEPTH + 1);
    for pair in captures.windows(2) {
        let a = u16::from_be_bytes(pair[0].bytes[2..4].try_into().unwrap());
        let b = u16::from_be_bytes(pair[1].bytes[2..4].try_into().unwrap());
        assert_eq!(
            b,
            a.wrapping_add(1),
            "cancelled frames consumed RTP numbers"
        );
    }
}

#[test]
#[ignore = "global allocator measurement requires exclusive test process"]
fn native_queue_allocations() {
    let (mut receiver, socket) = Receiver::start(200);
    let mut native = Native::start(socket, encoder(), DEPTH, 0);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let owner = dave::Handle::spawn_ready_fixture(8);
        let mut media = owner.media_encryptor();
        let mut buffers = (vec![0; MAX_OPUS], Vec::with_capacity(MAX_PREPARED));
        let (mut writer, mut reader) = crate::frame_channel();
        let source = tokio::spawn(async move {
            for i in 0..164 {
                let frame = Prepared::plain(i);
                writer.send(&frame.raw).await.unwrap();
            }
        });
        for i in 0..64 {
            let mut frame = Prepared::plain(i);
            assert_eq!(
                futures_util::future::poll_fn(|cx| reader.poll_owned(cx, &mut frame.raw)).await,
                FrameStatus::Frame { len: MAX_OPUS }
            );
            prepare(&mut media, &mut frame, &mut buffers).await;
            native.send(frame).await;
        }
        wait_count(&receiver.count, 64).await;
        let region = stats_alloc::Region::new(crate::TEST_ALLOCATOR);
        for i in 64..164 {
            let mut frame = Prepared::plain(i);
            assert_eq!(
                futures_util::future::poll_fn(|cx| reader.poll_owned(cx, &mut frame.raw)).await,
                FrameStatus::Frame { len: MAX_OPUS }
            );
            prepare(&mut media, &mut frame, &mut buffers).await;
            native.send(frame).await;
        }
        wait_count(&receiver.count, 164).await;
        let stats = region.change();
        println!("QUEUE_ALLOCATIONS={stats:?}");
        assert_eq!(stats.allocations, 0);
        assert_eq!(stats.reallocations, 0);
        source.await.unwrap();
    });
    native.shutdown();
    assert_eq!(receiver.finish().len(), 164);
}
