use std::future::pending;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Number, Value, json};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, sleep_until, timeout};
#[cfg(test)]
use tokio_tungstenite::Connector;
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_tls_with_config};

use crate::config::Config;
use crate::connection::StateStore;
use crate::error::{Error, ErrorKind, Operation, RetryDisposition};
use crate::model::{CloseReason, ConnectionGeneration, ConnectionPhase, VoiceConnectInfo};
use crate::transport::{
    DiscoveredAddress, DiscoveredSocket, DiscoveryFailure, TransportEncoder, TransportMode,
    discover, select_mode,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const WRITE_TIMEOUT: Duration = Duration::from_secs(2);
const MIN_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(10);
const MAX_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(300);
const MAX_RECONNECT_ATTEMPTS: u8 = 4;
const UDP_DRAIN_BATCH: usize = 32;
const MAX_TRANSPORT_MODES: usize = 32;

type ClientWebSocket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

pub(crate) enum Command {
    Replace {
        info: ValidatedInfo,
        reply: oneshot::Sender<Result<ConnectionGeneration, Error>>,
    },
    Ping {
        reply: oneshot::Sender<Result<Duration, Error>>,
    },
}

#[derive(Clone)]
pub(crate) struct ValidatedInfo {
    info: VoiceConnectInfo,
    url: Box<str>,
}

impl std::fmt::Debug for ValidatedInfo {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ValidatedInfo")
            .field("info", &self.info)
            .field("url", &self.url)
            .finish()
    }
}

impl ValidatedInfo {
    pub(crate) fn new(
        info: VoiceConnectInfo,
        operation: Operation,
        generation: Option<ConnectionGeneration>,
    ) -> Result<Self, Error> {
        let endpoint = info.endpoint();
        let authority = endpoint.strip_prefix("wss://").unwrap_or(endpoint);
        let valid_ids = info.server_id() != 0 && info.user_id() != 0 && info.channel_id() != 0;
        let valid_strings = !info.session_id().is_empty()
            && info.session_id().len() <= 4_096
            && !info.token().is_empty()
            && info.token().len() <= 8_192;
        let valid_endpoint = !authority.is_empty()
            && authority.len() <= 512
            && !authority.contains(['/', '?', '#', '@'])
            && !authority.chars().any(char::is_whitespace)
            && !endpoint.starts_with("ws://");
        if !valid_ids || !valid_strings || !valid_endpoint {
            return Err(Error::new(
                ErrorKind::InvalidVoiceInfo,
                operation,
                generation,
                RetryDisposition::Fatal,
                None,
                "invalid voice connection information",
            ));
        }
        let url = format!("wss://{authority}/?v=8").into_boxed_str();
        Ok(Self { info, url })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Attempt {
    Identify,
    Resume,
}

enum SessionOutcome {
    Resume,
    FreshIdentify,
    Replace(ValidatedInfo),
    NeedsFresh(Error),
    Fatal(Error),
    CleanRemote,
    Shutdown,
}

enum BackoffOutcome {
    Elapsed,
    Replaced,
    Shutdown,
}

#[derive(Deserialize)]
struct InboundEnvelope {
    op: u64,
    #[serde(default)]
    d: Value,
    #[serde(default)]
    seq: Option<Number>,
}

#[derive(Deserialize)]
struct ReadyData {
    ssrc: u32,
    ip: IpAddr,
    port: u16,
    modes: Vec<String>,
}

#[derive(Deserialize)]
struct SessionDescriptionData {
    mode: String,
    secret_key: [u8; 32],
}

struct DiscoveryCompletion {
    generation: ConnectionGeneration,
    ssrc: u32,
    mode: TransportMode,
    result: Result<DiscoveredSocket, DiscoveryFailure>,
}

struct DiscoveryTask(Option<JoinHandle<()>>);

impl DiscoveryTask {
    fn replace(&mut self, task: JoinHandle<()>) {
        if let Some(previous) = self.0.replace(task) {
            previous.abort();
        }
    }

    fn completed(&mut self) {
        self.0.take();
    }

    fn is_running(&self) -> bool {
        self.0.is_some()
    }
}

impl Drop for DiscoveryTask {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }
}

struct UdpTransport {
    socket: Arc<tokio::net::UdpSocket>,
    ssrc: u32,
    selected_mode: TransportMode,
    encoder: Option<TransportEncoder>,
}

impl UdpTransport {
    fn is_ready(&self) -> bool {
        self.encoder.is_some()
    }
}

pub(crate) async fn run(
    config: Arc<Config>,
    mut info: ValidatedInfo,
    mut commands: mpsc::Receiver<Command>,
    mut shutdown: watch::Receiver<bool>,
    mut store: StateStore,
    initial: oneshot::Sender<Result<(), Error>>,
) {
    let mut initial = Some(initial);
    let mut attempt = Attempt::Identify;
    let mut latest_sequence = None;
    let mut heartbeat_nonce = 0_u64;
    let mut reconnect_attempts = 0_u8;
    let mut transport = None;

    'control: loop {
        if *shutdown.borrow() {
            finish_shutdown(&mut store, &mut initial);
            return;
        }
        match attempt {
            Attempt::Identify if reconnect_attempts == 0 => {
                store.phase_to(ConnectionPhase::Connecting)
            }
            Attempt::Identify => store.reconnecting(),
            Attempt::Resume => store.resume_started(),
        }

        #[cfg(test)]
        let connector = config
            .tls_config
            .as_ref()
            .map(|config| Connector::Rustls(config.clone()));
        #[cfg(not(test))]
        let connector = None;
        let websocket_config = WebSocketConfig::default()
            .max_message_size(Some(
                config
                    .limits
                    .gateway_text_bytes()
                    .max(config.limits.gateway_binary_bytes()),
            ))
            .max_frame_size(Some(
                config
                    .limits
                    .gateway_text_bytes()
                    .max(config.limits.gateway_binary_bytes()),
            ));
        let connect_url = info.url.clone();
        let connect = timeout(
            CONNECT_TIMEOUT,
            connect_async_tls_with_config(
                connect_url.as_ref(),
                Some(websocket_config),
                true,
                connector,
            ),
        );
        tokio::pin!(connect);
        let websocket = loop {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        finish_shutdown(&mut store, &mut initial);
                        return;
                    }
                }
                command = commands.recv() => {
                    match command {
                        Some(Command::Replace { info: replacement, reply }) => {
                            if let Some(replacement) = accept_replacement(&mut store, replacement, reply) {
                                info = replacement;
                                latest_sequence = None;
                                transport = None;
                                reconnect_attempts = 0;
                                attempt = Attempt::Identify;
                                continue 'control;
                            }
                        }
                        Some(Command::Ping { reply }) => {
                            let _ = reply.send(Err(retrying_error(Operation::Ping, store.generation())));
                        }
                        None => {
                            finish_shutdown(&mut store, &mut initial);
                            return;
                        }
                    }
                }
                result = &mut connect => {
                    match result {
                        Ok(Ok((websocket, _response))) => break websocket,
                        Ok(Err(source)) => {
                            let error = endpoint_error(store.generation()).with_source(source);
                            if !schedule_retry(
                                &mut reconnect_attempts,
                                &mut attempt,
                                latest_sequence.is_some(),
                                &mut store,
                                &mut initial,
                                error,
                            ) {
                                return;
                            }
                            match backoff_or_command(
                                reconnect_attempts,
                                &mut info,
                                &mut latest_sequence,
                                &mut commands,
                                &mut shutdown,
                                &mut store,
                            ).await {
                                BackoffOutcome::Elapsed => {}
                                BackoffOutcome::Replaced => {
                                    reconnect_attempts = 0;
                                    attempt = Attempt::Identify;
                                    transport = None;
                                }
                                BackoffOutcome::Shutdown => {
                                    finish_shutdown(&mut store, &mut initial);
                                    return;
                                }
                            }
                            continue 'control;
                        }
                        Err(_) => {
                            let error = endpoint_error(store.generation());
                            if !schedule_retry(
                                &mut reconnect_attempts,
                                &mut attempt,
                                latest_sequence.is_some(),
                                &mut store,
                                &mut initial,
                                error,
                            ) {
                                return;
                            }
                            continue 'control;
                        }
                    }
                }
            }
        };
        store.phase_to(ConnectionPhase::Handshaking);

        let outcome = run_session(
            websocket,
            &config,
            &info,
            attempt,
            &mut latest_sequence,
            &mut heartbeat_nonce,
            &mut reconnect_attempts,
            &mut transport,
            &mut commands,
            &mut shutdown,
            &mut store,
            &mut initial,
        )
        .await;

        match outcome {
            SessionOutcome::Resume => {
                reconnect_attempts = reconnect_attempts.saturating_add(1);
                if reconnect_attempts > MAX_RECONNECT_ATTEMPTS {
                    let error = Error::new(
                        ErrorKind::HeartbeatTimeout,
                        Operation::Resume,
                        Some(store.generation()),
                        RetryDisposition::Fatal,
                        None,
                        "gateway reconnect budget exhausted",
                    );
                    terminal(&mut store, &mut initial, error);
                    return;
                }
                attempt = if latest_sequence.is_some() && transport.is_some() {
                    Attempt::Resume
                } else {
                    latest_sequence = None;
                    transport = None;
                    Attempt::Identify
                };
                match backoff_or_command(
                    reconnect_attempts,
                    &mut info,
                    &mut latest_sequence,
                    &mut commands,
                    &mut shutdown,
                    &mut store,
                )
                .await
                {
                    BackoffOutcome::Elapsed => {}
                    BackoffOutcome::Replaced => {
                        reconnect_attempts = 0;
                        attempt = Attempt::Identify;
                        transport = None;
                    }
                    BackoffOutcome::Shutdown => {
                        finish_shutdown(&mut store, &mut initial);
                        return;
                    }
                }
            }
            SessionOutcome::FreshIdentify => {
                latest_sequence = None;
                transport = None;
                reconnect_attempts = reconnect_attempts.saturating_add(1);
                if reconnect_attempts > MAX_RECONNECT_ATTEMPTS {
                    let error = Error::new(
                        ErrorKind::ResumeRejected,
                        Operation::Resume,
                        Some(store.generation()),
                        RetryDisposition::Fatal,
                        Some(4006),
                        "gateway rejected resume and reconnect budget was exhausted",
                    );
                    terminal(&mut store, &mut initial, error);
                    return;
                }
                attempt = Attempt::Identify;
                match backoff_or_command(
                    reconnect_attempts,
                    &mut info,
                    &mut latest_sequence,
                    &mut commands,
                    &mut shutdown,
                    &mut store,
                )
                .await
                {
                    BackoffOutcome::Elapsed => {}
                    BackoffOutcome::Replaced => {
                        reconnect_attempts = 0;
                        transport = None;
                    }
                    BackoffOutcome::Shutdown => {
                        finish_shutdown(&mut store, &mut initial);
                        return;
                    }
                }
            }
            SessionOutcome::Replace(replacement) => {
                info = replacement;
                latest_sequence = None;
                transport = None;
                reconnect_attempts = 0;
                attempt = Attempt::Identify;
            }
            SessionOutcome::NeedsFresh(error) => {
                store.fail(&error, ConnectionPhase::NeedsFreshVoiceInfo);
                send_initial_error(&mut initial, error.clone());
                match wait_for_replacement(&mut commands, &mut shutdown, &mut store).await {
                    Some(replacement) => {
                        info = replacement;
                        latest_sequence = None;
                        transport = None;
                        reconnect_attempts = 0;
                        attempt = Attempt::Identify;
                    }
                    None => {
                        finish_shutdown(&mut store, &mut initial);
                        return;
                    }
                }
            }
            SessionOutcome::Fatal(error) => {
                terminal(&mut store, &mut initial, error);
                return;
            }
            SessionOutcome::CleanRemote => {
                send_initial_error(
                    &mut initial,
                    Error::new(
                        ErrorKind::Shutdown,
                        Operation::Connect,
                        Some(store.generation()),
                        RetryDisposition::Shutdown,
                        None,
                        "voice gateway closed cleanly during connect",
                    ),
                );
                store.close(CloseReason::CleanRemote);
                return;
            }
            SessionOutcome::Shutdown => {
                finish_shutdown(&mut store, &mut initial);
                return;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_session(
    mut websocket: ClientWebSocket,
    config: &Config,
    info: &ValidatedInfo,
    attempt: Attempt,
    latest_sequence: &mut Option<Number>,
    heartbeat_nonce: &mut u64,
    reconnect_attempts: &mut u8,
    transport: &mut Option<UdpTransport>,
    commands: &mut mpsc::Receiver<Command>,
    shutdown: &mut watch::Receiver<bool>,
    store: &mut StateStore,
    initial: &mut Option<oneshot::Sender<Result<(), Error>>>,
) -> SessionOutcome {
    let mut heartbeat_interval = None;
    let mut heartbeat_deadline = None;
    let mut outstanding = None::<(u64, Instant)>;
    let mut missed_heartbeats = 0_u8;
    let mut pending_pings = Vec::new();
    let (discovery_sender, mut discovery_receiver) = mpsc::channel::<DiscoveryCompletion>(1);
    let mut discovery_task = DiscoveryTask(None);
    let mut udp_receive_buffer = vec![0_u8; config.limits.udp_datagram_bytes() + 1];

    loop {
        let udp_socket = transport.as_ref().map(|transport| transport.socket.clone());
        let has_udp_socket = udp_socket.is_some();
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    let _ = timed_close(&mut websocket).await;
                    fail_pending_pings(&mut pending_pings, store.generation());
                    return SessionOutcome::Shutdown;
                }
            }
            command = commands.recv() => {
                match command {
                    Some(Command::Replace { info: replacement, reply }) => {
                        if let Some(replacement) = accept_replacement(store, replacement, reply) {
                            let _ = timed_close(&mut websocket).await;
                            fail_pending_pings(&mut pending_pings, store.generation());
                            return SessionOutcome::Replace(replacement);
                        }
                    }
                    Some(Command::Ping { reply }) => {
                        if heartbeat_interval.is_none() {
                            let _ = reply.send(Err(retrying_error(Operation::Ping, store.generation())));
                            continue;
                        }
                        pending_pings.push(reply);
                        if outstanding.is_none() {
                            match send_heartbeat(
                                &mut websocket,
                                latest_sequence,
                                heartbeat_nonce,
                            ).await {
                                Ok(sent) => outstanding = Some(sent),
                                Err(error) => {
                                    fail_pending_pings(&mut pending_pings, store.generation());
                                    return SessionOutcome::Fatal(error_for_generation(
                                        error, Operation::Ping, store.generation()
                                    ));
                                }
                            }
                        }
                    }
                    None => {
                        let _ = timed_close(&mut websocket).await;
                        return SessionOutcome::Shutdown;
                    }
                }
            }
            _ = async {
                if let Some(deadline) = heartbeat_deadline {
                    sleep_until(deadline).await;
                } else {
                    pending::<()>().await;
                }
            } => {
                let interval = heartbeat_interval.expect("heartbeat deadline requires interval");
                let now = Instant::now();
                heartbeat_deadline = Some(next_deadline(
                    heartbeat_deadline.expect("heartbeat deadline exists"), interval, now
                ));
                if outstanding.is_some() {
                    missed_heartbeats = missed_heartbeats.saturating_add(1);
                    if missed_heartbeats >= 2 {
                        store.heartbeat_timeout();
                        fail_pending_pings(&mut pending_pings, store.generation());
                        return SessionOutcome::Resume;
                    }
                } else {
                    match send_heartbeat(&mut websocket, latest_sequence, heartbeat_nonce).await {
                        Ok(sent) => outstanding = Some(sent),
                        Err(_) => {
                            fail_pending_pings(&mut pending_pings, store.generation());
                            return SessionOutcome::Resume;
                        }
                    }
                }
            }
            completion = discovery_receiver.recv(), if discovery_task.is_running() => {
                let Some(completion) = completion else {
                    return SessionOutcome::Fatal(udp_discovery_error(store.generation()));
                };
                discovery_task.completed();
                if completion.generation != store.generation() {
                    continue;
                }
                let discovered = match completion.result {
                    Ok(discovered) => discovered,
                    Err(failure) => {
                        return SessionOutcome::Fatal(udp_discovery_error_with_source(
                            store.generation(), failure
                        ));
                    }
                };
                let select = select_protocol_payload(discovered.public, completion.mode);
                *transport = Some(UdpTransport {
                    socket: discovered.socket,
                    ssrc: completion.ssrc,
                    selected_mode: completion.mode,
                    encoder: None,
                });
                if let Err(error) = timed_send(&mut websocket, select).await {
                    return SessionOutcome::Fatal(error_for_generation(
                        error, Operation::Connect, store.generation()
                    ));
                }
            }
            readiness = async move {
                match udp_socket {
                    Some(socket) => Some(socket.readable().await),
                    None => pending::<Option<Result<(), std::io::Error>>>().await,
                }
            }, if has_udp_socket => {
                match readiness {
                    Some(Ok(())) => match drain_udp(
                        transport.as_ref().expect("readiness requires UDP transport"),
                        &mut udp_receive_buffer,
                    ) {
                        Ok(discarded) => {
                            if discarded != 0 {
                                store.discarded_udp_datagrams(discarded as u64);
                            }
                        }
                        Err(_) => return SessionOutcome::FreshIdentify,
                    },
                    Some(Err(_)) => return SessionOutcome::FreshIdentify,
                    None => unreachable!("UDP readiness branch requires socket"),
                }
            }
            incoming = websocket.next() => {
                let Some(incoming) = incoming else {
                    fail_pending_pings(&mut pending_pings, store.generation());
                    return resumable_outcome(latest_sequence.is_some());
                };
                let message = match incoming {
                    Ok(message) => message,
                    Err(tokio_tungstenite::tungstenite::Error::Capacity(_)) => {
                        return SessionOutcome::Fatal(Error::new(
                            ErrorKind::ResourceLimit,
                            Operation::Connect,
                            Some(store.generation()),
                            RetryDisposition::Fatal,
                            None,
                            "voice gateway frame exceeded configured limit",
                        ));
                    }
                    Err(_) => {
                        fail_pending_pings(&mut pending_pings, store.generation());
                        return resumable_outcome(latest_sequence.is_some());
                    }
                };
                match message {
                    Message::Text(text) => {
                        if text.len() > config.limits.gateway_text_bytes() {
                            return SessionOutcome::Fatal(resource_error(store.generation()));
                        }
                        let envelope: InboundEnvelope = match serde_json::from_str(&text) {
                            Ok(value) => value,
                            Err(source) => {
                                return SessionOutcome::Fatal(protocol_error(store.generation()).with_source(source));
                            }
                        };
                        if let Some(sequence) = envelope.seq {
                            if !sequence.is_i64() && !sequence.is_u64() {
                                return SessionOutcome::Fatal(protocol_error(store.generation()));
                            }
                            *latest_sequence = Some(sequence);
                        }
                        let data = envelope.d;
                        match envelope.op {
                            8 => {
                                if heartbeat_interval.is_some() {
                                    return SessionOutcome::Fatal(protocol_error(store.generation()));
                                }
                                let Some(milliseconds) = data
                                    .get("heartbeat_interval")
                                    .and_then(Value::as_u64)
                                else {
                                    return SessionOutcome::Fatal(protocol_error(store.generation()));
                                };
                                let interval = Duration::from_millis(milliseconds);
                                if interval < MIN_HEARTBEAT_INTERVAL
                                    || interval > MAX_HEARTBEAT_INTERVAL
                                {
                                    return SessionOutcome::Fatal(protocol_error(store.generation()));
                                }
                                heartbeat_interval = Some(interval);
                                let identify = match attempt {
                                    Attempt::Identify => identify_payload(info),
                                    Attempt::Resume => resume_payload(info, latest_sequence.as_ref()),
                                };
                                if let Err(error) = timed_send(&mut websocket, identify).await {
                                    return SessionOutcome::Fatal(error_for_generation(
                                        error, Operation::Connect, store.generation()
                                    ));
                                }
                                match send_heartbeat(
                                    &mut websocket,
                                    latest_sequence,
                                    heartbeat_nonce,
                                ).await {
                                    Ok(sent) => outstanding = Some(sent),
                                    Err(error) => {
                                        return SessionOutcome::Fatal(error_for_generation(
                                            error, Operation::Connect, store.generation()
                                        ));
                                    }
                                }
                                heartbeat_deadline = Some(Instant::now() + interval);
                            }
                            2 if attempt == Attempt::Identify => {
                                let ready = match parse_ready(data, store.generation()) {
                                    Ok(ready) => ready,
                                    Err(error) => return SessionOutcome::Fatal(error),
                                };
                                let Some(mode) = select_mode(&ready.modes) else {
                                    return SessionOutcome::Fatal(unsupported_transport_error(
                                        store.generation()
                                    ));
                                };
                                let generation = store.generation();
                                let remote = SocketAddr::new(ready.ip, ready.port);
                                let max_datagram_bytes = config.limits.udp_datagram_bytes();
                                let sender = discovery_sender.clone();
                                *transport = None;
                                discovery_task.replace(tokio::spawn(async move {
                                    let result = discover(remote, ready.ssrc, max_datagram_bytes).await;
                                    let _ = sender.send(DiscoveryCompletion {
                                        generation,
                                        ssrc: ready.ssrc,
                                        mode,
                                        result,
                                    }).await;
                                }));
                                store.phase_to(ConnectionPhase::EstablishingTransport);
                                *reconnect_attempts = 0;
                            }
                            4 => {
                                let Some(active) = transport.as_mut() else {
                                    return SessionOutcome::Fatal(protocol_error(store.generation()));
                                };
                                if active.encoder.is_some() {
                                    return SessionOutcome::Fatal(protocol_error(store.generation()));
                                }
                                let description: SessionDescriptionData = match serde_json::from_value(data) {
                                    Ok(description) => description,
                                    Err(source) => return SessionOutcome::Fatal(
                                        protocol_error(store.generation()).with_source(source)
                                    ),
                                };
                                let Some(mode) = TransportMode::parse(&description.mode) else {
                                    return SessionOutcome::Fatal(unsupported_transport_error(
                                        store.generation()
                                    ));
                                };
                                if mode != active.selected_mode {
                                    return SessionOutcome::Fatal(protocol_error(store.generation()));
                                }
                                let encoder = match TransportEncoder::new(
                                    mode,
                                    &description.secret_key,
                                    active.ssrc,
                                    config.limits.encoded_opus_frame_bytes(),
                                    config.limits.udp_datagram_bytes(),
                                ) {
                                    Ok(encoder) => encoder,
                                    Err(_) => return SessionOutcome::Fatal(transport_crypto_error(
                                        store.generation()
                                    )),
                                };
                                debug_assert_eq!(encoder.mode(), mode);
                                active.encoder = Some(encoder);
                                store.phase_to(ConnectionPhase::EstablishingDave);
                                *reconnect_attempts = 0;
                                if let Some(sender) = initial.take() {
                                    let _ = sender.send(Ok(()));
                                }
                            }
                            6 => {
                                let Some(nonce) = data.get("t").and_then(Value::as_u64) else {
                                    return SessionOutcome::Fatal(protocol_error(store.generation()));
                                };
                                if let Some((expected, sent)) = outstanding
                                    && nonce == expected
                                {
                                    let rtt = Instant::now().saturating_duration_since(sent);
                                    outstanding = None;
                                    missed_heartbeats = 0;
                                    store.rtt(rtt);
                                    for waiter in pending_pings.drain(..) {
                                        let _ = waiter.send(Ok(rtt));
                                    }
                                }
                            }
                            9 if attempt == Attempt::Resume => {
                                *reconnect_attempts = 0;
                                let phase = if transport.as_ref().is_some_and(UdpTransport::is_ready) {
                                    ConnectionPhase::EstablishingDave
                                } else {
                                    ConnectionPhase::EstablishingTransport
                                };
                                store.resume_succeeded(phase);
                            }
                            _ => store.unknown_opcode(),
                        }
                    }
                    Message::Binary(bytes) => {
                        if bytes.len() > config.limits.gateway_binary_bytes() {
                            return SessionOutcome::Fatal(resource_error(store.generation()));
                        }
                        if bytes.len() < 3 {
                            return SessionOutcome::Fatal(protocol_error(store.generation()));
                        }
                        *latest_sequence = Some(Number::from(u16::from_be_bytes([bytes[0], bytes[1]])));
                        // P08 owns binary DAVE payload handling. P05 only consumes the
                        // numbered envelope so Resume acknowledges it correctly.
                        store.unknown_opcode();
                    }
                    Message::Close(frame) => {
                        fail_pending_pings(&mut pending_pings, store.generation());
                        let Some(frame) = frame else {
                            return SessionOutcome::Resume;
                        };
                        return close_outcome(u16::from(frame.code), store.generation(), attempt);
                    }
                    Message::Ping(payload) => {
                        if timeout(WRITE_TIMEOUT, websocket.send(Message::Pong(payload))).await.is_err() {
                            return SessionOutcome::Resume;
                        }
                    }
                    Message::Pong(_) | Message::Frame(_) => {}
                }
            }
        }
    }
}

fn identify_payload(info: &ValidatedInfo) -> Message {
    json_message(json!({
        "op": 0,
        "d": {
            "server_id": info.info.server_id().to_string(),
            "user_id": info.info.user_id().to_string(),
            "session_id": info.info.session_id(),
            "token": info.info.token(),
            "max_dave_protocol_version": 1,
        }
    }))
}

fn resume_payload(info: &ValidatedInfo, latest_sequence: Option<&Number>) -> Message {
    let seq_ack = latest_sequence
        .cloned()
        .map(Value::Number)
        .unwrap_or_else(|| Value::from(-1));
    json_message(json!({
        "op": 7,
        "d": {
            "server_id": info.info.server_id().to_string(),
            "session_id": info.info.session_id(),
            "token": info.info.token(),
            "seq_ack": seq_ack,
        }
    }))
}

fn select_protocol_payload(public: DiscoveredAddress, mode: TransportMode) -> Message {
    json_message(json!({
        "op": 1,
        "d": {
            "protocol": "udp",
            "data": {
                "address": public.address.to_string(),
                "port": public.port,
                "mode": mode.name(),
            }
        }
    }))
}

fn drain_udp(transport: &UdpTransport, buffer: &mut [u8]) -> Result<usize, std::io::Error> {
    let mut discarded = 0;
    while discarded < UDP_DRAIN_BATCH {
        match transport.socket.try_recv(buffer) {
            Ok(_) => discarded += 1,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(error) => return Err(error),
        }
    }
    Ok(discarded)
}

async fn send_heartbeat(
    websocket: &mut ClientWebSocket,
    latest_sequence: &Option<Number>,
    nonce: &mut u64,
) -> Result<(u64, Instant), Error> {
    *nonce = nonce.checked_add(1).ok_or_else(|| {
        Error::new(
            ErrorKind::ResourceLimit,
            Operation::Connect,
            None,
            RetryDisposition::Fatal,
            None,
            "heartbeat nonce exhausted",
        )
    })?;
    let current = *nonce;
    let seq_ack = latest_sequence
        .clone()
        .map(Value::Number)
        .unwrap_or_else(|| Value::from(-1));
    let message = json_message(json!({
        "op": 3,
        "d": {"t": current, "seq_ack": seq_ack}
    }));
    timed_send(websocket, message).await?;
    Ok((current, Instant::now()))
}

async fn timed_send(websocket: &mut ClientWebSocket, message: Message) -> Result<(), Error> {
    match timeout(WRITE_TIMEOUT, websocket.send(message)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(source)) => Err(Error::new(
            ErrorKind::SendIo,
            Operation::Connect,
            None,
            RetryDisposition::RetryingInternally,
            None,
            "voice gateway write failed",
        )
        .with_source(source)),
        Err(_) => Err(Error::new(
            ErrorKind::SendIo,
            Operation::Connect,
            None,
            RetryDisposition::RetryingInternally,
            None,
            "voice gateway write timed out",
        )),
    }
}

async fn timed_close(websocket: &mut ClientWebSocket) -> Result<(), Error> {
    timed_send(websocket, Message::Close(None)).await
}

fn json_message(value: Value) -> Message {
    Message::Text(value.to_string().into())
}

fn parse_ready(data: Value, generation: ConnectionGeneration) -> Result<ReadyData, Error> {
    let ready: ReadyData = serde_json::from_value(data)
        .map_err(|source| protocol_error(generation).with_source(source))?;
    if ready.port == 0 || ready.modes.is_empty() || ready.modes.len() > MAX_TRANSPORT_MODES {
        return Err(protocol_error(generation));
    }
    Ok(ready)
}

fn next_deadline(previous: Instant, interval: Duration, now: Instant) -> Instant {
    let next = previous + interval;
    if next <= now { now + interval } else { next }
}

fn close_outcome(code: u16, generation: ConnectionGeneration, attempt: Attempt) -> SessionOutcome {
    let (kind, retry) = match code {
        1000 => return SessionOutcome::CleanRemote,
        4001 | 4002 | 4003 | 4005 | 4006 | 4009 => {
            if code == 4006 && attempt == Attempt::Resume {
                return SessionOutcome::FreshIdentify;
            }
            return SessionOutcome::FreshIdentify;
        }
        4004 => (ErrorKind::CredentialsRejected, RetryDisposition::Fatal),
        4011 | 4014 | 4021 | 4022 => (
            ErrorKind::NeedsFreshVoiceInfo,
            RetryDisposition::NeedsFreshVoiceInfo,
        ),
        4012 | 4016 => (ErrorKind::UnsupportedTransport, RetryDisposition::Fatal),
        4015 => return SessionOutcome::Resume,
        4017 => (ErrorKind::DaveRequired, RetryDisposition::Fatal),
        4020 => (ErrorKind::GatewayProtocol, RetryDisposition::Fatal),
        _ => return SessionOutcome::Resume,
    };
    let error = Error::new(
        kind,
        Operation::Connect,
        Some(generation),
        retry,
        Some(u32::from(code)),
        "voice gateway closed the connection",
    );
    if retry == RetryDisposition::NeedsFreshVoiceInfo {
        SessionOutcome::NeedsFresh(error)
    } else {
        SessionOutcome::Fatal(error)
    }
}

fn resumable_outcome(has_latest_sequence: bool) -> SessionOutcome {
    if has_latest_sequence {
        SessionOutcome::Resume
    } else {
        SessionOutcome::FreshIdentify
    }
}

fn accept_replacement(
    store: &mut StateStore,
    info: ValidatedInfo,
    reply: oneshot::Sender<Result<ConnectionGeneration, Error>>,
) -> Option<ValidatedInfo> {
    let Some(generation) = store.generation().next() else {
        let error = Error::new(
            ErrorKind::ResourceLimit,
            Operation::ReplaceVoiceInfo,
            Some(store.generation()),
            RetryDisposition::Fatal,
            None,
            "connection generation exhausted",
        );
        let _ = reply.send(Err(error.clone()));
        store.fail(&error, ConnectionPhase::Failed);
        return None;
    };
    store.replace_generation(generation);
    let _ = reply.send(Ok(generation));
    Some(info)
}

async fn wait_for_replacement(
    commands: &mut mpsc::Receiver<Command>,
    shutdown: &mut watch::Receiver<bool>,
    store: &mut StateStore,
) -> Option<ValidatedInfo> {
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { return None; }
            }
            command = commands.recv() => match command? {
                Command::Replace { info, reply } => {
                    if let Some(info) = accept_replacement(store, info, reply) {
                        return Some(info);
                    }
                }
                Command::Ping { reply } => {
                    let _ = reply.send(Err(Error::new(
                        ErrorKind::NeedsFreshVoiceInfo,
                        Operation::Ping,
                        Some(store.generation()),
                        RetryDisposition::NeedsFreshVoiceInfo,
                        None,
                        "fresh voice information is required",
                    )));
                }
            }
        }
    }
}

async fn backoff_or_command(
    attempt: u8,
    info: &mut ValidatedInfo,
    latest_sequence: &mut Option<Number>,
    commands: &mut mpsc::Receiver<Command>,
    shutdown: &mut watch::Receiver<bool>,
    store: &mut StateStore,
) -> BackoffOutcome {
    let base = 25_u64 << attempt.min(6);
    let mut identity = info.info.server_id() ^ info.info.user_id().rotate_left(17);
    for byte in info.info.session_id().bytes() {
        identity ^= u64::from(byte);
        identity = identity.wrapping_mul(0x100_0000_01B3);
    }
    let jitter = identity.wrapping_mul(0x9E37_79B9_7F4A_7C15) % (base / 2 + 1);
    let delay = Duration::from_millis(base + jitter);
    tokio::select! {
        _ = sleep(delay) => BackoffOutcome::Elapsed,
        changed = shutdown.changed() => {
            if changed.is_err() || *shutdown.borrow() {
                BackoffOutcome::Shutdown
            } else {
                BackoffOutcome::Elapsed
            }
        },
        command = commands.recv() => match command {
            Some(Command::Replace { info: replacement, reply }) => {
                if let Some(replacement) = accept_replacement(store, replacement, reply) {
                    *info = replacement;
                    *latest_sequence = None;
                    BackoffOutcome::Replaced
                } else {
                    BackoffOutcome::Elapsed
                }
            }
            Some(Command::Ping { reply }) => {
                let _ = reply.send(Err(retrying_error(Operation::Ping, store.generation())));
                BackoffOutcome::Elapsed
            }
            None => BackoffOutcome::Shutdown,
        }
    }
}

fn schedule_retry(
    attempts: &mut u8,
    attempt: &mut Attempt,
    has_latest_sequence: bool,
    store: &mut StateStore,
    initial: &mut Option<oneshot::Sender<Result<(), Error>>>,
    error: Error,
) -> bool {
    *attempts = attempts.saturating_add(1);
    if *attempts > MAX_RECONNECT_ATTEMPTS {
        terminal(store, initial, error);
        false
    } else {
        *attempt = if has_latest_sequence {
            Attempt::Resume
        } else {
            Attempt::Identify
        };
        true
    }
}

fn endpoint_error(generation: ConnectionGeneration) -> Error {
    Error::new(
        ErrorKind::EndpointOrTls,
        Operation::Connect,
        Some(generation),
        RetryDisposition::RetryingInternally,
        None,
        "voice gateway endpoint or TLS connection failed",
    )
}

fn protocol_error(generation: ConnectionGeneration) -> Error {
    Error::new(
        ErrorKind::GatewayProtocol,
        Operation::Connect,
        Some(generation),
        RetryDisposition::Fatal,
        None,
        "voice gateway protocol payload was invalid",
    )
}

fn udp_discovery_error(generation: ConnectionGeneration) -> Error {
    Error::new(
        ErrorKind::UdpDiscovery,
        Operation::Connect,
        Some(generation),
        RetryDisposition::Fatal,
        None,
        "UDP discovery failed after bounded retries",
    )
}

fn udp_discovery_error_with_source(
    generation: ConnectionGeneration,
    failure: DiscoveryFailure,
) -> Error {
    udp_discovery_error(generation).with_source(failure)
}

fn unsupported_transport_error(generation: ConnectionGeneration) -> Error {
    Error::new(
        ErrorKind::UnsupportedTransport,
        Operation::Connect,
        Some(generation),
        RetryDisposition::Fatal,
        None,
        "voice gateway offered no supported transport mode",
    )
}

fn transport_crypto_error(generation: ConnectionGeneration) -> Error {
    Error::new(
        ErrorKind::TransportCrypto,
        Operation::Connect,
        Some(generation),
        RetryDisposition::Fatal,
        None,
        "transport cipher initialization failed",
    )
}

fn resource_error(generation: ConnectionGeneration) -> Error {
    Error::new(
        ErrorKind::ResourceLimit,
        Operation::Connect,
        Some(generation),
        RetryDisposition::Fatal,
        None,
        "voice gateway message exceeded configured limit",
    )
}

fn retrying_error(operation: Operation, generation: ConnectionGeneration) -> Error {
    Error::new(
        ErrorKind::EndpointOrTls,
        operation,
        Some(generation),
        RetryDisposition::RetryingInternally,
        None,
        "voice gateway is reconnecting",
    )
}

fn error_for_generation(
    error: Error,
    operation: Operation,
    generation: ConnectionGeneration,
) -> Error {
    error.for_operation_generation(operation, generation)
}

fn fail_pending_pings(
    pending: &mut Vec<oneshot::Sender<Result<Duration, Error>>>,
    generation: ConnectionGeneration,
) {
    for waiter in pending.drain(..) {
        let _ = waiter.send(Err(retrying_error(Operation::Ping, generation)));
    }
}

fn terminal(
    store: &mut StateStore,
    initial: &mut Option<oneshot::Sender<Result<(), Error>>>,
    error: Error,
) {
    store.fail(&error, ConnectionPhase::Failed);
    send_initial_error(initial, error);
}

fn send_initial_error(initial: &mut Option<oneshot::Sender<Result<(), Error>>>, error: Error) {
    if let Some(sender) = initial.take() {
        let _ = sender.send(Err(error));
    }
}

fn finish_shutdown(
    store: &mut StateStore,
    initial: &mut Option<oneshot::Sender<Result<(), Error>>>,
) {
    send_initial_error(
        initial,
        Error::new(
            ErrorKind::Shutdown,
            Operation::Connect,
            Some(store.generation()),
            RetryDisposition::Shutdown,
            None,
            "connection was shut down",
        ),
    );
    if !matches!(
        store.phase(),
        ConnectionPhase::Closed | ConnectionPhase::Failed
    ) {
        store.phase_to(ConnectionPhase::Closing);
        store.close(CloseReason::ExplicitShutdown);
    }
}

#[cfg(test)]
mod tests {
    use oto_testkit::{
        FakeUdpServer, FakeUdpServerConfig, FakeVoiceGateway, FakeVoiceGatewayConfig, FaultAction,
        GatewayCloseStage, GatewayRecord, ManualClock, ScriptedClose, TestTls, VoiceClose,
    };

    use super::*;
    use crate::{ConnectionPhase, ErrorKind, EventReceiveError, Oto, ResourceLimits, VoiceToken};

    struct TestGateway {
        gateway: FakeVoiceGateway,
        udp: FakeUdpServer,
        clock: ManualClock,
    }

    impl std::ops::Deref for TestGateway {
        type Target = FakeVoiceGateway;

        fn deref(&self) -> &Self::Target {
            &self.gateway
        }
    }

    impl TestGateway {
        async fn start(
            mut config: FakeVoiceGatewayConfig,
        ) -> Result<Self, oto_testkit::GatewayError> {
            let clock = ManualClock::new(Duration::ZERO);
            let udp = FakeUdpServer::start(FakeUdpServerConfig::localhost(clock.clone()))
                .await
                .expect("fake UDP peer starts");
            config.voice_ip = udp.local_addr().ip().to_string();
            config.voice_port = udp.local_addr().port();
            let gateway = FakeVoiceGateway::start(config).await?;
            Ok(Self {
                gateway,
                udp,
                clock,
            })
        }

        async fn start_with_tls(
            mut config: FakeVoiceGatewayConfig,
            tls: TestTls,
        ) -> Result<Self, oto_testkit::GatewayError> {
            let clock = ManualClock::new(Duration::ZERO);
            let udp = FakeUdpServer::start(FakeUdpServerConfig::localhost(clock.clone()))
                .await
                .expect("fake UDP peer starts");
            config.voice_ip = udp.local_addr().ip().to_string();
            config.voice_port = udp.local_addr().port();
            let gateway = FakeVoiceGateway::start_with_tls(config, tls).await?;
            Ok(Self {
                gateway,
                udp,
                clock,
            })
        }

        async fn shutdown(self) -> Result<(), oto_testkit::GatewayError> {
            self.gateway.shutdown().await?;
            self.udp.shutdown().await.expect("fake UDP peer shuts down");
            Ok(())
        }
    }

    fn voice_info(gateway: &FakeVoiceGateway, session: &str, token: &str) -> VoiceConnectInfo {
        VoiceConnectInfo::new(
            1,
            2,
            3,
            session,
            format!("localhost:{}", gateway.local_addr().port()),
            VoiceToken::new(token),
        )
    }

    fn test_oto(gateway: &FakeVoiceGateway, limits: ResourceLimits) -> Oto {
        Oto::builder()
            .resource_limits(limits)
            .test_tls_config(gateway.tls().client_config())
            .build()
            .expect("test Oto config is valid")
    }

    async fn eventually(mut condition: impl FnMut() -> bool) {
        timeout(Duration::from_secs(2), async {
            while !condition() {
                tokio::task::yield_now().await;
                sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("condition did not become true");
    }

    async fn yield_eventually(label: &str, mut condition: impl FnMut() -> bool) {
        for _ in 0..20_000 {
            if condition() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("{label} did not become true after bounded scheduler yields");
    }

    #[tokio::test]
    async fn gateway_v8_identify_numbered_heartbeat_ping_and_unknown_payloads() {
        let mut config = FakeVoiceGatewayConfig::local();
        config.heartbeat_interval = Duration::from_millis(50);
        config.sequence_start = u16::MAX;
        let gateway = TestGateway::start(config).await.expect("gateway starts");
        let oto = test_oto(&gateway, ResourceLimits::default());
        let info = voice_info(&gateway, "session-a", "super-secret-token");
        assert!(!format!("{info:?}").contains("super-secret-token"));

        let connection = oto.connect(info).await.expect("gateway connects");
        assert_eq!(
            connection.state().phase(),
            ConnectionPhase::EstablishingDave
        );
        assert!(connection.ping().await.expect("ping is acknowledged") < Duration::from_secs(1));

        gateway
            .try_dispatch_json(250, json!({"future": true, "extra": [1, 2]}), true)
            .expect("unknown numbered dispatch queues");
        gateway
            .try_dispatch_binary(99, vec![1, 2, 3])
            .expect("unknown binary dispatch queues");
        eventually(|| connection.state().stats().unknown_opcodes() >= 2).await;
        connection
            .ping()
            .await
            .expect("post-sequence ping is acknowledged");
        let rtt = connection
            .state()
            .gateway_rtt()
            .expect("heartbeat RTT is durable");
        assert!(rtt < Duration::from_secs(1));

        eventually(|| {
            gateway.records().iter().any(|record| {
                matches!(
                    record,
                    GatewayRecord::Heartbeat {
                        seq_ack: Some(2),
                        ..
                    }
                )
            })
        })
        .await;

        let records = gateway.records();
        assert!(
            records
                .iter()
                .any(|record| matches!(record, GatewayRecord::Identify(_)))
        );
        assert!(
            records
                .iter()
                .any(|record| matches!(record, GatewayRecord::Heartbeat { .. }))
        );
        assert!(records.iter().any(|record| {
            matches!(
                record,
                GatewayRecord::Heartbeat {
                    seq_ack: Some(2),
                    ..
                }
            )
        }));

        let snapshot = connection.shutdown().await.expect("shutdown succeeds");
        assert_eq!(snapshot.phase(), ConnectionPhase::Closed);
        assert_eq!(snapshot.close_reason(), Some(CloseReason::ExplicitShutdown));
        gateway.shutdown().await.expect("gateway shuts down");
    }

    #[tokio::test]
    async fn transport_mode_preference_fallback_and_rejection_are_end_to_end() {
        for (modes, expected) in [
            (
                vec![
                    "aead_xchacha20_poly1305_rtpsize".to_owned(),
                    "future_mode".to_owned(),
                    "aead_aes256_gcm_rtpsize".to_owned(),
                ],
                "aead_aes256_gcm_rtpsize",
            ),
            (
                vec!["aead_xchacha20_poly1305_rtpsize".to_owned()],
                "aead_xchacha20_poly1305_rtpsize",
            ),
        ] {
            let mut config = FakeVoiceGatewayConfig::local();
            config.modes = modes;
            let gateway = TestGateway::start(config).await.expect("gateway starts");
            let oto = test_oto(&gateway, ResourceLimits::default());
            let connection = oto
                .connect(voice_info(&gateway, "mode-session", "mode-token"))
                .await
                .expect("supported transport negotiates");
            assert!(gateway.records().iter().any(|record| {
                matches!(record, GatewayRecord::SelectProtocol(data)
                    if data.get("data")
                        .and_then(|data| data.get("mode"))
                        .and_then(Value::as_str) == Some(expected))
            }));
            connection.shutdown().await.expect("connection shuts down");
            gateway.shutdown().await.expect("gateway shuts down");
        }

        let mut config = FakeVoiceGatewayConfig::local();
        config.modes = vec!["xsalsa20_poly1305_lite_rtpsize".to_owned()];
        let gateway = TestGateway::start(config).await.expect("gateway starts");
        let oto = test_oto(&gateway, ResourceLimits::default());
        let error = oto
            .connect(voice_info(&gateway, "legacy-session", "legacy-token"))
            .await
            .expect_err("discontinued-only offer is rejected");
        assert_eq!(error.kind(), ErrorKind::UnsupportedTransport);
        assert!(gateway.udp.capture().is_empty());
        gateway.shutdown().await.expect("gateway shuts down");
    }

    #[tokio::test]
    async fn connected_transport_continuously_drains_bounded_inbound_udp() {
        let gateway = TestGateway::start(FakeVoiceGatewayConfig::local())
            .await
            .expect("gateway starts");
        let oto = test_oto(&gateway, ResourceLimits::default());
        let connection = oto
            .connect(voice_info(&gateway, "drain-session", "drain-token"))
            .await
            .expect("transport connects");

        for packet in 0_u8..8 {
            gateway
                .udp
                .try_send_to_client(vec![packet; 64])
                .expect("bounded inbound datagram queues");
        }
        eventually(|| connection.state().stats().discarded_udp_datagrams() >= 8).await;
        assert_eq!(
            connection.state().phase(),
            ConnectionPhase::EstablishingDave
        );

        connection.shutdown().await.expect("connection shuts down");
        gateway.shutdown().await.expect("gateway shuts down");
    }

    #[tokio::test]
    async fn replacement_cancels_stale_udp_discovery_without_crossing_generations() {
        let gateway = TestGateway::start(FakeVoiceGatewayConfig::local())
            .await
            .expect("gateway starts");
        let oto = test_oto(&gateway, ResourceLimits::default());
        let connection = oto
            .connect(voice_info(&gateway, "generation-one", "token-one"))
            .await
            .expect("first generation connects");

        gateway
            .udp
            .push_fault(FaultAction::Delay(Duration::from_secs(30)))
            .expect("next discovery response is delayed");
        let second = connection
            .replace_voice_info(voice_info(&gateway, "generation-two", "token-two"))
            .await
            .expect("second generation is accepted");
        assert_eq!(second.get(), 2);
        yield_eventually("second generation UDP discovery", || {
            gateway.udp.capture().len() >= 2
        })
        .await;

        let third = connection
            .replace_voice_info(voice_info(&gateway, "generation-three", "token-three"))
            .await
            .expect("third generation supersedes discovery");
        assert_eq!(third.get(), 3);
        eventually(|| {
            connection.state().generation() == third
                && connection.state().phase() == ConnectionPhase::EstablishingDave
        })
        .await;

        gateway
            .clock
            .advance(Duration::from_secs(30))
            .expect("delayed stale response is released");
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        assert_eq!(connection.state().generation(), third);
        assert_eq!(
            connection.state().phase(),
            ConnectionPhase::EstablishingDave
        );

        connection.shutdown().await.expect("connection shuts down");
        gateway.shutdown().await.expect("gateway shuts down");
    }

    #[tokio::test]
    async fn dropped_and_delayed_heartbeat_ack_trigger_bounded_resume_and_rtt() {
        let mut config = FakeVoiceGatewayConfig::local();
        config.heartbeat_interval = Duration::from_millis(100);
        config.drop_heartbeat_acks = 1;
        config.heartbeat_ack_delay = Duration::from_millis(3);
        let gateway = TestGateway::start(config).await.expect("gateway starts");
        let oto = test_oto(&gateway, ResourceLimits::default());
        let connection = oto
            .connect(voice_info(&gateway, "session-heartbeat", "token-a"))
            .await
            .expect("gateway connects before missing ACK policy fires");
        tokio::time::pause();

        for _ in 0..4 {
            tokio::time::advance(Duration::from_millis(110)).await;
            for _ in 0..100 {
                tokio::task::yield_now().await;
            }
            if connection.state().stats().heartbeat_timeouts() == 1 {
                break;
            }
        }
        assert_eq!(connection.state().stats().heartbeat_timeouts(), 1);
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_millis(100)).await;
        for _ in 0..20_000 {
            if gateway
                .records()
                .iter()
                .any(|record| matches!(record, GatewayRecord::Resume { .. }))
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            gateway
                .records()
                .iter()
                .any(|record| matches!(record, GatewayRecord::Resume { .. })),
            "resume missing; state={:?}, records={:?}",
            connection.state(),
            gateway.records()
        );
        yield_eventually("resume success", || {
            connection.state().stats().resume_successes() == 1
        })
        .await;
        yield_eventually("post-resume heartbeat", || {
            gateway
                .records()
                .iter()
                .filter(|record| matches!(record, GatewayRecord::Heartbeat { .. }))
                .count()
                >= 2
        })
        .await;
        tokio::time::advance(Duration::from_millis(4)).await;
        yield_eventually("delayed ACK RTT", || {
            connection.state().gateway_rtt().is_some()
        })
        .await;
        let state = connection.state();
        assert_eq!(state.stats().heartbeat_timeouts(), 1);
        assert_eq!(state.stats().resume_attempts(), 1);
        assert!(
            connection
                .state()
                .gateway_rtt()
                .expect("delayed ACK records RTT")
                >= Duration::from_millis(2)
        );

        connection.shutdown().await.expect("shutdown succeeds");
        gateway.shutdown().await.expect("gateway shuts down");
    }

    #[tokio::test]
    async fn buffered_replay_sequence_wrap_and_failed_resume_fall_back_to_identify() {
        let mut config = FakeVoiceGatewayConfig::local();
        config.heartbeat_interval = Duration::from_millis(100);
        config.sequence_start = u16::MAX;
        config.scripted_close = Some(ScriptedClose {
            stage: GatewayCloseStage::OnResume,
            close: VoiceClose {
                code: 4006,
                reason: "resume buffer unavailable".to_owned(),
            },
        });
        let gateway = TestGateway::start(config).await.expect("gateway starts");
        let oto = test_oto(&gateway, ResourceLimits::default());
        let connection = oto
            .connect(voice_info(&gateway, "session-replay", "token-a"))
            .await
            .expect("gateway connects");
        gateway
            .try_dispatch_json(240, json!({"wrapped": true}), true)
            .expect("wrapped dispatch queues");
        eventually(|| connection.state().stats().unknown_opcodes() >= 1).await;
        gateway
            .try_close(VoiceClose {
                code: 4015,
                reason: "voice server crashed".to_owned(),
            })
            .expect("resumable close queues");

        eventually(|| {
            let records = gateway.records();
            let resumes = records
                .iter()
                .filter(|record| matches!(record, GatewayRecord::Resume { seq_ack: 1 }))
                .count();
            let identifies = records
                .iter()
                .filter(|record| matches!(record, GatewayRecord::Identify(_)))
                .count();
            resumes >= 1 && identifies >= 2
        })
        .await;
        assert_eq!(connection.state().generation().get(), 1);

        connection.shutdown().await.expect("shutdown succeeds");
        gateway.shutdown().await.expect("gateway shuts down");
    }

    #[tokio::test]
    async fn buffered_resume_replays_messages_after_last_ack_without_new_generation() {
        let mut config = FakeVoiceGatewayConfig::local();
        config.heartbeat_interval = Duration::from_millis(100);
        let gateway = TestGateway::start(config).await.expect("gateway starts");
        let oto = test_oto(&gateway, ResourceLimits::default());
        let connection = oto
            .connect(voice_info(&gateway, "session-buffer", "token"))
            .await
            .expect("gateway connects");
        gateway
            .try_buffer_json(251, json!({"buffered": true}))
            .expect("undelivered numbered message is buffered");
        gateway
            .try_close(VoiceClose {
                code: 4015,
                reason: "interruption".to_owned(),
            })
            .expect("resumable close queues");

        eventually(|| {
            let state = connection.state();
            state.stats().resume_successes() == 1 && state.stats().unknown_opcodes() >= 1
        })
        .await;
        assert_eq!(connection.state().generation().get(), 1);
        assert!(
            gateway
                .records()
                .iter()
                .any(|record| { matches!(record, GatewayRecord::Resume { seq_ack: 1 }) })
        );

        connection.shutdown().await.expect("shutdown succeeds");
        gateway.shutdown().await.expect("gateway shuts down");
    }

    #[tokio::test]
    async fn terminal_close_is_typed_at_every_gateway_handshake_stage() {
        for stage in [
            GatewayCloseStage::BeforeHello,
            GatewayCloseStage::AfterHello,
            GatewayCloseStage::AfterIdentify,
        ] {
            let mut config = FakeVoiceGatewayConfig::local();
            config.scripted_close = Some(ScriptedClose {
                stage,
                close: VoiceClose {
                    code: 4004,
                    reason: "bad token".to_owned(),
                },
            });
            let gateway = TestGateway::start(config).await.expect("gateway starts");
            let oto = test_oto(&gateway, ResourceLimits::default());
            let error = oto
                .connect(voice_info(&gateway, "session-close", "invalid-token"))
                .await
                .expect_err("terminal close rejects connect");
            assert_eq!(
                error.kind(),
                ErrorKind::CredentialsRejected,
                "stage {stage:?}"
            );
            assert_eq!(error.safe_code(), Some(4004));
            gateway.shutdown().await.expect("gateway shuts down");
        }

        let mut config = FakeVoiceGatewayConfig::local();
        config.scripted_close = Some(ScriptedClose {
            stage: GatewayCloseStage::AfterReady,
            close: VoiceClose {
                code: 4004,
                reason: "bad token".to_owned(),
            },
        });
        let gateway = TestGateway::start(config).await.expect("gateway starts");
        let oto = test_oto(&gateway, ResourceLimits::default());
        let error = oto
            .connect(voice_info(&gateway, "session-ready-close", "invalid-token"))
            .await
            .expect_err("connect waits for transport establishment after Ready");
        assert_eq!(error.kind(), ErrorKind::CredentialsRejected);
        assert_eq!(error.safe_code(), Some(4004));
        gateway.shutdown().await.expect("gateway shuts down");
    }

    #[tokio::test]
    async fn replacement_storm_rejects_stale_generation_even_on_same_endpoint_new_token() {
        let gateway = TestGateway::start(FakeVoiceGatewayConfig::local())
            .await
            .expect("gateway starts");
        let oto = test_oto(&gateway, ResourceLimits::default());
        let connection = oto
            .connect(voice_info(&gateway, "session-0", "token-0"))
            .await
            .expect("gateway connects");

        let mut tasks = Vec::new();
        for index in 1..=16_u64 {
            let connection = connection.clone();
            let info = voice_info(
                &gateway,
                &format!("session-{index}"),
                &format!("token-{index}"),
            );
            tasks.push(tokio::spawn(async move {
                connection.replace_voice_info(info).await
            }));
        }
        let mut generations = Vec::new();
        for task in tasks {
            generations.push(
                task.await
                    .expect("replacement task joins")
                    .expect("replacement accepted"),
            );
        }
        generations.sort();
        generations.dedup();
        assert_eq!(generations.len(), 16);
        assert_eq!(connection.state().generation().get(), 17);
        eventually(|| {
            gateway.records().iter().any(|record| {
                matches!(record, GatewayRecord::Identify(data)
                    if data.get("session_id").and_then(Value::as_str) == Some("session-16"))
            })
        })
        .await;
        assert_eq!(connection.state().generation().get(), 17);

        connection.shutdown().await.expect("shutdown succeeds");
        gateway.shutdown().await.expect("gateway shuts down");
    }

    #[tokio::test]
    async fn stale_slow_endpoint_completion_cannot_overwrite_new_generation() {
        let tls = TestTls::generate().expect("shared TLS material generates");
        let initial = TestGateway::start_with_tls(FakeVoiceGatewayConfig::local(), tls.clone())
            .await
            .expect("initial gateway starts");
        let mut slow_config = FakeVoiceGatewayConfig::local();
        slow_config.hello_delay = Duration::from_millis(150);
        let slow = TestGateway::start_with_tls(slow_config, tls.clone())
            .await
            .expect("slow gateway starts");
        let fast = TestGateway::start_with_tls(FakeVoiceGatewayConfig::local(), tls)
            .await
            .expect("fast gateway starts");
        let oto = test_oto(&initial, ResourceLimits::default());
        let connection = oto
            .connect(voice_info(&initial, "initial", "token-1"))
            .await
            .expect("initial gateway connects");

        assert_eq!(
            connection
                .replace_voice_info(voice_info(&slow, "slow", "token-2"))
                .await
                .expect("slow replacement accepted")
                .get(),
            2
        );
        sleep(Duration::from_millis(15)).await;
        assert_eq!(
            connection
                .replace_voice_info(voice_info(&fast, "fast", "token-3"))
                .await
                .expect("newer replacement accepted")
                .get(),
            3
        );
        eventually(|| {
            fast.records().iter().any(|record| {
                matches!(record, GatewayRecord::Identify(data)
                    if data.get("session_id").and_then(Value::as_str) == Some("fast"))
            })
        })
        .await;
        sleep(Duration::from_millis(175)).await;
        assert_eq!(connection.state().generation().get(), 3);
        assert_eq!(
            connection.state().phase(),
            ConnectionPhase::EstablishingDave
        );
        assert!(
            !slow
                .records()
                .iter()
                .any(|record| matches!(record, GatewayRecord::Identify(_)))
        );

        connection.shutdown().await.expect("shutdown succeeds");
        initial
            .shutdown()
            .await
            .expect("initial gateway shuts down");
        slow.shutdown().await.expect("slow gateway shuts down");
        fast.shutdown().await.expect("fast gateway shuts down");
    }

    #[tokio::test]
    async fn needs_fresh_voice_info_waits_without_spinning_and_accepts_replacement() {
        let gateway = TestGateway::start(FakeVoiceGatewayConfig::local())
            .await
            .expect("gateway starts");
        let oto = test_oto(&gateway, ResourceLimits::default());
        let connection = oto
            .connect(voice_info(&gateway, "session-old", "token-old"))
            .await
            .expect("gateway connects");
        gateway
            .try_close(VoiceClose {
                code: 4014,
                reason: "disconnected".to_owned(),
            })
            .expect("fresh-info close queues");
        eventually(|| connection.state().phase() == ConnectionPhase::NeedsFreshVoiceInfo).await;
        let ping_error = connection
            .ping()
            .await
            .expect_err("ping reports caller action");
        assert_eq!(ping_error.kind(), ErrorKind::NeedsFreshVoiceInfo);

        let generation = connection
            .replace_voice_info(voice_info(&gateway, "session-new", "token-new"))
            .await
            .expect("complete fresh info is accepted");
        assert_eq!(generation.get(), 2);
        eventually(|| {
            connection.state().generation() == generation
                && connection.state().phase() == ConnectionPhase::EstablishingDave
        })
        .await;

        connection.shutdown().await.expect("shutdown succeeds");
        gateway.shutdown().await.expect("gateway shuts down");
    }

    #[tokio::test]
    async fn text_and_binary_message_limits_accept_edge_and_reject_one_byte_over() {
        let text_gateway = TestGateway::start(FakeVoiceGatewayConfig::local())
            .await
            .expect("text gateway starts");
        let text_limit = 256;
        let oto = test_oto(
            &text_gateway,
            ResourceLimits::default().with_gateway_text_bytes(text_limit),
        );
        let text_connection = oto
            .connect(voice_info(&text_gateway, "text-limits", "token"))
            .await
            .expect("text gateway connects");
        let exact_padding = (0..text_limit)
            .find(|length| {
                serde_json::to_string(&json!({
                    "op": 250,
                    "d": {"pad": "x".repeat(*length)},
                    "seq": 1,
                }))
                .expect("test JSON serializes")
                .len()
                    == text_limit
            })
            .expect("an exact text payload exists");
        text_gateway
            .try_dispatch_json(250, json!({"pad": "x".repeat(exact_padding)}), true)
            .expect("edge text dispatch queues");
        eventually(|| text_connection.state().stats().unknown_opcodes() >= 1).await;
        text_gateway
            .try_dispatch_json(250, json!({"pad": "x".repeat(exact_padding + 1)}), true)
            .expect("one-over text dispatch queues");
        eventually(|| text_connection.state().phase() == ConnectionPhase::Failed).await;
        assert_eq!(
            text_connection
                .state()
                .failure()
                .expect("text failure persists")
                .kind(),
            ErrorKind::ResourceLimit
        );
        text_gateway
            .shutdown()
            .await
            .expect("text gateway shuts down");

        let binary_gateway = TestGateway::start(FakeVoiceGatewayConfig::local())
            .await
            .expect("binary gateway starts");
        let limits = ResourceLimits::default()
            .with_gateway_binary_bytes(8)
            .with_dave_binary_body_bytes(8);
        let oto = test_oto(&binary_gateway, limits);
        let binary_connection = oto
            .connect(voice_info(&binary_gateway, "binary-limits", "token"))
            .await
            .expect("binary gateway connects");
        binary_gateway
            .try_dispatch_binary(250, vec![0; 5])
            .expect("edge binary dispatch queues");
        eventually(|| binary_connection.state().stats().unknown_opcodes() >= 1).await;
        binary_gateway
            .try_dispatch_binary(250, vec![0; 6])
            .expect("one-over binary dispatch queues");
        eventually(|| binary_connection.state().phase() == ConnectionPhase::Failed).await;
        assert_eq!(
            binary_connection
                .state()
                .failure()
                .expect("binary failure persists")
                .kind(),
            ErrorKind::ResourceLimit
        );
        binary_gateway
            .shutdown()
            .await
            .expect("binary gateway shuts down");
    }

    #[tokio::test]
    async fn command_saturation_and_slow_observer_never_hide_durable_final_state() {
        let gateway = TestGateway::start(FakeVoiceGatewayConfig::local())
            .await
            .expect("gateway starts");
        let limits = ResourceLimits::default()
            .with_gateway_command_capacity(1)
            .with_event_capacity(2)
            .with_event_subscriber_capacity(1);
        let oto = test_oto(&gateway, limits);
        let connection = oto
            .connect(voice_info(&gateway, "session-slow", "token-0"))
            .await
            .expect("gateway connects");
        let mut slow = connection
            .subscribe_events()
            .expect("first subscriber admitted");
        assert_eq!(
            connection
                .subscribe_events()
                .expect_err("second subscriber is bounded")
                .kind(),
            ErrorKind::ResourceLimit
        );

        let barrier = Arc::new(tokio::sync::Barrier::new(17));
        let mut commands = Vec::new();
        for index in 1..=16_u64 {
            let connection = connection.clone();
            let barrier = barrier.clone();
            let info = voice_info(
                &gateway,
                &format!("slow-session-{index}"),
                &format!("slow-token-{index}"),
            );
            commands.push(tokio::spawn(async move {
                barrier.wait().await;
                connection.replace_voice_info(info).await
            }));
        }
        barrier.wait().await;
        let mut accepted = Vec::new();
        for command in commands {
            accepted.push(
                command
                    .await
                    .expect("saturated command joins")
                    .expect("bounded enqueue waits or returns explicit success"),
            );
        }
        accepted.sort();
        accepted.dedup();
        assert_eq!(accepted.len(), 16);
        assert_eq!(connection.state().generation().get(), 17);
        assert!(matches!(
            slow.recv().await,
            Err(EventReceiveError::Lagged { skipped }) if skipped > 0
        ));
        assert!(connection.state().stats().event_lagged() > 0);

        let final_state = connection
            .shutdown()
            .await
            .expect("shutdown cannot be stranded");
        assert_eq!(final_state.phase(), ConnectionPhase::Closed);
        assert_eq!(final_state.generation().get(), 17);
        gateway.shutdown().await.expect("gateway shuts down");
    }

    #[tokio::test]
    async fn cancelled_connect_and_last_handle_drop_leave_no_live_connection_owner() {
        let mut slow_config = FakeVoiceGatewayConfig::local();
        slow_config.hello_delay = Duration::from_secs(5);
        let slow = TestGateway::start(slow_config)
            .await
            .expect("slow gateway starts");
        let oto = test_oto(&slow, ResourceLimits::default());
        let cancelled = timeout(
            Duration::from_millis(20),
            oto.connect(voice_info(&slow, "cancelled", "token")),
        )
        .await;
        assert!(cancelled.is_err(), "connect remains pending before Hello");
        timeout(Duration::from_millis(200), slow.shutdown())
            .await
            .expect("cancelled connect leaves no blocking owner")
            .expect("slow gateway shuts down");

        let gateway = TestGateway::start(FakeVoiceGatewayConfig::local())
            .await
            .expect("gateway starts");
        let oto = test_oto(&gateway, ResourceLimits::default());
        let first = oto
            .connect(voice_info(&gateway, "drop-first", "token-1"))
            .await
            .expect("first connection succeeds");
        drop(first);
        let second = timeout(
            Duration::from_secs(1),
            oto.connect(voice_info(&gateway, "drop-second", "token-2")),
        )
        .await
        .expect("peer accepts after last handle drop")
        .expect("second connection succeeds");
        second
            .shutdown()
            .await
            .expect("second connection shuts down");
        gateway.shutdown().await.expect("gateway shuts down");
    }

    #[test]
    fn resource_and_voice_info_bounds_fail_before_any_task_starts() {
        let error = Oto::builder()
            .resource_limits(ResourceLimits::default().with_gateway_command_capacity(0))
            .build()
            .expect_err("zero capacity is rejected");
        assert_eq!(error.kind(), ErrorKind::InvalidConfiguration);

        let error = Oto::builder()
            .resource_limits(
                ResourceLimits::default().with_udp_datagram_bytes(usize::from(u16::MAX) + 1),
            )
            .build()
            .expect_err("impossible UDP datagram bound is rejected");
        assert_eq!(error.kind(), ErrorKind::InvalidConfiguration);

        let token = VoiceToken::new("never-print-this");
        assert!(!format!("{token:?}").contains("never-print-this"));

        for endpoint in [
            "ws://voice.example.test",
            "user@voice.example.test",
            "voice.example.test/path",
            "voice.example.test?token=secret",
            "voice.example.test#fragment",
        ] {
            let info =
                VoiceConnectInfo::new(1, 2, 3, "session", endpoint, VoiceToken::new("token"));
            assert_eq!(
                ValidatedInfo::new(info, Operation::Connect, None)
                    .expect_err("unsafe endpoint shape is rejected")
                    .kind(),
                ErrorKind::InvalidVoiceInfo
            );
        }
    }

    #[test]
    fn current_voice_close_policy_is_data_driven_and_typed() {
        for code in [4001, 4002, 4003, 4005, 4006, 4009] {
            assert!(matches!(
                close_outcome(code, ConnectionGeneration::FIRST, Attempt::Identify),
                SessionOutcome::FreshIdentify
            ));
        }
        assert!(matches!(
            close_outcome(4015, ConnectionGeneration::FIRST, Attempt::Identify),
            SessionOutcome::Resume
        ));
        for code in [4011, 4014, 4021, 4022] {
            match close_outcome(code, ConnectionGeneration::FIRST, Attempt::Identify) {
                SessionOutcome::NeedsFresh(error) => {
                    assert_eq!(error.kind(), ErrorKind::NeedsFreshVoiceInfo);
                    assert_eq!(error.safe_code(), Some(u32::from(code)));
                }
                _ => panic!("code {code} must await fresh voice information"),
            }
        }
        for (code, kind) in [
            (4004, ErrorKind::CredentialsRejected),
            (4012, ErrorKind::UnsupportedTransport),
            (4016, ErrorKind::UnsupportedTransport),
            (4017, ErrorKind::DaveRequired),
            (4020, ErrorKind::GatewayProtocol),
        ] {
            match close_outcome(code, ConnectionGeneration::FIRST, Attempt::Identify) {
                SessionOutcome::Fatal(error) => {
                    assert_eq!(error.kind(), kind);
                    assert_eq!(error.safe_code(), Some(u32::from(code)));
                }
                _ => panic!("code {code} must be terminal"),
            }
        }
    }
}
