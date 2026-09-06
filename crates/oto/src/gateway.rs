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
#[cfg(any(test, feature = "testkit"))]
use tokio_tungstenite::Connector;
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_tls_with_config};

use crate::audio::{InstalledTransport, TransportValidity};
use crate::config::Config;
use crate::connection::StateStore;
use crate::dave::{self, Control as DaveControl, Outbound as DaveOutbound};
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
    AttachAudio {
        audio_id: u64,
        updates: mpsc::Sender<InstalledTransport>,
        reply: oneshot::Sender<Result<InstalledTransport, Error>>,
    },
    Speaking {
        audio_id: u64,
        generation: ConnectionGeneration,
        speaking: bool,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    RenewTransport {
        audio_id: u64,
        generation: ConnectionGeneration,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    DetachAudio {
        audio_id: u64,
        transport: InstalledTransport,
        reply: oneshot::Sender<()>,
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
    dave_protocol_version: u16,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DaveBinaryEnvelopeError {
    Malformed,
    BodyTooLarge,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DaveRosterError {
    Malformed,
    TooManyMembers,
}

fn dave_binary_min_body_bytes(opcode: u8) -> Option<usize> {
    match opcode {
        25 => Some(4),
        27 => Some(2),
        29 | 30 => Some(3),
        _ => None,
    }
}

fn decode_dave_binary_envelope(
    bytes: &[u8],
    max_body_bytes: usize,
) -> Result<(u16, Option<DaveControl>), DaveBinaryEnvelopeError> {
    if bytes.len() < 3 {
        return Err(DaveBinaryEnvelopeError::Malformed);
    }
    let sequence = u16::from_be_bytes([bytes[0], bytes[1]]);
    let body = &bytes[3..];
    if body.len() > max_body_bytes {
        return Err(DaveBinaryEnvelopeError::BodyTooLarge);
    }
    if dave_binary_min_body_bytes(bytes[2]).is_some_and(|minimum| body.len() < minimum) {
        return Err(DaveBinaryEnvelopeError::Malformed);
    }
    if bytes[2] == 27 && !matches!(body[0], 0 | 1) {
        return Err(DaveBinaryEnvelopeError::Malformed);
    }
    let control = match bytes[2] {
        25 => Some(DaveControl::ExternalSender(body.to_vec())),
        27 => Some(DaveControl::Proposals(body.to_vec())),
        29 => Some(DaveControl::Commit(body.to_vec())),
        30 => Some(DaveControl::Welcome(body.to_vec())),
        _ => None,
    };
    Ok((sequence, control))
}

fn decode_dave_json_control(opcode: u64, data: &Value) -> Option<DaveControl> {
    match opcode {
        21 => Some(DaveControl::PrepareTransition {
            protocol_version: u16_field(data, "protocol_version")?,
            id: u16_field(data, "transition_id")?,
        }),
        22 => Some(DaveControl::ExecuteTransition {
            id: u16_field(data, "transition_id")?,
        }),
        24 => Some(DaveControl::PrepareEpoch {
            protocol_version: u16_field(data, "protocol_version")?,
            epoch: data.get("epoch")?.as_u64()?,
        }),
        _ => None,
    }
}

fn decode_dave_roster(data: &Value, max_members: usize) -> Result<Vec<u64>, DaveRosterError> {
    let users = data
        .get("user_ids")
        .and_then(Value::as_array)
        .ok_or(DaveRosterError::Malformed)?;
    if users.len() > max_members {
        return Err(DaveRosterError::TooManyMembers);
    }
    users
        .iter()
        .map(|user| {
            user.as_str()
                .and_then(parse_dave_user_id)
                .ok_or(DaveRosterError::Malformed)
        })
        .collect()
}

fn parse_dave_user_id(user: &str) -> Option<u64> {
    user.parse::<u64>().ok().filter(|user| *user != 0)
}

#[cfg(fuzzing)]
pub(crate) fn fuzz_dave_binary_envelope(input: &[u8]) {
    let exact_body = input.len().saturating_sub(3);
    for maximum in [0, exact_body.saturating_sub(1), exact_body, 1_048_576] {
        match decode_dave_binary_envelope(input, maximum) {
            Err(DaveBinaryEnvelopeError::Malformed) => {
                assert!(
                    input.len() < 3
                        || (exact_body <= maximum
                            && (dave_binary_min_body_bytes(input[2])
                                .is_some_and(|minimum| exact_body < minimum)
                                || (input[2] == 27
                                    && exact_body >= 2
                                    && !matches!(input[3], 0 | 1))))
                );
            }
            Err(DaveBinaryEnvelopeError::BodyTooLarge) => {
                assert!(input.len() >= 3 && exact_body > maximum);
            }
            Ok((sequence, control)) => {
                assert!(input.len() >= 3 && exact_body <= maximum);
                assert!(
                    dave_binary_min_body_bytes(input[2])
                        .is_none_or(|minimum| exact_body >= minimum)
                );
                assert!(input[2] != 27 || matches!(input[3], 0 | 1));
                assert_eq!(sequence, u16::from_be_bytes([input[0], input[1]]));
                assert_eq!(control.is_some(), matches!(input[2], 25 | 27 | 29 | 30));
            }
        }
    }
}

#[cfg(fuzzing)]
pub(crate) fn fuzz_gateway_json_dispatch(input: &[u8]) {
    if input.len() > 1_280_000 {
        return;
    }
    let Ok(envelope) = serde_json::from_slice::<InboundEnvelope>(input) else {
        return;
    };
    if let Some(sequence) = envelope.seq {
        let _ = sequence.is_i64() || sequence.is_u64();
    }
    match envelope.op {
        2 => {
            let _ = parse_ready(envelope.d, ConnectionGeneration::FIRST);
        }
        4 => {
            let _ = serde_json::from_value::<SessionDescriptionData>(envelope.d);
        }
        8 => {
            let _ = envelope.d.get("heartbeat_interval").and_then(Value::as_u64);
        }
        11 => {
            let _ = decode_dave_roster(&envelope.d, 4_096);
        }
        13 => {
            let _ = envelope
                .d
                .get("user_id")
                .and_then(Value::as_str)
                .and_then(parse_dave_user_id);
        }
        21 | 22 | 24 => {
            let _ = decode_dave_json_control(envelope.op, &envelope.d);
        }
        _ => {}
    }
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
    validity: Arc<TransportValidity>,
    ssrc: u32,
    selected_mode: TransportMode,
    encoder: Option<TransportEncoder>,
    dave_protocol_version: u16,
    dave: Option<dave::Handle>,
    session_ready: bool,
}

impl UdpTransport {
    fn is_ready(&self) -> bool {
        self.session_ready
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
    let mut audio_attachment = None;
    let mut dave = None;
    let mut dave_roster = Vec::new();

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

        #[cfg(any(test, feature = "testkit"))]
        let connector = config
            .tls_config
            .as_ref()
            .map(|config| Connector::Rustls(config.clone()));
        #[cfg(not(any(test, feature = "testkit")))]
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
                            if let Some(replacement) = accept_replacement(
                                &mut store,
                                &mut transport,
                                replacement,
                                reply,
                            ).await {
                                info = replacement;
                                latest_sequence = None;
                                transport = None;
                                dave = None;
                                dave_roster.clear();
                                reconnect_attempts = 0;
                                attempt = Attempt::Identify;
                                continue 'control;
                            }
                        }
                        Some(Command::Ping { reply }) => {
                            let _ = reply.send(Err(retrying_error(Operation::Ping, store.generation())));
                        }
                        Some(Command::AttachAudio { reply, .. }) => {
                            let _ = reply.send(Err(retrying_error(
                                Operation::StartAudio,
                                store.generation(),
                            )));
                        }
                        Some(Command::Speaking { reply, .. })
                        | Some(Command::RenewTransport { reply, .. }) => {
                            let _ = reply.send(Err(retrying_error(
                                Operation::StartAudio,
                                store.generation(),
                            )));
                        }
                        Some(Command::DetachAudio { audio_id, transport: returned, reply }) => {
                            detach_audio(
                                &mut transport,
                                &mut audio_attachment,
                                audio_id,
                                returned,
                                store.generation(),
                            );
                            let _ = reply.send(());
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
                            match backoff_or_command(reconnect_attempts, BackoffContext {
                                info: &mut info,
                                latest_sequence: &mut latest_sequence,
                                transport: &mut transport,
                                audio_attachment: &mut audio_attachment,
                                commands: &mut commands,
                                shutdown: &mut shutdown,
                                store: &mut store,
                            }).await {
                                BackoffOutcome::Elapsed => {}
                                BackoffOutcome::Replaced => {
                                    reconnect_attempts = 0;
                                    attempt = Attempt::Identify;
                                    transport = None;
                                    dave = None;
                                    dave_roster.clear();
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
            &mut audio_attachment,
            &mut dave,
            &mut dave_roster,
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
                    dave = None;
                    dave_roster.clear();
                    Attempt::Identify
                };
                match backoff_or_command(
                    reconnect_attempts,
                    BackoffContext {
                        info: &mut info,
                        latest_sequence: &mut latest_sequence,
                        transport: &mut transport,
                        audio_attachment: &mut audio_attachment,
                        commands: &mut commands,
                        shutdown: &mut shutdown,
                        store: &mut store,
                    },
                )
                .await
                {
                    BackoffOutcome::Elapsed => {}
                    BackoffOutcome::Replaced => {
                        reconnect_attempts = 0;
                        attempt = Attempt::Identify;
                        transport = None;
                        dave = None;
                        dave_roster.clear();
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
                dave = None;
                dave_roster.clear();
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
                    BackoffContext {
                        info: &mut info,
                        latest_sequence: &mut latest_sequence,
                        transport: &mut transport,
                        audio_attachment: &mut audio_attachment,
                        commands: &mut commands,
                        shutdown: &mut shutdown,
                        store: &mut store,
                    },
                )
                .await
                {
                    BackoffOutcome::Elapsed => {}
                    BackoffOutcome::Replaced => {
                        reconnect_attempts = 0;
                        transport = None;
                        dave = None;
                        dave_roster.clear();
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
                dave = None;
                dave_roster.clear();
                reconnect_attempts = 0;
                attempt = Attempt::Identify;
            }
            SessionOutcome::NeedsFresh(error) => {
                store.fail(&error, ConnectionPhase::NeedsFreshVoiceInfo);
                send_initial_error(&mut initial, error.clone());
                match wait_for_replacement(&mut commands, &mut shutdown, &mut store, &mut transport)
                    .await
                {
                    Some(replacement) => {
                        info = replacement;
                        latest_sequence = None;
                        transport = None;
                        dave = None;
                        dave_roster.clear();
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
    audio_attachment: &mut Option<(u64, mpsc::Sender<InstalledTransport>)>,
    dave: &mut Option<dave::Handle>,
    dave_roster: &mut Vec<u64>,
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
                    fail_pending_pings_shutdown(&mut pending_pings, store.generation());
                    return SessionOutcome::Shutdown;
                }
            }
            command = commands.recv() => {
                match command {
                    Some(Command::Replace { info: replacement, reply }) => {
                        if let Some(replacement) = accept_replacement(
                            store,
                            transport,
                            replacement,
                            reply,
                        ).await {
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
                        if pending_pings.len() >= config.limits.gateway_command_capacity() {
                            let _ = reply.send(Err(Error::new(
                                ErrorKind::Overloaded,
                                Operation::Ping,
                                Some(store.generation()),
                                RetryDisposition::Fatal,
                                None,
                                "pending gateway ping waiter limit reached",
                            )));
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
                    Some(Command::AttachAudio { audio_id, updates, reply }) => {
                        attach_audio(
                            transport,
                            audio_attachment,
                            audio_id,
                            updates,
                            reply,
                            store.generation(),
                        );
                    }
                    Some(Command::Speaking {
                        audio_id,
                        generation,
                        speaking,
                        reply,
                    }) => {
                        if generation != store.generation()
                            || audio_attachment.as_ref().map(|active| active.0) != Some(audio_id)
                        {
                            let _ = reply.send(Err(superseded_audio_error(store.generation())));
                            continue;
                        }
                        let Some(active) = transport.as_ref().filter(|active| active.is_ready()) else {
                            let _ = reply.send(Err(retrying_error(
                                Operation::StartAudio,
                                store.generation(),
                            )));
                            continue;
                        };
                        let payload = speaking_payload(active.ssrc, speaking);
                        match timed_send(&mut websocket, payload).await {
                            Ok(()) => { let _ = reply.send(Ok(())); }
                            Err(error) => {
                                let error = error_for_generation(
                                    error, Operation::StartAudio, store.generation()
                                );
                                let _ = reply.send(Err(error));
                                return SessionOutcome::Resume;
                            }
                        }
                    }
                    Some(Command::RenewTransport { audio_id, generation, reply }) => {
                        if generation != store.generation()
                            || audio_attachment.as_ref().map(|active| active.0) != Some(audio_id)
                        {
                            let _ = reply.send(Err(superseded_audio_error(store.generation())));
                            continue;
                        }
                        let _ = reply.send(Ok(()));
                        return SessionOutcome::FreshIdentify;
                    }
                    Some(Command::DetachAudio { audio_id, transport: returned, reply }) => {
                        detach_audio(
                            transport,
                            audio_attachment,
                            audio_id,
                            returned,
                            store.generation(),
                        );
                        let _ = reply.send(());
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
                    validity: TransportValidity::new(),
                    ssrc: completion.ssrc,
                    selected_mode: completion.mode,
                    encoder: None,
                    dave_protocol_version: 0,
                    dave: None,
                    session_ready: false,
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
                                if active.session_ready {
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
                                    config.limits.transport_payload_bytes(),
                                    config.limits.udp_datagram_bytes(),
                                ) {
                                    Ok(encoder) => encoder,
                                    Err(_) => return SessionOutcome::Fatal(transport_crypto_error(
                                        store.generation()
                                    )),
                                };
                                #[cfg(test)]
                                let encoder = {
                                    let mut encoder = encoder;
                                    if let Some(nonce) = config
                                        .transport_nonce_start
                                        .lock()
                                        .expect("test transport nonce mutex poisoned")
                                        .take()
                                    {
                                        encoder.set_test_nonce_start(nonce);
                                    }
                                    encoder
                                };
                                debug_assert_eq!(encoder.mode(), mode);
                                if description.dave_protocol_version > dave::MAX_PROTOCOL_VERSION {
                                    return SessionOutcome::Fatal(dave_unsupported_error(
                                        store.generation(),
                                    ));
                                }
                                if description.dave_protocol_version != 0 && dave.is_none() {
                                    #[cfg(test)]
                                    let spawned = if config.ready_dave_fixture.load(
                                        std::sync::atomic::Ordering::Acquire,
                                    ) {
                                        Ok(dave::Handle::spawn_ready_fixture(
                                            config.limits.gateway_command_capacity(),
                                        ))
                                    } else {
                                        dave::Handle::spawn(
                                            info.info.user_id(),
                                            info.info.channel_id(),
                                            config.limits.gateway_command_capacity(),
                                        )
                                    };
                                    #[cfg(not(test))]
                                    let spawned = dave::Handle::spawn(
                                        info.info.user_id(),
                                        info.info.channel_id(),
                                        config.limits.gateway_command_capacity(),
                                    );
                                    *dave = match spawned {
                                        Ok(handle) => Some(handle),
                                        Err(source) => return SessionOutcome::Fatal(
                                            dave_error(store.generation()).with_source(source)
                                        ),
                                    };
                                    if !dave_roster.is_empty()
                                        && let Err(error) = run_dave_control(
                                            dave.as_ref().expect("DAVE handle was just installed"),
                                            config,
                                            DaveControl::Roster(dave_roster.clone()),
                                            &mut websocket,
                                            store.generation(),
                                        )
                                        .await
                                    {
                                        return SessionOutcome::Fatal(error);
                                    }
                                }
                                if description.dave_protocol_version == 0 {
                                    *dave = None;
                                }
                                active.dave_protocol_version = description.dave_protocol_version;
                                active.dave = if description.dave_protocol_version != 0 {
                                    dave.clone()
                                } else {
                                    None
                                };
                                active.session_ready = true;
                                let installed = InstalledTransport {
                                    generation: store.generation(),
                                    socket: active.socket.clone(),
                                    encoder,
                                    validity: active.validity.clone(),
                                    dave_protocol_version: description.dave_protocol_version,
                                    dave: active.dave.clone(),
                                    dave_media: active
                                        .dave
                                        .as_ref()
                                        .map(dave::Handle::media_encryptor),
                                };
                                if let Some((_, updates)) = audio_attachment.as_ref() {
                                    match deliver_transport(updates, installed).await {
                                        Ok(()) => active.encoder = None,
                                        Err(returned) => {
                                            active.encoder = Some(returned.encoder);
                                            *audio_attachment = None;
                                        }
                                    }
                                } else {
                                    active.encoder = Some(installed.encoder);
                                }
                                let phase = if description.dave_protocol_version == 0 {
                                    ConnectionPhase::Connected
                                } else {
                                    ConnectionPhase::EstablishingDave
                                };
                                store.phase_to(phase);
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
                                let phase = transport.as_ref().map_or(
                                    ConnectionPhase::EstablishingTransport,
                                    |transport| {
                                        if !transport.is_ready() {
                                            ConnectionPhase::EstablishingTransport
                                        } else if transport.dave_protocol_version == 0
                                            || transport
                                                .dave
                                                .as_ref()
                                                .is_some_and(|dave| dave.snapshot().ready)
                                        {
                                            ConnectionPhase::Connected
                                        } else {
                                            ConnectionPhase::EstablishingDave
                                        }
                                    },
                                );
                                store.resume_succeeded(phase);
                            }
                            11 => {
                                let users = match decode_dave_roster(
                                    &data,
                                    config.limits.dave_roster_members(),
                                ) {
                                    Ok(users) => users,
                                    Err(DaveRosterError::Malformed) => {
                                        return SessionOutcome::Fatal(protocol_error(store.generation()));
                                    }
                                    Err(DaveRosterError::TooManyMembers) => {
                                        return SessionOutcome::Fatal(resource_error(store.generation()));
                                    }
                                };
                                *dave_roster = users.clone();
                                if let Some(dave) = dave.as_ref()
                                    && let Err(error) = run_dave_control(
                                        dave, config, DaveControl::Roster(users),
                                        &mut websocket, store.generation(),
                                    ).await
                                {
                                    return SessionOutcome::Fatal(error);
                                }
                            }
                            13 => {
                                let Some(user) = data.get("user_id").and_then(Value::as_str)
                                    .and_then(parse_dave_user_id) else {
                                    return SessionOutcome::Fatal(protocol_error(store.generation()));
                                };
                                dave_roster.retain(|candidate| *candidate != user);
                                if let Some(dave) = dave.as_ref()
                                    && let Err(error) = run_dave_control(
                                        dave, config, DaveControl::MemberDisconnected(user),
                                        &mut websocket, store.generation(),
                                    ).await
                                {
                                    return SessionOutcome::Fatal(error);
                                }
                            }
                            21 | 22 | 24 => {
                                let Some(control) = decode_dave_json_control(envelope.op, &data) else {
                                    return SessionOutcome::Fatal(protocol_error(store.generation()));
                                };
                                let Some(dave) = dave.as_ref() else {
                                    return SessionOutcome::Fatal(protocol_error(store.generation()));
                                };
                                if let Err(error) = run_dave_control(
                                    dave, config, control, &mut websocket, store.generation(),
                                ).await {
                                    return SessionOutcome::Fatal(error);
                                }
                                // DAVE controls can move an established session back to
                                // a non-ready state (for example PrepareEpoch starts a
                                // fresh MLS epoch). Reflect that transition immediately so
                                // attached audio pauses before attempting media encryption;
                                // otherwise the sender would observe Connected, attempt to
                                // encrypt with an unready context, and fail instead of
                                // waiting for Execute Transition.
                                store.phase_to(if dave.snapshot().ready {
                                    ConnectionPhase::Connected
                                } else {
                                    ConnectionPhase::EstablishingDave
                                });
                            }
                            _ => store.unknown_opcode(),
                        }
                    }
                    Message::Binary(bytes) => {
                        if bytes.len() > config.limits.gateway_binary_bytes() {
                            return SessionOutcome::Fatal(resource_error(store.generation()));
                        }
                        let (sequence, control) = match decode_dave_binary_envelope(
                            &bytes,
                            config.limits.dave_binary_body_bytes(),
                        ) {
                            Ok(decoded) => decoded,
                            Err(DaveBinaryEnvelopeError::Malformed) => {
                                return SessionOutcome::Fatal(protocol_error(store.generation()));
                            }
                            Err(DaveBinaryEnvelopeError::BodyTooLarge) => {
                                return SessionOutcome::Fatal(resource_error(store.generation()));
                            }
                        };
                        *latest_sequence = Some(Number::from(sequence));
                        if let Some(control) = control {
                            let Some(dave) = dave.as_ref() else {
                                return SessionOutcome::Fatal(protocol_error(store.generation()));
                            };
                            if let Err(error) = run_dave_control(
                                dave, config, control, &mut websocket, store.generation(),
                            ).await {
                                return SessionOutcome::Fatal(error);
                            }
                            store.phase_to(if dave.snapshot().ready {
                                ConnectionPhase::Connected
                            } else {
                                ConnectionPhase::EstablishingDave
                            });
                        } else {
                            store.unknown_opcode();
                        }
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

fn speaking_payload(ssrc: u32, speaking: bool) -> Message {
    json_message(json!({
        "op": 5,
        "d": {
            "speaking": if speaking { 1 } else { 0 },
            "delay": 0,
            "ssrc": ssrc,
        }
    }))
}

fn u16_field(data: &Value, field: &str) -> Option<u16> {
    data.get(field)
        .and_then(Value::as_u64)
        .and_then(|value| u16::try_from(value).ok())
}

async fn run_dave_control(
    dave: &dave::Handle,
    config: &Config,
    control: DaveControl,
    websocket: &mut ClientWebSocket,
    generation: ConnectionGeneration,
) -> Result<(), Error> {
    let actions = dave
        .control(control)
        .await
        .map_err(|source| dave_error(generation).with_source(source))?;
    let messages =
        dave_outbound_messages(actions, config.limits.gateway_binary_bytes(), generation)?;
    for message in messages {
        timed_send(websocket, message)
            .await
            .map_err(|error| error.for_operation_generation(Operation::Connect, generation))?;
    }
    Ok(())
}

fn dave_outbound_messages(
    actions: Vec<DaveOutbound>,
    maximum_binary_bytes: usize,
    generation: ConnectionGeneration,
) -> Result<Vec<Message>, Error> {
    actions
        .into_iter()
        .map(|action| dave_outbound_message(action, maximum_binary_bytes, generation))
        .collect()
}

fn dave_outbound_message(
    action: DaveOutbound,
    maximum_binary_bytes: usize,
    generation: ConnectionGeneration,
) -> Result<Message, Error> {
    match action {
        DaveOutbound::Json { opcode, data } => Ok(json_message(json!({"op": opcode, "d": data}))),
        DaveOutbound::Binary(bytes) => {
            if bytes.is_empty() {
                return Err(dave_error(generation));
            }
            if bytes.len() > maximum_binary_bytes {
                return Err(resource_error(generation));
            }
            Ok(Message::Binary(bytes.into()))
        }
    }
}

fn attach_audio(
    transport: &mut Option<UdpTransport>,
    attachment: &mut Option<(u64, mpsc::Sender<InstalledTransport>)>,
    audio_id: u64,
    updates: mpsc::Sender<InstalledTransport>,
    reply: oneshot::Sender<Result<InstalledTransport, Error>>,
    generation: ConnectionGeneration,
) {
    if attachment
        .as_ref()
        .is_some_and(|(_, sender)| !sender.is_closed())
    {
        let _ = reply.send(Err(Error::new(
            ErrorKind::ResourceLimit,
            Operation::StartAudio,
            Some(generation),
            RetryDisposition::Fatal,
            None,
            "a paced audio sender is already attached",
        )));
        return;
    }
    *attachment = None;
    let Some(active) = transport.as_mut().filter(|active| active.is_ready()) else {
        let _ = reply.send(Err(retrying_error(Operation::StartAudio, generation)));
        return;
    };
    if active.dave_protocol_version != 0
        && !active
            .dave
            .as_ref()
            .is_some_and(|dave| dave.snapshot().ready)
    {
        let _ = reply.send(Err(Error::new(
            ErrorKind::DaveRequired,
            Operation::StartAudio,
            Some(generation),
            RetryDisposition::Fatal,
            None,
            "DAVE is required before participant media can be sent",
        )));
        return;
    }
    let Some(encoder) = active.encoder.take() else {
        let _ = reply.send(Err(Error::new(
            ErrorKind::ResourceLimit,
            Operation::StartAudio,
            Some(generation),
            RetryDisposition::Fatal,
            None,
            "the current transport encoder is already attached",
        )));
        return;
    };
    let installed = InstalledTransport {
        generation,
        socket: active.socket.clone(),
        encoder,
        validity: active.validity.clone(),
        dave_protocol_version: active.dave_protocol_version,
        dave: active.dave.clone(),
        dave_media: active.dave.as_ref().map(dave::Handle::media_encryptor),
    };
    match reply.send(Ok(installed)) {
        Ok(()) => *attachment = Some((audio_id, updates)),
        Err(Ok(returned)) => active.encoder = Some(returned.encoder),
        Err(Err(_)) => unreachable!("attach reply sends an installed transport"),
    }
}

fn detach_audio(
    transport: &mut Option<UdpTransport>,
    attachment: &mut Option<(u64, mpsc::Sender<InstalledTransport>)>,
    audio_id: u64,
    returned: InstalledTransport,
    generation: ConnectionGeneration,
) {
    if attachment.as_ref().map(|active| active.0) != Some(audio_id) {
        return;
    }
    *attachment = None;
    let Some(active) = transport.as_mut() else {
        return;
    };
    if returned.generation == generation
        && active.session_ready
        && active.encoder.is_none()
        && Arc::ptr_eq(&active.socket, &returned.socket)
    {
        active.encoder = Some(returned.encoder);
    }
}

async fn deliver_transport(
    updates: &mpsc::Sender<InstalledTransport>,
    installed: InstalledTransport,
) -> Result<(), InstalledTransport> {
    match timeout(WRITE_TIMEOUT, updates.reserve()).await {
        Ok(Ok(permit)) => {
            permit.send(installed);
            Ok(())
        }
        _ => Err(installed),
    }
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

async fn accept_replacement(
    store: &mut StateStore,
    transport: &mut Option<UdpTransport>,
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
    invalidate_transport(transport).await;
    store.replace_generation(generation);
    let _ = reply.send(Ok(generation));
    Some(info)
}

async fn invalidate_transport(transport: &mut Option<UdpTransport>) {
    if let Some(transport) = transport.as_ref() {
        transport.validity.invalidate().await;
    }
}

async fn wait_for_replacement(
    commands: &mut mpsc::Receiver<Command>,
    shutdown: &mut watch::Receiver<bool>,
    store: &mut StateStore,
    transport: &mut Option<UdpTransport>,
) -> Option<ValidatedInfo> {
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { return None; }
            }
            command = commands.recv() => match command? {
                Command::Replace { info, reply } => {
                    if let Some(info) = accept_replacement(
                        store,
                        transport,
                        info,
                        reply,
                    ).await {
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
                Command::AttachAudio { reply, .. } => {
                    let _ = reply.send(Err(Error::new(
                        ErrorKind::NeedsFreshVoiceInfo,
                        Operation::StartAudio,
                        Some(store.generation()),
                        RetryDisposition::NeedsFreshVoiceInfo,
                        None,
                        "fresh voice information is required before audio can start",
                    )));
                }
                Command::Speaking { reply, .. } | Command::RenewTransport { reply, .. } => {
                    let _ = reply.send(Err(Error::new(
                        ErrorKind::NeedsFreshVoiceInfo,
                        Operation::StartAudio,
                        Some(store.generation()),
                        RetryDisposition::NeedsFreshVoiceInfo,
                        None,
                        "fresh voice information is required for audio lifecycle work",
                    )));
                }
                Command::DetachAudio { reply, .. } => {
                    let _ = reply.send(());
                }
            }
        }
    }
}

struct BackoffContext<'a> {
    info: &'a mut ValidatedInfo,
    latest_sequence: &'a mut Option<Number>,
    transport: &'a mut Option<UdpTransport>,
    audio_attachment: &'a mut Option<(u64, mpsc::Sender<InstalledTransport>)>,
    commands: &'a mut mpsc::Receiver<Command>,
    shutdown: &'a mut watch::Receiver<bool>,
    store: &'a mut StateStore,
}

async fn backoff_or_command(attempt: u8, context: BackoffContext<'_>) -> BackoffOutcome {
    let BackoffContext {
        info,
        latest_sequence,
        transport,
        audio_attachment,
        commands,
        shutdown,
        store,
    } = context;
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
                if let Some(replacement) = accept_replacement(
                    store,
                    transport,
                    replacement,
                    reply,
                ).await {
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
            Some(Command::AttachAudio { reply, .. }) => {
                let _ = reply.send(Err(retrying_error(
                    Operation::StartAudio,
                    store.generation(),
                )));
                BackoffOutcome::Elapsed
            }
            Some(Command::Speaking { reply, .. })
            | Some(Command::RenewTransport { reply, .. }) => {
                let _ = reply.send(Err(retrying_error(
                    Operation::StartAudio,
                    store.generation(),
                )));
                BackoffOutcome::Elapsed
            }
            Some(Command::DetachAudio { audio_id, transport: returned, reply }) => {
                detach_audio(
                    transport,
                    audio_attachment,
                    audio_id,
                    returned,
                    store.generation(),
                );
                let _ = reply.send(());
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

fn dave_error(generation: ConnectionGeneration) -> Error {
    Error::new(
        ErrorKind::DaveTransition,
        Operation::Connect,
        Some(generation),
        RetryDisposition::Fatal,
        None,
        "DAVE setup or transition failed",
    )
}

fn dave_unsupported_error(generation: ConnectionGeneration) -> Error {
    Error::new(
        ErrorKind::DaveUnsupported,
        Operation::Connect,
        Some(generation),
        RetryDisposition::Fatal,
        None,
        "voice gateway selected an unsupported DAVE protocol version",
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

fn superseded_audio_error(generation: ConnectionGeneration) -> Error {
    Error::new(
        ErrorKind::Superseded,
        Operation::StartAudio,
        Some(generation),
        RetryDisposition::Fatal,
        None,
        "audio lifecycle command belongs to a stale connection generation",
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

fn fail_pending_pings_shutdown(
    pending: &mut Vec<oneshot::Sender<Result<Duration, Error>>>,
    generation: ConnectionGeneration,
) {
    let error = Error::new(
        ErrorKind::Shutdown,
        Operation::Ping,
        Some(generation),
        RetryDisposition::Shutdown,
        None,
        "connection shut down before ping completed",
    );
    for waiter in pending.drain(..) {
        let _ = waiter.send(Err(error.clone()));
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
    use std::collections::HashMap;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Waker};

    use oto_testkit::{
        DaveClientRecord, FakeUdpServer, FakeUdpServerConfig, FakeVoiceGateway,
        FakeVoiceGatewayConfig, FaultAction, GatewayCloseStage, GatewayRecord, ManualClock,
        ScriptedClose, TestTls, TransportMode as OracleMode, VoiceClose,
    };

    use super::*;
    use crate::pacer::FRAME_PERIOD;
    use crate::{
        AudioPhase, ConnectionPhase, ErrorKind, EventReceiveError, FrameSource, FrameStatus, Oto,
        PacedAudioSender, ResourceLimits, VoiceConnection, VoiceToken,
    };

    #[derive(Clone)]
    struct QueueSourceHandle {
        state: Arc<Mutex<QueueSourceState>>,
        polls: Arc<AtomicUsize>,
    }

    struct QueueSource {
        state: Arc<Mutex<QueueSourceState>>,
        polls: Arc<AtomicUsize>,
    }

    #[test]
    fn dave_binary_envelope_decoder_enforces_structure_opcode_and_body_bound() {
        for malformed in [&[][..], &[0][..], &[0, 1][..]] {
            assert!(matches!(
                decode_dave_binary_envelope(malformed, 4),
                Err(DaveBinaryEnvelopeError::Malformed)
            ));
        }

        let external_sender = decode_dave_binary_envelope(&[0x12, 0x34, 25, 1, 2, 3, 4], 4)
            .expect("exact body bound is accepted");
        assert_eq!(external_sender.0, 0x1234);
        assert!(matches!(
            external_sender.1,
            Some(DaveControl::ExternalSender(body)) if body == [1, 2, 3, 4]
        ));
        assert!(matches!(
            decode_dave_binary_envelope(&[0, 1, 30, 1, 2, 3, 4, 5], 4),
            Err(DaveBinaryEnvelopeError::BodyTooLarge)
        ));
        assert!(matches!(
            decode_dave_binary_envelope(&[0, 2, 255, 9], 1),
            Ok((2, None))
        ));
        for operation in u8::MIN..=u8::MAX {
            let result = decode_dave_binary_envelope(&[0, 3, 27, operation, 9], 2);
            assert_eq!(result.is_ok(), matches!(operation, 0 | 1));
        }
    }

    #[test]
    fn dave_binary_envelope_decoder_exhausts_short_lengths_opcodes_and_bounds() {
        for body_len in 0..=64 {
            let mut bytes = vec![0xA5, 0x5A, 0];
            bytes.extend((0..body_len).map(|index| index as u8));
            for opcode in u8::MIN..=u8::MAX {
                bytes[2] = opcode;
                if dave_binary_min_body_bytes(opcode).is_some_and(|minimum| body_len < minimum) {
                    assert!(matches!(
                        decode_dave_binary_envelope(&bytes, body_len),
                        Err(DaveBinaryEnvelopeError::Malformed)
                    ));
                } else {
                    let decoded = decode_dave_binary_envelope(&bytes, body_len)
                        .expect("exact body bound accepts complete opcode payloads");
                    assert_eq!(decoded.0, 0xA55A);
                    assert_eq!(decoded.1.is_some(), matches!(opcode, 25 | 27 | 29 | 30));
                }

                if body_len > 0 {
                    assert!(matches!(
                        decode_dave_binary_envelope(&bytes, body_len - 1),
                        Err(DaveBinaryEnvelopeError::BodyTooLarge)
                    ));
                }
            }
        }
    }

    #[test]
    fn dave_json_control_decoder_enforces_integer_types_and_widths() {
        assert!(matches!(
            decode_dave_json_control(
                21,
                &json!({"protocol_version": u16::MAX, "transition_id": u16::MAX})
            ),
            Some(DaveControl::PrepareTransition {
                protocol_version: u16::MAX,
                id: u16::MAX,
            })
        ));
        assert!(matches!(
            decode_dave_json_control(22, &json!({"transition_id": 0})),
            Some(DaveControl::ExecuteTransition { id: 0 })
        ));
        assert!(matches!(
            decode_dave_json_control(24, &json!({"protocol_version": 1, "epoch": u64::MAX})),
            Some(DaveControl::PrepareEpoch {
                protocol_version: 1,
                epoch: u64::MAX,
            })
        ));
        assert!(decode_dave_json_control(20, &json!({})).is_none());

        for invalid_u16 in [
            Value::Null,
            json!(-1),
            json!(1.5),
            json!(u64::from(u16::MAX) + 1),
            json!("1"),
        ] {
            assert!(
                decode_dave_json_control(
                    21,
                    &json!({"protocol_version": invalid_u16, "transition_id": 1})
                )
                .is_none()
            );
            assert!(
                decode_dave_json_control(
                    21,
                    &json!({"protocol_version": 1, "transition_id": invalid_u16})
                )
                .is_none()
            );
            assert!(decode_dave_json_control(22, &json!({"transition_id": invalid_u16})).is_none());
            assert!(
                decode_dave_json_control(24, &json!({"protocol_version": invalid_u16, "epoch": 1}))
                    .is_none()
            );
        }

        for invalid_epoch in [Value::Null, json!(-1), json!(1.5), json!("1")] {
            assert!(
                decode_dave_json_control(
                    24,
                    &json!({"protocol_version": 1, "epoch": invalid_epoch})
                )
                .is_none()
            );
        }
    }

    #[test]
    fn dave_roster_decoder_bounds_cardinality_before_parsing() {
        assert_eq!(
            decode_dave_roster(&json!({"user_ids": ["1", "2"]}), 2),
            Ok(vec![1, 2])
        );
        assert_eq!(
            decode_dave_roster(&json!({"user_ids": ["1", "2", "invalid"]}), 2),
            Err(DaveRosterError::TooManyMembers)
        );
        for malformed in [
            json!({}),
            json!({"user_ids": null}),
            json!({"user_ids": "1"}),
            json!({"user_ids": [1]}),
            json!({"user_ids": ["0"]}),
            json!({"user_ids": ["18446744073709551616"]}),
        ] {
            assert_eq!(
                decode_dave_roster(&malformed, 2),
                Err(DaveRosterError::Malformed)
            );
        }
    }

    #[test]
    fn dave_outbound_binary_limit_accepts_edge_and_rejects_one_byte_over() {
        let exact = dave_outbound_message(
            DaveOutbound::Binary(vec![26, 1, 2, 3]),
            4,
            ConnectionGeneration::FIRST,
        )
        .expect("exact outbound binary limit is accepted");
        assert!(matches!(exact, Message::Binary(bytes) if bytes.as_ref() == [26, 1, 2, 3]));

        let error = dave_outbound_message(
            DaveOutbound::Binary(vec![26, 1, 2, 3, 4]),
            4,
            ConnectionGeneration::FIRST,
        )
        .expect_err("one-over outbound binary message is rejected");
        assert_eq!(error.kind(), ErrorKind::ResourceLimit);
        assert_eq!(error.operation(), Operation::Connect);
        assert_eq!(error.generation(), Some(ConnectionGeneration::FIRST));

        let error = dave_outbound_message(
            DaveOutbound::Binary(Vec::new()),
            4,
            ConnectionGeneration::FIRST,
        )
        .expect_err("an outbound DAVE binary message requires an opcode");
        assert_eq!(error.kind(), ErrorKind::DaveTransition);

        assert!(matches!(
            dave_outbound_message(
                DaveOutbound::Json {
                    opcode: 23,
                    data: json!({"transition_id": 7}),
                },
                1,
                ConnectionGeneration::FIRST,
            ),
            Ok(Message::Text(_))
        ));

        let error = dave_outbound_messages(
            vec![
                DaveOutbound::Json {
                    opcode: 31,
                    data: json!({"transition_id": 7}),
                },
                DaveOutbound::Binary(vec![26, 1, 2, 3, 4]),
            ],
            4,
            ConnectionGeneration::FIRST,
        )
        .expect_err("the complete outbound action batch is validated before writes");
        assert_eq!(error.kind(), ErrorKind::ResourceLimit);
    }

    #[derive(Default)]
    struct QueueSourceState {
        frames: VecDeque<Vec<u8>>,
        ended: bool,
        waker: Option<Waker>,
    }

    impl QueueSource {
        fn pair() -> (Self, QueueSourceHandle) {
            let state = Arc::new(Mutex::new(QueueSourceState::default()));
            let polls = Arc::new(AtomicUsize::new(0));
            (
                Self {
                    state: state.clone(),
                    polls: polls.clone(),
                },
                QueueSourceHandle { state, polls },
            )
        }
    }

    impl FrameSource for QueueSource {
        fn poll_frame(&mut self, cx: &mut Context<'_>, output: &mut [u8]) -> Poll<FrameStatus> {
            self.polls.fetch_add(1, Ordering::Relaxed);
            let mut state = self.state.lock().expect("queue source mutex poisoned");
            if let Some(frame) = state.frames.pop_front() {
                output[..frame.len()].copy_from_slice(&frame);
                return Poll::Ready(FrameStatus::Frame { len: frame.len() });
            }
            if state.ended {
                return Poll::Ready(FrameStatus::Ended);
            }
            state.waker = Some(cx.waker().clone());
            if let Some(frame) = state.frames.pop_front() {
                output[..frame.len()].copy_from_slice(&frame);
                Poll::Ready(FrameStatus::Frame { len: frame.len() })
            } else if state.ended {
                Poll::Ready(FrameStatus::Ended)
            } else {
                Poll::Pending
            }
        }
    }

    impl QueueSourceHandle {
        fn push(&self, frame: impl Into<Vec<u8>>) {
            let waker = {
                let mut state = self.state.lock().expect("queue source mutex poisoned");
                state.frames.push_back(frame.into());
                state.waker.take()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        }

        fn end(&self) {
            let waker = {
                let mut state = self.state.lock().expect("queue source mutex poisoned");
                state.ended = true;
                state.waker.take()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        }

        fn wake_spurious(&self) {
            let waker = self
                .state
                .lock()
                .expect("queue source mutex poisoned")
                .waker
                .clone();
            if let Some(waker) = waker {
                waker.wake();
            }
        }

        fn polls(&self) -> usize {
            self.polls.load(Ordering::Relaxed)
        }
    }

    struct SlowSource;

    impl FrameSource for SlowSource {
        fn poll_frame(&mut self, _cx: &mut Context<'_>, _output: &mut [u8]) -> Poll<FrameStatus> {
            #[cfg(target_os = "linux")]
            let started = crate::audio::source_cpu_time();
            #[cfg(target_os = "linux")]
            while crate::audio::source_cpu_time().saturating_sub(started) < Duration::from_millis(5)
            {
                std::hint::spin_loop();
            }
            #[cfg(not(target_os = "linux"))]
            {
                let started = std::time::Instant::now();
                while started.elapsed() < Duration::from_millis(5) {
                    std::hint::spin_loop();
                }
            }
            Poll::Pending
        }
    }

    struct InvalidLengthSource;

    impl FrameSource for InvalidLengthSource {
        fn poll_frame(&mut self, _cx: &mut Context<'_>, output: &mut [u8]) -> Poll<FrameStatus> {
            Poll::Ready(FrameStatus::Frame {
                len: output.len() + 1,
            })
        }
    }

    struct BenchmarkSource {
        shared: Arc<BenchmarkSourceShared>,
        slow: bool,
    }

    #[derive(Clone)]
    struct BenchmarkSourceHandle {
        shared: Arc<BenchmarkSourceShared>,
    }

    struct BenchmarkSourceShared {
        ready: AtomicBool,
        waker: Mutex<Option<Waker>>,
    }

    impl BenchmarkSource {
        fn pair(slow: bool) -> (Self, BenchmarkSourceHandle) {
            let shared = Arc::new(BenchmarkSourceShared {
                ready: AtomicBool::new(false),
                waker: Mutex::new(None),
            });
            (
                Self {
                    shared: shared.clone(),
                    slow,
                },
                BenchmarkSourceHandle { shared },
            )
        }
    }

    impl FrameSource for BenchmarkSource {
        fn poll_frame(&mut self, cx: &mut Context<'_>, output: &mut [u8]) -> Poll<FrameStatus> {
            if !self.shared.ready.load(Ordering::Acquire) {
                *self
                    .shared
                    .waker
                    .lock()
                    .expect("source waker mutex poisoned") = Some(cx.waker().clone());
                if !self.shared.ready.load(Ordering::Acquire) {
                    return Poll::Pending;
                }
            }
            if self.slow {
                return SlowSource.poll_frame(cx, output);
            }
            output[..4].copy_from_slice(&[0xF8, 0xFF, 0xFE, 0x01]);
            Poll::Ready(FrameStatus::Frame { len: 4 })
        }
    }

    impl BenchmarkSourceHandle {
        fn activate(&self) {
            self.shared.ready.store(true, Ordering::Release);
            if let Some(waker) = self
                .shared
                .waker
                .lock()
                .expect("source waker mutex poisoned")
                .take()
            {
                waker.wake();
            }
        }
    }

    #[derive(Default)]
    struct BenchmarkPeerTimeline {
        last: Option<std::time::Instant>,
        next: Option<std::time::Instant>,
    }

    struct BenchmarkPeerState {
        discovery_packets: u64,
        media_packets: u64,
        timelines: HashMap<std::net::SocketAddr, BenchmarkPeerTimeline>,
        lateness_nanos: Vec<u64>,
        interval_error_nanos: Vec<u64>,
    }

    struct BenchmarkUdpPeer {
        local_addr: std::net::SocketAddr,
        state: Arc<Mutex<BenchmarkPeerState>>,
        shutdown: watch::Sender<bool>,
        task: tokio::task::JoinHandle<std::io::Result<()>>,
    }

    impl BenchmarkUdpPeer {
        async fn start(senders: usize, sample_capacity: usize) -> Self {
            let socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
                .await
                .expect("benchmark UDP peer binds");
            let local_addr = socket.local_addr().expect("benchmark UDP address");
            let state = Arc::new(Mutex::new(BenchmarkPeerState {
                discovery_packets: 0,
                media_packets: 0,
                timelines: HashMap::with_capacity(senders + 1),
                lateness_nanos: Vec::with_capacity(sample_capacity),
                interval_error_nanos: Vec::with_capacity(sample_capacity),
            }));
            let (shutdown, mut shutdown_rx) = watch::channel(false);
            let task_state = state.clone();
            let task = tokio::spawn(async move {
                let mut buffer = [0_u8; 2_048];
                loop {
                    tokio::select! {
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() { return Ok(()); }
                        }
                        received = socket.recv_from(&mut buffer) => {
                            let (bytes, peer) = received?;
                            if bytes == 74 && buffer[..2] == 1_u16.to_be_bytes() {
                                let mut response = [0_u8; 74];
                                response[..2].copy_from_slice(&2_u16.to_be_bytes());
                                response[2..4].copy_from_slice(&70_u16.to_be_bytes());
                                response[4..8].copy_from_slice(&buffer[4..8]);
                                let address = peer.ip().to_string();
                                response[8..8 + address.len()].copy_from_slice(address.as_bytes());
                                response[72..].copy_from_slice(&peer.port().to_be_bytes());
                                socket.send_to(&response, peer).await?;
                                task_state.lock().expect("benchmark peer mutex poisoned")
                                    .discovery_packets += 1;
                                continue;
                            }
                            let now = std::time::Instant::now();
                            let mut state = task_state.lock().expect("benchmark peer mutex poisoned");
                            state.media_packets += 1;
                            let timeline = state.timelines.entry(peer).or_default();
                            let last = timeline.last.replace(now);
                            let expected = timeline.next.replace(
                                timeline.next.map_or(now + FRAME_PERIOD, |next| next + FRAME_PERIOD)
                            );
                            if let Some(last) = last {
                                let interval = now.saturating_duration_since(last);
                                let error = interval.abs_diff(FRAME_PERIOD);
                                state.interval_error_nanos.push(duration_nanos(error));
                            }
                            if let Some(expected) = expected {
                                state.lateness_nanos.push(duration_nanos(
                                    now.saturating_duration_since(expected)
                                ));
                            }
                        }
                    }
                }
            });
            Self {
                local_addr,
                state,
                shutdown,
                task,
            }
        }

        fn reset_measurement(&self) {
            let mut state = self.state.lock().expect("benchmark peer mutex poisoned");
            state.media_packets = 0;
            state.lateness_nanos.clear();
            state.interval_error_nanos.clear();
            for timeline in state.timelines.values_mut() {
                timeline.last = None;
                timeline.next = None;
            }
        }

        async fn shutdown(self) {
            self.shutdown.send_replace(true);
            self.task
                .await
                .expect("benchmark UDP task joins")
                .expect("benchmark UDP task succeeds");
        }
    }

    fn duration_nanos(duration: Duration) -> u64 {
        duration.as_nanos().min(u128::from(u64::MAX)) as u64
    }

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
        assert_eq!(connection.state().phase(), ConnectionPhase::Connected);
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
        assert_eq!(connection.state().phase(), ConnectionPhase::Connected);

        connection.shutdown().await.expect("connection shuts down");
        gateway.shutdown().await.expect("gateway shuts down");
    }

    #[tokio::test]
    async fn paced_sender_waits_for_readiness_then_sends_audio_and_exact_silence_drain() {
        let gateway = TestGateway::start(FakeVoiceGatewayConfig::local())
            .await
            .expect("gateway starts");
        let oto = test_oto(&gateway, ResourceLimits::default());
        let connection = oto
            .connect(voice_info(&gateway, "audio-session", "audio-token"))
            .await
            .expect("transport connects");
        let (source, handle) = QueueSource::pair();
        let sender = connection
            .start_audio(source)
            .await
            .expect("pending source attaches");
        let (duplicate_source, _) = QueueSource::pair();
        assert_eq!(
            connection
                .start_audio(duplicate_source)
                .await
                .expect_err("one connection admits exactly one sender")
                .kind(),
            ErrorKind::ResourceLimit
        );

        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        eventually(|| handle.polls() > 0).await;
        assert_eq!(sender.state().phase(), AudioPhase::WaitingForSource);
        assert_eq!(
            gateway.udp.capture().len(),
            1,
            "discovery is the only UDP packet"
        );
        assert!(
            !gateway
                .records()
                .iter()
                .any(|record| matches!(record, GatewayRecord::Speaking(_)))
        );

        let before_wake_storm = handle.polls();
        for _ in 0..100 {
            handle.wake_spurious();
        }
        eventually(|| handle.polls() > before_wake_storm).await;
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert!(
            handle.polls() <= before_wake_storm + 8,
            "coalesced notification bounds a 100-wake storm"
        );
        assert_eq!(sender.state().phase(), AudioPhase::WaitingForSource);
        handle.push(vec![1, 2, 3, 4]);
        eventually(|| {
            let stats = sender.state().stats();
            stats.frames_sent() == 1 && stats.silence_frames_sent() == 5
        })
        .await;
        eventually(|| sender.state().phase() == AudioPhase::WaitingForSource).await;
        eventually(|| gateway.speaking().len() >= 2).await;

        let speaking: Vec<u64> = gateway
            .speaking()
            .iter()
            .filter_map(|value| value.get("speaking").and_then(Value::as_u64))
            .collect();
        assert_eq!(speaking, [1, 0]);
        let capture = gateway.udp.capture();
        assert_eq!(capture.len(), 7, "discovery + one media + five silence");
        let media = gateway
            .udp
            .decrypt_captured_transport(1, OracleMode::Aes256GcmRtpSize, &[0x42; 32], 1_275)
            .expect("media decrypts");
        assert_eq!(media.payload, [1, 2, 3, 4]);
        for index in 2..7 {
            let silence = gateway
                .udp
                .decrypt_captured_transport(index, OracleMode::Aes256GcmRtpSize, &[0x42; 32], 1_275)
                .expect("silence decrypts");
            assert_eq!(silence.payload, [0xF8, 0xFF, 0xFE]);
        }

        let idle_polls = handle.polls();
        connection
            .ping()
            .await
            .expect("heartbeat state update is acknowledged");
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            handle.polls(),
            idle_polls,
            "heartbeat state updates do not poll an idle source"
        );
        sleep(Duration::from_millis(70)).await;
        assert_eq!(
            handle.polls(),
            idle_polls,
            "idle source has no 20 ms polling"
        );
        let stopped = sender.stop().await.expect("sender stops");
        assert_eq!(stopped.phase(), AudioPhase::Stopped);
        assert_eq!(stopped.stats().frames_sent(), 1);
        assert_eq!(stopped.stats().silence_frames_sent(), 5);
        assert_eq!(stopped.stats().send_failures(), 0);

        let (second_source, second_handle) = QueueSource::pair();
        second_handle.end();
        let second = connection
            .start_audio(second_source)
            .await
            .expect("transport encoder returns for a second sender");
        eventually(|| second.state().phase() == AudioPhase::Stopped).await;
        second.stop().await.expect("second sender stops");
        connection.shutdown().await.expect("connection shuts down");
        gateway.shutdown().await.expect("gateway shuts down");
    }

    #[tokio::test]
    async fn real_audio_resumes_during_silence_drain_then_requires_a_fresh_five_frames() {
        let gateway = TestGateway::start(FakeVoiceGatewayConfig::local())
            .await
            .expect("gateway starts");
        let oto = test_oto(&gateway, ResourceLimits::default());
        let connection = oto
            .connect(voice_info(&gateway, "resume-silence", "audio-token"))
            .await
            .expect("transport connects");
        let (source, handle) = QueueSource::pair();
        let sender = connection
            .start_audio(source)
            .await
            .expect("source attaches");
        handle.push(vec![1, 2, 3]);
        eventually(|| sender.state().stats().silence_frames_sent() >= 2).await;
        let interrupted_silence = sender.state().stats().silence_frames_sent();
        assert!(interrupted_silence < 5, "source resumes before drain ends");
        handle.push(vec![7, 8, 9]);
        eventually(|| {
            let stats = sender.state().stats();
            stats.frames_sent() == 2
                && stats.silence_frames_sent() == interrupted_silence + 5
                && sender.state().phase() == AudioPhase::WaitingForSource
        })
        .await;
        eventually(|| gateway.speaking().len() >= 2).await;
        let expected_packets = 1 + 2 + interrupted_silence as usize + 5;
        eventually(|| gateway.udp.capture().len() >= expected_packets).await;
        let speaking: Vec<u64> = gateway
            .speaking()
            .iter()
            .filter_map(|value| value.get("speaking").and_then(Value::as_u64))
            .collect();
        assert_eq!(speaking, [1, 0], "resume does not flap Speaking");

        let capture = gateway.udp.capture();
        let second_media_index = interrupted_silence as usize + 2;
        let second_media = gateway
            .udp
            .decrypt_captured_transport(
                second_media_index,
                OracleMode::Aes256GcmRtpSize,
                &[0x42; 32],
                1_275,
            )
            .expect("resumed media decrypts");
        assert_eq!(second_media.payload, [7, 8, 9]);
        assert_eq!(
            capture.len(),
            expected_packets,
            "only interrupted silence plus the new five-frame drain is sent"
        );

        sender.stop().await.expect("sender stops");
        connection.shutdown().await.expect("connection shuts down");
        gateway.shutdown().await.expect("gateway shuts down");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn off_cpu_poll_delay_does_not_permanently_kill_valid_audio() {
        // Sleep is a deterministic stand-in for a descheduled callback: wall
        // time advances while this thread consumes essentially no CPU. It is
        // not a recommended FrameSource implementation.
        struct OffCpuSource(bool);
        impl FrameSource for OffCpuSource {
            fn poll_frame(&mut self, _: &mut Context<'_>, output: &mut [u8]) -> Poll<FrameStatus> {
                if self.0 {
                    return Poll::Ready(FrameStatus::Ended);
                }
                self.0 = true;
                std::thread::sleep(Duration::from_millis(20));
                output[..3].copy_from_slice(&[7, 8, 9]);
                Poll::Ready(FrameStatus::Frame { len: 3 })
            }
        }
        let gateway = TestGateway::start(FakeVoiceGatewayConfig::local())
            .await
            .unwrap();
        let oto = test_oto(&gateway, ResourceLimits::default());
        let connection = oto
            .connect(voice_info(&gateway, "off-cpu", "token"))
            .await
            .unwrap();
        let sender = connection.start_audio(OffCpuSource(false)).await.unwrap();
        eventually(|| {
            matches!(
                sender.state().phase(),
                AudioPhase::Failed | AudioPhase::Stopped
            )
        })
        .await;
        let snapshot = sender.state();
        connection.shutdown().await.unwrap();
        gateway.shutdown().await.unwrap();
        assert_eq!(
            snapshot.phase(),
            AudioPhase::Stopped,
            "elapsed time alone must not reject a valid frame"
        );
        assert_eq!(snapshot.stats().frames_sent(), 1);
        assert_eq!(snapshot.stats().source_overruns(), 1);
    }

    #[tokio::test]
    async fn source_replacement_rejects_old_wakes_and_slow_source_isolated_failure() {
        let gateway = TestGateway::start(FakeVoiceGatewayConfig::local())
            .await
            .expect("gateway starts");
        let oto = test_oto(&gateway, ResourceLimits::default());
        let connection = oto
            .connect(voice_info(&gateway, "replace-source", "audio-token"))
            .await
            .expect("transport connects");
        let (old_source, old) = QueueSource::pair();
        let sender = connection
            .start_audio(old_source)
            .await
            .expect("source attaches");
        let (new_source, new) = QueueSource::pair();
        let generation = sender
            .replace_source(new_source)
            .await
            .expect("source replaces");
        assert_eq!(generation.get(), 2);

        old.push(vec![9, 9, 9]);
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        assert_eq!(gateway.udp.capture().len(), 1, "old wake cannot send");
        new.push(vec![7, 8, 9]);
        eventually(|| sender.state().stats().frames_sent() == 1).await;
        let packet = gateway
            .udp
            .decrypt_captured_transport(1, OracleMode::Aes256GcmRtpSize, &[0x42; 32], 1_275)
            .expect("replacement media decrypts");
        assert_eq!(packet.payload, [7, 8, 9]);
        sender.stop().await.expect("replacement sender stops");

        let invalid = connection
            .start_audio(InvalidLengthSource)
            .await
            .expect("invalid source attaches before it is polled");
        eventually(|| invalid.state().phase() == AudioPhase::Failed).await;
        assert_eq!(
            invalid.state().failure(),
            Some(ErrorKind::FrameSourceContract)
        );
        sleep(Duration::from_millis(10)).await;

        let slow = connection
            .start_audio(SlowSource)
            .await
            .expect("slow source attaches");
        eventually(|| slow.state().phase() == AudioPhase::Failed).await;
        assert_eq!(slow.state().failure(), Some(ErrorKind::FrameSourceContract));
        assert_eq!(slow.state().stats().source_overruns(), 1);
        assert_eq!(gateway.udp.capture().len(), 7, "slow source emits no media");

        connection.shutdown().await.expect("connection shuts down");
        gateway.shutdown().await.expect("gateway shuts down");
    }

    #[tokio::test]
    async fn active_playback_survives_resumable_websocket_loss_without_catch_up() {
        let mut config = FakeVoiceGatewayConfig::local();
        config.heartbeat_interval = Duration::from_secs(300);
        let gateway = TestGateway::start(config).await.expect("gateway starts");
        let oto = test_oto(&gateway, ResourceLimits::default());
        let connection = oto
            .connect(voice_info(&gateway, "active-resume", "token"))
            .await
            .expect("transport connects");
        let (source, handle) = BenchmarkSource::pair(false);
        handle.activate();
        let sender = connection.start_audio(source).await.expect("audio starts");
        eventually(|| sender.state().stats().frames_sent() >= 2).await;
        let before = sender.state().stats().frames_sent();
        let captured_before = gateway.udp.capture().len();

        let resume_started = std::time::Instant::now();
        gateway
            .try_close(VoiceClose {
                code: 4015,
                reason: "deterministic playback interruption".to_owned(),
            })
            .expect("resumable close queues");
        eventually(|| connection.state().stats().resume_successes() >= 1).await;
        let resume_millis = resume_started.elapsed().as_millis();
        assert!(resume_millis < 2_000, "local buffered Resume is bounded");
        println!("P14_LOCAL_RESUME_MILLIS={resume_millis}");
        eventually(|| sender.state().stats().frames_sent() > before).await;
        assert_eq!(connection.state().generation().get(), 1);
        assert!(gateway.udp.capture().len() > captured_before);
        assert_eq!(sender.state().stats().skipped_deadlines(), 0);

        sender.stop().await.expect("sender stops");
        connection.shutdown().await.expect("connection shuts down");
        gateway.shutdown().await.expect("gateway shuts down");
    }

    #[tokio::test]
    async fn udp_send_failure_is_typed_and_emits_no_partial_media_packet() {
        let gateway = TestGateway::start(FakeVoiceGatewayConfig::local())
            .await
            .expect("gateway starts");
        let oto = test_oto(&gateway, ResourceLimits::default());
        oto.config
            .fail_udp_sends
            .store(true, std::sync::atomic::Ordering::Release);
        let connection = oto
            .connect(voice_info(&gateway, "udp-failure", "token"))
            .await
            .expect("transport connects");
        let (source, handle) = QueueSource::pair();
        let sender = connection
            .start_audio(source)
            .await
            .expect("audio attaches");
        handle.push(vec![1, 2, 3, 4]);
        eventually(|| sender.state().phase() == AudioPhase::Failed).await;
        let state = sender.state();
        assert_eq!(state.failure(), Some(ErrorKind::SendIo));
        assert_eq!(state.stats().send_failures(), 1);
        assert_eq!(gateway.udp.capture().len(), 1, "discovery only");
        assert!(
            !gateway.speaking().is_empty(),
            "Speaking write precedes failed UDP"
        );
        assert_eq!(
            gateway.speaking()[0]
                .get("speaking")
                .and_then(Value::as_u64),
            Some(1)
        );

        connection.shutdown().await.expect("connection shuts down");
        gateway.shutdown().await.expect("gateway shuts down");
    }

    #[tokio::test]
    async fn close_during_dave_setup_is_terminal_and_never_starts_media() {
        let mut config = FakeVoiceGatewayConfig::local();
        config.dave_protocol_version = 1;
        let gateway = TestGateway::start(config).await.expect("gateway starts");
        let oto = test_oto(&gateway, ResourceLimits::default());
        let connection = oto
            .connect(voice_info(&gateway, "dave-close", "token"))
            .await
            .expect("transport reaches DAVE setup");
        gateway
            .try_close(VoiceClose {
                code: 4004,
                reason: "close during DAVE setup".to_owned(),
            })
            .expect("close queues");
        eventually(|| connection.state().phase() == ConnectionPhase::Failed).await;
        assert_eq!(
            connection
                .state()
                .failure()
                .expect("failure persists")
                .kind(),
            ErrorKind::CredentialsRejected
        );
        assert_eq!(gateway.udp.capture().len(), 1);
        assert!(gateway.speaking().is_empty());
        gateway.shutdown().await.expect("gateway shuts down");
    }

    #[tokio::test]
    async fn shutdown_during_resume_cancels_reconnect_without_new_generation() {
        let mut config = FakeVoiceGatewayConfig::local();
        config.hello_delay = Duration::from_millis(150);
        let gateway = TestGateway::start(config).await.expect("gateway starts");
        let oto = test_oto(&gateway, ResourceLimits::default());
        let connection = oto
            .connect(voice_info(&gateway, "resume-shutdown", "token"))
            .await
            .expect("transport connects");
        gateway
            .try_close(VoiceClose {
                code: 4015,
                reason: "resume then shutdown".to_owned(),
            })
            .expect("resumable close queues");
        eventually(|| connection.state().phase() == ConnectionPhase::Resuming).await;
        let snapshot = timeout(Duration::from_secs(1), connection.shutdown())
            .await
            .expect("shutdown is not stranded in resume")
            .expect("shutdown succeeds");
        assert_eq!(snapshot.phase(), ConnectionPhase::Closed);
        assert_eq!(snapshot.generation().get(), 1);
        gateway.shutdown().await.expect("gateway shuts down");
    }

    #[tokio::test]
    async fn fresh_voice_info_during_playback_invalidates_old_transport_before_replacement_media() {
        let tls = TestTls::generate().expect("shared TLS material generates");
        let initial = TestGateway::start_with_tls(FakeVoiceGatewayConfig::local(), tls.clone())
            .await
            .expect("initial gateway starts");
        let replacement = TestGateway::start_with_tls(FakeVoiceGatewayConfig::local(), tls)
            .await
            .expect("replacement gateway starts");
        let oto = test_oto(&initial, ResourceLimits::default());
        let connection = oto
            .connect(voice_info(&initial, "playback-old", "token-old"))
            .await
            .expect("initial transport connects");
        let (source, handle) = BenchmarkSource::pair(false);
        handle.activate();
        let sender = connection.start_audio(source).await.expect("audio starts");
        eventually(|| sender.state().stats().frames_sent() >= 2).await;
        let generation = connection
            .replace_voice_info(voice_info(&replacement, "playback-new", "token-new"))
            .await
            .expect("fresh voice information accepted");
        assert_eq!(generation.get(), 2);
        let old_after_replacement = initial.udp.capture().len();
        eventually(|| {
            connection.state().generation() == generation
                && connection.state().phase() == ConnectionPhase::Connected
                && replacement.udp.capture().len() >= 2
        })
        .await;
        let old_after = initial.udp.capture().len();
        assert_eq!(
            old_after, old_after_replacement,
            "old generation cannot send after replacement is acknowledged"
        );
        assert!(
            sender.state().stats().frames_sent() >= 3,
            "attached playback resumes on replacement"
        );

        sender.stop().await.expect("sender stops");
        connection.shutdown().await.expect("connection shuts down");
        initial
            .shutdown()
            .await
            .expect("initial gateway shuts down");
        replacement
            .shutdown()
            .await
            .expect("replacement gateway shuts down");
    }

    #[tokio::test]
    async fn dave_transport_replacement_pauses_attached_sender_until_dave_is_ready() {
        let tls = TestTls::generate().expect("shared TLS material generates");
        let initial = TestGateway::start_with_tls(FakeVoiceGatewayConfig::local(), tls.clone())
            .await
            .expect("initial gateway starts");
        let mut dave_config = FakeVoiceGatewayConfig::local();
        dave_config.dave_protocol_version = 1;
        let replacement = TestGateway::start_with_tls(dave_config, tls)
            .await
            .expect("DAVE replacement gateway starts");
        let oto = test_oto(&initial, ResourceLimits::default());
        let connection = oto
            .connect(voice_info(&initial, "dave-replace-old", "token-old"))
            .await
            .expect("initial transport connects");
        let (source, handle) = BenchmarkSource::pair(false);
        handle.activate();
        let sender = connection.start_audio(source).await.expect("audio starts");
        eventually(|| sender.state().stats().frames_sent() >= 2).await;
        connection
            .replace_voice_info(voice_info(&replacement, "dave-replace-new", "token-new"))
            .await
            .expect("DAVE replacement accepted");
        let old_after_replacement = initial.udp.capture().len();
        eventually(|| connection.state().phase() == ConnectionPhase::EstablishingDave).await;
        sleep(Duration::from_millis(30)).await;
        assert_ne!(sender.state().phase(), AudioPhase::Failed);
        assert_eq!(
            replacement.udp.capture().len(),
            1,
            "DAVE setup has discovery only"
        );
        assert_eq!(
            initial.udp.capture().len(),
            old_after_replacement,
            "old transport is invalidated before replacement is acknowledged"
        );

        sender
            .stop()
            .await
            .expect("sender stops while DAVE is pending");
        connection.shutdown().await.expect("connection shuts down");
        initial
            .shutdown()
            .await
            .expect("initial gateway shuts down");
        replacement
            .shutdown()
            .await
            .expect("replacement gateway shuts down");
    }

    #[tokio::test]
    async fn dave_required_transport_refuses_audio_without_plaintext_packet() {
        let mut config = FakeVoiceGatewayConfig::local();
        config.dave_protocol_version = 1;
        let gateway = TestGateway::start(config).await.expect("gateway starts");
        let oto = test_oto(&gateway, ResourceLimits::default());
        let connection = oto
            .connect(voice_info(&gateway, "dave-session", "dave-token"))
            .await
            .expect("transport reaches DAVE establishment");
        assert_eq!(
            connection.state().phase(),
            ConnectionPhase::EstablishingDave
        );
        let (source, handle) = QueueSource::pair();
        handle.push(vec![1, 2, 3]);
        let error = connection
            .start_audio(source)
            .await
            .expect_err("nonzero DAVE call cannot start plaintext audio");
        assert_eq!(error.kind(), ErrorKind::DaveRequired);
        assert_eq!(gateway.udp.capture().len(), 1);
        assert!(gateway.speaking().is_empty());

        connection.shutdown().await.expect("connection shuts down");
        gateway.shutdown().await.expect("gateway shuts down");
    }

    #[tokio::test]
    async fn dave_epoch_reset_during_playback_pauses_without_plaintext_or_sender_failure() {
        let mut config = FakeVoiceGatewayConfig::local();
        config.dave_protocol_version = 1;
        config.heartbeat_interval = Duration::from_secs(300);
        let gateway = TestGateway::start(config).await.expect("gateway starts");
        let oto = test_oto(&gateway, ResourceLimits::default());
        oto.config
            .ready_dave_fixture
            .store(true, std::sync::atomic::Ordering::Release);
        let info = voice_info(&gateway, "dave-playback", "dave-token");
        let connection = oto
            .connect(info)
            .await
            .expect("transport reaches DAVE setup");

        gateway
            .try_dave_prepare_transition(1, 0)
            .expect("initial transition queues");
        eventually(|| connection.state().phase() == ConnectionPhase::Connected).await;

        let (source, handle) = BenchmarkSource::pair(false);
        handle.activate();
        let sender = connection.start_audio(source).await.expect("audio starts");
        eventually(|| sender.state().stats().frames_sent() >= 2).await;

        let before_prepare = sender.state().stats().frames_sent();
        gateway
            .try_dave_prepare_transition(1, 8)
            .expect("playback transition prepares");
        eventually(|| {
            gateway
                .dave_client_records()
                .iter()
                .any(|record| matches!(record, DaveClientRecord::Ready { transition_id: 8 }))
                && sender.state().stats().frames_sent() > before_prepare
        })
        .await;
        let before_execute = sender.state().stats().frames_sent();
        gateway
            .try_dave_execute_transition(8)
            .expect("playback transition executes");
        eventually(|| sender.state().stats().frames_sent() > before_execute).await;
        assert_eq!(connection.state().phase(), ConnectionPhase::Connected);

        gateway
            .try_dave_prepare_epoch(1, 1)
            .expect("epoch reset queues");
        eventually(|| connection.state().phase() == ConnectionPhase::EstablishingDave).await;
        // A frame whose encryption command was ordered before PrepareEpoch may
        // finish its transport send after the connection snapshot changes. It
        // belongs wholly to the old epoch. Once that in-flight frame settles,
        // the paused sender must emit nothing further.
        sleep(Duration::from_millis(5)).await;
        let packets_before_reset = gateway.udp.capture().len();
        sleep(Duration::from_millis(40)).await;
        assert_ne!(sender.state().phase(), AudioPhase::Failed);
        assert_eq!(
            gateway.udp.capture().len(),
            packets_before_reset,
            "media pauses while the replacement DAVE epoch is unready"
        );
        for index in 1..packets_before_reset {
            let packet = gateway
                .udp
                .decrypt_captured_transport(
                    index,
                    OracleMode::Aes256GcmRtpSize,
                    &[0x42; 32],
                    1_275 + dave::OPUS_MAX_ENCRYPTION_OVERHEAD_BYTES,
                )
                .expect("transport layer decrypts around the DAVE transition");
            assert_ne!(packet.payload, [0xF8, 0xFF, 0xFE, 0x01]);
            assert!(packet.payload.ends_with(&[0xFA, 0xFA]));
        }

        sender.stop().await.expect("sender stops");
        connection.shutdown().await.expect("connection shuts down");
        gateway.shutdown().await.expect("gateway shuts down");
    }

    #[tokio::test]
    async fn unsupported_initial_dave_version_is_typed_before_transport_installation() {
        let mut config = FakeVoiceGatewayConfig::local();
        config.dave_protocol_version = dave::MAX_PROTOCOL_VERSION + 1;
        let gateway = TestGateway::start(config).await.expect("gateway starts");
        let oto = test_oto(&gateway, ResourceLimits::default());

        let error = oto
            .connect(voice_info(&gateway, "unsupported-dave", "dave-token"))
            .await
            .expect_err("unsupported initial DAVE version rejects connection");
        assert_eq!(error.kind(), ErrorKind::DaveUnsupported);
        assert_eq!(error.operation(), Operation::Connect);
        assert_eq!(error.retry_disposition(), RetryDisposition::Fatal);
        assert!(gateway.dave_client_records().is_empty());
        assert_eq!(gateway.udp.capture().len(), 1);

        gateway.shutdown().await.expect("gateway shuts down");
    }

    #[tokio::test]
    async fn unnegotiated_dave_controls_cannot_activate_dave_lazily() {
        enum ControlCase {
            Json(u8, Value),
            Binary(u8, Vec<u8>),
        }

        for (case, control) in [
            (
                "json-transition",
                ControlCase::Json(21, json!({"protocol_version": 1, "transition_id": 7})),
            ),
            (
                "binary-external-sender",
                ControlCase::Binary(25, crate::dave::test_external_sender_fixture()),
            ),
        ] {
            let gateway = TestGateway::start(FakeVoiceGatewayConfig::local())
                .await
                .expect("gateway starts");
            let oto = test_oto(&gateway, ResourceLimits::default());
            let connection = oto
                .connect(voice_info(&gateway, case, "dave-token"))
                .await
                .expect("version zero fixture connects");
            assert_eq!(connection.state().phase(), ConnectionPhase::Connected);

            match control {
                ControlCase::Json(opcode, data) => gateway
                    .try_dispatch_json(opcode, data, true)
                    .expect("unnegotiated JSON DAVE control queues"),
                ControlCase::Binary(opcode, data) => gateway
                    .try_dispatch_binary(opcode, data)
                    .expect("unnegotiated binary DAVE control queues"),
            }
            eventually(|| connection.state().phase() == ConnectionPhase::Failed).await;

            let state = connection.state();
            let failure = state.failure().expect("protocol failure persists");
            assert_eq!(failure.kind(), ErrorKind::GatewayProtocol, "case {case}");
            assert_eq!(failure.operation(), Operation::Connect, "case {case}");
            assert!(gateway.dave_client_records().is_empty(), "case {case}");
            assert_eq!(gateway.udp.capture().len(), 1, "case {case}");

            gateway.shutdown().await.expect("gateway shuts down");
        }
    }

    #[tokio::test]
    async fn oversized_generated_dave_output_fails_before_any_partial_response() {
        let external_sender = crate::dave::test_external_sender_fixture();
        let maximum_message_bytes = external_sender.len() + 3;
        let limits = ResourceLimits::default()
            .with_gateway_binary_bytes(maximum_message_bytes)
            .with_dave_binary_body_bytes(external_sender.len());
        let mut config = FakeVoiceGatewayConfig::local();
        config.dave_protocol_version = 1;
        let gateway = TestGateway::start(config).await.expect("gateway starts");
        let oto = test_oto(&gateway, limits);
        let connection = oto
            .connect(voice_info(&gateway, "dave-output-limit", "dave-token"))
            .await
            .expect("transport reaches DAVE establishment");

        gateway
            .try_dave_external_sender(external_sender)
            .expect("exact-limit external sender queues");
        eventually(|| connection.state().phase() == ConnectionPhase::Failed).await;

        let state = connection.state();
        let failure = state.failure().expect("resource failure persists");
        assert_eq!(failure.kind(), ErrorKind::ResourceLimit);
        assert_eq!(failure.operation(), Operation::Connect);
        assert!(gateway.dave_client_records().is_empty());
        assert_eq!(gateway.udp.capture().len(), 1);

        gateway.shutdown().await.expect("gateway shuts down");
    }

    #[tokio::test]
    async fn invalid_dave_commit_and_welcome_recover_with_fresh_packages_end_to_end() {
        let mut config = FakeVoiceGatewayConfig::local();
        config.dave_protocol_version = 1;
        let gateway = TestGateway::start(config).await.expect("gateway starts");
        let oto = test_oto(&gateway, ResourceLimits::default());
        let connection = oto
            .connect(voice_info(&gateway, "dave-recovery", "dave-token"))
            .await
            .expect("transport reaches DAVE establishment");

        gateway
            .try_dave_prepare_transition(1, 8)
            .expect("first transition queues");
        gateway
            .try_dave_commit(8, vec![0])
            .expect("invalid commit queues");
        eventually(|| gateway.dave_client_records().len() >= 2).await;

        gateway
            .try_dave_prepare_transition(1, 9)
            .expect("replacement transition queues");
        gateway
            .try_dave_welcome(9, vec![0])
            .expect("invalid welcome queues");
        eventually(|| gateway.dave_client_records().len() >= 4).await;

        let records = gateway.dave_client_records();
        assert!(matches!(
            records.as_slice(),
            [
                DaveClientRecord::InvalidCommitWelcome { transition_id: 8 },
                DaveClientRecord::KeyPackage(first),
                DaveClientRecord::InvalidCommitWelcome { transition_id: 9 },
                DaveClientRecord::KeyPackage(second),
            ] if first != second
        ));
        assert_eq!(
            connection.state().phase(),
            ConnectionPhase::EstablishingDave
        );
        assert!(connection.state().failure().is_none());

        connection.shutdown().await.expect("connection shuts down");
        gateway.shutdown().await.expect("gateway shuts down");
    }

    #[tokio::test]
    async fn malformed_dave_binary_controls_fail_before_backend_end_to_end() {
        for (case, opcode, payload) in [
            ("short-external-sender", 25, vec![1, 2, 3]),
            ("short-proposals", 27, vec![0]),
            ("invalid-proposals-operation", 27, vec![2, 9]),
            ("short-commit", 29, vec![0, 7]),
            ("short-welcome", 30, vec![0, 7]),
        ] {
            let mut config = FakeVoiceGatewayConfig::local();
            config.dave_protocol_version = 1;
            let gateway = TestGateway::start(config).await.expect("gateway starts");
            let oto = test_oto(&gateway, ResourceLimits::default());
            let connection = oto
                .connect(voice_info(&gateway, case, "dave-token"))
                .await
                .expect("transport reaches DAVE establishment");

            gateway
                .try_dispatch_binary(opcode, payload)
                .expect("malformed DAVE control queues");
            eventually(|| connection.state().phase() == ConnectionPhase::Failed).await;

            let state = connection.state();
            let failure = state.failure().expect("protocol failure persists");
            assert_eq!(failure.kind(), ErrorKind::GatewayProtocol, "case {case}");
            assert_eq!(failure.operation(), Operation::Connect, "case {case}");
            assert_eq!(
                failure.retry_disposition(),
                RetryDisposition::Fatal,
                "case {case}"
            );
            assert!(
                gateway.dave_client_records().is_empty(),
                "case {case} must fail before DAVE backend output"
            );

            gateway.shutdown().await.expect("gateway shuts down");
        }
    }

    #[tokio::test]
    async fn malformed_dave_json_controls_fail_before_backend_end_to_end() {
        for (case, opcode, data) in [
            ("prepare-missing-version", 21, json!({"transition_id": 7})),
            (
                "prepare-string-version",
                21,
                json!({"protocol_version": "1", "transition_id": 7}),
            ),
            (
                "prepare-transition-overflow",
                21,
                json!({"protocol_version": 1, "transition_id": u64::from(u16::MAX) + 1}),
            ),
            (
                "execute-negative-transition",
                22,
                json!({"transition_id": -1}),
            ),
            (
                "epoch-fractional-version",
                24,
                json!({"protocol_version": 1.5, "epoch": 1}),
            ),
            (
                "epoch-string-value",
                24,
                json!({"protocol_version": 1, "epoch": "1"}),
            ),
        ] {
            let mut config = FakeVoiceGatewayConfig::local();
            config.dave_protocol_version = 1;
            let gateway = TestGateway::start(config).await.expect("gateway starts");
            let oto = test_oto(&gateway, ResourceLimits::default());
            let connection = oto
                .connect(voice_info(&gateway, case, "dave-token"))
                .await
                .expect("transport reaches DAVE establishment");

            gateway
                .try_dispatch_json(opcode, data, true)
                .expect("malformed DAVE control queues");
            eventually(|| connection.state().phase() == ConnectionPhase::Failed).await;

            let state = connection.state();
            let failure = state.failure().expect("protocol failure persists");
            assert_eq!(failure.kind(), ErrorKind::GatewayProtocol, "case {case}");
            assert_eq!(failure.operation(), Operation::Connect, "case {case}");
            assert_eq!(
                failure.retry_disposition(),
                RetryDisposition::Fatal,
                "case {case}"
            );
            assert!(
                gateway.dave_client_records().is_empty(),
                "case {case} must fail before DAVE backend output"
            );

            gateway.shutdown().await.expect("gateway shuts down");
        }
    }

    #[tokio::test]
    async fn malformed_dave_membership_controls_fail_before_backend_end_to_end() {
        for (case, opcode, data) in [
            ("roster-not-array", 11, json!({"user_ids": "1"})),
            ("roster-numeric-id", 11, json!({"user_ids": [1]})),
            ("roster-zero-id", 11, json!({"user_ids": ["0"]})),
            ("disconnect-missing-id", 13, json!({})),
            ("disconnect-numeric-id", 13, json!({"user_id": 1})),
            ("disconnect-zero-id", 13, json!({"user_id": "0"})),
            (
                "disconnect-overflow-id",
                13,
                json!({"user_id": "18446744073709551616"}),
            ),
        ] {
            let mut config = FakeVoiceGatewayConfig::local();
            config.dave_protocol_version = 1;
            let gateway = TestGateway::start(config).await.expect("gateway starts");
            let oto = test_oto(&gateway, ResourceLimits::default());
            let connection = oto
                .connect(voice_info(&gateway, case, "dave-token"))
                .await
                .expect("transport reaches DAVE establishment");

            gateway
                .try_dispatch_json(opcode, data, true)
                .expect("malformed DAVE membership control queues");
            eventually(|| connection.state().phase() == ConnectionPhase::Failed).await;

            let state = connection.state();
            let failure = state.failure().expect("protocol failure persists");
            assert_eq!(failure.kind(), ErrorKind::GatewayProtocol, "case {case}");
            assert_eq!(failure.operation(), Operation::Connect, "case {case}");
            assert_eq!(
                failure.retry_disposition(),
                RetryDisposition::Fatal,
                "case {case}"
            );
            assert!(
                gateway.dave_client_records().is_empty(),
                "case {case} must fail before DAVE backend output"
            );

            gateway.shutdown().await.expect("gateway shuts down");
        }
    }

    #[tokio::test]
    async fn rejected_dave_versions_are_typed_transitions_end_to_end() {
        for (case, opcode, data) in [
            (
                "prepare-downgrade",
                21,
                json!({"protocol_version": 0, "transition_id": 7}),
            ),
            (
                "prepare-unsupported",
                21,
                json!({"protocol_version": 2, "transition_id": 7}),
            ),
            (
                "epoch-downgrade",
                24,
                json!({"protocol_version": 0, "epoch": 1}),
            ),
            (
                "epoch-unsupported",
                24,
                json!({"protocol_version": 2, "epoch": 1}),
            ),
        ] {
            let mut config = FakeVoiceGatewayConfig::local();
            config.dave_protocol_version = 1;
            let gateway = TestGateway::start(config).await.expect("gateway starts");
            let oto = test_oto(&gateway, ResourceLimits::default());
            let connection = oto
                .connect(voice_info(&gateway, case, "dave-token"))
                .await
                .expect("transport reaches DAVE establishment");

            gateway
                .try_dispatch_json(opcode, data, true)
                .expect("well-formed rejected DAVE version queues");
            eventually(|| connection.state().phase() == ConnectionPhase::Failed).await;

            let state = connection.state();
            let failure = state.failure().expect("DAVE transition failure persists");
            assert_eq!(failure.kind(), ErrorKind::DaveTransition, "case {case}");
            assert_eq!(failure.operation(), Operation::Connect, "case {case}");
            assert_eq!(
                failure.retry_disposition(),
                RetryDisposition::Fatal,
                "case {case}"
            );
            assert!(
                gateway.dave_client_records().is_empty(),
                "case {case} must emit no DAVE response"
            );
            assert_eq!(gateway.udp.capture().len(), 1, "case {case}");

            gateway.shutdown().await.expect("gateway shuts down");
        }
    }

    #[tokio::test]
    async fn dave_resume_retains_session_but_fresh_identify_and_replacement_reset_it() {
        let mut config = FakeVoiceGatewayConfig::local();
        config.dave_protocol_version = 1;
        config.heartbeat_interval = Duration::from_secs(300);
        let gateway = TestGateway::start(config).await.expect("gateway starts");
        let oto = test_oto(&gateway, ResourceLimits::default());
        let connection = oto
            .connect(voice_info(&gateway, "dave-reset-1", "dave-token-1"))
            .await
            .expect("transport reaches DAVE establishment");
        let key_packages = || {
            gateway
                .dave_client_records()
                .iter()
                .filter(|record| matches!(record, DaveClientRecord::KeyPackage(_)))
                .count()
        };
        let selects = || {
            gateway
                .records()
                .iter()
                .filter(|record| matches!(record, GatewayRecord::SelectProtocol(_)))
                .count()
        };

        gateway
            .try_dave_external_sender(crate::dave::test_external_sender_fixture())
            .expect("external sender queues");
        eventually(|| key_packages() == 1).await;

        gateway
            .try_close(VoiceClose {
                code: 4015,
                reason: "resumable DAVE interruption".to_owned(),
            })
            .expect("resumable close queues");
        eventually(|| connection.state().stats().resume_successes() == 1).await;
        gateway
            .try_dave_prepare_epoch(1, 1)
            .expect("post-resume epoch queues");
        eventually(|| key_packages() == 2).await;
        assert_eq!(selects(), 1, "successful Resume reuses the transport");

        gateway
            .try_close(VoiceClose {
                code: 4006,
                reason: "fresh identify required".to_owned(),
            })
            .expect("fresh-identify close queues");
        eventually(|| selects() == 2).await;
        let unknown_before = connection.state().stats().unknown_opcodes();
        gateway
            .try_dave_prepare_epoch(1, 1)
            .expect("new-session epoch queues");
        gateway
            .try_dispatch_json(250, json!({"barrier": "fresh"}), true)
            .expect("ordered barrier queues");
        eventually(|| connection.state().stats().unknown_opcodes() > unknown_before).await;
        assert_eq!(
            key_packages(),
            2,
            "fresh Identify must not retain the prior external sender"
        );
        gateway
            .try_dave_external_sender(crate::dave::test_external_sender_fixture())
            .expect("replacement external sender queues");
        eventually(|| key_packages() == 3).await;

        let generation = connection
            .replace_voice_info(voice_info(&gateway, "dave-reset-2", "dave-token-2"))
            .await
            .expect("replacement is accepted");
        eventually(|| connection.state().generation() == generation && selects() == 3).await;
        let unknown_before = connection.state().stats().unknown_opcodes();
        gateway
            .try_dave_prepare_epoch(1, 1)
            .expect("replacement epoch queues");
        gateway
            .try_dispatch_json(251, json!({"barrier": "replacement"}), true)
            .expect("replacement barrier queues");
        eventually(|| connection.state().stats().unknown_opcodes() > unknown_before).await;
        assert_eq!(
            key_packages(),
            3,
            "generation replacement must reset the DAVE session"
        );
        gateway
            .try_dave_external_sender(crate::dave::test_external_sender_fixture())
            .expect("new-generation external sender queues");
        eventually(|| key_packages() == 4).await;

        connection.shutdown().await.expect("connection shuts down");
        gateway.shutdown().await.expect("gateway shuts down");
    }

    #[tokio::test]
    async fn dave_resume_restores_connected_when_retained_sender_context_is_ready() {
        let mut config = FakeVoiceGatewayConfig::local();
        config.dave_protocol_version = 1;
        config.heartbeat_interval = Duration::from_secs(300);
        let gateway = TestGateway::start(config).await.expect("gateway starts");
        let oto = test_oto(&gateway, ResourceLimits::default());
        oto.config
            .ready_dave_fixture
            .store(true, std::sync::atomic::Ordering::Release);
        let connection = oto
            .connect(voice_info(&gateway, "dave-ready-resume", "dave-token"))
            .await
            .expect("transport reaches DAVE setup");

        gateway
            .try_dave_prepare_transition(1, 0)
            .expect("initial transition queues");
        eventually(|| connection.state().phase() == ConnectionPhase::Connected).await;
        gateway
            .try_close(VoiceClose {
                code: 4015,
                reason: "resumable ready DAVE interruption".to_owned(),
            })
            .expect("resumable close queues");

        eventually(|| connection.state().stats().resume_successes() == 1).await;
        assert_eq!(connection.state().generation().get(), 1);
        assert_eq!(connection.state().phase(), ConnectionPhase::Connected);

        connection.shutdown().await.expect("connection shuts down");
        gateway.shutdown().await.expect("gateway shuts down");
    }

    #[tokio::test]
    async fn nonce_exhaustion_renews_the_full_transport_before_another_media_packet() {
        let gateway = TestGateway::start(FakeVoiceGatewayConfig::local())
            .await
            .expect("gateway starts");
        let oto = Oto::builder()
            .resource_limits(ResourceLimits::default())
            .test_tls_config(gateway.tls().client_config())
            .test_transport_nonce_start(u32::MAX)
            .build()
            .expect("test Oto config is valid");
        let connection = oto
            .connect(voice_info(&gateway, "nonce-renewal", "nonce-token"))
            .await
            .expect("transport connects");
        let (source, handle) = BenchmarkSource::pair(false);
        handle.activate();
        let sender = connection
            .start_audio(source)
            .await
            .expect("source attaches");

        eventually(|| {
            sender.state().stats().frames_sent() >= 2
                && gateway
                    .records()
                    .iter()
                    .filter(|record| matches!(record, GatewayRecord::Identify(_)))
                    .count()
                    >= 2
        })
        .await;
        eventually(|| gateway.udp.capture().len() >= 4).await;
        let capture = gateway.udp.capture();
        assert!(capture.len() >= 4, "two discoveries and two media packets");
        let first_media = capture
            .snapshot()
            .into_iter()
            .find(|packet| packet.bytes.len() != 74)
            .expect("first media packet is captured");
        assert_eq!(
            &first_media.bytes[first_media.bytes.len() - 4..],
            &u32::MAX.to_le_bytes(),
            "the maximum nonce is used exactly once before renewal"
        );

        sender.stop().await.expect("sender stops");
        connection.shutdown().await.expect("connection shuts down");
        gateway.shutdown().await.expect("gateway shuts down");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 28)]
    #[ignore = "release-only P07 production-path performance gate"]
    async fn p07_complete_non_dave_path_benchmark() {
        let sender_count = benchmark_env("OTO_P07_SENDERS", 250);
        let warmup = Duration::from_millis(benchmark_env("OTO_P07_WARMUP_MS", 1_000) as u64);
        let measurement = Duration::from_millis(benchmark_env("OTO_P07_MEASURE_MS", 3_000) as u64);
        let churn_cycles = benchmark_env("OTO_P07_CHURN", 32);
        assert!(sender_count > 0);
        let samples = sender_count
            .saturating_mul((measurement.as_millis() / 20) as usize + 64)
            .max(1_024);
        let peer = BenchmarkUdpPeer::start(sender_count + 1, samples).await;
        let mut gateway_config = FakeVoiceGatewayConfig::local();
        gateway_config.voice_ip = peer.local_addr.ip().to_string();
        gateway_config.voice_port = peer.local_addr.port();
        gateway_config.heartbeat_interval = Duration::from_secs(300);
        let tls = TestTls::generate().expect("benchmark TLS generates");
        let mut gateways = Vec::with_capacity(sender_count + 1);
        for _ in 0..sender_count + 1 {
            gateways.push(
                FakeVoiceGateway::start_with_tls(gateway_config.clone(), tls.clone())
                    .await
                    .expect("benchmark gateway starts"),
            );
        }
        let oto = test_oto(&gateways[0], ResourceLimits::default());
        let base_tasks = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        let baseline_threads = process_thread_count();
        let baseline_pss_kib = process_pss_kib();

        let connect_started = std::time::Instant::now();
        let connections =
            futures_util::future::join_all(gateways.iter().enumerate().map(|(index, gateway)| {
                let oto = oto.clone();
                async move {
                    oto.connect(voice_info(
                        gateway,
                        &format!("p07-benchmark-{index}"),
                        &format!("p07-token-{index}"),
                    ))
                    .await
                    .unwrap_or_else(|error| {
                        panic!(
                            "benchmark connection establishes: {error:?}; records={:?}",
                            gateway.records()
                        )
                    })
                }
            }))
            .await;
        let connect_millis = connect_started.elapsed().as_millis();
        let idle_pss_kib = process_pss_kib();
        let idle_tasks = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        let idle_threads = process_thread_count();

        let mut senders = Vec::with_capacity(sender_count);
        let mut handles = Vec::with_capacity(sender_count);
        for connection in connections.iter().take(sender_count) {
            let (source, handle) = BenchmarkSource::pair(false);
            senders.push(
                connection
                    .start_audio(source)
                    .await
                    .expect("pending benchmark source attaches"),
            );
            handles.push(handle);
        }
        let (slow_source, slow_handle) = BenchmarkSource::pair(true);
        let slow_sender = connections[sender_count]
            .start_audio(slow_source)
            .await
            .expect("slow benchmark source attaches");
        let pending_pss_kib = process_pss_kib();
        let pending_tasks = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        let pending_threads = process_thread_count();

        let synchronized_start = std::time::Instant::now();
        slow_handle.activate();
        for handle in &handles {
            handle.activate();
        }
        timeout(Duration::from_secs(10), async {
            loop {
                let ready = senders
                    .iter()
                    .all(|sender| sender.state().stats().frames_sent() >= 5);
                if ready && slow_sender.state().phase() == AudioPhase::Failed {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all healthy senders start and the slow source is isolated");
        let synchronized_start_millis = synchronized_start.elapsed().as_millis();
        assert_eq!(
            slow_sender.state().failure(),
            Some(ErrorKind::FrameSourceContract)
        );
        sleep(warmup).await;

        let frames_before: Vec<u64> = senders
            .iter()
            .map(|sender| sender.state().stats().frames_sent())
            .collect();
        peer.reset_measurement();
        let active_pss_kib = process_pss_kib();
        let active_tasks = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        let active_threads = process_thread_count();
        let cpu_before = process_cpu_ticks();
        let measured_started = std::time::Instant::now();
        sleep(measurement).await;
        let measured_elapsed = measured_started.elapsed();
        let cpu_ticks = process_cpu_ticks().saturating_sub(cpu_before);
        let frames_after: Vec<u64> = senders
            .iter()
            .map(|sender| sender.state().stats().frames_sent())
            .collect();
        let sent_frames: u64 = frames_after
            .iter()
            .zip(&frames_before)
            .map(|(after, before)| after.saturating_sub(*before))
            .sum();
        let (udp_packets, mut lateness, mut interval_error) = {
            let state = peer.state.lock().expect("benchmark peer mutex poisoned");
            (
                state.media_packets,
                state.lateness_nanos.clone(),
                state.interval_error_nanos.clone(),
            )
        };
        let allocation_frames_before: u64 = senders
            .iter()
            .map(|sender| sender.state().stats().frames_sent())
            .sum();
        let allocation_deadline =
            std::time::Instant::now() + measurement.min(Duration::from_secs(1));
        let allocation_region = stats_alloc::Region::new(crate::TEST_ALLOCATOR);
        while std::time::Instant::now() < allocation_deadline {
            tokio::task::yield_now().await;
        }
        let allocation = allocation_region.change();
        let allocation_frames = senders
            .iter()
            .map(|sender| sender.state().stats().frames_sent())
            .sum::<u64>()
            .saturating_sub(allocation_frames_before);
        let max_sender_lateness_nanos = senders
            .iter()
            .map(|sender| duration_nanos(sender.state().stats().max_lateness()))
            .max()
            .unwrap_or(0);
        let measurement_pss_kib = process_pss_kib();
        let p50_lateness = percentile(&mut lateness, 500);
        let p95_lateness = percentile(&mut lateness, 950);
        let p99_lateness = percentile(&mut lateness, 990);
        let p999_lateness = percentile(&mut lateness, 999);
        let p50_interval_error = percentile(&mut interval_error, 500);
        let p95_interval_error = percentile(&mut interval_error, 950);
        let p99_interval_error = percentile(&mut interval_error, 990);
        let p999_interval_error = percentile(&mut interval_error, 999);

        let churn_started = std::time::Instant::now();
        for _ in 0..churn_cycles {
            let (source, handle) = BenchmarkSource::pair(false);
            handle.activate();
            senders[0]
                .replace_source(source)
                .await
                .expect("active source churn remains responsive");
        }
        let churn_millis = churn_started.elapsed().as_millis();

        let result = serde_json::json!({
            "schemaVersion": 1,
            "benchmarkId": "oto-p07-complete-non-dave",
            "profile": "release",
            "senders": sender_count,
            "oneSlowSource": true,
            "warmupMs": warmup.as_millis(),
            "measurementMs": measured_elapsed.as_millis(),
            "connectBatchMs": connect_millis,
            "synchronizedStartMs": synchronized_start_millis,
            "churn": {"cycles": churn_cycles, "elapsedMs": churn_millis},
            "memoryKiB": {
                "baselinePss": baseline_pss_kib,
                "idlePss": idle_pss_kib,
                "pendingPss": pending_pss_kib,
                "activePss": active_pss_kib,
                "measurementEndPss": measurement_pss_kib,
                "idleIncrementPerConnection": idle_pss_kib.saturating_sub(baseline_pss_kib) / sender_count,
                "pendingIncrementPerSender": pending_pss_kib.saturating_sub(idle_pss_kib) / sender_count,
                "activeIncrementPerSender": active_pss_kib.saturating_sub(pending_pss_kib) / sender_count
            },
            "tasks": {
                "baseline": base_tasks,
                "idle": idle_tasks,
                "pending": pending_tasks,
                "active": active_tasks,
                "pacerCoordinators": 4
            },
            "threads": {
                "baseline": baseline_threads,
                "idle": idle_threads,
                "pending": pending_threads,
                "active": active_threads
            },
            "cpu": {
                "processTicks": cpu_ticks,
                "clockTicksPerSecond": 100,
                "percentOfOneLogicalCore": cpu_ticks as f64 / 100.0 / measured_elapsed.as_secs_f64() * 100.0
            },
            "frames": {
                "senderCounted": sent_frames,
                "udpPeerReceived": udp_packets,
                "measurementBoundaryDifference": sent_frames.abs_diff(udp_packets),
                "deliveryRatio": udp_packets as f64 / sent_frames.max(1) as f64,
                "portableUdpSendCallsPerFrame": 1
            },
            "udpSyscalls": {
                "exactKernelCount": null,
                "measurementStatus": "UNAVAILABLE_NO_SYSCALL_TRACER",
                "sourceObservedSendApiCallsPerFrame": 1
            },
            "allocation": {
                "allocations": allocation.allocations,
                "reallocations": allocation.reallocations,
                "bytesAllocated": allocation.bytes_allocated,
                "observedFrames": allocation_frames,
                "allocationsPerFrame": allocation.allocations as f64 / allocation_frames.max(1) as f64
            },
            "timingNanos": {
                "p50Lateness": p50_lateness,
                "p95Lateness": p95_lateness,
                "p99Lateness": p99_lateness,
                "p999Lateness": p999_lateness,
                "p50IntervalError": p50_interval_error,
                "p95IntervalError": p95_interval_error,
                "p99IntervalError": p99_interval_error,
                "p999IntervalError": p999_interval_error,
                "maxSenderObservedLateness": max_sender_lateness_nanos
            }
        });
        println!("P07_BENCHMARK={result}");

        if sender_count == 1 {
            assert_eq!(allocation.allocations, 0, "steady normal frames allocate");
        } else {
            assert!(
                allocation.allocations <= allocation_frames as usize / 1_000,
                "concurrent process allocation noise exceeded the calibrated harness bound"
            );
        }
        assert_eq!(
            allocation.reallocations, 0,
            "steady normal frames reallocate"
        );
        assert!(udp_packets > 0);
        if sender_count <= 500 {
            assert!(
                sent_frames.abs_diff(udp_packets) <= (sender_count as u64).saturating_mul(2),
                "measurement-boundary in-flight UDP packets stay bounded by two per sender"
            );
        }
        if sender_count == 250 {
            assert!(p99_interval_error <= 2_000_000, "p99 interval target");
            assert!(p999_interval_error <= 5_000_000, "p99.9 interval target");
        }

        futures_util::future::join_all(senders.iter().map(PacedAudioSender::stop)).await;
        futures_util::future::join_all(connections.iter().map(VoiceConnection::shutdown)).await;
        for gateway in gateways {
            gateway
                .shutdown()
                .await
                .expect("benchmark gateway shuts down");
        }
        peer.shutdown().await;
    }

    fn benchmark_env(name: &str, default: usize) -> usize {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    }

    fn process_pss_kib() -> usize {
        std::fs::read_to_string("/proc/self/smaps_rollup")
            .expect("Linux smaps_rollup is readable")
            .lines()
            .find_map(|line| {
                line.strip_prefix("Pss:")?
                    .split_whitespace()
                    .next()?
                    .parse()
                    .ok()
            })
            .expect("PSS is present")
    }

    fn process_cpu_ticks() -> u64 {
        let stat = std::fs::read_to_string("/proc/self/stat").expect("Linux proc stat is readable");
        let fields: Vec<&str> = stat[stat.rfind(')').expect("process name terminates") + 1..]
            .split_whitespace()
            .collect();
        fields[11].parse::<u64>().expect("user ticks")
            + fields[12].parse::<u64>().expect("system ticks")
    }

    fn process_thread_count() -> usize {
        std::fs::read_to_string("/proc/self/status")
            .expect("Linux proc status is readable")
            .lines()
            .find_map(|line| line.strip_prefix("Threads:")?.trim().parse().ok())
            .expect("thread count is present")
    }

    fn percentile(samples: &mut [u64], permille: usize) -> u64 {
        if samples.is_empty() {
            return 0;
        }
        samples.sort_unstable();
        let index = (samples.len() - 1).saturating_mul(permille) / 1_000;
        samples[index]
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
        eventually(|| gateway.udp.capture().len() >= 2).await;

        let third = connection
            .replace_voice_info(voice_info(&gateway, "generation-three", "token-three"))
            .await
            .expect("third generation supersedes discovery");
        assert_eq!(third.get(), 3);
        eventually(|| {
            connection.state().generation() == third
                && connection.state().phase() == ConnectionPhase::Connected
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
        assert_eq!(connection.state().phase(), ConnectionPhase::Connected);

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
        // The reconnect performs real TCP/TLS I/O. Resume wall-clock time after
        // driving the heartbeat timeout so the OS reactor is not starved by an
        // always-runnable virtual-time polling loop in optimized tests.
        tokio::time::resume();
        eventually(|| {
            gateway
                .records()
                .iter()
                .any(|record| matches!(record, GatewayRecord::Resume { .. }))
        })
        .await;
        eventually(|| connection.state().stats().resume_successes() == 1).await;
        eventually(|| {
            gateway
                .records()
                .iter()
                .filter(|record| matches!(record, GatewayRecord::Heartbeat { .. }))
                .count()
                >= 2
        })
        .await;
        eventually(|| connection.state().gateway_rtt().is_some()).await;
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
    #[ignore = "release-only simulated 24-hour connected lifecycle soak"]
    async fn p14_simulated_24_hour_connected_lifecycle_soak() {
        const SIMULATED_HOURS: u64 = 24;
        const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
        const HEARTBEAT_CYCLES: u64 = SIMULATED_HOURS * 60 * 60 / 10;

        let mut config = FakeVoiceGatewayConfig::local();
        config.dave_protocol_version = 1;
        config.heartbeat_interval = HEARTBEAT_INTERVAL;
        config.capture_capacity = HEARTBEAT_CYCLES as usize + 128;
        let gateway = TestGateway::start(config).await.expect("gateway starts");
        let oto = test_oto(&gateway, ResourceLimits::default());
        oto.config
            .ready_dave_fixture
            .store(true, std::sync::atomic::Ordering::Release);
        let connection = oto
            .connect(voice_info(&gateway, "p14-lifecycle-soak", "token"))
            .await
            .expect("transport reaches DAVE setup");
        gateway
            .try_dave_prepare_transition(1, 0)
            .expect("initial DAVE transition queues");
        eventually(|| connection.state().phase() == ConnectionPhase::Connected).await;

        let (source, source_handle) = QueueSource::pair();
        let sender = connection
            .start_audio(source)
            .await
            .expect("Pending source attaches");
        eventually(|| {
            source_handle.polls() > 0 && sender.state().phase() == AudioPhase::WaitingForSource
        })
        .await;
        let polls_before = source_handle.polls();

        connection
            .ping()
            .await
            .expect("initial heartbeat is acknowledged before time simulation");
        tokio::time::pause();
        for cycle in 0..HEARTBEAT_CYCLES {
            tokio::time::advance(HEARTBEAT_INTERVAL).await;
            // Advancing the clock makes the heartbeat deadline ready, but the
            // fake peer still completes real loopback TLS/WebSocket I/O. Let
            // the reactor use wall time while waiting for that cycle's
            // acknowledgement; otherwise Tokio's paused current-thread clock
            // can auto-advance to another heartbeat deadline before loopback
            // I/O becomes readable.
            tokio::time::resume();
            connection.ping().await.unwrap_or_else(|error| {
                panic!(
                    "simulated heartbeat cycle {cycle} failed: {error:?}; state={:?}",
                    connection.state()
                )
            });
            tokio::time::pause();
            if cycle % 360 == 0 {
                let state = connection.state();
                assert_eq!(state.phase(), ConnectionPhase::Connected);
                assert!(state.failure().is_none());
            }
        }
        tokio::time::resume();

        let state = connection.state();
        assert_eq!(state.phase(), ConnectionPhase::Connected);
        assert!(state.failure().is_none());
        assert_eq!(state.stats().heartbeat_timeouts(), 0);
        assert_eq!(source_handle.polls(), polls_before);
        assert_eq!(sender.state().stats().frames_sent(), 0);
        assert_eq!(sender.state().stats().silence_frames_sent(), 0);
        let heartbeat_records = gateway
            .records()
            .iter()
            .filter(|record| matches!(record, GatewayRecord::Heartbeat { .. }))
            .count();
        assert!(heartbeat_records >= HEARTBEAT_CYCLES as usize);

        println!(
            "P14_CONNECTED_LIFECYCLE_SOAK={}",
            json!({
                "classification": "simulated connected lifecycle; not wall-clock or live Discord evidence",
                "simulatedHours": SIMULATED_HOURS,
                "heartbeatCycles": HEARTBEAT_CYCLES,
                "heartbeatTimeouts": state.stats().heartbeat_timeouts(),
                "pendingSourcePollsDuringSoak": source_handle.polls() - polls_before,
                "phase": format!("{:?}", state.phase()),
            })
        );

        sender.stop().await.expect("Pending sender stops");
        connection.shutdown().await.expect("connection shuts down");
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
        assert_eq!(connection.state().phase(), ConnectionPhase::Connected);
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
                && connection.state().phase() == ConnectionPhase::Connected
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
        let text_limit = ResourceLimits::default().gateway_text_bytes();
        let oto = test_oto(
            &text_gateway,
            ResourceLimits::default().with_gateway_text_bytes(text_limit),
        );
        let text_connection = oto
            .connect(voice_info(&text_gateway, "text-limits", "token"))
            .await
            .expect("text gateway connects");
        let empty_payload = serde_json::to_string(&json!({
            "op": 250,
            "d": {"pad": ""},
            "seq": 1,
        }))
        .expect("test JSON serializes");
        let exact_padding = text_limit
            .checked_sub(empty_payload.len())
            .expect("production text bound fits the test envelope");
        assert_eq!(
            serde_json::to_string(&json!({
                "op": 250,
                "d": {"pad": "x".repeat(exact_padding)},
                "seq": 1,
            }))
            .expect("exact-bound test JSON serializes")
            .len(),
            text_limit,
        );
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
            .with_dave_binary_body_bytes(5);
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
    async fn dave_roster_limit_accepts_edge_and_rejects_one_member_over() {
        let gateway = TestGateway::start(FakeVoiceGatewayConfig::local())
            .await
            .expect("gateway starts");
        let oto = test_oto(
            &gateway,
            ResourceLimits::default().with_dave_roster_members(2),
        );
        let connection = oto
            .connect(voice_info(&gateway, "roster-limits", "token"))
            .await
            .expect("gateway connects");

        gateway
            .try_dispatch_json(11, json!({"user_ids": ["1", "2"]}), true)
            .expect("exact roster queues");
        gateway
            .try_dispatch_json(250, json!({"barrier": "exact-roster"}), true)
            .expect("ordered barrier queues");
        eventually(|| connection.state().stats().unknown_opcodes() >= 1).await;
        assert_eq!(connection.state().phase(), ConnectionPhase::Connected);

        gateway
            .try_dispatch_json(11, json!({"user_ids": ["1", "2", "3"]}), true)
            .expect("one-over roster queues");
        eventually(|| connection.state().phase() == ConnectionPhase::Failed).await;
        assert_eq!(
            connection
                .state()
                .failure()
                .expect("roster limit failure persists")
                .kind(),
            ErrorKind::ResourceLimit
        );

        gateway.shutdown().await.expect("gateway shuts down");
    }

    #[tokio::test]
    async fn pending_ping_waiters_are_bounded_and_shutdown_is_typed() {
        let mut gateway_config = FakeVoiceGatewayConfig::local();
        gateway_config.heartbeat_interval = Duration::from_secs(300);
        gateway_config.drop_heartbeat_acks = 1;
        let gateway = TestGateway::start(gateway_config)
            .await
            .expect("gateway starts");
        let limits = ResourceLimits::default().with_gateway_command_capacity(4);
        let oto = test_oto(&gateway, limits);
        let connection = oto
            .connect(voice_info(&gateway, "pending-pings", "token"))
            .await
            .expect("gateway connects with its initial heartbeat outstanding");

        let barrier = Arc::new(tokio::sync::Barrier::new(9));
        let mut pings = Vec::new();
        for _ in 0..8 {
            let connection = connection.clone();
            let barrier = barrier.clone();
            pings.push(tokio::spawn(async move {
                barrier.wait().await;
                connection.ping().await
            }));
        }
        barrier.wait().await;
        eventually(|| pings.iter().filter(|ping| ping.is_finished()).count() == 4).await;

        let final_state = connection
            .shutdown()
            .await
            .expect("shutdown releases admitted ping waiters");
        assert_eq!(final_state.phase(), ConnectionPhase::Closed);

        let mut overloaded = 0;
        let mut shutdown = 0;
        for ping in pings {
            let error = ping
                .await
                .expect("ping task joins")
                .expect_err("the dropped ACK leaves every test ping unresolved");
            match error.kind() {
                ErrorKind::Overloaded => overloaded += 1,
                ErrorKind::Shutdown => shutdown += 1,
                kind => panic!("unexpected pending-ping outcome: {kind:?}"),
            }
        }
        assert_eq!(overloaded, 4, "excess ping waiters fail explicitly");
        assert_eq!(shutdown, 4, "admitted ping waiters observe shutdown");
        gateway.shutdown().await.expect("gateway shuts down");
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
            .resource_limits(ResourceLimits::default().with_dave_roster_members(0))
            .build()
            .expect_err("zero DAVE roster capacity is rejected");
        assert_eq!(error.kind(), ErrorKind::InvalidConfiguration);

        let error = Oto::builder()
            .resource_limits(
                ResourceLimits::default()
                    .with_gateway_binary_bytes(8)
                    .with_dave_binary_body_bytes(6),
            )
            .build()
            .expect_err("DAVE body and envelope must fit the gateway binary bound");
        assert_eq!(error.kind(), ErrorKind::InvalidConfiguration);

        let error = Oto::builder()
            .resource_limits(
                ResourceLimits::default().with_udp_datagram_bytes(usize::from(u16::MAX) + 1),
            )
            .build()
            .expect_err("impossible UDP datagram bound is rejected");
        assert_eq!(error.kind(), ErrorKind::InvalidConfiguration);

        let error = Oto::builder()
            .resource_limits(
                ResourceLimits::default()
                    .with_encoded_opus_frame_bytes(1_275)
                    .with_udp_datagram_bytes(1_275 + 32),
            )
            .build()
            .expect_err("datagram bound must include DAVE expansion before transport AEAD");
        assert_eq!(error.kind(), ErrorKind::InvalidConfiguration);

        let token = VoiceToken::new("never-print-this");
        assert!(!format!("{token:?}").contains("never-print-this"));

        let info = VoiceConnectInfo::new(
            1,
            2,
            3,
            "never-print-this-session",
            "voice.example.test",
            VoiceToken::new("never-print-this-token"),
        );
        let debug = format!("{info:?}");
        assert!(!debug.contains("never-print-this-session"));
        assert!(!debug.contains("never-print-this-token"));
        assert_eq!(debug.matches("[REDACTED]").count(), 2);

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
