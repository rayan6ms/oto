use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_rustls::{TlsAcceptor, server::TlsStream};
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Message, WebSocketConfig};
use tokio_tungstenite::{WebSocketStream, accept_async_with_config};

#[derive(Clone)]
pub struct TestTls {
    server_config: Arc<ServerConfig>,
    client_config: Arc<ClientConfig>,
    certificate: CertificateDer<'static>,
}

impl std::fmt::Debug for TestTls {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TestTls")
            .field("certificate_bytes", &self.certificate.len())
            .finish_non_exhaustive()
    }
}

impl TestTls {
    pub fn generate() -> Result<Self, GatewayError> {
        let CertifiedKey { cert, signing_key } =
            generate_simple_self_signed(vec!["localhost".to_owned()])?;
        let certificate = cert.der().clone();
        let private_key =
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(signing_key.serialize_der()));
        let server_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![certificate.clone()], private_key)?;

        let mut roots = RootCertStore::empty();
        roots.add(certificate.clone())?;
        let client_config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();

        Ok(Self {
            server_config: Arc::new(server_config),
            client_config: Arc::new(client_config),
            certificate,
        })
    }

    #[must_use]
    pub fn client_config(&self) -> Arc<ClientConfig> {
        self.client_config.clone()
    }

    #[must_use]
    pub fn certificate(&self) -> CertificateDer<'static> {
        self.certificate.clone()
    }
}

#[derive(Clone)]
pub struct FakeVoiceGatewayConfig {
    pub heartbeat_interval: Duration,
    pub ssrc: u32,
    pub voice_ip: String,
    pub voice_port: u16,
    pub modes: Vec<String>,
    pub secret_key: [u8; 32],
    pub dave_protocol_version: u16,
    pub max_message_bytes: usize,
    pub command_capacity: usize,
    pub capture_capacity: usize,
    pub speaking_capacity: usize,
    pub replay_capacity: usize,
    pub sequence_start: u16,
    pub sequence_modulus: u32,
    /// Number of heartbeat acknowledgements to drop across all connections.
    pub drop_heartbeat_acks: usize,
    /// Deterministic delay applied before each non-dropped heartbeat ACK.
    pub heartbeat_ack_delay: Duration,
    /// Optional close injected at a precise handshake stage on every attempt.
    pub scripted_close: Option<ScriptedClose>,
    /// Deterministic delay between WebSocket upgrade and Hello.
    pub hello_delay: Duration,
}

impl std::fmt::Debug for FakeVoiceGatewayConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FakeVoiceGatewayConfig")
            .field("heartbeat_interval", &self.heartbeat_interval)
            .field("ssrc", &self.ssrc)
            .field("voice_ip", &self.voice_ip)
            .field("voice_port", &self.voice_port)
            .field("modes", &self.modes)
            .field("secret_key", &"[REDACTED]")
            .field("dave_protocol_version", &self.dave_protocol_version)
            .field("max_message_bytes", &self.max_message_bytes)
            .field("command_capacity", &self.command_capacity)
            .field("capture_capacity", &self.capture_capacity)
            .field("speaking_capacity", &self.speaking_capacity)
            .field("replay_capacity", &self.replay_capacity)
            .field("sequence_start", &self.sequence_start)
            .field("sequence_modulus", &self.sequence_modulus)
            .field("drop_heartbeat_acks", &self.drop_heartbeat_acks)
            .field("heartbeat_ack_delay", &self.heartbeat_ack_delay)
            .field("scripted_close", &self.scripted_close)
            .field("hello_delay", &self.hello_delay)
            .finish()
    }
}

impl FakeVoiceGatewayConfig {
    #[must_use]
    pub fn local() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(10),
            ssrc: 0x1122_3344,
            voice_ip: "127.0.0.1".to_owned(),
            voice_port: 50_000,
            modes: vec![
                "aead_aes256_gcm_rtpsize".to_owned(),
                "aead_xchacha20_poly1305_rtpsize".to_owned(),
            ],
            secret_key: [0x42; 32],
            dave_protocol_version: 0,
            max_message_bytes: 1_280_000,
            command_capacity: 32,
            capture_capacity: 128,
            speaking_capacity: 32,
            replay_capacity: 64,
            sequence_start: 0,
            sequence_modulus: 65_536,
            drop_heartbeat_acks: 0,
            heartbeat_ack_delay: Duration::ZERO,
            scripted_close: None,
            hello_delay: Duration::ZERO,
        }
    }

    fn validate(&self) -> Result<(), GatewayError> {
        if self.heartbeat_interval.is_zero()
            || self.voice_port == 0
            || self.modes.is_empty()
            || self.max_message_bytes < 3
            || self.command_capacity == 0
            || self.capture_capacity == 0
            || self.speaking_capacity == 0
            || self.replay_capacity == 0
            || self.sequence_modulus == 0
            || self.sequence_modulus > 65_536
            || self.replay_capacity as u32 >= self.sequence_modulus
            || u32::from(self.sequence_start) >= self.sequence_modulus
        {
            return Err(GatewayError::InvalidConfig);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GatewayCloseStage {
    BeforeHello,
    AfterHello,
    AfterIdentify,
    AfterReady,
    OnResume,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScriptedClose {
    pub stage: GatewayCloseStage,
    pub close: VoiceClose,
}

#[derive(Clone, Debug, PartialEq)]
pub enum GatewayRecord {
    Identify(Value),
    SelectProtocol(Value),
    Heartbeat { nonce: Value, seq_ack: Option<i64> },
    Resume { seq_ack: i64 },
    Speaking(Value),
    Binary(Vec<u8>),
    OtherText(Value),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VoiceClose {
    pub code: u16,
    pub reason: String,
}

#[derive(Debug, Error)]
pub enum GatewayError {
    #[error("invalid fake Voice Gateway configuration")]
    InvalidConfig,
    #[error("gateway I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("gateway TLS certificate generation failed: {0}")]
    Certificate(#[from] rcgen::Error),
    #[error("gateway TLS configuration failed: {0}")]
    Tls(#[from] rustls::Error),
    #[error("WebSocket failed: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("gateway capture is full at capacity {capacity}")]
    CaptureFull { capacity: usize },
    #[error("gateway speaking capture is full at capacity {capacity}")]
    SpeakingFull { capacity: usize },
    #[error("gateway task failed to join: {0}")]
    Join(#[from] tokio::task::JoinError),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum GatewayCommandError {
    #[error("gateway command queue is full at capacity {capacity}")]
    Full { capacity: usize },
    #[error("gateway command queue is closed")]
    Closed,
    #[error("binary message has {actual} bytes, exceeding maximum {maximum}")]
    MessageTooLarge { actual: usize, maximum: usize },
}

#[derive(Debug)]
enum GatewayCommand {
    Json {
        opcode: u8,
        data: Value,
        numbered: bool,
    },
    Binary {
        opcode: u8,
        payload: Vec<u8>,
    },
    BufferJson {
        opcode: u8,
        data: Value,
    },
    Close(VoiceClose),
}

#[derive(Debug)]
struct BufferedMessage {
    sequence: u16,
    message: Message,
}

#[derive(Debug)]
struct GatewayProtocolState {
    sequence: u16,
    replay: VecDeque<BufferedMessage>,
    remaining_dropped_acks: usize,
}

#[derive(Debug)]
struct GatewayState {
    records: VecDeque<GatewayRecord>,
    speaking: VecDeque<Value>,
    capture_capacity: usize,
    speaking_capacity: usize,
}

impl GatewayState {
    fn record(&mut self, record: GatewayRecord) -> Result<(), GatewayError> {
        if self.records.len() == self.capture_capacity {
            return Err(GatewayError::CaptureFull {
                capacity: self.capture_capacity,
            });
        }
        self.records.push_back(record);
        Ok(())
    }

    fn record_speaking(&mut self, value: Value) -> Result<(), GatewayError> {
        if self.speaking.len() == self.speaking_capacity {
            return Err(GatewayError::SpeakingFull {
                capacity: self.speaking_capacity,
            });
        }
        if self.records.len() == self.capture_capacity {
            return Err(GatewayError::CaptureFull {
                capacity: self.capture_capacity,
            });
        }
        self.speaking.push_back(value.clone());
        self.records.push_back(GatewayRecord::Speaking(value));
        Ok(())
    }
}

pub struct FakeVoiceGateway {
    local_addr: SocketAddr,
    tls: TestTls,
    commands: mpsc::Sender<GatewayCommand>,
    command_capacity: usize,
    max_message_bytes: usize,
    state: Arc<Mutex<GatewayState>>,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<Result<(), GatewayError>>,
}

impl std::fmt::Debug for FakeVoiceGateway {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FakeVoiceGateway")
            .field("local_addr", &self.local_addr)
            .field("max_message_bytes", &self.max_message_bytes)
            .finish_non_exhaustive()
    }
}

impl FakeVoiceGateway {
    pub async fn start(config: FakeVoiceGatewayConfig) -> Result<Self, GatewayError> {
        let tls = TestTls::generate()?;
        Self::start_with_tls(config, tls).await
    }

    /// Starts a gateway with a caller-supplied test certificate.
    ///
    /// Sharing one [`TestTls`] across several peers keeps multi-connection
    /// reference benchmarks on one explicitly injected trust root.
    pub async fn start_with_tls(
        config: FakeVoiceGatewayConfig,
        tls: TestTls,
    ) -> Result<Self, GatewayError> {
        config.validate()?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let local_addr = listener.local_addr()?;
        let (commands, command_rx) = mpsc::channel(config.command_capacity);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let state = Arc::new(Mutex::new(GatewayState {
            records: VecDeque::with_capacity(config.capture_capacity),
            speaking: VecDeque::with_capacity(config.speaking_capacity),
            capture_capacity: config.capture_capacity,
            speaking_capacity: config.speaking_capacity,
        }));
        let task = tokio::spawn(run_gateway(
            listener,
            TlsAcceptor::from(tls.server_config.clone()),
            config.clone(),
            command_rx,
            shutdown_rx,
            state.clone(),
        ));

        Ok(Self {
            local_addr,
            tls,
            commands,
            command_capacity: config.command_capacity,
            max_message_bytes: config.max_message_bytes,
            state,
            shutdown,
            task,
        })
    }

    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    #[must_use]
    pub fn url(&self) -> String {
        format!("wss://localhost:{}/?v=8", self.local_addr.port())
    }

    #[must_use]
    pub fn tls(&self) -> TestTls {
        self.tls.clone()
    }

    #[must_use]
    pub fn records(&self) -> Vec<GatewayRecord> {
        self.state
            .lock()
            .expect("fake gateway state mutex poisoned")
            .records
            .iter()
            .cloned()
            .collect()
    }

    #[must_use]
    pub fn speaking(&self) -> Vec<Value> {
        self.state
            .lock()
            .expect("fake gateway state mutex poisoned")
            .speaking
            .iter()
            .cloned()
            .collect()
    }

    pub fn try_dispatch_json(
        &self,
        opcode: u8,
        data: Value,
        numbered: bool,
    ) -> Result<(), GatewayCommandError> {
        self.try_command(GatewayCommand::Json {
            opcode,
            data,
            numbered,
        })
    }

    pub fn try_dispatch_binary(
        &self,
        opcode: u8,
        payload: Vec<u8>,
    ) -> Result<(), GatewayCommandError> {
        let actual = 3 + payload.len();
        if actual > self.max_message_bytes {
            return Err(GatewayCommandError::MessageTooLarge {
                actual,
                maximum: self.max_message_bytes,
            });
        }
        self.try_command(GatewayCommand::Binary { opcode, payload })
    }

    /// Adds a numbered server message to Resume history without delivering it
    /// on the current connection, modeling a message lost at interruption.
    pub fn try_buffer_json(&self, opcode: u8, data: Value) -> Result<(), GatewayCommandError> {
        self.try_command(GatewayCommand::BufferJson { opcode, data })
    }

    pub fn try_close(&self, close: VoiceClose) -> Result<(), GatewayCommandError> {
        self.try_command(GatewayCommand::Close(close))
    }

    fn try_command(&self, command: GatewayCommand) -> Result<(), GatewayCommandError> {
        self.commands
            .try_send(command)
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => GatewayCommandError::Full {
                    capacity: self.command_capacity,
                },
                mpsc::error::TrySendError::Closed(_) => GatewayCommandError::Closed,
            })
    }

    pub async fn shutdown(self) -> Result<(), GatewayError> {
        self.shutdown.send_replace(true);
        self.task.await??;
        Ok(())
    }
}

async fn run_gateway(
    listener: TcpListener,
    tls_acceptor: TlsAcceptor,
    config: FakeVoiceGatewayConfig,
    mut commands: mpsc::Receiver<GatewayCommand>,
    mut shutdown: watch::Receiver<bool>,
    state: Arc<Mutex<GatewayState>>,
) -> Result<(), GatewayError> {
    let mut protocol = GatewayProtocolState {
        sequence: config.sequence_start,
        replay: VecDeque::with_capacity(config.replay_capacity),
        remaining_dropped_acks: config.drop_heartbeat_acks,
    };

    loop {
        let accepted = tokio::select! {
            result = shutdown.changed() => {
                if result.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
                continue;
            }
            accepted = listener.accept() => accepted?,
        };
        let (tcp, _) = accepted;
        let tls = tokio::select! {
            result = shutdown.changed() => {
                if result.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
                continue;
            }
            result = tls_acceptor.accept(tcp) => match result {
                Ok(tls) => tls,
                Err(_) => continue,
            }
        };
        let websocket_config = WebSocketConfig::default()
            .max_message_size(Some(config.max_message_bytes))
            .max_frame_size(Some(config.max_message_bytes));
        let mut websocket = tokio::select! {
            result = shutdown.changed() => {
                if result.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
                continue;
            }
            result = accept_async_with_config(tls, Some(websocket_config)) => match result {
                Ok(websocket) => websocket,
                Err(_) => continue,
            }
        };
        if !config.hello_delay.is_zero() {
            tokio::select! {
                result = shutdown.changed() => {
                    if result.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                    continue;
                }
                () = tokio::time::sleep(config.hello_delay) => {}
            }
        }
        if close_at_stage(&config, GatewayCloseStage::BeforeHello, &mut websocket).await? {
            continue;
        }
        if send_json(
            &mut websocket,
            json!({
                "op": 8,
                "d": {"heartbeat_interval": config.heartbeat_interval.as_millis() as u64}
            }),
        )
        .await
        .is_err()
        {
            continue;
        }
        let end = run_connection(
            &mut websocket,
            &config,
            &mut commands,
            &mut shutdown,
            &state,
            &mut protocol,
        )
        .await?;
        if end == ConnectionEnd::Shutdown {
            return Ok(());
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConnectionEnd {
    Disconnected,
    Shutdown,
}

type ServerWebSocket = WebSocketStream<TlsStream<TcpStream>>;

async fn run_connection(
    websocket: &mut ServerWebSocket,
    config: &FakeVoiceGatewayConfig,
    commands: &mut mpsc::Receiver<GatewayCommand>,
    shutdown: &mut watch::Receiver<bool>,
    state: &Arc<Mutex<GatewayState>>,
    protocol: &mut GatewayProtocolState,
) -> Result<ConnectionEnd, GatewayError> {
    loop {
        tokio::select! {
            result = shutdown.changed() => {
                if result.is_err() || *shutdown.borrow() {
                    let _ = websocket.close(None).await;
                    return Ok(ConnectionEnd::Shutdown);
                }
            }
            command = commands.recv() => {
                let Some(command) = command else {
                    return Ok(ConnectionEnd::Shutdown);
                };
                match command {
                    GatewayCommand::Json { opcode, data, numbered } => {
                        if numbered {
                            send_numbered_json(
                                websocket,
                                config,
                                &mut protocol.sequence,
                                &mut protocol.replay,
                                opcode,
                                data,
                            ).await?;
                        } else {
                            send_json(websocket, json!({"op": opcode, "d": data})).await?;
                        }
                    }
                    GatewayCommand::Binary { opcode, payload } => {
                        send_numbered_binary(
                            websocket,
                            config,
                            &mut protocol.sequence,
                            &mut protocol.replay,
                            opcode,
                            payload,
                        ).await?;
                    }
                    GatewayCommand::BufferJson { opcode, data } => {
                        buffer_numbered_json(
                            config,
                            &mut protocol.sequence,
                            &mut protocol.replay,
                            opcode,
                            data,
                        )?;
                    }
                    GatewayCommand::Close(close) => {
                        websocket.send(Message::Close(Some(CloseFrame {
                            code: close.code.into(),
                            reason: close.reason.into(),
                        }))).await?;
                        return Ok(ConnectionEnd::Disconnected);
                    }
                }
            }
            message = websocket.next() => {
                let Some(message) = message else {
                    return Ok(ConnectionEnd::Disconnected);
                };
                let message = match message {
                    Ok(message) => message,
                    Err(tokio_tungstenite::tungstenite::Error::ConnectionClosed
                        | tokio_tungstenite::tungstenite::Error::AlreadyClosed
                        | tokio_tungstenite::tungstenite::Error::Capacity(_)
                        | tokio_tungstenite::tungstenite::Error::Protocol(
                            tokio_tungstenite::tungstenite::error::ProtocolError::ResetWithoutClosingHandshake
                        )) => {
                        return Ok(ConnectionEnd::Disconnected);
                    }
                    // A peer may disappear without a WebSocket close handshake during
                    // reconnect fault tests. Keep the listener alive for the next
                    // deterministic attempt regardless of the dependency-specific
                    // disconnect discriminant.
                    Err(_) => return Ok(ConnectionEnd::Disconnected),
                };
                if config
                    .scripted_close
                    .as_ref()
                    .is_some_and(|scripted| scripted.stage == GatewayCloseStage::AfterHello)
                {
                    close_at_stage(config, GatewayCloseStage::AfterHello, websocket).await?;
                    return Ok(ConnectionEnd::Disconnected);
                }
                match message {
                    Message::Text(text) => {
                        if text.len() > config.max_message_bytes {
                            close_decode_error(websocket).await;
                            return Ok(ConnectionEnd::Disconnected);
                        }
                        let value: Value = match serde_json::from_str(&text) {
                            Ok(value) => value,
                            Err(_) => {
                                close_decode_error(websocket).await;
                                return Ok(ConnectionEnd::Disconnected);
                            }
                        };
                        if !handle_text(
                            websocket,
                            config,
                            state,
                            protocol,
                            value,
                        ).await? {
                            return Ok(ConnectionEnd::Disconnected);
                        }
                    }
                    Message::Binary(bytes) => {
                        if bytes.is_empty() || bytes.len() > config.max_message_bytes {
                            close_decode_error(websocket).await;
                            return Ok(ConnectionEnd::Disconnected);
                        }
                        state.lock().expect("fake gateway state mutex poisoned")
                            .record(GatewayRecord::Binary(bytes.to_vec()))?;
                    }
                    Message::Close(_) => return Ok(ConnectionEnd::Disconnected),
                    Message::Ping(payload) => websocket.send(Message::Pong(payload)).await?,
                    Message::Pong(_) | Message::Frame(_) => {}
                }
            }
        }
    }
}

async fn handle_text(
    websocket: &mut ServerWebSocket,
    config: &FakeVoiceGatewayConfig,
    state: &Arc<Mutex<GatewayState>>,
    protocol: &mut GatewayProtocolState,
    value: Value,
) -> Result<bool, GatewayError> {
    let Some(opcode) = value.get("op").and_then(Value::as_u64) else {
        close_decode_error(websocket).await;
        return Ok(false);
    };
    let data = value.get("d").cloned().unwrap_or(Value::Null);
    match opcode {
        0 => {
            state
                .lock()
                .expect("fake gateway state mutex poisoned")
                .record(GatewayRecord::Identify(data))?;
            if close_at_stage(config, GatewayCloseStage::AfterIdentify, websocket).await? {
                return Ok(false);
            }
            send_numbered_json(
                websocket,
                config,
                &mut protocol.sequence,
                &mut protocol.replay,
                2,
                json!({
                    "ssrc": config.ssrc,
                    "ip": config.voice_ip,
                    "port": config.voice_port,
                    "modes": config.modes,
                }),
            )
            .await?;
            if close_at_stage(config, GatewayCloseStage::AfterReady, websocket).await? {
                return Ok(false);
            }
        }
        1 => {
            state
                .lock()
                .expect("fake gateway state mutex poisoned")
                .record(GatewayRecord::SelectProtocol(data.clone()))?;
            let Some(mode) = data
                .get("data")
                .and_then(|data| data.get("mode"))
                .and_then(Value::as_str)
                .filter(|mode| config.modes.iter().any(|offered| offered == mode))
            else {
                close_decode_error(websocket).await;
                return Ok(false);
            };
            send_numbered_json(
                websocket,
                config,
                &mut protocol.sequence,
                &mut protocol.replay,
                4,
                json!({
                    "mode": mode,
                    "secret_key": config.secret_key,
                    "dave_protocol_version": config.dave_protocol_version,
                }),
            )
            .await?;
        }
        3 => {
            let nonce = data.get("t").cloned().unwrap_or(Value::Null);
            let seq_ack = data.get("seq_ack").and_then(Value::as_i64);
            state
                .lock()
                .expect("fake gateway state mutex poisoned")
                .record(GatewayRecord::Heartbeat {
                    nonce: nonce.clone(),
                    seq_ack,
                })?;
            if protocol.remaining_dropped_acks > 0 {
                protocol.remaining_dropped_acks -= 1;
            } else {
                if !config.heartbeat_ack_delay.is_zero() {
                    tokio::time::sleep(config.heartbeat_ack_delay).await;
                }
                send_json(websocket, json!({"op": 6, "d": {"t": nonce}})).await?;
            }
        }
        5 => {
            state
                .lock()
                .expect("fake gateway state mutex poisoned")
                .record_speaking(data)?;
        }
        7 => {
            let seq_ack = data.get("seq_ack").and_then(Value::as_i64).unwrap_or(-1);
            state
                .lock()
                .expect("fake gateway state mutex poisoned")
                .record(GatewayRecord::Resume { seq_ack })?;
            if close_at_stage(config, GatewayCloseStage::OnResume, websocket).await? {
                return Ok(false);
            }
            if !replay_after(websocket, &protocol.replay, seq_ack).await? {
                return Ok(false);
            }
            send_json(websocket, json!({"op": 9, "d": {}})).await?;
        }
        _ => {
            state
                .lock()
                .expect("fake gateway state mutex poisoned")
                .record(GatewayRecord::OtherText(value))?;
        }
    }
    Ok(true)
}

async fn close_at_stage(
    config: &FakeVoiceGatewayConfig,
    stage: GatewayCloseStage,
    websocket: &mut ServerWebSocket,
) -> Result<bool, GatewayError> {
    let Some(scripted) = config.scripted_close.as_ref() else {
        return Ok(false);
    };
    if scripted.stage != stage {
        return Ok(false);
    }
    websocket
        .send(Message::Close(Some(CloseFrame {
            code: scripted.close.code.into(),
            reason: scripted.close.reason.clone().into(),
        })))
        .await?;
    Ok(true)
}

async fn replay_after(
    websocket: &mut ServerWebSocket,
    replay: &VecDeque<BufferedMessage>,
    seq_ack: i64,
) -> Result<bool, GatewayError> {
    let start = if seq_ack == -1 {
        0
    } else {
        let Some(position) = replay
            .iter()
            .rposition(|message| i64::from(message.sequence) == seq_ack)
        else {
            websocket
                .send(Message::Close(Some(CloseFrame {
                    code: 4006_u16.into(),
                    reason: "buffer no longer contains seq_ack".into(),
                })))
                .await?;
            return Ok(false);
        };
        position + 1
    };
    for message in replay.iter().skip(start) {
        websocket.send(message.message.clone()).await?;
    }
    Ok(true)
}

async fn send_numbered_json(
    websocket: &mut ServerWebSocket,
    config: &FakeVoiceGatewayConfig,
    sequence: &mut u16,
    replay: &mut VecDeque<BufferedMessage>,
    opcode: u8,
    data: Value,
) -> Result<(), GatewayError> {
    let current = take_sequence(sequence, config.sequence_modulus);
    let text = serde_json::to_string(&json!({"op": opcode, "d": data, "seq": current}))?;
    let message = Message::Text(text.into());
    buffer_message(config.replay_capacity, replay, current, message.clone());
    websocket.send(message).await?;
    Ok(())
}

fn buffer_numbered_json(
    config: &FakeVoiceGatewayConfig,
    sequence: &mut u16,
    replay: &mut VecDeque<BufferedMessage>,
    opcode: u8,
    data: Value,
) -> Result<(), GatewayError> {
    let current = take_sequence(sequence, config.sequence_modulus);
    let text = serde_json::to_string(&json!({"op": opcode, "d": data, "seq": current}))?;
    buffer_message(
        config.replay_capacity,
        replay,
        current,
        Message::Text(text.into()),
    );
    Ok(())
}

async fn send_numbered_binary(
    websocket: &mut ServerWebSocket,
    config: &FakeVoiceGatewayConfig,
    sequence: &mut u16,
    replay: &mut VecDeque<BufferedMessage>,
    opcode: u8,
    payload: Vec<u8>,
) -> Result<(), GatewayError> {
    let current = take_sequence(sequence, config.sequence_modulus);
    let mut bytes = Vec::with_capacity(3 + payload.len());
    bytes.extend_from_slice(&current.to_be_bytes());
    bytes.push(opcode);
    bytes.extend_from_slice(&payload);
    let message = Message::Binary(bytes.into());
    buffer_message(config.replay_capacity, replay, current, message.clone());
    websocket.send(message).await?;
    Ok(())
}

fn buffer_message(
    capacity: usize,
    replay: &mut VecDeque<BufferedMessage>,
    sequence: u16,
    message: Message,
) {
    if replay.len() == capacity {
        replay.pop_front();
    }
    replay.push_back(BufferedMessage { sequence, message });
}

fn take_sequence(sequence: &mut u16, modulus: u32) -> u16 {
    let current = *sequence;
    *sequence = ((u32::from(current) + 1) % modulus) as u16;
    current
}

async fn send_json(websocket: &mut ServerWebSocket, value: Value) -> Result<(), GatewayError> {
    websocket
        .send(Message::Text(serde_json::to_string(&value)?.into()))
        .await?;
    Ok(())
}

async fn close_decode_error(websocket: &mut ServerWebSocket) {
    let _ = websocket
        .send(Message::Close(Some(CloseFrame {
            code: 4002_u16.into(),
            reason: "failed to decode payload".into(),
        })))
        .await;
}
