use std::collections::HashSet;
use std::num::NonZeroU16;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::timeout;

pub(crate) const MAX_PROTOCOL_VERSION: u16 = 1;
// DAVE v1 Opus encrypts the entire frame and appends an 8-byte tag, a
// worst-case 5-byte LEB128 nonce, one supplemental-length byte, and FA FA.
pub(crate) const OPUS_MAX_ENCRYPTION_OVERHEAD_BYTES: usize = 16;
const COMMAND_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Snapshot {
    pub(crate) active_version: u16,
    pub(crate) transition_id: Option<u16>,
    pub(crate) ready: bool,
}

use crate::DaveFailure as Failure;

#[derive(Debug)]
pub(crate) enum Outbound {
    Json { opcode: u8, data: Value },
    Binary(Vec<u8>),
}

#[derive(Debug)]
pub(crate) enum Control {
    PrepareTransition { protocol_version: u16, id: u16 },
    ExecuteTransition { id: u16 },
    PrepareEpoch { protocol_version: u16, epoch: u64 },
    ExternalSender(Vec<u8>),
    Proposals(Vec<u8>),
    Commit(Vec<u8>),
    Welcome(Vec<u8>),
    Roster(Vec<u64>),
    MemberDisconnected(u64),
}

impl Control {
    pub(crate) fn opcode(&self) -> u8 {
        match self {
            Self::Roster(_) => 11,
            Self::MemberDisconnected(_) => 13,
            Self::PrepareTransition { .. } => 21,
            Self::ExecuteTransition { .. } => 22,
            Self::PrepareEpoch { .. } => 24,
            Self::ExternalSender(_) => 25,
            Self::Proposals(_) => 27,
            Self::Commit(_) => 29,
            Self::Welcome(_) => 30,
        }
    }
}

#[derive(Clone)]
pub(crate) struct Handle {
    inner: Arc<Owner>,
}

struct Owner {
    commands: mpsc::Sender<Command>,
    state: watch::Receiver<Snapshot>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Drop for Owner {
    fn drop(&mut self) {
        if let Some(task) = self
            .task
            .get_mut()
            .expect("DAVE owner task mutex poisoned")
            .take()
        {
            task.abort();
        }
    }
}

impl std::fmt::Debug for Handle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DaveHandle")
            .field("state", &*self.inner.state.borrow())
            .finish_non_exhaustive()
    }
}

enum Command {
    #[cfg(test)]
    PrepareFixtureEpoch { reply: oneshot::Sender<()> },
    Control {
        control: Control,
        reply: oneshot::Sender<Result<Vec<Outbound>, Failure>>,
    },
    Encrypt {
        frame: Vec<u8>,
        len: usize,
        output: Vec<u8>,
        reply: mpsc::Sender<EncryptedBuffers>,
        measure: bool,
    },
}

pub(crate) struct EncryptedBuffers {
    pub(crate) frame: Vec<u8>,
    pub(crate) output: Vec<u8>,
    pub(crate) result: Result<MediaOutcome, Failure>,
    pub(crate) work_wall_micros: u64,
    pub(crate) work_cpu_micros: u64,
}

/// Readiness is decided in the same owner turn as encryption. A snapshot read
/// by the sender cannot exclude an epoch reset queued ahead of its frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MediaOutcome {
    Encrypted,
    NotReady,
}

pub(crate) struct MediaEncryptor {
    commands: mpsc::Sender<Command>,
    reply: mpsc::Sender<EncryptedBuffers>,
    responses: mpsc::Receiver<EncryptedBuffers>,
    pub(crate) measure: bool,
}

impl std::fmt::Debug for MediaEncryptor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("DaveMediaEncryptor").finish()
    }
}

impl Handle {
    pub(crate) fn spawn(user_id: u64, channel_id: u64, capacity: usize) -> Result<Self, Failure> {
        let core = Core::new(user_id, channel_id)?;
        Ok(Self::spawn_core(core, capacity))
    }

    fn spawn_core(core: Core, capacity: usize) -> Self {
        let (commands, mut receiver) = mpsc::channel(capacity);
        let (state_tx, state) = watch::channel(core.snapshot());
        let task = tokio::spawn(async move {
            let mut core = core;
            while let Some(command) = receiver.recv().await {
                match command {
                    #[cfg(test)]
                    Command::PrepareFixtureEpoch { reply } => {
                        // Models the fresh, authenticated MLS exchange after an
                        // epoch reset using the existing real crypto fixtures.
                        core = prepared_fixture_core();
                        state_tx.send_replace(core.snapshot());
                        let _ = reply.send(());
                    }
                    Command::Control { control, reply } => {
                        let result = core.control(control);
                        state_tx.send_replace(core.snapshot());
                        let _ = reply.send(result);
                    }
                    Command::Encrypt {
                        frame,
                        len,
                        mut output,
                        reply,
                        measure,
                    } => {
                        let wall = measure.then(std::time::Instant::now);
                        #[cfg(target_os = "linux")]
                        let cpu = measure.then(crate::audio::source_cpu_time);
                        let result = if core.snapshot().ready {
                            core.encrypt_into(&frame[..len], &mut output)
                                .map(|()| MediaOutcome::Encrypted)
                        } else {
                            output.clear();
                            Ok(MediaOutcome::NotReady)
                        };
                        #[cfg(target_os = "linux")]
                        let work_cpu_micros = cpu.map_or(0, |start| {
                            crate::send_trace::micros(
                                crate::audio::source_cpu_time().saturating_sub(start),
                            )
                        });
                        #[cfg(not(target_os = "linux"))]
                        let work_cpu_micros = u64::MAX;
                        let work_wall_micros =
                            wall.map_or(0, |start| crate::send_trace::micros(start.elapsed()));
                        let _ = reply
                            .send(EncryptedBuffers {
                                frame,
                                output,
                                result,
                                work_wall_micros,
                                work_cpu_micros,
                            })
                            .await;
                    }
                }
            }
        });
        Self {
            inner: Arc::new(Owner {
                commands,
                state,
                task: Mutex::new(Some(task)),
            }),
        }
    }

    pub(crate) fn snapshot(&self) -> Snapshot {
        *self.inner.state.borrow()
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<Snapshot> {
        self.inner.state.clone()
    }

    pub(crate) fn media_encryptor(&self) -> MediaEncryptor {
        let (reply, responses) = mpsc::channel(1);
        MediaEncryptor {
            commands: self.inner.commands.clone(),
            reply,
            responses,
            measure: false,
        }
    }

    pub(crate) async fn control(&self, control: Control) -> Result<Vec<Outbound>, Failure> {
        let (reply, response) = oneshot::channel();
        timeout(
            COMMAND_TIMEOUT,
            self.inner
                .commands
                .send(Command::Control { control, reply }),
        )
        .await
        .map_err(|_| Failure::QueueTimeout)?
        .map_err(|_| Failure::Closed)?;
        timeout(COMMAND_TIMEOUT, response)
            .await
            .map_err(|_| Failure::ResponseTimeout)?
            .map_err(|_| Failure::Closed)?
    }
}

impl MediaEncryptor {
    pub(crate) async fn encrypt_buffered(
        &mut self,
        frame: Vec<u8>,
        len: usize,
        output: Vec<u8>,
    ) -> Result<EncryptedBuffers, Failure> {
        if len > frame.len() {
            return Err(Failure::Malformed);
        }
        timeout(
            COMMAND_TIMEOUT,
            self.commands.send(Command::Encrypt {
                frame,
                len,
                output,
                reply: self.reply.clone(),
                measure: self.measure,
            }),
        )
        .await
        .map_err(|_| Failure::QueueTimeout)?
        .map_err(|_| Failure::Closed)?;
        timeout(COMMAND_TIMEOUT, self.responses.recv())
            .await
            .map_err(|_| Failure::ResponseTimeout)?
            .ok_or(Failure::Closed)
    }
}

#[cfg(test)]
impl Handle {
    pub(crate) fn spawn_ready_fixture(capacity: usize) -> Self {
        Self::spawn_core(ready_fixture_core(), capacity)
    }

    pub(crate) async fn prepare_fixture_epoch(&self) {
        let (reply, response) = oneshot::channel();
        self.inner
            .commands
            .send(Command::PrepareFixtureEpoch { reply })
            .await
            .unwrap();
        response.await.unwrap();
    }

    pub(crate) fn queue_fixture_epoch_reset(&self) {
        let (reply, _) = oneshot::channel();
        self.inner
            .commands
            .try_send(Command::Control {
                control: Control::PrepareEpoch {
                    protocol_version: 1,
                    epoch: 1,
                },
                reply,
            })
            .unwrap();
    }
}

#[derive(Clone, Copy)]
struct PendingTransition {
    id: u16,
    protocol_version: u16,
    prepared: bool,
    sender_ratchet_pending: bool,
}

struct Core {
    backend: davey::DaveSession,
    user_id: u64,
    channel_id: u64,
    external_sender: bool,
    active_version: u16,
    pending: Option<PendingTransition>,
    epoch_prepared: bool,
    roster: HashSet<u64>,
}

impl Core {
    fn new(user_id: u64, channel_id: u64) -> Result<Self, Failure> {
        let version = NonZeroU16::new(MAX_PROTOCOL_VERSION).expect("DAVE v1 is nonzero");
        let backend = guarded(|| davey::DaveSession::new(version, user_id, channel_id, None))?;
        Ok(Self {
            backend,
            user_id,
            channel_id,
            external_sender: false,
            active_version: 0,
            pending: None,
            epoch_prepared: false,
            roster: HashSet::from([user_id]),
        })
    }

    fn snapshot(&self) -> Snapshot {
        Snapshot {
            active_version: self.active_version,
            transition_id: self.pending.map(|pending| pending.id),
            ready: self.active_version == MAX_PROTOCOL_VERSION && self.backend.is_ready(),
        }
    }

    fn control(&mut self, control: Control) -> Result<Vec<Outbound>, Failure> {
        match control {
            Control::PrepareTransition {
                protocol_version,
                id,
            } => self.prepare_transition(protocol_version, id),
            Control::ExecuteTransition { id } => self.execute_transition(id),
            Control::PrepareEpoch {
                protocol_version,
                epoch,
            } => self.prepare_epoch(protocol_version, epoch),
            Control::ExternalSender(payload) => self.external_sender(&payload),
            Control::Proposals(payload) => self.proposals(&payload),
            Control::Commit(payload) => self.commit_or_welcome(&payload, false),
            Control::Welcome(payload) => self.commit_or_welcome(&payload, true),
            Control::Roster(users) => {
                self.roster = users.into_iter().collect();
                self.roster.insert(self.user_id);
                Ok(Vec::new())
            }
            Control::MemberDisconnected(user) => {
                if user != self.user_id {
                    self.roster.remove(&user);
                }
                Ok(Vec::new())
            }
        }
    }

    fn prepare_transition(&mut self, version: u16, id: u16) -> Result<Vec<Outbound>, Failure> {
        validate_version(version)?;
        if self.pending.is_some_and(|pending| pending.id != id) {
            return Err(Failure::InvalidState);
        }
        let prepared =
            self.active_version == version && self.backend.is_ready() && !self.epoch_prepared;
        self.pending = Some(PendingTransition {
            id,
            protocol_version: version,
            prepared,
            sender_ratchet_pending: false,
        });
        if id == 0 && prepared {
            return self.execute_transition(id);
        }
        Ok(prepared.then(|| ready(id)).into_iter().collect())
    }

    fn execute_transition(&mut self, id: u16) -> Result<Vec<Outbound>, Failure> {
        let pending = self.pending.ok_or(Failure::InvalidState)?;
        if pending.id != id || !pending.prepared {
            return Err(Failure::InvalidState);
        }
        if pending.sender_ratchet_pending {
            guarded(|| self.backend.execute_transition())?;
        }
        self.active_version = pending.protocol_version;
        self.pending = None;
        self.epoch_prepared = false;
        Ok(Vec::new())
    }

    fn prepare_epoch(&mut self, version: u16, epoch: u64) -> Result<Vec<Outbound>, Failure> {
        validate_version(version)?;
        self.epoch_prepared = true;
        if epoch != 1 {
            return Ok(Vec::new());
        }
        let version = NonZeroU16::new(version).ok_or(Failure::RequiredDowngrade)?;
        guarded(|| {
            self.backend
                .reinit(version, self.user_id, self.channel_id, None)
        })?;
        self.active_version = 0;
        self.pending = None;
        self.epoch_prepared = true;
        if self.external_sender {
            self.key_package()
        } else {
            Ok(Vec::new())
        }
    }

    fn external_sender(&mut self, payload: &[u8]) -> Result<Vec<Outbound>, Failure> {
        if payload.len() < 4 {
            return Err(Failure::Malformed);
        }
        guarded(|| self.backend.set_external_sender(payload))?;
        self.external_sender = true;
        self.key_package()
    }

    fn key_package(&mut self) -> Result<Vec<Outbound>, Failure> {
        let package = guarded(|| self.backend.create_key_package())?;
        let mut message = Vec::with_capacity(1 + package.len());
        message.push(26);
        message.extend_from_slice(&package);
        Ok(vec![Outbound::Binary(message)])
    }

    fn proposals(&mut self, payload: &[u8]) -> Result<Vec<Outbound>, Failure> {
        let (&operation, body) = payload.split_first().ok_or(Failure::Malformed)?;
        if body.is_empty() {
            return Err(Failure::Malformed);
        }
        let operation = match operation {
            0 => davey::ProposalsOperationType::APPEND,
            1 => davey::ProposalsOperationType::REVOKE,
            _ => return Err(Failure::Malformed),
        };
        let mut expected: Vec<_> = self.roster.iter().copied().collect();
        expected.sort_unstable();
        let response = guarded(|| {
            self.backend
                .process_proposals(operation, body, Some(&expected))
        })?;
        let Some(response) = response else {
            return Ok(Vec::new());
        };
        let welcome_len = response.welcome.as_ref().map_or(0, Vec::len);
        let mut message = Vec::with_capacity(1 + response.commit.len() + welcome_len);
        message.push(28);
        message.extend_from_slice(&response.commit);
        if let Some(welcome) = response.welcome {
            message.extend_from_slice(&welcome);
        }
        Ok(vec![Outbound::Binary(message)])
    }

    fn commit_or_welcome(
        &mut self,
        payload: &[u8],
        welcome: bool,
    ) -> Result<Vec<Outbound>, Failure> {
        if payload.len() < 3 {
            return Err(Failure::Malformed);
        }
        let id = u16::from_be_bytes([payload[0], payload[1]]);
        let body = &payload[2..];
        let initial = id == 0
            && self.pending.is_none()
            && self.active_version == 0
            && self.external_sender
            && !self.backend.is_ready();
        if initial {
            self.pending = Some(PendingTransition {
                id,
                protocol_version: MAX_PROTOCOL_VERSION,
                prepared: false,
                sender_ratchet_pending: false,
            });
        } else if !self.pending.is_some_and(|pending| pending.id == id) {
            return Err(Failure::InvalidState);
        }
        let result = if welcome {
            let mut recognized: Vec<_> = self.roster.iter().copied().collect();
            recognized.sort_unstable();
            guarded(|| self.backend.process_welcome(body, Some(&recognized)))
        } else {
            guarded(|| self.backend.process_commit(body))
        };
        if result.is_err() {
            return self.recover_invalid(id);
        }
        let pending = self.pending.as_mut().expect("matching transition exists");
        pending.prepared = true;
        pending.sender_ratchet_pending = true;
        if id == 0 {
            return self.execute_transition(id);
        }
        Ok(vec![ready(id)])
    }

    fn recover_invalid(&mut self, id: u16) -> Result<Vec<Outbound>, Failure> {
        let version = NonZeroU16::new(MAX_PROTOCOL_VERSION).expect("DAVE v1 is nonzero");
        guarded(|| {
            self.backend
                .reinit(version, self.user_id, self.channel_id, None)
        })?;
        self.active_version = 0;
        self.pending = None;
        self.epoch_prepared = true;
        let mut actions = vec![Outbound::Json {
            opcode: 31,
            data: json!({"transition_id": id}),
        }];
        actions.extend(self.key_package()?);
        Ok(actions)
    }

    #[cfg(test)]
    fn encrypt(&mut self, frame: &[u8]) -> Result<Vec<u8>, Failure> {
        let mut output = Vec::new();
        self.encrypt_into(frame, &mut output)?;
        Ok(output)
    }

    fn encrypt_into(&mut self, frame: &[u8], output: &mut Vec<u8>) -> Result<(), Failure> {
        output.clear();
        if self.active_version != MAX_PROTOCOL_VERSION || !self.backend.is_ready() {
            return Err(Failure::InvalidState);
        }
        guarded(|| self.backend.encrypt_opus_into(frame, output))
    }
}

fn validate_version(version: u16) -> Result<(), Failure> {
    match version {
        0 => Err(Failure::RequiredDowngrade),
        MAX_PROTOCOL_VERSION => Ok(()),
        _ => Err(Failure::UnsupportedVersion),
    }
}

fn ready(id: u16) -> Outbound {
    Outbound::Json {
        opcode: 23,
        data: json!({"transition_id": id}),
    }
}

fn guarded<T, E>(operation: impl FnOnce() -> Result<T, E>) -> Result<T, Failure> {
    catch_unwind(AssertUnwindSafe(operation))
        .map_err(|_| Failure::BackendPanic)?
        .map_err(|_| Failure::Backend)
}

#[cfg(any(test, fuzzing))]
const UPSTREAM_FIXTURES: &str =
    include_str!("../../../vendor/davey/fixtures/upstream_session_fixtures.py");
#[cfg(any(test, fuzzing))]
const FIXTURE_MY_USER_ID: u64 = 158_049_329_150_427_136;
#[cfg(any(test, fuzzing))]
const FIXTURE_OTHER_USER_ID: u64 = 158_533_742_254_751_744;
#[cfg(any(test, fuzzing))]
const FIXTURE_CHANNEL_ID: u64 = 927_310_423_890_473_011;

#[cfg(any(test, fuzzing))]
fn fixture(name: &str, next_name: &str) -> Vec<u8> {
    let section = UPSTREAM_FIXTURES
        .split_once(name)
        .expect("fixture name exists")
        .1
        .split_once(next_name)
        .expect("next fixture name exists")
        .0;
    section
        .split(|character: char| !character.is_ascii_hexdigit() && character != 'x')
        .filter_map(|token| token.strip_prefix("0x"))
        .map(|token| u8::from_str_radix(token, 16).expect("valid fixture byte"))
        .collect()
}

#[cfg(test)]
pub(crate) fn test_external_sender_fixture() -> Vec<u8> {
    fixture("EXTERNAL_SENDER", "APPENDING_PROPOSALS")
}

#[cfg(any(test, fuzzing))]
fn ready_fixture_core() -> Core {
    let mut core = prepared_fixture_core();
    core.control(Control::ExecuteTransition { id: 7 }).unwrap();
    assert!(core.snapshot().ready);
    core
}

#[cfg(any(test, fuzzing))]
fn prepared_fixture_core() -> Core {
    let external_sender = fixture("EXTERNAL_SENDER", "APPENDING_PROPOSALS");
    let proposals = fixture("APPENDING_PROPOSALS", "REVOKING_PROPOSALS");
    let mut core = Core::new(FIXTURE_MY_USER_ID, FIXTURE_CHANNEL_ID).unwrap();
    core.control(Control::Roster(vec![
        FIXTURE_MY_USER_ID,
        FIXTURE_OTHER_USER_ID,
    ]))
    .unwrap();
    core.control(Control::PrepareTransition {
        protocol_version: 1,
        id: 7,
    })
    .unwrap();
    core.control(Control::ExternalSender(external_sender))
        .unwrap();
    let response = core
        .backend
        .process_proposals(
            davey::ProposalsOperationType::APPEND,
            &proposals,
            Some(&[FIXTURE_MY_USER_ID, FIXTURE_OTHER_USER_ID]),
        )
        .unwrap()
        .expect("fixture creates a commit");
    let mut announced_commit = 7_u16.to_be_bytes().to_vec();
    announced_commit.extend_from_slice(&response.commit);
    let actions = core.control(Control::Commit(announced_commit)).unwrap();
    assert!(matches!(
        actions.as_slice(),
        [Outbound::Json { opcode: 23, data }]
            if data == &json!({"transition_id": 7})
    ));
    assert_eq!(
        core.snapshot(),
        Snapshot {
            active_version: 0,
            transition_id: Some(7),
            ready: false,
        }
    );
    core
}

#[cfg(fuzzing)]
pub(crate) fn fuzz_control_sequence(input: &[u8]) {
    if input.len() > 1_048_576 {
        return;
    }
    let Some((&initial, mut remaining)) = input.split_first() else {
        return;
    };
    let mut core = match initial % 3 {
        0 => Core::new(FIXTURE_MY_USER_ID, FIXTURE_CHANNEL_ID)
            .expect("fixed fuzz identity creates a DAVE session"),
        1 => prepared_fixture_core(),
        _ => ready_fixture_core(),
    };
    let mut output = Vec::new();

    for _ in 0..64 {
        let Some((&action, tail)) = remaining.split_first() else {
            break;
        };
        if tail.len() < 2 {
            break;
        }
        let declared = usize::from(u16::from_be_bytes([tail[0], tail[1]]));
        remaining = &tail[2..];
        let length = declared.min(remaining.len());
        let payload = &remaining[..length];
        remaining = &remaining[length..];

        let _result = match action % 10 {
            0 => core.control(Control::PrepareTransition {
                protocol_version: prefix_u16(payload, 0),
                id: prefix_u16(payload, 2),
            }),
            1 => core.control(Control::ExecuteTransition {
                id: prefix_u16(payload, 0),
            }),
            2 => core.control(Control::PrepareEpoch {
                protocol_version: prefix_u16(payload, 0),
                epoch: prefix_u64(payload, 2),
            }),
            3 => core.control(Control::ExternalSender(payload.to_vec())),
            4 => core.control(Control::Proposals(payload.to_vec())),
            5 => core.control(Control::Commit(payload.to_vec())),
            6 => core.control(Control::Welcome(payload.to_vec())),
            7 => core.control(Control::Roster(
                payload
                    .chunks_exact(8)
                    .map(|chunk| prefix_u64(chunk, 0))
                    .collect(),
            )),
            8 => core.control(Control::MemberDisconnected(prefix_u64(payload, 0))),
            _ => {
                let frame = &payload[..payload.len().min(1_275)];
                output.clear();
                let encrypted = core.encrypt_into(frame, &mut output);
                if encrypted.is_err() {
                    assert!(output.is_empty());
                }
                Ok(Vec::new())
            }
        };

        let snapshot = core.snapshot();
        assert!(snapshot.active_version <= MAX_PROTOCOL_VERSION);
        assert!(!snapshot.ready || snapshot.active_version == MAX_PROTOCOL_VERSION);
        if !snapshot.ready {
            output.clear();
            assert!(matches!(
                core.encrypt_into(b"must remain fail-closed", &mut output),
                Err(Failure::InvalidState)
            ));
            assert!(output.is_empty());
        }
    }
}

#[cfg(fuzzing)]
fn prefix_u16(input: &[u8], offset: usize) -> u16 {
    u16::from_be_bytes([
        input.get(offset).copied().unwrap_or(0),
        input.get(offset + 1).copied().unwrap_or(0),
    ])
}

#[cfg(fuzzing)]
fn prefix_u64(input: &[u8], offset: usize) -> u64 {
    let mut bytes = [0; 8];
    if let Some(available) = input.get(offset..) {
        let length = available.len().min(bytes.len());
        bytes[..length].copy_from_slice(&available[..length]);
    }
    u64::from_be_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use crate::transport::{TransportEncoder, TransportMode};

    use super::*;

    #[test]
    fn vendored_backend_source_does_not_log_dave_security_material() {
        const SOURCES: &[(&str, &str)] = &[
            (
                "session.rs",
                include_str!("../../../vendor/davey/src/session.rs"),
            ),
            (
                "hash_ratchet.rs",
                include_str!("../../../vendor/davey/src/cryptor/hash_ratchet.rs"),
            ),
            (
                "mlspp_crypto.rs",
                include_str!("../../../vendor/davey/src/cryptor/mlspp_crypto.rs"),
            ),
        ];
        const FORBIDDEN_LOG_FRAGMENTS: &[&str] = &[
            "Got base secret",
            "hash ratchet with secret",
            "Input secret",
            "Derived secret",
            "secret: {:x?}",
            "New Voice Privacy Code",
        ];

        for (path, source) in SOURCES {
            for fragment in FORBIDDEN_LOG_FRAGMENTS {
                assert!(
                    !source.contains(fragment),
                    "{path} must not log DAVE security material matching {fragment:?}"
                );
            }
        }
    }

    fn mutation_bytes(seed: &mut u64, maximum_len: usize) -> Vec<u8> {
        *seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let length = (*seed as usize) % (maximum_len + 1);
        (0..length)
            .map(|_| {
                *seed = seed
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                (*seed >> 32) as u8
            })
            .collect()
    }

    fn benchmark_env(name: &str, default: usize) -> usize {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    }

    fn percentile(samples: &mut [u64], permille: usize) -> u64 {
        samples.sort_unstable();
        let index = (samples.len().saturating_sub(1) * permille) / 1_000;
        samples[index]
    }

    fn process_pss_kib() -> usize {
        std::fs::read_to_string("/proc/self/smaps_rollup")
            .ok()
            .and_then(|contents| {
                contents.lines().find_map(|line| {
                    line.strip_prefix("Pss:")?
                        .split_whitespace()
                        .next()?
                        .parse()
                        .ok()
                })
            })
            .unwrap_or(0)
    }

    #[test]
    fn versions_and_transition_order_fail_closed() {
        let mut core = Core::new(7, 9).unwrap();
        assert!(matches!(
            core.control(Control::PrepareTransition {
                protocol_version: 0,
                id: 1
            }),
            Err(Failure::RequiredDowngrade)
        ));
        assert!(matches!(
            core.control(Control::PrepareTransition {
                protocol_version: 2,
                id: 1
            }),
            Err(Failure::UnsupportedVersion)
        ));
        assert!(matches!(
            core.control(Control::ExecuteTransition { id: 1 }),
            Err(Failure::InvalidState)
        ));
        assert!(!core.snapshot().ready);
    }

    #[test]
    fn transition_zero_executes_immediately_only_when_already_prepared() {
        let mut new_core = Core::new(7, 9).unwrap();
        assert!(
            new_core
                .control(Control::PrepareTransition {
                    protocol_version: 1,
                    id: 0,
                })
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            new_core.snapshot(),
            Snapshot {
                active_version: 0,
                transition_id: Some(0),
                ready: false,
            }
        );
        assert!(matches!(
            new_core.control(Control::ExecuteTransition { id: 0 }),
            Err(Failure::InvalidState)
        ));
        assert!(matches!(
            new_core.encrypt(b"must remain blocked"),
            Err(Failure::InvalidState)
        ));

        let mut ready_core = ready_fixture_core();
        assert!(
            ready_core
                .control(Control::PrepareTransition {
                    protocol_version: 1,
                    id: 0,
                })
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            ready_core.snapshot(),
            Snapshot {
                active_version: 1,
                transition_id: None,
                ready: true,
            }
        );
        assert_ne!(
            ready_core.encrypt(b"still encrypted").unwrap(),
            b"still encrypted"
        );
    }

    #[test]
    fn initial_commit_zero_activates_without_ready_round_trip() {
        let external_sender = fixture("EXTERNAL_SENDER", "APPENDING_PROPOSALS");
        let proposals = fixture("APPENDING_PROPOSALS", "REVOKING_PROPOSALS");
        let mut core = Core::new(FIXTURE_MY_USER_ID, FIXTURE_CHANNEL_ID).unwrap();
        core.control(Control::Roster(vec![FIXTURE_OTHER_USER_ID]))
            .unwrap();
        core.control(Control::ExternalSender(external_sender))
            .unwrap();
        let response = core
            .backend
            .process_proposals(
                davey::ProposalsOperationType::APPEND,
                &proposals,
                Some(&[FIXTURE_MY_USER_ID, FIXTURE_OTHER_USER_ID]),
            )
            .unwrap()
            .expect("fixture creates an initial commit");
        let mut announced_commit = 0_u16.to_be_bytes().to_vec();
        announced_commit.extend_from_slice(&response.commit);

        assert!(
            core.control(Control::Commit(announced_commit))
                .unwrap()
                .is_empty(),
            "transition zero never sends a ready acknowledgement"
        );
        assert_eq!(
            core.snapshot(),
            Snapshot {
                active_version: 1,
                transition_id: None,
                ready: true,
            }
        );
        assert_ne!(core.encrypt(b"protected").unwrap(), b"protected");
    }

    #[test]
    fn wrong_transition_ids_preserve_the_prepared_transition() {
        let mut core = prepared_fixture_core();
        let prepared = Snapshot {
            active_version: 0,
            transition_id: Some(7),
            ready: false,
        };
        assert_eq!(core.snapshot(), prepared);

        for rejected in [
            Control::PrepareTransition {
                protocol_version: 1,
                id: 8,
            },
            Control::ExecuteTransition { id: 8 },
            Control::Commit(vec![0, 8, 0]),
            Control::Welcome(vec![0, 8, 0]),
        ] {
            assert!(matches!(core.control(rejected), Err(Failure::InvalidState)));
            assert_eq!(core.snapshot(), prepared);
            assert!(matches!(
                core.encrypt(b"must remain blocked"),
                Err(Failure::InvalidState)
            ));
        }

        assert!(
            core.control(Control::ExecuteTransition { id: 7 })
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            core.snapshot(),
            Snapshot {
                active_version: 1,
                transition_id: None,
                ready: true,
            }
        );
        assert_ne!(core.encrypt(b"now encrypted").unwrap(), b"now encrypted");
    }

    #[test]
    fn valid_external_sender_produces_fresh_bounded_key_packages() {
        let mut core = Core::new(7, 9).unwrap();
        let external_sender = fixture("EXTERNAL_SENDER", "APPENDING_PROPOSALS");
        let first = core
            .control(Control::ExternalSender(external_sender))
            .unwrap();
        let [Outbound::Binary(first)] = first.as_slice() else {
            panic!("expected key package");
        };
        assert_eq!(first[0], 26);
        assert!(first.len() < 4_096);

        let second = core
            .control(Control::PrepareEpoch {
                protocol_version: 1,
                epoch: 1,
            })
            .unwrap();
        let [Outbound::Binary(second)] = second.as_slice() else {
            panic!("expected replacement key package");
        };
        assert_eq!(second[0], 26);
        assert_ne!(first, second);
        assert!(!core.snapshot().ready);
    }

    #[test]
    fn rejected_external_sender_replacement_preserves_active_group() {
        let mut core = ready_fixture_core();
        let ready = Snapshot {
            active_version: 1,
            transition_id: None,
            ready: true,
        };
        assert_eq!(core.snapshot(), ready);
        assert_ne!(
            core.encrypt(b"before replacement").unwrap(),
            b"before replacement"
        );

        assert!(matches!(
            core.control(Control::ExternalSender(test_external_sender_fixture())),
            Err(Failure::Backend)
        ));
        assert_eq!(core.snapshot(), ready);
        assert_ne!(
            core.encrypt(b"after replacement").unwrap(),
            b"after replacement"
        );
    }

    #[test]
    fn rejected_versions_preserve_active_encrypted_sender() {
        let mut core = ready_fixture_core();
        let ready = Snapshot {
            active_version: 1,
            transition_id: None,
            ready: true,
        };

        for (version, expected) in [
            (0, Failure::RequiredDowngrade),
            (MAX_PROTOCOL_VERSION + 1, Failure::UnsupportedVersion),
        ] {
            assert_eq!(
                core.control(Control::PrepareTransition {
                    protocol_version: version,
                    id: 8,
                })
                .unwrap_err(),
                expected
            );
            assert_eq!(core.snapshot(), ready);

            assert_eq!(
                core.control(Control::PrepareEpoch {
                    protocol_version: version,
                    epoch: 1,
                })
                .unwrap_err(),
                expected
            );
            assert_eq!(core.snapshot(), ready);
            assert_ne!(
                core.encrypt(b"still protected").unwrap(),
                b"still protected"
            );
        }
    }

    #[test]
    fn epoch_one_replaces_active_and_pending_group_state() {
        let mut core = ready_fixture_core();
        assert_ne!(core.encrypt(b"old group").unwrap(), b"old group");

        let actions = core
            .control(Control::PrepareTransition {
                protocol_version: 1,
                id: 8,
            })
            .unwrap();
        assert!(matches!(
            actions.as_slice(),
            [Outbound::Json { opcode: 23, .. }]
        ));
        assert_eq!(
            core.snapshot(),
            Snapshot {
                active_version: 1,
                transition_id: Some(8),
                ready: true,
            }
        );

        let actions = core
            .control(Control::PrepareEpoch {
                protocol_version: 1,
                epoch: 1,
            })
            .unwrap();
        assert!(matches!(
            actions.as_slice(),
            [Outbound::Binary(package)] if package.first() == Some(&26)
        ));
        assert_eq!(
            core.snapshot(),
            Snapshot {
                active_version: 0,
                transition_id: None,
                ready: false,
            }
        );
        assert!(matches!(
            core.control(Control::ExecuteTransition { id: 8 }),
            Err(Failure::InvalidState)
        ));
        assert!(matches!(
            core.encrypt(b"must not use old group"),
            Err(Failure::InvalidState)
        ));
    }

    #[test]
    fn later_epoch_retains_old_sender_until_new_transition_is_prepared() {
        let mut core = ready_fixture_core();
        assert_ne!(core.encrypt(b"before epoch").unwrap(), b"before epoch");

        assert!(
            core.control(Control::PrepareEpoch {
                protocol_version: 1,
                epoch: 2,
            })
            .unwrap()
            .is_empty()
        );
        assert_eq!(
            core.snapshot(),
            Snapshot {
                active_version: 1,
                transition_id: None,
                ready: true,
            }
        );
        assert_ne!(
            core.encrypt(b"old sender remains").unwrap(),
            b"old sender remains"
        );

        assert!(
            core.control(Control::PrepareTransition {
                protocol_version: 1,
                id: 8,
            })
            .unwrap()
            .is_empty()
        );
        let pending = Snapshot {
            active_version: 1,
            transition_id: Some(8),
            ready: true,
        };
        assert_eq!(core.snapshot(), pending);
        assert!(matches!(
            core.control(Control::ExecuteTransition { id: 8 }),
            Err(Failure::InvalidState)
        ));
        assert_eq!(core.snapshot(), pending);
        assert_ne!(
            core.encrypt(b"old sender still active").unwrap(),
            b"old sender still active"
        );
    }

    #[test]
    fn malformed_backend_input_is_typed_and_never_ready() {
        let mut core = Core::new(7, 9).unwrap();
        assert!(matches!(
            core.control(Control::ExternalSender(vec![0x40, 0x41])),
            Err(Failure::Malformed)
        ));
        assert!(matches!(
            core.control(Control::Proposals(vec![0])),
            Err(Failure::Malformed)
        ));
        assert!(matches!(
            core.control(Control::Proposals(vec![2, 1])),
            Err(Failure::Malformed)
        ));
        for truncated in [vec![], vec![0], vec![0, 7]] {
            assert!(matches!(
                core.control(Control::Commit(truncated.clone())),
                Err(Failure::Malformed)
            ));
            assert!(matches!(
                core.control(Control::Welcome(truncated)),
                Err(Failure::Malformed)
            ));
        }
        assert!(!core.snapshot().ready);
    }

    #[test]
    fn deterministic_malformed_control_mutations_are_contained_and_fail_closed() {
        let external_sender = fixture("EXTERNAL_SENDER", "APPENDING_PROPOSALS");
        let mut seed = 0xD4A6_E001_5EED_C0DE;

        for case in 0..256_u16 {
            let mut core = Core::new(FIXTURE_MY_USER_ID, FIXTURE_CHANNEL_ID).unwrap();
            let mut payload = mutation_bytes(&mut seed, 192);
            let result = match case % 4 {
                0 => core.control(Control::ExternalSender(payload)),
                1 => {
                    core.control(Control::ExternalSender(external_sender.clone()))
                        .expect("pinned external sender is valid");
                    payload.insert(0, (case & 0x03) as u8);
                    core.control(Control::Proposals(payload))
                }
                2 | 3 => {
                    core.control(Control::PrepareTransition {
                        protocol_version: 1,
                        id: case,
                    })
                    .expect("supported transition prepares");
                    payload.splice(0..0, case.to_be_bytes());
                    if case % 4 == 2 {
                        core.control(Control::Commit(payload))
                    } else {
                        core.control(Control::Welcome(payload))
                    }
                }
                _ => unreachable!(),
            };

            assert!(matches!(
                result,
                Ok(_) | Err(Failure::Malformed | Failure::Backend | Failure::BackendPanic)
            ));
            let snapshot = core.snapshot();
            assert!(snapshot.active_version <= MAX_PROTOCOL_VERSION);
            assert!(!snapshot.ready);
            assert!(matches!(
                core.encrypt(b"must not escape"),
                Err(Failure::InvalidState)
            ));
        }
    }

    #[test]
    fn invalid_commit_and_welcome_reset_group_and_emit_fresh_package() {
        for welcome in [false, true] {
            let mut core = ready_fixture_core();
            let actions = core
                .control(Control::PrepareTransition {
                    protocol_version: 1,
                    id: 8,
                })
                .unwrap();
            assert!(matches!(
                actions.as_slice(),
                [Outbound::Json { opcode: 23, .. }]
            ));

            let payload = vec![0, 8, 0];
            let control = if welcome {
                Control::Welcome(payload)
            } else {
                Control::Commit(payload)
            };
            let actions = core.control(control).unwrap();
            assert!(matches!(
                actions.as_slice(),
                [
                    Outbound::Json { opcode: 31, .. },
                    Outbound::Binary(package)
                ] if package.first() == Some(&26)
            ));
            assert_eq!(
                core.snapshot(),
                Snapshot {
                    active_version: 0,
                    transition_id: None,
                    ready: false,
                }
            );
            assert!(matches!(
                core.encrypt(b"must not escape"),
                Err(Failure::InvalidState)
            ));
        }
    }

    #[test]
    fn disconnected_member_is_removed_from_proposal_allowlist() {
        let mut core = Core::new(FIXTURE_MY_USER_ID, FIXTURE_CHANNEL_ID).unwrap();
        core.control(Control::Roster(vec![
            FIXTURE_MY_USER_ID,
            FIXTURE_OTHER_USER_ID,
        ]))
        .unwrap();
        core.control(Control::ExternalSender(test_external_sender_fixture()))
            .unwrap();
        core.control(Control::MemberDisconnected(FIXTURE_OTHER_USER_ID))
            .unwrap();
        let mut proposals = vec![0];
        proposals.extend(fixture("APPENDING_PROPOSALS", "REVOKING_PROPOSALS"));
        assert!(matches!(
            core.control(Control::Proposals(proposals)),
            Err(Failure::Backend)
        ));
        assert!(!core.snapshot().ready);
    }

    #[test]
    fn roster_controls_cannot_remove_local_identity() {
        let mut core = Core::new(FIXTURE_MY_USER_ID, FIXTURE_CHANNEL_ID).unwrap();
        core.control(Control::Roster(vec![
            FIXTURE_OTHER_USER_ID,
            FIXTURE_OTHER_USER_ID,
        ]))
        .unwrap();
        assert_eq!(core.roster.len(), 2);
        assert!(core.roster.contains(&FIXTURE_MY_USER_ID));
        assert!(core.roster.contains(&FIXTURE_OTHER_USER_ID));

        core.control(Control::MemberDisconnected(FIXTURE_MY_USER_ID))
            .unwrap();
        assert!(core.roster.contains(&FIXTURE_MY_USER_ID));

        core.control(Control::MemberDisconnected(FIXTURE_OTHER_USER_ID))
            .unwrap();
        assert_eq!(core.roster, HashSet::from([FIXTURE_MY_USER_ID]));
    }

    #[test]
    fn pinned_mls_fixture_reaches_execute_and_encrypts_participant_silence() {
        let mut core = ready_fixture_core();
        let encrypted = core.encrypt(&davey::OPUS_SILENCE_PACKET).unwrap();
        assert_ne!(encrypted, davey::OPUS_SILENCE_PACKET);
        assert_eq!(&encrypted[encrypted.len() - 2..], &[0xFA, 0xFA]);
    }

    #[test]
    fn successful_commit_stages_sender_and_clears_blocked_output() {
        let mut core = prepared_fixture_core();
        let mut output = vec![0xAA; 32];
        assert!(matches!(
            core.encrypt_into(b"before execute", &mut output),
            Err(Failure::InvalidState)
        ));
        assert!(output.is_empty());

        assert!(
            core.control(Control::ExecuteTransition { id: 7 })
                .unwrap()
                .is_empty()
        );
        core.encrypt_into(b"after execute", &mut output).unwrap();
        assert_ne!(output, b"after execute");
        assert_eq!(
            core.snapshot(),
            Snapshot {
                active_version: 1,
                transition_id: None,
                ready: true,
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn media_diagnostics_distinguish_queue_and_response_deadlines() {
        let (commands, mut receiver) = mpsc::channel(1);
        let (reply, responses) = mpsc::channel(1);
        let mut media = MediaEncryptor {
            commands,
            reply,
            responses,
            measure: false,
        };
        media
            .commands
            .send(Command::Encrypt {
                frame: vec![1],
                len: 1,
                output: Vec::new(),
                measure: false,
                reply: media.reply.clone(),
            })
            .await
            .unwrap();
        assert!(matches!(
            media.encrypt_buffered(vec![1], 1, Vec::new()).await,
            Err(Failure::QueueTimeout)
        ));
        drop(receiver.recv().await);
        assert!(matches!(
            media.encrypt_buffered(vec![1], 1, Vec::new()).await,
            Err(Failure::ResponseTimeout)
        ));
        drop(receiver);
        assert!(matches!(
            media.encrypt_buffered(vec![1], 1, Vec::new()).await,
            Err(Failure::Closed)
        ));
    }

    #[tokio::test]
    async fn cancelled_media_response_cannot_block_owner_and_last_handle_drop_closes_lane() {
        let handle = Handle::spawn_ready_fixture(2);
        let (reply, responses) = mpsc::channel(1);
        drop(responses);
        handle
            .inner
            .commands
            .send(Command::Encrypt {
                frame: vec![1, 2, 3],
                len: 3,
                output: Vec::new(),
                measure: false,
                reply,
            })
            .await
            .expect("cancelled media command enters owner");
        timeout(
            Duration::from_secs(1),
            handle.control(Control::Roster(vec![FIXTURE_MY_USER_ID])),
        )
        .await
        .expect("cancelled response cannot block later gateway control")
        .expect("later gateway control succeeds");

        let mut media = handle.media_encryptor();
        drop(handle);
        timeout(Duration::from_secs(1), media.commands.closed())
            .await
            .expect("last handle drop aborts the owner task");
        assert!(matches!(
            media.encrypt_buffered(vec![1, 2, 3], 3, Vec::new()).await,
            Err(Failure::Closed)
        ));
    }

    #[tokio::test]
    async fn owner_orders_media_across_execute_without_partial_visibility() {
        let handle = Handle::spawn_core(prepared_fixture_core(), 3);
        let commands = &handle.inner.commands;
        let frame = vec![1, 2, 3];

        let (before_reply, mut before_responses) = mpsc::channel(1);
        commands
            .send(Command::Encrypt {
                frame: frame.clone(),
                len: frame.len(),
                output: Vec::new(),
                measure: false,
                reply: before_reply,
            })
            .await
            .unwrap();

        let (execute_reply, execute_response) = oneshot::channel();
        commands
            .send(Command::Control {
                control: Control::ExecuteTransition { id: 7 },
                reply: execute_reply,
            })
            .await
            .unwrap();

        let (after_reply, mut after_responses) = mpsc::channel(1);
        commands
            .send(Command::Encrypt {
                frame: frame.clone(),
                len: frame.len(),
                output: Vec::new(),
                measure: false,
                reply: after_reply,
            })
            .await
            .unwrap();

        let before = before_responses.recv().await.unwrap();
        assert_eq!(before.result, Ok(MediaOutcome::NotReady));
        assert!(before.output.is_empty());

        assert!(execute_response.await.unwrap().unwrap().is_empty());

        let after = after_responses.recv().await.unwrap();
        after.result.unwrap();
        assert_ne!(after.output, frame);
        assert_eq!(
            handle.snapshot(),
            Snapshot {
                active_version: 1,
                transition_id: None,
                ready: true,
            }
        );
    }

    #[tokio::test]
    #[ignore = "release-only P08 DAVE adapter performance evidence"]
    async fn p08_dave_adapter_benchmark() {
        let sessions = benchmark_env("OTO_P08_SESSIONS", 12).max(1);
        let encryptions = benchmark_env("OTO_P08_ENCRYPTIONS", 10_000).max(1);
        let baseline_pss_kib = process_pss_kib();
        let setup_region = stats_alloc::Region::new(crate::TEST_ALLOCATOR);
        let mut setup_nanos = Vec::with_capacity(sessions);
        let mut execute_nanos = Vec::with_capacity(sessions);
        let mut cores = Vec::with_capacity(sessions);
        for _ in 0..sessions {
            let started = std::time::Instant::now();
            let mut core = prepared_fixture_core();
            setup_nanos.push(started.elapsed().as_nanos() as u64);
            let started = std::time::Instant::now();
            core.control(Control::ExecuteTransition { id: 7 }).unwrap();
            execute_nanos.push(started.elapsed().as_nanos() as u64);
            cores.push(core);
        }
        let setup_allocation = setup_region.change();
        let sessions_pss_kib = process_pss_kib();

        let frame = vec![0x55; 1_275];
        let core = cores.first_mut().expect("at least one benchmark session");
        let mut direct_output = Vec::new();
        for _ in 0..100 {
            core.encrypt_into(&frame, &mut direct_output)
                .expect("warmup encryption succeeds");
        }
        let mut direct_nanos = Vec::with_capacity(encryptions);
        let direct_region = stats_alloc::Region::new(crate::TEST_ALLOCATOR);
        for _ in 0..encryptions {
            let started = std::time::Instant::now();
            core.encrypt_into(&frame, &mut direct_output)
                .expect("direct encryption succeeds");
            direct_nanos.push(started.elapsed().as_nanos() as u64);
        }
        let direct_allocation = direct_region.change();

        let owner = Handle::spawn_ready_fixture(8);
        let mut media = owner.media_encryptor();
        let mut owner_frame = frame.clone();
        let mut owner_output = Vec::new();
        for _ in 0..100 {
            let buffers = media
                .encrypt_buffered(owner_frame, frame.len(), owner_output)
                .await
                .expect("owner warmup command succeeds");
            buffers.result.expect("owner warmup encryption succeeds");
            owner_frame = buffers.frame;
            owner_output = buffers.output;
        }
        let mut owner_nanos = Vec::with_capacity(encryptions);
        let owner_region = stats_alloc::Region::new(crate::TEST_ALLOCATOR);
        for _ in 0..encryptions {
            let started = std::time::Instant::now();
            let buffers = media
                .encrypt_buffered(owner_frame, frame.len(), owner_output)
                .await
                .expect("owner command succeeds");
            buffers.result.expect("owner encryption succeeds");
            owner_frame = buffers.frame;
            owner_output = buffers.output;
            owner_nanos.push(started.elapsed().as_nanos() as u64);
        }
        let owner_allocation = owner_region.change();

        let result = json!({
            "schemaVersion": 1,
            "benchmarkId": "oto-p08-dave-adapter",
            "profile": "release",
            "sessions": sessions,
            "setup": {
                "p50Nanos": percentile(&mut setup_nanos, 500),
                "p99Nanos": percentile(&mut setup_nanos, 990),
                "allocationsPerSession": setup_allocation.allocations as f64 / sessions as f64,
                "bytesAllocatedPerSession": setup_allocation.bytes_allocated as f64 / sessions as f64,
                "pssIncrementKiB": sessions_pss_kib.saturating_sub(baseline_pss_kib),
                "pssIncrementKiBPerSession": sessions_pss_kib.saturating_sub(baseline_pss_kib) as f64 / sessions as f64,
            },
            "executeTransition": {
                "p50Nanos": percentile(&mut execute_nanos, 500),
                "p99Nanos": percentile(&mut execute_nanos, 990),
            },
            "directEncrypt": {
                "frames": encryptions,
                "p50Nanos": percentile(&mut direct_nanos, 500),
                "p99Nanos": percentile(&mut direct_nanos, 990),
                "allocationsPerFrame": direct_allocation.allocations as f64 / encryptions as f64,
                "reallocationsPerFrame": direct_allocation.reallocations as f64 / encryptions as f64,
                "bytesAllocatedPerFrame": direct_allocation.bytes_allocated as f64 / encryptions as f64,
            },
            "ownerEncrypt": {
                "frames": encryptions,
                "p50Nanos": percentile(&mut owner_nanos, 500),
                "p99Nanos": percentile(&mut owner_nanos, 990),
                "allocationsPerFrame": owner_allocation.allocations as f64 / encryptions as f64,
                "reallocationsPerFrame": owner_allocation.reallocations as f64 / encryptions as f64,
                "bytesAllocatedPerFrame": owner_allocation.bytes_allocated as f64 / encryptions as f64,
            }
        });
        println!("P08_DAVE_ADAPTER_BENCHMARK={result}");
    }

    #[test]
    #[ignore = "release-only simulated 48-hour DAVE/RTP/transport-crypto frame-count soak"]
    fn p14_simulated_dave_transport_crypto_soak() {
        let simulated_hours = benchmark_env("OTO_P14_SIMULATED_SOAK_HOURS", 48);
        assert!((24..=72).contains(&simulated_hours));
        let frames = simulated_hours * 60 * 60 * 50;

        let frame = vec![0x55; 1_275];
        let mut dave_frame = Vec::with_capacity(1_275 + OPUS_MAX_ENCRYPTION_OVERHEAD_BYTES);
        let mut core = ready_fixture_core();
        let mut encoder = TransportEncoder::new(
            TransportMode::Aes256GcmRtpSize,
            &[0x42; 32],
            0x0102_0304,
            1_275 + OPUS_MAX_ENCRYPTION_OVERHEAD_BYTES,
            2_048,
        )
        .expect("transport encoder builds");
        encoder.set_test_nonce_start(0);
        let mut packet = Vec::with_capacity(2_048);

        core.encrypt_into(&frame, &mut dave_frame)
            .expect("first DAVE frame encrypts");
        encoder
            .encrypt_next(&dave_frame, &mut packet)
            .expect("first transport packet encrypts");
        let initial_sequence = u16::from_be_bytes(packet[2..4].try_into().unwrap());
        let initial_timestamp = u32::from_be_bytes(packet[4..8].try_into().unwrap());

        let started = std::time::Instant::now();
        let allocation_region = stats_alloc::Region::new(crate::TEST_ALLOCATOR);
        for _ in 1..frames {
            core.encrypt_into(&frame, &mut dave_frame)
                .expect("DAVE frame encrypts throughout simulated soak");
            encoder
                .encrypt_next(&dave_frame, &mut packet)
                .expect("transport packet encrypts throughout simulated soak");
            std::hint::black_box(packet.as_slice());
        }
        let allocation = allocation_region.change();
        let wall_seconds = started.elapsed().as_secs_f64();

        let final_sequence = u16::from_be_bytes(packet[2..4].try_into().unwrap());
        let final_timestamp = u32::from_be_bytes(packet[4..8].try_into().unwrap());
        let final_nonce = u32::from_le_bytes(packet[packet.len() - 4..].try_into().unwrap());
        let increments = (frames - 1) as u64;
        assert_eq!(
            final_sequence,
            initial_sequence.wrapping_add(increments as u16)
        );
        assert_eq!(
            final_timestamp,
            initial_timestamp.wrapping_add(increments.wrapping_mul(960) as u32)
        );
        assert_eq!(final_nonce, increments as u32);
        assert!(increments / u64::from(u16::MAX) > 1);
        assert!(increments.wrapping_mul(960) > u64::from(u32::MAX));
        assert_eq!(allocation.allocations, 0);
        assert_eq!(allocation.reallocations, 0);
        assert_eq!(allocation.bytes_allocated, 0);

        let result = json!({
            "schemaVersion": 1,
            "benchmarkId": "oto-p14-simulated-dave-transport-crypto-soak",
            "profile": "release",
            "classification": "simulated frame-count soak; not wall-clock or live Discord evidence",
            "simulatedHours": simulated_hours,
            "frames": frames,
            "wallSeconds": wall_seconds,
            "layers": ["DAVE v1", "RTP", "AES-256-GCM rtpsize"],
            "sequenceWrapsMinimum": increments / (u64::from(u16::MAX) + 1),
            "timestampWrapsMinimum": increments.wrapping_mul(960) / (u64::from(u32::MAX) + 1),
            "transportNonceStart": 0,
            "transportNonceEnd": final_nonce,
            "allocations": allocation.allocations,
            "reallocations": allocation.reallocations,
            "bytesAllocated": allocation.bytes_allocated
        });
        println!("P14_SIMULATED_SOAK={result}");
    }
}
