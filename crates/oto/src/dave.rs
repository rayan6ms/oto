use std::collections::HashSet;
use std::num::NonZeroU16;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::timeout;

pub(crate) const MAX_PROTOCOL_VERSION: u16 = 1;
const COMMAND_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Snapshot {
    pub(crate) active_version: u16,
    pub(crate) transition_id: Option<u16>,
    pub(crate) ready: bool,
}

#[derive(Debug)]
pub(crate) enum Failure {
    UnsupportedVersion,
    RequiredDowngrade,
    Malformed,
    InvalidState,
    Backend,
    BackendPanic,
    Overloaded,
    Closed,
}

impl std::fmt::Display for Failure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::UnsupportedVersion => "unsupported DAVE protocol version",
            Self::RequiredDowngrade => "DAVE-required call attempted a plaintext downgrade",
            Self::Malformed => "malformed DAVE control payload",
            Self::InvalidState => "invalid DAVE lifecycle transition",
            Self::Backend => "DAVE backend rejected the operation",
            Self::BackendPanic => "DAVE backend panicked while rejecting untrusted input",
            Self::Overloaded => "DAVE command queue remained full",
            Self::Closed => "DAVE owner task stopped",
        })
    }
}

impl std::error::Error for Failure {}

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
    Control {
        control: Control,
        reply: oneshot::Sender<Result<Vec<Outbound>, Failure>>,
    },
    Encrypt {
        frame: Vec<u8>,
        reply: oneshot::Sender<Result<Vec<u8>, Failure>>,
    },
}

impl Handle {
    pub(crate) fn spawn(user_id: u64, channel_id: u64, capacity: usize) -> Result<Self, Failure> {
        let core = Core::new(user_id, channel_id)?;
        let (commands, mut receiver) = mpsc::channel(capacity);
        let (state_tx, state) = watch::channel(core.snapshot());
        let task = tokio::spawn(async move {
            let mut core = core;
            while let Some(command) = receiver.recv().await {
                match command {
                    Command::Control { control, reply } => {
                        let result = core.control(control);
                        state_tx.send_replace(core.snapshot());
                        let _ = reply.send(result);
                    }
                    Command::Encrypt { frame, reply } => {
                        let _ = reply.send(core.encrypt(&frame));
                    }
                }
            }
        });
        Ok(Self {
            inner: Arc::new(Owner {
                commands,
                state,
                task: Mutex::new(Some(task)),
            }),
        })
    }

    pub(crate) fn snapshot(&self) -> Snapshot {
        *self.inner.state.borrow()
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
        .map_err(|_| Failure::Overloaded)?
        .map_err(|_| Failure::Closed)?;
        timeout(COMMAND_TIMEOUT, response)
            .await
            .map_err(|_| Failure::Overloaded)?
            .map_err(|_| Failure::Closed)?
    }

    pub(crate) async fn encrypt(&self, frame: &[u8]) -> Result<Vec<u8>, Failure> {
        let (reply, response) = oneshot::channel();
        timeout(
            COMMAND_TIMEOUT,
            self.inner.commands.send(Command::Encrypt {
                frame: frame.to_vec(),
                reply,
            }),
        )
        .await
        .map_err(|_| Failure::Overloaded)?
        .map_err(|_| Failure::Closed)?;
        timeout(COMMAND_TIMEOUT, response)
            .await
            .map_err(|_| Failure::Overloaded)?
            .map_err(|_| Failure::Closed)?
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
                self.roster.remove(&user);
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
        let matches = self.pending.is_some_and(|pending| pending.id == id);
        if !matches {
            return Err(Failure::InvalidState);
        }
        let result = if welcome {
            guarded(|| self.backend.process_welcome(body))
        } else {
            guarded(|| self.backend.process_commit(body))
        };
        if result.is_err() {
            return self.recover_invalid(id);
        }
        let pending = self.pending.as_mut().expect("matching transition exists");
        pending.prepared = true;
        pending.sender_ratchet_pending = true;
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

    fn encrypt(&mut self, frame: &[u8]) -> Result<Vec<u8>, Failure> {
        if self.active_version != MAX_PROTOCOL_VERSION || !self.backend.is_ready() {
            return Err(Failure::InvalidState);
        }
        guarded(|| self.backend.encrypt_opus(frame)).map(|encrypted| encrypted.into_owned())
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

#[cfg(test)]
mod tests {
    use super::*;

    const UPSTREAM_FIXTURES: &str =
        include_str!("../../../vendor/davey/fixtures/upstream_session_fixtures.py");
    const MY_USER_ID: u64 = 158_049_329_150_427_136;
    const OTHER_USER_ID: u64 = 158_533_742_254_751_744;
    const CHANNEL_ID: u64 = 927_310_423_890_473_011;

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
        assert!(!core.snapshot().ready);
    }

    #[test]
    fn pinned_mls_fixture_reaches_execute_and_encrypts_participant_silence() {
        let external_sender = fixture("EXTERNAL_SENDER", "APPENDING_PROPOSALS");
        let proposals = fixture("APPENDING_PROPOSALS", "REVOKING_PROPOSALS");
        let mut core = Core::new(MY_USER_ID, CHANNEL_ID).unwrap();
        core.control(Control::Roster(vec![MY_USER_ID, OTHER_USER_ID]))
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
                Some(&[MY_USER_ID, OTHER_USER_ID]),
            )
            .unwrap()
            .expect("fixture creates a commit");
        let mut announced_commit = 7_u16.to_be_bytes().to_vec();
        announced_commit.extend_from_slice(&response.commit);
        let actions = core.control(Control::Commit(announced_commit)).unwrap();
        assert!(matches!(
            actions.as_slice(),
            [Outbound::Json { opcode: 23, .. }]
        ));
        assert!(!core.snapshot().ready);

        core.control(Control::ExecuteTransition { id: 7 }).unwrap();
        assert!(core.snapshot().ready);
        let encrypted = core.encrypt(&davey::OPUS_SILENCE_PACKET).unwrap();
        assert_ne!(encrypted, davey::OPUS_SILENCE_PACKET);
        assert_eq!(&encrypted[encrypted.len() - 2..], &[0xFA, 0xFA]);
    }
}
