use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep_until, timeout};

const MAX_PACER_SHARDS: usize = 4;
const CONTROL_CAPACITY: usize = 64;
const CONTROL_TIMEOUT: Duration = Duration::from_secs(1);
pub(crate) const FRAME_PERIOD: Duration = Duration::from_millis(20);

#[derive(Clone)]
pub(crate) struct Pacer {
    inner: Arc<PacerInner>,
}

struct PacerInner {
    next_id: AtomicU64,
    shards: usize,
    coordinators: Mutex<Option<Vec<CoordinatorHandle>>>,
}

struct CoordinatorHandle {
    commands: mpsc::Sender<CoordinatorCommand>,
    cancellations: Arc<Notify>,
    task: JoinHandle<()>,
}

impl Drop for PacerInner {
    fn drop(&mut self) {
        if let Some(coordinators) = self
            .coordinators
            .get_mut()
            .expect("pacer coordinator mutex poisoned")
            .take()
        {
            for coordinator in coordinators {
                coordinator.task.abort();
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PacerFailure {
    Exhausted,
    Overloaded,
    Closed,
}

pub(crate) struct PacerRegistration {
    id: u64,
    commands: mpsc::Sender<CoordinatorCommand>,
    deadlines: Option<mpsc::Receiver<Instant>>,
    skipped: Arc<AtomicU64>,
    live: Arc<AtomicBool>,
    cancellations: Arc<Notify>,
    registered: bool,
}

impl Pacer {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(PacerInner {
                next_id: AtomicU64::new(1),
                shards: std::thread::available_parallelism().map_or(1, |parallelism| {
                    parallelism.get().clamp(1, MAX_PACER_SHARDS)
                }),
                coordinators: Mutex::new(None),
            }),
        }
    }

    pub(crate) async fn register(&self) -> Result<PacerRegistration, PacerFailure> {
        let id = self
            .inner
            .next_id
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map_err(|_| PacerFailure::Exhausted)?;
        let (commands, cancellations) = {
            let coordinator = self.coordinator(id)?;
            (
                coordinator.commands.clone(),
                coordinator.cancellations.clone(),
            )
        };
        let (deadlines, receiver) = mpsc::channel(1);
        let skipped = Arc::new(AtomicU64::new(0));
        let live = Arc::new(AtomicBool::new(true));
        let (reply, response) = oneshot::channel();
        timed_control(
            &commands,
            CoordinatorCommand::Register {
                id,
                deadlines,
                skipped: skipped.clone(),
                live: live.clone(),
                reply,
            },
        )
        .await?;
        response.await.map_err(|_| PacerFailure::Closed)?;
        Ok(PacerRegistration {
            id,
            commands,
            deadlines: Some(receiver),
            skipped,
            live,
            cancellations,
            registered: true,
        })
    }

    fn coordinator(&self, id: u64) -> Result<CoordinatorRef<'_>, PacerFailure> {
        let mut coordinators = self
            .inner
            .coordinators
            .lock()
            .expect("pacer coordinator mutex poisoned");
        if coordinators.is_none() {
            if tokio::runtime::Handle::try_current().is_err() {
                return Err(PacerFailure::Closed);
            }
            let mut started = Vec::with_capacity(self.inner.shards);
            for _ in 0..self.inner.shards {
                let (commands, receiver) = mpsc::channel(CONTROL_CAPACITY);
                let cancellations = Arc::new(Notify::new());
                started.push(CoordinatorHandle {
                    commands,
                    cancellations: cancellations.clone(),
                    task: tokio::spawn(run_coordinator(receiver, cancellations)),
                });
            }
            *coordinators = Some(started);
        }
        Ok(CoordinatorRef {
            guard: coordinators,
            index: id as usize % self.inner.shards,
        })
    }
}

struct CoordinatorRef<'a> {
    guard: std::sync::MutexGuard<'a, Option<Vec<CoordinatorHandle>>>,
    index: usize,
}

impl std::ops::Deref for CoordinatorRef<'_> {
    type Target = CoordinatorHandle;

    fn deref(&self) -> &Self::Target {
        &self.guard.as_ref().expect("coordinators initialized")[self.index]
    }
}

impl PacerRegistration {
    pub(crate) fn take_deadlines(&mut self) -> mpsc::Receiver<Instant> {
        self.deadlines
            .take()
            .expect("pacer deadline receiver taken once")
    }

    pub(crate) async fn activate(&self, first: Instant) -> Result<(), PacerFailure> {
        let (reply, response) = oneshot::channel();
        timed_control(
            &self.commands,
            CoordinatorCommand::Activate {
                id: self.id,
                first,
                reply,
            },
        )
        .await?;
        response.await.map_err(|_| PacerFailure::Closed)?;
        Ok(())
    }

    pub(crate) async fn deactivate(&self) -> Result<(), PacerFailure> {
        let (reply, response) = oneshot::channel();
        timed_control(
            &self.commands,
            CoordinatorCommand::Deactivate { id: self.id, reply },
        )
        .await?;
        response.await.map_err(|_| PacerFailure::Closed)?;
        Ok(())
    }

    pub(crate) fn skipped(&self) -> u64 {
        self.skipped.load(Ordering::Relaxed)
    }

    pub(crate) async fn unregister(&mut self) -> Result<(), PacerFailure> {
        if !self.registered {
            return Ok(());
        }
        self.live.store(false, Ordering::Release);
        self.cancellations.notify_one();
        let (reply, response) = oneshot::channel();
        timed_control(
            &self.commands,
            CoordinatorCommand::Unregister {
                id: self.id,
                reply: Some(reply),
            },
        )
        .await?;
        response.await.map_err(|_| PacerFailure::Closed)?;
        self.registered = false;
        Ok(())
    }
}

impl Drop for PacerRegistration {
    fn drop(&mut self) {
        if self.registered {
            self.live.store(false, Ordering::Release);
            self.cancellations.notify_one();
        }
    }
}

async fn timed_control(
    sender: &mpsc::Sender<CoordinatorCommand>,
    command: CoordinatorCommand,
) -> Result<(), PacerFailure> {
    match timeout(CONTROL_TIMEOUT, sender.send(command)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(PacerFailure::Closed),
        Err(_) => Err(PacerFailure::Overloaded),
    }
}

enum CoordinatorCommand {
    Register {
        id: u64,
        deadlines: mpsc::Sender<Instant>,
        skipped: Arc<AtomicU64>,
        live: Arc<AtomicBool>,
        reply: oneshot::Sender<()>,
    },
    Activate {
        id: u64,
        first: Instant,
        reply: oneshot::Sender<()>,
    },
    Deactivate {
        id: u64,
        reply: oneshot::Sender<()>,
    },
    Unregister {
        id: u64,
        reply: Option<oneshot::Sender<()>>,
    },
}

struct Slot {
    deadlines: mpsc::Sender<Instant>,
    skipped: Arc<AtomicU64>,
    live: Arc<AtomicBool>,
    next: Option<Instant>,
}

async fn run_coordinator(
    mut commands: mpsc::Receiver<CoordinatorCommand>,
    cancellations: Arc<Notify>,
) {
    let mut slots = HashMap::<u64, Slot>::new();
    loop {
        let next = slots.values().filter_map(|slot| slot.next).min();
        tokio::select! {
            biased;
            command = commands.recv() => {
                let Some(command) = command else { return; };
                apply_command(command, &mut slots);
            }
            () = cancellations.notified() => {
                slots.retain(|_, slot| slot.live.load(Ordering::Acquire));
            }
            _ = async {
                match next {
                    Some(deadline) => sleep_until(deadline).await,
                    None => std::future::pending::<()>().await,
                }
            } => dispatch_due(&mut slots),
        }
    }
}

fn apply_command(command: CoordinatorCommand, slots: &mut HashMap<u64, Slot>) {
    match command {
        CoordinatorCommand::Register {
            id,
            deadlines,
            skipped,
            live,
            reply,
        } => {
            slots.insert(
                id,
                Slot {
                    deadlines,
                    skipped,
                    live,
                    next: None,
                },
            );
            if reply.send(()).is_err() {
                slots.remove(&id);
            }
        }
        CoordinatorCommand::Activate { id, first, reply } => {
            if let Some(slot) = slots.get_mut(&id) {
                slot.next = Some(first);
            }
            let _ = reply.send(());
        }
        CoordinatorCommand::Deactivate { id, reply } => {
            if let Some(slot) = slots.get_mut(&id) {
                slot.next = None;
            }
            let _ = reply.send(());
        }
        CoordinatorCommand::Unregister { id, reply } => {
            slots.remove(&id);
            if let Some(reply) = reply {
                let _ = reply.send(());
            }
        }
    }
}

fn dispatch_due(slots: &mut HashMap<u64, Slot>) {
    let now = Instant::now();
    slots.retain(|_, slot| {
        if !slot.live.load(Ordering::Acquire) {
            return false;
        }
        let Some(deadline) = slot.next else {
            return true;
        };
        if deadline > now {
            return true;
        }
        match slot.deadlines.try_send(deadline) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                slot.skipped.fetch_add(1, Ordering::Relaxed);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => return false,
        }
        slot.next = Some(advance_deadline(deadline, now, &slot.skipped));
        true
    });
}

fn advance_deadline(deadline: Instant, now: Instant, skipped: &AtomicU64) -> Instant {
    let next = deadline + FRAME_PERIOD;
    if now < next {
        return next;
    }
    let periods = now.duration_since(next).as_nanos() / FRAME_PERIOD.as_nanos() + 1;
    skipped.fetch_add(periods as u64, Ordering::Relaxed);
    next + FRAME_PERIOD.mul_f64(periods as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn settle() {
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_executor_gets_one_signal_and_never_a_catch_up_burst() {
        let pacer = Pacer::new();
        let mut registration = pacer.register().await.expect("sender registers");
        let mut deadlines = registration.take_deadlines();
        registration
            .activate(Instant::now() + FRAME_PERIOD)
            .await
            .expect("sender activates");

        tokio::time::advance(FRAME_PERIOD * 8).await;
        settle().await;
        let first = deadlines.try_recv().expect("one due signal is retained");
        assert!(first <= Instant::now());
        assert!(
            deadlines.try_recv().is_err(),
            "a capacity-one signal cannot become a catch-up packet burst"
        );
        assert!(registration.skipped() >= 7);

        tokio::time::advance(FRAME_PERIOD).await;
        settle().await;
        assert!(deadlines.try_recv().is_ok(), "the next live period resumes");
        assert!(deadlines.try_recv().is_err());
        registration.unregister().await.expect("sender unregisters");
    }

    #[tokio::test(start_paused = true)]
    async fn full_sender_slot_cannot_block_an_unrelated_sender_on_the_same_shard() {
        let pacer = Pacer::new();
        let mut registrations = Vec::new();
        for _ in 0..5 {
            registrations.push(pacer.register().await.expect("sender registers"));
        }
        let mut blocked_deadlines = registrations[0].take_deadlines();
        let mut healthy_deadlines = registrations[4].take_deadlines();
        let first = Instant::now() + FRAME_PERIOD;
        registrations[0]
            .activate(first)
            .await
            .expect("blocked sender activates");
        registrations[4]
            .activate(first)
            .await
            .expect("healthy sender activates");

        tokio::time::advance(FRAME_PERIOD).await;
        settle().await;
        assert!(blocked_deadlines.try_recv().is_ok());
        assert!(healthy_deadlines.try_recv().is_ok());

        for _ in 0..4 {
            tokio::time::advance(FRAME_PERIOD).await;
            settle().await;
            assert!(
                healthy_deadlines.try_recv().is_ok(),
                "healthy sender remains scheduled while its shard peer is full"
            );
        }
        assert!(registrations[0].skipped() >= 3);

        for registration in &mut registrations {
            registration.unregister().await.expect("sender unregisters");
        }
    }
}
