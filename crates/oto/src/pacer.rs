use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::time::{Instant, sleep_until};

pub(crate) const FRAME_PERIOD: Duration = Duration::from_millis(20);

/// The audio executor owns its timer, avoiding a coordinator/channel wakeup
/// for every packet. No extra task, thread, or queue is created per sender.
#[derive(Clone)]
pub(crate) struct Pacer {
    next_id: Arc<AtomicU64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PacerFailure {
    Exhausted,
    Closed,
}

pub(crate) struct PacerRegistration {
    next: Option<Instant>,
    skipped: u64,
    observed_missed: u64,
    registered: bool,
}

impl Pacer {
    pub(crate) fn new() -> Self {
        Self {
            next_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub(crate) async fn register(&self) -> Result<PacerRegistration, PacerFailure> {
        if tokio::runtime::Handle::try_current().is_err() {
            return Err(PacerFailure::Closed);
        }
        self.next_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| PacerFailure::Exhausted)?;
        // Registration has no suspension point and no shared slot to roll back.
        Ok(PacerRegistration {
            next: None,
            skipped: 0,
            observed_missed: 0,
            registered: true,
        })
    }
}

impl PacerRegistration {
    /// Cancellation-safe: a losing select branch leaves the deadline unchanged.
    /// A late wake permits one frame, never a backlog of missed opportunities.
    pub(crate) async fn next_deadline(&mut self) -> Option<Instant> {
        if !self.registered {
            return None;
        }
        let Some(deadline) = self.next else {
            return std::future::pending().await;
        };
        sleep_until(deadline).await;
        let now = Instant::now();
        let missed = now.saturating_duration_since(deadline).as_nanos() / FRAME_PERIOD.as_nanos();
        self.observed_missed = missed.min(u128::from(u64::MAX)) as u64;
        self.skipped = self.skipped.saturating_add(self.observed_missed);
        self.next = Some(if missed > 0 {
            now + FRAME_PERIOD
        } else {
            deadline + FRAME_PERIOD
        });
        Some(deadline)
    }

    /// Include opportunities missed during polling/encryption/I/O, without
    /// counting the scheduling delay twice. Completion may also rebase the timer.
    pub(crate) fn complete(&mut self, deadline: Instant, completed: Instant) {
        let missed = (completed.saturating_duration_since(deadline).as_nanos()
            / FRAME_PERIOD.as_nanos())
        .min(u128::from(u64::MAX)) as u64;
        self.skipped = self
            .skipped
            .saturating_add(missed.saturating_sub(self.observed_missed));
        self.observed_missed = 0;
        if missed > 0 {
            self.next = Some(completed + FRAME_PERIOD);
        }
    }

    pub(crate) async fn activate(&mut self, first: Instant) -> Result<(), PacerFailure> {
        if !self.registered {
            return Err(PacerFailure::Closed);
        }
        self.next = Some(first);
        self.observed_missed = 0;
        Ok(())
    }

    pub(crate) async fn deactivate(&mut self) -> Result<(), PacerFailure> {
        self.next = None;
        Ok(())
    }

    pub(crate) fn skipped(&self) -> u64 {
        self.skipped
    }

    pub(crate) async fn unregister(&mut self) -> Result<(), PacerFailure> {
        self.next = None;
        self.registered = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;

    #[tokio::test(start_paused = true)]
    async fn cancelled_wait_does_not_advance_or_lose_deadline() {
        let mut p = Pacer::new().register().await.unwrap();
        let first = Instant::now() + FRAME_PERIOD;
        p.activate(first).await.unwrap();
        for _ in 0..4 {
            assert!(p.next_deadline().now_or_never().is_none());
            tokio::time::advance(Duration::from_millis(4)).await;
        }
        assert_eq!(p.next_deadline().await, Some(first));
        assert_eq!(p.skipped(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_executor_recovers_once_without_catchup() {
        let mut p = Pacer::new().register().await.unwrap();
        let first = Instant::now() + FRAME_PERIOD;
        p.activate(first).await.unwrap();
        tokio::time::advance(FRAME_PERIOD * 8).await;
        assert_eq!(p.next_deadline().await, Some(first));
        assert_eq!(p.skipped(), 7);
        assert!(p.next_deadline().now_or_never().is_none());
        let next = Instant::now() + FRAME_PERIOD;
        assert_eq!(p.next_deadline().await, Some(next));
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_sender_cannot_delay_another_sender() {
        let owner = Pacer::new();
        let mut stalled = owner.register().await.unwrap();
        let mut healthy = owner.register().await.unwrap();
        let first = Instant::now() + FRAME_PERIOD;
        stalled.activate(first).await.unwrap();
        healthy.activate(first).await.unwrap();
        for n in 0..5 {
            assert_eq!(
                healthy.next_deadline().await,
                Some(first + FRAME_PERIOD * n)
            );
        }
        assert_eq!(healthy.skipped(), 0);
        assert_eq!(stalled.next_deadline().await, Some(first));
        assert_eq!(stalled.skipped(), 4);
    }

    #[tokio::test(start_paused = true)]
    async fn deactivate_and_rearm_cannot_deliver_an_old_tick() {
        let mut p = Pacer::new().register().await.unwrap();
        p.activate(Instant::now()).await.unwrap();
        p.deactivate().await.unwrap();
        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(p.next_deadline().now_or_never().is_none());
        let first = Instant::now() + FRAME_PERIOD;
        p.activate(first).await.unwrap();
        assert_eq!(p.next_deadline().await, Some(first));
        assert_eq!(p.skipped(), 0, "idle time is not missed active audio");
        p.unregister().await.unwrap();
        assert_eq!(p.next_deadline().await, None);
        assert_eq!(p.activate(first).await, Err(PacerFailure::Closed));
    }

    #[tokio::test(start_paused = true)]
    async fn slow_frame_work_counts_missed_periods_without_double_counting() {
        let mut p = Pacer::new().register().await.unwrap();
        let first = Instant::now();
        p.activate(first).await.unwrap();
        tokio::time::advance(Duration::from_millis(45)).await;
        assert_eq!(p.next_deadline().await, Some(first));
        assert_eq!(p.skipped(), 2);
        tokio::time::advance(Duration::from_millis(40)).await;
        p.complete(first, Instant::now());
        assert_eq!(p.skipped(), 4);
        assert!(p.next_deadline().now_or_never().is_none());
        assert_eq!(
            p.next_deadline().await,
            Some(first + Duration::from_millis(105))
        );
    }
}
