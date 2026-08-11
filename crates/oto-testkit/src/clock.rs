use std::sync::{Arc, Mutex};
use std::time::Duration;

use thiserror::Error;
use tokio::sync::watch;

#[derive(Clone, Debug)]
pub struct ManualClock {
    inner: Arc<ManualClockInner>,
}

#[derive(Debug)]
struct ManualClockInner {
    now: Mutex<Duration>,
    updates: watch::Sender<Duration>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ManualClockError {
    #[error("manual clock cannot move backwards from {current:?} to {requested:?}")]
    WentBackwards {
        current: Duration,
        requested: Duration,
    },
    #[error("manual clock overflow")]
    Overflow,
    #[error("manual clock update channel closed")]
    Closed,
}

impl ManualClock {
    #[must_use]
    pub fn new(start: Duration) -> Self {
        let (updates, _) = watch::channel(start);
        Self {
            inner: Arc::new(ManualClockInner {
                now: Mutex::new(start),
                updates,
            }),
        }
    }

    #[must_use]
    pub fn now(&self) -> Duration {
        *self.inner.now.lock().expect("manual clock mutex poisoned")
    }

    pub fn advance(&self, delta: Duration) -> Result<Duration, ManualClockError> {
        let requested = self
            .now()
            .checked_add(delta)
            .ok_or(ManualClockError::Overflow)?;
        self.set(requested)?;
        Ok(requested)
    }

    pub fn set(&self, requested: Duration) -> Result<(), ManualClockError> {
        let mut now = self.inner.now.lock().expect("manual clock mutex poisoned");
        if requested < *now {
            return Err(ManualClockError::WentBackwards {
                current: *now,
                requested,
            });
        }
        *now = requested;
        self.inner.updates.send_replace(requested);
        Ok(())
    }

    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<Duration> {
        self.inner.updates.subscribe()
    }

    pub async fn sleep_until(&self, deadline: Duration) -> Result<(), ManualClockError> {
        let mut updates = self.subscribe();
        loop {
            if *updates.borrow_and_update() >= deadline {
                return Ok(());
            }
            updates
                .changed()
                .await
                .map_err(|_| ManualClockError::Closed)?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn wakes_only_after_deadline_without_wall_clock_sleep() {
        let clock = ManualClock::new(Duration::from_secs(10));
        let waiter_clock = clock.clone();
        let waiter =
            tokio::spawn(async move { waiter_clock.sleep_until(Duration::from_secs(15)).await });

        tokio::task::yield_now().await;
        clock.advance(Duration::from_secs(4)).unwrap();
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        clock.advance(Duration::from_secs(1)).unwrap();
        waiter.await.unwrap().unwrap();
    }

    #[test]
    fn rejects_backward_time() {
        let clock = ManualClock::new(Duration::from_secs(2));
        assert!(matches!(
            clock.set(Duration::from_secs(1)),
            Err(ManualClockError::WentBackwards { .. })
        ));
    }
}
