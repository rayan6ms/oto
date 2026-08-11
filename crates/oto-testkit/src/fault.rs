use std::collections::VecDeque;
use std::time::Duration;

use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FaultAction {
    Pass,
    Drop,
    Delay(Duration),
    Reorder,
    Replace(Vec<u8>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScheduledPacket {
    pub due: Duration,
    pub bytes: Vec<u8>,
}

#[derive(Debug)]
pub struct FaultInjector {
    actions: VecDeque<FaultAction>,
    action_capacity: usize,
    max_packet_bytes: usize,
    held_for_reorder: Option<Vec<u8>>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum FaultError {
    #[error("fault action capacity and maximum packet size must be nonzero")]
    InvalidCapacity,
    #[error("fault action schedule is full at capacity {capacity}")]
    ScheduleFull { capacity: usize },
    #[error("packet has {actual} bytes, exceeding fault-injector maximum {maximum}")]
    PacketTooLarge { actual: usize, maximum: usize },
    #[error("reorder slot is already occupied")]
    ReorderSlotFull,
    #[error("fault delivery time overflow")]
    TimeOverflow,
}

impl FaultInjector {
    pub fn new(action_capacity: usize, max_packet_bytes: usize) -> Result<Self, FaultError> {
        if action_capacity == 0 || max_packet_bytes == 0 {
            return Err(FaultError::InvalidCapacity);
        }
        Ok(Self {
            actions: VecDeque::with_capacity(action_capacity),
            action_capacity,
            max_packet_bytes,
            held_for_reorder: None,
        })
    }

    pub fn push(&mut self, action: FaultAction) -> Result<(), FaultError> {
        if let FaultAction::Replace(bytes) = &action {
            self.check_packet(bytes)?;
        }
        if self.actions.len() == self.action_capacity {
            return Err(FaultError::ScheduleFull {
                capacity: self.action_capacity,
            });
        }
        self.actions.push_back(action);
        Ok(())
    }

    pub fn apply(
        &mut self,
        now: Duration,
        packet: Vec<u8>,
    ) -> Result<Vec<ScheduledPacket>, FaultError> {
        self.check_packet(&packet)?;
        let action = self.actions.pop_front().unwrap_or(FaultAction::Pass);
        if action == FaultAction::Reorder {
            if self.held_for_reorder.is_some() {
                return Err(FaultError::ReorderSlotFull);
            }
            self.held_for_reorder = Some(packet);
            return Ok(Vec::new());
        }

        let mut scheduled = Vec::with_capacity(2);
        match action {
            FaultAction::Pass => scheduled.push(ScheduledPacket {
                due: now,
                bytes: packet,
            }),
            FaultAction::Drop => {}
            FaultAction::Delay(delay) => scheduled.push(ScheduledPacket {
                due: now.checked_add(delay).ok_or(FaultError::TimeOverflow)?,
                bytes: packet,
            }),
            FaultAction::Replace(bytes) => scheduled.push(ScheduledPacket { due: now, bytes }),
            FaultAction::Reorder => unreachable!("reorder handled above"),
        }

        if let Some(held) = self.held_for_reorder.take() {
            scheduled.push(ScheduledPacket {
                due: now,
                bytes: held,
            });
        }
        Ok(scheduled)
    }

    #[must_use]
    pub fn flush_reordered(&mut self, now: Duration) -> Option<ScheduledPacket> {
        self.held_for_reorder
            .take()
            .map(|bytes| ScheduledPacket { due: now, bytes })
    }

    fn check_packet(&self, packet: &[u8]) -> Result<(), FaultError> {
        if packet.len() > self.max_packet_bytes {
            return Err(FaultError::PacketTooLarge {
                actual: packet.len(),
                maximum: self.max_packet_bytes,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drop_delay_replace_and_reorder_are_deterministic() {
        let mut faults = FaultInjector::new(8, 32).unwrap();
        faults.push(FaultAction::Drop).unwrap();
        faults
            .push(FaultAction::Delay(Duration::from_secs(3)))
            .unwrap();
        faults.push(FaultAction::Replace(vec![9])).unwrap();
        faults.push(FaultAction::Reorder).unwrap();
        faults.push(FaultAction::Pass).unwrap();

        assert!(faults.apply(Duration::ZERO, vec![1]).unwrap().is_empty());
        assert_eq!(
            faults.apply(Duration::ZERO, vec![2]).unwrap(),
            [ScheduledPacket {
                due: Duration::from_secs(3),
                bytes: vec![2]
            }]
        );
        assert_eq!(faults.apply(Duration::ZERO, vec![3]).unwrap()[0].bytes, [9]);
        assert!(faults.apply(Duration::ZERO, vec![4]).unwrap().is_empty());
        let reordered = faults.apply(Duration::ZERO, vec![5]).unwrap();
        assert_eq!(reordered[0].bytes, [5]);
        assert_eq!(reordered[1].bytes, [4]);
    }

    #[test]
    fn action_queue_is_bounded() {
        let mut faults = FaultInjector::new(1, 8).unwrap();
        faults.push(FaultAction::Pass).unwrap();
        assert_eq!(
            faults.push(FaultAction::Drop),
            Err(FaultError::ScheduleFull { capacity: 1 })
        );
    }

    #[test]
    fn packet_and_reorder_slot_bounds_fail_explicitly() {
        let mut faults = FaultInjector::new(2, 1).unwrap();
        assert_eq!(
            faults.apply(Duration::ZERO, vec![1, 2]),
            Err(FaultError::PacketTooLarge {
                actual: 2,
                maximum: 1
            })
        );
        faults.push(FaultAction::Reorder).unwrap();
        faults.push(FaultAction::Reorder).unwrap();
        assert!(faults.apply(Duration::ZERO, vec![1]).unwrap().is_empty());
        assert_eq!(
            faults.apply(Duration::ZERO, vec![2]),
            Err(FaultError::ReorderSlotFull)
        );
    }
}
