use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PacketRecord {
    pub at: Duration,
    pub peer: SocketAddr,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct PacketCapture {
    inner: Arc<Mutex<VecDeque<PacketRecord>>>,
    capacity: usize,
    max_packet_bytes: usize,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CaptureError {
    #[error("packet capture capacity and maximum packet size must be nonzero")]
    InvalidCapacity,
    #[error("packet capture is full at capacity {capacity}")]
    Full { capacity: usize },
    #[error("packet has {actual} bytes, exceeding capture maximum {maximum}")]
    PacketTooLarge { actual: usize, maximum: usize },
}

impl PacketCapture {
    pub fn new(capacity: usize, max_packet_bytes: usize) -> Result<Self, CaptureError> {
        if capacity == 0 || max_packet_bytes == 0 {
            return Err(CaptureError::InvalidCapacity);
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(VecDeque::with_capacity(capacity))),
            capacity,
            max_packet_bytes,
        })
    }

    pub fn record(&self, record: PacketRecord) -> Result<(), CaptureError> {
        if record.bytes.len() > self.max_packet_bytes {
            return Err(CaptureError::PacketTooLarge {
                actual: record.bytes.len(),
                maximum: self.max_packet_bytes,
            });
        }
        let mut records = self.inner.lock().expect("packet capture mutex poisoned");
        if records.len() == self.capacity {
            return Err(CaptureError::Full {
                capacity: self.capacity,
            });
        }
        records.push_back(record);
        Ok(())
    }

    #[must_use]
    pub fn snapshot(&self) -> Vec<PacketRecord> {
        self.inner
            .lock()
            .expect("packet capture mutex poisoned")
            .iter()
            .cloned()
            .collect()
    }

    #[must_use]
    pub fn drain(&self) -> Vec<PacketRecord> {
        self.inner
            .lock()
            .expect("packet capture mutex poisoned")
            .drain(..)
            .collect()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("packet capture mutex poisoned")
            .len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_policy_rejects_new_records() {
        let capture = PacketCapture::new(1, 8).unwrap();
        let record = PacketRecord {
            at: Duration::ZERO,
            peer: "127.0.0.1:1".parse().unwrap(),
            bytes: vec![1],
        };
        capture.record(record.clone()).unwrap();
        assert_eq!(
            capture.record(record),
            Err(CaptureError::Full { capacity: 1 })
        );
    }

    #[test]
    fn oversized_record_is_rejected_without_consuming_capacity() {
        let capture = PacketCapture::new(1, 1).unwrap();
        let record = PacketRecord {
            at: Duration::ZERO,
            peer: "127.0.0.1:1".parse().unwrap(),
            bytes: vec![1, 2],
        };
        assert_eq!(
            capture.record(record),
            Err(CaptureError::PacketTooLarge {
                actual: 2,
                maximum: 1
            })
        );
        assert!(capture.is_empty());
    }
}
