//! Opt-in bounded RTP header/timing evidence. Payloads and keys never enter it.
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;
use tokio::time::Instant;

/// One successfully submitted UDP packet. Success is local submission, not delivery.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SendRecord {
    /// Monotonic record number, including records dropped by a full ring.
    pub index: u64,
    /// Local successful-send time relative to the trace epoch.
    pub elapsed_micros: u64,
    /// Voice transport generation, disambiguating reconnects.
    pub connection_generation: u64,
    /// Audio source generation, disambiguating replacements.
    pub source_generation: u64,
    /// RTP synchronization source identifier.
    pub ssrc: u32,
    /// RTP media timestamp; wraps at 32 bits.
    pub timestamp: u32,
    /// RTP sequence number; wraps at 16 bits.
    pub sequence: u16,
    /// Whether this was one of Oto's terminal/underflow silence packets.
    pub silence: bool,
}

/// Single-consumer trace. Drain outside the audio task; dropping it disables tracing.
pub struct SendTrace {
    consumer: rtrb::Consumer<SendRecord>,
    dropped: Arc<AtomicU64>,
    started_at: SystemTime,
}

impl SendTrace {
    /// Removes the next record, or returns `None` when the ring is empty.
    pub fn pop(&mut self) -> Option<SendRecord> {
        self.consumer.pop().ok()
    }
    /// Records omitted because the bounded ring was full; audio is unaffected.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
    /// Wall-clock anchor sampled immediately before the monotonic trace epoch.
    pub fn started_at(&self) -> SystemTime {
        self.started_at
    }
}

pub(crate) struct TraceWriter {
    producer: rtrb::Producer<SendRecord>,
    dropped: Arc<AtomicU64>,
    started: Instant,
    index: u64,
}

pub(crate) fn pair(capacity: usize) -> (TraceWriter, SendTrace) {
    let (producer, consumer) = rtrb::RingBuffer::new(capacity);
    let dropped = Arc::new(AtomicU64::new(0));
    let started_at = SystemTime::now();
    (
        TraceWriter {
            producer,
            dropped: dropped.clone(),
            started: Instant::now(),
            index: 0,
        },
        SendTrace {
            consumer,
            dropped,
            started_at,
        },
    )
}

impl TraceWriter {
    // Header is the actual encrypted datagram's unencrypted 12-byte RTP header.
    pub(crate) fn record(
        &mut self,
        now: Instant,
        header: &[u8],
        connection: u64,
        source: u64,
        silence: bool,
    ) -> bool {
        if self.producer.is_abandoned() {
            return false;
        }
        self.index = self.index.saturating_add(1);
        let record = SendRecord {
            index: self.index,
            elapsed_micros: now
                .saturating_duration_since(self.started)
                .as_micros()
                .min(u128::from(u64::MAX)) as u64,
            connection_generation: connection,
            source_generation: source,
            ssrc: u32::from_be_bytes(header[8..12].try_into().expect("RTP SSRC")),
            timestamp: u32::from_be_bytes(header[4..8].try_into().expect("RTP timestamp")),
            sequence: u16::from_be_bytes(header[2..4].try_into().expect("RTP sequence")),
            silence,
        };
        if self.producer.push(record).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn bounded_trace_preserves_headers_and_exposes_missing_records() {
        let (mut writer, mut reader) = pair(1);
        let header = [0x80, 0x78, 0xff, 0xff, 0, 0, 3, 0xc0, 0, 0, 0, 7];
        let now = Instant::now();
        assert!(writer.record(now, &header, 4, 8, false));
        assert!(writer.record(now, &header, 4, 8, false));
        assert_eq!(reader.dropped(), 1);
        let first = reader.pop().unwrap();
        assert_eq!(
            (first.index, first.sequence, first.timestamp, first.ssrc),
            (1, 65535, 960, 7)
        );
        assert_eq!(
            (first.connection_generation, first.source_generation),
            (4, 8)
        );
        assert!(!first.silence);
        tokio::time::advance(std::time::Duration::from_millis(20)).await;
        writer.record(Instant::now(), &header, 5, 9, true);
        let third = reader.pop().unwrap();
        assert_eq!(third.index, 3);
        assert_eq!(third.elapsed_micros, 20000);
        assert!(third.silence);
        drop(reader);
        assert!(!writer.record(Instant::now(), &header, 5, 9, false));
    }
}
