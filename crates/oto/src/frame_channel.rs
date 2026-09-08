//! A capacity-one encoded-frame handoff with bounded copying.
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll};
use std::time::Duration;

use crate::{FrameSource, FrameStatus};
use futures_util::task::AtomicWaker;
use rtrb::{Consumer, Producer, RingBuffer};

const MAX_FRAME: usize = 1275;

/// Creates a capacity-one channel for 20 ms, 48 kHz stereo encoded Opus frames.
///
/// No codec conversion or packet scheduling occurs here. Full-channel sends
/// wait for consumption. The concrete owned audio attachment copies the frame
/// then wakes its producer task. Ordinary `FrameSource` attachment keeps task
/// wake work outside its poll and still applies the callback watchdog.
pub fn frame_channel() -> (FrameWriter, FrameReader) {
    let (writer, reader) = RingBuffer::new(1);
    let shared = Arc::new(Shared {
        consumed: AtomicBool::new(false),
        ended: AtomicBool::new(false),
        closed: AtomicBool::new(false),
        undersized: AtomicBool::new(false),
        waker: AtomicWaker::new(),
        owned: AtomicBool::new(false),
        producer_waker: AtomicWaker::new(),
    });
    (
        FrameWriter {
            queue: writer,
            shared: shared.clone(),
            in_flight: false,
        },
        FrameReader {
            queue: reader,
            shared,
        },
    )
}

/// The single producer of an encoded-frame channel.
/// Dropping it publishes EOF after any already queued frame.
pub struct FrameWriter {
    queue: Producer<Frame>,
    shared: Arc<Shared>,
    in_flight: bool,
}

/// The single consumer of an encoded-frame channel.
/// Dropping it closes pending and subsequent writer sends.
pub struct FrameReader {
    queue: Consumer<Frame>,
    shared: Arc<Shared>,
}

/// A failed encoded-frame channel send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameSendError {
    /// Encoded length is outside 1..=1275 bytes; nothing was published.
    InvalidLength,
    /// The consumer was dropped; delivery of an in-flight frame is unknown.
    Closed,
    /// The consumer supplied an undersized buffer; no partial frame was copied.
    OutputTooSmall,
}

impl std::fmt::Display for FrameSendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidLength => "invalid encoded frame length",
            Self::Closed => "encoded frame consumer closed",
            Self::OutputTooSmall => "encoded frame output buffer too small",
        })
    }
}
impl std::error::Error for FrameSendError {}

struct Frame {
    len: usize,
    bytes: [u8; MAX_FRAME],
}
struct Shared {
    consumed: AtomicBool,
    ended: AtomicBool,
    closed: AtomicBool,
    undersized: AtomicBool,
    waker: AtomicWaker,
    owned: AtomicBool,
    producer_waker: AtomicWaker,
}

impl FrameWriter {
    /// Publishes one frame and waits until the reader copies it.
    ///
    /// Completion acknowledges a source read, not UDP delivery. Cancellation
    /// before publication changes nothing. Cancellation after publication keeps
    /// that frame queued; a subsequent send waits for it before publishing the
    /// next frame. Do not retry the cancelled frame unless duplication is wanted.
    /// The owned audio attachment wakes the producer after copying the frame.
    /// Ordinary `FrameSource` attachment checks consumption at 1 ms intervals,
    /// keeping runtime wake work outside arbitrary source callbacks.
    pub async fn send(&mut self, bytes: &[u8]) -> Result<(), FrameSendError> {
        if !(1..=MAX_FRAME).contains(&bytes.len()) {
            return Err(FrameSendError::InvalidLength);
        }
        self.wait_consumed().await?;
        self.check_open()?;
        let mut frame = Frame {
            len: bytes.len(),
            bytes: [0; MAX_FRAME],
        };
        frame.bytes[..bytes.len()].copy_from_slice(bytes);
        // Only this writer publishes, and the previous frame was acknowledged.
        self.in_flight = true;
        assert!(
            self.queue.push(frame).is_ok(),
            "frame channel capacity invariant"
        );
        self.shared.waker.wake();
        self.wait_consumed().await
    }

    fn check_open(&self) -> Result<(), FrameSendError> {
        self.shared.check_open()
    }

    async fn wait_consumed(&mut self) -> Result<(), FrameSendError> {
        while self.in_flight {
            self.check_open()?;
            if self.shared.consumed.swap(false, Ordering::AcqRel) {
                self.in_flight = false;
                break;
            }
            let shared = &self.shared;
            let owned = shared.owned.load(Ordering::Acquire);
            let ready = futures_util::future::poll_fn(|cx| {
                shared.producer_waker.register(cx.waker());
                // Register/recheck also covers close and attachment races.
                shared.check_open()?;
                if shared.consumed.load(Ordering::Acquire) {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                }
            });
            if owned {
                ready.await?;
            } else {
                tokio::select! {
                    result = ready => result?,
                    () = tokio::time::sleep(Duration::from_millis(1)) => {}
                }
            }
        }
        Ok(())
    }
}

impl Shared {
    fn check_open(&self) -> Result<(), FrameSendError> {
        if self.undersized.load(Ordering::Acquire) {
            return Err(FrameSendError::OutputTooSmall);
        }
        if self.closed.load(Ordering::Acquire) {
            return Err(FrameSendError::Closed);
        }
        Ok(())
    }
}

impl Drop for FrameWriter {
    fn drop(&mut self) {
        self.shared.ended.store(true, Ordering::Release);
        self.shared.waker.wake();
    }
}
impl Drop for FrameReader {
    fn drop(&mut self) {
        self.shared.closed.store(true, Ordering::Release);
        self.shared.producer_waker.wake();
    }
}

impl FrameReader {
    pub(crate) fn poll_owned(
        &mut self,
        cx: &mut Context<'_>,
        output: &mut [u8],
    ) -> Poll<FrameStatus> {
        self.shared.owned.store(true, Ordering::Release);
        let result = self.poll_frame(cx, output);
        // Only the concrete owned attachment runs scheduler work here. The
        // FrameSource implementation below still only copies and sets flags.
        if result.is_ready() {
            self.shared.producer_waker.wake();
        }
        result
    }

    fn try_frame(&mut self, output: &mut [u8]) -> Option<FrameStatus> {
        let frame = self.queue.pop().ok()?;
        if frame.len > output.len() {
            self.shared.undersized.store(true, Ordering::Release);
            return Some(FrameStatus::Ended);
        }
        output[..frame.len].copy_from_slice(&frame.bytes[..frame.len]);
        self.shared.consumed.store(true, Ordering::Release);
        Some(FrameStatus::Frame { len: frame.len })
    }
}

impl FrameSource for FrameReader {
    /// Copies one frame without blocking or invoking producer code. An
    /// undersized output permanently ends the channel and fails the writer.
    fn poll_frame(&mut self, cx: &mut Context<'_>, output: &mut [u8]) -> Poll<FrameStatus> {
        if self.shared.undersized.load(Ordering::Acquire) {
            return Poll::Ready(FrameStatus::Ended);
        }
        if let Some(frame) = self.try_frame(output) {
            return Poll::Ready(frame);
        }
        self.shared.waker.register(cx.waker());
        if let Some(frame) = self.try_frame(output) {
            return Poll::Ready(frame);
        }
        if self.shared.ended.load(Ordering::Acquire) {
            // EOF publication happens after the producer's final push.
            return Poll::Ready(self.try_frame(output).unwrap_or(FrameStatus::Ended));
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::task::noop_waker;
    use std::future::Future;
    use std::sync::atomic::AtomicUsize;
    use std::task::{Wake, Waker};

    fn read(reader: &mut FrameReader, output: &mut [u8]) -> Poll<FrameStatus> {
        reader.poll_frame(&mut Context::from_waker(&noop_waker()), output)
    }

    #[tokio::test(start_paused = true)]
    async fn owned_consumption_releases_producer_without_a_timer_tick() {
        let (mut writer, mut reader) = frame_channel();
        let task = tokio::spawn(async move {
            writer.send(&[1]).await.unwrap();
            writer.send(&[2]).await.unwrap();
        });
        tokio::task::yield_now().await;
        let mut output = [0; MAX_FRAME];
        assert_eq!(
            reader.poll_owned(&mut Context::from_waker(&noop_waker()), &mut output),
            Poll::Ready(FrameStatus::Frame { len: 1 })
        );
        assert_eq!(output[0], 1);
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            reader.poll_owned(&mut Context::from_waker(&noop_waker()), &mut output),
            Poll::Ready(FrameStatus::Frame { len: 1 }),
            "next frame must be published without waiting for a timer tick"
        );
        assert_eq!(output[0], 2);
        task.await.unwrap();
    }

    #[tokio::test]
    #[ignore = "bounded real-time owned handoff benchmark"]
    async fn benchmark_owned_consumption_wakes() {
        let (mut writer, mut reader) = frame_channel();
        let polls = Arc::new(AtomicUsize::new(0));
        let count = polls.clone();
        let producer = tokio::spawn(async move {
            for _ in 0..251 {
                let mut send = std::pin::pin!(writer.send(&[0x55; MAX_FRAME]));
                futures_util::future::poll_fn(|cx| {
                    count.fetch_add(1, Ordering::Relaxed);
                    send.as_mut().poll(cx)
                })
                .await
                .unwrap();
            }
        });
        let mut output = [0; MAX_FRAME];
        // Prime startup, then measure identical 20 ms consumption opportunities.
        futures_util::future::poll_fn(|cx| reader.poll_owned(cx, &mut output)).await;
        let before = polls.load(Ordering::Relaxed);
        let begin = tokio::time::Instant::now();
        let mut unavailable = 0;
        for frame in 1..=250 {
            tokio::time::sleep_until(begin + Duration::from_millis(frame * 20)).await;
            if reader
                .poll_owned(&mut Context::from_waker(&noop_waker()), &mut output)
                .is_pending()
            {
                unavailable += 1;
                futures_util::future::poll_fn(|cx| reader.poll_owned(cx, &mut output)).await;
            }
            assert_eq!(output, [0x55; MAX_FRAME]);
        }
        producer.await.unwrap();
        println!(
            "HANDOFF_BENCHMARK={}",
            serde_json::json!({"frames":250,"producerPolls":polls.load(Ordering::Relaxed)-before,"unavailable":unavailable,"elapsedMs":begin.elapsed().as_millis()})
        );
    }

    #[tokio::test(start_paused = true)]
    async fn owned_waiter_replacement_and_close_are_not_lost() {
        for undersized in [false, true] {
            let (mut writer, mut reader) = frame_channel();
            let mut output = [0; MAX_FRAME];
            assert!(
                reader
                    .poll_owned(&mut Context::from_waker(&noop_waker()), &mut output)
                    .is_pending()
            );
            let old = Arc::new(Counter::default());
            let new = Arc::new(Counter::default());
            let old_waker = Waker::from(old.clone());
            let new_waker = Waker::from(new.clone());
            let mut send = Box::pin(writer.send(&[1, 2, 3]));
            assert!(
                send.as_mut()
                    .poll(&mut Context::from_waker(&old_waker))
                    .is_pending()
            );
            // Cancelling the published send retains its bytes. The replacement
            // waiter must observe consumption/close without any timer fallback.
            drop(send);
            let mut next = Box::pin(writer.send(&[4]));
            assert!(
                next.as_mut()
                    .poll(&mut Context::from_waker(&new_waker))
                    .is_pending()
            );
            let expected = if undersized {
                assert_eq!(
                    reader.poll_owned(&mut Context::from_waker(&noop_waker()), &mut []),
                    Poll::Ready(FrameStatus::Ended)
                );
                FrameSendError::OutputTooSmall
            } else {
                drop(reader);
                FrameSendError::Closed
            };
            assert_eq!(old.0.load(Ordering::Relaxed), 0);
            assert!(new.0.load(Ordering::Relaxed) > 0);
            assert_eq!(next.await, Err(expected));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn ordinary_callback_read_does_not_wake_producer_scheduler() {
        let (mut writer, mut reader) = frame_channel();
        let counter = Arc::new(Counter::default());
        let waker = Waker::from(counter.clone());
        let mut send = Box::pin(writer.send(&[7]));
        assert!(
            send.as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        let mut output = [0; MAX_FRAME];
        assert_eq!(
            read(&mut reader, &mut output),
            Poll::Ready(FrameStatus::Frame { len: 1 })
        );
        assert_eq!(counter.0.load(Ordering::Relaxed), 0);
        // A manual poll can observe the permit, and the timer remains the
        // automatic fallback for ordinary FrameSource callers.
        assert_eq!(send.await, Ok(()));
    }

    #[tokio::test]
    async fn cancellation_retains_frame_and_next_send_waits_without_overwrite() {
        let (mut writer, mut reader) = frame_channel();
        let first = [0xa5; MAX_FRAME];
        let second = [0x5a; MAX_FRAME];
        let mut output = [0; MAX_FRAME];
        let mut send = Box::pin(writer.send(&first));
        assert!(
            send.as_mut()
                .poll(&mut Context::from_waker(&noop_waker()))
                .is_pending()
        );
        drop(send);
        let mut send = Box::pin(writer.send(&second));
        assert!(
            send.as_mut()
                .poll(&mut Context::from_waker(&noop_waker()))
                .is_pending()
        );
        assert_eq!(
            read(&mut reader, &mut output),
            Poll::Ready(FrameStatus::Frame { len: MAX_FRAME })
        );
        assert_eq!(output, first);
        // A retained consumption permit must be taken even if the sender was
        // not waiting on a runtime notification when consumption happened.
        tokio::time::sleep(Duration::from_millis(3)).await;
        assert!(
            send.as_mut()
                .poll(&mut Context::from_waker(&noop_waker()))
                .is_pending()
        );
        assert_eq!(
            read(&mut reader, &mut output),
            Poll::Ready(FrameStatus::Frame { len: MAX_FRAME })
        );
        assert_eq!(output, second);
        send.await.unwrap();
        assert_eq!(read(&mut reader, &mut output), Poll::Pending);
        drop(writer);
        assert_eq!(
            read(&mut reader, &mut output),
            Poll::Ready(FrameStatus::Ended)
        );
    }

    #[tokio::test]
    async fn invalid_length_publishes_nothing_and_undersized_output_fails_without_copy() {
        let (mut writer, mut reader) = frame_channel();
        assert_eq!(writer.send(&[]).await, Err(FrameSendError::InvalidLength));
        assert_eq!(
            writer.send(&[0; MAX_FRAME + 1]).await,
            Err(FrameSendError::InvalidLength)
        );
        let mut small = [0xcc; 2];
        assert_eq!(read(&mut reader, &mut small), Poll::Pending);
        let mut send = Box::pin(writer.send(&[1, 2, 3]));
        assert!(
            send.as_mut()
                .poll(&mut Context::from_waker(&noop_waker()))
                .is_pending()
        );
        assert_eq!(
            read(&mut reader, &mut small),
            Poll::Ready(FrameStatus::Ended)
        );
        assert_eq!(small, [0xcc; 2]);
        assert_eq!(send.await, Err(FrameSendError::OutputTooSmall));
        assert_eq!(writer.send(&[1]).await, Err(FrameSendError::OutputTooSmall));
    }

    #[tokio::test]
    async fn dropping_writer_delivers_queued_frame_before_eof() {
        let (mut writer, mut reader) = frame_channel();
        let mut send = Box::pin(writer.send(&[7]));
        assert!(
            send.as_mut()
                .poll(&mut Context::from_waker(&noop_waker()))
                .is_pending()
        );
        drop(send);
        drop(writer);
        let mut output = [0; MAX_FRAME];
        assert_eq!(
            read(&mut reader, &mut output),
            Poll::Ready(FrameStatus::Frame { len: 1 })
        );
        assert_eq!(output[0], 7);
        assert_eq!(
            read(&mut reader, &mut output),
            Poll::Ready(FrameStatus::Ended)
        );
    }

    #[tokio::test]
    async fn dropping_reader_releases_pending_and_future_sends() {
        let (mut writer, reader) = frame_channel();
        let mut send = Box::pin(writer.send(&[1]));
        assert!(
            send.as_mut()
                .poll(&mut Context::from_waker(&noop_waker()))
                .is_pending()
        );
        drop(reader);
        assert_eq!(send.await, Err(FrameSendError::Closed));
        assert_eq!(writer.send(&[2]).await, Err(FrameSendError::Closed));
    }

    #[derive(Default)]
    struct Counter(AtomicUsize);
    impl Wake for Counter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[tokio::test]
    async fn latest_waker_receives_readiness_and_abort_before_start_receives_eof() {
        let (mut writer, mut reader) = frame_channel();
        let old = Arc::new(Counter::default());
        let current = Arc::new(Counter::default());
        let old_waker = Waker::from(old.clone());
        let current_waker = Waker::from(current.clone());
        let mut output = [0; MAX_FRAME];
        assert!(
            reader
                .poll_frame(&mut Context::from_waker(&old_waker), &mut output)
                .is_pending()
        );
        assert!(
            reader
                .poll_frame(&mut Context::from_waker(&current_waker), &mut output)
                .is_pending()
        );
        // The writer is owned by the task before the task is ever polled.
        let task = tokio::spawn(async move { writer.send(&[1]).await });
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(old.0.load(Ordering::Relaxed), 0);
        assert!(current.0.load(Ordering::Relaxed) > 0);
        assert_eq!(
            read(&mut reader, &mut output),
            Poll::Ready(FrameStatus::Ended)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_publication_preserves_bytes_order_and_eof() {
        concurrent_publication(false).await;
        concurrent_publication(true).await;
    }

    async fn concurrent_publication(owned: bool) {
        let (mut writer, mut reader) = frame_channel();
        let task = tokio::spawn(async move {
            for sequence in 0..256u32 {
                let mut bytes = [0; MAX_FRAME];
                for (i, byte) in bytes.iter_mut().enumerate() {
                    *byte = (sequence as usize + i) as u8;
                }
                writer.send(&bytes).await.unwrap();
            }
        });
        let mut output = [0; MAX_FRAME];
        for sequence in 0..256u32 {
            let status = futures_util::future::poll_fn(|cx| {
                if owned {
                    reader.poll_owned(cx, &mut output)
                } else {
                    reader.poll_frame(cx, &mut output)
                }
            })
            .await;
            assert_eq!(status, FrameStatus::Frame { len: MAX_FRAME });
            for (i, byte) in output.iter().enumerate() {
                assert_eq!(*byte, (sequence as usize + i) as u8);
            }
        }
        let status = futures_util::future::poll_fn(|cx| {
            if owned {
                reader.poll_owned(cx, &mut output)
            } else {
                reader.poll_frame(cx, &mut output)
            }
        })
        .await;
        assert_eq!(status, FrameStatus::Ended);
        task.await.unwrap();
    }
}
