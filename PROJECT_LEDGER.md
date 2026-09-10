# Attachment ownership and diagnostic audit (2026-09-10)

The gateway retains a recoverable transport until the caller synchronously
claims it. Dropping 32 successfully acknowledged attachments then retrying keeps
the original encoder; the transport nonce advances 123 -> 124. Pacer registration
now owns its rollback guard before the first await, so cancellation after an
acknowledgement removes the otherwise orphaned coordinator slot. No per-frame
mutex was introduced; the shared attachment allocation is bounded per connection.

Audio snapshots now retain active-send gaps at 40/100/1000 ms and latest/max gap
with wall time. One monotonic clock read per successful UDP send; no new worker,
timer, per-frame allocation or log. Pausing the pacing timeline clears the gap
anchor, so these counters deliberately exclude inactive source/connection time.
98 tests pass, six manual benchmarks excluded; Clippy passes. A separately run
10-second release owned-channel+DAVE/UDP benchmark sent 500 packets before and
501 after, with zero allocations/reallocations in both. Maximum observed sender
lateness was 2.139/1.837 ms; this single pair demonstrates no gross regression,
not a statistically established latency improvement. Raw evidence is in Raydio
`evidence/dependency-fixes-20260910/`. Fresh Oracle qualification remains required.

# Active Raydio reliability finding

2026-09-08: The capacity-one owned producer unnecessarily remained asleep until
a 1 ms polling timer after its frame had been consumed. A paused-clock regression
failed before the change and passes now. The concrete owned reader copies first,
then wakes a register/recheck AtomicWaker waiter; the ordinary FrameSource poll
still sets flags without waking producer task code, with its timer fallback and
2 ms callback watchdog unchanged. Close, undersized buffers, cancelled waiter
replacement, concurrent ordering, EOF and the encrypted terminal-silence path
are covered: 92 tests pass, six benchmarks ignored; Clippy passes. Runtime task
wake cost is now paid by the concrete owned sender path and needs live review.
The public queue capacity and source-consumption acknowledgement are unchanged.
Sequential release tests: 250 frames require 2633 producer polls before and 501
after (zero unavailable in both); 10 s full DAVE paths send 501/500 frames with
zero allocations and max lateness 1.724/2.050 ms. No memory/latency or Oracle
quality improvement is claimed. Evidence: owned-consumption-wake-local.json.

A 20 ms off-CPU delay caused a valid frame to be rejected permanently by the 2 ms wall-time guard. The Linux guard now uses thread CPU time for fatal attribution, retaining elapsed-overrun telemetry and CPU-heavy/invalid-frame failure tests. Blocking source implementations remain prohibited; CPU time cannot enforce this part of the contract. 79 tests pass, 5 manual benchmarks excluded; Clippy passes. The 10 s local DAVE benchmark pair retained zero allocations and ~50 frames/s. Full Oracle receiver qualification remains in Raydio; the earlier terminal cloud failure has not yet been classified. The unrelated coordinator-count experiment is not included.

The six-hour Oracle attempt on 2026-09-06 stopped with FrameSourceContract at
18:05 UTC, one source overrun, and zero send failures. Crust's use of Tokio
try_recv has a possible parking path during publication, but causality is still
unproved. AudioStats now retains the last overrun's wall and Linux thread CPU
durations for terminal diagnosis, without extra clock reads or per-frame logs.
The 2 ms CPU kill gate is unchanged. Sleeping and CPU-bound source regressions
verify the diagnostic values; all 79 tests and Clippy pass. The 10-second DAVE
benchmark retains zero allocations and 500 frames/packets. Raydio retains raw
evidence; no cloud reliability or latency improvement is claimed from this test.

2026-09-07: native Raydio 134e496 with the rtrb bridge and staged input still
failed its Oracle callback CPU guard at 3.33 ms, despite zero source deficits.
A diagnostic build has not yet reproduced the specific stage. Independent
local evidence shows Oto's WakeToken can execute Notify/scheduler work inline
if AtomicWaker registration calls it synchronously: deliberately slow 5 ms
scheduler produced 5,011 us source-wake CPU. One atomic state now defers that
Oto-owned notification until the source callback completes (10 us in the same
unoptimized test). Caller CPU-heavy failure still passes unchanged. Two new
regressions cover delivery and the poll-exit race; 81 tests and Clippy pass.
Before/after 10-second DAVE benchmark: 501 packets/frames, zero allocations,
max lateness 1.538/1.326 ms. No Oracle cause or audio-reliability claim yet.
