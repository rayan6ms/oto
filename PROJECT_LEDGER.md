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
# Sender queue validation — 2026-09-15

The earlier independent-sender experiment prepared the full DAVE stream and
fed its ring from a native producer unaffected by runtime faults. Its 250 ms
stall result did not prove 160 ms bounded protection. The replacement Linux
test uses incremental owned-channel production and actual DAVE on Tokio,
8/16 fixed slots, one native sender, independent UDP receipt, and successful
submission traces. Full-runtime fault counters verify zero production and
preparation progress; an external bounded coordinator fixes a two-worker fault
injector deadlock. At 250 ms stalls, final local max send intervals are
251.391–251.567 ms baseline, 91.358 ms (8 slots), 20.175 ms (16 slots).
At 400 ms even the larger queue underflows (~81 ms gap). After 64 warm-up
frames, owned-channel/source/DAVE/native transport has zero allocations over
100 frames. Source/gateway/reconnect integration is still absent.

Six one-minute Oracle loopback runs, 3005 packets each, preserve every packet.
Three baseline runs and two 8-slot runs have zero >=40 ms send gaps; the 16-slot
run has three, max 65.755 ms. Sequential host variability prevents attributing
that regression to capacity; send traces do rule out receiver-only delay.
No demonstrated Oracle continuity gain or live packet-loss/audio-quality
claim. The prototype cannot be promoted without Crust consumption-aware
pause/EOF handling, real DAVE transition fencing, and transport/diagnostic
ownership. Report/raw evidence are in Raydio docs/SENDER-QUEUE-VALIDATION-2026-09-15.md
and evidence/sender-queue-20260915/. Production pins/deployment unchanged.


## 2026-09-23: DAVE readiness recovery

Oracle default-worker run lost its browser receiver at 03:25 UTC, then DAVE epoch reset/prepare at 03:27:31 left the sender unready. At 03:27:41 the gateway readiness watchdog classified this as an owner response timeout and terminated the connection. The initial receiver outage predates this failure; this change addresses recovery, not that unknown network cause.

Readiness expiry now has a distinct typed cause. Oto invalidates the old transport and allows at most two fresh identifies per externally supplied voice generation, retaining the audio source. No deadline extension, key bypass, plaintext fallback, or hot-path scheduling change. A ready transport or successful recovery does not replenish the DAVE budget. Genuine owner-response timeout and malformed/unsupported controls remain terminal. Control diagnostics include safe prepare/execute IDs, epoch and successfully written reply count; recovery emits the attempt and redacted context.

Current official protocol was rechecked against https://github.com/discord/dave-protocol/blob/main/protocol.md: epoch 1 requires a new key package and readiness before transition execution. Recovery deliberately renegotiates rather than reusing invalid epoch state. Local TLS/UDP fixture tests cover the observed reset/prepare sequence, retained frame source, strict readiness gating and bounded exhaustion; 114 tests pass. Live Crust/Raydio verification remains separate.


### Live follow-up: empty room and incremental membership

The first Oracle candidate reproduced opcode 13 (last listener leaves), epoch 1 reset, transition 0 preparation, then no completed key setup. The bounded retries worked but all expired before the listener returned after 37 seconds. This is not yet a successful live recovery fix. Added a 120-second no-media grace only for a confirmed empty roster, returning to the normal ten-second deadline when a listener joins; duplicate roster/epoch messages cannot replenish a deadline. Unknown membership keeps the normal deadline. The bot still owns its existing 120-second alone-disconnect policy.

Official DAVE clients_connect (11) describes additions, not replacement snapshots. The gateway previously replaced its roster, potentially losing still-connected members. The new empty-room decision requires accumulating/deduplicating these additions and enforcing the same total membership bound across batches. Core receives the complete internal snapshot. Tests cover cumulative limits, departure with another remaining listener, bounded grace, and resumed source after listener return.
