# Active Raydio reliability finding

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
