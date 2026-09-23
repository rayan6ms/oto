## Live follow-up: confirmed empty-room wait

- [x] Reproduce Oracle last-listener departure resetting DAVE without a completed group while alone.
- [x] Implement bounded empty-room readiness grace and incremental, globally bounded membership tracking.
- [ ] Finish full regression/Clippy checks and redeploy for verified listener return and five-minute receiver capture.

## September 23: bounded recovery after observed DAVE readiness timeout

- [x] Reproduce missing-successor expiry with epoch reset, duplicate controls and resume; distinguish gateway readiness timeout from owner-response timeout.
- [x] Retain the audio attachment through at most two fresh encrypted handshakes per external credential generation; transport readiness/resume cannot reset that budget.
- [x] Cover resumed media only after readiness, retained source, repeated-success exhaustion, missing-successor exhaustion, no plaintext and existing shutdown/replacement cases (114 ordinary tests pass).
- [ ] Complete Crust integration and Oracle Testbot receiver qualification. No six-hour or production readiness claim.

# Current task: Raydio Oracle audio reliability

## Sender queue decision experiment — 2026-09-15

- [x] Correct the full-stream/native-feeder benchmark shortcut and record the withdrawn claims.
- [x] Use incremental owned-channel production and actual DAVE encryption on the same two-worker runtime as the baseline; bound all prepared frames.
- [x] Test 8/16 slots, both/one worker stalls, >buffer stalls, native sender stalls, explicit pause/fence/rekey/replacement, and warmed channel+DAVE allocations.
- [x] Retain successful-send and independent-receiver interval arrays; run matched Oracle loopback trials without deploying or restarting services.
- [x] Record conditional promotion decision: synthetic stall protection passes, Oracle continuity benefit is unestablished and production lifecycle integration is absent. Do not enable this test-only prototype or claim six-hour/Discord validation.

Historical reliability items below predate this experiment; see Raydio's
`docs/SENDER-QUEUE-VALIDATION-2026-09-15.md` for the current evidence and scope.

- [ ] Diagnose wall-time source poll rejection under shared VM scheduling.
- [ ] Add regression coverage and retain isolation of CPU-bound sources.
- [ ] Measure transport pacing and source guard overhead; do not include the unrelated coordinator-count experiment.
- [ ] Complete live Oracle receiver and sender validation in Raydio.
- [x] Retain wall/CPU timing of the latest source overrun after the six-hour attempt failed; verify both guard paths without weakening the kill gate.

- [x] Reproduce synchronous Oto notifier work inside a reentrant source wake, bound that callback path, and preserve pending/readiness and CPU-source isolation.
- [x] Run 81 tests, Clippy and paired DAVE benchmark with zero allocations.
- [ ] Verify new callback isolation in Raydio on Oracle; exact cause remains diagnostic.

- [x] Reproduce discarded overdue pacing opportunities and recover one frame immediately without a catch-up burst; keep the source CPU guard unchanged.
- [x] Pass 82 ordinary tests and paired release DAVE-path allocation/frame-count measurements for overdue recovery.
- [ ] Validate overdue recovery on Oracle with sender, independent vCPU timers and receiver measurements; this cannot remove hypervisor stalls.

- [x] Add an Oto-owned capacity-one encoded frame channel; keep arbitrary callback CPU guard unchanged.
- [x] Test cancellation, EOF ordering, close, byte integrity, output bounds, wake replacement, and the full DAVE/silence path: 89 tests pass; Clippy passes.
- [x] Measure callback/channel release paths with identical maximum frames: both 501 packets, zero allocations/reallocations in 10 seconds; max lateness 1.148/2.001 ms. This is not a speedup or Oracle quality claim.
- [ ] Qualify the committed channel through Crust/Raydio on Oracle; host descheduling can still cause gaps.

- [x] Reproduce the owned producer's unnecessary timer wait after consumption; wake the producer after the concrete owned copy while retaining generic callback isolation, one-frame capacity, cancellation and EOF ordering.
- [x] Pass 92 ordinary tests and Clippy. At 250 frames, producer polls fell 2633 to 501; complete DAVE path retains zero allocations. Local maximum lateness 1.724/2.050 ms does not prove a latency improvement.
- [ ] Compare event-driven consumption on Oracle and a viable free alternative; host descheduling remains outside this repair.
