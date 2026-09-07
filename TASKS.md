# Current task: Raydio Oracle audio reliability

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
