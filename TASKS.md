# Current task: Raydio Oracle audio reliability

- [ ] Diagnose wall-time source poll rejection under shared VM scheduling.
- [ ] Add regression coverage and retain isolation of CPU-bound sources.
- [ ] Measure transport pacing and source guard overhead; do not include the unrelated coordinator-count experiment.
- [ ] Complete live Oracle receiver and sender validation in Raydio.
- [x] Retain wall/CPU timing of the latest source overrun after the six-hour attempt failed; verify both guard paths without weakening the kill gate.

- [x] Reproduce synchronous Oto notifier work inside a reentrant source wake, bound that callback path, and preserve pending/readiness and CPU-source isolation.
- [x] Run 81 tests, Clippy and paired DAVE benchmark with zero allocations.
- [ ] Verify new callback isolation in Raydio on Oracle; exact cause remains diagnostic.
