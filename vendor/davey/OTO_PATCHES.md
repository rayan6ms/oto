# Oto patches to davey 0.1.4

Upstream source: `Snazzah/davey` commit
`a1e2e741bea06bc3b7167a5c3792844b8975993c` (`rs-0.1.4`).

Oto carries two narrow sender-conformance corrections while the upstream Rust
API lacks them:

- exact participant Opus silence (`F8 FF FE`) follows the ordinary outbound
  DAVE encryptor path; only the receive-side SFU silence exception remains;
- a ratchet derived while processing Commit/Welcome is staged until the
  matching Execute Transition instead of changing the sender early.
- the unconditional OpenMLS `js` feature is removed from this native Rust
  build; it only enables browser randomness and is not part of Oto's targets.
- Python and Node binding-only optional dependencies are removed from the
  Rust-only vendored package so inactive vulnerable bindings do not enter the
  frozen production lockfile.

The MLS and media cryptographic implementations are otherwise unchanged.

`fixtures/upstream_session_fixtures.py` is copied unchanged from the pinned
upstream Python test suite so Oto's Rust admission tests can exercise the same
external-sender and proposal messages.
