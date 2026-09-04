# Changelog

All notable changes to Oto are documented in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-09-04

### Added

- Rust-native Discord Voice Gateway v8 lifecycle with numbered-message
  acknowledgement, reconnect, buffered Resume, and generation-safe replacement.
- UDP discovery, direct Discord audio RTP construction, preferred AES-256-GCM
  rtpsize transport encryption, and required XChaCha20-Poly1305 rtpsize support.
- Fail-closed DAVE v1 media encryption through a private, validated Rust adapter.
- Readiness-aware `PacedAudioSender` for exact 20 ms, 48 kHz stereo encoded Opus
  frames, including Speaking ordering and bounded terminal silence.
- Typed caller-owned connection, state, event, error, statistics, and explicit
  asynchronous shutdown APIs using the caller's Tokio runtime.
- Deterministic protocol testkit, downstream integration coverage, live Discord
  interoperability validation, scale/performance checks, and security gates.

### Security

- Bounded untrusted inputs and queues, redacted credentials and key material,
  no plaintext fallback for DAVE-required calls, and transport renewal before
  nonce reuse.
- Voice tokens and ephemeral voice session identifiers are redacted from public
  debug output.
- Vendored DAVE trace logging cannot disclose MLS secrets, sender-ratchet
  material, or voice privacy codes.
- Source release contains the admitted patched `davey` and OpenMLS RustCrypto
  dependencies; registry publication is disabled until equivalent publishable
  dependency provenance exists.

### Unsupported by design

- Decoding, encoding, resampling, mixing, media loading, Lavalink, full voice
  receive, video, Voice Gateway v4/v5, variable-duration Opus, and raw-send APIs.
