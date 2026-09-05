# Oto

<p align="center">
  <img src="icons/oto.png" alt="Oto project icon" width="220">
</p>

[![CI](https://github.com/rayan6ms/oto/actions/workflows/ci.yml/badge.svg)](https://github.com/rayan6ms/oto/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

Oto is a Rust-native Discord voice transport library. It connects to Discord's
Voice Gateway, establishes the UDP/RTP transport, applies Discord transport
encryption and DAVE end-to-end encryption, and sends already-encoded Opus on a
bounded 20 ms scheduler.

Oto is deliberately not an audio engine. It does not download media, decode,
mix, resample, or encode audio, and it does not implement Lavalink.

## Features

- Discord Voice Gateway v8 with heartbeat, reconnect, and buffered Resume
- UDP discovery and the current Discord audio RTP packet format
- AES-256-GCM rtpsize preference and XChaCha20-Poly1305 rtpsize support
- DAVE v1 media encryption with fail-closed behavior
- Readiness-aware, restartable paced sending for encoded Discord Opus
- Caller-owned connections using the caller's Tokio runtime
- Typed state, events, close reasons, errors, and low-cost statistics
- Bounded queues, messages, frames, and transport lifetimes

## Requirements

- Rust 1.97 or newer
- A Tokio runtime
- Voice connection information obtained from your bot's main Discord Gateway
  session: guild/server ID, bot user ID, voice channel ID, voice session ID,
  Voice Server Update endpoint, and its one-use voice token
- Already-encoded 20 ms, 48 kHz stereo Opus frames

The validated release platform is `x86_64-unknown-linux-gnu`. Other platforms
are not advertised as validated yet.

## Installation

Oto 1.0 is distributed from this repository because the validated DAVE backend
includes narrow vendored patches that cannot currently be represented by a
crates.io dependency graph.

```toml
[dependencies]
oto = { git = "https://github.com/rayan6ms/oto", tag = "v1.0.0" }
tokio = { version = "1.53", features = ["macros", "rt-multi-thread"] }
```

Both workspace packages intentionally use `publish = false`. Do not replace
the vendored `davey` path with upstream `davey 0.1.4`; it does not contain all
of the fixes required by this release.

## Connecting

Oto does not own your bot's main Gateway connection or guild/session map. Pass
the voice state and voice server values to a caller-owned connection:

```rust,no_run
use oto::{Oto, VoiceConnectInfo, VoiceToken};

async fn connect_voice(
    server_id: u64,
    user_id: u64,
    channel_id: u64,
    session_id: String,
    endpoint: String,
    token: String,
) -> Result<(), oto::Error> {
    let oto = Oto::builder().build()?;
    let info = VoiceConnectInfo::new(
        server_id,
        user_id,
        channel_id,
        session_id,
        endpoint,
        VoiceToken::new(token),
    );
    let connection = oto.connect(info).await?;

    // A connected voice transport does not require an attached audio source.
    println!("voice state: {:?}", connection.state().phase());

    // Explicit asynchronous shutdown is preferred over relying on Drop.
    connection.shutdown().await
}
```

`connect` completes when the voice transport is usable, including DAVE
readiness when Discord selects a nonzero DAVE version. Credentials and key
material are redacted from public debug output and are never intentionally
logged.

## Supplying Opus

Implement `FrameSource` for a non-blocking encoded-frame source, then call
`VoiceConnection::start_audio`. Each ready poll supplies exactly one 20 ms,
48 kHz stereo Opus frame. Return `Poll::Pending` only after registering the
current waker; Oto stops periodic polling while an attached source is idle.

The sender owns pacing and RTP timestamp advancement. A source should not run a
competing 20 ms timer. Ending a source drains the required bounded silence and
clears Discord's speaking state. A new source can later be attached to the same
connection.

Generate the complete API documentation locally with:

```sh
cargo doc --workspace --all-features --no-deps --open
```

## Building and testing

```sh
cargo test --locked --workspace --all-targets --all-features
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

The manual `live_dave_smoke` example reads voice connection JSON from standard
input so a voice token does not need to appear in command-line arguments. Never
commit, paste into an issue, or log Discord credentials.

## Scope

Oto 1.0 does not expose decoding, encoding, resampling, mixing, media loading,
full voice receive, RTCP parsing, experimental video, Voice Gateway v4/v5,
variable-duration Opus, or a caller-paced raw-send API.

## Security

See [SECURITY.md](SECURITY.md) for supported versions and private vulnerability
reporting. Dependency policy is checked by `cargo deny`; the current locked
graph has one accepted unmaintained build-time warning,
`RUSTSEC-2026-0173`, and no known vulnerability from that advisory.

## License

First-party Oto code is available under either the
[Apache License 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT), at your
option. Vendored dependencies remain under their respective upstream licenses;
see [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
