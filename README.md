
# Oto implementation specification

Oto is a Rust-native Discord voice media transport library and the intended replacement for Koe.

It is deliberately not an audio engine and not a Lavalink server.

```text
Mantle
    sources / decoding / PCM / Opus

Crust
    Lavalink REST/WS / sessions / players / filters / stats

Oto
    Discord Voice Gateway / UDP / RTP / transport encryption / DAVE / paced audio send
```

## Reference targets

Frozen behavioral artifact reference:

```text
moe.kyokobot.koe:core:3.0.0-pre6
moe.kyokobot.koe:ext-udpqueue:3.0.0-pre6
```

Exact source commit proven during P00:

```text
7436cf8ea57ddbc9348e261b1f377c78b6b98530
```


Moving protocol authority:

```text
Discord Voice Gateway v8
current supported Discord transport-encryption modes
current non-discontinued DAVE protocol versions
```

Koe is used to discover useful behavior and establish a performance reference. Current Discord documentation and DAVE reference material override Koe whenever the live protocol has moved on.

## Product goals

Oto must:

- be implemented in Rust;
- be an embeddable library using the caller's Tokio runtime;
- connect to Discord voice independently of whether audio is currently available;
- accept already-encoded Opus audio;
- implement Voice Gateway v8 identify/heartbeat/resume/buffered-resume behavior;
- implement UDP IP discovery and protocol selection;
- build the current Discord audio RTP packet format;
- implement current transport encryption;
- implement DAVE/E2EE using a maintained Rust implementation proven by conformance tests;
- provide a low-overhead paced audio sender that can poll an encoded-frame source;
- implement speaking and required silence interpolation;
- reconnect/resume predictably;
- remain bounded, low-allocation, and scalable;
- expose a small Rust API suitable for Crust and standalone Rust consumers.

## Explicit non-goals for core Oto 1.0

- decoding;
- Opus encoding;
- resampling;
- mixing;
- media HTTP/source loading;
- Lavalink protocol;
- Java/Kotlin/JNI compatibility;
- Koe Java API compatibility;
- a generic codec registry;
- Voice Gateway v4/v5;
- experimental video sending;
- full voice/audio receive API;
- automatic porting of Koe's native UDP-Queue extension;
- RTCP unless a real current consumer requirement admits it into 1.0.

Oto may receive and discard/classify UDP packets internally for IP discovery and protocol operation without exposing a receive API.

## Design direction

```text
Oto shared connector/config
          |
   VoiceConnection
   control + transport
          |
      attach audio
          |
  PacedAudioSender
          |
     FrameSource
```

A connection is not coupled to a frame source. Audio can start, stop, and be replaced without rebuilding the voice connection.

## Core principle

> Preserve Koe's useful separation of transport from encoding, follow current Discord protocol truth, and remove JVM/Netty/historical complexity instead of translating it.


## Core audio contract

Oto 1.0's standard `PacedAudioSender` is intentionally narrow:

```text
Discord Opus
48 kHz stereo
20 ms per frame
RTP timestamp increment = 960
```

This matches Koe's actual audio poller and the intended Crust/Mantle integration. Variable-duration Opus packets are deferred until a real consumer requires them; Oto should not carry timing genericity merely because Opus can theoretically encode other durations.
