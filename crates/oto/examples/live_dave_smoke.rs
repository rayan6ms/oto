//! Manual, credential-safe DAVE interoperability smoke test.
//!
//! Reads one Voice Gateway update as JSON from standard input. Never pass the
//! voice token on the command line or write it to a fixture/log file.

use std::error::Error;
use std::io::{self, Read};
use std::task::{Context, Poll};
use std::time::Duration;

use oto::{
    AudioPhase, CloseReason, ConnectionPhase, FrameSource, FrameStatus, Oto, VoiceConnectInfo,
    VoiceToken,
};
use serde::Deserialize;
use tokio::time::{sleep, timeout};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const AUDIO_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const IDLE_OBSERVATION: Duration = Duration::from_secs(1);
const PROBE_FRAMES: u8 = 10;
const OPUS_SILENCE: [u8; 3] = [0xF8, 0xFF, 0xFE];

#[derive(Deserialize)]
#[serde(untagged)]
enum Snowflake {
    String(String),
    Integer(u64),
}

impl Snowflake {
    fn parse(self, field: &str) -> Result<u64, Box<dyn Error>> {
        match self {
            Self::String(value) => value.parse().map_err(|_| {
                format!("{field} must be an unsigned 64-bit Discord snowflake").into()
            }),
            Self::Integer(value) => Ok(value),
        }
    }
}

#[derive(Deserialize)]
struct LiveVoiceInfo {
    server_id: Snowflake,
    user_id: Snowflake,
    channel_id: Snowflake,
    session_id: String,
    endpoint: String,
    token: String,
}

struct BoundedProbe {
    remaining: u8,
}

impl FrameSource for BoundedProbe {
    fn poll_frame(&mut self, _cx: &mut Context<'_>, output: &mut [u8]) -> Poll<FrameStatus> {
        if self.remaining == 0 {
            return Poll::Ready(FrameStatus::Ended);
        }
        output[..OPUS_SILENCE.len()].copy_from_slice(&OPUS_SILENCE);
        self.remaining -= 1;
        Poll::Ready(FrameStatus::Frame {
            len: OPUS_SILENCE.len(),
        })
    }
}

fn read_voice_info() -> Result<VoiceConnectInfo, Box<dyn Error>> {
    let mut input = String::new();
    io::stdin().read_to_string(&mut input)?;
    let value: LiveVoiceInfo = serde_json::from_str(&input)?;
    Ok(VoiceConnectInfo::new(
        value.server_id.parse("server_id")?,
        value.user_id.parse("user_id")?,
        value.channel_id.parse("channel_id")?,
        value.session_id,
        value.endpoint,
        VoiceToken::new(value.token),
    ))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let info = read_voice_info()?;
    let oto = Oto::builder().build()?;
    let connection = timeout(CONNECT_TIMEOUT, oto.connect(info))
        .await
        .map_err(|_| "live connection timed out")??;

    let probe = async {
        sleep(IDLE_OBSERVATION).await;
        let connected = connection.state();
        println!(
            "connected: generation={} phase={:?}",
            connected.generation().get(),
            connected.phase()
        );
        if connected.phase() != ConnectionPhase::Connected {
            return Err("connection left Connected during idle observation".into());
        }

        let sender = connection
            .start_audio(BoundedProbe {
                remaining: PROBE_FRAMES,
            })
            .await?;
        timeout(AUDIO_TIMEOUT, async {
            loop {
                let state = sender.state();
                if matches!(state.phase(), AudioPhase::Stopped | AudioPhase::Failed) {
                    return state;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .map_err(|_| "paced DAVE probe timed out")?;

        Ok::<_, Box<dyn Error>>(sender.stop().await?)
    }
    .await;

    // Once connect succeeds, every probe outcome gets an explicit bounded async
    // shutdown before its result is propagated.
    let final_connection = timeout(SHUTDOWN_TIMEOUT, connection.shutdown())
        .await
        .map_err(|_| "live connection shutdown timed out")??;
    println!(
        "shutdown: phase={:?} reason={:?}",
        final_connection.phase(),
        final_connection.close_reason()
    );
    let audio = probe?;
    let stats = audio.stats();
    println!(
        "audio: phase={:?} frames={} terminal_silence={} failures={}",
        audio.phase(),
        stats.frames_sent(),
        stats.silence_frames_sent(),
        stats.send_failures()
    );

    if final_connection.phase() != ConnectionPhase::Closed
        || final_connection.close_reason() != Some(CloseReason::ExplicitShutdown)
        || audio.phase() != AudioPhase::Stopped
        || audio.failure().is_some()
        || stats.frames_sent() != u64::from(PROBE_FRAMES)
        || stats.silence_frames_sent() != 5
        || stats.send_failures() != 0
    {
        return Err("live DAVE smoke did not meet its acceptance criteria".into());
    }
    Ok(())
}
