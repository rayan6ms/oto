//! Manual, credential-safe DAVE interoperability smoke test.
//!
//! Reads one Voice Gateway update as JSON from standard input. Never pass the
//! voice token on the command line or write it to a fixture/log file.

use std::error::Error;
use std::io::{self, Read};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use oto::{
    AudioPhase, CloseReason, ConnectionPhase, FrameSource, FrameStatus, Oto, VoiceConnectInfo,
    VoiceToken,
};
use serde::Deserialize;
use tokio::time::{sleep, timeout};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const RESUME_TIMEOUT: Duration = Duration::from_secs(120);
const AUDIO_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const IDLE_OBSERVATION: Duration = Duration::from_secs(1);
const STARVATION_OBSERVATION: Duration = Duration::from_millis(250);
const PROBE_FRAMES: u8 = 10;
const TERMINAL_SILENCE_FRAMES: u64 = 5;
const MAX_SOAK_SECONDS: u64 = 86_400;
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

#[derive(Clone)]
struct ProbeControl {
    shared: Arc<Mutex<ProbeState>>,
}

struct StarvableProbe {
    shared: Arc<Mutex<ProbeState>>,
}

#[derive(Default)]
struct ProbeState {
    available: u8,
    ended: bool,
    waker: Option<std::task::Waker>,
}

impl StarvableProbe {
    fn new() -> (Self, ProbeControl) {
        let shared = Arc::new(Mutex::new(ProbeState::default()));
        (
            Self {
                shared: shared.clone(),
            },
            ProbeControl { shared },
        )
    }
}

impl ProbeControl {
    fn provide(&self, frames: u8) -> Result<(), Box<dyn Error>> {
        let waker = {
            let mut state = self.shared.lock().map_err(|_| "probe state poisoned")?;
            state.available = state
                .available
                .checked_add(frames)
                .ok_or("probe frame count overflow")?;
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }

    fn end(&self) -> Result<(), Box<dyn Error>> {
        let waker = {
            let mut state = self.shared.lock().map_err(|_| "probe state poisoned")?;
            state.ended = true;
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }
}

impl FrameSource for StarvableProbe {
    fn poll_frame(&mut self, cx: &mut Context<'_>, output: &mut [u8]) -> Poll<FrameStatus> {
        let Ok(mut state) = self.shared.lock() else {
            return Poll::Ready(FrameStatus::Ended);
        };
        if state.available > 0 {
            output[..OPUS_SILENCE.len()].copy_from_slice(&OPUS_SILENCE);
            state.available -= 1;
            return Poll::Ready(FrameStatus::Frame {
                len: OPUS_SILENCE.len(),
            });
        }
        if state.ended {
            return Poll::Ready(FrameStatus::Ended);
        }
        if !state
            .waker
            .as_ref()
            .is_some_and(|waker| waker.will_wake(cx.waker()))
        {
            state.waker = Some(cx.waker().clone());
        }
        Poll::Pending
    }
}

fn soak_duration() -> Result<Duration, Box<dyn Error>> {
    let Some(value) = std::env::var_os("OTO_LIVE_SOAK_SECONDS") else {
        return Ok(Duration::ZERO);
    };
    let value = value
        .into_string()
        .map_err(|_| "OTO_LIVE_SOAK_SECONDS must be valid UTF-8")?;
    let seconds: u64 = value
        .parse()
        .map_err(|_| "OTO_LIVE_SOAK_SECONDS must be an unsigned integer")?;
    if seconds > MAX_SOAK_SECONDS {
        return Err(format!("OTO_LIVE_SOAK_SECONDS must not exceed {MAX_SOAK_SECONDS}").into());
    }
    Ok(Duration::from_secs(seconds))
}

async fn wait_for_audio_stop(
    sender: &oto::PacedAudioSender,
) -> Result<oto::AudioSnapshot, Box<dyn Error>> {
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
    .map_err(|_| "paced DAVE probe timed out".into())
}

async fn wait_for_frames(
    sender: &oto::PacedAudioSender,
    expected: u64,
) -> Result<(), Box<dyn Error>> {
    timeout(AUDIO_TIMEOUT, async {
        loop {
            let state = sender.state();
            if state.stats().frames_sent() >= expected || state.phase() == AudioPhase::Failed {
                return state;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| "paced DAVE frame burst timed out")?
    .failure()
    .map_or(Ok(()), |_| Err("paced DAVE frame burst failed".into()))
}

async fn wait_for_connected(
    connection: &oto::VoiceConnection,
) -> Result<oto::ConnectionSnapshot, Box<dyn Error>> {
    timeout(CONNECT_TIMEOUT, async {
        loop {
            let state = connection.state();
            if state.phase() == ConnectionPhase::Connected || state.failure().is_some() {
                return state;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| "DAVE-ready connection timed out".into())
}

async fn wait_for_resume(
    connection: &oto::VoiceConnection,
) -> Result<oto::ConnectionSnapshot, Box<dyn Error>> {
    timeout(RESUME_TIMEOUT, async {
        loop {
            let state = connection.state();
            if (state.stats().resume_successes() >= 1
                && state.phase() == ConnectionPhase::Connected)
                || state.failure().is_some()
            {
                return state;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| "buffered DAVE resume timed out".into())
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
    let soak = soak_duration()?;
    let expect_resume = std::env::var_os("OTO_LIVE_EXPECT_RESUME").is_some();
    let oto = Oto::builder().build()?;
    let connection = timeout(CONNECT_TIMEOUT, oto.connect(info))
        .await
        .map_err(|_| "live connection timed out")??;

    let probe = async {
        let ready = wait_for_connected(&connection).await?;
        if ready.phase() != ConnectionPhase::Connected {
            if let Some(failure) = ready.failure() {
                eprintln!(
                    "failure: kind={:?} operation={:?} retry={:?} safe_code={:?}",
                    failure.kind(),
                    failure.operation(),
                    failure.retry_disposition(),
                    failure.safe_code()
                );
            }
            return Err("connection did not reach DAVE-ready Connected state".into());
        }
        sleep(IDLE_OBSERVATION).await;
        let connected = connection.state();
        println!(
            "connected: generation={} phase={:?}",
            connected.generation().get(),
            connected.phase()
        );
        if connected.phase() != ConnectionPhase::Connected {
            if let Some(failure) = connected.failure() {
                eprintln!(
                    "failure: kind={:?} operation={:?} retry={:?} safe_code={:?}",
                    failure.kind(),
                    failure.operation(),
                    failure.retry_disposition(),
                    failure.safe_code()
                );
            }
            return Err("connection left Connected during idle observation".into());
        }

        if expect_resume {
            let resumed = wait_for_resume(&connection).await?;
            let stats = resumed.stats();
            println!(
                "resume: generation={} phase={:?} attempts={} successes={} heartbeat_timeouts={}",
                resumed.generation().get(),
                resumed.phase(),
                stats.resume_attempts(),
                stats.resume_successes(),
                stats.heartbeat_timeouts()
            );
            if resumed.generation().get() != 1
                || resumed.phase() != ConnectionPhase::Connected
                || resumed.failure().is_some()
                || stats.resume_attempts() != 1
                || stats.resume_successes() != 1
                || stats.heartbeat_timeouts() != 1
            {
                return Err("buffered DAVE resume did not meet acceptance criteria".into());
            }
        }

        let (starvable, control) = StarvableProbe::new();
        let sender = connection.start_audio(starvable).await?;
        sleep(STARVATION_OBSERVATION).await;
        let initially_starved = sender.state();
        if initially_starved.stats().frames_sent() != 0
            || initially_starved.stats().silence_frames_sent() != 0
        {
            return Err("initially starved source emitted media".into());
        }

        control.provide(PROBE_FRAMES)?;
        wait_for_frames(&sender, u64::from(PROBE_FRAMES)).await?;
        sleep(STARVATION_OBSERVATION).await;
        let starved = sender.state();
        if starved.stats().frames_sent() != u64::from(PROBE_FRAMES)
            || starved.stats().silence_frames_sent() != TERMINAL_SILENCE_FRAMES
            || starved.phase() != AudioPhase::WaitingForSource
        {
            return Err("starved source did not complete one bounded silence drain".into());
        }

        control.provide(PROBE_FRAMES)?;
        control.end()?;
        let _ = wait_for_audio_stop(&sender).await?;
        let starvation_cycle = sender.stop().await?;

        let restart = connection
            .start_audio(BoundedProbe {
                remaining: PROBE_FRAMES,
            })
            .await?;
        let _ = wait_for_audio_stop(&restart).await?;
        let restart_cycle = restart.stop().await?;

        if !soak.is_zero() {
            sleep(soak).await;
            if connection.state().phase() != ConnectionPhase::Connected {
                return Err("connection left Connected during soak".into());
            }
        }

        Ok::<_, Box<dyn Error>>((starvation_cycle, restart_cycle))
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
    let (starvation_cycle, restart_cycle) = probe?;
    let starvation_stats = starvation_cycle.stats();
    println!(
        "audio_starvation_cycle: phase={:?} frames={} terminal_silence={} failures={}",
        starvation_cycle.phase(),
        starvation_stats.frames_sent(),
        starvation_stats.silence_frames_sent(),
        starvation_stats.send_failures()
    );
    let restart_stats = restart_cycle.stats();
    println!(
        "audio_restart_cycle: phase={:?} frames={} terminal_silence={} failures={}",
        restart_cycle.phase(),
        restart_stats.frames_sent(),
        restart_stats.silence_frames_sent(),
        restart_stats.send_failures()
    );

    if final_connection.phase() != ConnectionPhase::Closed
        || final_connection.close_reason() != Some(CloseReason::ExplicitShutdown)
        || starvation_cycle.phase() != AudioPhase::Stopped
        || starvation_cycle.failure().is_some()
        || starvation_stats.frames_sent() != u64::from(PROBE_FRAMES) * 2
        || starvation_stats.silence_frames_sent() != TERMINAL_SILENCE_FRAMES * 2
        || starvation_stats.send_failures() != 0
        || restart_cycle.phase() != AudioPhase::Stopped
        || restart_cycle.failure().is_some()
        || restart_stats.frames_sent() != u64::from(PROBE_FRAMES)
        || restart_stats.silence_frames_sent() != TERMINAL_SILENCE_FRAMES
        || restart_stats.send_failures() != 0
    {
        return Err("live DAVE smoke did not meet its acceptance criteria".into());
    }
    Ok(())
}
