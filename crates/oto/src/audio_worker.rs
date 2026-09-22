//! Optional isolated audio executor. One bounded worker shared by all voices.
use std::future::Future;
#[cfg(feature = "experimental-audio-worker")]
use std::sync::OnceLock;
#[cfg(feature = "experimental-audio-worker")]
static WORKER: OnceLock<Result<tokio::runtime::Runtime, std::io::Error>> = OnceLock::new();

#[cfg(feature = "experimental-audio-worker")]
fn runtime() -> Result<&'static tokio::runtime::Runtime, &'static std::io::Error> {
    WORKER
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .max_blocking_threads(1)
                .thread_name("oto-audio")
                .enable_all()
                .build()
        })
        .as_ref()
}

pub(crate) fn initialize() -> Result<(), crate::Error> {
    #[cfg(feature = "experimental-audio-worker")]
    runtime().map_err(|_| {
        crate::Error::new(
            crate::ErrorKind::RuntimeUnavailable,
            crate::Operation::Connect,
            None,
            crate::RetryDisposition::Fatal,
            None,
            "failed to initialize isolated audio worker",
        )
    })?;
    Ok(())
}

pub(crate) fn spawn<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    #[cfg(feature = "experimental-audio-worker")]
    {
        runtime()
            .expect("audio worker initialized before connection")
            .spawn(future)
    }
    #[cfg(not(feature = "experimental-audio-worker"))]
    {
        tokio::spawn(future)
    }
}
