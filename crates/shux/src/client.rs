//! Client-side daemon auto-start and connection logic.
//!
//! The CLI probes the UDS. On `ConnectionRefused` or `NotFound`, it spawns
//! the daemon via re-exec, then retries with exponential backoff (PRD 4.5).
//!
//! On successful connection, the client performs a version handshake: if the
//! running daemon's version differs from the client binary's version (e.g.,
//! after a rebuild), the stale daemon is killed and a fresh one is spawned.

use std::io;
use std::path::Path;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tracing::{debug, info, warn};

use crate::daemon::{self, DaemonError};

/// The version of this binary, baked in at compile time.
const CLIENT_VERSION: &str = env!("CARGO_PKG_VERSION");

/// The git SHA of this binary, baked in at compile time.
const CLIENT_GIT_SHA: &str = env!("SHUX_GIT_SHA");

/// Maximum number of retries when waiting for the daemon to start.
const MAX_CONNECT_RETRIES: u32 = 10;

/// Initial backoff delay between connection attempts.
const INITIAL_BACKOFF: Duration = Duration::from_millis(50);

/// Maximum backoff delay.
const MAX_BACKOFF: Duration = Duration::from_millis(2000);

/// How long a daemon gets to exit after it stops serving. It drains panes for
/// up to 3s before it releases the instance lock.
const STALE_EXIT_TIMEOUT: Duration = Duration::from_secs(5);

/// How long to wait for another client to finish starting the daemon. Covers
/// the holder's stale-daemon wait plus its full connect backoff.
const START_LOCK_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("daemon error")]
    Daemon(#[from] DaemonError),
    #[error("failed to connect to daemon after {0} retries")]
    ConnectionFailed(u32),
    #[error("I/O error")]
    Io(#[from] io::Error),
    #[error(
        "the running daemon (pid {0}) is from another shux build and did not exit after SIGTERM; \
         it may be draining a session -- retry, or run `shux daemon stop`"
    )]
    StaleDaemonAlive(u32),
    #[error(
        "a daemon for this socket{0} is still running but no longer serving it; \
         it may be shutting down -- retry, or run `shux daemon stop`"
    )]
    DaemonNotServing(String),
    #[error(
        "{} is served by a daemon from another shux build that no pidfile names; \
         stop that process and retry",
        .0.display()
    )]
    StaleDaemonUnidentified(std::path::PathBuf),
    #[error("timed out waiting for another shux client to start the daemon (lock {})", .0.display())]
    StartLockTimeout(std::path::PathBuf),
}

/// Probe the daemon socket. Returns a connected `UnixStream` if successful.
async fn probe_socket(socket_path: &Path) -> Result<UnixStream, io::Error> {
    UnixStream::connect(socket_path).await
}

/// Version info returned by the daemon's `system.version` RPC call.
struct DaemonVersion {
    version: String,
    git_sha: String,
}

/// Check the daemon's version via `system.version` RPC call.
async fn check_daemon_version(stream: &mut UnixStream) -> Option<DaemonVersion> {
    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": "version-check",
        "method": "system.version",
        "params": {}
    });
    let payload = serde_json::to_vec(&request).ok()?;

    // Write length-prefixed frame
    let len = payload.len() as u32;
    stream.write_all(&len.to_be_bytes()).await.ok()?;
    stream.write_all(&payload).await.ok()?;
    stream.flush().await.ok()?;

    // Read response
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.ok()?;
    let resp_len = u32::from_be_bytes(len_buf) as usize;
    if resp_len > 1024 * 1024 {
        return None;
    }
    let mut resp_buf = vec![0u8; resp_len];
    stream.read_exact(&mut resp_buf).await.ok()?;

    let response: serde_json::Value = serde_json::from_slice(&resp_buf).ok()?;
    let result = response.get("result")?;
    let version = result.get("version")?.as_str()?.to_string();
    let git_sha = result
        .get("git_sha")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    Some(DaemonVersion { version, git_sha })
}

/// Kill the daemon serving `socket` by SIGTERM, if the pidfile names one of ours.
///
/// Fires on a version mismatch -- an ordinary `session list` against an older
/// daemon -- and used to SIGTERM whatever the pidfile held, unverified. Returns
/// the pid signalled, so the caller can wait for that process to exit.
fn kill_stale_daemon(socket: &Path) -> Option<u32> {
    let Some(crate::daemon_boot::LiveDaemon { pid, pidfile }) =
        crate::daemon_boot::our_live_daemon(socket)
    else {
        let _ = daemon::remove_pid_file_for(socket);
        return None;
    };

    // Safety: sending SIGTERM to a known process
    let result = nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid as i32),
        nix::sys::signal::Signal::SIGTERM,
    );

    if result.is_ok() {
        info!(pid, "Sent SIGTERM to stale daemon");
        Some(pid)
    } else {
        debug!(pid, "Failed to send SIGTERM (process may already be gone)");
        // Clean up stale files even if kill failed
        let _ = std::fs::remove_file(&pidfile);
        let _ = daemon::remove_socket_file_for(socket);
        None
    }
}

/// Wait for a stale daemon that was sent SIGTERM to exit.
///
/// Identity rather than bare liveness: a zombie under a non-reaping init still
/// answers `kill(pid, 0)`.
async fn wait_for_stale_exit(socket: &Path, pid: u32) -> Result<(), ClientError> {
    let deadline = tokio::time::Instant::now() + STALE_EXIT_TIMEOUT;
    while crate::daemon_boot::is_live_shux_daemon(pid, socket) {
        if tokio::time::Instant::now() >= deadline {
            return Err(ClientError::StaleDaemonAlive(pid));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Ok(())
}

/// Stop the stale daemon serving `socket`.
///
/// One that nothing names is left alone: unlinking its socket would strand it
/// running, unreachable, and still holding the instance lock.
async fn stop_stale_daemon(socket: &Path) -> Result<(), ClientError> {
    if let Some(pid) = kill_stale_daemon(socket) {
        return wait_for_stale_exit(socket, pid).await;
    }
    for _ in 0..20 {
        if probe_socket(socket).await.is_err() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(ClientError::StaleDaemonUnidentified(socket.to_path_buf()))
}

/// Wait until no daemon holds the instance lock for `socket`.
///
/// A daemon stops serving before it finishes draining its panes, and holds the
/// lock until it has; a replacement spawned in that window would exit at once.
async fn wait_for_instance_lock(socket: &Path) -> Result<(), ClientError> {
    let path = daemon::instance_lock_path_for(socket)?;
    let deadline = tokio::time::Instant::now() + STALE_EXIT_TIMEOUT;
    while daemon::try_lock_file(&path)?.is_none() {
        if tokio::time::Instant::now() >= deadline {
            let holder =
                daemon::read_pid_at(&path).map_or_else(String::new, |p| format!(" (pid {p})"));
            return Err(ClientError::DaemonNotServing(holder));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Ok(())
}

/// What a probe of the socket found.
enum Probe {
    /// A daemon of this build answered; the stream is fresh and unused.
    Current(UnixStream),
    /// A daemon answered, but it is another build or did not say which.
    Stale,
    /// Nothing is listening.
    Absent,
}

async fn probe_daemon(socket_path: &Path) -> Result<Probe, ClientError> {
    let mut stream = match probe_socket(socket_path).await {
        Ok(stream) => stream,
        Err(e) if is_connection_refused_or_not_found(&e) => return Ok(Probe::Absent),
        Err(e) => return Err(ClientError::Io(e)),
    };
    match check_daemon_version(&mut stream).await {
        Some(dv) if dv.version == CLIENT_VERSION && dv.git_sha == CLIENT_GIT_SHA => {
            debug!(
                version = CLIENT_VERSION,
                git_sha = CLIENT_GIT_SHA,
                "Daemon version matches"
            );
            // Need a fresh connection since we consumed the first one for version check
            drop(stream);
            Ok(Probe::Current(
                probe_socket(socket_path).await.map_err(ClientError::Io)?,
            ))
        }
        Some(dv) => {
            warn!(
                daemon_version = dv.version,
                daemon_sha = dv.git_sha,
                client_version = CLIENT_VERSION,
                client_sha = CLIENT_GIT_SHA,
                "Daemon version mismatch — restarting daemon"
            );
            Ok(Probe::Stale)
        }
        None => {
            warn!("Could not check daemon version — restarting daemon");
            Ok(Probe::Stale)
        }
    }
}

/// Take the start lock for `socket`, waiting for any client already holding it.
///
/// Polled rather than blocking so a wedged holder costs a bounded wait and a
/// clear error, not a hung CLI.
async fn acquire_start_lock(socket: &Path) -> Result<std::fs::File, ClientError> {
    daemon::ensure_runtime_dir()?;
    let path = daemon::start_lock_path_for(socket)?;
    let deadline = tokio::time::Instant::now() + START_LOCK_TIMEOUT;
    loop {
        if let Some(lock) = daemon::try_lock_file(&path)? {
            return Ok(lock);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(ClientError::StartLockTimeout(path));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Ensure the daemon is running at the given socket path and return a connected socket.
///
/// 1. Try to connect to the UDS; a daemon of this build is used as is
/// 2. Otherwise take the start lock, so concurrent clients start one daemon
/// 3. Probe again under the lock: another client may have started it already
/// 4. Stop a stale daemon, wait for any daemon still draining, then spawn
/// 5. Retry with exponential backoff until connected or max retries
pub async fn ensure_daemon_running_at(socket_path: &Path) -> Result<UnixStream, ClientError> {
    if let Probe::Current(stream) = probe_daemon(socket_path).await? {
        return Ok(stream);
    }

    // Held until this function returns, i.e. until the daemon it started (or
    // found) is accepting connections.
    let _start_lock = acquire_start_lock(socket_path).await?;
    match probe_daemon(socket_path).await? {
        Probe::Current(stream) => return Ok(stream),
        Probe::Stale => stop_stale_daemon(socket_path).await?,
        Probe::Absent => info!("Daemon not running, starting..."),
    }
    wait_for_instance_lock(socket_path).await?;

    // Spawn the daemon process
    start_daemon_process(socket_path)?;

    // Retry with exponential backoff
    let mut backoff = INITIAL_BACKOFF;
    for attempt in 1..=MAX_CONNECT_RETRIES {
        tokio::time::sleep(backoff).await;

        match probe_socket(socket_path).await {
            Ok(stream) => {
                info!(attempt, "Connected to daemon");
                return Ok(stream);
            }
            Err(e) if is_connection_refused_or_not_found(&e) => {
                debug!(
                    attempt,
                    backoff_ms = backoff.as_millis(),
                    "Daemon not ready yet, retrying..."
                );
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
            Err(e) => return Err(ClientError::Io(e)),
        }
    }

    Err(ClientError::ConnectionFailed(MAX_CONNECT_RETRIES))
}

/// Ensure the daemon is running (using default socket path) and return a connected socket.
#[allow(dead_code)] // Available for backward compatibility
pub async fn ensure_daemon_running() -> Result<UnixStream, ClientError> {
    let sock_path = daemon::socket_path()?;
    ensure_daemon_running_at(&sock_path).await
}

/// Start the daemon process by re-executing the current binary with
/// an internal `__daemon` subcommand.
///
/// We use re-exec rather than in-process fork+daemonize because the client
/// may already have a tokio runtime running. The `__daemon` subcommand
/// calls `daemonize()` before creating any runtime.
fn start_daemon_process(socket_path: &Path) -> Result<(), ClientError> {
    let exe = std::env::current_exe().map_err(ClientError::Io)?;

    let mut cmd = std::process::Command::new(exe);
    cmd.arg("__daemon");

    // Tell the daemon which socket to serve, explicitly. `--socket` is a
    // client-side flag and the re-exec carried neither it nor a guarantee that
    // `SHUX_SOCKET` was set, so the daemon fell back to the default path while
    // the client went on probing the one it was asked for. The two never met:
    // the client retried, gave up, and left a perfectly healthy daemon behind
    // with nothing referencing it — once per invocation, each overwriting the
    // pidfile so `daemon stop` could only reap the last.
    cmd.arg("--socket").arg(socket_path);

    // Detach: don't inherit stdin/stdout/stderr
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::null());

    cmd.spawn().map_err(ClientError::Io)?;

    Ok(())
}

/// Try to connect to an existing daemon without auto-starting.
/// Returns Ok(stream) if daemon is already running, Err otherwise.
pub async fn try_connect(socket_path: &Path) -> Result<UnixStream, ClientError> {
    probe_socket(socket_path).await.map_err(ClientError::Io)
}

fn is_connection_refused_or_not_found(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound
    )
}
