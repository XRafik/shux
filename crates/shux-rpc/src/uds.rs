//! A Unix socket that keeps its path.
//!
//! The daemon holds its instance lock for as long as it runs, so while it does
//! no replacement can start: if its socket file is unlinked -- by a tmp cleaner,
//! or by a daemon from before the lock tidying up on its way out -- clients find
//! nothing there for good. So the file is rebound when it goes missing, and
//! removed on drop only while it is still this listener's.

use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::net::UnixListener;
use tracing::warn;

/// How often a socket checks that its path still reaches it.
pub const CHECK_INTERVAL: Duration = Duration::from_millis(500);

pub struct OwnedSocket {
    path: PathBuf,
    listener: UnixListener,
    id: (u64, u64),
}

/// Device and inode: what makes a socket file this listener's.
fn file_id(path: &Path) -> io::Result<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    let meta = std::fs::symlink_metadata(path)?;
    Ok((meta.dev(), meta.ino()))
}

fn bind_at(path: &Path) -> io::Result<(UnixListener, (u64, u64))> {
    use std::os::unix::fs::PermissionsExt;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let listener = UnixListener::bind(path)?;
    // PRD §13.1.
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    Ok((listener, file_id(path)?))
}

impl OwnedSocket {
    /// Bind `path`, replacing a stale file there.
    pub fn bind(path: &Path) -> io::Result<Self> {
        match std::fs::remove_file(path) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
            _ => {}
        }
        let (listener, id) = bind_at(path)?;
        Ok(Self {
            path: path.to_path_buf(),
            listener,
            id,
        })
    }

    pub fn listener(&self) -> &UnixListener {
        &self.listener
    }

    /// Rebind if the path is missing. One bound since is someone else's.
    pub fn rebind_if_missing(&mut self) {
        if !std::fs::symlink_metadata(&self.path)
            .is_err_and(|e| e.kind() == io::ErrorKind::NotFound)
        {
            return;
        }
        match bind_at(&self.path) {
            Ok((listener, id)) => {
                warn!(path = %self.path.display(), "socket was unlinked; rebound it");
                self.listener = listener;
                self.id = id;
            }
            Err(e) => {
                warn!(path = %self.path.display(), error = %e, "socket was unlinked; rebind failed")
            }
        }
    }
}

impl Drop for OwnedSocket {
    fn drop(&mut self) {
        if file_id(&self.path).is_ok_and(|id| id == self.id) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}
