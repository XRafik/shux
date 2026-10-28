//! A Unix socket that keeps its path.
//!
//! The daemon holds its instance lock for as long as it runs, so while it does
//! no replacement can start: if its socket file is unlinked -- by a tmp cleaner,
//! or by a daemon from before the lock tidying up on its way out -- clients find
//! nothing there for good. So the file is rebound when it goes missing, and
//! removed on drop only while it is still this listener's.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::UnixListener;
use tracing::warn;

/// How often a socket checks that its path still reaches it.
pub const CHECK_INTERVAL: Duration = Duration::from_millis(500);

/// Asked before each rebind: whether this process may still serve the path.
pub type RebindGate = Arc<dyn Fn() -> bool + Send + Sync>;

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
    let listener = UnixListener::bind(path)?;
    // PRD §13.1.
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    Ok((listener, file_id(path)?))
}

/// Whether nobody else can put a file at a name in `path`'s directory: it is
/// not a symlink, and is ours and unwritable by others, or sticky. A rebind
/// binds and chmods by path, so anywhere weaker it could be redirected.
fn parent_is_private(path: &Path) -> bool {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    let Ok(dir) = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW)
        .open(parent)
    else {
        return false;
    };
    dir.metadata().is_ok_and(|meta| {
        let mine = meta.uid() == nix::unistd::geteuid().as_raw() && meta.mode() & 0o022 == 0;
        mine || meta.mode() & 0o1000 != 0
    })
}

impl OwnedSocket {
    /// Bind `path`, replacing a stale file there but never a live socket.
    pub fn bind(path: &Path) -> io::Result<Self> {
        if std::os::unix::net::UnixStream::connect(path).is_ok() {
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                format!("{} is already being served", path.display()),
            ));
        }
        match std::fs::remove_file(path) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
            _ => {}
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
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

    /// Rebind if the path is missing and `gate` allows it. Errors once another
    /// file holds the path: clients reach that, never this listener again.
    pub fn check(&mut self, gate: Option<&RebindGate>) -> io::Result<()> {
        match file_id(&self.path) {
            Ok(id) if id == self.id => return Ok(()),
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    format!("{} now belongs to another listener", self.path.display()),
                ));
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(_) => return Ok(()),
        }
        if gate.is_some_and(|may| !may()) {
            return Ok(());
        }
        if !parent_is_private(&self.path) {
            warn!(path = %self.path.display(), "socket was unlinked; its directory is not private, not rebinding");
            return Ok(());
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
        Ok(())
    }
}

impl Drop for OwnedSocket {
    fn drop(&mut self) {
        if file_id(&self.path).is_ok_and(|id| id == self.id) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}
