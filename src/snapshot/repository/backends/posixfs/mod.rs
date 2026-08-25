mod artifacts;
mod backend;
mod catalog;
mod layout;
mod runtime;

use std::fs;
use std::io::Write;
use std::path::Path;

use tempfile::NamedTempFile;
use tokio::task;

use crate::snapshot::{RepositoryError, RepositoryResult};

pub use backend::{PosixFsBackend, PosixFsBackendConfig};
pub(crate) use backend::{PosixFsLocalArtifactStore, PosixFsSnapshotRepository};
pub(crate) use layout::PosixFsSnapshotArtifactLayout;

fn persist_atomic_file(
    parent: &Path,
    destination: &Path,
    bytes: &[u8],
    kind: &str,
) -> RepositoryResult<()> {
    let mut temp = NamedTempFile::new_in(parent).map_err(|error| {
        RepositoryError::backend(
            format!("create temp {kind} in '{}'", parent.display()),
            error,
        )
    })?;
    temp.write_all(bytes).map_err(|error| {
        RepositoryError::backend(format!("write {kind} '{}'", temp.path().display()), error)
    })?;
    temp.as_file().sync_all().map_err(|error| {
        RepositoryError::backend(format!("sync {kind} '{}'", temp.path().display()), error)
    })?;
    let temp_path = temp.path().to_path_buf();
    temp.persist(destination).map_err(|error| {
        RepositoryError::backend(
            format!(
                "persist {kind} '{}' -> '{}'",
                temp_path.display(),
                destination.display()
            ),
            error.error,
        )
    })?;
    sync_dir(parent)
}

fn sync_dir(path: &Path) -> RepositoryResult<()> {
    fs::File::open(path)
        .map_err(|error| {
            RepositoryError::backend(format!("open directory '{}'", path.display()), error)
        })?
        .sync_all()
        .map_err(|error| {
            RepositoryError::backend(format!("sync directory '{}'", path.display()), error)
        })
}

async fn run_repository_blocking<T, F>(operation: &'static str, work: F) -> RepositoryResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> RepositoryResult<T> + Send + 'static,
{
    task::spawn_blocking(work)
        .await
        .map_err(|error| RepositoryError::Backend {
            message: format!("repository blocking task panicked while trying to {operation}"),
            source: Some(anyhow::Error::from(error)),
        })?
}
