use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use nix::errno::Errno;
use nix::fcntl::{Flock, FlockArg};
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::layout::PosixFsSnapshotArtifactLayout;
use super::persist_atomic_file;
use crate::snapshot::repository::SnapshotListFilter;
use crate::snapshot::types::now_unix_ms;
use crate::snapshot::{
    CommittedSnapshot, RepositoryError, RepositoryResult, SnapshotAlias, SnapshotId,
    SnapshotPublishMetadata, SnapshotRecord, SnapshotType, TemplateBuildErrorReason,
};
const FILE_LOCK_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct PosixFsCatalogStore {
    root: PathBuf,
}

#[derive(Debug)]
pub(crate) struct PublishSession {
    pub(crate) snapshot_id: SnapshotId,
    /// Serializes publish and delete for this identity.
    _record_lock: PosixFileLockGuard,
}

/// Kernel-owned advisory lock. Its stable path can outlive a process and be
/// reused safely after the file descriptor is closed or the process exits.
pub(super) type PosixFileLockGuard = Flock<fs::File>;

impl PosixFsCatalogStore {
    /// Creates a catalog store rooted at the repository's durable POSIX directory.
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn layout(&self, snapshot_id: &SnapshotId) -> PosixFsSnapshotArtifactLayout {
        PosixFsSnapshotArtifactLayout::new(&self.root, snapshot_id)
    }

    fn commit_marker_path(&self, snapshot_id: &SnapshotId) -> PathBuf {
        self.layout(snapshot_id)
            .path(super::layout::POSIXFS_SNAPSHOT_COMMIT_MARKER)
    }

    fn record_path(&self, snapshot_id: &SnapshotId) -> PathBuf {
        PosixFsSnapshotArtifactLayout::record_path(&self.root, snapshot_id)
    }

    /// Starts a publish session by creating the snapshot directory under the durable catalog root.
    pub(crate) fn begin_publish(
        &self,
        snapshot_id: &SnapshotId,
    ) -> RepositoryResult<PublishSession> {
        let record_lock = self.acquire_record_lock(snapshot_id)?;
        let snapshot_dir = self.layout(snapshot_id).snapshot_dir();
        fs::create_dir_all(&snapshot_dir).map_err(|error| {
            RepositoryError::backend(
                format!("create snapshot dir '{}'", snapshot_dir.display()),
                error,
            )
        })?;
        Ok(PublishSession {
            snapshot_id: snapshot_id.clone(),
            _record_lock: record_lock,
        })
    }

    /// Commits one imported snapshot into the catalog after its artifact
    /// closure and commit marker are ready.
    pub(crate) fn commit_publish(
        &self,
        session: &PublishSession,
        metadata: SnapshotPublishMetadata,
        committed: CommittedSnapshot,
    ) -> RepositoryResult<SnapshotRecord> {
        if let Some(existing) = self.validate_publish_transition(session, &metadata)? {
            return Ok(existing);
        }
        let now = now_unix_ms();
        let snapshot_id = metadata.id.clone();
        let record = self.committed_record_unlocked(&metadata, committed, now)?;
        let write_result = if let Some(alias) = metadata.alias.as_ref() {
            self.with_alias_lock(alias, |store| {
                let alias_path = PosixFsSnapshotArtifactLayout::alias_path(&store.root, alias);
                store.ensure_alias_available(alias, &snapshot_id)?;
                store.write_commit_marker(&session.snapshot_id)?;
                store.write_json(&alias_path, &snapshot_id)?;
                store.write_record_unlocked(&record)
            })
        } else {
            (|| {
                self.write_commit_marker(&session.snapshot_id)?;
                self.write_record_unlocked(&record)
            })()
        };

        match write_result {
            Ok(()) => Ok(record),
            Err(error) => {
                let _ = self.cleanup_uncommitted_snapshot_dir(&session.snapshot_id);
                Err(error)
            }
        }
    }

    /// Cleans up an unfinished publish session that never reached a visible committed record.
    pub(crate) fn abort_publish(&self, session: &PublishSession) -> RepositoryResult<()> {
        self.cleanup_uncommitted_snapshot_dir(&session.snapshot_id)
    }

    pub(crate) fn create(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        self.ensure_layout()?;
        record
            .validate_template_create()
            .map_err(|reason| RepositoryError::InvalidRequest { reason })?;

        let _record_guard = self.acquire_record_lock(&record.id)?;
        if let Some(existing) = self.load_record_by_id_unlocked(&record.id)? {
            if !existing.same_catalog_contents(&record) {
                return Err(RepositoryError::InvalidRequest {
                    reason: format!(
                        "snapshot '{}' already exists with different metadata",
                        record.id
                    ),
                });
            }
            if let Some(alias) = record.alias.as_ref() {
                self.with_alias_lock(alias, |store| store.bind_alias_unlocked(alias, &record.id))?;
            }
            return Ok(existing);
        }

        let write_result = if let Some(alias) = record.alias.as_ref() {
            self.with_alias_lock(alias, |store| {
                store.bind_alias_unlocked(alias, &record.id)?;
                store.write_record_unlocked(&record)
            })
        } else {
            self.write_record_unlocked(&record)
        };
        write_result.map(|()| record)
    }

    /// Persists canonical Local metadata without creating a second physical closure.
    pub(crate) fn commit_record(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        if record.snapshot_type != SnapshotType::Local {
            return Err(RepositoryError::InvalidRequest {
                reason: "metadata-only commits are supported only for Local snapshots".to_string(),
            });
        }
        record
            .validate_committed_metadata()
            .map_err(|reason| RepositoryError::InvalidRequest { reason })?;
        let _record_guard = self.acquire_record_lock(&record.id)?;
        if let Some(previous) = self.load_record_by_id_unlocked(&record.id)? {
            if !previous.same_catalog_contents(&record) {
                return Err(RepositoryError::InvalidRequest {
                    reason: format!(
                        "snapshot '{}' canonical metadata does not match the existing record",
                        record.id
                    ),
                });
            }
            return Ok(previous);
        }
        self.write_record_unlocked(&record)?;
        Ok(record)
    }

    /// Checks a publish while the session's record lock is held. Returning a
    /// record means the request is an equivalent retry and no artifact should
    /// be imported or metadata overwritten.
    pub(crate) fn validate_publish_transition(
        &self,
        session: &PublishSession,
        metadata: &SnapshotPublishMetadata,
    ) -> RepositoryResult<Option<SnapshotRecord>> {
        if session.snapshot_id != metadata.id {
            return Err(RepositoryError::InvalidRequest {
                reason: "publish session and metadata snapshot IDs do not match".to_string(),
            });
        }
        metadata
            .validate()
            .map_err(|reason| RepositoryError::InvalidRequest { reason })?;
        let Some(existing) = self.load_record_by_id_unlocked(&metadata.id)? else {
            return Ok(None);
        };

        if existing.committed.is_none() {
            return Ok(None);
        }
        if existing.snapshot_type != metadata.snapshot_type
            && !(existing.snapshot_type == SnapshotType::Local
                && metadata.snapshot_type == SnapshotType::Distributed)
        {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("snapshot '{}' storage type cannot transition", metadata.id),
            });
        }
        if existing.owner_node_id != metadata.owner_node_id
            && !(existing.snapshot_type == SnapshotType::Local
                && metadata.snapshot_type == SnapshotType::Distributed)
        {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("snapshot '{}' owner metadata does not match", metadata.id),
            });
        }
        if !existing.matches_publish_metadata(metadata) {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("snapshot '{}' metadata does not match", metadata.id),
            });
        }
        if existing.snapshot_type == SnapshotType::Local
            && metadata.snapshot_type == SnapshotType::Distributed
        {
            return Ok(None);
        }
        if !self.commit_marker_path(&metadata.id).exists() {
            return Ok(None);
        }
        if let Some(alias) = existing.alias.as_ref() {
            self.with_alias_lock(alias, |store| {
                store.bind_alias_unlocked(alias, &existing.id)
            })?;
        }
        Ok(Some(existing))
    }

    pub(crate) fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        self.ensure_layout()?;
        if let Ok(direct_id) = SnapshotId::parse(id_or_alias) {
            if let Some(record) = self.load_record_by_id_unlocked(&direct_id)? {
                return Ok(Some(record));
            }
        }

        let alias =
            SnapshotAlias::parse(id_or_alias).map_err(|error| RepositoryError::InvalidRequest {
                reason: error.to_string(),
            })?;
        self.load_alias_record(&alias, |store, id| store.load_record_by_id_unlocked(id))
    }

    pub(crate) fn list(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        self.ensure_layout()?;
        let mut records = self
            .load_all_records_unlocked()?
            .into_iter()
            .filter(|record| {
                (record.committed.is_none() || self.is_committed(&record.id))
                    && filter.matches(record)
            })
            .collect::<Vec<_>>();
        records.sort_by(|left, right| {
            right
                .created_at_unix_ms
                .cmp(&left.created_at_unix_ms)
                .then_with(|| left.id.to_string().cmp(&right.id.to_string()))
        });
        Ok(records)
    }

    fn load_all_records_unlocked(&self) -> RepositoryResult<Vec<SnapshotRecord>> {
        let records_dir = self.records_dir();
        let mut records = Vec::new();
        for entry in fs::read_dir(&records_dir).map_err(|error| {
            RepositoryError::backend(
                format!("read records dir '{}'", records_dir.display()),
                error,
            )
        })? {
            let entry = entry.map_err(|error| {
                RepositoryError::backend(
                    format!("read entry in '{}'", records_dir.display()),
                    error,
                )
            })?;
            if !entry
                .file_type()
                .map_err(|error| {
                    RepositoryError::backend(
                        format!("inspect file type '{}'", entry.path().display()),
                        error,
                    )
                })?
                .is_file()
            {
                continue;
            }
            if entry.path().extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            records.push(self.read_json(&entry.path())?);
        }
        Ok(records)
    }

    pub(crate) fn delete_record(&self, id: &SnapshotId) -> RepositoryResult<bool> {
        let _record_guard = self.acquire_record_lock(id)?;
        let Some(record) = self.load_record_by_id_unlocked(id)? else {
            return Ok(false);
        };
        let snapshot_layout = PosixFsSnapshotArtifactLayout::new(&self.root, id);
        self.remove_file_if_exists(
            &snapshot_layout.path(super::layout::POSIXFS_SNAPSHOT_COMMIT_MARKER),
        )?;
        self.remove_dir_if_exists(&snapshot_layout.snapshot_dir())?;
        if let Some(alias) = record.alias.as_ref() {
            self.with_alias_lock(alias, |store| {
                if store.load_alias_target(alias)?.as_ref() == Some(id) {
                    store.remove_file_if_exists(&PosixFsSnapshotArtifactLayout::alias_path(
                        &store.root,
                        alias,
                    ))?;
                }
                store.remove_file_if_exists(&store.record_path(id))
            })?;
        } else {
            self.remove_file_if_exists(&self.record_path(id))?;
        }
        Ok(true)
    }

    /// Resolves one alias to a committed snapshot id and drops stale alias entries on the way.
    pub(crate) fn resolve_alias(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        let alias =
            SnapshotAlias::parse(alias).map_err(|error| RepositoryError::InvalidRequest {
                reason: error.to_string(),
            })?;
        self.with_alias_lock(&alias, |store| {
            let Some(id) = store.load_alias_target(&alias)? else {
                return Ok(None);
            };
            if store.load_record_by_id_unlocked(&id)?.is_some() {
                return Ok(Some(id));
            }
            if store.load_record_by_id_unlocked(&id)?.is_none() {
                let alias_path = PosixFsSnapshotArtifactLayout::alias_path(&store.root, &alias);
                store.remove_file_if_exists(&alias_path)?;
            }
            Ok(None)
        })
    }

    fn aliases_dir(&self) -> PathBuf {
        PosixFsSnapshotArtifactLayout::aliases_dir(&self.root)
    }

    fn records_dir(&self) -> PathBuf {
        PosixFsSnapshotArtifactLayout::records_dir(&self.root)
    }

    fn snapshots_dir(&self) -> PathBuf {
        PosixFsSnapshotArtifactLayout::snapshots_dir(&self.root)
    }

    fn ensure_layout(&self) -> RepositoryResult<()> {
        let catalog_dir = PosixFsSnapshotArtifactLayout::catalog_dir(&self.root);
        let aliases_dir = self.aliases_dir();
        let records_dir = self.records_dir();
        let snapshots_dir = self.snapshots_dir();
        for dir in [&catalog_dir, &aliases_dir, &records_dir, &snapshots_dir] {
            fs::create_dir_all(dir).map_err(|error| {
                RepositoryError::backend(format!("create catalog dir '{}'", dir.display()), error)
            })?;
        }
        Ok(())
    }

    pub(crate) fn try_start(&self, id: &SnapshotId) -> RepositoryResult<SnapshotRecord> {
        let _guard = self.acquire_record_lock(id)?;
        let mut record = self.load_record_by_id_unlocked(id)?.ok_or_else(|| {
            RepositoryError::SnapshotNotFound {
                lookup: id.to_string(),
            }
        })?;
        record
            .start_template_build(now_unix_ms())
            .map_err(|reason| RepositoryError::InvalidRequest { reason })?;
        self.write_record_unlocked(&record)?;
        Ok(record)
    }

    pub(crate) fn mark_error(
        &self,
        id: &SnapshotId,
        reason: TemplateBuildErrorReason,
    ) -> RepositoryResult<()> {
        let _guard = self.acquire_record_lock(id)?;
        let mut record = self.load_record_by_id_unlocked(id)?.ok_or_else(|| {
            RepositoryError::SnapshotNotFound {
                lookup: id.to_string(),
            }
        })?;
        let changed = record
            .mark_template_build_error(&reason, now_unix_ms())
            .map_err(|reason| RepositoryError::InvalidRequest { reason })?;
        if !changed {
            return Ok(());
        }
        self.write_record_unlocked(&record)
    }

    fn read_json<T>(&self, path: &Path) -> RepositoryResult<T>
    where
        T: DeserializeOwned,
    {
        let bytes = fs::read(path).map_err(|error| {
            RepositoryError::backend(format!("read '{}'", path.display()), error)
        })?;
        serde_json::from_slice(&bytes).map_err(|error| {
            RepositoryError::backend(format!("parse json '{}'", path.display()), error)
        })
    }

    fn write_json<T>(&self, path: &Path, value: &T) -> RepositoryResult<()>
    where
        T: Serialize,
    {
        let bytes = serde_json::to_vec_pretty(value).map_err(|error| {
            RepositoryError::backend(format!("serialize json '{}'", path.display()), error)
        })?;
        self.write_atomic_bytes(path, &bytes, "json")
    }

    fn write_atomic_bytes(&self, path: &Path, bytes: &[u8], kind: &str) -> RepositoryResult<()> {
        let parent = path.parent().ok_or_else(|| RepositoryError::Backend {
            message: format!("resolve parent for '{}'", path.display()),
            source: None,
        })?;
        fs::create_dir_all(parent).map_err(|error| {
            RepositoryError::backend(format!("create '{}'", parent.display()), error)
        })?;
        persist_atomic_file(parent, path, bytes, kind)
    }

    fn write_commit_marker(&self, id: &SnapshotId) -> RepositoryResult<()> {
        let path = self.commit_marker_path(id);
        self.write_atomic_bytes(&path, b"committed", "commit marker")
    }

    fn remove_file_if_exists(&self, path: &Path) -> RepositoryResult<()> {
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(RepositoryError::backend(
                format!("remove '{}'", path.display()),
                error,
            )),
        }
    }

    fn remove_dir_if_exists(&self, path: &Path) -> RepositoryResult<()> {
        match fs::remove_dir_all(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(RepositoryError::backend(
                format!("remove '{}'", path.display()),
                error,
            )),
        }
    }

    fn is_committed(&self, id: &SnapshotId) -> bool {
        self.load_record_by_id_unlocked(id)
            .ok()
            .flatten()
            .is_some_and(|record| {
                record.committed.is_some()
                    && (record.snapshot_type == SnapshotType::Local
                        || self.commit_marker_path(id).exists())
            })
    }

    fn cleanup_uncommitted_snapshot_dir(&self, id: &SnapshotId) -> RepositoryResult<()> {
        // An existing Distributed record still owns its repository closure
        // even if its visibility marker is missing. Local canonical metadata
        // owns no artifacts under the primary repository root.
        if self.load_record_by_id_unlocked(id)?.is_some_and(|record| {
            record.snapshot_type == SnapshotType::Distributed && record.committed.is_some()
        }) {
            return Ok(());
        }
        let snapshot_layout = self.layout(id);
        self.remove_dir_if_exists(&snapshot_layout.snapshot_dir())
    }

    fn load_record_by_id_unlocked(
        &self,
        id: &SnapshotId,
    ) -> RepositoryResult<Option<SnapshotRecord>> {
        let path = self.record_path(id);
        if !path.exists() {
            return Ok(None);
        }
        self.read_json(&path).map(Some)
    }

    fn load_alias_target(&self, alias: &SnapshotAlias) -> RepositoryResult<Option<SnapshotId>> {
        let path = PosixFsSnapshotArtifactLayout::alias_path(&self.root, alias);
        if !path.exists() {
            return Ok(None);
        }
        self.read_json(&path).map(Some)
    }

    fn load_alias_record(
        &self,
        alias: &SnapshotAlias,
        read: impl FnOnce(&Self, &SnapshotId) -> RepositoryResult<Option<SnapshotRecord>>,
    ) -> RepositoryResult<Option<SnapshotRecord>> {
        self.with_alias_lock(alias, |store| {
            let Some(id) = store.load_alias_target(alias)? else {
                return Ok(None);
            };
            let record = read(store, &id)?;
            if record.is_none() && store.load_record_by_id_unlocked(&id)?.is_none() {
                store.remove_file_if_exists(&PosixFsSnapshotArtifactLayout::alias_path(
                    &store.root,
                    alias,
                ))?;
            }
            Ok(record)
        })
    }

    fn acquire_file_lock(
        &self,
        lock_path: PathBuf,
        label: &'static str,
        lock_arg: FlockArg,
    ) -> RepositoryResult<PosixFileLockGuard> {
        let deadline = Instant::now() + FILE_LOCK_TIMEOUT;
        loop {
            if let Some(guard) = self.try_acquire_file_lock(&lock_path, label, lock_arg)? {
                return Ok(guard);
            }
            if Instant::now() < deadline {
                thread::sleep(Duration::from_millis(25));
                continue;
            }
            return Err(RepositoryError::Backend {
                message: format!(
                    "timed out waiting for {label} lock '{}'",
                    lock_path.display()
                ),
                source: None,
            });
        }
    }

    fn try_acquire_file_lock(
        &self,
        lock_path: &Path,
        label: &'static str,
        lock_arg: FlockArg,
    ) -> RepositoryResult<Option<PosixFileLockGuard>> {
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                RepositoryError::backend(
                    format!("create {label} lock dir '{}'", parent.display()),
                    error,
                )
            })?;
        }
        let file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)
            .map_err(|error| {
                RepositoryError::backend(
                    format!("open {label} lock '{}'", lock_path.display()),
                    error,
                )
            })?;
        match Flock::lock(file, lock_arg) {
            Ok(file) => Ok(Some(file)),
            Err((_file, Errno::EWOULDBLOCK)) => Ok(None),
            Err((_file, error)) => Err(RepositoryError::backend(
                format!("acquire {label} lock '{}'", lock_path.display()),
                std::io::Error::from_raw_os_error(error as i32),
            )),
        }
    }

    fn acquire_alias_lock(&self, alias: &SnapshotAlias) -> RepositoryResult<PosixFileLockGuard> {
        self.acquire_file_lock(
            PosixFsSnapshotArtifactLayout::alias_lock_path(&self.root, alias),
            "alias",
            FlockArg::LockExclusiveNonblock,
        )
    }

    fn acquire_record_lock(&self, id: &SnapshotId) -> RepositoryResult<PosixFileLockGuard> {
        self.acquire_file_lock(
            PosixFsSnapshotArtifactLayout::record_lock_path(&self.root, id),
            "record",
            FlockArg::LockExclusiveNonblock,
        )
    }

    fn with_alias_lock<T>(
        &self,
        alias: &SnapshotAlias,
        action: impl FnOnce(&Self) -> RepositoryResult<T>,
    ) -> RepositoryResult<T> {
        let _guard = self.acquire_alias_lock(alias)?;
        action(self)
    }

    fn bind_alias_unlocked(
        &self,
        alias: &SnapshotAlias,
        snapshot_id: &SnapshotId,
    ) -> RepositoryResult<()> {
        self.ensure_alias_available(alias, snapshot_id)?;
        if self.load_alias_target(alias)?.as_ref() != Some(snapshot_id) {
            self.write_json(
                &PosixFsSnapshotArtifactLayout::alias_path(&self.root, alias),
                snapshot_id,
            )?;
        }
        Ok(())
    }

    fn ensure_alias_available(
        &self,
        alias: &SnapshotAlias,
        new_id: &SnapshotId,
    ) -> RepositoryResult<()> {
        let alias_path = PosixFsSnapshotArtifactLayout::alias_path(&self.root, alias);
        if let Some(existing) = self.load_alias_target(alias)? {
            if &existing == new_id {
                return Ok(());
            }
            if self.load_record_by_id_unlocked(&existing)?.is_some() {
                return Err(RepositoryError::AliasConflict {
                    alias: alias.to_string(),
                    existing,
                    new_id: new_id.clone(),
                });
            }
            self.remove_file_if_exists(&alias_path)?;
        }
        Ok(())
    }

    fn write_record_unlocked(&self, record: &SnapshotRecord) -> RepositoryResult<()> {
        self.write_json(&self.record_path(&record.id), record)
    }

    fn committed_record_unlocked(
        &self,
        metadata: &SnapshotPublishMetadata,
        committed: CommittedSnapshot,
        now_unix_ms: i64,
    ) -> RepositoryResult<SnapshotRecord> {
        if let Some(mut record) = self.load_record_by_id_unlocked(&metadata.id)? {
            if record.snapshot_type == metadata.snapshot_type {
                if let Some(existing) = record.committed.as_ref() {
                    if existing != &committed {
                        return Err(RepositoryError::InvalidRequest {
                            reason: format!(
                                "snapshot '{}' committed artifact metadata does not match the existing record",
                                metadata.id
                            ),
                        });
                    }
                }
            }
            record.mark_committed(metadata, committed, now_unix_ms);
            record
                .validate_committed_metadata()
                .map_err(|reason| RepositoryError::InvalidRequest { reason })?;
            return Ok(record);
        }
        Ok(SnapshotRecord::new_committed(
            metadata,
            committed,
            now_unix_ms,
        ))
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::super::layout::PosixFsSnapshotArtifactLayout;
    use super::PosixFsCatalogStore;
    use crate::snapshot::{
        CommittedSnapshot, SnapshotAlias, SnapshotId, SnapshotListFilter, SnapshotPublishMetadata,
        SnapshotPublishSource, SnapshotRecord, SnapshotSourceKind, TemplateBuildStatus,
    };

    #[test]
    fn begin_and_commit_make_snapshot_visible() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let snapshot_id = SnapshotId::generate();
        let session = store
            .begin_publish(&snapshot_id)
            .expect("begin should work");

        store
            .commit_publish(
                &session,
                SnapshotPublishMetadata {
                    id: snapshot_id.clone(),
                    source: SnapshotPublishSource::Template,
                    ..SnapshotPublishMetadata::mock()
                },
                CommittedSnapshot::mock(),
            )
            .expect("commit should work");

        assert!(store
            .get(&snapshot_id.to_string())
            .expect("get should work")
            .expect("snapshot should exist")
            .committed
            .is_some());
        assert!(
            PosixFsSnapshotArtifactLayout::new(tempdir.path(), &snapshot_id)
                .path(super::super::layout::POSIXFS_SNAPSHOT_COMMIT_MARKER)
                .exists()
        );
    }

    fn committed_metadata(
        id: SnapshotId,
        alias: &str,
        source: SnapshotPublishSource,
    ) -> SnapshotPublishMetadata {
        SnapshotPublishMetadata {
            id,
            alias: Some(SnapshotAlias::parse(alias).expect("alias should parse")),
            source,
            ..SnapshotPublishMetadata::mock()
        }
    }

    fn commit_record(store: &PosixFsCatalogStore, metadata: SnapshotPublishMetadata) -> SnapshotId {
        let snapshot_id = metadata.id.clone();
        let session = store
            .begin_publish(&snapshot_id)
            .expect("begin should work");
        store
            .commit_publish(&session, metadata, CommittedSnapshot::mock())
            .expect("commit should work");
        snapshot_id
    }

    fn listed_ids(store: &PosixFsCatalogStore, filter: SnapshotListFilter) -> Vec<SnapshotId> {
        store
            .list(filter)
            .expect("list should work")
            .into_iter()
            .map(|record| record.id)
            .collect()
    }

    #[test]
    fn list_applies_record_filters() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let template_alpha = commit_record(
            &store,
            committed_metadata(
                SnapshotId::generate(),
                "template-alpha",
                SnapshotPublishSource::Template,
            ),
        );
        let template_beta = commit_record(
            &store,
            committed_metadata(
                SnapshotId::generate(),
                "template-beta",
                SnapshotPublishSource::Template,
            ),
        );
        let sandbox_one = commit_record(
            &store,
            committed_metadata(
                SnapshotId::generate(),
                "sandbox-one",
                SnapshotPublishSource::Sandbox {
                    source_sandbox_id: "sandbox-1".to_string(),
                },
            ),
        );
        let sandbox_two = commit_record(
            &store,
            committed_metadata(
                SnapshotId::generate(),
                "sandbox-two",
                SnapshotPublishSource::Sandbox {
                    source_sandbox_id: "sandbox-2".to_string(),
                },
            ),
        );
        let errored_template = SnapshotId::generate();
        store
            .create(SnapshotRecord::template_waiting(
                errored_template.clone(),
                Some(SnapshotAlias::parse("template-error").expect("alias should parse")),
                Default::default(),
            ))
            .expect("create template should work");
        store
            .mark_error(
                &errored_template,
                crate::snapshot::TemplateBuildErrorReason::new("boom"),
            )
            .expect("mark error should work");

        let ids = listed_ids(
            &store,
            SnapshotListFilter::by_ids([template_alpha.clone(), sandbox_one.clone()]),
        );
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&template_alpha));
        assert!(ids.contains(&sandbox_one));

        let ids = listed_ids(
            &store,
            SnapshotListFilter {
                alias_prefix: Some("template-".to_string()),
                ..SnapshotListFilter::default()
            },
        );
        assert_eq!(ids.len(), 3);
        assert!(ids.contains(&template_alpha));
        assert!(ids.contains(&template_beta));
        assert!(ids.contains(&errored_template));

        let ids = listed_ids(&store, SnapshotListFilter::templates());
        assert_eq!(ids.len(), 3);
        assert!(ids.contains(&template_alpha));
        assert!(ids.contains(&template_beta));
        assert!(ids.contains(&errored_template));
        assert!(!ids.contains(&sandbox_one));

        let ids = listed_ids(
            &store,
            SnapshotListFilter::sandbox_snapshots(Some("sandbox-1".to_string()), None),
        );
        assert_eq!(ids, vec![sandbox_one.clone()]);

        let ids = listed_ids(
            &store,
            SnapshotListFilter::sandbox_snapshots(None, Some("team/sandbox-one:v1".to_string())),
        );
        assert_eq!(ids, vec![sandbox_one.clone()]);

        let ids = listed_ids(
            &store,
            SnapshotListFilter::sandbox_snapshots(None, Some(format!("{}:v1", sandbox_one))),
        );
        assert_eq!(ids, vec![sandbox_one.clone()]);

        let ids = listed_ids(
            &store,
            SnapshotListFilter::sandbox_snapshots(
                Some("sandbox-2".to_string()),
                Some("sandbox-one".to_string()),
            ),
        );
        assert!(ids.is_empty());

        let ids = listed_ids(
            &store,
            SnapshotListFilter {
                template_statuses: Some(vec![TemplateBuildStatus::Error]),
                ..SnapshotListFilter::templates()
            },
        );
        assert_eq!(ids, vec![errored_template]);

        let ids = listed_ids(
            &store,
            SnapshotListFilter {
                alias_prefix: Some("sandbox-".to_string()),
                sources: Some(vec![SnapshotSourceKind::Sandbox]),
                snapshot_ids: Some(vec![sandbox_two.clone(), template_alpha]),
                ..SnapshotListFilter::default()
            },
        );
        assert_eq!(ids, vec![sandbox_two]);
    }

    #[test]
    fn get_rejects_path_traversal_as_alias() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        // "../../etc/passwd" is not a valid alias (nor a UUID), so alias parsing
        // validation rejects it as InvalidRequest.
        let err = store
            .get("../../etc/passwd")
            .expect_err("path traversal should be rejected");
        assert!(
            matches!(err, crate::snapshot::RepositoryError::InvalidRequest { .. }),
            "expected InvalidRequest, got: {err:?}"
        );
    }

    #[test]
    fn get_returns_none_for_unknown_valid_uuid() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let unknown = SnapshotId::generate();
        let result = store
            .get(&unknown.to_string())
            .expect("valid UUID lookup should not error");
        assert!(result.is_none(), "non-existent snapshot should return None");
    }
}
