use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use nix::errno::Errno;
use nix::fcntl::{Flock, FlockArg};
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::layout::PosixFsSnapshotArtifactLayout;
use crate::snapshot::repository::SnapshotListFilter;
use crate::snapshot::{
    CommittedSnapshot, RepositoryError, RepositoryResult, SnapshotAlias, SnapshotId,
    SnapshotPublishMetadata, SnapshotPublishSource, SnapshotRecord, SnapshotSource,
    TemplateBuildErrorReason, TemplateBuildInfo, TemplateBuildStatus,
};
const FILE_LOCK_TIMEOUT: Option<Duration> = Some(Duration::from_secs(10));

pub struct PosixFsCatalogStore {
    root: PathBuf,
}

#[derive(Debug)]
pub(crate) struct PublishSession {
    pub(crate) snapshot_id: SnapshotId,
    /// Held for the complete import/commit window so a second repository
    /// instance cannot reconcile the staging directory while it is active.
    _record_lock: PosixFileLockGuard,
}

/// Kernel-owned advisory lock. Its stable path can outlive a process and be
/// reused safely after the file descriptor is closed or the process exits.
type PosixFileLockGuard = Flock<fs::File>;

impl PosixFsCatalogStore {
    /// Creates a catalog store rooted at the repository's durable POSIX directory.
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    /// Removes staging directories and committed records left behind by a
    /// crashed publisher. Pending template records are intentionally kept;
    /// only records that claim committed artifacts but lack the final marker
    /// are discarded.
    pub(crate) fn reconcile_startup(&self) -> RepositoryResult<()> {
        // A freshly configured repository has no catalog yet. Keep startup
        // reconciliation side-effect free in that case; the first real
        // repository operation will create the layout on demand.
        if !PosixFsSnapshotArtifactLayout::catalog_dir(&self.root).exists() {
            return Ok(());
        }
        self.ensure_layout()?;

        for entry in fs::read_dir(self.snapshots_dir())
            .map_err(|error| RepositoryError::backend("read snapshot staging directory", error))?
        {
            let entry = entry
                .map_err(|error| RepositoryError::backend("read snapshot staging entry", error))?;
            if !entry
                .file_type()
                .map_err(|error| RepositoryError::backend("inspect snapshot staging entry", error))?
                .is_dir()
            {
                continue;
            }
            let Some(id_text) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let Ok(id) = SnapshotId::parse(&id_text) else {
                self.remove_dir_if_exists(&entry.path())?;
                continue;
            };
            // A publisher may be importing large layers for a long time. Only
            // reconcile after acquiring the same record lock used by publish.
            let Some(_record_guard) = self.try_acquire_record_lock(&id)? else {
                continue;
            };
            if !self.is_committed(&id) {
                self.remove_dir_if_exists(&entry.path())?;
            }
        }

        for entry in fs::read_dir(self.records_dir()).map_err(|error| {
            RepositoryError::backend("read snapshot records during startup reconcile", error)
        })? {
            let entry = entry.map_err(|error| {
                RepositoryError::backend("read snapshot record during startup reconcile", error)
            })?;
            if entry.path().extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let Ok(observed_record) = self.read_json::<SnapshotRecord>(&entry.path()) else {
                continue;
            };
            if observed_record.committed.is_none()
                || self.commit_marker_path(&observed_record.id).exists()
            {
                continue;
            }
            let Some(_record_guard) = self.try_acquire_record_lock(&observed_record.id)? else {
                continue;
            };
            let Some(record) = self.load_record_by_id_unlocked(&observed_record.id)? else {
                continue;
            };
            if record.committed.is_some() && !self.commit_marker_path(&record.id).exists() {
                self.remove_file_if_exists(&entry.path())?;
                if let Some(alias) = record.alias.as_ref() {
                    self.with_alias_lock(alias, |store| {
                        let alias_path =
                            PosixFsSnapshotArtifactLayout::alias_path(&store.root, alias);
                        if store.load_alias_target(alias)?.as_ref() == Some(&record.id) {
                            store.remove_file_if_exists(&alias_path)?;
                        }
                        Ok(())
                    })?;
                }
                self.remove_dir_if_exists(&self.layout(&record.id).snapshot_dir())?;
            }
        }
        Ok(())
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
        self.ensure_layout()?;
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

    /// Commits one imported snapshot into the catalog and makes it visible via an atomic record write.
    ///
    /// Flow:
    /// 1. acquire the alias lock when an alias is present
    /// 2. write the completion marker after all artifacts are durable
    /// 3. atomically replace the record, making the committed state visible
    /// 4. bind the alias after the record is visible by id
    pub(crate) fn commit_publish(
        &self,
        session: &PublishSession,
        metadata: SnapshotPublishMetadata,
        committed: CommittedSnapshot,
    ) -> RepositoryResult<SnapshotRecord> {
        let now = now_unix_ms();
        let snapshot_id = metadata.id.clone();
        let previous_record = self.load_record_by_id_unlocked(&snapshot_id)?;
        let write_result = if let Some(alias) = metadata.alias.as_ref() {
            self.with_alias_lock(alias, |store| {
                let record = store.committed_record_unlocked(&metadata, committed.clone(), now)?;
                let alias_path = PosixFsSnapshotArtifactLayout::alias_path(&store.root, alias);
                if let Some(existing) = store.load_alias_target(alias)? {
                    if existing != snapshot_id {
                        if store.load_record_by_id_unlocked(&existing)?.is_some() {
                            return Err(RepositoryError::AliasConflict {
                                alias: alias.to_string(),
                                existing,
                                new_id: snapshot_id.clone(),
                            });
                        }
                        store.remove_file_if_exists(&alias_path)?;
                    }
                }
                store.write_commit_marker(&session.snapshot_id)?;
                store.write_record_unlocked(&record)?;
                store.write_json(&alias_path, &snapshot_id)?;
                Ok(record)
            })
        } else {
            (|| {
                let record = self.committed_record_unlocked(&metadata, committed.clone(), now)?;
                self.write_commit_marker(&session.snapshot_id)?;
                self.write_record_unlocked(&record)?;
                Ok(record)
            })()
        };

        match write_result {
            Ok(record) => Ok(record),
            Err(error) => {
                match previous_record.as_ref() {
                    Some(record) => {
                        let _ = self.write_record_unlocked(record);
                    }
                    None => {
                        let _ = self.remove_file_if_exists(&self.record_path(&snapshot_id));
                    }
                }
                if let Some(alias) = metadata.alias.as_ref() {
                    if previous_record
                        .as_ref()
                        .and_then(|record| record.alias.as_ref())
                        != Some(alias)
                    {
                        let _ = self.with_alias_lock(alias, |store| {
                            let alias_path =
                                PosixFsSnapshotArtifactLayout::alias_path(&store.root, alias);
                            if store.load_alias_target(alias)?.as_ref() == Some(&snapshot_id) {
                                store.remove_file_if_exists(&alias_path)?;
                            }
                            Ok(())
                        });
                    }
                }
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
        let _record_guard = self.acquire_record_lock(&record.id)?;
        if !matches!(record.source, SnapshotSource::Template { .. }) {
            return Err(RepositoryError::InvalidRequest {
                reason: "only template snapshots can be pre-created".to_string(),
            });
        }
        if record.committed.is_some() {
            return Err(RepositoryError::InvalidRequest {
                reason: "pre-created template snapshots must not already be committed".to_string(),
            });
        }
        if self.load_record_by_id_unlocked(&record.id)?.is_some() {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("snapshot '{}' already exists", record.id),
            });
        }

        if let Some(alias) = record.alias.as_ref() {
            self.with_alias_lock(alias, |store| {
                store.ensure_alias_available(alias, &record.id)?;
                store.write_record_unlocked(&record)?;
                store.write_json(
                    &PosixFsSnapshotArtifactLayout::alias_path(&store.root, alias),
                    &record.id,
                )
            })?;
        } else {
            self.write_record_unlocked(&record)?;
        }
        Ok(record)
    }

    pub(crate) fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        self.ensure_layout()?;
        if let Ok(direct_id) = SnapshotId::parse(id_or_alias) {
            if let Some(record) = self.load_visible_record_by_id_unlocked(&direct_id)? {
                return Ok(Some(record));
            }
        }

        let alias =
            SnapshotAlias::parse(id_or_alias).map_err(|error| RepositoryError::InvalidRequest {
                reason: error.to_string(),
            })?;
        self.with_alias_lock(&alias, |store| {
            let Some(id) = store.load_alias_target(&alias)? else {
                return Ok(None);
            };
            match store.load_visible_record_by_id_unlocked(&id)? {
                Some(record) => Ok(Some(record)),
                None => {
                    // A raw committed record without its marker may be an
                    // interrupted delete. Keep the alias so delete can be
                    // retried by the same user-facing reference.
                    if store.load_record_by_id_unlocked(&id)?.is_none() {
                        store.remove_file_if_exists(&PosixFsSnapshotArtifactLayout::alias_path(
                            &store.root,
                            &alias,
                        ))?;
                    }
                    Ok(None)
                }
            }
        })
    }

    pub(crate) fn get_for_delete(
        &self,
        id_or_alias: &str,
    ) -> RepositoryResult<Option<SnapshotRecord>> {
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
        self.with_alias_lock(&alias, |store| {
            let Some(id) = store.load_alias_target(&alias)? else {
                return Ok(None);
            };
            let record = store.load_record_by_id_unlocked(&id)?;
            if record.is_none() {
                store.remove_file_if_exists(&PosixFsSnapshotArtifactLayout::alias_path(
                    &store.root,
                    &alias,
                ))?;
            }
            Ok(record)
        })
    }

    pub(crate) fn list(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        self.ensure_layout()?;
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
            let record: SnapshotRecord = self.read_json(&entry.path())?;
            if record.committed.is_some() && !self.is_committed(&record.id) {
                continue;
            }
            if Self::matches_record_filter(&record, &filter) {
                records.push(record);
            }
        }
        records.sort_by(|left, right| {
            right
                .created_at_unix_ms
                .cmp(&left.created_at_unix_ms)
                .then_with(|| left.id.to_string().cmp(&right.id.to_string()))
        });
        Ok(records)
    }

    pub(crate) fn delete_record(&self, id: &SnapshotId) -> RepositoryResult<bool> {
        let _record_guard = self.acquire_record_lock(id)?;
        let Some(record) = self.load_record_by_id_unlocked(id)? else {
            // Idempotent: already doesn't exist
            return Ok(false);
        };
        if let Some(alias) = record.alias.as_ref() {
            self.with_alias_lock(alias, |store| {
                let snapshot_layout = PosixFsSnapshotArtifactLayout::new(&store.root, id);
                let alias_path = PosixFsSnapshotArtifactLayout::alias_path(&store.root, alias);
                store.remove_file_if_exists(
                    &snapshot_layout.path(super::layout::POSIXFS_SNAPSHOT_COMMIT_MARKER),
                )?;
                if record.committed.is_some() {
                    store.remove_dir_if_exists(&snapshot_layout.snapshot_dir())?;
                }
                store.remove_file_if_exists(&store.record_path(id))?;
                if store.load_alias_target(alias)?.as_ref() == Some(id) {
                    store.remove_file_if_exists(&alias_path)?;
                }
                Ok(())
            })?;
            return Ok(true);
        }
        let snapshot_layout = self.layout(id);
        self.remove_file_if_exists(&self.commit_marker_path(id))?;
        if record.committed.is_some() {
            self.remove_dir_if_exists(&snapshot_layout.snapshot_dir())?;
        }
        self.remove_file_if_exists(&self.record_path(id))?;
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
            if store.load_visible_record_by_id_unlocked(&id)?.is_some() {
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
        let now = now_unix_ms();
        let SnapshotSource::Template { build } = &mut record.source else {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("snapshot '{id}' is not a template build"),
            });
        };
        if build.status != TemplateBuildStatus::Waiting {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("template build '{id}' is not in waiting state"),
            });
        }
        build.status = TemplateBuildStatus::Building;
        build.started_at_unix_ms = Some(now);
        build.error_reason = None;
        record.updated_at_unix_ms = now;
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
        let now = now_unix_ms();
        let SnapshotSource::Template { build } = &mut record.source else {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("snapshot '{id}' is not a template build"),
            });
        };
        build.status = TemplateBuildStatus::Error;
        build.finished_at_unix_ms = Some(now);
        build.error_reason = Some(reason);
        record.updated_at_unix_ms = now;
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
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                RepositoryError::backend(format!("create '{}'", parent.display()), error)
            })?;
        }
        let bytes = serde_json::to_vec_pretty(value).map_err(|error| {
            RepositoryError::backend(format!("serialize json '{}'", path.display()), error)
        })?;
        let parent = path.parent().ok_or_else(|| RepositoryError::Backend {
            message: format!("resolve parent for '{}'", path.display()),
            source: None,
        })?;
        let mut temp = tempfile::NamedTempFile::new_in(parent).map_err(|error| {
            RepositoryError::backend(format!("create temp file in '{}'", parent.display()), error)
        })?;
        temp.write_all(&bytes).map_err(|error| {
            RepositoryError::backend(
                format!("write temp json '{}'", temp.path().display()),
                error,
            )
        })?;
        temp.as_file().sync_all().map_err(|error| {
            RepositoryError::backend(format!("sync temp json '{}'", temp.path().display()), error)
        })?;
        let tmp_path = temp.path().to_path_buf();
        temp.persist(path).map_err(|error| {
            RepositoryError::backend(
                format!(
                    "persist json '{}' -> '{}'",
                    tmp_path.display(),
                    path.display()
                ),
                error.error,
            )
        })?;
        Ok(())
    }

    fn write_commit_marker(&self, id: &SnapshotId) -> RepositoryResult<()> {
        let path = self.commit_marker_path(id);
        let parent = path.parent().ok_or_else(|| RepositoryError::Backend {
            message: format!("resolve parent for '{}'", path.display()),
            source: None,
        })?;
        fs::create_dir_all(parent).map_err(|error| {
            RepositoryError::backend(format!("create '{}'", parent.display()), error)
        })?;
        let mut temp = tempfile::NamedTempFile::new_in(parent).map_err(|error| {
            RepositoryError::backend(
                format!("create temp commit marker in '{}'", path.display()),
                error,
            )
        })?;
        temp.write_all(b"committed").map_err(|error| {
            RepositoryError::backend(
                format!("write commit marker '{}'", temp.path().display()),
                error,
            )
        })?;
        temp.persist(&path).map_err(|error| {
            RepositoryError::backend(
                format!("persist commit marker '{}'", path.display()),
                error.error,
            )
        })?;
        Ok(())
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
        self.commit_marker_path(id).exists()
            && self
                .load_record_by_id_unlocked(id)
                .ok()
                .flatten()
                .is_some_and(|record| record.committed.is_some())
    }

    fn cleanup_uncommitted_snapshot_dir(&self, id: &SnapshotId) -> RepositoryResult<()> {
        if self.is_committed(id) {
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

    fn load_visible_record_by_id_unlocked(
        &self,
        id: &SnapshotId,
    ) -> RepositoryResult<Option<SnapshotRecord>> {
        let Some(record) = self.load_record_by_id_unlocked(id)? else {
            return Ok(None);
        };
        if record.committed.is_none() || self.commit_marker_path(id).exists() {
            return Ok(Some(record));
        }
        Ok(None)
    }

    fn load_alias_target(&self, alias: &SnapshotAlias) -> RepositoryResult<Option<SnapshotId>> {
        let path = PosixFsSnapshotArtifactLayout::alias_path(&self.root, alias);
        if !path.exists() {
            return Ok(None);
        }
        self.read_json(&path).map(Some)
    }

    fn acquire_file_lock(
        &self,
        lock_path: PathBuf,
        label: &'static str,
    ) -> RepositoryResult<PosixFileLockGuard> {
        let deadline = FILE_LOCK_TIMEOUT.map(|timeout| Instant::now() + timeout);
        loop {
            if let Some(guard) = self.try_acquire_file_lock(&lock_path, label)? {
                return Ok(guard);
            }
            if deadline.is_some_and(|deadline| Instant::now() < deadline) {
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
        match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
            Ok(file) => Ok(Some(file)),
            Err((_file, Errno::EWOULDBLOCK)) => Ok(None),
            Err((_file, error)) => Err(RepositoryError::backend(
                format!("acquire {label} lock '{}'", lock_path.display()),
                std::io::Error::from_raw_os_error(error as i32),
            )),
        }
    }

    fn try_acquire_record_lock(
        &self,
        id: &SnapshotId,
    ) -> RepositoryResult<Option<PosixFileLockGuard>> {
        self.try_acquire_file_lock(
            &PosixFsSnapshotArtifactLayout::record_lock_path(&self.root, id),
            "record",
        )
    }

    fn acquire_alias_lock(&self, alias: &SnapshotAlias) -> RepositoryResult<PosixFileLockGuard> {
        self.acquire_file_lock(
            PosixFsSnapshotArtifactLayout::alias_lock_path(&self.root, alias),
            "alias",
        )
    }

    fn acquire_record_lock(&self, id: &SnapshotId) -> RepositoryResult<PosixFileLockGuard> {
        self.acquire_file_lock(
            PosixFsSnapshotArtifactLayout::record_lock_path(&self.root, id),
            "record",
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
        let id = metadata.id.clone();
        let alias = metadata.alias.clone();
        let resources = metadata.resources;
        let source = metadata.source.clone();
        if let Some(mut record) = self.load_record_by_id_unlocked(&id)? {
            record.mark_committed(
                metadata.snapshot_type,
                alias,
                resources,
                committed,
                source,
                now_unix_ms,
            );
            return Ok(record);
        }

        let source = match source {
            SnapshotPublishSource::Template => SnapshotSource::Template {
                build: TemplateBuildInfo {
                    status: TemplateBuildStatus::Ready,
                    started_at_unix_ms: None,
                    finished_at_unix_ms: Some(now_unix_ms),
                    error_reason: None,
                },
            },
            SnapshotPublishSource::Sandbox { source_sandbox_id } => {
                SnapshotSource::Sandbox { source_sandbox_id }
            }
        };

        Ok(SnapshotRecord {
            id,
            snapshot_type: metadata.snapshot_type,
            alias,
            source,
            resources,
            created_at_unix_ms: now_unix_ms,
            updated_at_unix_ms: now_unix_ms,
            committed: Some(committed),
        })
    }

    fn matches_record_filter(record: &SnapshotRecord, filter: &SnapshotListFilter) -> bool {
        if let Some(alias_prefix) = filter.alias_prefix.as_deref() {
            match record.alias.as_ref() {
                Some(alias) if alias.to_string().starts_with(alias_prefix) => {}
                _ => return false,
            }
        }

        if let Some(ids) = filter.snapshot_ids.as_ref() {
            if !ids.iter().any(|id| id == &record.id) {
                return false;
            }
        }

        if let Some(id_or_alias) = filter.snapshot_id_or_alias.as_deref() {
            if record.id.to_string() != id_or_alias
                && record
                    .alias
                    .as_ref()
                    .is_none_or(|alias| alias.as_ref() != id_or_alias)
            {
                return false;
            }
        }

        if let Some(source_sandbox_id) = filter.source_sandbox_id.as_deref() {
            match &record.source {
                SnapshotSource::Sandbox {
                    source_sandbox_id: record_source_sandbox_id,
                } if record_source_sandbox_id == source_sandbox_id => {}
                _ => return false,
            }
        }

        if let Some(sources) = filter.sources.as_ref() {
            if !sources.contains(&record.source.kind()) {
                return false;
            }
        }

        if let Some(statuses) = filter.template_statuses.as_ref() {
            let SnapshotSource::Template { build } = &record.source else {
                return false;
            };
            if !statuses.contains(&build.status) {
                return false;
            };
        }

        true
    }
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::super::layout::PosixFsSnapshotArtifactLayout;
    use super::PosixFsCatalogStore;
    use crate::snapshot::{
        CommittedSnapshot, RepositoryError, SnapshotAlias, SnapshotId, SnapshotListFilter,
        SnapshotPublishMetadata, SnapshotPublishSource, SnapshotRecord, SnapshotSource,
        SnapshotSourceKind, TemplateBuildStatus,
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

    #[test]
    fn failed_commit_restores_pending_template_identity() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let snapshot_id = SnapshotId::generate();
        let alias = SnapshotAlias::parse("pending-template").expect("alias should parse");
        store
            .create(SnapshotRecord::template_waiting(
                snapshot_id.clone(),
                Some(alias.clone()),
                Default::default(),
            ))
            .expect("pending template should be created");
        let session = store
            .begin_publish(&snapshot_id)
            .expect("publish should begin");
        let snapshot_dir = store.layout(&snapshot_id).snapshot_dir();
        std::fs::remove_dir_all(&snapshot_dir).expect("staging directory should be removable");
        std::fs::write(&snapshot_dir, b"block commit marker")
            .expect("marker parent should be replaced with a file");

        store
            .commit_publish(
                &session,
                committed_metadata(
                    snapshot_id.clone(),
                    alias.as_ref(),
                    SnapshotPublishSource::Template,
                ),
                CommittedSnapshot::mock(),
            )
            .expect_err("commit marker creation should fail");

        let restored = store
            .get(alias.as_ref())
            .expect("pending template lookup should work")
            .expect("failed publication must preserve the pending template and alias");
        assert!(restored.committed.is_none());
        assert!(matches!(
            restored.source,
            SnapshotSource::Template { ref build }
                if build.status == TemplateBuildStatus::Waiting
        ));
    }

    #[test]
    fn marker_before_record_preserves_pending_template_on_restart() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let snapshot_id = SnapshotId::generate();
        let alias = SnapshotAlias::parse("restart-safe-template").expect("alias should parse");
        store
            .create(SnapshotRecord::template_waiting(
                snapshot_id.clone(),
                Some(alias.clone()),
                Default::default(),
            ))
            .expect("pending template should be created");
        let session = store
            .begin_publish(&snapshot_id)
            .expect("begin should work");
        store
            .write_commit_marker(&snapshot_id)
            .expect("commit marker should write");

        let pending = store
            .get(alias.as_ref())
            .expect("pending template lookup should work")
            .expect("marker alone must not replace the pending template");
        assert!(pending.committed.is_none());

        store
            .reconcile_startup()
            .expect("active startup reconcile should work");
        assert!(
            PosixFsSnapshotArtifactLayout::new(tempdir.path(), &snapshot_id)
                .snapshot_dir()
                .exists()
        );

        // Simulate a publisher exiting after the marker but before the atomic
        // record replacement. Reconciliation removes only staged artifacts.
        drop(session);
        store
            .reconcile_startup()
            .expect("startup reconcile should work");
        assert!(
            !PosixFsSnapshotArtifactLayout::new(tempdir.path(), &snapshot_id)
                .snapshot_dir()
                .exists()
        );
        let pending = store
            .get(alias.as_ref())
            .expect("pending template lookup after reconcile should work")
            .expect("pending template identity and alias must survive restart");
        assert!(pending.committed.is_none());
    }

    #[test]
    fn hidden_record_keeps_alias_reserved_until_delete_finishes() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let alias = "delete-in-progress";
        let original_id = commit_record(
            &store,
            committed_metadata(
                SnapshotId::generate(),
                alias,
                SnapshotPublishSource::Sandbox {
                    source_sandbox_id: "sandbox".to_string(),
                },
            ),
        );
        std::fs::remove_file(store.commit_marker_path(&original_id))
            .expect("record should be hidden while delete is incomplete");

        let replacement_id = SnapshotId::generate();
        let session = store
            .begin_publish(&replacement_id)
            .expect("replacement publish should begin");
        let error = store
            .commit_publish(
                &session,
                committed_metadata(
                    replacement_id.clone(),
                    alias,
                    SnapshotPublishSource::Sandbox {
                        source_sandbox_id: "sandbox".to_string(),
                    },
                ),
                CommittedSnapshot::mock(),
            )
            .expect_err("an incomplete delete must retain its alias reservation");

        assert!(matches!(
            error,
            RepositoryError::AliasConflict {
                existing,
                new_id,
                ..
            } if existing == original_id && new_id == replacement_id
        ));
        assert_eq!(
            store
                .load_alias_target(&SnapshotAlias::parse(alias).expect("alias should remain valid"))
                .expect("alias lookup should work"),
            Some(original_id)
        );
    }

    #[test]
    fn active_record_lock_cannot_be_stolen_from_an_old_lock_file() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let snapshot_id = SnapshotId::generate();
        let session = store
            .begin_publish(&snapshot_id)
            .expect("publish should hold the record lock");
        let lock_path =
            PosixFsSnapshotArtifactLayout::record_lock_path(tempdir.path(), &snapshot_id);
        std::fs::File::options()
            .write(true)
            .open(&lock_path)
            .expect("record lock file should exist")
            .set_times(std::fs::FileTimes::new().set_modified(std::time::SystemTime::UNIX_EPOCH))
            .expect("record lock mtime should be adjustable");

        assert!(store
            .try_acquire_record_lock(&snapshot_id)
            .expect("competing lock attempt should be observable")
            .is_none());

        drop(session);
        assert!(store
            .try_acquire_record_lock(&snapshot_id)
            .expect("orphaned lock path should remain reusable")
            .is_some());
    }

    #[test]
    fn reconcile_does_not_remove_an_alias_rebound_by_a_live_writer() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let orphan_id = commit_record(
            &store,
            committed_metadata(
                SnapshotId::generate(),
                "reconcile-race",
                SnapshotPublishSource::Template,
            ),
        );
        std::fs::remove_file(store.commit_marker_path(&orphan_id))
            .expect("orphan should be hidden before reconciliation");
        let replacement_id = SnapshotId::generate();
        store
            .create(SnapshotRecord::template_waiting(
                replacement_id.clone(),
                None,
                Default::default(),
            ))
            .expect("replacement record should exist");
        let alias = SnapshotAlias::parse("reconcile-race").expect("alias should parse");
        let alias_guard = store
            .acquire_alias_lock(&alias)
            .expect("writer should own the alias lock");

        let root = tempdir.path().to_path_buf();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let reconcile = std::thread::spawn(move || {
            result_tx
                .send(PosixFsCatalogStore::new(root).reconcile_startup())
                .expect("test receiver should remain available");
        });
        let record_path = PosixFsSnapshotArtifactLayout::record_path(tempdir.path(), &orphan_id);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while record_path.exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(
            !record_path.exists(),
            "reconcile should reach alias cleanup"
        );
        assert!(matches!(
            result_rx.recv_timeout(std::time::Duration::from_millis(100)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));

        store
            .write_json(
                &PosixFsSnapshotArtifactLayout::alias_path(tempdir.path(), &alias),
                &replacement_id,
            )
            .expect("writer should rebind the alias while holding its lock");
        drop(alias_guard);
        result_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("reconcile should finish after alias unlock")
            .expect("reconcile should succeed");
        reconcile.join().expect("reconcile thread should join");

        assert_eq!(
            store
                .load_alias_target(&alias)
                .expect("alias lookup should work"),
            Some(replacement_id)
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
