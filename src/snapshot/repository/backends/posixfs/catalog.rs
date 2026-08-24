use std::collections::HashSet;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use nix::errno::Errno;
use nix::fcntl::{Flock, FlockArg};
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::layout::PosixFsSnapshotArtifactLayout;
use super::{persist_atomic_file, sync_dir};
use crate::snapshot::repository::SnapshotListFilter;
use crate::snapshot::types::{next_revision, now_unix_ms};
use crate::snapshot::{
    CommittedAttachedDrive, CommittedSnapshot, OverlaybdLayerRef, RepositoryError,
    RepositoryResult, SnapshotAlias, SnapshotId, SnapshotLifecycle, SnapshotPublishMetadata,
    SnapshotRecord, SnapshotType, TemplateBuildErrorReason,
};
const FILE_LOCK_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct PosixFsCatalogStore {
    root: PathBuf,
}

#[derive(Debug)]
pub(crate) struct PublishSession {
    pub(crate) snapshot_id: SnapshotId,
    /// Held for the complete import/commit window so a second repository
    /// instance cannot reconcile the staging directory while it is active.
    _record_lock: PosixFileLockGuard,
    /// Prevents managed-layer GC from deleting artifacts while this publish
    /// session imports them and commits their catalog record.
    _repository_lock: PosixFileLockGuard,
}

/// Kernel-owned advisory lock. Its stable path can outlive a process and be
/// reused safely after the file descriptor is closed or the process exits.
pub(super) type PosixFileLockGuard = Flock<fs::File>;

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
        // Reconciliation removes snapshot directories and catalog records, so
        // it must obey the same repository -> record lock order as delete and
        // managed-layer GC. If another process is publishing or still holds a
        // runnable artifact lease, leave the repository untouched; a later
        // startup can retry after that shared lease is released.
        let Some(_repository_guard) = self.try_acquire_file_lock(
            &PosixFsSnapshotArtifactLayout::repository_lock_path(&self.root),
            "repository",
            FlockArg::LockExclusiveNonblock,
        )?
        else {
            return Ok(());
        };

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
            if !observed_record.is_ready()
                || observed_record.committed.is_none()
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
            if record.is_ready()
                && record.committed.is_some()
                && !self.commit_marker_path(&record.id).exists()
            {
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
        let repository_lock = self.acquire_repository_shared_lock()?;
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
            _repository_lock: repository_lock,
        })
    }

    /// Commits one imported snapshot into the catalog and makes it visible via an atomic record write.
    ///
    /// New identities are first persisted as Preparing, then receive their
    /// marker and alias, and become Ready last. Existing template completions
    /// and Local-to-Distributed promotions keep their prior Ready record
    /// visible while the replacement is committed.
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
        let previous_record = self.load_record_by_id_unlocked(&snapshot_id)?;
        let record = self.committed_record_unlocked(&metadata, committed, now)?;
        let is_new = previous_record.is_none();
        let mut preparing = record.clone();
        preparing.lifecycle = SnapshotLifecycle::Preparing;
        let write_result = if let Some(alias) = metadata.alias.as_ref() {
            self.with_alias_lock(alias, |store| {
                let alias_path = PosixFsSnapshotArtifactLayout::alias_path(&store.root, alias);
                // Reject deterministic conflicts before exposing a new hidden
                // identity. Later I/O failures retain Preparing state.
                store.ensure_alias_available(alias, &snapshot_id)?;
                if is_new {
                    store.write_record_unlocked(&preparing)?;
                }
                store.write_commit_marker(&session.snapshot_id)?;
                store.write_json(&alias_path, &snapshot_id)?;
                store.write_record_unlocked(&record)
            })
        } else {
            (|| {
                if is_new {
                    self.write_record_unlocked(&preparing)?;
                }
                self.write_commit_marker(&session.snapshot_id)?;
                self.write_record_unlocked(&record)
            })()
        };

        match write_result {
            Ok(()) => Ok(record),
            Err(error) => {
                // The record/marker writers persist via rename and sync the
                // containing directory afterward. If that sync reports an
                // error, the rename may already be durable; writing the old
                // record back here could therefore undo a successful
                // promotion. Preserve the current catalog state and let an
                // exact retry or startup reconciliation converge it. Keep an
                // existing Preparing identity even when alias reservation
                // fails: its closure may already be durable, and deleting it
                // would make a later retry destructive.
                // Only remove the physical staging directory when it is not
                // backed by a committed marker/record. Never roll back catalog
                // metadata after a write has been attempted.
                let _ = self.cleanup_uncommitted_snapshot_dir(&session.snapshot_id);
                Err(error)
            }
        }
    }

    /// Cleans up an unfinished publish session that never reached a visible committed record.
    pub(crate) fn abort_publish(&self, session: &PublishSession) -> RepositoryResult<()> {
        self.cleanup_uncommitted_snapshot_dir(&session.snapshot_id)
    }

    pub(crate) fn create(&self, mut record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        self.ensure_layout()?;
        record
            .validate_template_create()
            .map_err(|reason| RepositoryError::InvalidRequest { reason })?;

        let _record_guard = self.acquire_record_lock(&record.id)?;
        let preparing = if let Some(existing) = self.load_record_by_id_unlocked(&record.id)? {
            if existing.lifecycle == SnapshotLifecycle::Deleting {
                return Err(RepositoryError::ConcurrentModification {
                    resource: format!("snapshot '{}' is being deleted", record.id),
                });
            }
            if !existing.same_catalog_contents(&record) {
                return Err(RepositoryError::InvalidRequest {
                    reason: format!(
                        "snapshot '{}' already exists with different metadata",
                        record.id
                    ),
                });
            }
            if existing.is_ready() {
                // A Ready pending-template record is already the canonical
                // identity. Repair a missing alias binding before returning it
                // so callers never observe a Ready-but-unbound record.
                if let Some(alias) = record.alias.as_ref() {
                    self.with_alias_lock(alias, |store| {
                        store.bind_alias_unlocked(alias, &record.id)
                    })?;
                }
                return Ok(existing);
            }
            // Deleting was fenced above; the remaining non-Ready state is
            // Preparing and can be completed by this exact retry.
            existing
        } else {
            let mut preparing = record.clone();
            preparing.lifecycle = SnapshotLifecycle::Preparing;
            self.write_record_unlocked(&preparing)?;
            preparing
        };

        let mut ready = record.clone();
        ready.revision = next_revision(preparing.revision);

        let write_result = if let Some(alias) = record.alias.as_ref() {
            self.with_alias_lock(alias, |store| {
                // Bind the alias before publishing Ready. If either this
                // check or the bind fails, the hidden Preparing identity is
                // intentionally retained for an exact retry.
                store.bind_alias_unlocked(alias, &record.id)?;
                store.write_record_unlocked(&ready)
            })
        } else {
            self.write_record_unlocked(&ready)
        };
        if let Err(error) = write_result {
            // A deterministic conflict for a brand-new identity happens
            // before alias or Ready writes. Its Preparing record is the
            // useful retry anchor, so keep it. Other I/O failures are
            // ambiguous: re-read before returning in case the atomic
            // record rename succeeded but its durability sync failed.
            if !matches!(error, RepositoryError::AliasConflict { .. }) {
                if let Some(current) = self.load_record_by_id_unlocked(&record.id)? {
                    if current.is_ready() && current.same_catalog_contents(&record) {
                        return Ok(current);
                    }
                }
            }
            return Err(error);
        }
        Ok(ready)
    }

    /// Persists canonical logical metadata without importing snapshot bytes.
    ///
    /// Local artifact publication has already completed before this method is
    /// called. A new identity is first persisted as Preparing, then receives
    /// its marker and alias, and only then becomes Ready. Any I/O failure after
    /// the Preparing write leaves a hidden record that a later exact retry can
    /// complete.
    pub(crate) fn commit_record(
        &self,
        mut record: SnapshotRecord,
    ) -> RepositoryResult<SnapshotRecord> {
        if record.snapshot_type != SnapshotType::Local {
            return Err(RepositoryError::InvalidRequest {
                reason: "metadata-only commits are supported only for Local snapshots".to_string(),
            });
        }
        if !record.is_ready() {
            return Err(RepositoryError::InvalidRequest {
                reason: "metadata-only commits require a complete Ready record".to_string(),
            });
        }
        record
            .validate_committed_metadata()
            .map_err(|reason| RepositoryError::InvalidRequest { reason })?;
        let _repository_guard = self.acquire_repository_shared_lock()?;
        let _record_guard = self.acquire_record_lock(&record.id)?;
        let previous_record = self.load_record_by_id_unlocked(&record.id)?;
        if let Some(previous) = previous_record.as_ref() {
            if previous.lifecycle == SnapshotLifecycle::Deleting {
                return Err(RepositoryError::ConcurrentModification {
                    resource: format!("snapshot '{}' is being deleted", record.id),
                });
            }
            if !previous.same_catalog_contents(&record) {
                return Err(RepositoryError::InvalidRequest {
                    reason: format!(
                        "snapshot '{}' canonical metadata does not match the existing record",
                        record.id
                    ),
                });
            }
            match (previous.lifecycle, record.lifecycle) {
                (SnapshotLifecycle::Preparing, SnapshotLifecycle::Ready) => {}
                (left, right) if left == right => {}
                _ => {
                    return Err(RepositoryError::InvalidRequest {
                        reason: format!(
                            "snapshot '{}' lifecycle cannot transition from {:?} to {:?}",
                            record.id, previous.lifecycle, record.lifecycle
                        ),
                    });
                }
            }
        }

        let snapshot_id = record.id.clone();
        let is_new = previous_record.is_none();
        if is_new {
            // A record decoded from a pre-revision legacy payload may carry
            // zero. New identities cannot choose their catalog revision; the
            // hidden Preparing write starts at one and the Ready transition
            // advances it, matching the OSS CAS lifecycle.
            record.revision = 1;
        }
        let repairs_ready = previous_record
            .as_ref()
            .is_some_and(SnapshotRecord::is_ready);
        let mut preparing = record.clone();
        preparing.lifecycle = SnapshotLifecycle::Preparing;
        let mut ready = record.clone();
        ready.revision = if repairs_ready {
            record.revision
        } else {
            next_revision(
                previous_record
                    .as_ref()
                    .map_or(preparing.revision, |previous| previous.revision),
            )
        };
        let write_result = if let Some(alias) = record.alias.as_ref() {
            self.with_alias_lock(alias, |store| {
                // Check deterministic conflicts before creating a new hidden
                // identity. I/O failures after this point intentionally retain
                // Preparing state for recovery.
                store.ensure_alias_available(alias, &snapshot_id)?;
                if is_new {
                    store.write_record_unlocked(&preparing)?;
                }
                let alias_path = PosixFsSnapshotArtifactLayout::alias_path(&store.root, alias);
                if repairs_ready {
                    // A missing marker hides the exact Ready record. Restore
                    // its alias first so making the ID visible cannot expose
                    // an unbound canonical identity.
                    store.write_json(&alias_path, &snapshot_id)?;
                    store.write_commit_marker(&snapshot_id)?;
                } else {
                    store.write_commit_marker(&snapshot_id)?;
                    store.write_json(&alias_path, &snapshot_id)?;
                    store.write_record_unlocked(&ready)?;
                }
                Ok(())
            })
        } else {
            (|| {
                if is_new {
                    self.write_record_unlocked(&preparing)?;
                }
                self.write_commit_marker(&snapshot_id)?;
                if !repairs_ready {
                    self.write_record_unlocked(&ready)?;
                }
                Ok(())
            })()
        };

        // Preserve an existing Preparing record on every failure. The
        // immutable closure may already be durable and a later exact retry
        // must be able to finish the same identity.
        write_result?;

        // A Ready record is never overwritten by this path: it is only
        // repaired when its marker or alias binding is missing.  Return the
        // persisted canonical value rather than the caller's retry payload;
        // revision and update time are deliberately ignored for identity
        // comparison, so the two values may otherwise disagree with storage.
        if repairs_ready {
            Ok(previous_record.expect("repairs_ready implies an existing record"))
        } else {
            Ok(ready)
        }
    }

    pub(crate) fn get_record(&self, id: &SnapshotId) -> RepositoryResult<Option<SnapshotRecord>> {
        self.ensure_layout()?;
        self.load_record_by_id_unlocked(id)
    }

    /// Loads an exact record only when its committed closure is visible.
    ///
    /// The raw exact read remains intentionally separate because startup
    /// reconciliation must be able to discover hidden lifecycle records.
    pub(crate) fn get_committed_record(
        &self,
        id: &SnapshotId,
    ) -> RepositoryResult<Option<SnapshotRecord>> {
        Ok(self
            .get_recovery_record(id)?
            .filter(SnapshotRecord::is_ready))
    }

    /// Loads an exact record with a durable committed closure for recovery,
    /// without requiring the catalog lifecycle to have reached Ready.
    pub(crate) fn get_recovery_record(
        &self,
        id: &SnapshotId,
    ) -> RepositoryResult<Option<SnapshotRecord>> {
        self.ensure_layout()?;
        let Some(record) = self.load_record_by_id_unlocked(id)? else {
            return Ok(None);
        };
        if record.committed.is_some()
            && matches!(
                record.lifecycle,
                SnapshotLifecycle::Preparing | SnapshotLifecycle::Ready
            )
            && self.commit_marker_path(id).exists()
        {
            Ok(Some(record))
        } else {
            Ok(None)
        }
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

        // A terminal delete marker is an identity fence. Check it before the
        // pending-template branch below (which deliberately accepts
        // `committed = None` Preparing records), otherwise a stale publish
        // could resurrect a deleted SnapshotId.
        if existing.lifecycle == SnapshotLifecycle::Deleting {
            return Err(RepositoryError::ConcurrentModification {
                resource: format!("snapshot '{}' is being deleted", metadata.id),
            });
        }

        if existing.committed.is_none() {
            if !existing.matches_pending_template(metadata) {
                return Err(RepositoryError::InvalidRequest {
                    reason: format!(
                        "snapshot '{}' publish metadata does not match the pending template identity",
                        metadata.id
                    ),
                });
            }
            return Ok(None);
        }
        if !existing.matches_publish_metadata(metadata) {
            return Err(RepositoryError::InvalidRequest {
                reason: format!(
                    "snapshot '{}' publish metadata does not match the existing identity",
                    metadata.id
                ),
            });
        }
        if !existing.is_ready() {
            if existing.lifecycle == SnapshotLifecycle::Preparing
                && existing.snapshot_type == metadata.snapshot_type
                && existing.owner_node_id == metadata.owner_node_id
            {
                return Ok(None);
            }
            return Err(RepositoryError::InvalidRequest {
                reason: format!(
                    "snapshot '{}' must be Ready before artifact publication",
                    metadata.id
                ),
            });
        }

        match (existing.snapshot_type, metadata.snapshot_type) {
            (left, right) if left == right => {
                if existing.owner_node_id != metadata.owner_node_id {
                    return Err(RepositoryError::InvalidRequest {
                        reason: format!(
                            "snapshot '{}' owner_node_id does not match the existing record",
                            metadata.id
                        ),
                    });
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
            (SnapshotType::Local, SnapshotType::Distributed) => Ok(None),
            (from, to) => Err(RepositoryError::InvalidRequest {
                reason: format!(
                    "snapshot '{}' cannot transition from {:?} to {:?}",
                    metadata.id, from, to
                ),
            }),
        }
    }

    pub(crate) fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        self.load_by_id_or_alias(id_or_alias, Self::load_visible_record_by_id_unlocked)
    }

    pub(crate) fn get_for_delete(
        &self,
        id_or_alias: &str,
    ) -> RepositoryResult<Option<SnapshotRecord>> {
        self.load_by_id_or_alias(id_or_alias, Self::load_record_by_id_unlocked)
    }

    fn load_by_id_or_alias(
        &self,
        id_or_alias: &str,
        read: impl Fn(&Self, &SnapshotId) -> RepositoryResult<Option<SnapshotRecord>>,
    ) -> RepositoryResult<Option<SnapshotRecord>> {
        self.ensure_layout()?;
        if let Ok(direct_id) = SnapshotId::parse(id_or_alias) {
            if let Some(record) = read(self, &direct_id)? {
                return Ok(Some(record));
            }
        }

        let alias =
            SnapshotAlias::parse(id_or_alias).map_err(|error| RepositoryError::InvalidRequest {
                reason: error.to_string(),
            })?;
        self.load_alias_record(&alias, |store, id| read(store, id))
    }

    pub(crate) fn list(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        self.ensure_layout()?;
        let mut records = self
            .load_all_records_unlocked()?
            .into_iter()
            .filter(|record| {
                record.is_ready()
                    && (record.committed.is_none() || self.is_committed(&record.id))
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

    /// Lists every durable local record, including hidden lifecycle states.
    /// Public list/get intentionally hide Preparing and Deleting identities,
    /// but node-local reconciliation must discover them after a crash so an
    /// interrupted metadata commit or purge can converge.
    pub(crate) fn list_recovery_candidates(&self) -> RepositoryResult<Vec<SnapshotRecord>> {
        self.ensure_layout()?;
        let mut records = self.load_all_records_unlocked()?;
        records.sort_by(|left, right| {
            right
                .updated_at_unix_ms
                .cmp(&left.updated_at_unix_ms)
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

    /// Removes canonical content-addressed managed layers that no durable
    /// record with a physical closure references. Hidden Preparing records are
    /// included because an exact retry may still complete them. Any unreadable
    /// record aborts the sweep rather than risking deletion of live data.
    pub(super) fn gc_unreferenced_managed_layers(&self) -> RepositoryResult<usize> {
        let _repository_guard = self.acquire_repository_exclusive_lock()?;
        let managed_layers_dir = PosixFsSnapshotArtifactLayout::managed_layers_dir(&self.root);
        if !managed_layers_dir.exists() {
            return Ok(0);
        }

        let mut referenced = HashSet::new();
        for record in self.load_all_records_unlocked()? {
            let Some(committed) = record.committed else {
                continue;
            };
            for layer in &committed.rootfs_layers {
                if let OverlaybdLayerRef::Managed(layer) = layer {
                    referenced.insert(PosixFsSnapshotArtifactLayout::managed_layer_path(
                        &self.root,
                        &layer.digest,
                    ));
                }
            }
            for layer in &committed.memory_layers {
                referenced.insert(PosixFsSnapshotArtifactLayout::managed_layer_path(
                    &self.root,
                    &layer.digest,
                ));
            }
            for drive in &committed.attached_drives {
                let CommittedAttachedDrive::Overlaybd { layers, .. } = drive;
                for layer in layers {
                    if let OverlaybdLayerRef::Managed(layer) = layer {
                        referenced.insert(PosixFsSnapshotArtifactLayout::managed_layer_path(
                            &self.root,
                            &layer.digest,
                        ));
                    }
                }
            }
        }

        let mut candidates = Vec::new();
        for entry in fs::read_dir(&managed_layers_dir).map_err(|error| {
            RepositoryError::backend(
                format!(
                    "read managed layer directory '{}'",
                    managed_layers_dir.display()
                ),
                error,
            )
        })? {
            let entry = entry.map_err(|error| {
                RepositoryError::backend(
                    format!(
                        "read managed layer entry in '{}'",
                        managed_layers_dir.display()
                    ),
                    error,
                )
            })?;
            let file_type = entry.file_type().map_err(|error| {
                RepositoryError::backend(
                    format!("inspect managed layer entry '{}'", entry.path().display()),
                    error,
                )
            })?;
            if file_type.is_file()
                && is_canonical_managed_layer_file_name(&entry.file_name())
                && !referenced.contains(&entry.path())
            {
                candidates.push(entry.path());
            }
        }

        let mut removed = 0;
        for path in candidates {
            match fs::remove_file(&path) {
                Ok(()) => removed += 1,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(RepositoryError::backend(
                        format!("remove orphan managed layer '{}'", path.display()),
                        error,
                    ));
                }
            }
        }
        if removed > 0 {
            sync_dir(&managed_layers_dir)?;
        }
        Ok(removed)
    }

    pub(crate) fn delete_record(&self, id: &SnapshotId) -> RepositoryResult<bool> {
        self.delete_record_with_policy(id, true)
    }

    /// Removes a node-local recovery closure after its canonical identity is
    /// no longer served from this store. The same fence and runtime lease
    /// ordering as canonical deletion are retained, but the local record is
    /// physically removed instead of becoming a reusable identity tombstone.
    pub(crate) fn purge_record(&self, id: &SnapshotId) -> RepositoryResult<bool> {
        self.delete_record_with_policy(id, false)
    }

    fn delete_record_with_policy(
        &self,
        id: &SnapshotId,
        retain_tombstone: bool,
    ) -> RepositoryResult<bool> {
        // Fence the identity before waiting for a runtime artifact lease. A
        // runnable snapshot holds the repository shared lock for its whole
        // lifetime, so taking the exclusive lock first would leave the
        // public Ready record visible while deletion is blocked. The
        // record-level lock is sufficient for this short state transition:
        // every publisher re-reads the record while holding the same lock and
        // rejects Deleting identities.
        {
            let _record_guard = self.acquire_record_lock(id)?;
            let Some(record) = self.load_record_by_id_unlocked(id)? else {
                // Idempotent: already doesn't exist.
                return Ok(false);
            };
            if record.lifecycle != SnapshotLifecycle::Deleting {
                let mut deleting = record;
                deleting.lifecycle = SnapshotLifecycle::Deleting;
                deleting.updated_at_unix_ms = now_unix_ms();
                deleting.revision = next_revision(deleting.revision);
                self.write_record_unlocked(&deleting)?;
            }
        }

        // Artifact cleanup must wait for all runnable leases, but the
        // identity is already hidden and fenced above. Re-acquire the locks
        // in the repository -> record order used by publish and reconciliation
        // to avoid lock inversion with another publisher.
        let _repository_guard = self.acquire_repository_exclusive_lock()?;
        let _record_guard = self.acquire_record_lock(id)?;
        let Some(record) = self.load_record_by_id_unlocked(id)? else {
            // Another deleter may have completed after our fence.
            return Ok(true);
        };
        if record.lifecycle != SnapshotLifecycle::Deleting {
            return Err(RepositoryError::ConcurrentModification {
                resource: format!("snapshot '{}' delete fence was replaced", id),
            });
        }

        let alias = record.alias.clone();
        let finish = |store: &Self| -> RepositoryResult<bool> {
            let snapshot_layout = PosixFsSnapshotArtifactLayout::new(&store.root, id);
            store.remove_file_if_exists(
                &snapshot_layout.path(super::layout::POSIXFS_SNAPSHOT_COMMIT_MARKER),
            )?;
            // Once the identity is fenced, no staged closure may be reused;
            // remove both committed and any empty/interrupted publish
            // directory on every retry.
            store.remove_dir_if_exists(&snapshot_layout.snapshot_dir())?;
            if retain_tombstone && record.committed.is_some() {
                // Keep a compact terminal identity tombstone after the
                // physical closure is gone. This prevents a late publisher
                // from resurrecting the same SnapshotId, while public reads
                // remain hidden by the Deleting lifecycle.
                let mut tombstone = record.clone();
                tombstone.committed = None;
                tombstone.updated_at_unix_ms = now_unix_ms();
                tombstone.revision = next_revision(tombstone.revision);
                store.write_record_unlocked(&tombstone)?;
            } else if !retain_tombstone {
                store.remove_file_if_exists(&store.record_path(id))?;
            }
            Ok(true)
        };

        if let Some(alias) = alias.as_ref() {
            self.with_alias_lock(alias, |store| {
                let deleted = finish(store)?;
                // Do not remove a newer binding that may have been installed
                // after an interrupted delete. The alias is released only
                // when it still targets this identity.
                if store.load_alias_target(alias)?.as_ref() == Some(id) {
                    store.remove_file_if_exists(&PosixFsSnapshotArtifactLayout::alias_path(
                        &store.root,
                        alias,
                    ))?;
                }
                Ok(deleted)
            })
        } else {
            finish(self)
        }
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
        self.commit_marker_path(id).exists()
            && self
                .load_record_by_id_unlocked(id)
                .ok()
                .flatten()
                .is_some_and(|record| record.committed.is_some())
    }

    fn cleanup_uncommitted_snapshot_dir(&self, id: &SnapshotId) -> RepositoryResult<()> {
        // A committed record without its visibility marker is still an
        // authoritative recovery candidate. In particular, a changed retry
        // must not make an import error destructive by deleting the closure
        // that belonged to the earlier record.
        if self
            .load_record_by_id_unlocked(id)?
            .is_some_and(|record| record.committed.is_some())
        {
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
        if !record.is_ready() {
            return Ok(None);
        }
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

    fn try_acquire_record_lock(
        &self,
        id: &SnapshotId,
    ) -> RepositoryResult<Option<PosixFileLockGuard>> {
        self.try_acquire_file_lock(
            &PosixFsSnapshotArtifactLayout::record_lock_path(&self.root, id),
            "record",
            FlockArg::LockExclusiveNonblock,
        )
    }

    pub(super) fn acquire_repository_shared_lock(&self) -> RepositoryResult<PosixFileLockGuard> {
        self.ensure_layout()?;
        self.acquire_repository_lock(FlockArg::LockSharedNonblock)
    }

    fn acquire_repository_exclusive_lock(&self) -> RepositoryResult<PosixFileLockGuard> {
        self.ensure_layout()?;
        self.acquire_repository_lock(FlockArg::LockExclusiveNonblock)
    }

    fn acquire_repository_lock(&self, lock_arg: FlockArg) -> RepositoryResult<PosixFileLockGuard> {
        self.acquire_file_lock(
            PosixFsSnapshotArtifactLayout::repository_lock_path(&self.root),
            "repository",
            lock_arg,
        )
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
            let existing_record = self.load_record_by_id_unlocked(&existing)?;
            if &existing == new_id {
                if existing_record
                    .as_ref()
                    .is_some_and(SnapshotRecord::is_terminal_tombstone)
                {
                    return Err(RepositoryError::ConcurrentModification {
                        resource: format!("snapshot '{}' is an identity tombstone", new_id),
                    });
                }
                return Ok(());
            }
            if existing_record
                .as_ref()
                .is_some_and(|record| !record.is_terminal_tombstone())
            {
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

fn is_canonical_managed_layer_file_name(name: &OsStr) -> bool {
    let Some(name) = name.to_str() else {
        return false;
    };
    let Some(hex) = name
        .strip_prefix("sha256_")
        .and_then(|name| name.strip_suffix(".overlaybd.commit"))
    else {
        return false;
    };
    hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::ffi::OsString;
    use std::fs;
    use std::os::unix::ffi::OsStringExt;
    use std::os::unix::fs::symlink;
    use std::sync::mpsc::{self, RecvTimeoutError};
    use std::time::Duration;

    use tempfile::TempDir;

    use super::super::layout::PosixFsSnapshotArtifactLayout;
    use super::PosixFsCatalogStore;
    use crate::snapshot::{
        CommittedAttachedDrive, CommittedSnapshot, ManagedLayer, OverlaybdLayerRef,
        RepositoryError, SnapshotAlias, SnapshotId, SnapshotLifecycle, SnapshotListFilter,
        SnapshotPublishMetadata, SnapshotPublishSource, SnapshotRecord, SnapshotSource,
        SnapshotSourceKind, SnapshotType, TemplateBuildStatus,
    };

    fn local_record(
        id: SnapshotId,
        alias: SnapshotAlias,
        lifecycle: SnapshotLifecycle,
    ) -> SnapshotRecord {
        let mut record = SnapshotRecord::mock_ready(CommittedSnapshot::mock());
        record.id = id;
        record.snapshot_type = SnapshotType::Local;
        record.owner_node_id = Some("node-a".to_string());
        record.lifecycle = lifecycle;
        record.alias = Some(alias);
        record.source = SnapshotSource::Sandbox {
            source_sandbox_id: "sandbox-a".to_string(),
        };
        record
    }

    fn persist_preparing_record(store: &PosixFsCatalogStore, record: &SnapshotRecord) {
        assert_eq!(record.lifecycle, SnapshotLifecycle::Preparing);
        store.ensure_layout().expect("catalog layout should exist");
        store
            .write_record_unlocked(record)
            .expect("Preparing record should persist");
    }

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
    fn persisted_preparing_record_is_exactly_readable_but_publicly_hidden() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let snapshot_id = SnapshotId::generate();
        let alias = SnapshotAlias::parse("canonical-local").expect("alias should parse");
        let preparing = local_record(
            snapshot_id.clone(),
            alias.clone(),
            SnapshotLifecycle::Preparing,
        );

        persist_preparing_record(&store, &preparing);
        store
            .write_commit_marker(&snapshot_id)
            .expect("commit marker should persist");
        store
            .write_json(
                &PosixFsSnapshotArtifactLayout::alias_path(tempdir.path(), &alias),
                &snapshot_id,
            )
            .expect("alias binding should persist");

        assert_eq!(
            store
                .get_record(&snapshot_id)
                .expect("exact read should work")
                .expect("exact read should include Preparing")
                .lifecycle,
            SnapshotLifecycle::Preparing
        );
        assert!(store
            .get(&snapshot_id.to_string())
            .expect("public id read should work")
            .is_none());
        assert!(store
            .get(alias.as_ref())
            .expect("public alias read should work")
            .is_none());
        assert!(store
            .resolve_alias(alias.as_ref())
            .expect("public alias resolution should work")
            .is_none());
        assert!(store
            .list(SnapshotListFilter::matches_all())
            .expect("public list should work")
            .is_empty());
        assert!(store.commit_marker_path(&snapshot_id).exists());

        let mut ready = preparing;
        ready.lifecycle = SnapshotLifecycle::Ready;
        store
            .commit_record(ready)
            .expect("equivalent Ready transition should commit");

        assert!(store
            .get(&snapshot_id.to_string())
            .expect("public id read should work")
            .is_some());
        assert_eq!(
            store
                .resolve_alias(alias.as_ref())
                .expect("public alias resolution should work"),
            Some(snapshot_id.clone())
        );
        assert_eq!(
            store
                .list(SnapshotListFilter::matches_all())
                .expect("public list should work")
                .len(),
            1
        );
        assert!(store.commit_marker_path(&snapshot_id).exists());
    }

    #[test]
    fn recovery_candidates_include_hidden_lifecycle_records() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let preparing_id = SnapshotId::generate();
        let deleting_id = SnapshotId::generate();

        let preparing = local_record(
            preparing_id.clone(),
            SnapshotAlias::parse("hidden-preparing").expect("alias should parse"),
            SnapshotLifecycle::Preparing,
        );
        persist_preparing_record(&store, &preparing);

        let mut deleting = local_record(
            deleting_id.clone(),
            SnapshotAlias::parse("hidden-deleting").expect("alias should parse"),
            SnapshotLifecycle::Deleting,
        );
        deleting.committed = None;
        store.ensure_layout().expect("catalog layout should exist");
        store
            .write_record_unlocked(&deleting)
            .expect("deleting record should persist");

        assert!(store
            .list(SnapshotListFilter::matches_all())
            .expect("public list should work")
            .is_empty());
        let candidates = store
            .list_recovery_candidates()
            .expect("recovery candidate list should work");
        assert_eq!(
            candidates
                .iter()
                .map(|record| record.id.clone())
                .collect::<HashSet<_>>(),
            HashSet::from([preparing_id, deleting_id])
        );
    }

    #[test]
    fn try_start_rejects_hidden_preparing_identity() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let snapshot_id = SnapshotId::generate();
        let mut preparing = SnapshotRecord::template_waiting(
            snapshot_id.clone(),
            Some(SnapshotAlias::parse("hidden-start").expect("alias should parse")),
            Default::default(),
        );
        preparing.lifecycle = SnapshotLifecycle::Preparing;
        persist_preparing_record(&store, &preparing);

        let error = store
            .try_start(&snapshot_id)
            .expect_err("hidden identities must not start template builds");
        assert!(matches!(error, RepositoryError::InvalidRequest { .. }));
        let unchanged = store
            .get_record(&snapshot_id)
            .expect("exact read should work")
            .expect("Preparing identity should remain");
        assert_eq!(unchanged.lifecycle, SnapshotLifecycle::Preparing);
        assert_eq!(unchanged.revision, preparing.revision);
        assert!(matches!(
            unchanged.source,
            SnapshotSource::Template { ref build } if build.status == TemplateBuildStatus::Waiting
        ));
    }

    #[test]
    fn canonical_metadata_commit_rejects_distributed_records() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());

        let error = store
            .commit_record(SnapshotRecord::mock_ready(CommittedSnapshot::mock()))
            .expect_err("metadata-only Distributed commit must be rejected");

        assert!(matches!(error, RepositoryError::InvalidRequest { .. }));
        assert!(!PosixFsSnapshotArtifactLayout::catalog_dir(tempdir.path()).exists());
    }

    #[test]
    fn canonical_metadata_commit_rejects_preparing_input() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let preparing = local_record(
            SnapshotId::generate(),
            SnapshotAlias::parse("invalid-preparing").expect("alias should parse"),
            SnapshotLifecycle::Preparing,
        );

        let error = store
            .commit_record(preparing)
            .expect_err("callers must submit a complete Ready record");

        assert!(matches!(error, RepositoryError::InvalidRequest { .. }));
        assert!(!PosixFsSnapshotArtifactLayout::catalog_dir(tempdir.path()).exists());
    }

    #[test]
    fn equivalent_ready_metadata_retry_repairs_marker_and_alias() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let snapshot_id = SnapshotId::generate();
        let alias = SnapshotAlias::parse("repair-local").expect("alias should parse");
        let record = local_record(snapshot_id.clone(), alias.clone(), SnapshotLifecycle::Ready);
        store
            .commit_record(record.clone())
            .expect("initial Local metadata should commit");

        fs::remove_file(store.commit_marker_path(&snapshot_id))
            .expect("commit marker should exist");
        fs::remove_file(PosixFsSnapshotArtifactLayout::alias_path(
            tempdir.path(),
            &alias,
        ))
        .expect("alias binding should exist");

        store
            .commit_record(record)
            .expect("equivalent retry should repair the commit");

        assert!(store.commit_marker_path(&snapshot_id).exists());
        assert_eq!(
            store
                .resolve_alias(alias.as_ref())
                .expect("alias resolution should work"),
            Some(snapshot_id)
        );
    }

    #[test]
    fn ready_metadata_repair_returns_persisted_canonical_record() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let snapshot_id = SnapshotId::generate();
        let alias = SnapshotAlias::parse("canonical-return").expect("alias should parse");
        let canonical = local_record(snapshot_id.clone(), alias.clone(), SnapshotLifecycle::Ready);
        let persisted_initial = store
            .commit_record(canonical.clone())
            .expect("initial Local metadata should commit");

        fs::remove_file(store.commit_marker_path(&snapshot_id))
            .expect("commit marker should exist");
        fs::remove_file(PosixFsSnapshotArtifactLayout::alias_path(
            tempdir.path(),
            &alias,
        ))
        .expect("alias binding should exist");

        let mut retry = persisted_initial.clone();
        retry.revision = persisted_initial.revision.saturating_add(9);
        retry.updated_at_unix_ms = persisted_initial.updated_at_unix_ms.saturating_add(9_999);
        let returned = store
            .commit_record(retry)
            .expect("equivalent retry should repair the commit");
        let persisted = store
            .get_record(&snapshot_id)
            .expect("exact read should work")
            .expect("canonical record should remain present");

        assert_eq!(returned.id, persisted.id);
        assert_eq!(returned.lifecycle, persisted.lifecycle);
        assert_eq!(returned.committed, persisted.committed);
        assert_eq!(returned.revision, persisted_initial.revision);
        assert_eq!(
            returned.updated_at_unix_ms,
            persisted_initial.updated_at_unix_ms
        );
    }

    #[test]
    fn new_metadata_commit_upgrades_legacy_revision_zero() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let mut record = local_record(
            SnapshotId::generate(),
            SnapshotAlias::parse("revision-zero").expect("alias should parse"),
            SnapshotLifecycle::Ready,
        );
        record.revision = 0;

        let committed = store
            .commit_record(record)
            .expect("new metadata commit should succeed");
        assert_eq!(committed.revision, 2);
        assert_eq!(
            store
                .get_record(&committed.id)
                .expect("exact read should work")
                .expect("record should persist")
                .revision,
            2
        );
    }

    #[test]
    fn preparing_metadata_commit_advances_existing_revision() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let snapshot_id = SnapshotId::generate();
        let alias = SnapshotAlias::parse("preparing-revision").expect("alias should parse");
        let mut preparing = local_record(snapshot_id, alias, SnapshotLifecycle::Preparing);
        preparing.revision = 7;
        persist_preparing_record(&store, &preparing);

        let mut ready = preparing.clone();
        ready.lifecycle = SnapshotLifecycle::Ready;
        let committed = store
            .commit_record(ready)
            .expect("Preparing metadata should become Ready");
        assert_eq!(committed.revision, 8);
        assert_eq!(
            store
                .get_record(&committed.id)
                .expect("exact read should work")
                .expect("record should persist")
                .revision,
            8
        );
    }

    #[test]
    fn template_alias_conflict_preserves_exact_retry() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let alias = SnapshotAlias::parse("claimed-template").expect("alias should parse");
        let winner = SnapshotRecord::template_waiting(
            SnapshotId::generate(),
            Some(alias.clone()),
            Default::default(),
        );
        store
            .create(winner.clone())
            .expect("winner should claim the alias");

        let loser = SnapshotRecord::template_waiting(
            SnapshotId::generate(),
            Some(alias.clone()),
            Default::default(),
        );
        let error = store
            .create(loser.clone())
            .expect_err("a live alias owner must reject the loser");
        assert!(matches!(error, RepositoryError::AliasConflict { .. }));

        let pending = store
            .get_record(&loser.id)
            .expect("exact read should work")
            .expect("the loser identity should be retained");
        assert_eq!(pending.lifecycle, SnapshotLifecycle::Preparing);
        assert_eq!(pending.revision, 1);
        assert!(pending.committed.is_none());
        assert!(store
            .get(&loser.id.to_string())
            .expect("public exact read should work")
            .is_none());
        assert!(store
            .list(SnapshotListFilter::matches_all())
            .expect("public list should work")
            .iter()
            .all(|record| record.id != loser.id));
        assert_eq!(
            store
                .resolve_alias(alias.as_ref())
                .expect("the winning alias should resolve"),
            Some(winner.id.clone())
        );

        store
            .delete_record(&winner.id)
            .expect("the winner should delete");
        let retried = store
            .create(loser.clone())
            .expect("the exact retry should complete after alias release");
        assert_eq!(retried.id, loser.id);
        assert_eq!(retried.lifecycle, SnapshotLifecycle::Ready);
        assert_eq!(retried.revision, 2);
        assert_eq!(
            store
                .resolve_alias(alias.as_ref())
                .expect("the rebound alias should resolve"),
            Some(loser.id)
        );
    }

    #[test]
    fn canonical_record_match_ignores_revision_and_update_time() {
        let mut legacy = SnapshotRecord::template_waiting(
            SnapshotId::generate(),
            Some(SnapshotAlias::parse("legacy-retry").expect("alias should parse")),
            Default::default(),
        );
        legacy.lifecycle = SnapshotLifecycle::Preparing;
        legacy.revision = 0;
        legacy.updated_at_unix_ms = 1;

        let mut retry = legacy.clone();
        retry.lifecycle = SnapshotLifecycle::Ready;
        retry.revision = 27;
        retry.updated_at_unix_ms = 999;

        assert!(legacy.same_catalog_contents(&retry));

        retry.resources.cpu_count += 1;
        assert!(!legacy.same_catalog_contents(&retry));
    }

    #[test]
    fn template_build_mutations_increment_revision() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let snapshot_id = SnapshotId::generate();
        let waiting = SnapshotRecord::template_waiting(
            snapshot_id.clone(),
            Some(SnapshotAlias::parse("build-revision").expect("alias should parse")),
            Default::default(),
        );
        store.create(waiting).expect("template should be created");
        let created = store
            .get_record(&snapshot_id)
            .expect("exact read should work")
            .expect("template should exist");
        assert_eq!(created.revision, 2);

        // Simulate a legacy Ready record decoded without the revision field.
        let mut legacy = created;
        legacy.revision = 0;
        store
            .write_record_unlocked(&legacy)
            .expect("legacy revision should persist");

        let started = store
            .try_start(&snapshot_id)
            .expect("waiting template should start");
        assert_eq!(started.revision, 1);

        store
            .mark_error(
                &snapshot_id,
                crate::snapshot::TemplateBuildErrorReason::new("boom"),
            )
            .expect("building template should accept an error");
        let failed = store
            .get_record(&snapshot_id)
            .expect("exact read should work")
            .expect("failed template should remain");
        assert_eq!(failed.revision, 2);
    }

    #[test]
    fn mark_error_is_idempotent_and_cannot_mutate_completed_or_deleting_builds() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let snapshot_id = SnapshotId::generate();
        store
            .create(SnapshotRecord::template_waiting(
                snapshot_id.clone(),
                Some(SnapshotAlias::parse("error-guard").expect("alias should parse")),
                Default::default(),
            ))
            .expect("template should be created");

        let reason = crate::snapshot::TemplateBuildErrorReason::new("boom");
        store
            .mark_error(&snapshot_id, reason.clone())
            .expect("waiting build should transition to Error");
        let failed = store
            .get_record(&snapshot_id)
            .expect("exact read should work")
            .expect("failed template should remain");
        assert_eq!(failed.revision, 3);
        assert!(matches!(
            failed.source,
            SnapshotSource::Template { ref build }
                if build.status == TemplateBuildStatus::Error
                    && build.error_reason.as_ref() == Some(&reason)
        ));

        store
            .mark_error(&snapshot_id, reason)
            .expect("repeating the same error should be idempotent");
        assert_eq!(
            store
                .get_record(&snapshot_id)
                .expect("exact read should work")
                .expect("failed template should remain")
                .revision,
            failed.revision
        );

        let different = store
            .mark_error(
                &snapshot_id,
                crate::snapshot::TemplateBuildErrorReason::new("different"),
            )
            .expect_err("a different error must not overwrite the first failure");
        assert!(matches!(different, RepositoryError::InvalidRequest { .. }));

        let mut completed = failed.clone();
        if let SnapshotSource::Template { build } = &mut completed.source {
            build.status = TemplateBuildStatus::Ready;
            build.error_reason = None;
        }
        store
            .write_record_unlocked(&completed)
            .expect("completed template should persist");
        store
            .mark_error(
                &snapshot_id,
                crate::snapshot::TemplateBuildErrorReason::new("stale"),
            )
            .expect("stale error after completion should be idempotent");
        let completed_after = store
            .get_record(&snapshot_id)
            .expect("exact read should work")
            .expect("completed template should remain");
        assert_eq!(completed_after.revision, completed.revision);
        assert!(matches!(
            completed_after.source,
            SnapshotSource::Template { ref build } if build.status == TemplateBuildStatus::Ready
        ));

        completed.lifecycle = SnapshotLifecycle::Deleting;
        store
            .write_record_unlocked(&completed)
            .expect("deleting record should persist");
        let deleting = store
            .mark_error(
                &snapshot_id,
                crate::snapshot::TemplateBuildErrorReason::new("late"),
            )
            .expect_err("deleting template must reject build mutation");
        assert!(matches!(deleting, RepositoryError::InvalidRequest { .. }));
        let unchanged = store
            .get_record(&snapshot_id)
            .expect("exact read should work")
            .expect("deleting record should remain");
        assert_eq!(unchanged.lifecycle, SnapshotLifecycle::Deleting);
        assert_eq!(unchanged.revision, completed.revision);
    }

    #[test]
    fn new_alias_conflict_does_not_leave_a_preparing_identity() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let alias = SnapshotAlias::parse("canonical-conflict").expect("alias should parse");
        let existing = local_record(
            SnapshotId::generate(),
            alias.clone(),
            SnapshotLifecycle::Ready,
        );
        store
            .commit_record(existing.clone())
            .expect("existing Local metadata should commit");

        let candidate = local_record(
            SnapshotId::generate(),
            alias.clone(),
            SnapshotLifecycle::Ready,
        );
        let error = store
            .commit_record(candidate.clone())
            .expect_err("live alias conflict must reject the new identity");

        assert!(matches!(error, RepositoryError::AliasConflict { .. }));
        assert!(store
            .get_record(&candidate.id)
            .expect("exact lookup should work")
            .is_none());
        assert!(!store.commit_marker_path(&candidate.id).exists());
        assert_eq!(
            store
                .resolve_alias(alias.as_ref())
                .expect("alias resolution should work"),
            Some(existing.id)
        );
    }

    #[test]
    fn alias_conflict_preserves_preparing_identity_for_exact_retry() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let alias = SnapshotAlias::parse("recover-conflict").expect("alias should parse");
        let preparing = local_record(
            SnapshotId::generate(),
            alias.clone(),
            SnapshotLifecycle::Preparing,
        );
        persist_preparing_record(&store, &preparing);
        store
            .write_commit_marker(&preparing.id)
            .expect("simulate marker persisted before alias binding");

        let conflicting = local_record(
            SnapshotId::generate(),
            alias.clone(),
            SnapshotLifecycle::Ready,
        );
        store
            .commit_record(conflicting.clone())
            .expect("replacement should claim the unbound alias");

        let mut ready = preparing.clone();
        ready.lifecycle = SnapshotLifecycle::Ready;
        let error = store
            .commit_record(ready.clone())
            .expect_err("conflicting recovery must be rejected");
        assert!(matches!(error, RepositoryError::AliasConflict { .. }));
        let preserved = store
            .get_record(&preparing.id)
            .expect("exact lookup should work")
            .expect("alias conflict must preserve the Preparing identity");
        assert_eq!(preserved.lifecycle, SnapshotLifecycle::Preparing);
        assert!(preserved.committed.is_some());
        assert!(store.commit_marker_path(&preparing.id).exists());
        assert_eq!(
            store
                .resolve_alias(alias.as_ref())
                .expect("conflicting alias should remain intact"),
            Some(conflicting.id.clone())
        );

        store
            .delete_record(&conflicting.id)
            .expect("conflicting identity should delete");
        store
            .commit_record(ready)
            .expect("exact retry should complete after the conflict is gone");
        assert_eq!(
            store
                .resolve_alias(alias.as_ref())
                .expect("alias resolution should work"),
            Some(preparing.id)
        );
    }

    #[test]
    fn startup_reconcile_keeps_preparing_record_without_marker() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let snapshot_id = SnapshotId::generate();
        let alias = SnapshotAlias::parse("restart-preparing").expect("alias should parse");
        let preparing = local_record(snapshot_id.clone(), alias, SnapshotLifecycle::Preparing);
        persist_preparing_record(&store, &preparing);
        fs::create_dir_all(store.layout(&snapshot_id).snapshot_dir())
            .expect("simulate a partial snapshot directory");

        store
            .reconcile_startup()
            .expect("startup reconcile should work");

        assert_eq!(
            store
                .get_record(&snapshot_id)
                .expect("exact lookup should work")
                .expect("Preparing identity should survive")
                .lifecycle,
            SnapshotLifecycle::Preparing
        );
        assert!(!store.layout(&snapshot_id).snapshot_dir().exists());
    }

    #[test]
    fn startup_reconcile_respects_active_repository_lease() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let snapshot_id = commit_record(
            &store,
            committed_metadata(
                SnapshotId::generate(),
                "leased-reconcile",
                SnapshotPublishSource::Template,
            ),
        );
        fs::remove_file(store.commit_marker_path(&snapshot_id))
            .expect("commit marker should exist");
        let snapshot_dir = store.layout(&snapshot_id).snapshot_dir();

        let lease = store
            .acquire_repository_shared_lock()
            .expect("runtime lease should acquire");
        store
            .reconcile_startup()
            .expect("busy reconciliation should remain conservative");
        assert!(store.record_path(&snapshot_id).exists());
        assert!(snapshot_dir.exists());

        drop(lease);
        store
            .reconcile_startup()
            .expect("reconciliation should resume after the lease is released");
        assert!(!store.record_path(&snapshot_id).exists());
        assert!(!snapshot_dir.exists());
    }

    #[test]
    fn interrupted_new_publish_preparing_state_is_hidden_and_recoverable() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let alias = SnapshotAlias::parse("recover-new-publish").expect("alias should parse");
        let metadata = SnapshotPublishMetadata {
            alias: Some(alias.clone()),
            ..SnapshotPublishMetadata::mock()
        };
        let committed = CommittedSnapshot::mock();
        let mut preparing = store
            .committed_record_unlocked(&metadata, committed.clone(), 1)
            .expect("Preparing metadata should build");
        preparing.lifecycle = SnapshotLifecycle::Preparing;
        persist_preparing_record(&store, &preparing);
        store
            .write_commit_marker(&metadata.id)
            .expect("commit marker should persist");
        store
            .write_json(
                &PosixFsSnapshotArtifactLayout::alias_path(tempdir.path(), &alias),
                &metadata.id,
            )
            .expect("alias binding should persist");
        assert!(store
            .get(&metadata.id.to_string())
            .expect("public lookup should work")
            .is_none());

        let session = store
            .begin_publish(&metadata.id)
            .expect("recovery publish should begin");
        store
            .commit_publish(&session, metadata.clone(), committed)
            .expect("equivalent retry should complete the hidden identity");

        assert!(store
            .get(&metadata.id.to_string())
            .expect("public lookup should work")
            .is_some());
        assert_eq!(
            store
                .resolve_alias(alias.as_ref())
                .expect("alias resolution should work"),
            Some(metadata.id)
        );
    }

    #[test]
    fn managed_layer_gc_preserves_all_ready_record_references_and_shared_layers() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let layer = |hex: char| ManagedLayer {
            digest: format!("sha256:{}", hex.to_string().repeat(64)),
            size: 1,
            uuid: None,
        };
        let shared = layer('a');
        let memory = layer('b');
        let attached = layer('c');
        let orphan = layer('d');
        let managed_dir = PosixFsSnapshotArtifactLayout::managed_layers_dir(tempdir.path());
        fs::create_dir_all(&managed_dir).expect("managed layer directory should exist");
        for managed in [&shared, &memory, &attached, &orphan] {
            fs::write(
                PosixFsSnapshotArtifactLayout::managed_layer_path(tempdir.path(), &managed.digest),
                b"x",
            )
            .expect("managed layer should write");
        }

        let mut first = CommittedSnapshot::mock();
        first
            .rootfs_layers
            .push(OverlaybdLayerRef::Managed(shared.clone()));
        first.memory_layers.push(memory.clone());
        first
            .attached_drives
            .push(CommittedAttachedDrive::Overlaybd {
                drive_id: "data".to_string(),
                layers: vec![OverlaybdLayerRef::Managed(attached.clone())],
                read_only: true,
                virtual_size: 1,
                mount_path: "/mnt/data".into(),
                sub_path: None,
            });
        let first_metadata = SnapshotPublishMetadata::mock();
        let first_session = store
            .begin_publish(&first_metadata.id)
            .expect("first publish should begin");
        store
            .commit_publish(&first_session, first_metadata, first)
            .expect("first publish should commit");
        drop(first_session);

        let mut second = CommittedSnapshot::mock();
        second
            .rootfs_layers
            .push(OverlaybdLayerRef::Managed(shared.clone()));
        let second_metadata = SnapshotPublishMetadata::mock();
        let second_session = store
            .begin_publish(&second_metadata.id)
            .expect("second publish should begin");
        store
            .commit_publish(&second_session, second_metadata, second)
            .expect("second publish should commit");
        drop(second_session);

        assert_eq!(
            store
                .gc_unreferenced_managed_layers()
                .expect("GC should work"),
            1
        );
        for retained in [&shared, &memory, &attached] {
            assert!(PosixFsSnapshotArtifactLayout::managed_layer_path(
                tempdir.path(),
                &retained.digest
            )
            .exists());
        }
        assert!(
            !PosixFsSnapshotArtifactLayout::managed_layer_path(tempdir.path(), &orphan.digest)
                .exists()
        );
    }

    #[test]
    fn managed_layer_gc_preserves_hidden_preparing_record_references() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let layer = |hex: char| ManagedLayer {
            digest: format!("sha256:{}", hex.to_string().repeat(64)),
            size: 1,
            uuid: None,
        };
        let referenced = layer('a');
        let orphan = layer('b');
        let managed_dir = PosixFsSnapshotArtifactLayout::managed_layers_dir(tempdir.path());
        fs::create_dir_all(&managed_dir).expect("managed layer directory should exist");
        for managed in [&referenced, &orphan] {
            fs::write(
                PosixFsSnapshotArtifactLayout::managed_layer_path(tempdir.path(), &managed.digest),
                b"layer",
            )
            .expect("managed layer should write");
        }

        let mut preparing = local_record(
            SnapshotId::generate(),
            SnapshotAlias::parse("gc-preparing").expect("alias should parse"),
            SnapshotLifecycle::Preparing,
        );
        preparing
            .committed
            .as_mut()
            .expect("local record should have committed metadata")
            .rootfs_layers
            .push(OverlaybdLayerRef::Managed(referenced.clone()));
        persist_preparing_record(&store, &preparing);

        assert_eq!(
            store
                .gc_unreferenced_managed_layers()
                .expect("GC should work"),
            1
        );
        assert!(PosixFsSnapshotArtifactLayout::managed_layer_path(
            tempdir.path(),
            &referenced.digest
        )
        .exists());
        assert!(
            !PosixFsSnapshotArtifactLayout::managed_layer_path(tempdir.path(), &orphan.digest)
                .exists()
        );
    }

    #[test]
    fn managed_layer_gc_aborts_before_sweep_when_a_record_is_unreadable() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        store.ensure_layout().expect("catalog layout should exist");
        let orphan = PosixFsSnapshotArtifactLayout::managed_layer_path(
            tempdir.path(),
            &format!("sha256:{}", "c".repeat(64)),
        );
        fs::create_dir_all(orphan.parent().expect("managed layer should have a parent"))
            .expect("managed layer directory should exist");
        fs::write(&orphan, b"orphan").expect("managed layer should write");
        fs::write(store.records_dir().join("corrupt.json"), b"{")
            .expect("corrupt record should write");

        store
            .gc_unreferenced_managed_layers()
            .expect_err("an unreadable record must conservatively abort GC");

        assert!(orphan.exists());
    }

    #[test]
    fn active_publish_session_blocks_managed_layer_gc() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let session = store
            .begin_publish(&SnapshotId::generate())
            .expect("publish should begin");
        let managed_dir = PosixFsSnapshotArtifactLayout::managed_layers_dir(tempdir.path());
        fs::create_dir_all(&managed_dir).expect("managed layer directory should exist");
        let orphan = PosixFsSnapshotArtifactLayout::managed_layer_path(
            tempdir.path(),
            &format!("sha256:{}", "d".repeat(64)),
        );
        fs::write(&orphan, b"orphan").expect("managed layer should write");

        let root = tempdir.path().to_path_buf();
        let (started_tx, started_rx) = mpsc::channel();
        let (completed_tx, completed_rx) = mpsc::channel();
        let gc_thread = std::thread::spawn(move || {
            started_tx.send(()).expect("start signal should send");
            completed_tx
                .send(PosixFsCatalogStore::new(root).gc_unreferenced_managed_layers())
                .expect("GC result should send");
        });

        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("GC thread should start");
        assert!(matches!(
            completed_rx.recv_timeout(Duration::from_millis(200)),
            Err(RecvTimeoutError::Timeout)
        ));
        assert!(orphan.exists());

        drop(session);
        assert_eq!(
            completed_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("GC should finish after publish releases its lock")
                .expect("GC should succeed"),
            1
        );
        gc_thread.join().expect("GC thread should join");
        assert!(!orphan.exists());
    }

    #[test]
    fn managed_layer_gc_retains_noncanonical_and_nonregular_entries() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let managed_dir = PosixFsSnapshotArtifactLayout::managed_layers_dir(tempdir.path());
        fs::create_dir_all(&managed_dir).expect("managed layer directory should exist");
        let canonical = |hex: char| {
            managed_dir.join(format!(
                "sha256_{}.overlaybd.commit",
                hex.to_string().repeat(64)
            ))
        };

        let removable = canonical('e');
        fs::write(&removable, b"orphan").expect("canonical orphan should write");
        let noncanonical = managed_dir.join("sharedfs_orphan.overlaybd.commit");
        fs::write(&noncanonical, b"keep").expect("noncanonical file should write");
        let target = managed_dir.join("symlink-target");
        fs::write(&target, b"keep").expect("symlink target should write");
        let symlink_path = canonical('f');
        symlink(&target, &symlink_path).expect("symlink should write");
        let directory = canonical('1');
        fs::create_dir(&directory).expect("canonical-looking directory should write");
        let mut non_utf8 = b"sha256_".to_vec();
        non_utf8.extend(std::iter::repeat_n(b'2', 64));
        non_utf8.extend_from_slice(b".overlaybd.commit");
        non_utf8.push(0xff);
        let non_utf8 = managed_dir.join(OsString::from_vec(non_utf8));
        fs::write(&non_utf8, b"keep").expect("non-UTF8 file should write");

        assert_eq!(
            PosixFsCatalogStore::new(tempdir.path().to_path_buf())
                .gc_unreferenced_managed_layers()
                .expect("GC should work"),
            1
        );
        assert!(!removable.exists());
        for retained in [noncanonical, target, symlink_path, directory, non_utf8] {
            assert!(fs::symlink_metadata(retained).is_ok());
        }
    }

    #[test]
    fn same_backing_recovery_rejects_changed_committed_references() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let metadata = SnapshotPublishMetadata::mock();
        let snapshot_id = metadata.id.clone();
        let mut committed = CommittedSnapshot::mock();
        committed
            .rootfs_layers
            .push(OverlaybdLayerRef::Managed(ManagedLayer {
                digest: "sha256:original".to_string(),
                size: 8,
                uuid: None,
            }));

        let session = store
            .begin_publish(&snapshot_id)
            .expect("initial publish should begin");
        store
            .commit_publish(&session, metadata.clone(), committed.clone())
            .expect("initial publish should commit");
        drop(session);
        fs::remove_file(store.commit_marker_path(&snapshot_id))
            .expect("commit marker should exist");

        let mut changed = committed.clone();
        changed.rootfs_layers[0] = OverlaybdLayerRef::Managed(ManagedLayer {
            digest: "sha256:changed".to_string(),
            size: 8,
            uuid: None,
        });
        let session = store
            .begin_publish(&snapshot_id)
            .expect("recovery publish should begin");
        let error = store
            .commit_publish(&session, metadata.clone(), changed)
            .expect_err("same backing must not replace committed references");
        assert!(matches!(error, RepositoryError::InvalidRequest { .. }));
        drop(session);

        let unchanged = store
            .get_record(&snapshot_id)
            .expect("exact read should work")
            .expect("original record should remain");
        assert_eq!(
            unchanged
                .committed
                .expect("committed payload")
                .rootfs_layers,
            committed.rootfs_layers
        );
        assert!(!store.commit_marker_path(&snapshot_id).exists());

        let session = store
            .begin_publish(&snapshot_id)
            .expect("equivalent recovery should begin");
        store
            .commit_publish(&session, metadata, committed)
            .expect("equivalent recovery should restore the marker");
        assert!(store.commit_marker_path(&snapshot_id).exists());
    }

    #[test]
    fn canonical_ready_record_rejects_overwrite_but_allows_same_id_promotion() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let snapshot_id = SnapshotId::generate();
        let alias = SnapshotAlias::parse("promoted-local").expect("alias should parse");
        let local = local_record(snapshot_id.clone(), alias.clone(), SnapshotLifecycle::Ready);
        store
            .commit_record(local.clone())
            .expect("Local metadata should commit");

        let mut overwritten = local.clone();
        overwritten.owner_node_id = Some("node-b".to_string());
        let error = store
            .commit_record(overwritten)
            .expect_err("Ready metadata overwrite must be rejected");
        assert!(matches!(error, RepositoryError::InvalidRequest { .. }));
        assert_eq!(
            store
                .get_record(&snapshot_id)
                .expect("exact read should work")
                .expect("Local record should remain")
                .owner_node_id
                .as_deref(),
            Some("node-a")
        );

        let promotion = SnapshotPublishMetadata {
            id: snapshot_id.clone(),
            snapshot_type: SnapshotType::Distributed,
            owner_node_id: None,
            alias: Some(alias.clone()),
            source: SnapshotPublishSource::Sandbox {
                source_sandbox_id: "sandbox-a".to_string(),
            },
            resources: local.resources,
            ..SnapshotPublishMetadata::mock()
        };
        let mut invalid_promotion = promotion.clone();
        invalid_promotion.source = SnapshotPublishSource::Sandbox {
            source_sandbox_id: "sandbox-b".to_string(),
        };
        let invalid_session = store
            .begin_publish(&snapshot_id)
            .expect("invalid promotion should acquire the record lock");
        let error = store
            .commit_publish(
                &invalid_session,
                invalid_promotion,
                CommittedSnapshot::mock(),
            )
            .expect_err("promotion must not overwrite provenance");
        assert!(matches!(error, RepositoryError::InvalidRequest { .. }));
        drop(invalid_session);
        let unchanged = store
            .get_record(&snapshot_id)
            .expect("exact read should work")
            .expect("Local record should survive failed promotion");
        assert_eq!(unchanged.snapshot_type, SnapshotType::Local);
        assert_eq!(unchanged.owner_node_id.as_deref(), Some("node-a"));
        assert!(store.commit_marker_path(&snapshot_id).exists());

        let session = store
            .begin_publish(&snapshot_id)
            .expect("promotion should begin");
        let promoted = store
            .commit_publish(&session, promotion, CommittedSnapshot::mock())
            .expect("Local record should promote in place");

        assert_eq!(promoted.id, snapshot_id);
        assert_eq!(promoted.alias.as_ref(), Some(&alias));
        assert_eq!(promoted.snapshot_type, SnapshotType::Distributed);
        assert!(promoted.owner_node_id.is_none());
        assert_eq!(promoted.lifecycle, SnapshotLifecycle::Ready);
        assert!(store.commit_marker_path(&promoted.id).exists());

        drop(session);
        let retry_session = store
            .begin_publish(&promoted.id)
            .expect("idempotent retry should acquire the record lock");
        let existing = store
            .validate_publish_transition(
                &retry_session,
                &SnapshotPublishMetadata {
                    id: promoted.id.clone(),
                    snapshot_type: SnapshotType::Distributed,
                    owner_node_id: None,
                    alias: promoted.alias.clone(),
                    source: SnapshotPublishSource::Sandbox {
                        source_sandbox_id: "sandbox-a".to_string(),
                    },
                    resources: promoted.resources,
                    ..SnapshotPublishMetadata::mock()
                },
            )
            .expect("idempotent retry should validate")
            .expect("idempotent retry should return the existing record");
        assert_eq!(existing.id, promoted.id);
        assert_eq!(existing.snapshot_type, SnapshotType::Distributed);
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
    fn deleting_tombstone_rejects_publish_transition() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let snapshot_id = SnapshotId::generate();
        let alias = SnapshotAlias::parse("deleting-tombstone").expect("alias should parse");
        let mut tombstone = SnapshotRecord::template_waiting(
            snapshot_id.clone(),
            Some(alias.clone()),
            Default::default(),
        );
        tombstone.lifecycle = SnapshotLifecycle::Deleting;
        store.ensure_layout().expect("catalog layout should exist");
        store
            .write_record_unlocked(&tombstone)
            .expect("tombstone should persist");

        let session = store
            .begin_publish(&snapshot_id)
            .expect("publish should acquire the identity lock");
        let error = store
            .validate_publish_transition(
                &session,
                &SnapshotPublishMetadata {
                    id: snapshot_id,
                    alias: Some(alias),
                    source: SnapshotPublishSource::Template,
                    ..SnapshotPublishMetadata::mock()
                },
            )
            .expect_err("a deleting identity must not be resurrected");
        assert!(matches!(
            error,
            RepositoryError::ConcurrentModification { .. }
        ));
        store
            .abort_publish(&session)
            .expect("rejected publish staging should be cleaned");
    }

    #[test]
    fn delete_fences_identity_and_releases_alias_after_cleanup() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let snapshot_id = SnapshotId::generate();
        let alias = SnapshotAlias::parse("delete-fence").expect("alias should parse");

        store
            .create(SnapshotRecord::template_waiting(
                snapshot_id.clone(),
                Some(alias.clone()),
                Default::default(),
            ))
            .expect("template identity should be created");
        store
            .delete_record(&snapshot_id)
            .expect("delete should complete");

        let tombstone = store
            .get_record(&snapshot_id)
            .expect("terminal identity lookup should work")
            .expect("delete should retain a terminal identity tombstone");
        assert_eq!(tombstone.lifecycle, SnapshotLifecycle::Deleting);
        assert!(tombstone.committed.is_none());
        assert!(store
            .get(&snapshot_id.to_string())
            .expect("public lookup should work")
            .is_none());
        assert_eq!(
            store
                .resolve_alias(alias.as_ref())
                .expect("deleted alias lookup should work"),
            None
        );
        assert!(!PosixFsSnapshotArtifactLayout::alias_path(tempdir.path(), &alias).exists());

        // Simulate a crash after the terminal tombstone was durable but
        // before alias cleanup. A retry must remove only this stale binding,
        // making the alias safe to reclaim by a different identity.
        store
            .write_json(
                &PosixFsSnapshotArtifactLayout::alias_path(tempdir.path(), &alias),
                &snapshot_id,
            )
            .expect("stale alias should be writable for the crash simulation");
        let replacement_id = SnapshotId::generate();
        store
            .create(SnapshotRecord::template_waiting(
                replacement_id.clone(),
                Some(alias.clone()),
                Default::default(),
            ))
            .expect("a different identity should reclaim a stale tombstone alias");
        assert_eq!(
            store
                .resolve_alias(alias.as_ref())
                .expect("rebound alias lookup should work"),
            Some(replacement_id.clone())
        );
        store
            .delete_record(&snapshot_id)
            .expect("retry should not remove a newer alias binding");
        assert_eq!(
            store
                .resolve_alias(alias.as_ref())
                .expect("new alias binding should remain"),
            Some(replacement_id)
        );

        let session = store
            .begin_publish(&snapshot_id)
            .expect("late publisher should acquire the identity lock");
        let error = store
            .commit_publish(
                &session,
                committed_metadata(
                    snapshot_id.clone(),
                    alias.as_ref(),
                    SnapshotPublishSource::Template,
                ),
                CommittedSnapshot::mock(),
            )
            .expect_err("late publish must not resurrect a deleted identity");
        assert!(matches!(
            error,
            RepositoryError::ConcurrentModification { .. }
        ));
        store
            .abort_publish(&session)
            .expect("failed late publish staging should be cleaned");
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
