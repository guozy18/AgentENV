use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use super::super::shared_runtime_cache_root;
use super::artifacts::{CollectedBuiltArtifacts, PosixFsArtifactStore};
use super::catalog::PosixFsCatalogStore;
use super::layout::{PosixFsSnapshotArtifactLayout, POSIXFS_SNAPSHOT_COMMIT_MARKER};
use super::persist_atomic_file;
use super::run_repository_blocking;
use super::runtime::PosixFsRuntimeResolver;
use crate::image::cache::{local_image_services_from_global_config, OverlaybdLayerStore};
use crate::sandbox::FirecrackerSnapshotManifest;
use crate::snapshot::artifact_cache::LocalArtifactCache;
use crate::snapshot::repository::backends::common::validate_attached_drives;
use crate::snapshot::repository::interfaces::{SnapshotRepository, SnapshotRuntimeResolver};
use crate::snapshot::repository::{RepositoryError, RepositoryResult, SnapshotListFilter};
use crate::snapshot::types::{
    CommittedSnapshot, SnapshotId, SnapshotPublishMetadata, SnapshotRecord,
};

#[derive(Clone, Debug)]
pub struct PosixFsBackendConfig {
    /// Durable repository root. This may live on a shared POSIX or distributed filesystem.
    pub root: std::path::PathBuf,
    /// Optional shared node-local cache root for downloaded/materialized runtime artifacts.
    ///
    /// When `None`, a stable node-local default under the process temp directory is used.
    pub cache_root: Option<std::path::PathBuf>,
    /// Optional node-local cache root for runtime-materialized files such as runnable image configs.
    ///
    /// When `None`, defaults to `<cache_root>/runtime`.
    pub runtime_cache_root: Option<std::path::PathBuf>,
}

pub struct PosixFsBackend {
    repository: Arc<dyn SnapshotRepository>,
    runtime_resolver: Arc<dyn SnapshotRuntimeResolver>,
}

impl PosixFsBackend {
    /// Builds the POSIX backend bundle: durable repository state plus node-local runtime resolver.
    pub fn new(config: PosixFsBackendConfig) -> Result<Self> {
        let cache_root = config
            .cache_root
            .clone()
            .unwrap_or_else(shared_runtime_cache_root);
        let cache = LocalArtifactCache::new(cache_root, None)?;
        Ok(Self::from_parts(
            config,
            local_image_services_from_global_config().overlaybd_layers,
            cache,
        ))
    }

    /// Builds the POSIX backend bundle using a shared node-local artifact cache.
    ///
    /// `runtime_image_hints` is a node-local runtime-acceleration hint (e.g. the
    /// overlaybd commit-store path) consumed only by the runtime resolver. It is
    /// passed here rather than carried in [`PosixFsBackendConfig`] so the durable
    /// backend config stays free of runtime-only concerns.
    pub(crate) fn from_parts(
        config: PosixFsBackendConfig,
        store: Arc<dyn OverlaybdLayerStore>,
        cache: Arc<LocalArtifactCache>,
    ) -> Self {
        let PosixFsBackendConfig {
            root,
            cache_root,
            runtime_cache_root,
        } = config;
        let cache_root = cache_root.unwrap_or_else(shared_runtime_cache_root);
        let runtime_cache_root = runtime_cache_root.unwrap_or_else(|| cache_root.join("runtime"));
        let repository: Arc<dyn SnapshotRepository> =
            Arc::new(PosixFsSnapshotRepository::new(root.clone()));
        let runtime_resolver: Arc<dyn SnapshotRuntimeResolver> = Arc::new(
            PosixFsRuntimeResolver::new(root, runtime_cache_root, store, cache),
        );

        Self {
            repository,
            runtime_resolver,
        }
    }

    /// Returns the committed-state repository for publish/get/list/delete operations.
    pub fn repository(&self) -> Arc<dyn SnapshotRepository> {
        Arc::clone(&self.repository)
    }

    /// Returns the node-local runtime resolver used to materialize runnable paths.
    pub fn runtime_resolver(&self) -> Arc<dyn SnapshotRuntimeResolver> {
        Arc::clone(&self.runtime_resolver)
    }

    /// Splits the backend into its repository and runtime-resolution components.
    pub fn into_parts(
        self,
    ) -> (
        Arc<dyn SnapshotRepository>,
        Arc<dyn SnapshotRuntimeResolver>,
    ) {
        (self.repository, self.runtime_resolver)
    }

    pub(crate) fn local_from_parts(
        root: std::path::PathBuf,
        runtime_cache_root: std::path::PathBuf,
        store: Arc<dyn OverlaybdLayerStore>,
        cache: Arc<LocalArtifactCache>,
    ) -> (
        Arc<PosixFsLocalArtifactStore>,
        Arc<dyn SnapshotRuntimeResolver>,
    ) {
        let artifact_store = Arc::new(PosixFsLocalArtifactStore::new(root.clone()));
        let runtime_resolver: Arc<dyn SnapshotRuntimeResolver> =
            Arc::new(PosixFsRuntimeResolver::new_without_repository_lock(
                root,
                runtime_cache_root,
                store,
                cache,
            ));
        (artifact_store, runtime_resolver)
    }
}

#[derive(Clone)]
/// Node-local physical snapshot artifacts without a metadata catalog.
///
/// The configured primary repository owns the canonical `SnapshotRecord`.
/// This store only writes fixed files, managed layers, and a commit marker;
/// the marker makes the closure visible to the local runtime resolver.
pub(crate) struct PosixFsLocalArtifactStore {
    root: std::path::PathBuf,
    artifact_store: PosixFsArtifactStore,
}

impl PosixFsLocalArtifactStore {
    pub(crate) fn new(root: std::path::PathBuf) -> Self {
        Self {
            artifact_store: PosixFsArtifactStore::new(root.clone()),
            root,
        }
    }

    pub(crate) async fn commit(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: FirecrackerSnapshotManifest,
    ) -> RepositoryResult<CommittedSnapshot> {
        let store = self.clone();
        run_repository_blocking("publish local snapshot artifacts", move || {
            metadata
                .validate()
                .map_err(|reason| RepositoryError::InvalidRequest { reason })?;
            validate_attached_drives(&manifest)?;
            let built = store
                .artifact_store
                .import_built_artifacts(&metadata.id, &manifest);
            let built = match built {
                Ok(built) => built,
                Err(error) => {
                    store.remove_snapshot_dir(&metadata.id);
                    return Err(error);
                }
            };
            let committed = PosixFsSnapshotRepository::committed_snapshot(&metadata, built);
            let layout = PosixFsSnapshotArtifactLayout::new(&store.root, &metadata.id);
            let marker = layout.path(POSIXFS_SNAPSHOT_COMMIT_MARKER);
            let parent = marker.parent().ok_or_else(|| RepositoryError::Backend {
                message: format!(
                    "resolve local snapshot marker parent '{}',",
                    marker.display()
                ),
                source: None,
            })?;
            persist_atomic_file(parent, &marker, b"ready\n", "local snapshot commit marker")?;
            Ok(committed)
        })
        .await
    }

    pub(crate) fn commit_marker(&self, id: &SnapshotId) -> std::path::PathBuf {
        PosixFsSnapshotArtifactLayout::new(&self.root, id).path(POSIXFS_SNAPSHOT_COMMIT_MARKER)
    }

    pub(crate) fn has_committed(&self, id: &SnapshotId) -> bool {
        self.commit_marker(id).is_file()
    }

    pub(crate) async fn purge(&self, id: &SnapshotId) -> RepositoryResult<bool> {
        let root = self.root.clone();
        let id = id.clone();
        run_repository_blocking("purge local snapshot artifacts", move || {
            let path = PosixFsSnapshotArtifactLayout::new(&root, &id).snapshot_dir();
            match std::fs::remove_dir_all(&path) {
                Ok(()) => Ok(true),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
                Err(error) => Err(RepositoryError::backend(
                    format!("remove local snapshot artifacts '{}'", path.display()),
                    error,
                )),
            }
        })
        .await
    }

    pub(crate) async fn list_committed_ids(&self) -> RepositoryResult<Vec<SnapshotId>> {
        let root = self.root.clone();
        run_repository_blocking("list local snapshot artifacts", move || {
            let snapshots_dir = PosixFsSnapshotArtifactLayout::snapshots_dir(&root);
            let entries = match std::fs::read_dir(&snapshots_dir) {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
                Err(error) => {
                    return Err(RepositoryError::backend(
                        format!(
                            "read local snapshot directory '{}'",
                            snapshots_dir.display()
                        ),
                        error,
                    ));
                }
            };
            let mut ids = Vec::new();
            for entry in entries {
                let path = entry
                    .map_err(|error| RepositoryError::backend("read local snapshot entry", error))?
                    .path();
                if !path.is_dir() {
                    continue;
                }
                let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                    continue;
                };
                let Ok(id) = SnapshotId::parse(name) else {
                    continue;
                };
                let marker = PosixFsSnapshotArtifactLayout::new(&root, &id)
                    .path(POSIXFS_SNAPSHOT_COMMIT_MARKER);
                if marker.is_file() {
                    ids.push(id);
                }
            }
            Ok(ids)
        })
        .await
    }

    fn remove_snapshot_dir(&self, id: &SnapshotId) {
        let path = PosixFsSnapshotArtifactLayout::new(&self.root, id).snapshot_dir();
        let _ = std::fs::remove_dir_all(path);
    }
}

#[derive(Clone)]
pub(crate) struct PosixFsSnapshotRepository {
    catalog_store: PosixFsCatalogStore,
    artifact_store: PosixFsArtifactStore,
}

impl PosixFsSnapshotRepository {
    pub(crate) fn new(root: std::path::PathBuf) -> Self {
        let catalog_store = PosixFsCatalogStore::new(root.clone());
        if let Err(error) = catalog_store.reconcile_startup() {
            tracing::warn!(error = %error, "snapshot catalog startup reconciliation failed");
        }
        Self {
            catalog_store,
            artifact_store: PosixFsArtifactStore::new(root),
        }
    }

    fn committed_snapshot(
        metadata: &SnapshotPublishMetadata,
        built: CollectedBuiltArtifacts,
    ) -> CommittedSnapshot {
        CommittedSnapshot {
            context: metadata.context.clone(),
            startup: metadata.startup.clone(),
            runtime_versions: metadata.runtime_versions.clone(),
            virtualization_mode: metadata.virtualization_mode,
            image_configs: metadata.image_configs.clone(),
            custom_extension_params: metadata.custom_extension_params.clone(),
            rootfs_layers: built.rootfs_layers,
            attached_drives: built.attached_drives,
            memory_layers: built.memory_layers,
            disk_publications: Vec::new(),
            legacy_artifact_namespace: None,
        }
    }

    fn publish_sync(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: FirecrackerSnapshotManifest,
    ) -> RepositoryResult<SnapshotRecord> {
        validate_attached_drives(&manifest)?;

        let session = self.catalog_store.begin_publish(&metadata.id)?;
        let existing = match self
            .catalog_store
            .validate_publish_transition(&session, &metadata)
        {
            Ok(existing) => existing,
            Err(error) => {
                // `begin_publish` creates the per-identity staging directory
                // before validation. Remove that empty staging directory on
                // a rejected retry (in particular a Deleting tombstone), but
                // keep any already committed closure intact.
                let _ = self.catalog_store.abort_publish(&session);
                return Err(error);
            }
        };
        if let Some(existing) = existing {
            return Ok(existing);
        }
        let built = match self
            .artifact_store
            .import_built_artifacts(&metadata.id, &manifest)
        {
            Ok(built) => built,
            Err(error) => {
                let _ = self.catalog_store.abort_publish(&session);
                return Err(error);
            }
        };

        let committed = Self::committed_snapshot(&metadata, built);
        match self
            .catalog_store
            .commit_publish(&session, metadata, committed)
        {
            Ok(stored) => Ok(stored),
            Err(error) => {
                let _ = self.catalog_store.abort_publish(&session);
                Err(error)
            }
        }
    }

    async fn run_catalog<T, F>(&self, operation: &'static str, work: F) -> RepositoryResult<T>
    where
        T: Send + 'static,
        F: FnOnce(PosixFsCatalogStore) -> RepositoryResult<T> + Send + 'static,
    {
        let catalog = self.catalog_store.clone();
        run_repository_blocking(operation, move || work(catalog)).await
    }
}

#[async_trait]
impl SnapshotRepository for PosixFsSnapshotRepository {
    async fn create(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        self.run_catalog("create snapshot record", move |catalog| {
            catalog.create(record)
        })
        .await
    }

    async fn publish(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: FirecrackerSnapshotManifest,
    ) -> RepositoryResult<SnapshotRecord> {
        let repository = self.clone();
        run_repository_blocking("publish snapshot", move || {
            repository.publish_sync(metadata, manifest)
        })
        .await
    }

    async fn commit_record(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        self.run_catalog("commit snapshot record", move |catalog| {
            catalog.commit_record(record)
        })
        .await
    }

    async fn get_record(&self, id: &SnapshotId) -> RepositoryResult<Option<SnapshotRecord>> {
        let id = id.clone();
        self.run_catalog("load exact snapshot record", move |catalog| {
            catalog.get_record(&id)
        })
        .await
    }

    async fn get_committed_record(
        &self,
        id: &SnapshotId,
    ) -> RepositoryResult<Option<SnapshotRecord>> {
        let id = id.clone();
        self.run_catalog("load committed snapshot record", move |catalog| {
            catalog.get_committed_record(&id)
        })
        .await
    }

    async fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        let id_or_alias = id_or_alias.to_string();
        self.run_catalog("load snapshot", move |catalog| catalog.get(&id_or_alias))
            .await
    }

    async fn get_for_delete(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        let id_or_alias = id_or_alias.to_string();
        self.run_catalog("load snapshot for delete", move |catalog| {
            catalog.get_for_delete(&id_or_alias)
        })
        .await
    }

    async fn list(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        self.run_catalog("list snapshots", move |catalog| catalog.list(filter))
            .await
    }

    async fn delete(&self, id_or_alias: &str) -> RepositoryResult<()> {
        let id_or_alias = id_or_alias.to_string();
        self.run_catalog("delete snapshot", move |catalog| {
            let Some(record) = catalog.get_for_delete(&id_or_alias)? else {
                return Ok(());
            };
            catalog.delete_record(&record.id).map(|_| ())
        })
        .await
    }

    async fn delete_by_id(&self, id: &SnapshotId) -> RepositoryResult<bool> {
        let id = id.clone();
        self.run_catalog("delete snapshot by id", move |catalog| {
            catalog.delete_record(&id)
        })
        .await
    }

    async fn resolve_alias(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        let alias = alias.to_string();
        self.run_catalog("resolve snapshot alias", move |catalog| {
            catalog.resolve_alias(&alias)
        })
        .await
    }

    async fn try_start_build(&self, id: &SnapshotId) -> RepositoryResult<SnapshotRecord> {
        let id = id.clone();
        self.run_catalog("start template build", move |catalog| {
            catalog.try_start(&id)
        })
        .await
    }

    async fn mark_build_error(
        &self,
        id: &SnapshotId,
        reason: crate::snapshot::TemplateBuildErrorReason,
    ) -> RepositoryResult<()> {
        let id = id.clone();
        self.run_catalog("mark template build error", move |catalog| {
            catalog.mark_error(&id, reason)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    use overlaybd::config::ImageConfig as OverlaybdImageConfig;
    use tempfile::TempDir;
    use tokio::sync::oneshot;
    use tokio::time::timeout;

    use super::super::layout::{PosixFsSnapshotArtifactLayout, POSIXFS_SNAPSHOT_COMMIT_MARKER};
    use super::super::runtime::PosixFsRuntimeResolver;
    use super::{PosixFsBackend, PosixFsBackendConfig, PosixFsSnapshotRepository};
    use crate::image::cache::{OverlaybdLayerLocation, OverlaybdLayerStore};
    use crate::sandbox::{ExtraDrive, FirecrackerSnapshotManifest};
    use crate::snapshot::artifact_cache::LocalArtifactCache;
    use crate::snapshot::mock::write_mock_built_artifacts;
    use crate::snapshot::repository::{
        RepositoryError, SnapshotRepository, SnapshotRuntimeResolver,
    };
    use crate::snapshot::{
        CommittedSnapshot, ManagedLayer, OverlaybdLayerRef, SnapshotAlias, SnapshotId,
        SnapshotPublishMetadata, SnapshotPublishSource, SnapshotRecord, SnapshotSource,
        TemplateBuildErrorReason, SNAPSHOT_ARTIFACT_LAYOUT,
    };
    use crate::types::SandboxResources;

    use super::super::catalog::PosixFsCatalogStore;

    #[derive(Debug)]
    struct TestOverlaybdLayerStore;

    impl OverlaybdLayerStore for TestOverlaybdLayerStore {
        fn layer_location(&self, _: &str, _: u64, _: bool) -> OverlaybdLayerLocation {
            OverlaybdLayerLocation::CacheDir("test-image-cache/commits".into())
        }

        fn publishable_roots(&self) -> Vec<std::path::PathBuf> {
            Vec::new()
        }
    }

    fn test_overlaybd_layer_store() -> Arc<dyn OverlaybdLayerStore> {
        Arc::new(TestOverlaybdLayerStore)
    }

    fn sample_metadata(id: SnapshotId, alias: Option<&str>) -> SnapshotPublishMetadata {
        SnapshotPublishMetadata {
            id,
            alias: alias.map(|value| SnapshotAlias::parse(value).expect("alias should parse")),
            ..SnapshotPublishMetadata::mock()
        }
    }

    fn ready_record(
        metadata: SnapshotPublishMetadata,
        committed: CommittedSnapshot,
    ) -> SnapshotRecord {
        let mut record = SnapshotRecord::mock_ready(committed);
        record.id = metadata.id;
        record.snapshot_type = metadata.snapshot_type;
        record.alias = metadata.alias;
        record.resources = metadata.resources;
        let source = match metadata.source {
            SnapshotPublishSource::Template => record.source,
            SnapshotPublishSource::Sandbox { source_sandbox_id } => {
                SnapshotSource::Sandbox { source_sandbox_id }
            }
        };
        record.source = source;
        record
    }

    fn test_backend(root: &Path) -> PosixFsBackend {
        let config = PosixFsBackendConfig {
            root: root.to_path_buf(),
            cache_root: Some(root.join("runtime-cache")),
            runtime_cache_root: Some(root.join("runtime-cache").join("runtime")),
        };
        let cache = LocalArtifactCache::new(root.join("runtime-cache"), None)
            .expect("local artifact cache");
        PosixFsBackend::from_parts(config, test_overlaybd_layer_store(), cache)
    }

    fn test_repository(root: &Path) -> PosixFsSnapshotRepository {
        PosixFsSnapshotRepository::new(root.to_path_buf())
    }

    fn seed_built_snapshot(root: &Path) -> FirecrackerSnapshotManifest {
        let local_root = root.join("local").join(uuid::Uuid::now_v7().to_string());
        let (_, _, manifest) =
            write_mock_built_artifacts(&local_root).expect("mock built artifacts should write");
        manifest
    }

    fn seed_committed_firecracker_manifest(
        repository_root: &Path,
        snapshot_id: &SnapshotId,
        memory_virtual_size: u64,
        rootfs_virtual_size: u64,
    ) {
        let snapshot_dir = repository_root
            .join("snapshots")
            .join(snapshot_id.to_string());
        fs::create_dir_all(&snapshot_dir).expect("snapshot dir");
        let manifest = FirecrackerSnapshotManifest::new(
            snapshot_dir.join(SNAPSHOT_ARTIFACT_LAYOUT.vm_state),
            snapshot_dir.join(SNAPSHOT_ARTIFACT_LAYOUT.memory_image_config),
            memory_virtual_size,
            snapshot_dir.join(SNAPSHOT_ARTIFACT_LAYOUT.rootfs_image_config),
            rootfs_virtual_size,
            &[],
        )
        .expect("seed manifest should be valid");
        fs::write(
            snapshot_dir.join(SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest),
            serde_json::to_vec_pretty(&manifest).expect("serialize firecracker manifest"),
        )
        .expect("write firecracker manifest");
    }

    #[tokio::test]
    async fn publishes_gets_and_resolves_snapshot() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let backend = test_backend(tempdir.path());
        let repository = backend.repository();
        let resolver = backend.runtime_resolver();
        let snapshot_id = SnapshotId::generate();
        let local_artifacts = seed_built_snapshot(tempdir.path());
        let metadata = sample_metadata(snapshot_id, Some("mvp"));
        let stored = repository
            .publish(metadata, local_artifacts)
            .await
            .expect("publish should work");

        let fetched = repository
            .get("mvp")
            .await
            .expect("get should work")
            .expect("snapshot should exist");
        assert_eq!(stored.id, fetched.id);

        let runnable = resolver
            .resolve(Arc::new(fetched))
            .await
            .expect("resolve should work");
        assert!(runnable.manifest().rootfs.image_config_path.exists());
        assert!(runnable.manifest().vm_state.path.exists());
        assert!(runnable.manifest().memory.image_config_path.exists());

        let image_config: OverlaybdImageConfig = serde_json::from_slice(
            &std::fs::read(runnable.manifest().rootfs.image_config_path.as_path())
                .expect("read image config"),
        )
        .expect("parse overlaybd image config");
        assert_eq!(image_config.repo_blob_url, "");
        assert_eq!(image_config.lowers.len(), 1);
    }

    #[tokio::test]
    async fn resolved_runnable_lease_blocks_delete_until_drop() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let backend = test_backend(tempdir.path());
        let repository = backend.repository();
        let resolver = backend.runtime_resolver();
        let snapshot_id = SnapshotId::generate();
        let stored = repository
            .publish(
                sample_metadata(snapshot_id.clone(), Some("leased-delete")),
                seed_built_snapshot(tempdir.path()),
            )
            .await
            .expect("publish should work");
        let runnable = resolver
            .resolve(Arc::new(stored))
            .await
            .expect("resolve should work");

        let root = tempdir.path().to_path_buf();
        let id_for_delete = snapshot_id.clone();
        let (started_tx, started_rx) = oneshot::channel();
        let mut delete_task = tokio::task::spawn_blocking(move || {
            started_tx.send(()).expect("start signal should send");
            PosixFsCatalogStore::new(root).delete_record(&id_for_delete)
        });
        started_rx.await.expect("delete task should start");
        assert!(timeout(Duration::from_millis(200), &mut delete_task)
            .await
            .is_err());
        assert!(PosixFsSnapshotArtifactLayout::record_path(tempdir.path(), &snapshot_id).exists());
        assert_eq!(
            repository
                .get_record(&snapshot_id)
                .await
                .expect("fenced record lookup should work")
                .expect("delete fence should retain the identity")
                .lifecycle,
            crate::snapshot::SnapshotLifecycle::Deleting
        );

        drop(runnable);
        assert!(timeout(Duration::from_secs(2), delete_task)
            .await
            .expect("delete should finish after the runnable lease is dropped")
            .expect("delete task should join")
            .expect("delete should succeed"));
        let tombstone = repository
            .get_record(&snapshot_id)
            .await
            .expect("terminal tombstone lookup should work")
            .expect("delete should retain the terminal identity tombstone");
        assert_eq!(
            tombstone.lifecycle,
            crate::snapshot::SnapshotLifecycle::Deleting
        );
        assert!(tombstone.committed.is_none());
    }

    #[tokio::test]
    async fn resolved_runnable_lease_blocks_managed_layer_gc_until_drop() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let backend = test_backend(tempdir.path());
        let repository = backend.repository();
        let resolver = backend.runtime_resolver();
        let snapshot_id = SnapshotId::generate();
        let stored = repository
            .publish(
                sample_metadata(snapshot_id, Some("leased-gc")),
                seed_built_snapshot(tempdir.path()),
            )
            .await
            .expect("publish should work");
        let runnable = resolver
            .resolve(Arc::new(stored))
            .await
            .expect("resolve should work");
        let orphan = PosixFsSnapshotArtifactLayout::managed_layer_path(
            tempdir.path(),
            &format!("sha256:{}", "d".repeat(64)),
        );
        fs::create_dir_all(orphan.parent().expect("managed layer should have a parent"))
            .expect("managed layer directory should exist");
        fs::write(&orphan, b"orphan").expect("managed layer should write");

        let root = tempdir.path().to_path_buf();
        let (started_tx, started_rx) = oneshot::channel();
        let mut gc_task = tokio::task::spawn_blocking(move || {
            started_tx.send(()).expect("start signal should send");
            PosixFsCatalogStore::new(root).gc_unreferenced_managed_layers()
        });
        started_rx.await.expect("GC task should start");
        assert!(timeout(Duration::from_millis(200), &mut gc_task)
            .await
            .is_err());
        assert!(orphan.exists());

        drop(runnable);
        assert_eq!(
            timeout(Duration::from_secs(2), gc_task)
                .await
                .expect("GC should finish after the runnable lease is dropped")
                .expect("GC task should join")
                .expect("GC should succeed"),
            1
        );
        assert!(!orphan.exists());
    }

    #[tokio::test]
    async fn equivalent_publish_retry_skips_missing_inputs_but_changed_metadata_is_rejected() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let repository = test_repository(tempdir.path());
        let snapshot_id = SnapshotId::generate();
        let metadata = sample_metadata(snapshot_id.clone(), Some("idempotent-publish"));
        let original_manifest = seed_built_snapshot(tempdir.path());
        let stored = repository
            .publish(metadata.clone(), original_manifest.clone())
            .await
            .expect("initial publish should work");

        let missing_manifest = FirecrackerSnapshotManifest::new(
            tempdir.path().join("missing-vm-state"),
            tempdir.path().join("missing-memory-config"),
            1,
            tempdir.path().join("missing-rootfs-config"),
            1,
            &[],
        )
        .expect("manifest shape should be valid");
        let retried = repository
            .publish(metadata.clone(), missing_manifest.clone())
            .await
            .expect("equivalent retry should not read publish inputs");
        assert_eq!(retried.id, stored.id);
        assert_eq!(retried.updated_at_unix_ms, stored.updated_at_unix_ms);

        let alias = metadata
            .alias
            .clone()
            .expect("test metadata should have alias");
        fs::remove_file(PosixFsSnapshotArtifactLayout::alias_path(
            tempdir.path(),
            &alias,
        ))
        .expect("alias binding should exist");
        repository
            .publish(metadata.clone(), missing_manifest.clone())
            .await
            .expect("equivalent retry should repair the alias without reading inputs");
        assert_eq!(
            repository
                .resolve_alias(alias.as_ref())
                .await
                .expect("alias resolution should work"),
            Some(snapshot_id.clone())
        );

        let mut changed_metadata = metadata.clone();
        changed_metadata.context.workdir = "/different".to_string();
        let error = repository
            .publish(changed_metadata, missing_manifest)
            .await
            .expect_err("changed logical metadata must not be accepted as an idempotent retry");
        assert!(matches!(error, RepositoryError::InvalidRequest { .. }));

        let unchanged = repository
            .get(&snapshot_id.to_string())
            .await
            .expect("snapshot lookup should work")
            .expect("original snapshot should remain visible");
        assert_eq!(unchanged.updated_at_unix_ms, stored.updated_at_unix_ms);
        assert_eq!(
            unchanged.committed.expect("committed payload").context,
            stored.committed.expect("committed payload").context
        );

        let layout = PosixFsSnapshotArtifactLayout::new(tempdir.path(), &snapshot_id);
        fs::remove_file(layout.path(POSIXFS_SNAPSHOT_COMMIT_MARKER))
            .expect("remove marker to simulate an interrupted delete");
        assert!(repository
            .get(&snapshot_id.to_string())
            .await
            .expect("hidden snapshot lookup should work")
            .is_none());

        repository
            .publish(metadata, original_manifest)
            .await
            .expect("equivalent retry should restore the missing marker");
        assert!(layout.path(POSIXFS_SNAPSHOT_COMMIT_MARKER).exists());
        assert!(repository
            .get(&snapshot_id.to_string())
            .await
            .expect("recovered snapshot lookup should work")
            .is_some());
    }

    #[tokio::test]
    async fn changed_recovery_retry_preserves_existing_fixed_artifacts() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let repository = test_repository(tempdir.path());
        let snapshot_id = SnapshotId::generate();
        let metadata = sample_metadata(snapshot_id.clone(), Some("immutable-recovery"));
        let original_manifest = seed_built_snapshot(tempdir.path());
        repository
            .publish(metadata.clone(), original_manifest.clone())
            .await
            .expect("initial publish should work");

        let layout = PosixFsSnapshotArtifactLayout::new(tempdir.path(), &snapshot_id);
        let vm_state_path = layout.path(SNAPSHOT_ARTIFACT_LAYOUT.vm_state);
        let manifest_path = layout.path(SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest);
        let committed_vm_state = fs::read(&vm_state_path).expect("committed vm state should exist");
        let committed_manifest = fs::read(&manifest_path).expect("committed manifest should exist");
        fs::remove_file(layout.path(POSIXFS_SNAPSHOT_COMMIT_MARKER))
            .expect("commit marker should exist");

        let changed_vm_state_path = tempdir.path().join("changed-vm-state");
        fs::write(&changed_vm_state_path, b"different vm state")
            .expect("changed vm state should write");
        let mut changed_vm_state = original_manifest.clone();
        changed_vm_state.vm_state.path = changed_vm_state_path;
        let error = repository
            .publish(metadata.clone(), changed_vm_state)
            .await
            .expect_err("changed vm state must not replace committed bytes");
        assert!(matches!(error, RepositoryError::IntegrityMismatch { .. }));
        assert_eq!(
            fs::read(&vm_state_path).expect("committed vm state should remain"),
            committed_vm_state
        );
        assert_eq!(
            fs::read(&manifest_path).expect("committed manifest should remain"),
            committed_manifest
        );

        let mut changed_manifest = original_manifest.clone();
        changed_manifest.rootfs.virtual_size += 1;
        let error = repository
            .publish(metadata.clone(), changed_manifest)
            .await
            .expect_err("changed firecracker manifest must not replace committed bytes");
        assert!(matches!(error, RepositoryError::IntegrityMismatch { .. }));
        assert_eq!(
            fs::read(&vm_state_path).expect("committed vm state should remain"),
            committed_vm_state
        );
        assert_eq!(
            fs::read(&manifest_path).expect("committed manifest should remain"),
            committed_manifest
        );
        assert!(repository
            .get_record(&snapshot_id)
            .await
            .expect("exact record lookup should work")
            .is_some());

        repository
            .publish(metadata, original_manifest)
            .await
            .expect("exact recovery should restore visibility");
        assert!(layout.path(POSIXFS_SNAPSHOT_COMMIT_MARKER).exists());
    }

    #[tokio::test]
    async fn pending_template_publish_accepts_the_computed_disk_size() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let repository = test_repository(tempdir.path());
        let snapshot_id = SnapshotId::generate();
        let alias = SnapshotAlias::parse("computed-disk-size").expect("alias should parse");
        let pending_resources = SandboxResources {
            disk_size_mib: 0,
            ..Default::default()
        };
        repository
            .create(SnapshotRecord::template_waiting(
                snapshot_id.clone(),
                Some(alias.clone()),
                pending_resources,
            ))
            .await
            .expect("pending template should create");

        let metadata = sample_metadata(snapshot_id, Some(alias.as_ref()));
        let expected_resources = metadata.resources;
        let published = repository
            .publish(metadata, seed_built_snapshot(tempdir.path()))
            .await
            .expect("template publication should accept its computed disk size");

        assert_eq!(published.resources, expected_resources);
    }

    #[tokio::test]
    async fn failed_commit_cleans_uncommitted_snapshot_directory() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let repository_root = tempdir.path().to_path_buf();
        let repository = test_backend(tempdir.path()).repository();

        let first_id = SnapshotId::generate();
        let local_artifacts = seed_built_snapshot(tempdir.path());
        let first_metadata = sample_metadata(first_id.clone(), Some("conflict"));
        repository
            .publish(first_metadata, local_artifacts)
            .await
            .expect("first publish should work");

        let second_id = SnapshotId::generate();
        let local_artifacts = seed_built_snapshot(tempdir.path());
        let err = repository
            .publish(
                sample_metadata(second_id.clone(), Some("conflict")),
                local_artifacts,
            )
            .await
            .expect_err("second publish should fail");

        assert!(matches!(err, RepositoryError::AliasConflict { .. }));
        assert!(repository
            .get_record(&second_id)
            .await
            .expect("exact lookup after conflict should work")
            .is_none());
        assert!(
            !repository_root
                .join("snapshots")
                .join(second_id.to_string())
                .exists(),
            "failed publish should not leave a committed revision directory"
        );
    }

    #[tokio::test]
    async fn delete_removes_committed_snapshot_directory() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let repository = test_backend(tempdir.path()).repository();
        let snapshot_id = SnapshotId::generate();
        let local_artifacts = seed_built_snapshot(tempdir.path());
        let metadata = sample_metadata(snapshot_id.clone(), Some("cleanup"));

        repository
            .publish(metadata, local_artifacts)
            .await
            .expect("publish should work");

        let committed_dir = tempdir
            .path()
            .join("snapshots")
            .join(snapshot_id.to_string());
        assert!(
            committed_dir.exists(),
            "committed snapshot dir should exist"
        );

        repository
            .delete("cleanup")
            .await
            .expect("delete should work");

        assert!(
            !committed_dir.exists(),
            "delete should remove the whole committed snapshot directory"
        );
        assert!(
            repository
                .get(&snapshot_id.to_string())
                .await
                .expect("get after delete should work")
                .is_none(),
            "deleted snapshot should no longer be visible"
        );
    }

    #[tokio::test]
    async fn delete_recovers_after_visibility_marker_was_removed() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let repository = test_backend(tempdir.path()).repository();
        let snapshot_id = SnapshotId::generate();
        let alias = SnapshotAlias::parse("interrupted-delete").expect("alias should parse");

        repository
            .publish(
                sample_metadata(snapshot_id.clone(), Some(alias.as_ref())),
                seed_built_snapshot(tempdir.path()),
            )
            .await
            .expect("publish should work");

        let layout = PosixFsSnapshotArtifactLayout::new(tempdir.path(), &snapshot_id);
        fs::remove_file(layout.path(POSIXFS_SNAPSHOT_COMMIT_MARKER))
            .expect("simulate delete interrupted after hiding the record");
        assert!(repository
            .get(alias.as_ref())
            .await
            .expect("hidden snapshot lookup should work")
            .is_none());

        repository
            .delete(alias.as_ref())
            .await
            .expect("retry should fence and remove the hidden record artifacts");

        assert!(!layout.snapshot_dir().exists());
        let tombstone = repository
            .get_record(&snapshot_id)
            .await
            .expect("terminal tombstone lookup should work")
            .expect("interrupted delete should retain the identity fence");
        assert_eq!(
            tombstone.lifecycle,
            crate::snapshot::SnapshotLifecycle::Deleting
        );
        assert!(tombstone.committed.is_none());
        assert!(!PosixFsSnapshotArtifactLayout::alias_path(tempdir.path(), &alias).exists());
    }

    #[tokio::test]
    async fn delete_removes_failed_template_build_record() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let repository = test_backend(tempdir.path()).repository();
        let snapshot_id = SnapshotId::generate();
        let record =
            SnapshotRecord::template_waiting(snapshot_id.clone(), None, Default::default());

        repository.create(record).await.expect("create should work");
        repository
            .mark_build_error(&snapshot_id, TemplateBuildErrorReason::new("boom"))
            .await
            .expect("mark error should work");

        let record_path = tempdir
            .path()
            .join("catalog")
            .join("records")
            .join(format!("{snapshot_id}.json"));
        assert!(record_path.exists(), "failed build record should exist");

        repository
            .delete(&snapshot_id.to_string())
            .await
            .expect("delete should work");

        let tombstone = repository
            .get_record(&snapshot_id)
            .await
            .expect("terminal tombstone lookup should work")
            .expect("delete should retain failed build identity fence");
        assert_eq!(
            tombstone.lifecycle,
            crate::snapshot::SnapshotLifecycle::Deleting
        );
        assert!(tombstone.committed.is_none());
        assert!(
            record_path.exists(),
            "terminal tombstone should remain durable"
        );
        assert!(
            repository
                .get(&snapshot_id.to_string())
                .await
                .expect("get after delete should work")
                .is_none(),
            "deleted failed build should no longer be visible"
        );
    }

    #[tokio::test]
    async fn publish_rejects_duplicate_attached_drive_ids_before_touching_catalog() {
        let tempdir = TempDir::new().expect("tempdir");
        let repository = test_repository(tempdir.path());
        let snapshot_id = SnapshotId::generate();
        let manifest = FirecrackerSnapshotManifest::for_test(
            32768,
            &[
                ExtraDrive::Overlaybd {
                    drive_id: "data".to_string(),
                    image_config_path: "drives/data/image.json".into(),
                    read_only: true,
                    mount_path: ExtraDrive::default_mount_path("data"),
                    virtual_size: Some(32768),
                    sub_path: None,
                },
                ExtraDrive::Overlaybd {
                    drive_id: "data".to_string(),
                    image_config_path: "drives/data/image.json".into(),
                    read_only: true,
                    mount_path: ExtraDrive::default_mount_path("data"),
                    virtual_size: Some(32768),
                    sub_path: None,
                },
            ],
        );
        let err = repository
            .publish(sample_metadata(snapshot_id, Some("dup-drive")), manifest)
            .await
            .expect_err("duplicate attached drive ids should be rejected");

        assert!(matches!(err, RepositoryError::InvalidRequest { .. }));
        assert!(!tempdir.path().join("catalog").exists());
    }

    #[tokio::test]
    async fn committed_record_requires_a_commit_marker() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let repository = test_repository(tempdir.path());
        let snapshot_id = SnapshotId::generate();
        repository
            .publish(
                sample_metadata(snapshot_id.clone(), Some("marker-read")),
                seed_built_snapshot(tempdir.path()),
            )
            .await
            .expect("snapshot publish should work");

        assert!(repository
            .get_committed_record(&snapshot_id)
            .await
            .expect("committed read should work")
            .is_some());
        std::fs::remove_file(
            PosixFsSnapshotArtifactLayout::new(tempdir.path(), &snapshot_id)
                .path(POSIXFS_SNAPSHOT_COMMIT_MARKER),
        )
        .expect("commit marker should exist");

        assert!(repository
            .get_record(&snapshot_id)
            .await
            .expect("raw exact read should work")
            .is_some());
        assert!(repository
            .get_committed_record(&snapshot_id)
            .await
            .expect("committed read should work")
            .is_none());
    }

    #[tokio::test]
    async fn runtime_resolve_rejects_a_missing_commit_marker() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let cache =
            LocalArtifactCache::new(tempdir.path().join("cache"), None).expect("local cache");
        let resolver = PosixFsRuntimeResolver::new(
            tempdir.path().to_path_buf(),
            tempdir.path().join("runtime-cache"),
            test_overlaybd_layer_store(),
            cache,
        );
        let metadata = sample_metadata(SnapshotId::generate(), None);
        let snapshot = Arc::new(ready_record(metadata, CommittedSnapshot::mock()));

        let error = resolver
            .resolve(snapshot)
            .await
            .expect_err("runtime resolution must require the commit marker");
        assert!(matches!(error, RepositoryError::Unavailable { .. }));
        assert!(error.to_string().contains("commit marker"));
    }

    #[tokio::test]
    async fn resolve_rejects_missing_artifact_paths() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let cache =
            LocalArtifactCache::new(tempdir.path().join("cache"), None).expect("local cache");
        let resolver = PosixFsRuntimeResolver::new(
            tempdir.path().to_path_buf(),
            tempdir.path().join("runtime-cache"),
            test_overlaybd_layer_store(),
            cache,
        );
        let metadata = sample_metadata(SnapshotId::generate(), None);
        let committed = CommittedSnapshot {
            context: metadata.context.clone(),
            startup: metadata.startup.clone(),
            runtime_versions: metadata.runtime_versions.clone(),
            virtualization_mode: metadata.virtualization_mode,
            image_configs: metadata.image_configs.clone(),
            rootfs_layers: vec![OverlaybdLayerRef::Managed(ManagedLayer {
                digest: "sharedfs:missing".to_string(),
                size: 1,
                uuid: None,
            })],
            attached_drives: Vec::new(),
            memory_layers: Vec::new(),
            disk_publications: Vec::new(),
            legacy_artifact_namespace: None,
            custom_extension_params: None,
        };
        let snapshot_dir = tempdir
            .path()
            .join("snapshots")
            .join(metadata.id.to_string());
        fs::create_dir_all(&snapshot_dir).expect("snapshot dir");
        fs::write(
            snapshot_dir.join(super::super::layout::POSIXFS_SNAPSHOT_COMMIT_MARKER),
            b"committed",
        )
        .expect("commit marker");
        let snapshot = Arc::new(ready_record(metadata, committed));

        let err = resolver
            .resolve(snapshot)
            .await
            .expect_err("resolve should fail");
        assert!(matches!(err, RepositoryError::ArtifactNotFound { .. }));
    }

    #[tokio::test]
    async fn resolve_materializes_runtime_image_config_from_rootfs_layers() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let cache =
            LocalArtifactCache::new(tempdir.path().join("cache"), None).expect("local cache");
        let resolver = PosixFsRuntimeResolver::new(
            tempdir.path().to_path_buf(),
            tempdir.path().join("runtime-cache"),
            test_overlaybd_layer_store(),
            cache,
        );
        let layer_path = tempdir.path().join("managed-layers").join(
            super::super::layout::managed_layer_file_name("sharedfs:test"),
        );
        std::fs::create_dir_all(layer_path.parent().expect("managed-layers dir"))
            .expect("managed-layers dir");
        std::fs::write(&layer_path, b"layer").expect("layer");
        let snapshot_id = SnapshotId::generate();
        let snapshot_dir = tempdir
            .path()
            .join("snapshots")
            .join(snapshot_id.to_string());
        std::fs::create_dir_all(&snapshot_dir).expect("snapshot dir");
        std::fs::write(
            snapshot_dir.join(super::super::layout::POSIXFS_SNAPSHOT_COMMIT_MARKER),
            b"committed",
        )
        .expect("commit marker");
        std::fs::write(snapshot_dir.join("vm_state.bin"), b"vm state").expect("vm state");
        seed_committed_firecracker_manifest(tempdir.path(), &snapshot_id, 0, 32 * 1024);
        std::fs::write(
            snapshot_dir.join(SNAPSHOT_ARTIFACT_LAYOUT.memory_dump),
            b"memory",
        )
        .expect("memory");

        let metadata = sample_metadata(snapshot_id, None);
        let committed = CommittedSnapshot {
            context: metadata.context.clone(),
            startup: metadata.startup.clone(),
            runtime_versions: metadata.runtime_versions.clone(),
            virtualization_mode: metadata.virtualization_mode,
            image_configs: metadata.image_configs.clone(),
            rootfs_layers: vec![OverlaybdLayerRef::Managed(ManagedLayer {
                digest: "sharedfs:test".to_string(),
                size: 5,
                uuid: Some("11111111-2222-3333-4444-555555555555".to_string()),
            })],
            attached_drives: Vec::new(),
            memory_layers: Vec::new(),
            disk_publications: Vec::new(),
            legacy_artifact_namespace: None,
            custom_extension_params: None,
        };
        let snapshot = Arc::new(ready_record(metadata, committed));

        let runnable = resolver
            .resolve(snapshot)
            .await
            .expect("resolve should work from rootfs layers");

        let image_config: OverlaybdImageConfig = serde_json::from_slice(
            &std::fs::read(runnable.manifest().rootfs.image_config_path.as_path())
                .expect("read image config"),
        )
        .expect("parse runtime image config");
        assert_eq!(image_config.lowers.len(), 1);
        assert_eq!(
            image_config.lowers[0].file,
            layer_path.display().to_string()
        );
        assert_eq!(
            image_config.lowers[0].uuid,
            "11111111-2222-3333-4444-555555555555"
        );
    }
}
