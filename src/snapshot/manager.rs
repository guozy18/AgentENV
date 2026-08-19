use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Context;
use futures::{stream, StreamExt};
use tracing::warn;

use super::p2p::SnapshotP2pArtifact;
use super::types::SNAPSHOT_ARTIFACT_LAYOUT;
use crate::p2p::P2pTransport;
use crate::sandbox::{
    CapturedSandboxSnapshot, FirecrackerCapturedSnapshot, FirecrackerSnapshotManifest,
};
use crate::snapshot::repository::backends::{build_local_snapshot_backend, build_snapshot_backend};
use crate::snapshot::repository::interfaces::{SnapshotRepository, SnapshotRuntimeResolver};
use crate::snapshot::repository::{RepositoryError, SnapshotListFilter};
use crate::snapshot::{
    ManagedLayer, OverlaybdLayerRef, RunnableSnapshot, SnapshotAlias, SnapshotId,
    SnapshotPublishMetadata, SnapshotRecord, SnapshotSource, SnapshotSourceKind, SnapshotType,
};

/// Concurrency limit for publishing snapshot artifacts to P2P after commit.
const SNAPSHOT_P2P_PUBLISH_CONCURRENCY: usize = 8;

fn managed_layer_uuids(layers: &[OverlaybdLayerRef]) -> HashSet<String> {
    layers
        .iter()
        .filter_map(|layer| match layer {
            OverlaybdLayerRef::Managed(managed) => managed.uuid.clone(),
            OverlaybdLayerRef::External(_) => None,
        })
        .collect()
}

fn managed_layer_uuids_from_managed(layers: &[ManagedLayer]) -> HashSet<String> {
    layers
        .iter()
        .filter_map(|layer| layer.uuid.clone())
        .collect()
}

#[derive(Clone)]
struct SnapshotStore {
    repository: Arc<dyn SnapshotRepository>,
    runtime_resolver: Arc<dyn SnapshotRuntimeResolver>,
}

impl SnapshotStore {
    fn new(
        repository: Arc<dyn SnapshotRepository>,
        runtime_resolver: Arc<dyn SnapshotRuntimeResolver>,
    ) -> Self {
        Self {
            repository,
            runtime_resolver,
        }
    }

    fn matches_source(record: &SnapshotRecord, source: Option<SnapshotSourceKind>) -> bool {
        source.is_none_or(|source| record.source.kind() == source)
    }

    async fn get_matching(
        &self,
        lookup: &str,
        source: Option<SnapshotSourceKind>,
    ) -> crate::snapshot::RepositoryResult<Option<SnapshotRecord>> {
        let record = self.repository.get(lookup).await?;
        Ok(record.filter(|record| Self::matches_source(record, source)))
    }

    async fn get_by_id(
        &self,
        id: &SnapshotId,
        source: Option<SnapshotSourceKind>,
    ) -> crate::snapshot::RepositoryResult<Option<SnapshotRecord>> {
        let record = self.repository.get(&id.to_string()).await?;
        Ok(record.filter(|record| &record.id == id && Self::matches_source(record, source)))
    }

    async fn get_alias_matching(
        &self,
        alias: &str,
        source: SnapshotSourceKind,
    ) -> crate::snapshot::RepositoryResult<Option<SnapshotRecord>> {
        let Some(id) = self.repository.resolve_alias(alias).await? else {
            return Ok(None);
        };
        self.get_by_id(&id, Some(source)).await
    }
}

#[derive(Clone)]
/// Coordinates committed snapshot lifecycle operations over repository-backed state.
///
/// Durable reachability of committed snapshots is owned entirely by the
/// [`SnapshotRepository`] (PosixFS `managed-layers/`, OSS object storage, or the
/// source registry). The node-local overlaybd layer cache (`image-cache/commits/`)
/// is reclaimable - committed snapshots never pin it - so this manager records no
/// local image ref pins.
pub struct SnapshotManager {
    primary: SnapshotStore,
    local: Option<SnapshotStore>,
    p2p_transport: Option<Arc<dyn P2pTransport>>,
}

impl SnapshotManager {
    /// Builds a manager using the configured repository backend.
    pub fn new(p2p_transport: Option<Arc<dyn P2pTransport>>) -> anyhow::Result<Self> {
        let (repository, runtime_resolver) = build_snapshot_backend(p2p_transport.clone())?;
        let (local_repository, local_runtime_resolver) = build_local_snapshot_backend()?;
        Ok(Self {
            primary: SnapshotStore::new(repository, runtime_resolver),
            local: Some(SnapshotStore::new(local_repository, local_runtime_resolver)),
            p2p_transport,
        })
    }

    /// Builds a manager from the given components.
    pub fn from_parts(
        repository: Arc<dyn SnapshotRepository>,
        runtime_resolver: Arc<dyn SnapshotRuntimeResolver>,
        p2p_transport: Option<Arc<dyn P2pTransport>>,
    ) -> Self {
        Self {
            primary: SnapshotStore::new(repository, runtime_resolver),
            local: None,
            p2p_transport,
        }
    }

    pub async fn create(
        &self,
        record: SnapshotRecord,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        self.primary.repository.create(record).await
    }

    #[tracing::instrument(skip(self, metadata, manifest), fields(snapshot_id = %metadata.id))]
    pub async fn publish(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: FirecrackerSnapshotManifest,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        let record = self
            .primary
            .repository
            .publish(metadata.clone(), manifest.clone())
            .await?;
        self.publish_p2p_artifacts(&record, &manifest).await;
        Ok(record)
    }

    #[tracing::instrument(skip(self, metadata), fields(snapshot_id = %metadata.id))]
    pub async fn publish_captured(
        &self,
        metadata: SnapshotPublishMetadata,
        captured_snapshot: CapturedSandboxSnapshot,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        let manifest = captured_snapshot
            .downcast_ref::<FirecrackerCapturedSnapshot>()
            .map(|snapshot| snapshot.manifest().clone())
            .ok_or_else(|| RepositoryError::Unsupported {
                feature: "publishing captured snapshots for this sandbox backend".to_string(),
            })?;

        self.publish_captured_manifest(metadata, manifest).await
    }

    async fn publish_captured_manifest(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: FirecrackerSnapshotManifest,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        match metadata.snapshot_type {
            SnapshotType::Local => {
                let local = self
                    .local
                    .as_ref()
                    .ok_or_else(|| RepositoryError::Unsupported {
                        feature: "durable local snapshot repository is not configured".to_string(),
                    })?;
                local.repository.publish(metadata, manifest).await
            }
            SnapshotType::Distributed => {
                // Commit the node-local recovery point first. Promotion reads
                // the committed local artifacts through its resolver, so it
                // never refreezes or recaptures the live sandbox.
                let Some(local) = self.local.as_ref() else {
                    return self.publish(metadata, manifest).await;
                };
                let mut local_metadata = metadata.clone();
                // The local catalog records the availability that was
                // actually committed. A Distributed request becomes
                // Distributed only after promotion succeeds.
                local_metadata.snapshot_type = SnapshotType::Local;
                let local_record = local.repository.publish(local_metadata, manifest).await?;
                let runnable = local
                    .runtime_resolver
                    .resolve(Arc::new(local_record))
                    .await
                    .map_err(|error| {
                        RepositoryError::backend(
                            "resolve local snapshot for distributed promotion",
                            error,
                        )
                    })?;
                self.publish(metadata, runnable.manifest().clone()).await
            }
        }
    }

    /// Best effort attempt to publish snapshot artifacts to P2P.
    #[tracing::instrument(skip(self, record, manifest), fields(snapshot_id = %record.id))]
    async fn publish_p2p_artifacts(
        &self,
        record: &SnapshotRecord,
        manifest: &FirecrackerSnapshotManifest,
    ) {
        let Some(transport) = self.p2p_transport.as_ref() else {
            return;
        };
        let snapshot_id = &record.id;
        let Some(committed) = record.committed.as_ref() else {
            return;
        };

        // Prepare the manifest and VM state.
        let manifest_bytes = serde_json::to_vec(manifest).expect("manifest should serialize");
        let mut artifacts = vec![
            SnapshotP2pArtifact::fixed(
                snapshot_id,
                SNAPSHOT_ARTIFACT_LAYOUT.vm_state,
                manifest.vm_state.path.clone(),
            ),
            SnapshotP2pArtifact::bytes(
                snapshot_id,
                SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest,
                manifest_bytes,
            ),
        ];

        // Collect any overlaybd layers referenced by this snapshot's runtime images.
        let rootfs_uuids = managed_layer_uuids(&committed.rootfs_layers);
        artifacts.extend(SnapshotP2pArtifact::local_overlaybd_layers(
            &manifest.rootfs.image_config_path,
            &rootfs_uuids,
        ));
        let memory_uuids = managed_layer_uuids_from_managed(&committed.memory_layers);
        artifacts.extend(SnapshotP2pArtifact::local_overlaybd_layers(
            &manifest.memory.image_config_path,
            &memory_uuids,
        ));
        for drive in &manifest.attached_drives {
            let drive_uuids = committed
                .attached_drives
                .iter()
                .find_map(|committed_drive| match committed_drive {
                    crate::snapshot::CommittedAttachedDrive::Overlaybd {
                        drive_id, layers, ..
                    } if drive_id == &drive.drive_id => Some(managed_layer_uuids(layers)),
                    _ => None,
                })
                .unwrap_or_default();
            artifacts.extend(SnapshotP2pArtifact::local_overlaybd_layers(
                &drive.image_config_path,
                &drive_uuids,
            ));
        }

        // Publish all artifacts concurrently, but don't fail if any individual artifact fails to publish.
        stream::iter(artifacts)
            .for_each_concurrent(SNAPSHOT_P2P_PUBLISH_CONCURRENCY, |artifact| async move {
                if let Err(error) = artifact.publish(transport).await {
                    warn!(
                        key = %artifact.key,
                        source = %artifact.source,
                        error = %error,
                        "failed to publish snapshot artifact to P2P"
                    );
                }
            })
            .await;
    }

    /// Loads a snapshot record by id or alias.
    pub async fn get(
        &self,
        id_or_alias: impl AsRef<str>,
    ) -> anyhow::Result<Option<SnapshotRecord>> {
        self.get_matching(id_or_alias.as_ref(), None).await
    }

    async fn get_matching(
        &self,
        lookup: &str,
        source: Option<SnapshotSourceKind>,
    ) -> anyhow::Result<Option<SnapshotRecord>> {
        match self.primary.get_matching(lookup, source).await {
            Ok(Some(record)) => {
                let Some(direct_id) = SnapshotId::parse(lookup)
                    .ok()
                    .filter(|direct_id| direct_id != &record.id)
                else {
                    return Ok(Some(record));
                };
                let Some(local) = self.local.as_ref() else {
                    return Ok(Some(record));
                };
                let local_record = local
                    .get_matching(lookup, source)
                    .await
                    .with_context(|| format!("confirm node-local direct snapshot id '{lookup}'"))?;
                match local_record {
                    Some(local_record) if local_record.id == direct_id => Ok(Some(local_record)),
                    _ => Ok(Some(record)),
                }
            }
            Ok(None) => match self.local.as_ref() {
                Some(local) => {
                    let Some(local_record) =
                        local.get_matching(lookup, source).await.with_context(|| {
                            format!("load local snapshot '{lookup}' through repository")
                        })?
                    else {
                        return Ok(None);
                    };
                    if SnapshotId::parse(lookup).is_ok_and(|id| id == local_record.id) {
                        return Ok(Some(local_record));
                    }

                    let snapshot_id = local_record.id.clone();
                    match self.primary.get_by_id(&snapshot_id, source).await {
                        Ok(Some(record)) => Ok(Some(record)),
                        Ok(None) => Ok(Some(local_record)),
                        Err(error) => {
                            warn!(
                                snapshot = lookup,
                                %snapshot_id,
                                %error,
                                "primary repository became unavailable while confirming local alias identity"
                            );
                            Ok(Some(local_record))
                        }
                    }
                }
                None => Ok(None),
            },
            Err(primary_error) => {
                let Some(local) = self.local.as_ref() else {
                    return Err(anyhow::Error::new(primary_error)).with_context(|| {
                        format!("load committed snapshot '{lookup}' through repository")
                    });
                };
                match local.get_matching(lookup, source).await {
                    Ok(Some(record)) => {
                        if SnapshotId::parse(lookup).is_ok_and(|id| id != record.id) {
                            return Err(anyhow::Error::new(primary_error)).with_context(|| {
                                format!(
                                    "load committed snapshot '{lookup}' through repository; local fallback resolved a same-text alias while the primary direct identity is unavailable"
                                )
                            });
                        }
                        warn!(
                            snapshot = lookup,
                            error = %primary_error,
                            "primary snapshot repository unavailable; using local recovery point"
                        );
                        Ok(Some(record))
                    }
                    Ok(None) => Err(anyhow::Error::new(primary_error)).with_context(|| {
                        format!("load committed snapshot '{lookup}' through repository")
                    }),
                    Err(local_error) => Err(anyhow::Error::new(primary_error)).with_context(|| {
                        format!(
                            "load committed snapshot '{lookup}' through repository; local fallback failed: {local_error}"
                        )
                    }),
                }
            }
        }
    }

    /// Loads a sandbox-captured reusable snapshot without allowing a Template
    /// record in the primary repository to shadow a local snapshot alias.
    pub async fn get_sandbox_snapshot(
        &self,
        id_or_alias: impl AsRef<str>,
    ) -> anyhow::Result<Option<SnapshotRecord>> {
        self.get_matching(id_or_alias.as_ref(), Some(SnapshotSourceKind::Sandbox))
            .await
    }

    /// Loads a template-owned snapshot from the primary repository.
    ///
    /// Template records are never published to the node-local recovery store;
    /// a local sandbox snapshot must not mask a missing or unavailable template.
    pub async fn get_template(
        &self,
        id_or_alias: impl AsRef<str>,
    ) -> anyhow::Result<Option<SnapshotRecord>> {
        let lookup = id_or_alias.as_ref();
        self.primary
            .get_matching(lookup, Some(SnapshotSourceKind::Template))
            .await
            .with_context(|| format!("load template snapshot '{lookup}' through repository"))
    }

    /// Lists snapshot records that match the given filter.
    pub async fn list(&self, filter: SnapshotListFilter) -> anyhow::Result<Vec<SnapshotRecord>> {
        const CONTEXT: &str = "list committed snapshots through repository";
        let primary = self.primary.repository.list(filter.clone()).await;
        if filter.sources.as_deref() == Some(&[SnapshotSourceKind::Template]) {
            return primary.context(CONTEXT);
        }
        let Some(local) = self.local.as_ref() else {
            return primary.context(CONTEXT);
        };
        let local = local.repository.list(filter).await;

        match (primary, local) {
            (Ok(mut records), Ok(local_records)) => {
                let mut existing: HashSet<SnapshotId> =
                    records.iter().map(|record| record.id.clone()).collect();
                records.extend(
                    local_records
                        .into_iter()
                        .filter(|record| existing.insert(record.id.clone())),
                );
                records.sort_by(|left, right| {
                    right
                        .created_at_unix_ms
                        .cmp(&left.created_at_unix_ms)
                        .then_with(|| left.id.to_string().cmp(&right.id.to_string()))
                });
                Ok(records)
            }
            (Ok(records), Err(error)) => {
                warn!(
                    error = %error,
                    "failed to list local recovery points; returning primary snapshots"
                );
                Ok(records)
            }
            (Err(error), Ok(records)) => {
                warn!(
                    error = %error,
                    "primary snapshot repository unavailable; listing local recovery points"
                );
                Ok(records)
            }
            (Err(primary_error), Err(local_error)) => Err(anyhow::Error::new(primary_error))
                .context(format!("{CONTEXT}; local fallback failed: {local_error}")),
        }
    }

    /// Deletes a snapshot by id or alias.
    ///
    /// Returns `Ok(())` on success. The operation is idempotent:
    /// if the snapshot does not exist, it is still considered success.
    pub async fn delete(&self, id_or_alias: impl AsRef<str>) -> anyhow::Result<()> {
        let lookup = id_or_alias.as_ref();
        let Some(local_store) = self.local.as_ref() else {
            return self
                .primary
                .repository
                .delete(lookup)
                .await
                .with_context(|| format!("delete snapshot '{lookup}' through repository"));
        };
        let primary = self
            .primary
            .repository
            .get(lookup)
            .await
            .with_context(|| format!("delete snapshot '{lookup}': load primary record"))?;
        let local = local_store
            .repository
            .get(lookup)
            .await
            .with_context(|| format!("delete snapshot '{lookup}': load local record"))?;

        if let Ok(direct_id) = SnapshotId::parse(lookup) {
            if self.delete_from_stores(&direct_id, local_store).await? {
                return Ok(());
            }
        }

        let target = match (primary, local) {
            (None, None) => {
                // A failed repository delete can hide a record before all of
                // its artifacts are removed. Let each repository run its own
                // idempotent cleanup even when neither record is visible.
                self.primary
                    .repository
                    .delete(lookup)
                    .await
                    .with_context(|| format!("delete hidden snapshot '{lookup}' from primary"))?;
                local_store
                    .repository
                    .delete(lookup)
                    .await
                    .with_context(|| {
                        format!("delete hidden snapshot '{lookup}' from local repository")
                    })?;
                return Ok(());
            }
            (Some(primary), Some(local)) if primary.id != local.id => {
                return Err(anyhow::Error::new(RepositoryError::AliasConflict {
                    alias: lookup.to_string(),
                    existing: primary.id,
                    new_id: local.id,
                }))
                .with_context(|| format!("delete snapshot '{lookup}'"));
            }
            (Some(primary), _) => primary.id,
            (None, Some(local)) => local.id,
        };
        self.delete_from_stores(&target, local_store)
            .await
            .map(|_| ())
    }

    async fn delete_from_stores(
        &self,
        id: &SnapshotId,
        local: &SnapshotStore,
    ) -> anyhow::Result<bool> {
        let primary_deleted = self
            .primary
            .repository
            .delete_by_id(id)
            .await
            .with_context(|| format!("delete snapshot '{id}' from primary"))?;
        let local_deleted = local
            .repository
            .delete_by_id(id)
            .await
            .with_context(|| format!("delete snapshot '{id}' from local repository"))?;
        Ok(primary_deleted || local_deleted)
    }

    /// Deletes a template record without falling back to sandbox recovery points.
    pub async fn delete_template(&self, id_or_alias: impl AsRef<str>) -> anyhow::Result<()> {
        let lookup = id_or_alias.as_ref();
        self.primary
            .repository
            .delete_matching_source(lookup, SnapshotSourceKind::Template)
            .await
            .with_context(|| format!("delete template snapshot '{lookup}' through repository"))
            .map(|_| ())
    }

    /// Resolves an alias to its committed snapshot id.
    pub async fn resolve_committed_alias(&self, alias: &str) -> anyhow::Result<Option<SnapshotId>> {
        match self.primary.repository.resolve_alias(alias).await {
            Ok(Some(snapshot_id)) => Ok(Some(snapshot_id)),
            Ok(None) => match self.local.as_ref() {
                Some(local) => local
                    .repository
                    .resolve_alias(alias)
                    .await
                    .with_context(|| {
                        format!("resolve local snapshot alias '{alias}' through repository")
                    }),
                None => Ok(None),
            },
            Err(primary_error) => {
                let Some(local) = self.local.as_ref() else {
                    return Err(anyhow::Error::new(primary_error)).with_context(|| {
                        format!("resolve committed snapshot alias '{alias}' through repository")
                    });
                };
                match local.repository.resolve_alias(alias).await {
                    Ok(Some(snapshot_id)) => {
                        warn!(
                            alias,
                            error = %primary_error,
                            "primary snapshot repository unavailable; using local alias"
                        );
                        Ok(Some(snapshot_id))
                    }
                    Ok(None) => Err(anyhow::Error::new(primary_error)).with_context(|| {
                        format!("resolve committed snapshot alias '{alias}' through repository")
                    }),
                    Err(local_error) => Err(anyhow::Error::new(primary_error)).with_context(|| {
                        format!(
                            "resolve committed snapshot alias '{alias}' through repository; local fallback failed: {local_error}"
                        )
                    }),
                }
            }
        }
    }

    /// Resolves an alias only when it names a primary-owned Template record.
    pub async fn resolve_template_alias(&self, alias: &str) -> anyhow::Result<Option<SnapshotId>> {
        self.primary
            .get_alias_matching(alias, SnapshotSourceKind::Template)
            .await
            .with_context(|| format!("load template alias target '{alias}' through repository"))
            .map(|record| record.map(|record| record.id))
    }

    /// Resolves a committed snapshot into node-local runnable artifact paths.
    pub async fn resolve_runnable(
        &self,
        snapshot: SnapshotRecord,
    ) -> anyhow::Result<RunnableSnapshot> {
        const CONTEXT: &str = "resolve committed snapshot into runnable runtime paths";
        let Some(local) = self.local.as_ref() else {
            return self
                .primary
                .runtime_resolver
                .resolve(Arc::new(snapshot))
                .await
                .context(CONTEXT);
        };
        if snapshot.snapshot_type == SnapshotType::Local {
            return local
                .runtime_resolver
                .resolve(Arc::new(snapshot))
                .await
                .context(CONTEXT);
        }

        if matches!(&snapshot.source, SnapshotSource::Template { .. }) {
            return self
                .primary
                .runtime_resolver
                .resolve(Arc::new(snapshot))
                .await
                .context(CONTEXT);
        }

        let snapshot_id = snapshot.id.clone();
        let primary_error = match self
            .primary
            .runtime_resolver
            .resolve(Arc::new(snapshot))
            .await
        {
            Ok(runnable) => return Ok(runnable),
            Err(error) => error,
        };
        let local_record = match local
            .get_by_id(&snapshot_id, Some(SnapshotSourceKind::Sandbox))
            .await
        {
            Ok(Some(record)) => record,
            Ok(None) => return Err(anyhow::Error::new(primary_error)).context(CONTEXT),
            Err(local_error) => {
                return Err(anyhow::Error::new(primary_error)).context(format!(
                    "{CONTEXT}; load local fallback failed: {local_error}"
                ))
            }
        };
        match local.runtime_resolver.resolve(Arc::new(local_record)).await {
            Ok(runnable) => {
                warn!(
                    %snapshot_id,
                    error = %primary_error,
                    "primary snapshot runtime resolution failed; using local recovery point"
                );
                Ok(runnable)
            }
            Err(local_error) => Err(anyhow::Error::new(primary_error))
                .context(format!("{CONTEXT}; local fallback failed: {local_error}")),
        }
    }

    /// Loads a committed snapshot and immediately resolves it into runnable state.
    #[tracing::instrument(
        skip(self, id_or_alias),
        fields(snapshot_ref = %id_or_alias.as_ref())
    )]
    pub async fn load_runnable(
        &self,
        id_or_alias: impl AsRef<str>,
    ) -> anyhow::Result<Option<RunnableSnapshot>> {
        let Some(snapshot) = self.get(id_or_alias.as_ref()).await? else {
            return Ok(None);
        };
        self.resolve_runnable(snapshot).await.map(Some)
    }

    /// Loads a runnable template by alias from primary-owned template state only.
    pub async fn load_template_alias_runnable(
        &self,
        alias: &SnapshotAlias,
    ) -> anyhow::Result<Option<RunnableSnapshot>> {
        let Some(snapshot) = self
            .primary
            .get_alias_matching(alias.as_ref(), SnapshotSourceKind::Template)
            .await
            .with_context(|| {
                format!(
                    "load template alias '{}' through repository",
                    alias.as_ref()
                )
            })?
        else {
            return Ok(None);
        };
        self.primary
            .runtime_resolver
            .resolve(Arc::new(snapshot))
            .await
            .context("resolve template snapshot into runnable runtime paths")
            .map(Some)
    }

    /// Atomically transitions one template build from waiting to building.
    pub async fn try_start_build(
        &self,
        id: &SnapshotId,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        self.primary.repository.try_start_build(id).await
    }

    /// Marks one template build as failed.
    pub async fn mark_build_error(
        &self,
        id: &SnapshotId,
        reason: crate::snapshot::TemplateBuildErrorReason,
    ) -> crate::snapshot::RepositoryResult<()> {
        self.primary.repository.mark_build_error(id, reason).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::overlaybd::layer_key_from_digest;
    use crate::p2p::mock::MockTransport;
    use crate::snapshot::mock::write_mock_built_artifacts;
    use crate::snapshot::p2p::fixed_artifact_key;
    use crate::snapshot::repository::backends::{PosixFsBackend, PosixFsBackendConfig};
    use crate::snapshot::{SnapshotAlias, SnapshotId, SnapshotPublishMetadata};
    use std::path::Path;
    use tempfile::TempDir;

    fn test_store(
        root: &Path,
        name: &str,
    ) -> (
        Arc<dyn SnapshotRepository>,
        Arc<dyn SnapshotRuntimeResolver>,
    ) {
        let cache = root.join(format!("{name}-cache"));
        let backend = PosixFsBackend::new(PosixFsBackendConfig {
            root: root.join(name),
            cache_root: Some(cache.clone()),
            runtime_cache_root: Some(cache.join("runtime")),
        })
        .expect("posix backend");
        backend.into_parts()
    }

    fn test_manager(root: &Path) -> SnapshotManager {
        let (repository, runtime_resolver) = test_store(root, "repository");
        SnapshotManager::from_parts(repository, runtime_resolver, None)
    }

    fn test_manager_with_local(
        root: &Path,
        primary_name: &str,
    ) -> (
        SnapshotManager,
        Arc<dyn SnapshotRepository>,
        Arc<dyn SnapshotRepository>,
    ) {
        let (primary_repository, primary_resolver) = test_store(root, primary_name);
        let (local_repository, local_resolver) = test_store(root, "local");
        let manager = SnapshotManager {
            primary: SnapshotStore::new(Arc::clone(&primary_repository), primary_resolver),
            local: Some(SnapshotStore::new(
                Arc::clone(&local_repository),
                local_resolver,
            )),
            p2p_transport: None,
        };
        (manager, primary_repository, local_repository)
    }

    fn mock_manifest(root: &Path) -> FirecrackerSnapshotManifest {
        write_mock_built_artifacts(root)
            .expect("mock artifacts should write")
            .2
    }

    fn test_publish_metadata(
        id: SnapshotId,
        alias: Option<SnapshotAlias>,
        snapshot_type: SnapshotType,
    ) -> SnapshotPublishMetadata {
        SnapshotPublishMetadata {
            id,
            alias,
            snapshot_type,
            source: crate::snapshot::SnapshotPublishSource::Sandbox {
                source_sandbox_id: "source-sandbox".to_string(),
            },
            ..SnapshotPublishMetadata::mock()
        }
    }

    async fn seed_built_snapshot(
        manager: &SnapshotManager,
        root: &Path,
        snapshot_id: SnapshotId,
        alias: &str,
    ) {
        let metadata = SnapshotPublishMetadata {
            id: snapshot_id,
            alias: Some(SnapshotAlias::parse(alias).expect("alias should parse")),
            ..SnapshotPublishMetadata::mock()
        };
        manager
            .publish(metadata, mock_manifest(root))
            .await
            .expect("seed publish should work");
    }

    #[tokio::test]
    async fn repository_management_methods_delegate_to_committed_store() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let manager = test_manager(tempdir.path());
        let snapshot_id = SnapshotId::generate();
        seed_built_snapshot(&manager, tempdir.path(), snapshot_id.clone(), "managed").await;

        let resolved = manager
            .resolve_committed_alias("managed")
            .await
            .expect("resolve alias should work");
        assert_eq!(resolved, Some(snapshot_id.clone()));

        let loaded = manager
            .get("managed")
            .await
            .expect("load should work")
            .expect("snapshot should exist");
        assert_eq!(loaded.id, snapshot_id);

        let listed = manager
            .list(crate::snapshot::repository::SnapshotListFilter::matches_all())
            .await
            .expect("list should work");
        assert_eq!(listed.len(), 1);

        manager.delete("managed").await.expect("delete should work");
        assert!(manager
            .get("managed")
            .await
            .expect("load after delete should work")
            .is_none());
    }

    #[tokio::test]
    async fn load_runnable_uses_committed_snapshot_and_runtime_resolution() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let manager = test_manager(tempdir.path());
        let snapshot_id = SnapshotId::generate();
        seed_built_snapshot(&manager, tempdir.path(), snapshot_id.clone(), "runnable").await;

        let runnable = manager
            .load_runnable("runnable")
            .await
            .expect("load runnable should work")
            .expect("runnable snapshot should exist");

        assert_eq!(runnable.record().id, snapshot_id);
        assert!(runnable.manifest().rootfs.image_config_path.exists());
        assert!(runnable.manifest().vm_state.path.exists());
    }

    #[tokio::test]
    async fn publish_advertises_snapshot_artifacts_to_p2p_after_commit() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (repository, runtime_resolver) = test_store(tempdir.path(), "repository");
        let p2p = Arc::new(MockTransport::default());
        let manager = SnapshotManager::from_parts(repository, runtime_resolver, Some(p2p.clone()));

        let (rootfs_lower, _, manifest) =
            write_mock_built_artifacts(tempdir.path()).expect("mock artifacts should write");
        let snapshot_id = SnapshotId::generate();
        let metadata = SnapshotPublishMetadata {
            id: snapshot_id.clone(),
            ..SnapshotPublishMetadata::mock()
        };

        manager
            .publish(metadata, manifest)
            .await
            .expect("publish should commit");

        let vm_state_key = fixed_artifact_key(&snapshot_id, SNAPSHOT_ARTIFACT_LAYOUT.vm_state);
        let manifest_key =
            fixed_artifact_key(&snapshot_id, SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest);
        let rootfs_layer_digest = crate::digest::FileDigest::describe(&rootfs_lower)
            .await
            .expect("describe rootfs lower");
        let rootfs_layer_key = layer_key_from_digest(&rootfs_layer_digest.sha256);

        assert!(p2p
            .lookup(&vm_state_key)
            .await
            .expect("lookup vm state")
            .is_some());
        assert!(p2p
            .lookup(&manifest_key)
            .await
            .expect("lookup manifest")
            .is_some());
        assert!(p2p
            .lookup(&rootfs_layer_key)
            .await
            .expect("lookup rootfs layer")
            .is_some());
    }

    #[tokio::test]
    async fn local_recovery_point_remains_usable_when_primary_repository_is_unavailable() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let broken_primary_root = tempdir.path().join("broken-primary");
        std::fs::write(&broken_primary_root, b"not a repository directory")
            .expect("write broken primary root");
        let (manager, _, local_repository) =
            test_manager_with_local(tempdir.path(), "broken-primary");
        let snapshot_id = SnapshotId::generate();
        let alias = SnapshotAlias::parse("local-fallback").expect("alias should parse");
        local_repository
            .publish(
                test_publish_metadata(
                    snapshot_id.clone(),
                    Some(alias.clone()),
                    SnapshotType::Local,
                ),
                mock_manifest(tempdir.path()),
            )
            .await
            .expect("local publish should work");

        let loaded = manager
            .get(alias.as_ref())
            .await
            .expect("local fallback get should work")
            .expect("local record should exist");
        assert_eq!(loaded.id, snapshot_id);
        assert_eq!(
            manager
                .resolve_committed_alias(alias.as_ref())
                .await
                .expect("local alias fallback should work"),
            Some(snapshot_id.clone())
        );
        assert_eq!(
            manager
                .list(SnapshotListFilter::matches_all())
                .await
                .expect("local list fallback should work")
                .len(),
            1
        );
        manager
            .list(SnapshotListFilter::templates())
            .await
            .expect_err("local recovery points must not mask a primary template-list failure");
        manager
            .get_template(alias.as_ref())
            .await
            .expect_err("a local sandbox snapshot must not mask a primary template lookup failure");

        let runnable = manager
            .resolve_runnable(loaded)
            .await
            .expect("local runtime fallback should work");
        assert_eq!(runnable.record().id, snapshot_id);
        assert!(runnable.manifest().vm_state.path.exists());

        let error = manager
            .delete(alias.as_ref())
            .await
            .expect_err("delete must not report global success during a primary outage");
        assert!(
            error.to_string().contains("delete snapshot"),
            "unexpected delete error: {error:#}"
        );
        assert!(local_repository
            .get(alias.as_ref())
            .await
            .expect("local lookup after delete should work")
            .is_some());
    }

    #[tokio::test]
    async fn template_operations_ignore_local_sandbox_snapshots() -> anyhow::Result<()> {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, primary_repository, local_repository) =
            test_manager_with_local(tempdir.path(), "primary");
        let snapshot_id = SnapshotId::generate();
        let template_id = SnapshotId::generate();
        let alias = SnapshotAlias::parse("shared-alias").expect("alias should parse");
        local_repository
            .publish(
                test_publish_metadata(
                    snapshot_id.clone(),
                    Some(alias.clone()),
                    SnapshotType::Local,
                ),
                mock_manifest(&tempdir.path().join("local-artifacts")),
            )
            .await
            .expect("local snapshot publish should work");
        primary_repository
            .publish(
                SnapshotPublishMetadata {
                    id: template_id.clone(),
                    alias: Some(alias.clone()),
                    ..SnapshotPublishMetadata::mock()
                },
                mock_manifest(&tempdir.path().join("template-artifacts")),
            )
            .await
            .expect("template publish should work");

        assert_eq!(
            manager
                .get_template(alias.as_ref())
                .await?
                .expect("template should exist")
                .id,
            template_id
        );
        assert_eq!(
            manager
                .get_sandbox_snapshot(alias.as_ref())
                .await?
                .expect("local sandbox snapshot should exist")
                .id,
            snapshot_id
        );
        assert_eq!(
            manager
                .load_template_alias_runnable(&alias)
                .await?
                .expect("template should be runnable")
                .record()
                .id,
            template_id
        );
        std::fs::remove_file(
            tempdir
                .path()
                .join("primary/snapshots")
                .join(template_id.to_string())
                .join("commit"),
        )
        .expect("simulate an interrupted template delete");
        manager.delete_template(alias.as_ref()).await?;
        assert!(manager
            .resolve_template_alias(alias.as_ref())
            .await?
            .is_none());
        assert!(!tempdir
            .path()
            .join("primary/catalog/records")
            .join(format!("{template_id}.json"))
            .exists());
        assert!(primary_repository
            .get(&template_id.to_string())
            .await?
            .is_none());
        assert!(local_repository
            .get(&snapshot_id.to_string())
            .await?
            .is_some());
        Ok(())
    }

    #[tokio::test]
    async fn template_alias_lookup_ignores_same_text_sandbox_id() -> anyhow::Result<()> {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (repository, runtime_resolver) = test_store(tempdir.path(), "primary");
        let manager = SnapshotManager::from_parts(Arc::clone(&repository), runtime_resolver, None);
        let sandbox_id = SnapshotId::generate();
        let template_id = SnapshotId::generate();
        let alias = SnapshotAlias::parse(&sandbox_id.to_string())
            .expect("UUID-shaped template alias should parse");

        repository
            .publish(
                test_publish_metadata(sandbox_id.clone(), None, SnapshotType::Distributed),
                mock_manifest(&tempdir.path().join("sandbox-artifacts")),
            )
            .await?;
        repository
            .publish(
                SnapshotPublishMetadata {
                    id: template_id.clone(),
                    alias: Some(alias.clone()),
                    ..SnapshotPublishMetadata::mock()
                },
                mock_manifest(&tempdir.path().join("template-artifacts")),
            )
            .await?;

        assert_eq!(
            manager
                .load_template_alias_runnable(&alias)
                .await?
                .expect("template alias should remain runnable")
                .record()
                .id,
            template_id
        );
        assert_eq!(
            manager
                .get_sandbox_snapshot(alias.as_ref())
                .await?
                .expect("same-text sandbox id should remain addressable")
                .id,
            sandbox_id
        );

        manager.delete_template(alias.as_ref()).await?;
        assert!(repository.get(&sandbox_id.to_string()).await?.is_some());
        assert!(repository.get(&template_id.to_string()).await?.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn delete_rejects_aliases_that_resolve_to_different_store_identities() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, primary_repository, local_repository) =
            test_manager_with_local(tempdir.path(), "primary");
        let alias = SnapshotAlias::parse("ambiguous-delete").expect("alias should parse");
        let primary_id = SnapshotId::generate();
        let local_id = SnapshotId::generate();

        for (repository, id, snapshot_type, artifacts) in [
            (
                &primary_repository,
                &primary_id,
                SnapshotType::Distributed,
                "primary-artifacts",
            ),
            (
                &local_repository,
                &local_id,
                SnapshotType::Local,
                "local-artifacts",
            ),
        ] {
            repository
                .publish(
                    test_publish_metadata(id.clone(), Some(alias.clone()), snapshot_type),
                    mock_manifest(&tempdir.path().join(artifacts)),
                )
                .await
                .expect("test snapshot publish should work");
        }

        manager
            .delete(alias.as_ref())
            .await
            .expect_err("an ambiguous cross-store alias must not delete either snapshot");
        for (repository, id) in [
            (&primary_repository, &primary_id),
            (&local_repository, &local_id),
        ] {
            assert!(repository
                .get(&id.to_string())
                .await
                .expect("snapshot lookup should work")
                .is_some());
        }
    }

    #[tokio::test]
    async fn delete_hidden_direct_id_ignores_same_text_alias_for_another_snapshot() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, primary_repository, local_repository) =
            test_manager_with_local(tempdir.path(), "primary");
        let direct_id = SnapshotId::generate();
        let local_id = SnapshotId::generate();
        let uuid_alias =
            SnapshotAlias::parse(&direct_id.to_string()).expect("UUID-shaped alias should parse");

        primary_repository
            .publish(
                test_publish_metadata(direct_id.clone(), None, SnapshotType::Distributed),
                mock_manifest(&tempdir.path().join("primary-artifacts")),
            )
            .await
            .expect("primary snapshot publish should work");
        local_repository
            .publish(
                test_publish_metadata(
                    local_id.clone(),
                    Some(uuid_alias.clone()),
                    SnapshotType::Local,
                ),
                mock_manifest(&tempdir.path().join("local-artifacts")),
            )
            .await
            .expect("local snapshot publish should work");
        std::fs::remove_file(
            tempdir
                .path()
                .join("primary/snapshots")
                .join(direct_id.to_string())
                .join("commit"),
        )
        .expect("simulate delete interrupted after hiding the direct record");

        manager
            .delete(direct_id.to_string())
            .await
            .expect("hidden direct id should take precedence over a same-text alias");

        assert!(primary_repository
            .get(&direct_id.to_string())
            .await
            .expect("primary lookup should work")
            .is_none());
        assert!(!tempdir
            .path()
            .join("primary/catalog/records")
            .join(format!("{direct_id}.json"))
            .exists());
        assert_eq!(
            local_repository
                .get(uuid_alias.as_ref())
                .await
                .expect("local alias lookup should work")
                .expect("same-text alias target must remain")
                .id,
            local_id
        );
    }

    #[tokio::test]
    async fn get_direct_id_precedes_same_text_primary_alias() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, primary_repository, local_repository) =
            test_manager_with_local(tempdir.path(), "primary");
        let local_id = SnapshotId::generate();
        let primary_id = SnapshotId::generate();
        let uuid_alias =
            SnapshotAlias::parse(&local_id.to_string()).expect("UUID-shaped alias should parse");

        primary_repository
            .publish(
                test_publish_metadata(primary_id, Some(uuid_alias), SnapshotType::Distributed),
                mock_manifest(&tempdir.path().join("primary-artifacts")),
            )
            .await
            .expect("primary alias publish should work");
        local_repository
            .publish(
                test_publish_metadata(local_id.clone(), None, SnapshotType::Local),
                mock_manifest(&tempdir.path().join("local-artifacts")),
            )
            .await
            .expect("local direct snapshot publish should work");

        let loaded = manager
            .get(local_id.to_string())
            .await
            .expect("global lookup should work")
            .expect("direct snapshot should exist");
        assert_eq!(loaded.id, local_id);
        assert_eq!(loaded.snapshot_type, SnapshotType::Local);
    }

    #[tokio::test]
    async fn get_does_not_treat_a_target_id_as_a_primary_alias() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, primary_repository, local_repository) =
            test_manager_with_local(tempdir.path(), "primary");
        let local_id = SnapshotId::generate();
        let unrelated_primary_id = SnapshotId::generate();
        let lookup_alias = SnapshotAlias::parse("local-only-alias").expect("alias should parse");
        let same_text_alias =
            SnapshotAlias::parse(&local_id.to_string()).expect("UUID-shaped alias should parse");

        local_repository
            .publish(
                test_publish_metadata(
                    local_id.clone(),
                    Some(lookup_alias.clone()),
                    SnapshotType::Local,
                ),
                mock_manifest(&tempdir.path().join("local-artifacts")),
            )
            .await
            .expect("local snapshot publish should work");
        primary_repository
            .publish(
                test_publish_metadata(
                    unrelated_primary_id,
                    Some(same_text_alias),
                    SnapshotType::Distributed,
                ),
                mock_manifest(&tempdir.path().join("primary-artifacts")),
            )
            .await
            .expect("primary alias publish should work");

        let loaded = manager
            .get(lookup_alias.as_ref())
            .await
            .expect("global lookup should work")
            .expect("local snapshot should exist");
        assert_eq!(loaded.id, local_id);
        assert_eq!(loaded.snapshot_type, SnapshotType::Local);
    }

    #[tokio::test]
    async fn delete_does_not_treat_a_target_id_as_an_alias_in_another_store() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, primary_repository, local_repository) =
            test_manager_with_local(tempdir.path(), "primary");
        let target_id = SnapshotId::generate();
        let unrelated_local_id = SnapshotId::generate();
        let lookup_alias = SnapshotAlias::parse("delete-target").expect("alias should parse");
        let same_text_alias =
            SnapshotAlias::parse(&target_id.to_string()).expect("UUID-shaped alias should parse");

        primary_repository
            .publish(
                test_publish_metadata(
                    target_id.clone(),
                    Some(lookup_alias.clone()),
                    SnapshotType::Distributed,
                ),
                mock_manifest(&tempdir.path().join("primary-artifacts")),
            )
            .await
            .expect("primary snapshot publish should work");
        local_repository
            .publish(
                test_publish_metadata(
                    unrelated_local_id.clone(),
                    Some(same_text_alias.clone()),
                    SnapshotType::Local,
                ),
                mock_manifest(&tempdir.path().join("local-artifacts")),
            )
            .await
            .expect("local alias publish should work");

        manager
            .delete(lookup_alias.as_ref())
            .await
            .expect("resolved identity should be deleted exactly");

        assert!(primary_repository
            .get(&target_id.to_string())
            .await
            .expect("primary lookup should work")
            .is_none());
        assert_eq!(
            local_repository
                .get(same_text_alias.as_ref())
                .await
                .expect("local alias lookup should work")
                .expect("unrelated local snapshot must remain")
                .id,
            unrelated_local_id
        );
    }

    #[tokio::test]
    async fn get_does_not_substitute_uuid_alias_during_primary_outage() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let broken_primary_root = tempdir.path().join("broken-primary");
        std::fs::write(&broken_primary_root, b"not a repository directory")
            .expect("write broken primary root");
        let (manager, _, local_repository) =
            test_manager_with_local(tempdir.path(), "broken-primary");
        let direct_id = SnapshotId::generate();
        let local_id = SnapshotId::generate();
        let uuid_alias =
            SnapshotAlias::parse(&direct_id.to_string()).expect("UUID-shaped alias should parse");
        local_repository
            .publish(
                test_publish_metadata(local_id.clone(), Some(uuid_alias), SnapshotType::Local),
                mock_manifest(&tempdir.path().join("local-artifacts")),
            )
            .await
            .expect("local alias publish should work");

        manager
            .get(direct_id.to_string())
            .await
            .expect_err("an alias must not replace an unavailable primary direct identity");
        assert_eq!(
            local_repository
                .get(&direct_id.to_string())
                .await
                .expect("local alias lookup should work")
                .expect("local alias target should remain")
                .id,
            local_id
        );
    }

    #[tokio::test]
    async fn delete_retries_cleanup_for_hidden_local_record() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, _, local_repository) = test_manager_with_local(tempdir.path(), "primary");
        let snapshot_id = SnapshotId::generate();
        let alias = SnapshotAlias::parse("hidden-local-delete").expect("alias should parse");
        local_repository
            .publish(
                test_publish_metadata(
                    snapshot_id.clone(),
                    Some(alias.clone()),
                    SnapshotType::Local,
                ),
                mock_manifest(tempdir.path()),
            )
            .await
            .expect("local publish should work");

        let snapshot_dir = tempdir
            .path()
            .join("local/snapshots")
            .join(snapshot_id.to_string());
        std::fs::remove_file(snapshot_dir.join("commit"))
            .expect("simulate interrupted local delete");

        manager
            .delete(alias.as_ref())
            .await
            .expect("manager retry should finish repository cleanup");

        assert!(!snapshot_dir.exists());
        assert!(!tempdir
            .path()
            .join("local/catalog/records")
            .join(format!("{snapshot_id}.json"))
            .exists());
    }

    #[tokio::test]
    async fn captured_local_publish_does_not_touch_primary_repository() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let primary_root = tempdir.path().join("primary-file");
        std::fs::write(&primary_root, b"primary sentinel").expect("write primary sentinel");
        let (manager, _, _) = test_manager_with_local(tempdir.path(), "primary-file");
        let snapshot_id = SnapshotId::generate();
        let record = manager
            .publish_captured_manifest(
                test_publish_metadata(snapshot_id.clone(), None, SnapshotType::Local),
                mock_manifest(tempdir.path()),
            )
            .await
            .expect("local captured publish should work");

        assert_eq!(record.id, snapshot_id);
        assert_eq!(record.snapshot_type, SnapshotType::Local);
        assert_eq!(
            std::fs::read(&primary_root).expect("read primary sentinel"),
            b"primary sentinel"
        );
    }

    #[tokio::test]
    async fn distributed_capture_keeps_local_record_when_promotion_fails() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let primary_root = tempdir.path().join("primary-file");
        std::fs::write(&primary_root, b"primary sentinel").expect("write primary sentinel");
        let (manager, _, local_repository) =
            test_manager_with_local(tempdir.path(), "primary-file");
        let snapshot_id = SnapshotId::generate();
        let error = manager
            .publish_captured_manifest(
                test_publish_metadata(snapshot_id.clone(), None, SnapshotType::Distributed),
                mock_manifest(tempdir.path()),
            )
            .await
            .expect_err("promotion should fail against an invalid primary root");
        assert!(error.to_string().contains("create catalog dir"));

        let local_record = local_repository
            .get(&snapshot_id.to_string())
            .await
            .expect("local lookup should work")
            .expect("local record should remain after promotion failure");
        assert_eq!(local_record.snapshot_type, SnapshotType::Local);
    }

    #[tokio::test]
    async fn distributed_capture_promotes_same_id_after_local_commit() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, primary_repository, local_repository) =
            test_manager_with_local(tempdir.path(), "primary");
        let snapshot_id = SnapshotId::generate();
        let promoted = manager
            .publish_captured_manifest(
                test_publish_metadata(snapshot_id.clone(), None, SnapshotType::Distributed),
                mock_manifest(tempdir.path()),
            )
            .await
            .expect("distributed promotion should work");

        assert_eq!(promoted.id, snapshot_id);
        assert_eq!(promoted.snapshot_type, SnapshotType::Distributed);
        assert_eq!(
            local_repository
                .get(&snapshot_id.to_string())
                .await
                .expect("local lookup should work")
                .expect("local record should exist")
                .snapshot_type,
            SnapshotType::Local
        );
        assert_eq!(
            primary_repository
                .get(&snapshot_id.to_string())
                .await
                .expect("primary lookup should work")
                .expect("primary record should exist")
                .snapshot_type,
            SnapshotType::Distributed
        );

        std::fs::remove_file(
            tempdir
                .path()
                .join("primary/snapshots")
                .join(snapshot_id.to_string())
                .join(SNAPSHOT_ARTIFACT_LAYOUT.vm_state),
        )
        .expect("primary vm state should exist");
        let recovered = manager
            .load_runnable(snapshot_id.to_string())
            .await
            .expect("runtime resolution should use the local recovery point")
            .expect("snapshot should remain resolvable");
        assert_eq!(recovered.record().id, snapshot_id);
        assert_eq!(recovered.record().snapshot_type, SnapshotType::Local);

        manager
            .delete(snapshot_id.to_string())
            .await
            .expect("delete should remove both copies of the same snapshot");
        for repository in [&primary_repository, &local_repository] {
            assert!(repository
                .get(&snapshot_id.to_string())
                .await
                .expect("snapshot lookup after delete should work")
                .is_none());
        }
    }

    #[tokio::test]
    async fn uuid_shaped_local_alias_recovers_incomplete_primary_bind() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, primary_repository, local_repository) =
            test_manager_with_local(tempdir.path(), "primary");
        let snapshot_id = SnapshotId::generate();
        let alias = SnapshotAlias::parse(&SnapshotId::generate().to_string())
            .expect("UUID-shaped alias should parse");

        manager
            .publish_captured_manifest(
                test_publish_metadata(
                    snapshot_id.clone(),
                    Some(alias.clone()),
                    SnapshotType::Distributed,
                ),
                mock_manifest(tempdir.path()),
            )
            .await
            .expect("distributed promotion should work");
        std::fs::remove_file(
            tempdir
                .path()
                .join("primary/catalog/aliases")
                .join(alias.as_ref()),
        )
        .expect("primary alias should exist");

        let recovered = manager
            .get(alias.as_ref())
            .await
            .expect("alias recovery should work")
            .expect("same-id primary record should be recovered");
        assert_eq!(recovered.id, snapshot_id);
        assert_eq!(recovered.snapshot_type, SnapshotType::Distributed);

        manager
            .delete(alias.as_ref())
            .await
            .expect("recovered alias should delete both copies");
        for repository in [&primary_repository, &local_repository] {
            assert!(repository
                .get(&snapshot_id.to_string())
                .await
                .expect("snapshot lookup after delete should work")
                .is_none());
        }
    }
}
