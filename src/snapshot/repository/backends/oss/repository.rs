use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use futures::{stream, StreamExt, TryStreamExt};
use overlaybd::config::{load_image_config as load_overlaybd_image_config, LayerConfig};
use overlaybd::dense_export;
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::client::{OssClient, OssUploadArtifact};
use super::layout::OssSnapshotArtifactLayout;
use crate::cfg::SnapshotImageStoragePolicy;
use crate::sandbox::FirecrackerSnapshotManifest;
use crate::snapshot::repository::backends::common::acr::{
    AcrDiskImageExporter, DiskImageExportOutcome, DiskImageSubject, SnapshotOciConfigInput,
};
use crate::snapshot::repository::backends::common::{
    overlaybd_layer_uuid, validate_attached_drives, write_dense_overlaybd_layer_to_file,
};
use crate::snapshot::repository::interfaces::SnapshotRepository;
use crate::snapshot::repository::{
    validate_legacy_artifact_namespace, RepositoryError, RepositoryResult,
};
use crate::snapshot::types::now_unix_ms;
use crate::snapshot::{
    CommittedAttachedDrive, CommittedSnapshot, ExternalLayer, ManagedLayer, OverlaybdLayerRef,
    PersistedDiskImagePublication, SnapshotAlias, SnapshotId, SnapshotLifecycle,
    SnapshotListFilter, SnapshotPublishMetadata, SnapshotRecord, SnapshotSource, SnapshotType,
    TemplateBuildErrorReason, SNAPSHOT_ARTIFACT_LAYOUT,
};

/// Manages the committed‐state layer of the OSS snapshot repository.
///
/// Object layout under the configured prefix:
///
/// ```text
/// catalog/aliases/{name}.json              → "snapshot-id"
/// artifacts/{id}/firecracker-manifest.json → FirecrackerSnapshotManifest (paths omitted)
/// artifacts/{id}/vm_state.bin
/// managed-layers/{digest}
/// ```
pub(crate) struct OssSnapshotRepository {
    client: Arc<OssClient>,
    snapshot_image_storage: SnapshotImageStoragePolicy,
    acr_exporter: AcrDiskImageExporter,
}

const MAX_ALIAS_BIND_ATTEMPTS: usize = 5;

#[derive(Clone, Debug)]
struct StoredRecord {
    record: SnapshotRecord,
    etag: String,
}

#[derive(Clone, Debug)]
struct StoredAlias {
    target: SnapshotId,
    etag: String,
}

impl OssSnapshotRepository {
    pub(crate) fn new(
        client: Arc<OssClient>,
        snapshot_image_storage: SnapshotImageStoragePolicy,
    ) -> Self {
        Self {
            client,
            snapshot_image_storage,
            acr_exporter: AcrDiskImageExporter::new(),
        }
    }

    fn layout<'a>(&self, id: &'a SnapshotId) -> OssSnapshotArtifactLayout<'a> {
        OssSnapshotArtifactLayout::new(id)
    }

    fn layout_for_record<'a>(
        &self,
        record: &'a SnapshotRecord,
    ) -> RepositoryResult<OssSnapshotArtifactLayout<'a>> {
        match record
            .committed
            .as_ref()
            .and_then(|committed| committed.legacy_artifact_namespace.as_deref())
        {
            Some(namespace) => {
                validate_legacy_artifact_namespace(Some(namespace))?;
                Ok(self.layout(&record.id).with_legacy_namespace(namespace))
            }
            None => Ok(self.layout(&record.id)),
        }
    }
}

fn validated_alias_key(alias: &str) -> RepositoryResult<String> {
    SnapshotAlias::parse(alias).map_err(|e| RepositoryError::InvalidRequest {
        reason: format!("invalid alias '{alias}': {e}"),
    })?;
    Ok(OssSnapshotArtifactLayout::alias_key(alias))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MetadataCommitPlan {
    Create,
    Update,
    AlreadyCommitted,
}

fn ready_distributed_publish_retry(
    existing: Option<&SnapshotRecord>,
    metadata: &SnapshotPublishMetadata,
) -> RepositoryResult<Option<SnapshotRecord>> {
    let Some(existing) = existing.filter(|record| {
        record.is_ready()
            && record.snapshot_type == SnapshotType::Distributed
            && record.committed.is_some()
    }) else {
        return Ok(None);
    };
    if existing.same_publish_identity(metadata) {
        return Ok(Some(existing.clone()));
    }
    Err(RepositoryError::InvalidRequest {
        reason: format!(
            "snapshot '{}' is already Ready with different metadata",
            existing.id
        ),
    })
}

/// Validates an existing catalog identity before any per-snapshot object is
/// uploaded.  A retry must not import a different closure before this check:
/// fixed keys are safe only after the existing logical identity matches.
fn validate_publish_preflight(
    existing: Option<&SnapshotRecord>,
    metadata: &SnapshotPublishMetadata,
) -> RepositoryResult<()> {
    let Some(existing) = existing else {
        return Ok(());
    };

    if existing.lifecycle == SnapshotLifecycle::Deleting {
        return Err(RepositoryError::ConcurrentModification {
            resource: format!("snapshot '{}' is being deleted", metadata.id),
        });
    }

    if existing.committed.is_none() {
        if !existing.matches_pending_template(metadata) {
            return Err(RepositoryError::InvalidRequest {
                reason: format!(
                    "snapshot '{}' has different pending template metadata",
                    metadata.id
                ),
            });
        }
        return Ok(());
    }

    if existing.lifecycle == SnapshotLifecycle::Preparing {
        if !existing.same_publish_identity(metadata) {
            return Err(RepositoryError::InvalidRequest {
                reason: format!(
                    "snapshot '{}' has different Preparing metadata",
                    metadata.id
                ),
            });
        }
        return Ok(());
    }

    if existing.is_ready()
        && existing.snapshot_type == SnapshotType::Local
        && metadata.snapshot_type == SnapshotType::Distributed
    {
        let identity_matches =
            existing.id == metadata.id && existing.matches_publish_metadata(metadata);
        if existing.owner_node_id.is_none() || metadata.owner_node_id.is_some() || !identity_matches
        {
            return Err(RepositoryError::InvalidRequest {
                reason: format!(
                    "snapshot '{}' Local metadata does not match the promotion identity",
                    metadata.id
                ),
            });
        }
    }

    Ok(())
}

fn metadata_commit_plan(
    existing: Option<&SnapshotRecord>,
    requested: &SnapshotRecord,
) -> RepositoryResult<MetadataCommitPlan> {
    if requested.lifecycle != SnapshotLifecycle::Ready {
        return Err(RepositoryError::InvalidRequest {
            reason: format!(
                "snapshot '{}' metadata commit must request Ready lifecycle",
                requested.id
            ),
        });
    }
    requested
        .validate_committed_metadata()
        .map_err(|reason| RepositoryError::InvalidRequest { reason })?;

    let Some(existing) = existing else {
        return Ok(MetadataCommitPlan::Create);
    };
    if existing.id != requested.id {
        return Err(RepositoryError::InvalidRequest {
            reason: format!(
                "snapshot metadata id mismatch: existing '{}', requested '{}'",
                existing.id, requested.id
            ),
        });
    }

    if existing.lifecycle == SnapshotLifecycle::Deleting {
        return Err(RepositoryError::InvalidRequest {
            reason: format!(
                "snapshot '{}' is being deleted and cannot be committed",
                requested.id
            ),
        });
    }

    // An equivalent Ready record is an acknowledged or ambiguous-response
    // retry. The persisted canonical record wins.
    if existing.lifecycle == SnapshotLifecycle::Ready
        && existing.same_logical_identity(requested)
        && existing.same_committed_logical_metadata(requested)
    {
        return Ok(MetadataCommitPlan::AlreadyCommitted);
    }

    if existing.same_catalog_contents(requested) {
        return Ok(if existing.lifecycle == SnapshotLifecycle::Preparing {
            MetadataCommitPlan::Update
        } else {
            MetadataCommitPlan::AlreadyCommitted
        });
    }

    if existing.lifecycle == SnapshotLifecycle::Preparing {
        return Err(RepositoryError::InvalidRequest {
            reason: format!(
                "snapshot '{}' has different Preparing metadata",
                requested.id
            ),
        });
    }

    // Template CPU/memory are requested up front; disk size remains zero
    // until the builder measures the completed rootfs.
    let template_resources_match = existing.resources.cpu_count == requested.resources.cpu_count
        && existing.resources.memory_mib == requested.resources.memory_mib
        && (existing.resources.disk_size_mib == 0
            || existing.resources.disk_size_mib == requested.resources.disk_size_mib);
    let completes_template = existing.committed.is_none()
        && matches!(existing.source, SnapshotSource::Template { .. })
        && matches!(requested.source, SnapshotSource::Template { .. })
        && existing.snapshot_type == SnapshotType::Distributed
        && requested.snapshot_type == SnapshotType::Distributed
        && existing.owner_node_id.is_none()
        && requested.owner_node_id.is_none()
        && existing.id == requested.id
        && existing.alias == requested.alias
        && existing.created_at_unix_ms == requested.created_at_unix_ms
        && template_resources_match;
    if completes_template {
        return Ok(MetadataCommitPlan::Update);
    }

    let promotes_local = existing.is_ready()
        && existing.snapshot_type == SnapshotType::Local
        && requested.snapshot_type == SnapshotType::Distributed
        && existing.owner_node_id.is_some()
        && requested.owner_node_id.is_none()
        && existing.same_stable_identity(requested);
    let promotes_local = promotes_local && existing.same_committed_logical_metadata(requested);
    if promotes_local {
        return Ok(MetadataCommitPlan::Update);
    }

    Err(RepositoryError::InvalidRequest {
        reason: format!(
            "snapshot '{}' is already Ready with different metadata",
            requested.id
        ),
    })
}

fn preparing_record(record: &SnapshotRecord) -> SnapshotRecord {
    let mut preparing = record.clone();
    preparing.lifecycle = SnapshotLifecycle::Preparing;
    preparing
}

fn same_repo_blob_url(left: &str, right: &str) -> bool {
    !left.is_empty() && left.trim_end_matches('/') == right.trim_end_matches('/')
}

fn canonicalize_managed_layer(source: &Path) -> RepositoryResult<PathBuf> {
    std::fs::canonicalize(source).map_err(|error| {
        RepositoryError::backend(
            format!("canonicalize managed layer '{}'", source.display()),
            error,
        )
    })
}

fn memory_layer_digest(index: usize, layer: &LayerConfig) -> RepositoryResult<&str> {
    let digest = if !layer.digest.is_empty() {
        &layer.digest
    } else if !layer.target_digest.is_empty() {
        &layer.target_digest
    } else {
        return Err(RepositoryError::Unsupported {
            feature: format!("memory layer {index} without digest"),
        });
    };
    Ok(digest)
}

async fn verify_staged_memory_layer(
    path: &Path,
    digest: &str,
    expected_size: u64,
) -> RepositoryResult<u64> {
    let actual = crate::digest::FileDigest::describe(path)
        .await
        .map_err(|error| {
            RepositoryError::backend(
                format!("describe staged memory layer '{}'", path.display()),
                error,
            )
        })?;
    if actual.sha256 != digest {
        return Err(RepositoryError::IntegrityMismatch {
            artifact: format!("staged memory layer '{}'", path.display()),
            expected: digest.to_string(),
            actual: actual.sha256,
        });
    }
    if expected_size != 0 && actual.size != expected_size {
        return Err(RepositoryError::IntegrityMismatch {
            artifact: format!("staged memory layer '{}'", path.display()),
            expected: expected_size.to_string(),
            actual: actual.size.to_string(),
        });
    }
    Ok(actual.size)
}

async fn stage_memory_layer(
    client: &OssClient,
    capture_dir: &Path,
    layer: &mut LayerConfig,
    index: usize,
) -> RepositoryResult<()> {
    let digest = memory_layer_digest(index, layer)?.to_owned();
    let expected_size = layer.size;
    let destination =
        crate::image::commit_index::commit_file(&capture_dir.join("local-memory-layers"), &digest);
    let staging_dir = destination
        .parent()
        .expect("staged memory layer path has a parent");
    tokio::fs::create_dir_all(staging_dir)
        .await
        .map_err(|error| {
            RepositoryError::backend(
                format!(
                    "create captured memory layer dir '{}'",
                    staging_dir.display()
                ),
                error,
            )
        })?;
    let destination_exists = tokio::fs::try_exists(&destination).await.map_err(|error| {
        RepositoryError::backend(
            format!("check staged memory layer '{}'", destination.display()),
            error,
        )
    })?;
    if !destination_exists {
        let source = (!layer.file.is_empty()).then(|| PathBuf::from(&layer.file));
        if let Some(source) = source.filter(|path| path.is_file()) {
            let temporary = destination.with_extension(format!("tmp-{}", Uuid::now_v7()));
            tokio::fs::copy(&source, &temporary)
                .await
                .map_err(|error| {
                    RepositoryError::backend(
                        format!(
                            "copy captured memory layer '{}' into local staging",
                            source.display()
                        ),
                        error,
                    )
                })?;
            tokio::fs::rename(&temporary, &destination)
                .await
                .map_err(|error| {
                    RepositoryError::backend(
                        format!("commit staged memory layer '{}'", destination.display()),
                        error,
                    )
                })?;
        } else {
            client
                .get_to_file(
                    &OssSnapshotArtifactLayout::managed_layer_key(&digest),
                    &destination,
                )
                .await
                .map_err(|error| {
                    if OssClient::is_not_found_error(&error) {
                        RepositoryError::ManagedLayerNotFound {
                            digest: digest.clone(),
                        }
                    } else {
                        RepositoryError::backend(
                            format!("download captured memory layer '{digest}'"),
                            error,
                        )
                    }
                })?;
        }
    }
    let size = verify_staged_memory_layer(&destination, &digest, expected_size).await?;
    layer.file = destination.display().to_string();
    layer.dir.clear();
    layer.repo_blob_url.clear();
    layer.digest = digest;
    layer.size = size;
    Ok(())
}

fn fallback_to_object_storage_would_mix_sources(
    image_config_path: &Path,
    managed_layers_repo_blob_url: &str,
) -> RepositoryResult<bool> {
    let image_config = load_overlaybd_image_config(image_config_path).map_err(|e| {
        RepositoryError::backend(
            format!(
                "load overlaybd image config '{}'",
                image_config_path.display()
            ),
            e,
        )
    })?;
    Ok(image_config.lowers.iter().any(|layer| {
        layer.file.is_empty()
            && !same_repo_blob_url(
                layer.effective_repo_blob_url(&image_config.repo_blob_url),
                managed_layers_repo_blob_url,
            )
    }))
}

// ── SnapshotRepository impl ────────────────────────────────────────────

#[async_trait]
impl SnapshotRepository for OssSnapshotRepository {
    async fn create(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        record
            .validate_template_create()
            .map_err(|reason| RepositoryError::InvalidRequest { reason })?;
        let existing = self.read_record_state(&record.id).await?;
        let stored = if let Some(existing) = existing {
            if existing.record.lifecycle == SnapshotLifecycle::Deleting {
                return Err(RepositoryError::ConcurrentModification {
                    resource: format!("snapshot '{}' is being deleted", record.id),
                });
            }
            if !existing.record.same_catalog_contents(&record) {
                return Err(RepositoryError::InvalidRequest {
                    reason: format!(
                        "snapshot '{}' already exists with different metadata",
                        record.id
                    ),
                });
            }
            if existing.record.is_ready() {
                self.bind_record_alias(&record).await?;
                return Ok(existing.record);
            }
            existing
        } else if record.alias.is_none() {
            self.write_record(&record, None).await?;
            return Ok(record);
        } else {
            let preparing = preparing_record(&record);
            let etag = self.write_record(&preparing, None).await?;
            StoredRecord {
                record: preparing,
                etag,
            }
        };

        let Some(alias) = record.alias.as_ref() else {
            return Err(RepositoryError::InvalidRequest {
                reason: format!(
                    "snapshot '{}' template create reservation is missing its alias",
                    record.id
                ),
            });
        };
        // Keep the hidden Preparing record for an exact retry. The alias
        // conflict may be transient (the current owner can be deleted later).
        self.bind_alias(alias.as_ref(), &record.id).await?;

        self.write_ready_record(record, Some(&stored.etag)).await
    }

    async fn prepare_local_capture(
        &self,
        manifest: &mut FirecrackerSnapshotManifest,
    ) -> RepositoryResult<()> {
        let config_path = &manifest.memory.image_config_path;
        let mut image_config = load_overlaybd_image_config(config_path).map_err(|error| {
            RepositoryError::backend(
                format!(
                    "load captured memory image config '{}'",
                    config_path.display()
                ),
                error,
            )
        })?;
        let managed_repo_blob_url = self.client.managed_layers_repo_blob_url();
        let image_repo_blob_url = image_config.repo_blob_url.clone();
        let capture_dir = config_path
            .parent()
            .ok_or_else(|| RepositoryError::Backend {
                message: format!(
                    "resolve parent dir for memory image config '{}'",
                    config_path.display()
                ),
                source: None,
            })?
            .join(format!("local-capture-{}", Uuid::now_v7()));
        let local_config_path = capture_dir.join("memory.json");
        let mut staged_any = false;

        for (index, layer) in image_config.lowers.iter_mut().enumerate() {
            let has_local_file = !layer.file.is_empty() && Path::new(&layer.file).is_file();
            // The newest capture delta is local and intentionally descriptorless;
            // do not interpret the inherited image-level managed URL as its
            // source. POSIX import will hash this file during local commit.
            if has_local_file && (layer.digest.is_empty() || layer.size == 0) {
                continue;
            }
            let repo_blob_url = layer
                .effective_repo_blob_url(&image_repo_blob_url)
                .to_string();
            if !repo_blob_url.is_empty()
                && same_repo_blob_url(&repo_blob_url, &managed_repo_blob_url)
            {
                stage_memory_layer(&self.client, &capture_dir, layer, index).await?;
                staged_any = true;
            } else if !has_local_file {
                let feature = if repo_blob_url.is_empty() {
                    format!("memory layer {index} without local file path")
                } else {
                    format!("memory layer {index} uses non-OSS managed repoBlobUrl")
                };
                return Err(RepositoryError::Unsupported { feature });
            }
        }

        if !staged_any {
            return Ok(());
        }
        image_config.repo_blob_url.clear();
        crate::snapshot::runtime_support::write_image_config(
            &local_config_path,
            "local capture memory",
            &image_config,
        )
        .await?;
        manifest.memory.image_config_path = local_config_path;
        Ok(())
    }

    async fn publish(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: FirecrackerSnapshotManifest,
    ) -> RepositoryResult<SnapshotRecord> {
        metadata
            .validate()
            .map_err(|reason| RepositoryError::InvalidRequest { reason })?;
        let id = &metadata.id;
        // Snapshot IDs are system allocated and have one active artifact
        // publisher. Retries for the same ID reuse these fixed keys.
        let layout = self.layout(id);

        // 0. Validate attached-drive identity and virtual sizes.
        validate_attached_drives(&manifest)?;

        let previous_record = self.read_record(id).await?;
        // Fixed per-snapshot object keys are shared by retries. Reject a
        // metadata mismatch while the old identity is still authoritative,
        // before any import can overwrite its closure.
        validate_publish_preflight(previous_record.as_ref(), &metadata)?;
        if let Some(existing) =
            ready_distributed_publish_retry(previous_record.as_ref(), &metadata)?
        {
            return self.commit_record(existing).await;
        }
        if let Some(existing) = previous_record.as_ref().filter(|record| {
            record.lifecycle == SnapshotLifecycle::Preparing && record.committed.is_some()
        }) {
            // Preparing is written only after the complete closure is durable.
            // The preflight above proved this is the same logical publish, so
            // resume its metadata commit without overwriting fixed artifact
            // keys from a potentially different retry input.
            let mut ready = existing.clone();
            ready.lifecycle = SnapshotLifecycle::Ready;
            return self.commit_record(ready).await;
        }
        let mut disk_publications = Vec::new();

        let publish_result = async {
            validate_publish_manifest_image_configs(&manifest)?;

            // 1. Export rootfs disk image with the effective runtime config.
            let rootfs_config = SnapshotOciConfigInput::new(
                &metadata.context,
                metadata.image_configs.rootfs_config(),
            );
            let rootfs_outcome = self
                .export_disk_image(
                    id,
                    DiskImageSubject::Rootfs,
                    &manifest.rootfs.image_config_path,
                    Some(rootfs_config),
                )
                .await?;
            if let Some(publication) = rootfs_outcome.publication.clone() {
                disk_publications.push(publication);
            }
            let rootfs_layers = rootfs_outcome.layers;

            let memory_layers = self
                .derive_and_upload_memory_layers(&manifest.memory.image_config_path)
                .await?;

            // 2. Upload per-snapshot fixed artifacts.
            let vm_state_local_path = manifest.vm_state.path.as_path();
            self.client
                .put_file(
                    &layout.artifact_key(SNAPSHOT_ARTIFACT_LAYOUT.vm_state),
                    vm_state_local_path,
                    OssUploadArtifact::VmState,
                )
                .await
                .map_err(|e| {
                    RepositoryError::backend(
                        format!(
                            "upload artifact '{}' from '{}' for snapshot '{}'",
                            SNAPSHOT_ARTIFACT_LAYOUT.vm_state,
                            vm_state_local_path.display(),
                            id
                        ),
                        e,
                    )
                })?;

            let persisted_manifest_bytes = serde_json::to_vec_pretty(&manifest)
                .map_err(|e| RepositoryError::backend("serialize firecracker manifest", e))?;
            self.client
                .put_bytes(
                    &layout.artifact_key(SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest),
                    persisted_manifest_bytes,
                    OssUploadArtifact::FirecrackerManifest,
                )
                .await
                .map_err(|e| RepositoryError::backend("write firecracker manifest to oss", e))?;

            // 3. Export attached-drive disk images and derive their committed metadata.
            let attached_drives = self
                .export_attached_drives(id, &manifest, &mut disk_publications)
                .await?;

            // 4. Construct committed CommittedSnapshot.
            let committed = CommittedSnapshot {
                context: metadata.context.clone(),
                startup: metadata.startup.clone(),
                runtime_versions: metadata.runtime_versions.clone(),
                virtualization_mode: metadata.virtualization_mode,
                image_configs: metadata.image_configs.clone(),
                custom_extension_params: metadata.custom_extension_params.clone(),
                rootfs_layers,
                attached_drives,
                memory_layers,
                disk_publications: disk_publications.clone(),
                legacy_artifact_namespace: None,
            };

            // 5. Commit metadata only after the complete artifact closure is
            // durable. Only alias-bearing identities need Preparing while
            // reserving their second catalog object; Local promotion keeps
            // the previous record visible.
            let record = self.build_committed_record(&metadata, committed, previous_record.clone());
            let record = self.commit_record(record).await?;

            Ok(record)
        }
        .await;

        let record = match publish_result {
            Ok(record) => record,
            Err(error) => {
                // A failed response can hide a successful metadata commit.
                // Delete fixed artifacts only when a fresh canonical read
                // proves that no record was ever created.
                let cleanup_fixed = match self.read_record(id).await {
                    Ok(None) => true,
                    Ok(Some(record)) => record.lifecycle == SnapshotLifecycle::Deleting,
                    Err(read_error) => {
                        warn!(
                            snapshot_id = %id,
                            error = %read_error,
                            "retaining fixed snapshot artifacts because canonical ownership is unknown"
                        );
                        false
                    }
                };
                if cleanup_fixed {
                    if let Err(cleanup_error) =
                        self.client.delete_prefix(&layout.artifact_prefix()).await
                    {
                        warn!(
                            snapshot_id = %id,
                            error = %cleanup_error,
                            "failed to clean unowned fixed snapshot artifacts"
                        );
                    }
                }
                if !disk_publications.is_empty() {
                    warn!(
                        snapshot_id = %id,
                        publications = disk_publications.len(),
                        "retaining ACR publications after failed publish; ownership is not atomically provable"
                    );
                }
                warn!(
                    snapshot_id = %id,
                    error = %error,
                    "OSS snapshot publication failed"
                );
                return Err(error);
            }
        };

        debug!(snapshot_id = %id, "published snapshot to oss");
        Ok(record)
    }

    async fn commit_record(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        let existing = self.read_record_state(&record.id).await?;
        let plan = metadata_commit_plan(existing.as_ref().map(|stored| &stored.record), &record)?;

        let stored = match plan {
            MetadataCommitPlan::AlreadyCommitted => {
                self.bind_record_alias(&record).await?;
                return Ok(existing
                    .expect("commit plan requires an existing record")
                    .record);
            }
            MetadataCommitPlan::Create => {
                if record.alias.is_none() {
                    return self.write_ready_record(record, None).await;
                }
                let preparing = preparing_record(&record);
                let etag = self.write_record(&preparing, None).await?;
                StoredRecord {
                    record: preparing,
                    etag,
                }
            }
            MetadataCommitPlan::Update => existing.expect("non-create commit plan requires record"),
        };

        // Keep the alias hidden until the final Ready CAS succeeds. A caller
        // retry re-reads the same Preparing record and completes it.
        self.bind_record_alias(&record).await?;
        self.write_ready_record(record, Some(&stored.etag)).await
    }

    async fn get_record(&self, id: &SnapshotId) -> RepositoryResult<Option<SnapshotRecord>> {
        // Terminal identity tombstones remain in the raw catalog so a stale
        // writer cannot reuse the SnapshotId, but they are not part of the
        // repository's exact-read API.  Internal lifecycle paths use
        // `read_record_state` when they must observe the tombstone.
        Ok(self
            .read_record(id)
            .await?
            .filter(|record| !record.is_terminal_tombstone()))
    }

    async fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        // Try by id first.
        if let Ok(direct_id) = crate::snapshot::SnapshotId::parse(id_or_alias) {
            if let Some(record) = self.read_record(&direct_id).await? {
                // A hidden Preparing record reserves its exact identity but
                // must not mask a Ready snapshot whose alias happens to have
                // the same UUID-shaped text. Internal lifecycle callers use
                // get_record for exact-ID semantics; public get preserves the
                // historical ID-or-alias fallback contract.
                if record.is_ready() {
                    return Ok(Some(record));
                }
            }
        }

        // Try by alias.
        let Some(resolved_id) = self.resolve_alias(id_or_alias).await? else {
            return Ok(None);
        };
        Ok(self
            .read_record(&resolved_id)
            .await?
            .filter(SnapshotRecord::is_ready))
    }

    async fn get_for_delete(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        Ok(self
            .record_state_for_delete(id_or_alias)
            .await?
            .map(|stored| stored.record))
    }

    async fn list(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        let keys = self
            .client
            .list_keys_recursive("catalog/records/")
            .await
            .map_err(|e| RepositoryError::backend("list snapshot records", e))?;

        let record_keys = keys.into_iter().filter(|key| key.ends_with(".json"));
        let mut records: Vec<SnapshotRecord> = stream::iter(record_keys)
            .map(|key| async move {
                let bytes = match self.client.get_bytes(&key).await {
                    Ok(bytes) => bytes,
                    Err(e) if OssClient::is_not_found_error(&e) => return Ok(None),
                    Err(e) => {
                        return Err(RepositoryError::backend(
                            format!("read snapshot record '{key}'"),
                            e,
                        ))
                    }
                };
                serde_json::from_slice::<SnapshotRecord>(&bytes)
                    .map(Some)
                    .map_err(|e| {
                        RepositoryError::backend(format!("parse snapshot record '{key}'"), e)
                    })
            })
            .buffer_unordered(16)
            .try_filter_map(|record| async move { Ok(record) })
            .try_collect()
            .await?;

        records.retain(|record| record.is_ready() && filter.matches(record));
        records.sort_by(|a, b| {
            b.created_at_unix_ms
                .cmp(&a.created_at_unix_ms)
                .then_with(|| a.id.to_string().cmp(&b.id.to_string()))
        });

        Ok(records)
    }

    async fn delete(&self, id_or_alias: &str) -> RepositoryResult<()> {
        let Some(stored) = self.record_state_for_delete(id_or_alias).await? else {
            return Ok(());
        };
        self.delete_record(stored).await
    }

    async fn delete_by_id(&self, id: &SnapshotId) -> RepositoryResult<bool> {
        let Some(stored) = self.read_record_state(id).await? else {
            return Ok(false);
        };
        self.delete_record(stored).await?;
        Ok(true)
    }

    async fn resolve_alias(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        let key = validated_alias_key(alias)?;
        let data = match self.client.get_bytes(&key).await {
            Ok(d) => d,
            Err(e) if OssClient::is_not_found_error(&e) => return Ok(None),
            Err(e) => {
                return Err(RepositoryError::backend(format!("read alias '{alias}'"), e));
            }
        };

        let id: SnapshotId = serde_json::from_slice(&data)
            .map_err(|e| RepositoryError::backend(format!("parse alias '{alias}'"), e))?;

        match self.read_record(&id).await? {
            Some(record)
                if record.is_ready()
                    && record
                        .alias
                        .as_ref()
                        .is_some_and(|record_alias| record_alias.as_ref() == alias) =>
            {
                Ok(Some(id))
            }
            // Preparing records deliberately retain the alias reservation but
            // are not public until the final record PUT succeeds.
            Some(_) => Ok(None),
            None => {
                // Do not delete here: the alias may have been replaced after
                // the read. A maintenance/reconciliation pass can reclaim it
                // with an ETag-aware compare-and-delete once that primitive is
                // available.
                debug!(alias = %alias, snapshot_id = %id, "stale alias points to missing snapshot");
                Ok(None)
            }
        }
    }

    async fn try_start_build(&self, id: &SnapshotId) -> RepositoryResult<SnapshotRecord> {
        let stored =
            self.read_record_state(id)
                .await?
                .ok_or_else(|| RepositoryError::SnapshotNotFound {
                    lookup: id.to_string(),
                })?;
        let mut record = stored.record;
        record
            .start_template_build(now_unix_ms())
            .map_err(|reason| RepositoryError::InvalidRequest { reason })?;
        self.write_record(&record, Some(&stored.etag)).await?;
        Ok(record)
    }

    async fn mark_build_error(
        &self,
        id: &SnapshotId,
        reason: TemplateBuildErrorReason,
    ) -> RepositoryResult<()> {
        let stored =
            self.read_record_state(id)
                .await?
                .ok_or_else(|| RepositoryError::SnapshotNotFound {
                    lookup: id.to_string(),
                })?;
        let mut record = stored.record;
        let changed = record
            .mark_template_build_error(&reason, now_unix_ms())
            .map_err(|reason| RepositoryError::InvalidRequest { reason })?;
        if changed {
            self.write_record(&record, Some(&stored.etag)).await?;
        }
        Ok(())
    }
}

// ── private helpers ────────────────────────────────────────────────────

impl OssSnapshotRepository {
    async fn delete_record(&self, observed: StoredRecord) -> RepositoryResult<()> {
        let id = &observed.record.id;
        let was_terminal_tombstone = observed.record.is_terminal_tombstone();

        let Some(stored) = self.begin_delete(id, Some(&observed.etag)).await? else {
            return Ok(());
        };
        if was_terminal_tombstone && stored.record.is_terminal_tombstone() {
            // A retry may be the first operation after a publisher crashed
            // or uploaded late. The identity fence makes the whole prefix
            // private to this deleted SnapshotId, so it is safe to reclaim
            // any leftover attempt objects before returning.
            let layout = self.layout_for_record(&stored.record)?;
            self.client
                .delete_prefix(&layout.artifact_prefix())
                .await
                .map_err(|error| {
                    RepositoryError::backend("delete oss snapshot artifacts", error)
                })?;
            return Ok(());
        }
        let record = stored.record;
        let layout = self.layout_for_record(&record)?;

        if let Some(committed) = record.committed.as_ref() {
            self.delete_disk_publications(id, &committed.disk_publications)
                .await?;
        }
        self.client
            .delete_prefix(&layout.artifact_prefix())
            .await
            .map_err(|error| RepositoryError::backend("delete oss snapshot artifacts", error))?;

        // Retain a tiny hidden terminal record instead of deleting the catalog
        // key. This fences late Template publishers until build/delete writer
        // ownership is coordinated explicitly.
        let Some(current) = self.read_record_state(id).await? else {
            return Ok(());
        };
        if current.record.is_terminal_tombstone() {
            return Ok(());
        }
        if current.record.lifecycle != SnapshotLifecycle::Deleting {
            return Err(RepositoryError::ConcurrentModification {
                resource: format!("snapshot record '{id}'"),
            });
        }
        let mut terminal = current.record;
        terminal.committed = None;
        terminal.updated_at_unix_ms = now_unix_ms();
        self.write_record(&terminal, Some(&current.etag)).await?;
        debug!(snapshot_id = %id, "deleted snapshot artifacts and retained identity tombstone");
        Ok(())
    }

    async fn begin_delete(
        &self,
        id: &SnapshotId,
        expected_etag: Option<&str>,
    ) -> RepositoryResult<Option<StoredRecord>> {
        let Some(stored) = self.read_record_state(id).await? else {
            return Ok(None);
        };
        if expected_etag.is_some_and(|expected| expected != stored.etag) {
            return Err(RepositoryError::ConcurrentModification {
                resource: format!("snapshot record '{id}'"),
            });
        }
        if stored.record.lifecycle == SnapshotLifecycle::Deleting {
            return Ok(Some(stored));
        }
        let mut deleting = stored.record;
        deleting.lifecycle = SnapshotLifecycle::Deleting;
        deleting.updated_at_unix_ms = now_unix_ms();
        let etag = self.write_record(&deleting, Some(&stored.etag)).await?;
        Ok(Some(StoredRecord {
            record: deleting,
            etag,
        }))
    }

    async fn read_record_state(&self, id: &SnapshotId) -> RepositoryResult<Option<StoredRecord>> {
        let key = OssSnapshotArtifactLayout::record_key(id);
        let Some(etag) = self
            .client
            .stat_etag(&key)
            .await
            .map_err(|e| RepositoryError::backend(format!("stat snapshot record '{id}'"), e))?
        else {
            return Ok(None);
        };

        // Stat before read. If a writer replaces the object between these two
        // calls, the bytes are paired with the old ETag and the subsequent CAS
        // necessarily fails instead of overwriting the newer record.
        let bytes = match self.client.get_bytes(&key).await {
            Ok(bytes) => bytes,
            Err(e) if OssClient::is_not_found_error(&e) => return Ok(None),
            Err(e) => {
                return Err(RepositoryError::backend(
                    format!("read snapshot record '{id}'"),
                    e,
                ));
            }
        };
        let record = serde_json::from_slice(&bytes)
            .map_err(|e| RepositoryError::backend(format!("parse snapshot record '{id}'"), e))?;
        Ok(Some(StoredRecord { record, etag }))
    }

    async fn read_record(&self, id: &SnapshotId) -> RepositoryResult<Option<SnapshotRecord>> {
        Ok(self
            .read_record_state(id)
            .await?
            .map(|stored| stored.record))
    }

    async fn put_catalog_bytes(
        &self,
        key: &str,
        bytes: &[u8],
        artifact: OssUploadArtifact,
        expected_etag: Option<&str>,
        conflict_resource: String,
        backend_context: String,
    ) -> RepositoryResult<String> {
        let result = match expected_etag {
            Some(expected) => {
                self.client
                    .put_bytes_if_match(key, bytes.to_vec(), expected, artifact)
                    .await
            }
            None => {
                self.client
                    .put_bytes_if_not_exists(key, bytes.to_vec(), artifact)
                    .await
            }
        };
        match result {
            Ok(etag) => Ok(etag),
            Err(error) if OssClient::is_condition_not_match_error(&error) => {
                Err(RepositoryError::ConcurrentModification {
                    resource: conflict_resource,
                })
            }
            Err(error) => Err(RepositoryError::backend(backend_context, error)),
        }
    }

    async fn write_record(
        &self,
        record: &SnapshotRecord,
        expected_etag: Option<&str>,
    ) -> RepositoryResult<String> {
        let bytes = serde_json::to_vec_pretty(record)
            .map_err(|e| RepositoryError::backend("serialize snapshot record", e))?;
        self.put_catalog_bytes(
            &OssSnapshotArtifactLayout::record_key(&record.id),
            &bytes,
            OssUploadArtifact::CatalogRecord,
            expected_etag,
            format!("snapshot record '{}'", record.id),
            if expected_etag.is_some() {
                "update snapshot record"
            } else {
                "create snapshot record"
            }
            .to_string(),
        )
        .await
    }

    async fn write_ready_record(
        &self,
        record: SnapshotRecord,
        expected_etag: Option<&str>,
    ) -> RepositoryResult<SnapshotRecord> {
        match self.write_record(&record, expected_etag).await {
            Ok(_) => Ok(record),
            Err(error @ RepositoryError::ConcurrentModification { .. }) => Err(error),
            Err(error) => match self.read_record_state(&record.id).await {
                Ok(Some(current))
                    if current.record.is_ready()
                        && current.record.same_catalog_contents(&record) =>
                {
                    Ok(current.record)
                }
                Ok(Some(_)) | Ok(None) => Err(error),
                Err(read_error) => {
                    warn!(
                        snapshot_id = %record.id,
                        error = %read_error,
                        "could not verify an ambiguous Ready metadata commit"
                    );
                    Err(error)
                }
            },
        }
    }

    async fn read_alias_state(&self, alias: &str) -> RepositoryResult<Option<StoredAlias>> {
        let key = validated_alias_key(alias)?;
        let Some(etag) = self
            .client
            .stat_etag(&key)
            .await
            .map_err(|e| RepositoryError::backend(format!("stat alias '{alias}'"), e))?
        else {
            return Ok(None);
        };
        let data = match self.client.get_bytes(&key).await {
            Ok(data) => data,
            Err(e) if OssClient::is_not_found_error(&e) => return Ok(None),
            Err(e) => return Err(RepositoryError::backend(format!("read alias '{alias}'"), e)),
        };
        let target = serde_json::from_slice(&data)
            .map_err(|e| RepositoryError::backend(format!("parse alias '{alias}'"), e))?;
        Ok(Some(StoredAlias { target, etag }))
    }

    async fn put_alias(
        &self,
        alias: &str,
        id: &SnapshotId,
        payload: &[u8],
        expected_etag: Option<&str>,
    ) -> RepositoryResult<String> {
        let key = validated_alias_key(alias)?;
        self.put_catalog_bytes(
            &key,
            payload,
            OssUploadArtifact::Alias,
            expected_etag,
            format!("snapshot alias '{alias}'"),
            if expected_etag.is_some() {
                format!("replace alias '{alias}' for snapshot '{id}'")
            } else {
                format!("create alias '{alias}' for snapshot '{id}'")
            },
        )
        .await
    }

    fn build_committed_record(
        &self,
        metadata: &SnapshotPublishMetadata,
        committed: CommittedSnapshot,
        previous_record: Option<SnapshotRecord>,
    ) -> SnapshotRecord {
        let now = now_unix_ms();
        if let Some(mut record) = previous_record {
            record.mark_committed(metadata, committed, now);
            record
        } else {
            SnapshotRecord::new_committed(metadata, committed, now)
        }
    }

    async fn bind_record_alias(&self, record: &SnapshotRecord) -> RepositoryResult<()> {
        let Some(alias) = record.alias.as_ref() else {
            return Ok(());
        };
        self.bind_alias(alias.as_ref(), &record.id).await
    }

    /// Bind an alias to a snapshot id with ETag-based conflict detection.
    ///
    /// The configured snapshot backend must support conditional writes;
    /// [`OssClient`] rejects endpoints that cannot provide that contract.
    ///
    /// The algorithm is:
    ///   1. Read the current alias target.
    ///   2. If it already points to `id`, return success (idempotent).
    ///   3. If it points to a Ready snapshot, return `AliasConflict`.
    ///   4. Fence a superseded Preparing target with its record ETag; Deleting
    ///      and missing targets are already reclaimable.
    ///   5. Write our binding only when the alias ETag from step 1 still matches.
    ///
    /// A backend without conditional writes fails closed in
    /// [`OssClient::put_bytes_if_match`]; it must not silently fall back to an
    /// unconditional alias overwrite.
    async fn bind_alias(&self, alias: &str, id: &SnapshotId) -> RepositoryResult<()> {
        let payload = serde_json::to_vec(id)
            .map_err(|e| RepositoryError::backend("serialize alias binding", e))?;

        for _attempt in 0..MAX_ALIAS_BIND_ATTEMPTS {
            let Some(existing) = self.read_alias_state(alias).await? else {
                match self.put_alias(alias, id, &payload, None).await {
                    Ok(_) => return Ok(()),
                    Err(RepositoryError::ConcurrentModification { .. }) => continue,
                    Err(error) => return Err(error),
                }
            };

            if existing.target == *id {
                match self.read_record_state(id).await? {
                    Some(record) if record.record.is_terminal_tombstone() => {
                        // A terminal identity tombstone is intentionally
                        // retained so that a stale writer can never reuse
                        // this SnapshotId.  In particular, do not treat an
                        // alias that already points at the tombstone as an
                        // idempotent successful claim for the same id.
                        return Err(RepositoryError::ConcurrentModification {
                            resource: format!("snapshot '{id}' is an identity tombstone"),
                        });
                    }
                    Some(record)
                        if record.record.lifecycle == SnapshotLifecycle::Deleting
                            || record
                                .record
                                .alias
                                .as_ref()
                                .is_none_or(|record_alias| record_alias.as_ref() != alias) =>
                    {
                        return Err(RepositoryError::InvalidRequest {
                            reason: format!("snapshot alias '{alias}' is not bound to '{id}'"),
                        });
                    }
                    Some(_) => return Ok(()),
                    None => {
                        // The alias target disappeared between the alias
                        // read and this exact record read.  Treat this as a
                        // conflict rather than silently claiming a deleted
                        // identity; the next retry may safely reclaim the
                        // stale alias for a different id.
                        return Err(RepositoryError::ConcurrentModification {
                            resource: format!("snapshot alias '{alias}' target '{id}'"),
                        });
                    }
                }
            }

            if let Some(mut target) = self.read_record_state(&existing.target).await? {
                let claims_alias = target
                    .record
                    .alias
                    .as_ref()
                    .is_some_and(|target_alias| target_alias.as_ref() == alias);
                if claims_alias && target.record.is_ready() {
                    return Err(RepositoryError::AliasConflict {
                        alias: alias.to_string(),
                        existing: existing.target,
                        new_id: id.clone(),
                    });
                }
                if claims_alias && target.record.lifecycle == SnapshotLifecycle::Preparing {
                    let mut deleting = target.record;
                    deleting.lifecycle = SnapshotLifecycle::Deleting;
                    deleting.updated_at_unix_ms = now_unix_ms();
                    let etag = match self.write_record(&deleting, Some(&target.etag)).await {
                        Ok(etag) => etag,
                        Err(RepositoryError::ConcurrentModification { .. }) => continue,
                        Err(error) => return Err(error),
                    };
                    target = StoredRecord {
                        record: deleting,
                        etag,
                    };
                }
                if claims_alias && target.record.lifecycle == SnapshotLifecycle::Deleting {
                    // Cleanup owns the lifecycle until it reaches the terminal
                    // fence; do not make the alias available while artifacts
                    // have no future retry owner.
                    self.delete_record(target).await?;
                }
            }

            match self
                .put_alias(alias, id, &payload, Some(&existing.etag))
                .await
            {
                Ok(_) => return Ok(()),
                Err(RepositoryError::ConcurrentModification { .. }) => continue,
                Err(error) => return Err(error),
            }
        }

        Err(RepositoryError::ConcurrentModification {
            resource: format!("snapshot alias '{alias}'"),
        })
    }

    async fn derive_and_upload_disk_image_layers(
        &self,
        image_config_path: &Path,
        artifact: OssUploadArtifact,
    ) -> RepositoryResult<Vec<OverlaybdLayerRef>> {
        let image_config = load_overlaybd_image_config(image_config_path).map_err(|e| {
            RepositoryError::backend(
                format!(
                    "load overlaybd image config '{}'",
                    image_config_path.display()
                ),
                e,
            )
        })?;

        let mut layers = Vec::with_capacity(image_config.lowers.len());

        for (index, layer) in image_config.lowers.into_iter().enumerate() {
            if !layer.file.is_empty() {
                let layer_path = Path::new(&layer.file);
                if !layer.digest.is_empty() && layer.size > 0 {
                    let managed = self
                        .import_managed_layer_with_descriptor(
                            layer_path,
                            &layer.digest,
                            layer.size,
                            artifact,
                        )
                        .await?;
                    layers.push(OverlaybdLayerRef::Managed(managed));
                    continue;
                }
                if crate::image::local_layer::rootfs_layer_is_runtime_generated_delta(layer_path) {
                    let managed = self
                        .import_descriptorless_rootfs_layer(layer_path, artifact)
                        .await?;
                    layers.push(OverlaybdLayerRef::Managed(managed));
                    continue;
                }
                return Err(RepositoryError::Unsupported {
                    feature: format!(
                        "local overlaybd lower layer {index} '{}' missing digest/size",
                        layer_path.display()
                    ),
                });
            }
            let repo_blob_url = layer
                .effective_repo_blob_url(&image_config.repo_blob_url)
                .to_string();
            if !repo_blob_url.is_empty() {
                let digest = if !layer.digest.is_empty() {
                    layer.digest
                } else if !layer.target_digest.is_empty() {
                    layer.target_digest
                } else {
                    format!("external:{index}")
                };
                layers.push(OverlaybdLayerRef::External(ExternalLayer {
                    digest,
                    repo_blob_url: repo_blob_url.clone(),
                    size: layer.size,
                }));
                continue;
            }
            return Err(RepositoryError::Unsupported {
                feature: format!("overlaybd lower layer {index} without local file or repoBlobUrl"),
            });
        }

        Ok(layers)
    }

    async fn derive_and_upload_memory_layers(
        &self,
        mem_image_config_path: &Path,
    ) -> RepositoryResult<Vec<ManagedLayer>> {
        let image_config = load_overlaybd_image_config(mem_image_config_path).map_err(|e| {
            RepositoryError::backend(
                format!(
                    "load mem image config '{}'",
                    mem_image_config_path.display()
                ),
                e,
            )
        })?;

        let mut layers = Vec::with_capacity(image_config.lowers.len());
        for (index, layer) in image_config.lowers.into_iter().enumerate() {
            if layer.file.is_empty() {
                let repo_blob_url = layer
                    .effective_repo_blob_url(&image_config.repo_blob_url)
                    .to_string();
                if !repo_blob_url.is_empty() {
                    if !same_repo_blob_url(
                        &repo_blob_url,
                        &self.client.managed_layers_repo_blob_url(),
                    ) {
                        return Err(RepositoryError::Unsupported {
                            feature: format!(
                                "memory layer {index} uses non-OSS managed repoBlobUrl"
                            ),
                        });
                    }
                    let digest = memory_layer_digest(index, &layer)?.to_owned();
                    layers.push(ManagedLayer {
                        digest,
                        size: layer.size,
                        uuid: None,
                    });
                    continue;
                }
                return Err(RepositoryError::Unsupported {
                    feature: format!("memory layer {index} without local file path"),
                });
            }
            let layer_path = Path::new(&layer.file);
            if !layer.digest.is_empty() && layer.size > 0 {
                layers.push(
                    self.import_managed_layer_with_descriptor(
                        layer_path,
                        &layer.digest,
                        layer.size,
                        OssUploadArtifact::MemoryLayer,
                    )
                    .await?,
                );
                continue;
            }
            layers.push(
                self.import_managed_layer_by_hash(layer_path, OssUploadArtifact::MemoryLayer)
                    .await?,
            );
        }

        Ok(layers)
    }

    async fn export_disk_image(
        &self,
        snapshot_id: &SnapshotId,
        subject: DiskImageSubject,
        image_config_path: &Path,
        config: Option<SnapshotOciConfigInput<'_>>,
    ) -> RepositoryResult<DiskImageExportOutcome> {
        let artifact = match &subject {
            DiskImageSubject::Rootfs => OssUploadArtifact::RootfsLayer,
            DiskImageSubject::AttachedDrive { .. } => OssUploadArtifact::AttachedDriveLayer,
        };
        if !matches!(
            self.snapshot_image_storage,
            SnapshotImageStoragePolicy::SourceRegistry
        ) {
            let layers = self
                .derive_and_upload_disk_image_layers(image_config_path, artifact)
                .await?;
            return Ok(DiskImageExportOutcome {
                layers,
                publication: None,
            });
        }
        match self
            .acr_exporter
            .export(snapshot_id, subject.clone(), image_config_path, config)
            .await
        {
            Err(RepositoryError::Unsupported { feature }) => {
                if fallback_to_object_storage_would_mix_sources(
                    image_config_path,
                    &self.client.managed_layers_repo_blob_url(),
                )? {
                    return Err(RepositoryError::Unsupported {
                        feature: format!(
                            "source-registry export is unsupported for this remote-backed disk image: {feature}"
                        ),
                    });
                }
                info!(
                    snapshot_id = %snapshot_id,
                    subject = subject.log_label(),
                    reason = %feature,
                    "falling back to managed disk image layers"
                );
                let layers = self
                    .derive_and_upload_disk_image_layers(image_config_path, artifact)
                    .await?;
                Ok(DiskImageExportOutcome {
                    layers,
                    publication: None,
                })
            }
            result => result,
        }
    }

    async fn delete_disk_publications(
        &self,
        snapshot_id: &SnapshotId,
        publications: &[PersistedDiskImagePublication],
    ) -> RepositoryResult<()> {
        for publication in publications.iter().rev() {
            self.acr_exporter
                .rollback_publication(publication)
                .await
                .map_err(|error| {
                    RepositoryError::backend(
                        format!(
                            "delete ACR publication '{}' for snapshot '{}'",
                            publication.image_ref, snapshot_id
                        ),
                        error,
                    )
                })?;
        }
        Ok(())
    }

    /// Export attached-drive disk images and derive committed metadata.
    async fn export_attached_drives(
        &self,
        snapshot_id: &SnapshotId,
        manifest: &crate::sandbox::FirecrackerSnapshotManifest,
        publications: &mut Vec<PersistedDiskImagePublication>,
    ) -> RepositoryResult<Vec<CommittedAttachedDrive>> {
        let mut drives = Vec::new();

        for drive in &manifest.attached_drives {
            let outcome = self
                .export_disk_image(
                    snapshot_id,
                    DiskImageSubject::AttachedDrive {
                        drive_id: drive.drive_id.clone(),
                    },
                    &drive.image_config_path,
                    None,
                )
                .await?;
            if let Some(publication) = outcome.publication.clone() {
                publications.push(publication);
            }
            drives.push(CommittedAttachedDrive::Overlaybd {
                drive_id: drive.drive_id.clone(),
                layers: outcome.layers,
                read_only: drive.read_only,
                virtual_size: drive.virtual_size,
                mount_path: crate::sandbox::normalize_mount_path_or_default(
                    &drive.drive_id,
                    drive.mount_path.clone(),
                ),
                sub_path: drive.sub_path.clone(),
            });
        }

        Ok(drives)
    }

    async fn load_alias_target(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        let key = validated_alias_key(alias)?;
        let data = match self.client.get_bytes(&key).await {
            Ok(data) => data,
            Err(e) if OssClient::is_not_found_error(&e) => return Ok(None),
            Err(e) => {
                return Err(RepositoryError::backend(
                    format!("read alias target '{alias}'"),
                    e,
                ));
            }
        };

        let target = serde_json::from_slice::<SnapshotId>(&data)
            .map_err(|e| RepositoryError::backend(format!("parse alias target '{alias}'"), e))?;
        Ok(Some(target))
    }

    /// Loads a record for source-scoped deletion, including hidden Preparing
    /// records. Public `get` intentionally hides those records, but a
    /// template cleanup retry must still be able to remove its exact record
    /// and release the reserved alias. Exact UUID lookup wins over alias
    /// fallback so a UUID-shaped alias cannot redirect a delete request.
    async fn record_state_for_delete(
        &self,
        id_or_alias: &str,
    ) -> RepositoryResult<Option<StoredRecord>> {
        if let Ok(id) = SnapshotId::parse(id_or_alias) {
            if let Some(record) = self.read_record_state(&id).await? {
                return Ok(Some(record));
            }
        }

        let Some(id) = self.load_alias_target(id_or_alias).await? else {
            return Ok(None);
        };
        match self.read_record_state(&id).await? {
            Some(stored)
                if stored
                    .record
                    .alias
                    .as_ref()
                    .is_some_and(|record_alias| record_alias.as_ref() == id_or_alias) =>
            {
                Ok(Some(stored))
            }
            None => Ok(None),
            Some(_) => Ok(None),
        }
    }
}

impl OssSnapshotRepository {
    async fn import_descriptorless_rootfs_layer(
        &self,
        source: &Path,
        artifact: OssUploadArtifact,
    ) -> RepositoryResult<ManagedLayer> {
        let canonical = canonicalize_managed_layer(source)?;
        if dense_export::should_dense_export_layer(&canonical) {
            return self
                .import_sparse_overlaybd_layer_dense(&canonical, artifact)
                .await;
        }
        self.import_managed_layer_by_hash(&canonical, artifact)
            .await
    }

    async fn import_sparse_overlaybd_layer_dense(
        &self,
        canonical: &Path,
        artifact: OssUploadArtifact,
    ) -> RepositoryResult<ManagedLayer> {
        let dense_temp = tempfile::NamedTempFile::new().map_err(|e| {
            RepositoryError::backend(
                format!(
                    "create temp dense overlaybd layer for '{}'",
                    canonical.display()
                ),
                e,
            )
        })?;
        let dense_path = dense_temp.path().to_path_buf();
        let descriptor = write_dense_overlaybd_layer_to_file(canonical, &dense_path)
            .await
            .map_err(|e| {
                RepositoryError::backend(
                    format!(
                        "dense-export sparse overlaybd layer '{}'",
                        canonical.display()
                    ),
                    e,
                )
            })?;
        let oss_key = OssSnapshotArtifactLayout::managed_layer_key(&descriptor.digest);
        upload_managed_layer_if_missing(&self.client, &oss_key, &dense_path, artifact).await?;

        Ok(ManagedLayer {
            digest: descriptor.digest,
            size: descriptor.size,
            uuid: None,
        })
    }

    async fn import_managed_layer_by_hash(
        &self,
        source: &Path,
        artifact: OssUploadArtifact,
    ) -> RepositoryResult<ManagedLayer> {
        let canonical = canonicalize_managed_layer(source)?;
        let descriptor = crate::digest::FileDigest::describe(&canonical)
            .await
            .map_err(|e| {
                RepositoryError::backend(
                    format!("describe managed layer '{}'", canonical.display()),
                    e,
                )
            })?;
        let oss_key = OssSnapshotArtifactLayout::managed_layer_key(&descriptor.sha256);
        upload_managed_layer_if_missing(&self.client, &oss_key, &canonical, artifact).await?;

        Ok(ManagedLayer {
            digest: descriptor.sha256,
            size: descriptor.size,
            uuid: overlaybd_layer_uuid(&canonical),
        })
    }

    async fn import_managed_layer_with_descriptor(
        &self,
        source: &Path,
        digest: &str,
        size: u64,
        artifact: OssUploadArtifact,
    ) -> RepositoryResult<ManagedLayer> {
        let canonical = canonicalize_managed_layer(source)?;
        let source_size = std::fs::metadata(&canonical)
            .map_err(|e| {
                RepositoryError::backend(
                    format!("read managed layer metadata '{}'", canonical.display()),
                    e,
                )
            })?
            .len();
        if source_size != size {
            return Err(RepositoryError::Backend {
                message: format!(
                    "managed layer descriptor size mismatch for '{}': descriptor says {}, file has {}",
                    canonical.display(),
                    size,
                    source_size
                ),
                source: None,
            });
        }

        // Descriptor-backed imports intentionally trust internally generated
        // content digests and only validate the cheap size invariant here.
        let oss_key = OssSnapshotArtifactLayout::managed_layer_key(digest);
        upload_managed_layer_if_missing(&self.client, &oss_key, &canonical, artifact).await?;

        Ok(ManagedLayer {
            digest: digest.to_string(),
            size,
            uuid: overlaybd_layer_uuid(&canonical),
        })
    }
}

/// Upload a content-addressed managed layer if it is not already present.
///
/// Managed layers are keyed by `sha256:{digest}`, so concurrent writers
/// uploading the same digest always produce identical content.  This makes
/// the `exists() → put()` TOCTOU benign: the worst case is a redundant
/// upload of identical bytes, never data corruption.
async fn upload_managed_layer_if_missing(
    client: &OssClient,
    key: &str,
    canonical: &Path,
    artifact: OssUploadArtifact,
) -> RepositoryResult<()> {
    let already_exists = client
        .exists(key)
        .await
        .map_err(|e| RepositoryError::backend("check managed layer existence", e))?;

    if !already_exists {
        client
            .put_file(key, canonical, artifact)
            .await
            .map_err(|e| {
                RepositoryError::backend(
                    format!("upload managed layer '{}'", canonical.display()),
                    e,
                )
            })?;
    }

    Ok(())
}

fn validate_publish_manifest_image_configs(
    manifest: &FirecrackerSnapshotManifest,
) -> RepositoryResult<()> {
    load_overlaybd_image_config(&manifest.rootfs.image_config_path).map_err(|e| {
        RepositoryError::backend(
            format!(
                "validate rootfs image config '{}'",
                manifest.rootfs.image_config_path.display()
            ),
            e,
        )
    })?;
    load_overlaybd_image_config(&manifest.memory.image_config_path).map_err(|e| {
        RepositoryError::backend(
            format!(
                "validate memory image config '{}'",
                manifest.memory.image_config_path.display()
            ),
            e,
        )
    })?;
    for drive in &manifest.attached_drives {
        load_overlaybd_image_config(&drive.image_config_path).map_err(|e| {
            RepositoryError::backend(
                format!(
                    "validate drive image config '{}' for drive '{}'",
                    drive.image_config_path.display(),
                    drive.drive_id
                ),
                e,
            )
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store_operator::CredentialSource;
    use serde_json::json;

    use crate::snapshot::SnapshotPublishSource;

    fn local_record() -> SnapshotRecord {
        SnapshotRecord {
            id: SnapshotId::generate(),
            snapshot_type: SnapshotType::Local,
            owner_node_id: Some("node-a".to_string()),
            lifecycle: SnapshotLifecycle::Ready,
            alias: None,
            source: SnapshotSource::Sandbox {
                source_sandbox_id: "sandbox-a".to_string(),
            },
            resources: crate::types::SandboxResources::default(),
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
            committed: Some(CommittedSnapshot::mock()),
        }
    }

    fn publish_metadata_for(record: &SnapshotRecord) -> SnapshotPublishMetadata {
        let committed = record.committed.as_ref().expect("committed snapshot");
        let source = match &record.source {
            SnapshotSource::Template { .. } => SnapshotPublishSource::Template,
            SnapshotSource::Sandbox { source_sandbox_id } => SnapshotPublishSource::Sandbox {
                source_sandbox_id: source_sandbox_id.clone(),
            },
        };
        SnapshotPublishMetadata {
            id: record.id.clone(),
            snapshot_type: record.snapshot_type,
            owner_node_id: record.owner_node_id.clone(),
            alias: record.alias.clone(),
            source,
            context: committed.context.clone(),
            startup: committed.startup.clone(),
            resources: record.resources,
            runtime_versions: committed.runtime_versions.clone(),
            virtualization_mode: committed.virtualization_mode,
            image_configs: committed.image_configs.clone(),
            custom_extension_params: committed.custom_extension_params.clone(),
        }
    }

    #[test]
    fn publish_preflight_rejects_changed_preparing_metadata_before_artifact_import() {
        let ready = local_record();
        let mut preparing = preparing_record(&ready);
        preparing.snapshot_type = SnapshotType::Distributed;
        preparing.owner_node_id = None;
        let metadata = publish_metadata_for(&preparing);

        validate_publish_preflight(Some(&preparing), &metadata)
            .expect("equivalent Preparing retry should be allowed");

        let mut changed = metadata;
        changed.context.workdir = "/different".to_string();
        let error = validate_publish_preflight(Some(&preparing), &changed)
            .expect_err("changed Preparing metadata must fail before upload");
        assert!(matches!(error, RepositoryError::InvalidRequest { .. }));
    }

    #[test]
    fn publish_preflight_accepts_matching_local_promotion_and_rejects_changed_logical_metadata() {
        let local = local_record();
        let mut distributed = publish_metadata_for(&local);
        distributed.snapshot_type = SnapshotType::Distributed;
        distributed.owner_node_id = None;
        validate_publish_preflight(Some(&local), &distributed)
            .expect("matching Local promotion should be allowed");

        distributed.context.workdir = "/different".to_string();
        let error = validate_publish_preflight(Some(&local), &distributed)
            .expect_err("changed promotion metadata must fail before upload");
        assert!(matches!(error, RepositoryError::InvalidRequest { .. }));
    }

    #[test]
    fn metadata_commit_allows_only_the_explicit_local_promotion() {
        let local = local_record();
        let mut distributed = local.clone();
        distributed.snapshot_type = SnapshotType::Distributed;
        distributed.owner_node_id = None;
        distributed.updated_at_unix_ms += 1;

        assert_eq!(
            metadata_commit_plan(Some(&local), &distributed).expect("promotion plan"),
            MetadataCommitPlan::Update
        );

        let mut conflicting = distributed.clone();
        conflicting.alias = Some(SnapshotAlias::parse("different").expect("valid alias"));
        let error = metadata_commit_plan(Some(&distributed), &conflicting)
            .expect_err("arbitrary Ready overwrite must fail");
        assert!(matches!(error, RepositoryError::InvalidRequest { .. }));

        let mut changed_config = distributed;
        changed_config
            .committed
            .as_mut()
            .expect("committed snapshot")
            .context
            .workdir = "/different".to_string();
        let error = metadata_commit_plan(Some(&local), &changed_config)
            .expect_err("promotion must preserve logical runtime metadata");
        assert!(matches!(error, RepositoryError::InvalidRequest { .. }));
    }

    #[test]
    fn equivalent_ready_publish_retry_short_circuits_before_manifest_validation() {
        let mut distributed = local_record();
        distributed.snapshot_type = SnapshotType::Distributed;
        distributed.owner_node_id = None;
        let metadata = publish_metadata_for(&distributed);

        let retry = ready_distributed_publish_retry(Some(&distributed), &metadata)
            .expect("equivalent retry check")
            .expect("Ready Distributed retry");
        assert_eq!(retry.id, distributed.id);

        let mut changed_metadata = metadata;
        changed_metadata.context.workdir = "/different".to_string();
        let error = ready_distributed_publish_retry(Some(&distributed), &changed_metadata)
            .expect_err("changed logical metadata must not be treated as a retry");
        assert!(matches!(error, RepositoryError::InvalidRequest { .. }));
    }

    #[test]
    fn concurrent_publish_identity_ignores_attempt_creation_time_but_not_backing() {
        let mut winner = local_record();
        winner.snapshot_type = SnapshotType::Distributed;
        winner.owner_node_id = None;
        let mut loser = winner.clone();
        loser.created_at_unix_ms += 10;
        loser.updated_at_unix_ms += 10;
        assert!(winner.same_logical_identity(&loser));
        assert!(!winner.same_stable_identity(&loser));

        loser.snapshot_type = SnapshotType::Local;
        loser.owner_node_id = Some("node-a".to_string());
        assert!(!winner.same_logical_identity(&loser));
    }

    #[test]
    fn legacy_record_without_lifecycle_is_ready() {
        let record = local_record();
        let mut value = serde_json::to_value(record).expect("serialize record");
        value
            .as_object_mut()
            .expect("record object")
            .remove("lifecycle");

        let decoded: SnapshotRecord = serde_json::from_value(value).expect("decode legacy record");
        assert!(decoded.is_ready());
    }

    fn write_test_image(path: &Path, value: serde_json::Value) {
        std::fs::write(
            path,
            serde_json::to_vec_pretty(&value).expect("serialize image config"),
        )
        .expect("write image config");
    }

    fn test_repository() -> OssSnapshotRepository {
        let client = OssClient::new(
            "bucket".to_string(),
            "https://oss.example.com".to_string(),
            "region".to_string(),
            "prefix".to_string(),
            CredentialSource::Anonymous,
            None,
        )
        .expect("oss client");
        OssSnapshotRepository::new(Arc::new(client), SnapshotImageStoragePolicy::ObjectStorage)
    }

    #[test]
    fn publish_manifest_preflight_rejects_invalid_memory_image_config() {
        let temp = tempfile::tempdir().expect("tempdir");
        let rootfs_image_config = temp.path().join("rootfs-image.json");
        let memory_image_config = temp.path().join("mem-image.json");

        write_test_image(
            &rootfs_image_config,
            json!({
                "lowers": [
                    { "file": "rootfs.commit" }
                ],
                "upper": {},
                "resultFile": "",
                "download": {}
            }),
        );
        write_test_image(
            &memory_image_config,
            json!({
                "lowers": [
                    { "digest": "sha256:parent", "size": 4096 }
                ],
                "upper": {},
                "resultFile": "",
                "download": {}
            }),
        );

        let mut manifest = FirecrackerSnapshotManifest::for_test(1024, &[]);
        manifest.rootfs.image_config_path = rootfs_image_config;
        manifest.memory.image_config_path = memory_image_config;

        let err = validate_publish_manifest_image_configs(&manifest)
            .expect_err("missing memory repoBlobUrl should fail preflight");
        assert!(err.to_string().contains("validate memory image config"));
    }

    #[test]
    fn detects_source_registry_fallback_source_mixing() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source_registry_image = temp.path().join("source-image.json");
        write_test_image(
            &source_registry_image,
            json!({
                "repoBlobUrl": "https://registry.example/v2/ns/image/blobs",
                "lowers": [
                    { "digest": "sha256:base", "size": 4096 },
                    { "file": "snapshot.commit", "digest": "sha256:delta", "size": 5 }
                ],
                "upper": {},
                "resultFile": "",
                "download": {}
            }),
        );
        assert!(fallback_to_object_storage_would_mix_sources(
            &source_registry_image,
            "s3://bucket/prefix/managed-layers"
        )
        .unwrap());

        let oss_image = temp.path().join("oss-image.json");
        write_test_image(
            &oss_image,
            json!({
                "repoBlobUrl": "s3://bucket/prefix/managed-layers",
                "lowers": [
                    { "digest": "sha256:base", "size": 4096 },
                    { "file": "snapshot.commit", "digest": "sha256:delta", "size": 5 }
                ],
                "upper": {},
                "resultFile": "",
                "download": {}
            }),
        );

        assert!(!fallback_to_object_storage_would_mix_sources(
            &oss_image,
            "s3://bucket/prefix/managed-layers"
        )
        .unwrap());

        let layer_level_image = temp.path().join("layer-level-image.json");
        write_test_image(
            &layer_level_image,
            json!({
                "repoBlobUrl": "",
                "lowers": [
                    {
                        "digest": "sha256:base",
                        "size": 4096,
                        "repoBlobUrl": "https://registry.example/v2/ns/image/blobs"
                    },
                    { "file": "snapshot.commit", "digest": "sha256:delta", "size": 5 }
                ],
                "upper": {},
                "resultFile": "",
                "download": {}
            }),
        );
        assert!(fallback_to_object_storage_would_mix_sources(
            &layer_level_image,
            "s3://bucket/prefix/managed-layers"
        )
        .unwrap());
    }

    #[tokio::test]
    async fn derives_external_layers_from_layer_repo_blob_urls() {
        let temp = tempfile::tempdir().expect("tempdir");
        let image = temp.path().join("image.json");
        write_test_image(
            &image,
            json!({
                "repoBlobUrl": "",
                "lowers": [
                    {
                        "digest": "sha256:base",
                        "size": 4096,
                        "repoBlobUrl": "https://registry.example/v2/ns/image/blobs"
                    },
                    {
                        "digest": "sha256:delta",
                        "size": 8192,
                        "repoBlobUrl": "s3://bucket/prefix/managed-layers"
                    }
                ],
                "upper": {},
                "resultFile": "",
                "download": {}
            }),
        );

        let layers = test_repository()
            .derive_and_upload_disk_image_layers(&image, OssUploadArtifact::RootfsLayer)
            .await
            .expect("derive layers");

        assert_eq!(
            layers,
            vec![
                OverlaybdLayerRef::External(ExternalLayer {
                    digest: "sha256:base".to_string(),
                    repo_blob_url: "https://registry.example/v2/ns/image/blobs".to_string(),
                    size: 4096,
                }),
                OverlaybdLayerRef::External(ExternalLayer {
                    digest: "sha256:delta".to_string(),
                    repo_blob_url: "s3://bucket/prefix/managed-layers".to_string(),
                    size: 8192,
                }),
            ]
        );
    }

    #[tokio::test]
    async fn prepare_local_capture_materializes_managed_memory_lower() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source = temp.path().join("cache").join("memory.commit");
        std::fs::create_dir_all(source.parent().expect("source parent")).expect("source dir");
        std::fs::write(&source, b"captured memory").expect("memory layer");
        let local_delta = temp.path().join("snapshot.delta");
        std::fs::write(&local_delta, b"new memory delta").expect("local memory delta");
        let descriptor = crate::digest::FileDigest::describe(&source)
            .await
            .expect("describe memory layer");
        let image = temp.path().join("memory.json");
        write_test_image(
            &image,
            json!({
                "repoBlobUrl": "s3://bucket/prefix/managed-layers",
                "lowers": [
                    {
                        "file": source,
                        "digest": descriptor.sha256.clone(),
                        "size": descriptor.size
                    },
                    { "file": local_delta.display().to_string() }
                ],
                "upper": {},
                "resultFile": ""
            }),
        );
        let mut manifest = FirecrackerSnapshotManifest::for_test(1024, &[]);
        manifest.memory.image_config_path = image.clone();

        test_repository()
            .prepare_local_capture(&mut manifest)
            .await
            .expect("managed memory lower should become local");

        assert_ne!(manifest.memory.image_config_path, image);
        let config = load_overlaybd_image_config(&manifest.memory.image_config_path)
            .expect("load rewritten image config");
        assert!(config.repo_blob_url.is_empty());
        let lower = config.lowers.first().expect("memory lower");
        assert!(lower.dir.is_empty());
        assert_eq!(
            crate::digest::FileDigest::describe(Path::new(&lower.file))
                .await
                .expect("describe staged layer"),
            descriptor
        );
        assert_eq!(
            config.lowers.get(1).expect("local capture delta").file,
            local_delta.display().to_string()
        );
    }

    #[tokio::test]
    async fn prepare_local_capture_rejects_non_oss_remote_memory_lower() {
        let temp = tempfile::tempdir().expect("tempdir");
        let image = temp.path().join("memory.json");
        write_test_image(
            &image,
            json!({
                "repoBlobUrl": "https://registry.example/v2/ns/image/blobs",
                "lowers": [{ "digest": "sha256:remote", "size": 4096 }],
                "upper": {},
                "resultFile": ""
            }),
        );
        let mut manifest = FirecrackerSnapshotManifest::for_test(1024, &[]);
        manifest.memory.image_config_path = image;

        let error = test_repository()
            .prepare_local_capture(&mut manifest)
            .await
            .expect_err("local capture must fail closed for non-OSS memory lowers");
        assert!(matches!(error, RepositoryError::Unsupported { .. }));
    }
}
