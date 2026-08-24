pub(crate) mod acr;

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use overlaybd::backend::local::LocalFile;
use overlaybd::dense_export;
use overlaybd::index_file::CommitArgs;
use overlaybd::layer_metadata::read_overlaybd_layer_uuid;
use overlaybd::virtual_file::VirtualFile;

use crate::sandbox::FirecrackerSnapshotManifest;
use crate::snapshot::repository::{
    validate_attached_drive_virtual_size, RepositoryError, RepositoryResult,
};

/// Validates the attached-drive identity and persisted virtual-size contract
/// shared by every snapshot repository backend.
pub(crate) fn validate_attached_drives(
    manifest: &FirecrackerSnapshotManifest,
) -> RepositoryResult<()> {
    let mut drive_ids = HashSet::with_capacity(manifest.attached_drives.len());
    for drive in &manifest.attached_drives {
        if !drive_ids.insert(drive.drive_id.as_str()) {
            return Err(RepositoryError::InvalidRequest {
                reason: format!(
                    "duplicate attached drive id in publish request: {}",
                    drive.drive_id
                ),
            });
        }
        validate_attached_drive_virtual_size(&drive.drive_id, drive.virtual_size)?;
    }
    Ok(())
}

pub(crate) fn overlaybd_layer_uuid(source: &Path) -> Option<String> {
    read_overlaybd_layer_uuid(source)
        .ok()
        .filter(|uuid| !uuid.is_nil())
        .map(|uuid| uuid.to_string())
}

pub(crate) async fn write_dense_overlaybd_layer_to_file(
    source: &Path,
    destination: &Path,
) -> Result<dense_export::DenseLayerDescriptor> {
    let file: Arc<dyn VirtualFile> = Arc::new(LocalFile::new(destination).with_context(|| {
        format!(
            "create dense overlaybd layer destination '{}'",
            destination.display()
        )
    })?);
    dense_export::write_dense_layer_to(source, CommitArgs::new(file.clone()).writer).await?;
    file.sync()
        .await
        .with_context(|| format!("sync dense overlaybd layer '{}'", destination.display()))?;

    let descriptor = crate::digest::FileDigest::describe(destination)
        .await
        .with_context(|| {
            format!(
                "describe dense overlaybd layer destination '{}'",
                destination.display()
            )
        })?;
    Ok(dense_export::DenseLayerDescriptor {
        digest: descriptor.sha256,
        size: descriptor.size,
    })
}

pub(crate) fn write_dense_overlaybd_layer_to_file_blocking(
    source: &Path,
    destination: &Path,
) -> Result<dense_export::DenseLayerDescriptor> {
    // POSIX snapshot publish calls this inside `run_repository_blocking`.
    // Do not call it from an async task; use the async variant above instead.
    let source = source.to_path_buf();
    let destination = destination.to_path_buf();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("create runtime for dense overlaybd export")?;
    runtime.block_on(write_dense_overlaybd_layer_to_file(&source, &destination))
}
