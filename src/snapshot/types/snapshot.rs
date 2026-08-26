use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(test)]
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

use crate::sandbox::CustomExtensionParams;
use crate::virtualization::VirtualizationMode;
use shell_util::shell_quote;

use super::drive::{CommittedAttachedDrive, ResolvedAttachedDrive};
use super::value::{SnapshotAlias, SnapshotId};
use super::version::SnapshotRuntimeVersions;
use crate::sandbox::FirecrackerSnapshotManifest;
use crate::types::{ImageConfigs, SandboxResources};

#[derive(Clone, Debug)]
pub struct SnapshotPublishMetadata {
    pub id: SnapshotId,
    /// Storage availability requested for this reusable snapshot.
    ///
    /// The value is persisted with the record so readers do not infer
    /// availability from the configured repository backend.
    pub snapshot_type: SnapshotType,
    /// Serving Pod identity that owns the immutable bytes for a Local snapshot.
    ///
    /// Distributed snapshots and template records never carry placement.
    pub owner_node_id: Option<String>,
    pub alias: Option<SnapshotAlias>,
    pub source: SnapshotPublishSource,
    pub context: CommandContext,
    pub startup: Option<StartupCommand>,
    pub resources: SandboxResources,
    pub runtime_versions: SnapshotRuntimeVersions,
    pub virtualization_mode: VirtualizationMode,
    pub image_configs: ImageConfigs,
    /// Opaque user-provided JSON passed through to the custom extension hooks.
    /// Template launches inherit it unless overridden at create time.
    pub custom_extension_params: Option<CustomExtensionParams>,
}

#[cfg(test)]
impl SnapshotPublishMetadata {
    pub fn mock() -> Self {
        Self {
            id: SnapshotId::generate(),
            snapshot_type: SnapshotType::Distributed,
            owner_node_id: None,
            alias: None,
            source: SnapshotPublishSource::Template,
            context: CommandContext::default(),
            startup: None,
            resources: SandboxResources::default(),
            runtime_versions: SnapshotRuntimeVersions {
                kernel_version: "kernel".to_string(),
                firecracker_version: "firecracker".to_string(),
                envd_version: "envd".to_string(),
                tools_drive_version: "0.1.0".to_string(),
            },
            virtualization_mode: crate::cfg::ConfigManager::global_config().virtualization_mode,
            image_configs: ImageConfigs::new(),
            custom_extension_params: None,
        }
    }
}

/// Storage availability of a committed reusable snapshot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotType {
    Local,
    #[default]
    Distributed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SnapshotPublishSource {
    Template,
    Sandbox { source_sandbox_id: String },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SnapshotSourceKind {
    Template,
    Sandbox,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TemplateBuildStatus {
    Waiting,
    Building,
    Ready,
    Error,
}

#[derive(Clone, Debug, Serialize)]
pub struct TemplateBuildErrorReason {
    pub message: String,
    pub step: Option<String>,
}

impl<'de> Deserialize<'de> for TemplateBuildErrorReason {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Reason {
            Structured {
                message: String,
                #[serde(default)]
                step: Option<String>,
            },
            LegacyString(String),
        }

        match Reason::deserialize(deserializer)? {
            Reason::Structured { message, step } => Ok(Self { message, step }),
            Reason::LegacyString(message) => Ok(Self::new(message)),
        }
    }
}

impl TemplateBuildErrorReason {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            step: None,
        }
    }

    pub fn with_step(message: impl Into<String>, step: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            step: Some(step.into()),
        }
    }
}

impl fmt::Display for TemplateBuildErrorReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TemplateBuildInfo {
    pub status: TemplateBuildStatus,
    pub started_at_unix_ms: Option<i64>,
    pub finished_at_unix_ms: Option<i64>,
    pub error_reason: Option<TemplateBuildErrorReason>,
}

impl TemplateBuildInfo {
    pub fn waiting() -> Self {
        Self {
            status: TemplateBuildStatus::Waiting,
            started_at_unix_ms: None,
            finished_at_unix_ms: None,
            error_reason: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SnapshotSource {
    Template { build: TemplateBuildInfo },
    Sandbox { source_sandbox_id: String },
}

impl SnapshotSource {
    pub fn kind(&self) -> SnapshotSourceKind {
        match self {
            Self::Template { .. } => SnapshotSourceKind::Template,
            Self::Sandbox { .. } => SnapshotSourceKind::Sandbox,
        }
    }
}

fn validate_snapshot_metadata(
    snapshot_type: SnapshotType,
    source_is_sandbox: bool,
    owner_node_id: Option<&str>,
    alias: Option<&SnapshotAlias>,
) -> Result<(), String> {
    if snapshot_type == SnapshotType::Local && !source_is_sandbox {
        return Err("Local snapshots must originate from a sandbox".to_string());
    }
    if snapshot_type == SnapshotType::Local
        && owner_node_id.is_none_or(|owner| owner.trim().is_empty())
    {
        return Err("Local snapshots require owner_node_id".to_string());
    }
    if snapshot_type == SnapshotType::Distributed && owner_node_id.is_some() {
        return Err("Distributed snapshots must not carry owner_node_id".to_string());
    }
    if snapshot_type == SnapshotType::Local && alias.is_some() {
        return Err("Local snapshots do not support aliases".to_string());
    }
    Ok(())
}

impl SnapshotPublishMetadata {
    pub(crate) fn validate(&self) -> Result<(), String> {
        validate_snapshot_metadata(
            self.snapshot_type,
            matches!(&self.source, SnapshotPublishSource::Sandbox { .. }),
            self.owner_node_id.as_deref(),
            self.alias.as_ref(),
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandContext {
    pub env_vars: HashMap<String, String>,
    pub workdir: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exposed_ports: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entrypoint: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cmd: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volumes: Vec<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub labels: HashMap<String, String>,
}

impl Default for CommandContext {
    fn default() -> Self {
        Self {
            env_vars: HashMap::new(),
            workdir: "/".to_string(),
            user: None,
            exposed_ports: Vec::new(),
            entrypoint: None,
            cmd: None,
            volumes: Vec::new(),
            labels: HashMap::new(),
        }
    }
}

impl CommandContext {
    pub fn new(env_vars: HashMap<String, String>, workdir: impl Into<String>) -> Self {
        Self {
            env_vars,
            workdir: normalize_workdir(workdir.into()),
            ..Self::default()
        }
    }

    pub fn from_env_and_workdir(
        env_vars: HashMap<String, String>,
        workdir: Option<String>,
    ) -> Self {
        Self::new(env_vars, workdir.unwrap_or_default())
    }

    pub fn with_env_var(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env_vars.insert(key.into(), value.into());
        self
    }

    pub fn with_env_overrides(mut self, overrides: HashMap<String, String>) -> Self {
        self.env_vars.extend(overrides);
        self
    }

    pub fn with_workdir(mut self, workdir: impl Into<String>) -> Self {
        self.workdir = normalize_workdir(workdir.into());
        self
    }

    pub fn with_user(mut self, user: Option<String>) -> Self {
        self.user = user;
        self
    }

    pub fn with_exposed_ports(mut self, ports: Vec<String>) -> Self {
        self.exposed_ports = ports;
        self
    }

    pub fn with_entrypoint(mut self, entrypoint: Option<Vec<String>>) -> Self {
        self.entrypoint = entrypoint;
        self
    }

    pub fn with_cmd(mut self, cmd: Option<Vec<String>>) -> Self {
        self.cmd = cmd;
        self
    }

    pub fn with_volumes(mut self, volumes: Vec<String>) -> Self {
        self.volumes = volumes;
        self
    }

    pub fn with_labels(mut self, labels: HashMap<String, String>) -> Self {
        self.labels = labels;
        self
    }

    /// Returns a shell-safe command string combining entrypoint and cmd, or `None` if both are
    /// absent or empty. The result is suitable for passing to `bash -lc`.
    pub fn effective_start_cmd(&self) -> Option<String> {
        let entrypoint = self.entrypoint.as_deref().unwrap_or(&[]);
        let cmd = self.cmd.as_deref().unwrap_or(&[]);
        let parts: Vec<_> = entrypoint.iter().chain(cmd.iter()).collect();
        if parts.is_empty() {
            return None;
        }
        Some(
            parts
                .iter()
                .map(|s| shell_quote(s))
                .collect::<Vec<_>>()
                .join(" "),
        )
    }
}

fn normalize_workdir(workdir: String) -> String {
    if workdir.trim().is_empty() {
        "/".to_string()
    } else {
        workdir
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartupCommand {
    pub start_cmd: String,
    pub ready_cmd: String,
    pub context: CommandContext,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommittedSnapshot {
    pub context: CommandContext,
    pub startup: Option<StartupCommand>,
    pub runtime_versions: SnapshotRuntimeVersions,
    /// Node virtualization ABI used to capture this snapshot.
    #[serde(default)]
    pub virtualization_mode: VirtualizationMode,
    #[serde(default, skip_serializing_if = "ImageConfigs::is_empty")]
    pub image_configs: ImageConfigs,
    pub rootfs_layers: Vec<OverlaybdLayerRef>,
    pub attached_drives: Vec<CommittedAttachedDrive>,
    /// Managed overlaybd layers for the memory snapshot image, ordered bottom-up.
    pub memory_layers: Vec<ManagedLayer>,
    #[serde(default)]
    pub disk_publications: Vec<PersistedDiskImagePublication>,
    /// Opaque user-provided JSON passed through to the custom extension hooks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_extension_params: Option<CustomExtensionParams>,
}

#[cfg(test)]
impl CommittedSnapshot {
    pub fn mock() -> Self {
        Self {
            context: CommandContext::default(),
            startup: None,
            runtime_versions: SnapshotRuntimeVersions {
                kernel_version: "kernel".to_string(),
                firecracker_version: "firecracker".to_string(),
                envd_version: "envd".to_string(),
                tools_drive_version: "0.1.0".to_string(),
            },
            virtualization_mode: crate::cfg::ConfigManager::global_config().virtualization_mode,
            image_configs: ImageConfigs::new(),
            rootfs_layers: Vec::new(),
            attached_drives: Vec::new(),
            memory_layers: Vec::new(),
            disk_publications: Vec::new(),
            custom_extension_params: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotRecord {
    pub id: SnapshotId,
    /// Storage availability of the committed reusable snapshot.
    ///
    /// Older records did not carry this field; those records are the legacy
    /// durable (distributed) form.
    #[serde(default)]
    pub snapshot_type: SnapshotType,
    /// Placement for Local snapshot bytes. The owner is the publishing Pod;
    /// if that Pod is deleted, the Local snapshot is unavailable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_node_id: Option<String>,
    pub alias: Option<SnapshotAlias>,
    pub source: SnapshotSource,
    pub resources: SandboxResources,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    pub committed: Option<CommittedSnapshot>,
}

impl SnapshotRecord {
    pub(crate) fn new_committed(
        metadata: &SnapshotPublishMetadata,
        committed: CommittedSnapshot,
        now_unix_ms: i64,
    ) -> Self {
        let source = match &metadata.source {
            SnapshotPublishSource::Template => SnapshotSource::Template {
                build: TemplateBuildInfo {
                    status: TemplateBuildStatus::Ready,
                    started_at_unix_ms: None,
                    finished_at_unix_ms: Some(now_unix_ms),
                    error_reason: None,
                },
            },
            SnapshotPublishSource::Sandbox { source_sandbox_id } => SnapshotSource::Sandbox {
                source_sandbox_id: source_sandbox_id.clone(),
            },
        };
        Self {
            id: metadata.id.clone(),
            snapshot_type: metadata.snapshot_type,
            owner_node_id: metadata.owner_node_id.clone(),
            alias: metadata.alias.clone(),
            source,
            resources: metadata.resources,
            created_at_unix_ms: now_unix_ms,
            updated_at_unix_ms: now_unix_ms,
            committed: Some(committed),
        }
    }

    pub(crate) fn validate_committed_metadata(&self) -> Result<(), String> {
        if self.committed.is_none() {
            return Err("committed snapshot metadata requires an artifact payload".to_string());
        }
        validate_snapshot_metadata(
            self.snapshot_type,
            matches!(&self.source, SnapshotSource::Sandbox { .. }),
            self.owner_node_id.as_deref(),
            self.alias.as_ref(),
        )
    }

    pub(crate) fn validate_template_create(&self) -> Result<(), String> {
        if !matches!(&self.source, SnapshotSource::Template { .. }) {
            return Err("only template snapshots can be pre-created".to_string());
        }
        if self.committed.is_some() {
            return Err("pre-created template snapshots must not already be committed".to_string());
        }
        Ok(())
    }

    pub(crate) fn start_template_build(&mut self, now_unix_ms: i64) -> Result<(), String> {
        let SnapshotSource::Template { build } = &mut self.source else {
            return Err(format!("snapshot '{}' is not a template build", self.id));
        };
        if build.status != TemplateBuildStatus::Waiting {
            return Err(format!(
                "template build '{}' is not in waiting state",
                self.id
            ));
        }

        build.status = TemplateBuildStatus::Building;
        build.started_at_unix_ms = Some(now_unix_ms);
        build.error_reason = None;
        self.updated_at_unix_ms = now_unix_ms;
        Ok(())
    }

    pub(crate) fn mark_template_build_error(
        &mut self,
        reason: &TemplateBuildErrorReason,
        now_unix_ms: i64,
    ) -> Result<(), String> {
        let SnapshotSource::Template { build } = &mut self.source else {
            return Err(format!("snapshot '{}' is not a template build", self.id));
        };
        build.status = TemplateBuildStatus::Error;
        build.finished_at_unix_ms = Some(now_unix_ms);
        build.error_reason = Some(reason.clone());
        self.updated_at_unix_ms = now_unix_ms;
        Ok(())
    }

    pub fn template_waiting(
        id: SnapshotId,
        alias: Option<SnapshotAlias>,
        resources: SandboxResources,
    ) -> Self {
        let now_unix_ms = now_unix_ms();
        Self {
            id,
            snapshot_type: SnapshotType::Distributed,
            owner_node_id: None,
            alias,
            source: SnapshotSource::Template {
                build: TemplateBuildInfo::waiting(),
            },
            resources,
            created_at_unix_ms: now_unix_ms,
            updated_at_unix_ms: now_unix_ms,
            committed: None,
        }
    }

    pub fn mark_committed(
        &mut self,
        metadata: &SnapshotPublishMetadata,
        committed: CommittedSnapshot,
        now_unix_ms: i64,
    ) {
        if let SnapshotPublishSource::Sandbox { source_sandbox_id } = &metadata.source {
            self.source = SnapshotSource::Sandbox {
                source_sandbox_id: source_sandbox_id.clone(),
            };
        }
        if let SnapshotSource::Template { build } = &mut self.source {
            build.status = TemplateBuildStatus::Ready;
            build.finished_at_unix_ms = Some(now_unix_ms);
            build.error_reason = None;
        }
        self.alias = metadata.alias.clone();
        self.snapshot_type = metadata.snapshot_type;
        self.owner_node_id = metadata.owner_node_id.clone();
        self.resources = metadata.resources;
        self.updated_at_unix_ms = now_unix_ms;
        self.committed = Some(committed);
    }

    /// Returns the published rootfs OCI image reference, if source-registry
    /// image publication produced one for this snapshot.
    pub(crate) fn published_rootfs_image_ref(&self) -> Option<&str> {
        let committed = self.committed.as_ref()?;
        let expected_tag = rootfs_snapshot_image_tag(&self.id);
        committed
            .disk_publications
            .iter()
            .find(|publication| publication.tag == expected_tag)
            .map(|publication| publication.image_ref.as_str())
    }

    #[cfg(test)]
    pub fn mock_ready(committed: CommittedSnapshot) -> Self {
        Self {
            id: SnapshotId::generate(),
            snapshot_type: SnapshotType::Distributed,
            owner_node_id: None,
            alias: None,
            source: SnapshotSource::Template {
                build: TemplateBuildInfo {
                    status: TemplateBuildStatus::Ready,
                    started_at_unix_ms: None,
                    finished_at_unix_ms: Some(0),
                    error_reason: None,
                },
            },
            resources: SandboxResources::default(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
            committed: Some(committed),
        }
    }
}

pub(crate) fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OverlaybdLayerRef {
    Managed(ManagedLayer),
    External(ExternalLayer),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedLayer {
    pub digest: String,
    pub size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uuid: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalLayer {
    pub digest: String,
    pub repo_blob_url: String,
    pub size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedDiskImagePublication {
    pub image_ref: String,
    pub tag: String,
    pub manifest_digest: String,
    pub repo_blob_url: String,
}

const SNAPSHOT_IMAGE_TAG_PREFIX: &str = "agentenv-snapshot-";

/// OCI tag used when publishing a snapshot rootfs image to its source registry.
pub(crate) fn rootfs_snapshot_image_tag(snapshot_id: &SnapshotId) -> String {
    format!("{SNAPSHOT_IMAGE_TAG_PREFIX}{snapshot_id}")
}

pub(crate) type RuntimeArtifactLease = dyn Send + Sync;

#[cfg(test)]
fn default_runtime_artifact_lease() -> Arc<RuntimeArtifactLease> {
    static INSTANCE: OnceLock<Arc<RuntimeArtifactLease>> = OnceLock::new();
    INSTANCE.get_or_init(|| Arc::new(())).clone()
}

#[derive(Clone)]
/// Runtime-ready snapshot with node-local artifact paths.
pub struct RunnableSnapshot {
    record: SnapshotRecord,
    manifest: FirecrackerSnapshotManifest,
    _lease: Arc<RuntimeArtifactLease>,
}

impl RunnableSnapshot {
    pub(crate) fn new(
        record: SnapshotRecord,
        manifest: FirecrackerSnapshotManifest,
        lease: Arc<RuntimeArtifactLease>,
    ) -> Self {
        Self {
            record,
            manifest,
            _lease: lease,
        }
    }

    /// Returns the runtime-resolved attached drives for this snapshot.
    pub fn attached_drives(&self) -> Vec<ResolvedAttachedDrive> {
        self.manifest
            .attached_drives
            .iter()
            .map(|drive| ResolvedAttachedDrive::Overlaybd {
                drive_id: drive.drive_id.clone(),
                image_config_path: drive.image_config_path.clone(),
                read_only: drive.read_only,
                virtual_size: drive.virtual_size,
                mount_path: crate::sandbox::normalize_mount_path_or_default(
                    &drive.drive_id,
                    drive.mount_path.clone(),
                ),
                sub_path: drive.sub_path.clone(),
            })
            .collect()
    }

    pub fn manifest(&self) -> &FirecrackerSnapshotManifest {
        &self.manifest
    }

    /// Returns the committed snapshot record backing this runnable snapshot.
    pub fn record(&self) -> &SnapshotRecord {
        &self.record
    }

    /// Returns the committed snapshot artifact payload backing this runnable snapshot.
    pub fn committed(&self) -> &CommittedSnapshot {
        self.record
            .committed
            .as_ref()
            .expect("runnable snapshots always have committed artifact payloads")
    }

    /// Returns the CPU and memory settings for this runnable snapshot.
    pub fn resources(&self) -> &SandboxResources {
        &self.record.resources
    }

    #[cfg(test)]
    pub fn mock() -> Self {
        Self::from_test_manifest(
            SnapshotRecord::mock_ready(CommittedSnapshot::mock()),
            Vec::new(),
        )
    }

    #[cfg(test)]
    pub(crate) fn from_test_manifest(
        record: SnapshotRecord,
        attached_drives: Vec<ResolvedAttachedDrive>,
    ) -> Self {
        let extra_drives: Vec<crate::sandbox::ExtraDrive> = attached_drives
            .iter()
            .map(ResolvedAttachedDrive::to_extra_drive)
            .collect();

        Self {
            record,
            manifest: FirecrackerSnapshotManifest::for_test(0, &extra_drives),
            _lease: default_runtime_artifact_lease(),
        }
    }
}

impl fmt::Debug for RunnableSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunnableSnapshot")
            .field("record", &self.record)
            .field("manifest", &self.manifest)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        rootfs_snapshot_image_tag, CommandContext, CommittedSnapshot, ManagedLayer,
        PersistedDiskImagePublication, SnapshotAlias, SnapshotPublishMetadata,
        SnapshotPublishSource, SnapshotRecord, SnapshotType, TemplateBuildErrorReason,
    };
    use std::collections::HashMap;

    fn publication(
        image_ref: impl Into<String>,
        tag: impl Into<String>,
    ) -> PersistedDiskImagePublication {
        PersistedDiskImagePublication {
            image_ref: image_ref.into(),
            tag: tag.into(),
            manifest_digest: "sha256:manifest".to_string(),
            repo_blob_url: "https://registry.example/v2/ns/app/blobs".to_string(),
        }
    }

    #[test]
    fn snapshot_record_without_tools_drive_version_remains_readable() {
        let record = SnapshotRecord::mock_ready(CommittedSnapshot::mock());
        let mut value = serde_json::to_value(record).expect("serialize snapshot record");
        value["committed"]["runtime_versions"]
            .as_object_mut()
            .expect("runtime versions must be an object")
            .remove("tools_drive_version");

        let record: SnapshotRecord =
            serde_json::from_value(value).expect("deserialize legacy snapshot record");

        assert!(record
            .committed
            .expect("snapshot must remain committed")
            .runtime_versions
            .tools_drive_version
            .is_empty());
    }

    #[test]
    fn snapshot_record_without_snapshot_type_defaults_to_distributed() {
        let record = SnapshotRecord::mock_ready(CommittedSnapshot::mock());
        let mut value = serde_json::to_value(record).expect("serialize snapshot record");
        value
            .as_object_mut()
            .expect("snapshot record must serialize as an object")
            .remove("snapshot_type");

        let record: SnapshotRecord =
            serde_json::from_value(value).expect("deserialize legacy snapshot record");

        assert_eq!(record.snapshot_type, SnapshotType::Distributed);
    }

    #[test]
    fn snapshot_type_uses_stable_snake_case_values() {
        assert_eq!(
            serde_json::to_value(SnapshotType::Local).expect("serialize local type"),
            serde_json::json!("local")
        );
        assert_eq!(
            serde_json::from_str::<SnapshotType>("\"distributed\"")
                .expect("deserialize distributed type"),
            SnapshotType::Distributed
        );
    }

    #[test]
    fn local_snapshot_metadata_rejects_alias() {
        let mut metadata = SnapshotPublishMetadata::mock();
        metadata.snapshot_type = SnapshotType::Local;
        metadata.owner_node_id = Some("node-a".to_string());
        metadata.alias = Some(SnapshotAlias::parse("checkpoint").expect("valid alias"));
        metadata.source = SnapshotPublishSource::Sandbox {
            source_sandbox_id: "sandbox-a".to_string(),
        };

        assert_eq!(
            metadata.validate().expect_err("Local alias must fail"),
            "Local snapshots do not support aliases"
        );
        metadata.alias = None;
        metadata.validate().expect("aliasless Local must be valid");
    }

    #[test]
    fn managed_layer_uuid_serde_is_backward_compatible() {
        let legacy = r#"{"digest":"sha256:abc","size":123}"#;
        let layer: ManagedLayer = serde_json::from_str(legacy).expect("parse legacy layer");
        assert_eq!(layer.uuid, None);

        let with_uuid =
            r#"{"digest":"sha256:abc","size":123,"uuid":"11111111-2222-3333-4444-555555555555"}"#;
        let layer: ManagedLayer = serde_json::from_str(with_uuid).expect("parse uuid layer");
        assert_eq!(
            layer.uuid.as_deref(),
            Some("11111111-2222-3333-4444-555555555555")
        );
        let value = serde_json::to_value(&layer).expect("serialize uuid layer");
        assert_eq!(value["uuid"], "11111111-2222-3333-4444-555555555555");
    }

    #[test]
    fn returns_published_rootfs_image_ref_by_exact_tag() {
        let mut record = SnapshotRecord::mock_ready(CommittedSnapshot::mock());
        let rootfs_tag = rootfs_snapshot_image_tag(&record.id);
        let expected = format!("registry.example/ns/app:{rootfs_tag}");
        record.committed.as_mut().unwrap().disk_publications = vec![
            publication(
                "registry.example/ns/app:drive",
                format!("{rootfs_tag}-drive-data-0123456789ab"),
            ),
            publication(expected.clone(), rootfs_tag),
        ];

        assert_eq!(record.published_rootfs_image_ref(), Some(expected.as_str()));
    }

    #[test]
    fn drive_only_publication_has_no_rootfs_image_ref() {
        let mut record = SnapshotRecord::mock_ready(CommittedSnapshot::mock());
        let rootfs_tag = rootfs_snapshot_image_tag(&record.id);
        record.committed.as_mut().unwrap().disk_publications = vec![publication(
            "registry.example/ns/app:drive",
            format!("{rootfs_tag}-drive-data-0123456789ab"),
        )];

        assert_eq!(record.published_rootfs_image_ref(), None);
    }

    #[test]
    fn command_context_normalizes_workdir_and_applies_env_overrides() {
        let context = CommandContext::from_env_and_workdir(
            HashMap::from([
                ("BASE".to_string(), "1".to_string()),
                ("SHARED".to_string(), "base".to_string()),
            ]),
            Some("  ".to_string()),
        )
        .with_env_overrides(HashMap::from([
            ("SHARED".to_string(), "override".to_string()),
            ("ADDED".to_string(), "2".to_string()),
        ]))
        .with_workdir("/workspace");

        assert_eq!(context.workdir, "/workspace");
        assert_eq!(context.env_vars.get("BASE").map(String::as_str), Some("1"));
        assert_eq!(
            context.env_vars.get("SHARED").map(String::as_str),
            Some("override")
        );
        assert_eq!(context.env_vars.get("ADDED").map(String::as_str), Some("2"));
    }

    #[test]
    fn effective_start_cmd_composition() {
        let ctx = CommandContext::default()
            .with_entrypoint(Some(vec!["/docker-entrypoint.sh".to_string()]))
            .with_cmd(Some(vec![
                "nginx".to_string(),
                "-g".to_string(),
                "daemon off;".to_string(),
            ]));
        assert_eq!(
            ctx.effective_start_cmd().as_deref(),
            Some("/docker-entrypoint.sh nginx -g 'daemon off;'"),
        );

        let ctx = CommandContext::default().with_entrypoint(Some(vec!["node".to_string()]));
        assert_eq!(ctx.effective_start_cmd().as_deref(), Some("node"));

        let ctx = CommandContext::default()
            .with_cmd(Some(vec!["python3".to_string(), "app.py".to_string()]));
        assert_eq!(ctx.effective_start_cmd().as_deref(), Some("python3 app.py"),);

        assert_eq!(CommandContext::default().effective_start_cmd(), None);

        let ctx = CommandContext::default()
            .with_entrypoint(Some(vec![]))
            .with_cmd(Some(vec![]));
        assert_eq!(ctx.effective_start_cmd(), None);
    }

    #[test]
    fn template_build_error_reason_reads_legacy_string() {
        let reason: TemplateBuildErrorReason =
            serde_json::from_str(r#""legacy failure""#).expect("deserialize legacy reason");

        assert_eq!(reason.message, "legacy failure");
        assert_eq!(reason.step, None);
    }

    #[test]
    fn template_build_error_reason_reads_structured_reason() {
        let reason: TemplateBuildErrorReason =
            serde_json::from_str(r#"{"message":"boom","step":"resolve image"}"#)
                .expect("deserialize structured reason");

        assert_eq!(reason.message, "boom");
        assert_eq!(reason.step.as_deref(), Some("resolve image"));
    }
}
