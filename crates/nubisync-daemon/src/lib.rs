//! NubiSync synchronization orchestration owned by the daemon layer.

#![forbid(unsafe_code)]

use nubisync_core::{
    ChangeCursor, ChangePage, ContinuationToken, LocalItemKind, LocalItemSnapshot, RemoteChange,
    RemoteItem, RemoteItemKind, SyncMode, SyncRoot,
};
use nubisync_drive::{
    DriveApiError, DriveBlobFingerprint, DriveFolderRoot, DriveRootMembership, GoogleDriveApi,
};
use nubisync_storage::{
    LocalChangeEventInput, LocalChangeEventKind, LocalChangeEventRecord, LocalChangeJournalCommit,
    RemoteWriteAuthoritySnapshot, RemoteWriteFileCreateCandidate,
    RemoteWriteFileCreateSettlementInput, RemoteWriteFolderCreateCandidate,
    RemoteWriteFolderCreateSettlementInput, RemoteWriteIntentInput, RemoteWriteIntentOperation,
    RemoteWriteIntentStatus, Storage, StorageError, SyncRootCatalogBatchCommit,
    SyncRootCatalogMutation, SyncRootDirectoryMaterializationReceipt,
    SyncRootFileMaterializationReceipt,
};
use nubisync_sync::{
    LocalTreeEntry, LocalTreeEntryKind, ReceiveOnlyConvergenceActionKind,
    ReceiveOnlyConvergencePlan, ReceiveOnlyConvergencePlanError, ReceiveOnlyDirectoryTarget,
    ReceiveOnlyFileTarget, ReceiveOnlyMaterializationPlan, ReceiveOnlyMaterializationPlanError,
    ReceiveOnlyOwnershipReceipt, ReceiveOnlyReceiptState, RootCatalogMutationPlan,
    RootCatalogProjection, RootCatalogProjectionError, RootCatalogResolution, RootChangeMembership,
    plan_receive_only_directory_targets, plan_receive_only_existing_file_targets,
    plan_receive_only_materialization, plan_receive_only_missing_directory_targets,
    plan_receive_only_missing_file_targets,
};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    fmt, fs,
    io::{Read, Write},
    os::unix::fs::MetadataExt,
    path::{Component, Path, PathBuf},
    sync::{
        Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};
use thiserror::Error;

#[derive(Clone, PartialEq, Eq)]
pub struct SelectedRootFolderCreateLocalValidation {
    leaf_name: String,
}

impl SelectedRootFolderCreateLocalValidation {
    pub fn leaf_name(&self) -> &str {
        &self.leaf_name
    }
}

impl fmt::Debug for SelectedRootFolderCreateLocalValidation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SelectedRootFolderCreateLocalValidation")
            .field("leaf_name", &"[redacted]")
            .finish()
    }
}

pub struct SelectedRootFileCreateLocalSource {
    file: fs::File,
    absolute_path: PathBuf,
    leaf_name: String,
    total_bytes: u64,
    initial_metadata: fs::Metadata,
    bytes_read: u64,
    hasher: Sha256,
}

impl SelectedRootFileCreateLocalSource {
    pub fn leaf_name(&self) -> &str {
        &self.leaf_name
    }

    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    pub fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    pub fn read_next_chunk(
        &mut self,
        max_bytes: usize,
    ) -> Result<Option<Vec<u8>>, SelectedRootExecutorError> {
        if max_bytes == 0 {
            return Err(SelectedRootExecutorError::RemoteWriteFileCreateChunkSizeInvalid);
        }
        let remaining = self
            .total_bytes
            .checked_sub(self.bytes_read)
            .ok_or(SelectedRootExecutorError::RemoteWriteFileCreateStreamIncomplete)?;
        if remaining == 0 {
            return Ok(None);
        }

        let requested = remaining.min(max_bytes as u64);
        let requested =
            usize::try_from(requested).map_err(|_| SelectedRootExecutorError::CountOverflow)?;
        let mut chunk = vec![0_u8; requested];
        self.file
            .read_exact(&mut chunk)
            .map_err(|_| SelectedRootExecutorError::RemoteWriteFileCreateReadFailed)?;
        self.hasher.update(&chunk);
        self.bytes_read = self
            .bytes_read
            .checked_add(requested as u64)
            .ok_or(SelectedRootExecutorError::CountOverflow)?;
        Ok(Some(chunk))
    }

    pub fn completed_sha256_hex(&self) -> Result<String, SelectedRootExecutorError> {
        if self.bytes_read != self.total_bytes {
            return Err(SelectedRootExecutorError::RemoteWriteFileCreateStreamIncomplete);
        }
        Ok(digest_to_hex(self.hasher.clone().finalize().as_slice()))
    }

    pub fn finish(self) -> Result<SelectedRootFileCreateStreamResult, SelectedRootExecutorError> {
        if self.bytes_read != self.total_bytes {
            return Err(SelectedRootExecutorError::RemoteWriteFileCreateStreamIncomplete);
        }

        let open_after = self
            .file
            .metadata()
            .map_err(|_| SelectedRootExecutorError::RemoteWriteFileCreateReadFailed)?;
        let path_after = fs::symlink_metadata(&self.absolute_path)
            .map_err(|_| SelectedRootExecutorError::RemoteWriteFileCreateLocalIdentityMismatch)?;

        let source_stable = same_local_file_state(&self.initial_metadata, &open_after)
            && same_local_file_state(&self.initial_metadata, &path_after);

        Ok(SelectedRootFileCreateStreamResult {
            bytes_streamed: self.bytes_read,
            sha256_hex: digest_to_hex(self.hasher.finalize().as_slice()),
            source_stable,
        })
    }
}

impl fmt::Debug for SelectedRootFileCreateLocalSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SelectedRootFileCreateLocalSource")
            .field("absolute_path", &"[redacted]")
            .field("leaf_name", &"[redacted]")
            .field("total_bytes", &self.total_bytes)
            .field("bytes_read", &self.bytes_read)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct SelectedRootFileCreateStreamResult {
    pub bytes_streamed: u64,
    sha256_hex: String,
    pub source_stable: bool,
}

impl SelectedRootFileCreateStreamResult {
    pub fn sha256_hex(&self) -> &str {
        &self.sha256_hex
    }
}

impl fmt::Debug for SelectedRootFileCreateStreamResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SelectedRootFileCreateStreamResult")
            .field("bytes_streamed", &self.bytes_streamed)
            .field("sha256_hex", &"[redacted]")
            .field("source_stable", &self.source_stable)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectedRootLocalDiffKind {
    Created,
    Deleted,
    Modified,
    TypeChanged,
}

impl SelectedRootLocalDiffKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Deleted => "deleted",
            Self::Modified => "modified",
            Self::TypeChanged => "type_changed",
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct SelectedRootLocalDiffEntry {
    relative_path: String,
    pub kind: SelectedRootLocalDiffKind,
    pub baseline_kind: Option<LocalItemKind>,
    pub current_kind: Option<LocalItemKind>,
}

impl SelectedRootLocalDiffEntry {
    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }
}

impl fmt::Debug for SelectedRootLocalDiffEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SelectedRootLocalDiffEntry")
            .field("relative_path", &"[redacted]")
            .field("kind", &self.kind)
            .field("baseline_kind", &self.baseline_kind)
            .field("current_kind", &self.current_kind)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedRootLocalInventoryDiff {
    pub baseline_items: usize,
    pub baseline_generation: u64,
    pub baseline_snapshot_completed_at_unix_ms: Option<i64>,
    pub observed_items: usize,
    pub created: usize,
    pub deleted: usize,
    pub modified: usize,
    pub type_changed: usize,
    entries: Vec<SelectedRootLocalDiffEntry>,
}

impl SelectedRootLocalInventoryDiff {
    pub fn action_count(&self) -> usize {
        self.entries.len()
    }

    pub fn entries(&self) -> &[SelectedRootLocalDiffEntry] {
        &self.entries
    }

    pub fn clean(&self) -> bool {
        self.entries.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectedRootRemoteWritePlanDisposition {
    Ready,
    NeedsPredeterminedRemoteId,
    Conflict,
    BlockedIdentity,
    BlockedAuthority,
}

impl SelectedRootRemoteWritePlanDisposition {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::NeedsPredeterminedRemoteId => "needs_predetermined_remote_id",
            Self::Conflict => "conflict",
            Self::BlockedIdentity => "blocked_identity",
            Self::BlockedAuthority => "blocked_authority",
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct SelectedRootRemoteWritePlanEntry {
    pub source_local_event_id: i64,
    relative_path: String,
    pub operation: Option<RemoteWriteIntentOperation>,
    pub disposition: SelectedRootRemoteWritePlanDisposition,
    pub local_kind: Option<LocalItemKind>,
    pub local_size_bytes: Option<u64>,
    pub local_modified_unix_ns: Option<i64>,
    pub local_device_id: Option<u64>,
    pub local_inode: Option<u64>,
    target_remote_id: Option<String>,
    expected_parent_remote_id: Option<String>,
    pub expected_remote_kind: Option<RemoteItemKind>,
    pub expected_remote_version: Option<u64>,
    pub expected_remote_size_bytes: Option<u64>,
    expected_checksum_algorithm: Option<String>,
    expected_content_checksum: Option<String>,
}

impl SelectedRootRemoteWritePlanEntry {
    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }

    pub fn create_intent_input(
        &self,
        baseline_generation: u64,
        predetermined_remote_id: String,
        planned_at_unix_ms: i64,
    ) -> Result<RemoteWriteIntentInput, SelectedRootExecutorError> {
        if self.disposition != SelectedRootRemoteWritePlanDisposition::NeedsPredeterminedRemoteId {
            return Err(SelectedRootExecutorError::RemoteWriteCreateIntentNotEligible);
        }

        let operation = self
            .operation
            .ok_or(SelectedRootExecutorError::RemoteWriteCreateIntentNotEligible)?;
        if !matches!(
            operation,
            RemoteWriteIntentOperation::CreateFile | RemoteWriteIntentOperation::CreateFolder
        ) {
            return Err(SelectedRootExecutorError::RemoteWriteCreateIntentNotEligible);
        }

        let local_kind = self
            .local_kind
            .ok_or(SelectedRootExecutorError::RemoteWriteCreateIntentNotEligible)?;
        let expected_parent_remote_id = self
            .expected_parent_remote_id
            .clone()
            .ok_or(SelectedRootExecutorError::RemoteWriteCreateIntentNotEligible)?;

        Ok(RemoteWriteIntentInput::new(
            self.source_local_event_id,
            baseline_generation,
            operation,
            self.relative_path.clone(),
            local_kind,
            self.local_size_bytes,
            self.local_modified_unix_ns,
            self.local_device_id,
            self.local_inode,
            None,
            Some(predetermined_remote_id),
            Some(expected_parent_remote_id),
            None,
            None,
            None,
            None,
            None,
            planned_at_unix_ms,
        )?)
    }
}

impl fmt::Debug for SelectedRootRemoteWritePlanEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SelectedRootRemoteWritePlanEntry")
            .field("source_local_event_id", &self.source_local_event_id)
            .field("relative_path", &"[redacted]")
            .field("operation", &self.operation)
            .field("disposition", &self.disposition)
            .field("local_kind", &self.local_kind)
            .field("local_size_bytes", &self.local_size_bytes)
            .field(
                "target_remote_id",
                &self.target_remote_id.as_deref().map(|_| "[redacted]"),
            )
            .field(
                "expected_parent_remote_id",
                &self
                    .expected_parent_remote_id
                    .as_deref()
                    .map(|_| "[redacted]"),
            )
            .field("expected_remote_kind", &self.expected_remote_kind)
            .field("expected_remote_version", &self.expected_remote_version)
            .field(
                "expected_remote_size_bytes",
                &self.expected_remote_size_bytes,
            )
            .field(
                "expected_checksum_algorithm",
                &self
                    .expected_checksum_algorithm
                    .as_deref()
                    .map(|_| "[redacted]"),
            )
            .field(
                "expected_content_checksum",
                &self
                    .expected_content_checksum
                    .as_deref()
                    .map(|_| "[redacted]"),
            )
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedRootRemoteWriteIntentPlan {
    pub baseline_generation: u64,
    pub pending_events: usize,
    pub create_file_needs_id: usize,
    pub create_folder_needs_id: usize,
    pub update_file_ready: usize,
    pub trash_item_ready: usize,
    pub conflicts: usize,
    pub blocked_identity: usize,
    pub blocked_authority: usize,
    pub root_write_capable: bool,
    pub full_sync_credential_present: bool,
    entries: Vec<SelectedRootRemoteWritePlanEntry>,
}

impl SelectedRootRemoteWriteIntentPlan {
    pub fn entries(&self) -> &[SelectedRootRemoteWritePlanEntry] {
        &self.entries
    }

    pub fn write_gates_satisfied(&self) -> bool {
        self.root_write_capable && self.full_sync_credential_present
    }

    pub fn persistable_existing_intents(&self) -> usize {
        if self.write_gates_satisfied() {
            self.update_file_ready + self.trash_item_ready
        } else {
            0
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedRootLocalBaselineCapture {
    pub items_captured: usize,
    pub files_captured: usize,
    pub directories_captured: usize,
    pub convergence_actions: usize,
    pub snapshot_complete: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedRootDirectoryMaterialization {
    pub remote_directories: usize,
    pub planned_directory_actions: usize,
    pub batch_action_limit: usize,
    pub created_directories: usize,
    pub existing_directories: usize,
    pub pending_files: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedRootDirectoryAdoption {
    pub directories_adopted: usize,
    pub current_directory_receipts: u64,
    pub stale_directory_receipts: u64,
}

pub const SELECTED_ROOT_CONVERGENCE_MAX_ACTIONS: usize = 10_000;
pub const SUPERVISED_RECEIVE_ONLY_RUN_MAX_ROUNDS: usize = 8;
pub const RECEIVE_ONLY_PERIODIC_POLL_INTERVAL_MS: i64 = 30_000;
pub const RECEIVE_ONLY_PERIODIC_BUSY_RETRY_MS: i64 = 5_000;
pub const RECEIVE_ONLY_PERIODIC_ERROR_BACKOFF_INITIAL_MS: i64 = 5_000;
pub const RECEIVE_ONLY_PERIODIC_ERROR_BACKOFF_MAX_MS: i64 = 300_000;
pub const SUPERVISED_FILE_BATCH_MAX_ACTIONS: usize = 64;
pub const SUPERVISED_STALE_FILE_PLAN_MAX_ACTIONS: usize = 64;
pub const SUPERVISED_STALE_DIRECTORY_DELETION_MAX_ACTIONS: usize = 64;
pub const SUPERVISED_FILE_DOWNLOAD_MAX_BYTES: u64 = 16 * 1024 * 1024;
static DOWNLOAD_TEMP_COUNTER: AtomicU64 = AtomicU64::new(1);
static RECEIVE_ONLY_SINGLE_FLIGHT_ROOTS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedRootFileBatchMaterialization {
    pub planned_file_actions: usize,
    pub batch_action_limit: usize,
    pub files_downloaded: usize,
    pub bytes_downloaded: u64,
    pub max_file_bytes: u64,
    pub provider_fingerprints_verified: usize,
    pub receipts_recorded: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedRootFileMaterialization {
    pub files_downloaded: usize,
    pub bytes_downloaded: u64,
    pub max_file_bytes: u64,
    pub size_match_verified: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedRootFileReplacement {
    pub files_replaced: usize,
    pub bytes_downloaded: u64,
    pub stale_baseline_match: bool,
    pub provider_fingerprint_match: bool,
    pub receipt_recorded: bool,
    pub atomic_replace: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedRootStaleFileBatchReplacement {
    pub planned_replacement_actions: usize,
    pub batch_action_limit: usize,
    pub files_replaced: usize,
    pub bytes_downloaded: u64,
    pub max_file_bytes: u64,
    pub provider_fingerprints_verified: usize,
    pub stale_baselines_verified: usize,
    pub receipts_recorded: usize,
    pub atomic_replacements: usize,
    pub replacement_backups_cleaned: usize,
}

#[derive(Clone, PartialEq, Eq)]
pub struct SelectedRootContentFingerprint {
    pub size_bytes: u64,
    sha256_hex: String,
}

impl SelectedRootContentFingerprint {
    fn from_drive(value: DriveBlobFingerprint) -> Self {
        Self {
            size_bytes: value.size_bytes,
            sha256_hex: value.sha256_hex().to_owned(),
        }
    }
}

impl fmt::Debug for SelectedRootContentFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SelectedRootContentFingerprint")
            .field("size_bytes", &self.size_bytes)
            .field("sha256_hex", &"[redacted]")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedRootFileVerification {
    pub files_verified: usize,
    pub bytes_verified: u64,
    pub hash_match: bool,
    pub receipt_recorded: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedRootFileBatchVerification {
    pub planned_verification_actions: usize,
    pub batch_action_limit: usize,
    pub files_verified: usize,
    pub bytes_verified: u64,
    pub max_file_bytes: u64,
    pub remote_content_hashes_verified: usize,
    pub receipts_recorded: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectedRootUnifiedConvergencePhase {
    CreateDirectories,
    MaterializeMissingFiles,
    VerifyExistingFiles,
    ReplaceStaleFiles,
    DeleteStaleFiles,
    DeleteStaleDirectories,
}

impl SelectedRootUnifiedConvergencePhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CreateDirectories => "create_directories",
            Self::MaterializeMissingFiles => "materialize_missing_files",
            Self::VerifyExistingFiles => "verify_existing_files",
            Self::ReplaceStaleFiles => "replace_stale_files",
            Self::DeleteStaleFiles => "delete_stale_files",
            Self::DeleteStaleDirectories => "delete_stale_directories",
        }
    }

    pub fn requires_content_provider(self) -> bool {
        matches!(
            self,
            Self::MaterializeMissingFiles | Self::VerifyExistingFiles | Self::ReplaceStaleFiles
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectedRootUnifiedConvergenceStopReason {
    Converged,
    PhaseCompleted,
    MixedActionClasses,
    BatchActionLimitExceeded,
    BlockedAfterPhase,
}

impl SelectedRootUnifiedConvergenceStopReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Converged => "converged",
            Self::PhaseCompleted => "phase_completed",
            Self::MixedActionClasses => "mixed_action_classes",
            Self::BatchActionLimitExceeded => "batch_action_limit_exceeded",
            Self::BlockedAfterPhase => "blocked_after_phase",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectedRootUnifiedConvergenceDecision {
    Converged,
    Dispatch {
        phase: SelectedRootUnifiedConvergencePhase,
        planned_actions: usize,
    },
    SafeStop {
        reason: SelectedRootUnifiedConvergenceStopReason,
    },
}

impl SelectedRootUnifiedConvergenceDecision {
    pub fn requires_content_provider(self) -> bool {
        match self {
            Self::Dispatch { phase, .. } => phase.requires_content_provider(),
            Self::Converged | Self::SafeStop { .. } => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedRootUnifiedConvergenceExecution {
    pub initial_actions: usize,
    pub phase_executed: Option<SelectedRootUnifiedConvergencePhase>,
    pub phase_actions_planned: usize,
    pub phase_actions_executed: usize,
    pub directories_created: usize,
    pub files_materialized: usize,
    pub files_verified: usize,
    pub files_replaced: usize,
    pub files_deleted: usize,
    pub directories_deleted: usize,
    pub bytes_downloaded: u64,
    pub bytes_verified: u64,
    pub receipts_recorded: usize,
    pub receipts_deleted: usize,
    pub final_actions: usize,
    pub final_blocked_actions: usize,
    pub next_phase: Option<SelectedRootUnifiedConvergencePhase>,
    pub converged: bool,
    pub requires_another_invocation: bool,
    pub manual_intervention_required: bool,
    pub stop_reason: SelectedRootUnifiedConvergenceStopReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectedRootReceiveOnlyCycleMetadataPhase {
    Bootstrap,
    CollectChangePage,
    ExecuteWindow,
}

impl SelectedRootReceiveOnlyCycleMetadataPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Bootstrap => "bootstrap",
            Self::CollectChangePage => "collect_change_page",
            Self::ExecuteWindow => "execute_window",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectedRootReceiveOnlyCycleStopReason {
    MetadataStepCompleted,
    Converged,
    ConvergencePhaseCompleted,
    ManualInterventionRequired,
}

impl SelectedRootReceiveOnlyCycleStopReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MetadataStepCompleted => "metadata_step_completed",
            Self::Converged => "converged",
            Self::ConvergencePhaseCompleted => "convergence_phase_completed",
            Self::ManualInterventionRequired => "manual_intervention_required",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedRootReceiveOnlyCycleExecution {
    pub metadata_phase: SelectedRootReceiveOnlyCycleMetadataPhase,
    pub metadata_authoritative_items: u64,
    pub metadata_page_count: u64,
    pub metadata_change_count: u64,
    pub metadata_catalog_mutations: usize,
    pub metadata_window_complete: bool,
    pub initial_catchup_complete: bool,
    pub convergence_executed: bool,
    pub convergence_phase: Option<SelectedRootUnifiedConvergencePhase>,
    pub convergence_actions_executed: usize,
    pub directories_created: usize,
    pub files_materialized: usize,
    pub files_verified: usize,
    pub files_replaced: usize,
    pub files_deleted: usize,
    pub directories_deleted: usize,
    pub bytes_downloaded: u64,
    pub bytes_verified: u64,
    pub receipts_recorded: usize,
    pub receipts_deleted: usize,
    pub final_actions: usize,
    pub final_blocked_actions: usize,
    pub next_metadata_phase: SelectedRootReceiveOnlyCycleMetadataPhase,
    pub next_convergence_phase: Option<SelectedRootUnifiedConvergencePhase>,
    pub converged: bool,
    pub requires_another_invocation: bool,
    pub manual_intervention_required: bool,
    pub stop_reason: SelectedRootReceiveOnlyCycleStopReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectedRootReceiveOnlyRunStopReason {
    Converged,
    RoundBudgetExhausted,
    ManualInterventionRequired,
}

impl SelectedRootReceiveOnlyRunStopReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Converged => "converged",
            Self::RoundBudgetExhausted => "round_budget_exhausted",
            Self::ManualInterventionRequired => "manual_intervention_required",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedRootReceiveOnlyRunExecution {
    pub max_rounds: usize,
    pub rounds_executed: usize,
    pub metadata_rounds: usize,
    pub convergence_only_rounds: usize,
    pub bootstrap_rounds: usize,
    pub collect_change_page_rounds: usize,
    pub execute_window_rounds: usize,
    pub convergence_phases_executed: usize,
    pub directories_created: usize,
    pub files_materialized: usize,
    pub files_verified: usize,
    pub files_replaced: usize,
    pub files_deleted: usize,
    pub directories_deleted: usize,
    pub bytes_downloaded: u64,
    pub bytes_verified: u64,
    pub receipts_recorded: usize,
    pub receipts_deleted: usize,
    pub final_convergence_known: bool,
    pub final_actions: usize,
    pub final_blocked_actions: usize,
    pub next_metadata_phase: SelectedRootReceiveOnlyCycleMetadataPhase,
    pub next_convergence_phase: Option<SelectedRootUnifiedConvergencePhase>,
    pub converged: bool,
    pub requires_another_invocation: bool,
    pub manual_intervention_required: bool,
    pub stop_reason: SelectedRootReceiveOnlyRunStopReason,
}

pub struct SelectedRootCrossProcessExecutionGuard {
    _file: fs::File,
}

pub enum SelectedRootCrossProcessExecutionLock {
    Acquired(SelectedRootCrossProcessExecutionGuard),
    Busy,
}

pub fn try_acquire_selected_root_cross_process_execution_lock(
    lock_path: &Path,
) -> Result<SelectedRootCrossProcessExecutionLock, SelectedRootExecutorError> {
    let parent = lock_path
        .parent()
        .ok_or(SelectedRootExecutorError::CrossProcessExecutionLockPathInvalid)?;

    let parent_metadata =
        fs::metadata(parent).map_err(|_| SelectedRootExecutorError::CrossProcessExecutionLockIo)?;
    if !parent_metadata.is_dir() {
        return Err(SelectedRootExecutorError::CrossProcessExecutionLockPathInvalid);
    }

    match fs::symlink_metadata(lock_path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(SelectedRootExecutorError::CrossProcessExecutionLockPathInvalid);
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err(SelectedRootExecutorError::CrossProcessExecutionLockIo),
    }

    let file = fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(lock_path)
        .map_err(|_| SelectedRootExecutorError::CrossProcessExecutionLockIo)?;

    if !file
        .metadata()
        .map_err(|_| SelectedRootExecutorError::CrossProcessExecutionLockIo)?
        .is_file()
    {
        return Err(SelectedRootExecutorError::CrossProcessExecutionLockPathInvalid);
    }

    match file.try_lock() {
        Ok(()) => Ok(SelectedRootCrossProcessExecutionLock::Acquired(
            SelectedRootCrossProcessExecutionGuard { _file: file },
        )),
        Err(fs::TryLockError::WouldBlock) => Ok(SelectedRootCrossProcessExecutionLock::Busy),
        Err(fs::TryLockError::Error(_)) => {
            Err(SelectedRootExecutorError::CrossProcessExecutionLockIo)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectedRootReceiveOnlySingleFlightStatus {
    Idle,
    Busy,
}

impl SelectedRootReceiveOnlySingleFlightStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Busy => "busy",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectedRootReceiveOnlySingleFlightResult {
    Busy,
    Executed(SelectedRootReceiveOnlyRunExecution),
}

impl SelectedRootReceiveOnlySingleFlightResult {
    pub fn status(self) -> SelectedRootReceiveOnlySingleFlightStatus {
        match self {
            Self::Busy => SelectedRootReceiveOnlySingleFlightStatus::Busy,
            Self::Executed(_) => SelectedRootReceiveOnlySingleFlightStatus::Idle,
        }
    }
}

struct SelectedRootReceiveOnlyRunSlotGuard {
    sync_root_id: String,
}

impl Drop for SelectedRootReceiveOnlyRunSlotGuard {
    fn drop(&mut self) {
        if let Ok(mut active) = receive_only_single_flight_registry().lock() {
            active.remove(&self.sync_root_id);
        }
    }
}

fn receive_only_single_flight_registry() -> &'static Mutex<HashSet<String>> {
    RECEIVE_ONLY_SINGLE_FLIGHT_ROOTS.get_or_init(|| Mutex::new(HashSet::new()))
}

fn try_acquire_selected_root_receive_only_run_slot(
    sync_root_id: &str,
) -> Result<Option<SelectedRootReceiveOnlyRunSlotGuard>, SelectedRootExecutorError> {
    let mut active = receive_only_single_flight_registry()
        .lock()
        .map_err(|_| SelectedRootExecutorError::ReceiveOnlySingleFlightStatePoisoned)?;

    if !active.insert(sync_root_id.to_owned()) {
        return Ok(None);
    }

    Ok(Some(SelectedRootReceiveOnlyRunSlotGuard {
        sync_root_id: sync_root_id.to_owned(),
    }))
}

pub fn selected_root_receive_only_single_flight_status(
    sync_root_id: &str,
) -> Result<SelectedRootReceiveOnlySingleFlightStatus, SelectedRootExecutorError> {
    let active = receive_only_single_flight_registry()
        .lock()
        .map_err(|_| SelectedRootExecutorError::ReceiveOnlySingleFlightStatePoisoned)?;

    Ok(if active.contains(sync_root_id) {
        SelectedRootReceiveOnlySingleFlightStatus::Busy
    } else {
        SelectedRootReceiveOnlySingleFlightStatus::Idle
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectedRootReceiveOnlyPeriodicDecision {
    Due,
    Waiting { delay_ms: i64 },
    PausedForManualIntervention,
    Shutdown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectedRootPeriodicLocalObservation {
    Journaled(SelectedRootLocalJournalResult),
    BaselineMissing,
    BaselineInvalidated,
    DeferredUntilReceiveOnlyConverged,
}

impl SelectedRootPeriodicLocalObservation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Journaled(_) => "journaled",
            Self::BaselineMissing => "baseline_missing",
            Self::BaselineInvalidated => "baseline_invalidated",
            Self::DeferredUntilReceiveOnlyConverged => "deferred_until_receive_only_converged",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectedRootExecutionBusyScope {
    CrossProcess,
    InProcess,
}

impl SelectedRootExecutionBusyScope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CrossProcess => "cross_process",
            Self::InProcess => "in_process",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectedRootReceiveOnlyPeriodicTick {
    Waiting {
        delay_ms: i64,
    },
    PausedForManualIntervention,
    Shutdown,
    Busy {
        retry_after_ms: i64,
        scope: SelectedRootExecutionBusyScope,
    },
    Executed {
        execution: SelectedRootReceiveOnlyRunExecution,
        local_observation: SelectedRootPeriodicLocalObservation,
        next_delay_ms: Option<i64>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedRootReceiveOnlyPeriodicState {
    next_due_unix_ms: i64,
    consecutive_failures: u32,
    paused_for_manual_intervention: bool,
    shutdown_requested: bool,
}

impl SelectedRootReceiveOnlyPeriodicState {
    pub fn new_immediate(now_unix_ms: i64) -> Self {
        Self {
            next_due_unix_ms: now_unix_ms,
            consecutive_failures: 0,
            paused_for_manual_intervention: false,
            shutdown_requested: false,
        }
    }

    pub fn decision(self, now_unix_ms: i64) -> SelectedRootReceiveOnlyPeriodicDecision {
        if self.shutdown_requested {
            return SelectedRootReceiveOnlyPeriodicDecision::Shutdown;
        }

        if self.paused_for_manual_intervention {
            return SelectedRootReceiveOnlyPeriodicDecision::PausedForManualIntervention;
        }

        if now_unix_ms >= self.next_due_unix_ms {
            SelectedRootReceiveOnlyPeriodicDecision::Due
        } else {
            SelectedRootReceiveOnlyPeriodicDecision::Waiting {
                delay_ms: self.next_due_unix_ms.saturating_sub(now_unix_ms),
            }
        }
    }

    pub fn request_shutdown(&mut self) {
        self.shutdown_requested = true;
    }

    pub fn record_external_failure(&mut self, now_unix_ms: i64) {
        if !self.shutdown_requested && !self.paused_for_manual_intervention {
            self.schedule_failure(now_unix_ms);
        }
    }

    pub fn resume_after_manual_intervention(&mut self, now_unix_ms: i64) {
        if !self.shutdown_requested {
            self.paused_for_manual_intervention = false;
            self.consecutive_failures = 0;
            self.next_due_unix_ms = now_unix_ms;
        }
    }

    pub fn next_due_unix_ms(self) -> i64 {
        self.next_due_unix_ms
    }

    pub fn consecutive_failures(self) -> u32 {
        self.consecutive_failures
    }

    pub fn paused_for_manual_intervention(self) -> bool {
        self.paused_for_manual_intervention
    }

    pub fn shutdown_requested(self) -> bool {
        self.shutdown_requested
    }

    fn schedule_success(&mut self, now_unix_ms: i64) {
        self.consecutive_failures = 0;
        self.next_due_unix_ms = now_unix_ms.saturating_add(RECEIVE_ONLY_PERIODIC_POLL_INTERVAL_MS);
    }

    fn schedule_busy_retry(&mut self, now_unix_ms: i64) {
        self.next_due_unix_ms = now_unix_ms.saturating_add(RECEIVE_ONLY_PERIODIC_BUSY_RETRY_MS);
    }

    fn schedule_failure(&mut self, now_unix_ms: i64) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.next_due_unix_ms = now_unix_ms.saturating_add(self.error_backoff_ms());
    }

    fn pause_for_manual_intervention(&mut self) {
        self.consecutive_failures = 0;
        self.paused_for_manual_intervention = true;
    }

    fn error_backoff_ms(self) -> i64 {
        let mut delay = RECEIVE_ONLY_PERIODIC_ERROR_BACKOFF_INITIAL_MS;
        let doublings = self.consecutive_failures.saturating_sub(1).min(16);

        for _ in 0..doublings {
            delay = delay
                .saturating_mul(2)
                .min(RECEIVE_ONLY_PERIODIC_ERROR_BACKOFF_MAX_MS);
        }

        delay.min(RECEIVE_ONLY_PERIODIC_ERROR_BACKOFF_MAX_MS)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedRootLocalReceiptVerification {
    pub receipts_total: usize,
    pub files_matching_receipt: usize,
    pub files_modified_since_receipt: usize,
    pub files_missing: usize,
    pub type_conflicts: usize,
    pub bytes_hashed: u64,
}

impl SelectedRootLocalReceiptVerification {
    pub fn all_receipts_match(self) -> bool {
        self.receipts_total > 0
            && self.files_matching_receipt == self.receipts_total
            && self.files_modified_since_receipt == 0
            && self.files_missing == 0
            && self.type_conflicts == 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedRootStaleFileBatchPlan {
    pub max_actions: usize,
    pub current_receipts: usize,
    pub stale_receipts_total: usize,
    pub replacement_candidates: usize,
    pub deletion_candidates: usize,
    pub safe_to_replace: usize,
    pub safe_to_delete: usize,
    pub local_conflicts: usize,
    pub files_missing: usize,
    pub type_conflicts: usize,
    pub bytes_hashed: u64,
    pub convergence_replacement_actions: usize,
    pub convergence_deletion_actions: usize,
}

impl SelectedRootStaleFileBatchPlan {
    pub fn all_stale_files_safe(self) -> bool {
        self.stale_receipts_total > 0
            && self.replacement_candidates + self.deletion_candidates == self.stale_receipts_total
            && self.safe_to_replace == self.replacement_candidates
            && self.safe_to_delete == self.deletion_candidates
            && self.local_conflicts == 0
            && self.files_missing == 0
            && self.type_conflicts == 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedRootRemoteReplacementPlan {
    pub stale_receipts_total: usize,
    pub replacement_candidates: usize,
    pub safe_to_replace: usize,
    pub local_conflicts: usize,
    pub files_missing: usize,
    pub type_conflicts: usize,
    pub bytes_hashed: u64,
}

impl SelectedRootRemoteReplacementPlan {
    pub fn ready(self) -> bool {
        self.stale_receipts_total == 1
            && self.replacement_candidates == 1
            && self.safe_to_replace == 1
            && self.local_conflicts == 0
            && self.files_missing == 0
            && self.type_conflicts == 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedRootRemoteDirectoryDeletionPlan {
    pub stale_receipts_total: usize,
    pub deletion_candidates: usize,
    pub safe_to_delete: usize,
    pub directories_already_missing: usize,
    pub non_empty_directories: usize,
    pub type_conflicts: usize,
    pub remote_id_absent: bool,
}

impl SelectedRootRemoteDirectoryDeletionPlan {
    pub fn ready(self) -> bool {
        self.stale_receipts_total == 1
            && self.deletion_candidates == 1
            && self.safe_to_delete == 1
            && self.directories_already_missing == 0
            && self.non_empty_directories == 0
            && self.type_conflicts == 0
            && self.remote_id_absent
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedRootDirectoryDeletion {
    pub directories_deleted: usize,
    pub empty_directory_verified: bool,
    pub receipt_deleted: bool,
    pub quarantine_rename: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedRootStaleDirectoryBatchDeletion {
    pub planned_deletion_actions: usize,
    pub batch_action_limit: usize,
    pub directories_deleted: usize,
    pub empty_directories_verified: usize,
    pub receipts_deleted: usize,
    pub quarantine_renames: usize,
    pub quarantine_directories_removed: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedRootFileDeletion {
    pub files_deleted: usize,
    pub bytes_verified: u64,
    pub stale_baseline_match: bool,
    pub receipt_deleted: bool,
    pub quarantine_rename: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedRootStaleFileBatchDeletion {
    pub planned_deletion_actions: usize,
    pub batch_action_limit: usize,
    pub files_deleted: usize,
    pub bytes_verified: u64,
    pub stale_baselines_verified: usize,
    pub receipts_deleted: usize,
    pub quarantine_renames: usize,
    pub quarantine_files_removed: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedRootRemoteDeletionPlan {
    pub stale_receipts_total: usize,
    pub deletion_candidates: usize,
    pub safe_to_delete: usize,
    pub local_conflicts: usize,
    pub files_already_missing: usize,
    pub type_conflicts: usize,
    pub bytes_hashed: u64,
    pub remote_id_absent: bool,
}

impl SelectedRootRemoteDeletionPlan {
    pub fn ready(self) -> bool {
        self.stale_receipts_total == 1
            && self.deletion_candidates == 1
            && self.safe_to_delete == 1
            && self.local_conflicts == 0
            && self.files_already_missing == 0
            && self.type_conflicts == 0
            && self.remote_id_absent
    }
}

pub trait SelectedRootContentProvider {
    fn content_fingerprint(
        &self,
        remote_id: &str,
    ) -> Result<SelectedRootContentFingerprint, SelectedRootExecutorError>;

    fn download_file_content(
        &self,
        remote_id: &str,
        max_bytes: u64,
        writer: &mut dyn Write,
    ) -> Result<u64, SelectedRootExecutorError>;
}

impl SelectedRootContentProvider for GoogleDriveApi {
    fn content_fingerprint(
        &self,
        remote_id: &str,
    ) -> Result<SelectedRootContentFingerprint, SelectedRootExecutorError> {
        let fingerprint = self.fetch_blob_fingerprint(remote_id)?;
        Ok(SelectedRootContentFingerprint::from_drive(fingerprint))
    }

    fn download_file_content(
        &self,
        remote_id: &str,
        max_bytes: u64,
        writer: &mut dyn Write,
    ) -> Result<u64, SelectedRootExecutorError> {
        GoogleDriveApi::download_blob_to_writer(self, remote_id, max_bytes, writer)
            .map_err(SelectedRootExecutorError::from)
    }
}

pub fn plan_selected_root_local_materialization(
    storage: &Storage,
    sync_root: &SyncRoot,
) -> Result<ReceiveOnlyMaterializationPlan, SelectedRootExecutorError> {
    let (remote_items, local_entries) = selected_root_materialization_inputs(storage, sync_root)?;

    plan_receive_only_materialization(&remote_items, &local_entries)
        .map_err(SelectedRootExecutorError::from)
}

pub fn plan_selected_root_receive_only_convergence(
    storage: &Storage,
    sync_root: &SyncRoot,
) -> Result<ReceiveOnlyConvergencePlan, SelectedRootExecutorError> {
    let (remote_items, local_entries) = selected_root_materialization_inputs(storage, sync_root)?;
    let mut receipts = Vec::new();

    for receipt in storage.list_sync_root_file_materialization_receipts(&sync_root.id)? {
        receipts.push(ReceiveOnlyOwnershipReceipt::new(
            receipt.remote_id,
            receipt.relative_path,
            LocalTreeEntryKind::File,
            ReceiveOnlyReceiptState::Current,
        )?);
    }

    for receipt in storage.list_sync_root_stale_file_materialization_receipts(&sync_root.id)? {
        receipts.push(ReceiveOnlyOwnershipReceipt::new(
            receipt.remote_id,
            receipt.relative_path,
            LocalTreeEntryKind::File,
            ReceiveOnlyReceiptState::Stale,
        )?);
    }

    for receipt in storage.list_sync_root_directory_materialization_receipts(&sync_root.id)? {
        receipts.push(ReceiveOnlyOwnershipReceipt::new(
            receipt.remote_id,
            receipt.relative_path,
            LocalTreeEntryKind::Directory,
            ReceiveOnlyReceiptState::Current,
        )?);
    }

    for receipt in storage.list_sync_root_stale_directory_materialization_receipts(&sync_root.id)? {
        receipts.push(ReceiveOnlyOwnershipReceipt::new(
            receipt.remote_id,
            receipt.relative_path,
            LocalTreeEntryKind::Directory,
            ReceiveOnlyReceiptState::Stale,
        )?);
    }

    nubisync_sync::plan_receive_only_convergence(
        &remote_items,
        &local_entries,
        &receipts,
        SELECTED_ROOT_CONVERGENCE_MAX_ACTIONS,
    )
    .map_err(SelectedRootExecutorError::from)
}

fn classify_selected_root_receive_only_cycle_metadata_phase(
    snapshot_complete: bool,
    window_complete: Option<bool>,
) -> SelectedRootReceiveOnlyCycleMetadataPhase {
    if !snapshot_complete {
        return SelectedRootReceiveOnlyCycleMetadataPhase::Bootstrap;
    }

    match window_complete {
        Some(true) => SelectedRootReceiveOnlyCycleMetadataPhase::ExecuteWindow,
        Some(false) | None => SelectedRootReceiveOnlyCycleMetadataPhase::CollectChangePage,
    }
}

pub fn plan_selected_root_receive_only_cycle_metadata_phase(
    storage: &Storage,
    sync_root: &SyncRoot,
) -> Result<SelectedRootReceiveOnlyCycleMetadataPhase, SelectedRootExecutorError> {
    if sync_root.mode != SyncMode::ReceiveOnly {
        return Err(SelectedRootExecutorError::ReceiveOnlyCycleModeUnsupported);
    }

    let inventory = storage.sync_root_remote_inventory_state(&sync_root.id)?;
    let window = storage.sync_root_change_window_state(&sync_root.id)?;

    Ok(classify_selected_root_receive_only_cycle_metadata_phase(
        inventory.snapshot_complete,
        window.as_ref().map(|state| state.is_complete()),
    ))
}

pub fn execute_selected_root_receive_only_cycle<P>(
    provider: &P,
    storage: &mut Storage,
    sync_root: &SyncRoot,
    observed_at_unix_ms: i64,
) -> Result<SelectedRootReceiveOnlyCycleExecution, SelectedRootExecutorError>
where
    P: SelectedRootBootstrapProvider
        + SelectedRootChangeProvider
        + SelectedRootProvider
        + SelectedRootContentProvider,
{
    let metadata_phase = plan_selected_root_receive_only_cycle_metadata_phase(storage, sync_root)?;

    let mut execution = SelectedRootReceiveOnlyCycleExecution {
        metadata_phase,
        metadata_authoritative_items: 0,
        metadata_page_count: 0,
        metadata_change_count: 0,
        metadata_catalog_mutations: 0,
        metadata_window_complete: false,
        initial_catchup_complete: false,
        convergence_executed: false,
        convergence_phase: None,
        convergence_actions_executed: 0,
        directories_created: 0,
        files_materialized: 0,
        files_verified: 0,
        files_replaced: 0,
        files_deleted: 0,
        directories_deleted: 0,
        bytes_downloaded: 0,
        bytes_verified: 0,
        receipts_recorded: 0,
        receipts_deleted: 0,
        final_actions: 0,
        final_blocked_actions: 0,
        next_metadata_phase: metadata_phase,
        next_convergence_phase: None,
        converged: false,
        requires_another_invocation: true,
        manual_intervention_required: false,
        stop_reason: SelectedRootReceiveOnlyCycleStopReason::MetadataStepCompleted,
    };

    match metadata_phase {
        SelectedRootReceiveOnlyCycleMetadataPhase::Bootstrap => {
            let result = bootstrap_selected_root_snapshot(
                provider,
                storage,
                sync_root,
                observed_at_unix_ms,
            )?;
            execution.metadata_authoritative_items = result.authoritative_items;
        }
        SelectedRootReceiveOnlyCycleMetadataPhase::CollectChangePage => {
            let result = collect_selected_root_change_window_page(provider, storage, sync_root)?;
            execution.metadata_page_count = result.page_count;
            execution.metadata_change_count = result.change_count;
            execution.metadata_window_complete = result.complete;
        }
        SelectedRootReceiveOnlyCycleMetadataPhase::ExecuteWindow => {
            let result = execute_completed_selected_root_change_window(
                provider,
                storage,
                sync_root,
                observed_at_unix_ms,
            )?;
            execution.metadata_authoritative_items = result.authoritative_items;
            execution.metadata_change_count = u64::try_from(result.provider_changes)
                .map_err(|_| SelectedRootExecutorError::CountOverflow)?;
            execution.metadata_catalog_mutations = result.storage_mutations;
        }
    }

    let inventory = storage.sync_root_remote_inventory_state(&sync_root.id)?;
    let window = storage.sync_root_change_window_state(&sync_root.id)?;
    execution.initial_catchup_complete = inventory.catchup_complete;
    execution.metadata_window_complete = window
        .as_ref()
        .map(|state| state.is_complete())
        .unwrap_or(false);
    execution.next_metadata_phase = classify_selected_root_receive_only_cycle_metadata_phase(
        inventory.snapshot_complete,
        window.as_ref().map(|state| state.is_complete()),
    );

    let metadata_stable =
        inventory.snapshot_complete && inventory.catchup_complete && window.is_none();

    if metadata_phase != SelectedRootReceiveOnlyCycleMetadataPhase::ExecuteWindow
        || !metadata_stable
    {
        return Ok(execution);
    }

    let convergence =
        execute_selected_root_unified_convergence_step(Some(provider), storage, sync_root)?;

    execution.convergence_executed = true;
    execution.convergence_phase = convergence.phase_executed;
    execution.convergence_actions_executed = convergence.phase_actions_executed;
    execution.directories_created = convergence.directories_created;
    execution.files_materialized = convergence.files_materialized;
    execution.files_verified = convergence.files_verified;
    execution.files_replaced = convergence.files_replaced;
    execution.files_deleted = convergence.files_deleted;
    execution.directories_deleted = convergence.directories_deleted;
    execution.bytes_downloaded = convergence.bytes_downloaded;
    execution.bytes_verified = convergence.bytes_verified;
    execution.receipts_recorded = convergence.receipts_recorded;
    execution.receipts_deleted = convergence.receipts_deleted;
    execution.final_actions = convergence.final_actions;
    execution.final_blocked_actions = convergence.final_blocked_actions;
    execution.next_convergence_phase = convergence.next_phase;
    execution.converged = convergence.converged;
    execution.requires_another_invocation = convergence.requires_another_invocation;
    execution.manual_intervention_required = convergence.manual_intervention_required;
    execution.stop_reason = if convergence.manual_intervention_required {
        SelectedRootReceiveOnlyCycleStopReason::ManualInterventionRequired
    } else if convergence.converged {
        SelectedRootReceiveOnlyCycleStopReason::Converged
    } else {
        SelectedRootReceiveOnlyCycleStopReason::ConvergencePhaseCompleted
    };

    Ok(execution)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SelectedRootReceiveOnlyRunMode {
    Metadata,
    Convergence,
}

fn classify_selected_root_receive_only_run_start(
    snapshot_complete: bool,
    catchup_complete: bool,
    change_window_present: bool,
    convergence_actions: usize,
) -> SelectedRootReceiveOnlyRunMode {
    if snapshot_complete && catchup_complete && !change_window_present && convergence_actions != 0 {
        SelectedRootReceiveOnlyRunMode::Convergence
    } else {
        SelectedRootReceiveOnlyRunMode::Metadata
    }
}

fn receive_only_run_add_usize(
    total: &mut usize,
    value: usize,
) -> Result<(), SelectedRootExecutorError> {
    *total = total
        .checked_add(value)
        .ok_or(SelectedRootExecutorError::CountOverflow)?;
    Ok(())
}

fn receive_only_run_add_u64(total: &mut u64, value: u64) -> Result<(), SelectedRootExecutorError> {
    *total = total
        .checked_add(value)
        .ok_or(SelectedRootExecutorError::CountOverflow)?;
    Ok(())
}

fn accumulate_receive_only_cycle(
    run: &mut SelectedRootReceiveOnlyRunExecution,
    cycle: &SelectedRootReceiveOnlyCycleExecution,
) -> Result<(), SelectedRootExecutorError> {
    receive_only_run_add_usize(&mut run.metadata_rounds, 1)?;

    match cycle.metadata_phase {
        SelectedRootReceiveOnlyCycleMetadataPhase::Bootstrap => {
            receive_only_run_add_usize(&mut run.bootstrap_rounds, 1)?;
        }
        SelectedRootReceiveOnlyCycleMetadataPhase::CollectChangePage => {
            receive_only_run_add_usize(&mut run.collect_change_page_rounds, 1)?;
        }
        SelectedRootReceiveOnlyCycleMetadataPhase::ExecuteWindow => {
            receive_only_run_add_usize(&mut run.execute_window_rounds, 1)?;
        }
    }

    if cycle.convergence_phase.is_some() {
        receive_only_run_add_usize(&mut run.convergence_phases_executed, 1)?;
    }

    receive_only_run_add_usize(&mut run.directories_created, cycle.directories_created)?;
    receive_only_run_add_usize(&mut run.files_materialized, cycle.files_materialized)?;
    receive_only_run_add_usize(&mut run.files_verified, cycle.files_verified)?;
    receive_only_run_add_usize(&mut run.files_replaced, cycle.files_replaced)?;
    receive_only_run_add_usize(&mut run.files_deleted, cycle.files_deleted)?;
    receive_only_run_add_usize(&mut run.directories_deleted, cycle.directories_deleted)?;
    receive_only_run_add_u64(&mut run.bytes_downloaded, cycle.bytes_downloaded)?;
    receive_only_run_add_u64(&mut run.bytes_verified, cycle.bytes_verified)?;
    receive_only_run_add_usize(&mut run.receipts_recorded, cycle.receipts_recorded)?;
    receive_only_run_add_usize(&mut run.receipts_deleted, cycle.receipts_deleted)?;

    run.next_metadata_phase = cycle.next_metadata_phase;

    if cycle.convergence_executed {
        run.final_convergence_known = true;
        run.final_actions = cycle.final_actions;
        run.final_blocked_actions = cycle.final_blocked_actions;
        run.next_convergence_phase = cycle.next_convergence_phase;
    }

    Ok(())
}

fn accumulate_receive_only_convergence(
    run: &mut SelectedRootReceiveOnlyRunExecution,
    convergence: &SelectedRootUnifiedConvergenceExecution,
) -> Result<(), SelectedRootExecutorError> {
    receive_only_run_add_usize(&mut run.convergence_only_rounds, 1)?;

    if convergence.phase_executed.is_some() {
        receive_only_run_add_usize(&mut run.convergence_phases_executed, 1)?;
    }

    receive_only_run_add_usize(
        &mut run.directories_created,
        convergence.directories_created,
    )?;
    receive_only_run_add_usize(&mut run.files_materialized, convergence.files_materialized)?;
    receive_only_run_add_usize(&mut run.files_verified, convergence.files_verified)?;
    receive_only_run_add_usize(&mut run.files_replaced, convergence.files_replaced)?;
    receive_only_run_add_usize(&mut run.files_deleted, convergence.files_deleted)?;
    receive_only_run_add_usize(
        &mut run.directories_deleted,
        convergence.directories_deleted,
    )?;
    receive_only_run_add_u64(&mut run.bytes_downloaded, convergence.bytes_downloaded)?;
    receive_only_run_add_u64(&mut run.bytes_verified, convergence.bytes_verified)?;
    receive_only_run_add_usize(&mut run.receipts_recorded, convergence.receipts_recorded)?;
    receive_only_run_add_usize(&mut run.receipts_deleted, convergence.receipts_deleted)?;

    run.final_convergence_known = true;
    run.final_actions = convergence.final_actions;
    run.final_blocked_actions = convergence.final_blocked_actions;
    run.next_convergence_phase = convergence.next_phase;

    Ok(())
}

pub fn execute_selected_root_receive_only_run_to_idle<P>(
    provider: &P,
    storage: &mut Storage,
    sync_root: &SyncRoot,
    observed_at_unix_ms: i64,
) -> Result<SelectedRootReceiveOnlyRunExecution, SelectedRootExecutorError>
where
    P: SelectedRootBootstrapProvider
        + SelectedRootChangeProvider
        + SelectedRootProvider
        + SelectedRootContentProvider,
{
    if sync_root.mode != SyncMode::ReceiveOnly {
        return Err(SelectedRootExecutorError::ReceiveOnlyCycleModeUnsupported);
    }

    let inventory = storage.sync_root_remote_inventory_state(&sync_root.id)?;
    let window = storage.sync_root_change_window_state(&sync_root.id)?;

    let convergence_actions =
        if inventory.snapshot_complete && inventory.catchup_complete && window.is_none() {
            plan_selected_root_receive_only_convergence(storage, sync_root)?.action_count()
        } else {
            0
        };

    let mut mode = classify_selected_root_receive_only_run_start(
        inventory.snapshot_complete,
        inventory.catchup_complete,
        window.is_some(),
        convergence_actions,
    );
    let mut metadata_observation_started = mode == SelectedRootReceiveOnlyRunMode::Metadata;

    let mut run = SelectedRootReceiveOnlyRunExecution {
        max_rounds: SUPERVISED_RECEIVE_ONLY_RUN_MAX_ROUNDS,
        rounds_executed: 0,
        metadata_rounds: 0,
        convergence_only_rounds: 0,
        bootstrap_rounds: 0,
        collect_change_page_rounds: 0,
        execute_window_rounds: 0,
        convergence_phases_executed: 0,
        directories_created: 0,
        files_materialized: 0,
        files_verified: 0,
        files_replaced: 0,
        files_deleted: 0,
        directories_deleted: 0,
        bytes_downloaded: 0,
        bytes_verified: 0,
        receipts_recorded: 0,
        receipts_deleted: 0,
        final_convergence_known: false,
        final_actions: 0,
        final_blocked_actions: 0,
        next_metadata_phase: plan_selected_root_receive_only_cycle_metadata_phase(
            storage, sync_root,
        )?,
        next_convergence_phase: None,
        converged: false,
        requires_another_invocation: true,
        manual_intervention_required: false,
        stop_reason: SelectedRootReceiveOnlyRunStopReason::RoundBudgetExhausted,
    };

    while run.rounds_executed < run.max_rounds {
        receive_only_run_add_usize(&mut run.rounds_executed, 1)?;

        match mode {
            SelectedRootReceiveOnlyRunMode::Metadata => {
                let cycle = execute_selected_root_receive_only_cycle(
                    provider,
                    storage,
                    sync_root,
                    observed_at_unix_ms,
                )?;

                accumulate_receive_only_cycle(&mut run, &cycle)?;

                if cycle.manual_intervention_required {
                    run.manual_intervention_required = true;
                    run.requires_another_invocation = false;
                    run.stop_reason =
                        SelectedRootReceiveOnlyRunStopReason::ManualInterventionRequired;
                    return Ok(run);
                }

                if cycle.converged {
                    run.converged = true;
                    run.requires_another_invocation = false;
                    run.stop_reason = SelectedRootReceiveOnlyRunStopReason::Converged;
                    return Ok(run);
                }

                if cycle.convergence_executed {
                    mode = SelectedRootReceiveOnlyRunMode::Convergence;
                }
            }
            SelectedRootReceiveOnlyRunMode::Convergence => {
                let convergence = execute_selected_root_unified_convergence_step(
                    Some(provider),
                    storage,
                    sync_root,
                )?;

                accumulate_receive_only_convergence(&mut run, &convergence)?;

                if convergence.manual_intervention_required {
                    run.manual_intervention_required = true;
                    run.requires_another_invocation = false;
                    run.stop_reason =
                        SelectedRootReceiveOnlyRunStopReason::ManualInterventionRequired;
                    return Ok(run);
                }

                if convergence.converged {
                    if metadata_observation_started {
                        run.converged = true;
                        run.requires_another_invocation = false;
                        run.stop_reason = SelectedRootReceiveOnlyRunStopReason::Converged;
                        return Ok(run);
                    }

                    mode = SelectedRootReceiveOnlyRunMode::Metadata;
                    metadata_observation_started = true;
                    run.next_metadata_phase =
                        plan_selected_root_receive_only_cycle_metadata_phase(storage, sync_root)?;
                }
            }
        }
    }

    run.requires_another_invocation = true;
    run.stop_reason = SelectedRootReceiveOnlyRunStopReason::RoundBudgetExhausted;
    Ok(run)
}

pub fn execute_selected_root_receive_only_single_flight<P>(
    provider: &P,
    storage: &mut Storage,
    sync_root: &SyncRoot,
    observed_at_unix_ms: i64,
) -> Result<SelectedRootReceiveOnlySingleFlightResult, SelectedRootExecutorError>
where
    P: SelectedRootBootstrapProvider
        + SelectedRootChangeProvider
        + SelectedRootProvider
        + SelectedRootContentProvider,
{
    let Some(_slot) = try_acquire_selected_root_receive_only_run_slot(&sync_root.id)? else {
        return Ok(SelectedRootReceiveOnlySingleFlightResult::Busy);
    };

    let execution = execute_selected_root_receive_only_run_to_idle(
        provider,
        storage,
        sync_root,
        observed_at_unix_ms,
    )?;

    Ok(SelectedRootReceiveOnlySingleFlightResult::Executed(
        execution,
    ))
}

enum SelectedRootReceiveOnlyPeriodicSingleFlightResult {
    Busy,
    Executed {
        execution: SelectedRootReceiveOnlyRunExecution,
        local_observation: SelectedRootPeriodicLocalObservation,
    },
}

fn receive_only_execution_ready_for_local_observation(
    converged: bool,
    requires_another_invocation: bool,
    manual_intervention_required: bool,
) -> bool {
    converged && !requires_another_invocation && !manual_intervention_required
}

fn execute_selected_root_receive_only_periodic_single_flight<P>(
    provider: &P,
    storage: &mut Storage,
    sync_root: &SyncRoot,
    observed_at_unix_ms: i64,
) -> Result<SelectedRootReceiveOnlyPeriodicSingleFlightResult, SelectedRootExecutorError>
where
    P: SelectedRootBootstrapProvider
        + SelectedRootChangeProvider
        + SelectedRootProvider
        + SelectedRootContentProvider,
{
    let Some(_slot) = try_acquire_selected_root_receive_only_run_slot(&sync_root.id)? else {
        return Ok(SelectedRootReceiveOnlyPeriodicSingleFlightResult::Busy);
    };

    let execution = execute_selected_root_receive_only_run_to_idle(
        provider,
        storage,
        sync_root,
        observed_at_unix_ms,
    )?;

    let local_observation = if receive_only_execution_ready_for_local_observation(
        execution.converged,
        execution.requires_another_invocation,
        execution.manual_intervention_required,
    ) {
        let baseline = storage.sync_root_local_inventory_state(&sync_root.id)?;
        if !baseline.snapshot_complete {
            SelectedRootPeriodicLocalObservation::BaselineMissing
        } else if !baseline.observation_valid {
            SelectedRootPeriodicLocalObservation::BaselineInvalidated
        } else {
            SelectedRootPeriodicLocalObservation::Journaled(
                journal_selected_root_local_inventory_diff(
                    storage,
                    sync_root,
                    observed_at_unix_ms,
                )?,
            )
        }
    } else {
        SelectedRootPeriodicLocalObservation::DeferredUntilReceiveOnlyConverged
    };

    Ok(
        SelectedRootReceiveOnlyPeriodicSingleFlightResult::Executed {
            execution,
            local_observation,
        },
    )
}

pub fn execute_selected_root_receive_only_periodic_tick<P>(
    state: &mut SelectedRootReceiveOnlyPeriodicState,
    provider: &P,
    storage: &mut Storage,
    sync_root: &SyncRoot,
    execution_lock_path: &Path,
    now_unix_ms: i64,
) -> Result<SelectedRootReceiveOnlyPeriodicTick, SelectedRootExecutorError>
where
    P: SelectedRootBootstrapProvider
        + SelectedRootChangeProvider
        + SelectedRootProvider
        + SelectedRootContentProvider,
{
    match state.decision(now_unix_ms) {
        SelectedRootReceiveOnlyPeriodicDecision::Waiting { delay_ms } => {
            return Ok(SelectedRootReceiveOnlyPeriodicTick::Waiting { delay_ms });
        }
        SelectedRootReceiveOnlyPeriodicDecision::PausedForManualIntervention => {
            return Ok(SelectedRootReceiveOnlyPeriodicTick::PausedForManualIntervention);
        }
        SelectedRootReceiveOnlyPeriodicDecision::Shutdown => {
            return Ok(SelectedRootReceiveOnlyPeriodicTick::Shutdown);
        }
        SelectedRootReceiveOnlyPeriodicDecision::Due => {}
    }

    let _cross_process_guard =
        match try_acquire_selected_root_cross_process_execution_lock(execution_lock_path) {
            Ok(SelectedRootCrossProcessExecutionLock::Acquired(guard)) => guard,
            Ok(SelectedRootCrossProcessExecutionLock::Busy) => {
                state.schedule_busy_retry(now_unix_ms);
                return Ok(SelectedRootReceiveOnlyPeriodicTick::Busy {
                    retry_after_ms: RECEIVE_ONLY_PERIODIC_BUSY_RETRY_MS,
                    scope: SelectedRootExecutionBusyScope::CrossProcess,
                });
            }
            Err(error) => {
                state.schedule_failure(now_unix_ms);
                return Err(error);
            }
        };

    let execution = match execute_selected_root_receive_only_periodic_single_flight(
        provider,
        storage,
        sync_root,
        now_unix_ms,
    ) {
        Ok(result) => result,
        Err(error) => {
            state.schedule_failure(now_unix_ms);
            return Err(error);
        }
    };

    match execution {
        SelectedRootReceiveOnlyPeriodicSingleFlightResult::Busy => {
            state.schedule_busy_retry(now_unix_ms);
            Ok(SelectedRootReceiveOnlyPeriodicTick::Busy {
                retry_after_ms: RECEIVE_ONLY_PERIODIC_BUSY_RETRY_MS,
                scope: SelectedRootExecutionBusyScope::InProcess,
            })
        }
        SelectedRootReceiveOnlyPeriodicSingleFlightResult::Executed {
            execution,
            local_observation,
        } => {
            if execution.manual_intervention_required {
                state.pause_for_manual_intervention();
                Ok(SelectedRootReceiveOnlyPeriodicTick::Executed {
                    execution,
                    local_observation,
                    next_delay_ms: None,
                })
            } else {
                state.schedule_success(now_unix_ms);
                Ok(SelectedRootReceiveOnlyPeriodicTick::Executed {
                    execution,
                    local_observation,
                    next_delay_ms: Some(RECEIVE_ONLY_PERIODIC_POLL_INTERVAL_MS),
                })
            }
        }
    }
}

pub fn plan_selected_root_unified_convergence_step(
    storage: &Storage,
    sync_root: &SyncRoot,
) -> Result<SelectedRootUnifiedConvergenceDecision, SelectedRootExecutorError> {
    let plan = plan_selected_root_receive_only_convergence(storage, sync_root)?;
    decide_selected_root_unified_convergence_step(&plan)
}

fn decide_selected_root_unified_convergence_step(
    plan: &ReceiveOnlyConvergencePlan,
) -> Result<SelectedRootUnifiedConvergenceDecision, SelectedRootExecutorError> {
    if plan.blocked() {
        return Err(SelectedRootExecutorError::UnifiedConvergenceBlocked);
    }

    if plan.action_count() == 0 {
        return Ok(SelectedRootUnifiedConvergenceDecision::Converged);
    }

    if plan.create_directories != 0 {
        return Ok(SelectedRootUnifiedConvergenceDecision::Dispatch {
            phase: SelectedRootUnifiedConvergencePhase::CreateDirectories,
            planned_actions: plan.create_directories,
        });
    }

    let active_classes = [
        plan.materialize_missing_files != 0,
        plan.verify_existing_files != 0,
        plan.revalidate_stale_file_replacements != 0,
        plan.revalidate_stale_file_deletions != 0,
        plan.delete_owned_empty_directories != 0,
    ]
    .into_iter()
    .filter(|active| *active)
    .count();

    if active_classes != 1 {
        return Ok(SelectedRootUnifiedConvergenceDecision::SafeStop {
            reason: SelectedRootUnifiedConvergenceStopReason::MixedActionClasses,
        });
    }

    let (phase, planned_actions, max_actions) = if plan.materialize_missing_files != 0 {
        (
            SelectedRootUnifiedConvergencePhase::MaterializeMissingFiles,
            plan.materialize_missing_files,
            SUPERVISED_FILE_BATCH_MAX_ACTIONS,
        )
    } else if plan.verify_existing_files != 0 {
        (
            SelectedRootUnifiedConvergencePhase::VerifyExistingFiles,
            plan.verify_existing_files,
            SUPERVISED_FILE_BATCH_MAX_ACTIONS,
        )
    } else if plan.revalidate_stale_file_replacements != 0 {
        (
            SelectedRootUnifiedConvergencePhase::ReplaceStaleFiles,
            plan.revalidate_stale_file_replacements,
            SUPERVISED_STALE_FILE_PLAN_MAX_ACTIONS,
        )
    } else if plan.revalidate_stale_file_deletions != 0 {
        (
            SelectedRootUnifiedConvergencePhase::DeleteStaleFiles,
            plan.revalidate_stale_file_deletions,
            SUPERVISED_STALE_FILE_PLAN_MAX_ACTIONS,
        )
    } else {
        (
            SelectedRootUnifiedConvergencePhase::DeleteStaleDirectories,
            plan.delete_owned_empty_directories,
            SUPERVISED_STALE_DIRECTORY_DELETION_MAX_ACTIONS,
        )
    };

    if planned_actions > max_actions {
        return Ok(SelectedRootUnifiedConvergenceDecision::SafeStop {
            reason: SelectedRootUnifiedConvergenceStopReason::BatchActionLimitExceeded,
        });
    }

    Ok(SelectedRootUnifiedConvergenceDecision::Dispatch {
        phase,
        planned_actions,
    })
}

pub fn execute_selected_root_unified_convergence_step<P: SelectedRootContentProvider>(
    provider: Option<&P>,
    storage: &mut Storage,
    sync_root: &SyncRoot,
) -> Result<SelectedRootUnifiedConvergenceExecution, SelectedRootExecutorError> {
    let initial_plan = plan_selected_root_receive_only_convergence(storage, sync_root)?;
    let initial_actions = initial_plan.action_count();
    let decision = decide_selected_root_unified_convergence_step(&initial_plan)?;

    match decision {
        SelectedRootUnifiedConvergenceDecision::Converged => {
            return Ok(SelectedRootUnifiedConvergenceExecution {
                initial_actions,
                phase_executed: None,
                phase_actions_planned: 0,
                phase_actions_executed: 0,
                directories_created: 0,
                files_materialized: 0,
                files_verified: 0,
                files_replaced: 0,
                files_deleted: 0,
                directories_deleted: 0,
                bytes_downloaded: 0,
                bytes_verified: 0,
                receipts_recorded: 0,
                receipts_deleted: 0,
                final_actions: 0,
                final_blocked_actions: 0,
                next_phase: None,
                converged: true,
                requires_another_invocation: false,
                manual_intervention_required: false,
                stop_reason: SelectedRootUnifiedConvergenceStopReason::Converged,
            });
        }
        SelectedRootUnifiedConvergenceDecision::SafeStop { reason } => {
            return Ok(SelectedRootUnifiedConvergenceExecution {
                initial_actions,
                phase_executed: None,
                phase_actions_planned: 0,
                phase_actions_executed: 0,
                directories_created: 0,
                files_materialized: 0,
                files_verified: 0,
                files_replaced: 0,
                files_deleted: 0,
                directories_deleted: 0,
                bytes_downloaded: 0,
                bytes_verified: 0,
                receipts_recorded: 0,
                receipts_deleted: 0,
                final_actions: initial_actions,
                final_blocked_actions: initial_plan.blocked_actions,
                next_phase: None,
                converged: false,
                requires_another_invocation: false,
                manual_intervention_required: true,
                stop_reason: reason,
            });
        }
        SelectedRootUnifiedConvergenceDecision::Dispatch {
            phase,
            planned_actions,
        } => {
            if phase.requires_content_provider() && provider.is_none() {
                return Err(SelectedRootExecutorError::UnifiedConvergenceProviderRequired);
            }

            let mut execution = SelectedRootUnifiedConvergenceExecution {
                initial_actions,
                phase_executed: Some(phase),
                phase_actions_planned: planned_actions,
                phase_actions_executed: 0,
                directories_created: 0,
                files_materialized: 0,
                files_verified: 0,
                files_replaced: 0,
                files_deleted: 0,
                directories_deleted: 0,
                bytes_downloaded: 0,
                bytes_verified: 0,
                receipts_recorded: 0,
                receipts_deleted: 0,
                final_actions: initial_actions,
                final_blocked_actions: initial_plan.blocked_actions,
                next_phase: None,
                converged: false,
                requires_another_invocation: false,
                manual_intervention_required: false,
                stop_reason: SelectedRootUnifiedConvergenceStopReason::PhaseCompleted,
            };

            match phase {
                SelectedRootUnifiedConvergencePhase::CreateDirectories => {
                    let result = materialize_selected_root_directories(storage, sync_root)?;
                    execution.phase_actions_executed = result.created_directories;
                    execution.directories_created = result.created_directories;
                    execution.receipts_recorded = result.created_directories;
                }
                SelectedRootUnifiedConvergencePhase::MaterializeMissingFiles => {
                    let provider = provider
                        .ok_or(SelectedRootExecutorError::UnifiedConvergenceProviderRequired)?;
                    let result =
                        materialize_selected_root_missing_files(provider, storage, sync_root)?;
                    execution.phase_actions_executed = result.files_downloaded;
                    execution.files_materialized = result.files_downloaded;
                    execution.bytes_downloaded = result.bytes_downloaded;
                    execution.receipts_recorded = result.receipts_recorded;
                }
                SelectedRootUnifiedConvergencePhase::VerifyExistingFiles => {
                    let provider = provider
                        .ok_or(SelectedRootExecutorError::UnifiedConvergenceProviderRequired)?;
                    let result = verify_selected_root_existing_files(provider, storage, sync_root)?;
                    execution.phase_actions_executed = result.files_verified;
                    execution.files_verified = result.files_verified;
                    execution.bytes_verified = result.bytes_verified;
                    execution.receipts_recorded = result.receipts_recorded;
                }
                SelectedRootUnifiedConvergencePhase::ReplaceStaleFiles => {
                    let provider = provider
                        .ok_or(SelectedRootExecutorError::UnifiedConvergenceProviderRequired)?;
                    let result = replace_selected_root_stale_files(provider, storage, sync_root)?;
                    execution.phase_actions_executed = result.files_replaced;
                    execution.files_replaced = result.files_replaced;
                    execution.bytes_downloaded = result.bytes_downloaded;
                    execution.receipts_recorded = result.receipts_recorded;
                }
                SelectedRootUnifiedConvergencePhase::DeleteStaleFiles => {
                    let result = delete_selected_root_stale_files(storage, sync_root)?;
                    execution.phase_actions_executed = result.files_deleted;
                    execution.files_deleted = result.files_deleted;
                    execution.bytes_verified = result.bytes_verified;
                    execution.receipts_deleted = result.receipts_deleted;
                }
                SelectedRootUnifiedConvergencePhase::DeleteStaleDirectories => {
                    let result = delete_selected_root_stale_directories(storage, sync_root)?;
                    execution.phase_actions_executed = result.directories_deleted;
                    execution.directories_deleted = result.directories_deleted;
                    execution.receipts_deleted = result.receipts_deleted;
                }
            }

            if execution.phase_actions_executed != planned_actions {
                return Err(SelectedRootExecutorError::UnifiedConvergencePhaseCountMismatch);
            }

            let final_plan = plan_selected_root_receive_only_convergence(storage, sync_root)?;
            execution.final_actions = final_plan.action_count();
            execution.final_blocked_actions = final_plan.blocked_actions;

            if final_plan.blocked() {
                execution.stop_reason = SelectedRootUnifiedConvergenceStopReason::BlockedAfterPhase;
                execution.manual_intervention_required = true;
                return Ok(execution);
            }

            match decide_selected_root_unified_convergence_step(&final_plan)? {
                SelectedRootUnifiedConvergenceDecision::Converged => {
                    execution.converged = true;
                    execution.stop_reason = SelectedRootUnifiedConvergenceStopReason::Converged;
                }
                SelectedRootUnifiedConvergenceDecision::Dispatch { phase, .. } => {
                    execution.next_phase = Some(phase);
                    execution.requires_another_invocation = true;
                    execution.stop_reason =
                        SelectedRootUnifiedConvergenceStopReason::PhaseCompleted;
                }
                SelectedRootUnifiedConvergenceDecision::SafeStop { reason } => {
                    execution.stop_reason = reason;
                    execution.manual_intervention_required = true;
                }
            }

            Ok(execution)
        }
    }
}

pub fn materialize_selected_root_directories(
    storage: &mut Storage,
    sync_root: &SyncRoot,
) -> Result<SelectedRootDirectoryMaterialization, SelectedRootExecutorError> {
    let convergence = plan_selected_root_receive_only_convergence(storage, sync_root)?;
    if convergence.blocked() {
        return Err(SelectedRootExecutorError::LocalDirectoryPhaseBlocked);
    }

    let (remote_items, local_entries) = selected_root_materialization_inputs(storage, sync_root)?;
    let preflight = plan_receive_only_materialization(&remote_items, &local_entries)?;

    if !preflight.ready_for_directory_phase()
        || convergence.remote_items != preflight.remote_items
        || convergence.local_entries != preflight.local_entries
        || convergence.create_directories != preflight.missing_directories
        || convergence.materialize_missing_files != preflight.missing_files
    {
        return Err(SelectedRootExecutorError::LocalDirectoryPhaseBlocked);
    }

    let targets = plan_receive_only_missing_directory_targets(&remote_items, &local_entries)?;
    if targets.len() != preflight.missing_directories
        || targets.len() != convergence.create_directories
        || targets.len() > SELECTED_ROOT_CONVERGENCE_MAX_ACTIONS
    {
        return Err(SelectedRootExecutorError::LocalDirectoryTargetCountMismatch);
    }

    let root_path = validated_selected_root_path(sync_root)?;
    storage.invalidate_sync_root_local_observation_baseline(&sync_root.id)?;
    let outcome = apply_selected_root_directory_targets(&root_path, &targets)?;

    let post_result = (|| {
        if outcome.existing_directories != 0
            || outcome.created_paths.len() != targets.len()
            || outcome.created_targets.len() != targets.len()
        {
            return Err(SelectedRootExecutorError::LocalDirectoryPostconditionFailed);
        }

        let post_entries = scan_selected_root_local_tree(sync_root)?;
        let post_plan = plan_receive_only_materialization(&remote_items, &post_entries)?;

        if !post_plan.ready_for_directory_phase()
            || post_plan.missing_directories != 0
            || post_plan.matching_directories != post_plan.remote_directories
            || preflight
                .matching_directories
                .checked_add(outcome.created_paths.len())
                .ok_or(SelectedRootExecutorError::CountOverflow)?
                != post_plan.remote_directories
        {
            return Err(SelectedRootExecutorError::LocalDirectoryPostconditionFailed);
        }

        let created_receipts = outcome
            .created_targets
            .iter()
            .map(|target| {
                (
                    target.remote_id().to_owned(),
                    target.relative_path().to_owned(),
                )
            })
            .collect::<Vec<_>>();

        let recorded = storage.record_sync_root_directory_materializations(
            &sync_root.id,
            &created_receipts,
            current_unix_time_ms()?,
        )?;
        if recorded != created_receipts.len() {
            return Err(SelectedRootExecutorError::DirectoryReceiptPostconditionFailed);
        }

        Ok(SelectedRootDirectoryMaterialization {
            remote_directories: post_plan.remote_directories,
            planned_directory_actions: targets.len(),
            batch_action_limit: SELECTED_ROOT_CONVERGENCE_MAX_ACTIONS,
            created_directories: outcome.created_paths.len(),
            existing_directories: preflight.matching_directories,
            pending_files: post_plan.remote_files,
        })
    })();

    match post_result {
        Ok(result) => Ok(result),
        Err(error) => {
            rollback_created_directories(&outcome.created_paths)?;
            Err(error)
        }
    }
}

pub fn adopt_selected_root_existing_directory(
    storage: &mut Storage,
    sync_root: &SyncRoot,
) -> Result<SelectedRootDirectoryAdoption, SelectedRootExecutorError> {
    let (remote_items, local_entries) = selected_root_materialization_inputs(storage, sync_root)?;
    let plan = plan_receive_only_materialization(&remote_items, &local_entries)?;

    if plan.remote_items != 1
        || plan.remote_directories != 1
        || plan.remote_files != 0
        || plan.local_entries != 1
        || plan.missing_directories != 0
        || plan.missing_files != 0
        || plan.matching_directories != 1
        || plan.existing_files_unverified != 0
        || plan.local_only_entries != 0
        || plan.type_conflicts != 0
    {
        return Err(SelectedRootExecutorError::DirectoryAdoptionPhaseBlocked);
    }

    if storage.sync_root_materialization_receipt_count(&sync_root.id)? != 0
        || storage.sync_root_stale_materialization_receipt_count(&sync_root.id)? != 0
        || storage.sync_root_directory_materialization_receipt_count(&sync_root.id)? != 0
        || storage.sync_root_stale_directory_materialization_receipt_count(&sync_root.id)? != 0
    {
        return Err(SelectedRootExecutorError::DirectoryAdoptionReceiptStateConflict);
    }

    let targets = plan_receive_only_directory_targets(&remote_items)?;
    if targets.len() != 1 {
        return Err(SelectedRootExecutorError::DirectoryAdoptionTargetMismatch);
    }
    let target = targets
        .first()
        .ok_or(SelectedRootExecutorError::DirectoryAdoptionTargetMismatch)?;

    let root_path = validated_selected_root_path(sync_root)?;
    let target_path = root_path.join(target.relative_path());
    if !target_path.starts_with(&root_path) {
        return Err(SelectedRootExecutorError::LocalDirectoryTargetEscapedRoot);
    }

    let metadata = fs::symlink_metadata(&target_path)
        .map_err(|_| SelectedRootExecutorError::DirectoryAdoptionTargetMismatch)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(SelectedRootExecutorError::DirectoryAdoptionTargetMismatch);
    }

    let canonical_target = fs::canonicalize(&target_path)
        .map_err(|_| SelectedRootExecutorError::LocalFilesystemInspectionFailed)?;
    if !canonical_target.starts_with(&root_path) {
        return Err(SelectedRootExecutorError::LocalDirectoryTargetEscapedRoot);
    }

    let mut entries = fs::read_dir(&target_path)
        .map_err(|_| SelectedRootExecutorError::LocalFilesystemInspectionFailed)?;
    match entries.next() {
        None => {}
        Some(Ok(_)) => return Err(SelectedRootExecutorError::DirectoryAdoptionTargetNotEmpty),
        Some(Err(_)) => return Err(SelectedRootExecutorError::LocalFilesystemInspectionFailed),
    }

    let recorded = storage.record_sync_root_directory_materializations(
        &sync_root.id,
        &[(
            target.remote_id().to_owned(),
            target.relative_path().to_owned(),
        )],
        current_unix_time_ms()?,
    )?;
    if recorded != 1 {
        return Err(SelectedRootExecutorError::DirectoryReceiptPostconditionFailed);
    }

    let current = storage.sync_root_directory_materialization_receipt_count(&sync_root.id)?;
    let stale = storage.sync_root_stale_directory_materialization_receipt_count(&sync_root.id)?;
    if current != 1 || stale != 0 {
        return Err(SelectedRootExecutorError::DirectoryReceiptPostconditionFailed);
    }

    Ok(SelectedRootDirectoryAdoption {
        directories_adopted: 1,
        current_directory_receipts: current,
        stale_directory_receipts: stale,
    })
}

pub fn materialize_selected_root_missing_files<P: SelectedRootContentProvider>(
    provider: &P,
    storage: &mut Storage,
    sync_root: &SyncRoot,
) -> Result<SelectedRootFileBatchMaterialization, SelectedRootExecutorError> {
    let convergence = plan_selected_root_receive_only_convergence(storage, sync_root)?;

    if convergence.blocked()
        || convergence.create_directories != 0
        || convergence.verify_existing_files != 0
        || convergence.revalidate_stale_file_replacements != 0
        || convergence.revalidate_stale_file_deletions != 0
        || convergence.delete_owned_empty_directories != 0
    {
        return Err(SelectedRootExecutorError::LocalFilePhaseBlocked);
    }

    let (remote_items, local_entries) = selected_root_materialization_inputs(storage, sync_root)?;
    let preflight = plan_receive_only_materialization(&remote_items, &local_entries)?;

    if !preflight.ready_for_directory_phase()
        || preflight.missing_directories != 0
        || preflight.matching_directories != preflight.remote_directories
        || convergence.remote_items != preflight.remote_items
        || convergence.local_entries != preflight.local_entries
        || convergence.materialize_missing_files != preflight.missing_files
        || preflight.existing_files_unverified
            != convergence
                .current_owned_files
                .checked_add(convergence.verify_existing_files)
                .ok_or(SelectedRootExecutorError::CountOverflow)?
    {
        return Err(SelectedRootExecutorError::LocalFilePhaseBlocked);
    }

    let targets = plan_receive_only_missing_file_targets(&remote_items, &local_entries)?;
    if targets.len() != preflight.missing_files
        || targets.len() != convergence.materialize_missing_files
        || targets.len() > SUPERVISED_FILE_BATCH_MAX_ACTIONS
    {
        return Err(SelectedRootExecutorError::LocalFileTargetCountMismatch);
    }

    if targets.is_empty() {
        return Ok(SelectedRootFileBatchMaterialization {
            planned_file_actions: 0,
            batch_action_limit: SUPERVISED_FILE_BATCH_MAX_ACTIONS,
            files_downloaded: 0,
            bytes_downloaded: 0,
            max_file_bytes: SUPERVISED_FILE_DOWNLOAD_MAX_BYTES,
            provider_fingerprints_verified: 0,
            receipts_recorded: 0,
        });
    }

    let root_path = validated_selected_root_path(sync_root)?;
    storage.invalidate_sync_root_local_observation_baseline(&sync_root.id)?;
    let mut completed = Vec::with_capacity(targets.len());
    let mut total_bytes = 0_u64;

    for target in &targets {
        let expected_size = target
            .size_bytes()
            .ok_or(SelectedRootExecutorError::LocalFileSizeUnknown)?;
        if expected_size > SUPERVISED_FILE_DOWNLOAD_MAX_BYTES {
            rollback_file_batch(&completed)?;
            return Err(SelectedRootExecutorError::LocalFileTooLarge);
        }

        let fingerprint_before = match provider.content_fingerprint(target.remote_id()) {
            Ok(value) => value,
            Err(error) => {
                rollback_file_batch(&completed)?;
                return Err(error);
            }
        };

        if fingerprint_before.size_bytes != expected_size {
            rollback_file_batch(&completed)?;
            return Err(SelectedRootExecutorError::RemoteReplacementProviderFingerprintMismatch);
        }

        let outcome = match apply_selected_root_file_target(
            provider,
            &root_path,
            target,
            SUPERVISED_FILE_DOWNLOAD_MAX_BYTES,
        ) {
            Ok(value) => value,
            Err(error) => {
                rollback_file_batch(&completed)?;
                return Err(error);
            }
        };

        let fingerprint_after = match provider.content_fingerprint(target.remote_id()) {
            Ok(value) => value,
            Err(error) => {
                rollback_created_file(&outcome.target_path)?;
                rollback_file_batch(&completed)?;
                return Err(error);
            }
        };

        if fingerprint_before != fingerprint_after
            || outcome.bytes_downloaded != fingerprint_after.size_bytes
            || outcome.sha256_hex != fingerprint_after.sha256_hex
        {
            rollback_created_file(&outcome.target_path)?;
            rollback_file_batch(&completed)?;
            return Err(SelectedRootExecutorError::RemoteReplacementProviderFingerprintMismatch);
        }

        let (promoted_bytes, promoted_sha256) =
            match hash_local_file(&outcome.target_path, SUPERVISED_FILE_DOWNLOAD_MAX_BYTES) {
                Ok(value) => value,
                Err(error) => {
                    rollback_created_file(&outcome.target_path)?;
                    rollback_file_batch(&completed)?;
                    return Err(error);
                }
            };

        if promoted_bytes != outcome.bytes_downloaded
            || promoted_sha256 != outcome.sha256_hex
            || promoted_sha256 != fingerprint_after.sha256_hex
        {
            rollback_created_file(&outcome.target_path)?;
            rollback_file_batch(&completed)?;
            return Err(SelectedRootExecutorError::LocalFilePostconditionFailed);
        }

        total_bytes = match total_bytes.checked_add(outcome.bytes_downloaded) {
            Some(value) => value,
            None => {
                rollback_created_file(&outcome.target_path)?;
                rollback_file_batch(&completed)?;
                return Err(SelectedRootExecutorError::CountOverflow);
            }
        };

        completed.push((
            target.clone(),
            outcome.target_path,
            outcome.bytes_downloaded,
            outcome.sha256_hex,
        ));
    }

    let post_entries = match scan_selected_root_local_tree(sync_root) {
        Ok(value) => value,
        Err(error) => {
            rollback_file_batch(&completed)?;
            return Err(error);
        }
    };
    let post_plan = match plan_receive_only_materialization(&remote_items, &post_entries) {
        Ok(value) => value,
        Err(error) => {
            rollback_file_batch(&completed)?;
            return Err(error.into());
        }
    };

    if !post_plan.ready_for_directory_phase()
        || post_plan.missing_directories != 0
        || post_plan.missing_files != 0
        || post_plan.matching_directories != post_plan.remote_directories
        || post_plan.existing_files_unverified != post_plan.remote_files
    {
        rollback_file_batch(&completed)?;
        return Err(SelectedRootExecutorError::LocalFilePostconditionFailed);
    }

    let receipt_rows = completed
        .iter()
        .map(|(target, _, bytes, sha256_hex)| {
            (
                target.remote_id().to_owned(),
                target.relative_path().to_owned(),
                *bytes,
                sha256_hex.clone(),
            )
        })
        .collect::<Vec<_>>();

    let recorded = match storage.record_sync_root_file_materializations(
        &sync_root.id,
        &receipt_rows,
        current_unix_time_ms()?,
    ) {
        Ok(value) => value,
        Err(error) => {
            rollback_file_batch(&completed)?;
            return Err(error.into());
        }
    };

    if recorded != receipt_rows.len() {
        rollback_file_batch(&completed)?;
        return Err(SelectedRootExecutorError::LocalFileReceiptPostconditionFailed);
    }

    Ok(SelectedRootFileBatchMaterialization {
        planned_file_actions: targets.len(),
        batch_action_limit: SUPERVISED_FILE_BATCH_MAX_ACTIONS,
        files_downloaded: completed.len(),
        bytes_downloaded: total_bytes,
        max_file_bytes: SUPERVISED_FILE_DOWNLOAD_MAX_BYTES,
        provider_fingerprints_verified: completed.len(),
        receipts_recorded: recorded,
    })
}

fn rollback_file_batch(
    completed: &[(ReceiveOnlyFileTarget, PathBuf, u64, String)],
) -> Result<(), SelectedRootExecutorError> {
    for (_, path, _, _) in completed.iter().rev() {
        rollback_created_file(path)?;
    }
    Ok(())
}

pub fn materialize_selected_root_missing_file<P: SelectedRootContentProvider>(
    provider: &P,
    storage: &mut Storage,
    sync_root: &SyncRoot,
) -> Result<SelectedRootFileMaterialization, SelectedRootExecutorError> {
    let (remote_items, local_entries) = selected_root_materialization_inputs(storage, sync_root)?;
    let preflight = plan_receive_only_materialization(&remote_items, &local_entries)?;
    if !preflight.ready_for_directory_phase()
        || preflight.missing_directories != 0
        || preflight.matching_directories != preflight.remote_directories
        || preflight.existing_files_unverified != 0
        || preflight.missing_files != 1
    {
        return Err(SelectedRootExecutorError::LocalFilePhaseBlocked);
    }

    let targets = plan_receive_only_missing_file_targets(&remote_items, &local_entries)?;
    if targets.len() != 1 {
        return Err(SelectedRootExecutorError::LocalFileTargetCountMismatch);
    }
    let target = targets
        .first()
        .ok_or(SelectedRootExecutorError::LocalFileTargetCountMismatch)?;
    let expected_size = target
        .size_bytes()
        .ok_or(SelectedRootExecutorError::LocalFileSizeUnknown)?;
    if expected_size > SUPERVISED_FILE_DOWNLOAD_MAX_BYTES {
        return Err(SelectedRootExecutorError::LocalFileTooLarge);
    }

    let root_path = validated_selected_root_path(sync_root)?;
    storage.invalidate_sync_root_local_observation_baseline(&sync_root.id)?;
    let outcome = apply_selected_root_file_target(
        provider,
        &root_path,
        target,
        SUPERVISED_FILE_DOWNLOAD_MAX_BYTES,
    )?;

    let post = (|| {
        let entries = scan_selected_root_local_tree(sync_root)?;
        let plan = plan_receive_only_materialization(&remote_items, &entries)?;
        if !plan.ready_for_directory_phase()
            || plan.missing_directories != 0
            || plan.missing_files != 0
            || plan.matching_directories != plan.remote_directories
            || plan.existing_files_unverified != 1
        {
            return Err(SelectedRootExecutorError::LocalFilePostconditionFailed);
        }
        storage.record_sync_root_file_materialization(
            &sync_root.id,
            target.remote_id(),
            target.relative_path(),
            expected_size,
            &outcome.sha256_hex,
            current_unix_time_ms()?,
        )?;

        Ok(SelectedRootFileMaterialization {
            files_downloaded: 1,
            bytes_downloaded: outcome.bytes_downloaded,
            max_file_bytes: SUPERVISED_FILE_DOWNLOAD_MAX_BYTES,
            size_match_verified: outcome.bytes_downloaded == expected_size,
        })
    })();

    match post {
        Ok(result) => Ok(result),
        Err(error) => {
            rollback_created_file(&outcome.target_path)?;
            Err(error)
        }
    }
}

struct FileApplyOutcome {
    target_path: PathBuf,
    bytes_downloaded: u64,
    sha256_hex: String,
}

fn apply_selected_root_file_target<P: SelectedRootContentProvider>(
    provider: &P,
    root_path: &Path,
    target: &ReceiveOnlyFileTarget,
    max_bytes: u64,
) -> Result<FileApplyOutcome, SelectedRootExecutorError> {
    let expected_size = target
        .size_bytes()
        .ok_or(SelectedRootExecutorError::LocalFileSizeUnknown)?;
    if expected_size > max_bytes {
        return Err(SelectedRootExecutorError::LocalFileTooLarge);
    }

    let target_path = root_path.join(target.relative_path());
    if !target_path.starts_with(root_path) {
        return Err(SelectedRootExecutorError::LocalFileTargetEscapedRoot);
    }
    match fs::symlink_metadata(&target_path) {
        Ok(_) => return Err(SelectedRootExecutorError::LocalFileTargetConflict),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err(SelectedRootExecutorError::LocalFilesystemInspectionFailed),
    }

    let parent = target_path
        .parent()
        .ok_or(SelectedRootExecutorError::LocalFileParentInvalid)?;
    let meta = fs::symlink_metadata(parent)
        .map_err(|_| SelectedRootExecutorError::LocalFileParentInvalid)?;
    if meta.file_type().is_symlink() || !meta.is_dir() {
        return Err(SelectedRootExecutorError::LocalFileParentInvalid);
    }
    let canonical_parent =
        fs::canonicalize(parent).map_err(|_| SelectedRootExecutorError::LocalFileParentInvalid)?;
    if !canonical_parent.starts_with(root_path) {
        return Err(SelectedRootExecutorError::LocalFileParentInvalid);
    }

    let (temp_path, mut temp_file) = create_download_temp(parent)?;
    let (bytes, sha256_hex) = {
        let mut hashing_writer = HashingWriter::new(&mut temp_file);
        let provider_bytes = match provider.download_file_content(
            target.remote_id(),
            max_bytes,
            &mut hashing_writer,
        ) {
            Ok(bytes) => bytes,
            Err(error) => {
                drop(temp_file);
                cleanup_temp_file(&temp_path)?;
                return Err(error);
            }
        };
        let hashed_bytes = hashing_writer.bytes_written();
        let sha256_hex = hashing_writer.finish_hex();
        if provider_bytes != hashed_bytes {
            drop(temp_file);
            cleanup_temp_file(&temp_path)?;
            return Err(SelectedRootExecutorError::LocalFileProviderByteCountMismatch);
        }
        (provider_bytes, sha256_hex)
    };
    if bytes != expected_size {
        drop(temp_file);
        cleanup_temp_file(&temp_path)?;
        return Err(SelectedRootExecutorError::LocalFileDownloadSizeMismatch);
    }
    if temp_file.sync_all().is_err() {
        drop(temp_file);
        cleanup_temp_file(&temp_path)?;
        return Err(SelectedRootExecutorError::LocalFileSyncFailed);
    }
    drop(temp_file);

    match fs::hard_link(&temp_path, &target_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            cleanup_temp_file(&temp_path)?;
            return Err(SelectedRootExecutorError::LocalFileTargetConflict);
        }
        Err(_) => {
            cleanup_temp_file(&temp_path)?;
            return Err(SelectedRootExecutorError::LocalFilePromoteFailed);
        }
    }

    if fs::remove_file(&temp_path).is_err() {
        rollback_created_file(&target_path)?;
        cleanup_temp_file(&temp_path)?;
        return Err(SelectedRootExecutorError::LocalFileTempCleanupFailed);
    }
    let parent_handle =
        fs::File::open(parent).map_err(|_| SelectedRootExecutorError::LocalFileSyncFailed)?;
    if parent_handle.sync_all().is_err() {
        rollback_created_file(&target_path)?;
        return Err(SelectedRootExecutorError::LocalFileSyncFailed);
    }
    let meta = fs::symlink_metadata(&target_path)
        .map_err(|_| SelectedRootExecutorError::LocalFilePostconditionFailed)?;
    if meta.file_type().is_symlink() || !meta.is_file() || meta.len() != bytes {
        rollback_created_file(&target_path)?;
        return Err(SelectedRootExecutorError::LocalFilePostconditionFailed);
    }
    Ok(FileApplyOutcome {
        target_path,
        bytes_downloaded: bytes,
        sha256_hex,
    })
}

struct HashingWriter<W: Write> {
    inner: W,
    hasher: Sha256,
    bytes_written: u64,
}

impl<W: Write> HashingWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
            bytes_written: 0,
        }
    }

    fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    fn finish_hex(self) -> String {
        digest_to_hex(self.hasher.finalize().as_slice())
    }
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buffer)?;
        self.hasher.update(&buffer[..written]);
        self.bytes_written = self
            .bytes_written
            .checked_add(written as u64)
            .ok_or_else(|| std::io::Error::other("NubiSync hash byte counter overflow"))?;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

fn digest_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn hash_local_file(
    path: &Path,
    max_bytes: u64,
) -> Result<(u64, String), SelectedRootExecutorError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|_| SelectedRootExecutorError::LocalFileVerifyFailed)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > max_bytes {
        return Err(SelectedRootExecutorError::LocalFileVerifyFailed);
    }

    let mut file =
        fs::File::open(path).map_err(|_| SelectedRootExecutorError::LocalFileVerifyFailed)?;
    let mut hasher = Sha256::new();
    let mut total = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];

    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|_| SelectedRootExecutorError::LocalFileVerifyFailed)?;
        if read == 0 {
            break;
        }
        total = total
            .checked_add(read as u64)
            .ok_or(SelectedRootExecutorError::CountOverflow)?;
        if total > max_bytes {
            return Err(SelectedRootExecutorError::LocalFileTooLarge);
        }
        hasher.update(&buffer[..read]);
    }

    Ok((total, digest_to_hex(hasher.finalize().as_slice())))
}

fn current_unix_time_ms() -> Result<i64, SelectedRootExecutorError> {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| SelectedRootExecutorError::ClockBeforeUnixEpoch)?;
    i64::try_from(duration.as_millis()).map_err(|_| SelectedRootExecutorError::CountOverflow)
}

fn create_download_temp(parent: &Path) -> Result<(PathBuf, fs::File), SelectedRootExecutorError> {
    for _ in 0..128 {
        let n = DOWNLOAD_TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = parent.join(format!(".nubisync-download-{}-{n}.tmp", std::process::id()));
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(_) => return Err(SelectedRootExecutorError::LocalFileTempCreateFailed),
        }
    }
    Err(SelectedRootExecutorError::LocalFileTempCreateFailed)
}

fn cleanup_temp_file(path: &Path) -> Result<(), SelectedRootExecutorError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(SelectedRootExecutorError::LocalFileTempCleanupFailed),
    }
}

fn rollback_created_file(path: &Path) -> Result<(), SelectedRootExecutorError> {
    match fs::remove_file(path) {
        Ok(()) => {
            if let Some(parent) = path.parent() {
                let dir = fs::File::open(parent)
                    .map_err(|_| SelectedRootExecutorError::LocalFileRollbackFailed)?;
                dir.sync_all()
                    .map_err(|_| SelectedRootExecutorError::LocalFileRollbackFailed)?;
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(SelectedRootExecutorError::LocalFileRollbackFailed),
    }
}

pub fn plan_selected_root_stale_files(
    storage: &Storage,
    sync_root: &SyncRoot,
) -> Result<SelectedRootStaleFileBatchPlan, SelectedRootExecutorError> {
    let convergence = plan_selected_root_receive_only_convergence(storage, sync_root)?;

    if convergence.blocked()
        || convergence.create_directories != 0
        || convergence.materialize_missing_files != 0
        || convergence.verify_existing_files != 0
        || convergence.delete_owned_empty_directories != 0
    {
        return Err(SelectedRootExecutorError::RemoteReplacementPlanPhaseBlocked);
    }

    let (remote_items, local_entries) = selected_root_materialization_inputs(storage, sync_root)?;
    let materialization = plan_receive_only_materialization(&remote_items, &local_entries)?;

    if materialization.missing_directories != 0
        || materialization.missing_files != 0
        || materialization.matching_directories != materialization.remote_directories
        || materialization.type_conflicts != 0
        || materialization.local_only_entries != convergence.revalidate_stale_file_deletions
    {
        return Err(SelectedRootExecutorError::RemoteReplacementPlanPhaseBlocked);
    }

    let current_count = storage.sync_root_materialization_receipt_count(&sync_root.id)?;
    let stale_count = storage.sync_root_stale_materialization_receipt_count(&sync_root.id)?;
    let stale_receipts =
        storage.list_sync_root_stale_file_materialization_receipts(&sync_root.id)?;

    let current_receipts =
        usize::try_from(current_count).map_err(|_| SelectedRootExecutorError::CountOverflow)?;
    let stale_receipts_total =
        usize::try_from(stale_count).map_err(|_| SelectedRootExecutorError::CountOverflow)?;

    if stale_receipts.len() != stale_receipts_total {
        return Err(SelectedRootExecutorError::LocalReceiptVerificationCountMismatch);
    }
    if stale_receipts_total > SUPERVISED_STALE_FILE_PLAN_MAX_ACTIONS {
        return Err(SelectedRootExecutorError::LocalReceiptVerificationSafetyLimitExceeded);
    }

    let expected_stale_actions = convergence
        .revalidate_stale_file_replacements
        .checked_add(convergence.revalidate_stale_file_deletions)
        .ok_or(SelectedRootExecutorError::CountOverflow)?;

    if expected_stale_actions != stale_receipts_total {
        return Err(SelectedRootExecutorError::RemoteReplacementPlanPhaseBlocked);
    }

    let current_targets = plan_receive_only_existing_file_targets(&remote_items, &local_entries)?;
    let root_path = validated_selected_root_path(sync_root)?;

    let mut result = SelectedRootStaleFileBatchPlan {
        max_actions: SUPERVISED_STALE_FILE_PLAN_MAX_ACTIONS,
        current_receipts,
        stale_receipts_total,
        replacement_candidates: 0,
        deletion_candidates: 0,
        safe_to_replace: 0,
        safe_to_delete: 0,
        local_conflicts: 0,
        files_missing: 0,
        type_conflicts: 0,
        bytes_hashed: 0,
        convergence_replacement_actions: convergence.revalidate_stale_file_replacements,
        convergence_deletion_actions: convergence.revalidate_stale_file_deletions,
    };

    for receipt in &stale_receipts {
        let remote_identity_present = remote_items
            .iter()
            .any(|item| item.remote_id == receipt.remote_id);

        let matching_target = current_targets.iter().find(|target| {
            target.remote_id() == receipt.remote_id.as_str()
                && target.relative_path() == receipt.relative_path.as_str()
        });

        if remote_identity_present {
            result.replacement_candidates = result
                .replacement_candidates
                .checked_add(1)
                .ok_or(SelectedRootExecutorError::CountOverflow)?;

            let Some(target) = matching_target else {
                return Err(SelectedRootExecutorError::RemoteReplacementPlanTargetMismatch);
            };

            let current_remote_size = target
                .size_bytes()
                .ok_or(SelectedRootExecutorError::LocalFileSizeUnknown)?;
            if current_remote_size > SUPERVISED_FILE_DOWNLOAD_MAX_BYTES {
                return Err(SelectedRootExecutorError::LocalFileTooLarge);
            }
        } else {
            result.deletion_candidates = result
                .deletion_candidates
                .checked_add(1)
                .ok_or(SelectedRootExecutorError::CountOverflow)?;
        }

        match inspect_receipt_target(&root_path, receipt)? {
            ReceiptTargetInspection::Missing => {
                result.files_missing = result
                    .files_missing
                    .checked_add(1)
                    .ok_or(SelectedRootExecutorError::CountOverflow)?;
            }
            ReceiptTargetInspection::Conflict => {
                result.type_conflicts = result
                    .type_conflicts
                    .checked_add(1)
                    .ok_or(SelectedRootExecutorError::CountOverflow)?;
            }
            ReceiptTargetInspection::File(target_path, observed_size) => {
                if observed_size != receipt.size_bytes {
                    result.local_conflicts = result
                        .local_conflicts
                        .checked_add(1)
                        .ok_or(SelectedRootExecutorError::CountOverflow)?;
                    continue;
                }

                if receipt.size_bytes > SUPERVISED_FILE_DOWNLOAD_MAX_BYTES {
                    return Err(SelectedRootExecutorError::LocalFileTooLarge);
                }

                let (bytes, sha256_hex) =
                    hash_local_file(&target_path, SUPERVISED_FILE_DOWNLOAD_MAX_BYTES)?;
                result.bytes_hashed = result
                    .bytes_hashed
                    .checked_add(bytes)
                    .ok_or(SelectedRootExecutorError::CountOverflow)?;

                if bytes == receipt.size_bytes && sha256_hex == receipt.sha256_hex {
                    if remote_identity_present {
                        result.safe_to_replace = result
                            .safe_to_replace
                            .checked_add(1)
                            .ok_or(SelectedRootExecutorError::CountOverflow)?;
                    } else {
                        result.safe_to_delete = result
                            .safe_to_delete
                            .checked_add(1)
                            .ok_or(SelectedRootExecutorError::CountOverflow)?;
                    }
                } else {
                    result.local_conflicts = result
                        .local_conflicts
                        .checked_add(1)
                        .ok_or(SelectedRootExecutorError::CountOverflow)?;
                }
            }
        }
    }

    if result.replacement_candidates != result.convergence_replacement_actions
        || result.deletion_candidates != result.convergence_deletion_actions
    {
        return Err(SelectedRootExecutorError::RemoteReplacementPlanTargetMismatch);
    }

    Ok(result)
}

pub fn plan_selected_root_remote_replacement(
    storage: &Storage,
    sync_root: &SyncRoot,
) -> Result<SelectedRootRemoteReplacementPlan, SelectedRootExecutorError> {
    let (remote_items, local_entries) = selected_root_materialization_inputs(storage, sync_root)?;
    let materialization = plan_receive_only_materialization(&remote_items, &local_entries)?;

    if !materialization.ready_for_directory_phase()
        || materialization.missing_directories != 0
        || materialization.missing_files != 0
        || materialization.matching_directories != materialization.remote_directories
        || materialization.remote_files != 1
        || materialization.existing_files_unverified != 1
    {
        return Err(SelectedRootExecutorError::RemoteReplacementPlanPhaseBlocked);
    }

    if storage.sync_root_materialization_receipt_count(&sync_root.id)? != 0 {
        return Err(SelectedRootExecutorError::RemoteReplacementPlanCurrentReceiptPresent);
    }

    let stale_count = storage.sync_root_stale_materialization_receipt_count(&sync_root.id)?;
    let stale_receipts =
        storage.list_sync_root_stale_file_materialization_receipts(&sync_root.id)?;

    if stale_count != 1 || stale_receipts.len() != 1 {
        return Err(SelectedRootExecutorError::RemoteReplacementPlanStaleReceiptCountMismatch);
    }

    let current_targets = plan_receive_only_existing_file_targets(&remote_items, &local_entries)?;

    if current_targets.len() != 1 {
        return Err(SelectedRootExecutorError::RemoteReplacementPlanTargetMismatch);
    }

    let receipt = stale_receipts
        .first()
        .ok_or(SelectedRootExecutorError::RemoteReplacementPlanStaleReceiptCountMismatch)?;
    let target = current_targets
        .first()
        .ok_or(SelectedRootExecutorError::RemoteReplacementPlanTargetMismatch)?;

    if target.remote_id() != receipt.remote_id.as_str()
        || target.relative_path() != receipt.relative_path.as_str()
    {
        return Err(SelectedRootExecutorError::RemoteReplacementPlanTargetMismatch);
    }

    let current_remote_size = target
        .size_bytes()
        .ok_or(SelectedRootExecutorError::LocalFileSizeUnknown)?;

    if current_remote_size > SUPERVISED_FILE_DOWNLOAD_MAX_BYTES {
        return Err(SelectedRootExecutorError::LocalFileTooLarge);
    }

    let root_path = validated_selected_root_path(sync_root)?;
    let mut result = SelectedRootRemoteReplacementPlan {
        stale_receipts_total: 1,
        replacement_candidates: 1,
        safe_to_replace: 0,
        local_conflicts: 0,
        files_missing: 0,
        type_conflicts: 0,
        bytes_hashed: 0,
    };

    match inspect_receipt_target(&root_path, receipt)? {
        ReceiptTargetInspection::Missing => {
            result.files_missing = 1;
        }
        ReceiptTargetInspection::Conflict => {
            result.type_conflicts = 1;
        }
        ReceiptTargetInspection::File(target_path, observed_size) => {
            if observed_size != receipt.size_bytes {
                result.local_conflicts = 1;
                return Ok(result);
            }

            if receipt.size_bytes > SUPERVISED_FILE_DOWNLOAD_MAX_BYTES {
                return Err(SelectedRootExecutorError::LocalFileTooLarge);
            }

            let (bytes, sha256_hex) =
                hash_local_file(&target_path, SUPERVISED_FILE_DOWNLOAD_MAX_BYTES)?;
            result.bytes_hashed = bytes;

            if bytes == receipt.size_bytes && sha256_hex == receipt.sha256_hex {
                result.safe_to_replace = 1;
            } else {
                result.local_conflicts = 1;
            }
        }
    }

    Ok(result)
}

pub fn plan_selected_root_remote_directory_deletion(
    storage: &Storage,
    sync_root: &SyncRoot,
) -> Result<SelectedRootRemoteDirectoryDeletionPlan, SelectedRootExecutorError> {
    let (remote_items, local_entries) = selected_root_materialization_inputs(storage, sync_root)?;
    let materialization = plan_receive_only_materialization(&remote_items, &local_entries)?;

    if materialization.remote_items != 0
        || materialization.remote_directories != 0
        || materialization.remote_files != 0
        || materialization.local_entries != 1
        || materialization.missing_directories != 0
        || materialization.missing_files != 0
        || materialization.matching_directories != 0
        || materialization.existing_files_unverified != 0
        || materialization.local_only_entries != 1
        || materialization.type_conflicts != 0
    {
        return Err(SelectedRootExecutorError::RemoteDirectoryDeletionPlanPhaseBlocked);
    }

    if storage.sync_root_materialization_receipt_count(&sync_root.id)? != 0
        || storage.sync_root_stale_materialization_receipt_count(&sync_root.id)? != 0
    {
        return Err(SelectedRootExecutorError::RemoteDirectoryDeletionPlanFileReceiptPresent);
    }

    if storage.sync_root_directory_materialization_receipt_count(&sync_root.id)? != 0 {
        return Err(SelectedRootExecutorError::RemoteDirectoryDeletionPlanCurrentReceiptPresent);
    }

    let stale_count =
        storage.sync_root_stale_directory_materialization_receipt_count(&sync_root.id)?;
    let stale_receipts =
        storage.list_sync_root_stale_directory_materialization_receipts(&sync_root.id)?;

    if stale_count != 1 || stale_receipts.len() != 1 {
        return Err(
            SelectedRootExecutorError::RemoteDirectoryDeletionPlanStaleReceiptCountMismatch,
        );
    }

    let receipt = stale_receipts
        .first()
        .ok_or(SelectedRootExecutorError::RemoteDirectoryDeletionPlanStaleReceiptCountMismatch)?;

    let remote_id_absent = !remote_items
        .iter()
        .any(|item| item.remote_id == receipt.remote_id);

    if !remote_id_absent {
        return Err(
            SelectedRootExecutorError::RemoteDirectoryDeletionPlanRemoteIdentityStillPresent,
        );
    }

    let root_path = validated_selected_root_path(sync_root)?;
    let mut result = SelectedRootRemoteDirectoryDeletionPlan {
        stale_receipts_total: 1,
        deletion_candidates: 1,
        safe_to_delete: 0,
        directories_already_missing: 0,
        non_empty_directories: 0,
        type_conflicts: 0,
        remote_id_absent,
    };

    match inspect_directory_receipt_target(&root_path, receipt)? {
        DirectoryReceiptTargetInspection::Missing => {
            result.directories_already_missing = 1;
        }
        DirectoryReceiptTargetInspection::Conflict => {
            result.type_conflicts = 1;
        }
        DirectoryReceiptTargetInspection::NonEmptyDirectory => {
            result.non_empty_directories = 1;
        }
        DirectoryReceiptTargetInspection::EmptyDirectory => {
            result.safe_to_delete = 1;
        }
    }

    Ok(result)
}

enum DirectoryReceiptTargetInspection {
    Missing,
    Conflict,
    NonEmptyDirectory,
    EmptyDirectory,
}

fn inspect_directory_receipt_target(
    root_path: &Path,
    receipt: &SyncRootDirectoryMaterializationReceipt,
) -> Result<DirectoryReceiptTargetInspection, SelectedRootExecutorError> {
    let relative = Path::new(&receipt.relative_path);

    if relative.is_absolute() {
        return Err(SelectedRootExecutorError::LocalReceiptPathInvalid);
    }

    let mut current = root_path.to_path_buf();
    let mut components = relative.components().peekable();

    if components.peek().is_none() {
        return Err(SelectedRootExecutorError::LocalReceiptPathInvalid);
    }

    while let Some(component) = components.next() {
        let Component::Normal(name) = component else {
            return Err(SelectedRootExecutorError::LocalReceiptPathInvalid);
        };

        current.push(name);
        if !current.starts_with(root_path) {
            return Err(SelectedRootExecutorError::LocalReceiptPathInvalid);
        }

        let is_last = components.peek().is_none();
        let metadata = match fs::symlink_metadata(&current) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(DirectoryReceiptTargetInspection::Missing);
            }
            Err(_) => {
                return Err(SelectedRootExecutorError::LocalFilesystemInspectionFailed);
            }
        };

        if metadata.file_type().is_symlink() {
            return Ok(DirectoryReceiptTargetInspection::Conflict);
        }

        if !metadata.is_dir() {
            return Ok(DirectoryReceiptTargetInspection::Conflict);
        }

        let canonical = fs::canonicalize(&current)
            .map_err(|_| SelectedRootExecutorError::LocalFilesystemInspectionFailed)?;
        if !canonical.starts_with(root_path) {
            return Err(SelectedRootExecutorError::LocalReceiptPathInvalid);
        }

        if is_last {
            let mut entries = fs::read_dir(&current)
                .map_err(|_| SelectedRootExecutorError::LocalFilesystemInspectionFailed)?;

            return match entries.next() {
                None => Ok(DirectoryReceiptTargetInspection::EmptyDirectory),
                Some(Ok(_)) => Ok(DirectoryReceiptTargetInspection::NonEmptyDirectory),
                Some(Err(_)) => Err(SelectedRootExecutorError::LocalFilesystemInspectionFailed),
            };
        }
    }

    Err(SelectedRootExecutorError::LocalReceiptPathInvalid)
}

pub fn delete_selected_root_existing_directory(
    storage: &mut Storage,
    sync_root: &SyncRoot,
) -> Result<SelectedRootDirectoryDeletion, SelectedRootExecutorError> {
    let readiness = plan_selected_root_remote_directory_deletion(storage, sync_root)?;
    if !readiness.ready() {
        return Err(SelectedRootExecutorError::RemoteDirectoryDeletionNotReady);
    }

    let stale_receipts =
        storage.list_sync_root_stale_directory_materialization_receipts(&sync_root.id)?;
    if stale_receipts.len() != 1 {
        return Err(
            SelectedRootExecutorError::RemoteDirectoryDeletionPlanStaleReceiptCountMismatch,
        );
    }

    let receipt = stale_receipts
        .first()
        .ok_or(SelectedRootExecutorError::RemoteDirectoryDeletionPlanStaleReceiptCountMismatch)?;

    let root_path = validated_selected_root_path(sync_root)?;
    storage.invalidate_sync_root_local_observation_baseline(&sync_root.id)?;
    delete_verified_stale_local_directory(&root_path, receipt)?;

    let receipt_deleted = storage.delete_sync_root_stale_directory_materialization_receipt(
        &sync_root.id,
        &receipt.remote_id,
    )?;
    if !receipt_deleted {
        return Err(SelectedRootExecutorError::LocalDirectoryDeletionReceiptCleanupFailed);
    }

    if storage.sync_root_directory_materialization_receipt_count(&sync_root.id)? != 0
        || storage.sync_root_stale_directory_materialization_receipt_count(&sync_root.id)? != 0
        || storage.sync_root_materialization_receipt_count(&sync_root.id)? != 0
        || storage.sync_root_stale_materialization_receipt_count(&sync_root.id)? != 0
    {
        return Err(SelectedRootExecutorError::LocalDirectoryDeletionReceiptCleanupFailed);
    }

    let (remote_items, local_entries) = selected_root_materialization_inputs(storage, sync_root)?;
    let post = plan_receive_only_materialization(&remote_items, &local_entries)?;

    if post.remote_items != 0
        || post.remote_directories != 0
        || post.remote_files != 0
        || post.local_entries != 0
        || post.missing_directories != 0
        || post.missing_files != 0
        || post.matching_directories != 0
        || post.existing_files_unverified != 0
        || post.local_only_entries != 0
        || post.type_conflicts != 0
    {
        return Err(SelectedRootExecutorError::LocalDirectoryDeletionPostconditionFailed);
    }

    Ok(SelectedRootDirectoryDeletion {
        directories_deleted: 1,
        empty_directory_verified: true,
        receipt_deleted: true,
        quarantine_rename: true,
    })
}

#[derive(Debug)]
struct QuarantinedStaleDirectoryDeletion {
    remote_id: String,
    target_path: PathBuf,
    quarantine_path: PathBuf,
    metadata_before: fs::Metadata,
}

pub fn delete_selected_root_stale_directories(
    storage: &mut Storage,
    sync_root: &SyncRoot,
) -> Result<SelectedRootStaleDirectoryBatchDeletion, SelectedRootExecutorError> {
    let convergence = plan_selected_root_receive_only_convergence(storage, sync_root)?;
    let planned_deletions = convergence.delete_owned_empty_directories;

    if convergence.blocked()
        || planned_deletions == 0
        || planned_deletions > SUPERVISED_STALE_DIRECTORY_DELETION_MAX_ACTIONS
        || convergence.create_directories != 0
        || convergence.materialize_missing_files != 0
        || convergence.verify_existing_files != 0
        || convergence.revalidate_stale_file_replacements != 0
        || convergence.revalidate_stale_file_deletions != 0
    {
        return Err(SelectedRootExecutorError::RemoteDirectoryDeletionBatchNotReady);
    }

    let stale_receipts =
        storage.list_sync_root_stale_directory_materialization_receipts(&sync_root.id)?;
    if stale_receipts.len() != planned_deletions
        || stale_receipts.len() > SUPERVISED_STALE_DIRECTORY_DELETION_MAX_ACTIONS
    {
        return Err(SelectedRootExecutorError::RemoteDirectoryDeletionBatchTargetMismatch);
    }

    let remote_items = storage.list_sync_root_remote_items(&sync_root.id)?;
    if stale_receipts.iter().any(|receipt| {
        remote_items
            .iter()
            .any(|item| item.remote_id == receipt.remote_id)
    }) {
        return Err(SelectedRootExecutorError::RemoteDirectoryDeletionBatchTargetMismatch);
    }

    let root_path = validated_selected_root_path(sync_root)?;
    storage.invalidate_sync_root_local_observation_baseline(&sync_root.id)?;
    let mut quarantined = Vec::with_capacity(stale_receipts.len());

    for receipt in &stale_receipts {
        let outcome = match quarantine_verified_stale_local_directory(&root_path, receipt) {
            Ok(value) => value,
            Err(error) => {
                rollback_stale_directory_deletion_batch(&quarantined)?;
                return Err(error);
            }
        };
        quarantined.push(outcome);
    }

    let remote_ids = quarantined
        .iter()
        .map(|item| item.remote_id.clone())
        .collect::<Vec<_>>();

    let deleted_receipts = match storage
        .delete_sync_root_stale_directory_materialization_receipts(&sync_root.id, &remote_ids)
    {
        Ok(value) => value,
        Err(error) => {
            rollback_stale_directory_deletion_batch(&quarantined)?;
            return Err(error.into());
        }
    };

    if deleted_receipts != quarantined.len()
        || storage.sync_root_stale_directory_materialization_receipt_count(&sync_root.id)? != 0
    {
        return Err(SelectedRootExecutorError::LocalDirectoryDeletionBatchPostconditionFailed);
    }

    let removed_quarantines = cleanup_stale_directory_deletion_quarantines(&quarantined)?;

    let convergence = plan_selected_root_receive_only_convergence(storage, sync_root)?;
    if convergence.blocked()
        || convergence.create_directories != 0
        || convergence.materialize_missing_files != 0
        || convergence.verify_existing_files != 0
        || convergence.revalidate_stale_file_replacements != 0
        || convergence.revalidate_stale_file_deletions != 0
        || convergence.delete_owned_empty_directories != 0
    {
        return Err(SelectedRootExecutorError::LocalDirectoryDeletionBatchPostconditionFailed);
    }

    Ok(SelectedRootStaleDirectoryBatchDeletion {
        planned_deletion_actions: planned_deletions,
        batch_action_limit: SUPERVISED_STALE_DIRECTORY_DELETION_MAX_ACTIONS,
        directories_deleted: quarantined.len(),
        empty_directories_verified: quarantined.len(),
        receipts_deleted: deleted_receipts,
        quarantine_renames: quarantined.len(),
        quarantine_directories_removed: removed_quarantines,
    })
}

fn quarantine_verified_stale_local_directory(
    root_path: &Path,
    receipt: &SyncRootDirectoryMaterializationReceipt,
) -> Result<QuarantinedStaleDirectoryDeletion, SelectedRootExecutorError> {
    if !matches!(
        inspect_directory_receipt_target(root_path, receipt)?,
        DirectoryReceiptTargetInspection::EmptyDirectory
    ) {
        return Err(SelectedRootExecutorError::RemoteDirectoryDeletionNotReady);
    }

    let target_path = root_path.join(&receipt.relative_path);
    if !target_path.starts_with(root_path) {
        return Err(SelectedRootExecutorError::LocalReceiptPathInvalid);
    }

    let metadata_before = fs::symlink_metadata(&target_path)
        .map_err(|_| SelectedRootExecutorError::RemoteDirectoryDeletionTargetRace)?;
    if metadata_before.file_type().is_symlink() || !metadata_before.is_dir() {
        return Err(SelectedRootExecutorError::RemoteDirectoryDeletionTargetRace);
    }

    let mut entries = fs::read_dir(&target_path)
        .map_err(|_| SelectedRootExecutorError::LocalFilesystemInspectionFailed)?;
    if entries.next().is_some() {
        return Err(SelectedRootExecutorError::RemoteDirectoryDeletionNotReady);
    }

    if !matches!(
        inspect_directory_receipt_target(root_path, receipt)?,
        DirectoryReceiptTargetInspection::EmptyDirectory
    ) {
        return Err(SelectedRootExecutorError::RemoteDirectoryDeletionTargetRace);
    }

    let parent = target_path
        .parent()
        .ok_or(SelectedRootExecutorError::LocalDirectoryParentInvalid)?;
    let parent_metadata = fs::symlink_metadata(parent)
        .map_err(|_| SelectedRootExecutorError::LocalDirectoryParentInvalid)?;
    if parent_metadata.file_type().is_symlink() || !parent_metadata.is_dir() {
        return Err(SelectedRootExecutorError::LocalDirectoryParentInvalid);
    }
    let canonical_parent = fs::canonicalize(parent)
        .map_err(|_| SelectedRootExecutorError::LocalDirectoryParentInvalid)?;
    if !canonical_parent.starts_with(root_path) {
        return Err(SelectedRootExecutorError::LocalDirectoryParentInvalid);
    }

    let quarantine_path = unique_directory_deletion_quarantine_path(parent)?;
    fs::rename(&target_path, &quarantine_path)
        .map_err(|_| SelectedRootExecutorError::LocalDirectoryDeletionRenameFailed)?;

    let quarantine_meta = match fs::symlink_metadata(&quarantine_path) {
        Ok(meta) => meta,
        Err(_) => {
            rollback_directory_deletion_quarantine(&quarantine_path, &target_path, parent)?;
            return Err(SelectedRootExecutorError::RemoteDirectoryDeletionTargetRace);
        }
    };

    if !same_local_directory_identity(&metadata_before, &quarantine_meta) {
        rollback_directory_deletion_quarantine(&quarantine_path, &target_path, parent)?;
        return Err(SelectedRootExecutorError::RemoteDirectoryDeletionTargetRace);
    }

    let mut quarantine_entries = fs::read_dir(&quarantine_path)
        .map_err(|_| SelectedRootExecutorError::LocalFilesystemInspectionFailed)?;
    if quarantine_entries.next().is_some() {
        rollback_directory_deletion_quarantine(&quarantine_path, &target_path, parent)?;
        return Err(SelectedRootExecutorError::RemoteDirectoryDeletionNotReady);
    }

    if let Err(error) = sync_parent_directory(parent) {
        rollback_directory_deletion_quarantine(&quarantine_path, &target_path, parent)?;
        return Err(error);
    }

    match fs::symlink_metadata(&target_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        _ => {
            rollback_directory_deletion_quarantine(&quarantine_path, &target_path, parent)?;
            return Err(SelectedRootExecutorError::LocalDirectoryDeletionPostconditionFailed);
        }
    }

    Ok(QuarantinedStaleDirectoryDeletion {
        remote_id: receipt.remote_id.clone(),
        target_path,
        quarantine_path,
        metadata_before,
    })
}

fn rollback_quarantined_stale_directory(
    item: &QuarantinedStaleDirectoryDeletion,
) -> Result<(), SelectedRootExecutorError> {
    if fs::symlink_metadata(&item.target_path).is_ok() {
        return Err(SelectedRootExecutorError::LocalDirectoryDeletionRollbackFailed);
    }

    let quarantine_meta = fs::symlink_metadata(&item.quarantine_path)
        .map_err(|_| SelectedRootExecutorError::LocalDirectoryDeletionRollbackFailed)?;
    if !same_local_directory_identity(&item.metadata_before, &quarantine_meta) {
        return Err(SelectedRootExecutorError::LocalDirectoryDeletionRollbackFailed);
    }

    let mut entries = fs::read_dir(&item.quarantine_path)
        .map_err(|_| SelectedRootExecutorError::LocalDirectoryDeletionRollbackFailed)?;
    if entries.next().is_some() {
        return Err(SelectedRootExecutorError::LocalDirectoryDeletionRollbackFailed);
    }

    let parent = item
        .target_path
        .parent()
        .ok_or(SelectedRootExecutorError::LocalDirectoryDeletionRollbackFailed)?;
    fs::rename(&item.quarantine_path, &item.target_path)
        .map_err(|_| SelectedRootExecutorError::LocalDirectoryDeletionRollbackFailed)?;
    sync_parent_directory(parent)
        .map_err(|_| SelectedRootExecutorError::LocalDirectoryDeletionRollbackFailed)
}

fn rollback_stale_directory_deletion_batch(
    items: &[QuarantinedStaleDirectoryDeletion],
) -> Result<(), SelectedRootExecutorError> {
    for item in items.iter().rev() {
        rollback_quarantined_stale_directory(item)?;
    }
    Ok(())
}

fn cleanup_stale_directory_deletion_quarantines(
    items: &[QuarantinedStaleDirectoryDeletion],
) -> Result<usize, SelectedRootExecutorError> {
    let mut removed = 0_usize;

    for item in items {
        if fs::symlink_metadata(&item.target_path).is_ok() {
            return Err(SelectedRootExecutorError::LocalDirectoryDeletionBatchCleanupFailed);
        }

        let quarantine_meta = fs::symlink_metadata(&item.quarantine_path)
            .map_err(|_| SelectedRootExecutorError::LocalDirectoryDeletionBatchCleanupFailed)?;
        if !same_local_directory_identity(&item.metadata_before, &quarantine_meta) {
            return Err(SelectedRootExecutorError::LocalDirectoryDeletionBatchCleanupFailed);
        }

        let mut entries = fs::read_dir(&item.quarantine_path)
            .map_err(|_| SelectedRootExecutorError::LocalDirectoryDeletionBatchCleanupFailed)?;
        if entries.next().is_some() {
            return Err(SelectedRootExecutorError::LocalDirectoryDeletionBatchCleanupFailed);
        }

        fs::remove_dir(&item.quarantine_path)
            .map_err(|_| SelectedRootExecutorError::LocalDirectoryDeletionBatchCleanupFailed)?;

        let parent = item
            .target_path
            .parent()
            .ok_or(SelectedRootExecutorError::LocalDirectoryDeletionBatchCleanupFailed)?;
        sync_parent_directory(parent)
            .map_err(|_| SelectedRootExecutorError::LocalDirectoryDeletionBatchCleanupFailed)?;

        removed = removed
            .checked_add(1)
            .ok_or(SelectedRootExecutorError::CountOverflow)?;
    }

    Ok(removed)
}

fn delete_verified_stale_local_directory(
    root_path: &Path,
    receipt: &SyncRootDirectoryMaterializationReceipt,
) -> Result<(), SelectedRootExecutorError> {
    if !matches!(
        inspect_directory_receipt_target(root_path, receipt)?,
        DirectoryReceiptTargetInspection::EmptyDirectory
    ) {
        return Err(SelectedRootExecutorError::RemoteDirectoryDeletionNotReady);
    }

    let target_path = root_path.join(&receipt.relative_path);
    if !target_path.starts_with(root_path) {
        return Err(SelectedRootExecutorError::LocalReceiptPathInvalid);
    }

    let metadata_before = fs::symlink_metadata(&target_path)
        .map_err(|_| SelectedRootExecutorError::RemoteDirectoryDeletionTargetRace)?;
    if metadata_before.file_type().is_symlink() || !metadata_before.is_dir() {
        return Err(SelectedRootExecutorError::RemoteDirectoryDeletionTargetRace);
    }

    let mut entries = fs::read_dir(&target_path)
        .map_err(|_| SelectedRootExecutorError::LocalFilesystemInspectionFailed)?;
    if entries.next().is_some() {
        return Err(SelectedRootExecutorError::RemoteDirectoryDeletionNotReady);
    }

    if !matches!(
        inspect_directory_receipt_target(root_path, receipt)?,
        DirectoryReceiptTargetInspection::EmptyDirectory
    ) {
        return Err(SelectedRootExecutorError::RemoteDirectoryDeletionTargetRace);
    }

    let parent = target_path
        .parent()
        .ok_or(SelectedRootExecutorError::LocalDirectoryParentInvalid)?;
    let quarantine_path = unique_directory_deletion_quarantine_path(parent)?;

    fs::rename(&target_path, &quarantine_path)
        .map_err(|_| SelectedRootExecutorError::LocalDirectoryDeletionRenameFailed)?;

    let quarantine_meta = match fs::symlink_metadata(&quarantine_path) {
        Ok(meta) => meta,
        Err(_) => {
            rollback_directory_deletion_quarantine(&quarantine_path, &target_path, parent)?;
            return Err(SelectedRootExecutorError::RemoteDirectoryDeletionTargetRace);
        }
    };

    if !same_local_directory_identity(&metadata_before, &quarantine_meta) {
        rollback_directory_deletion_quarantine(&quarantine_path, &target_path, parent)?;
        return Err(SelectedRootExecutorError::RemoteDirectoryDeletionTargetRace);
    }

    let mut quarantine_entries = fs::read_dir(&quarantine_path)
        .map_err(|_| SelectedRootExecutorError::LocalFilesystemInspectionFailed)?;
    if quarantine_entries.next().is_some() {
        rollback_directory_deletion_quarantine(&quarantine_path, &target_path, parent)?;
        return Err(SelectedRootExecutorError::RemoteDirectoryDeletionNotReady);
    }

    if let Err(error) = sync_parent_directory(parent) {
        rollback_directory_deletion_quarantine(&quarantine_path, &target_path, parent)?;
        return Err(error);
    }

    if fs::remove_dir(&quarantine_path).is_err() {
        rollback_directory_deletion_quarantine(&quarantine_path, &target_path, parent)?;
        return Err(SelectedRootExecutorError::LocalDirectoryDeletionRemoveFailed);
    }

    sync_parent_directory(parent)?;

    match fs::symlink_metadata(&target_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        _ => return Err(SelectedRootExecutorError::LocalDirectoryDeletionPostconditionFailed),
    }

    match fs::symlink_metadata(&quarantine_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        _ => return Err(SelectedRootExecutorError::LocalDirectoryDeletionPostconditionFailed),
    }

    Ok(())
}

fn unique_directory_deletion_quarantine_path(
    parent: &Path,
) -> Result<PathBuf, SelectedRootExecutorError> {
    for _ in 0..128 {
        let counter = DOWNLOAD_TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let candidate = parent.join(format!(
            ".nubisync-delete-dir-{}-{counter}.tmp",
            std::process::id()
        ));

        match fs::symlink_metadata(&candidate) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(candidate);
            }
            Ok(_) => continue,
            Err(_) => {
                return Err(SelectedRootExecutorError::LocalFilesystemInspectionFailed);
            }
        }
    }

    Err(SelectedRootExecutorError::LocalDirectoryDeletionQuarantineUnavailable)
}

fn rollback_directory_deletion_quarantine(
    quarantine_path: &Path,
    target_path: &Path,
    parent: &Path,
) -> Result<(), SelectedRootExecutorError> {
    if fs::symlink_metadata(target_path).is_ok() {
        return Err(SelectedRootExecutorError::LocalDirectoryDeletionRollbackFailed);
    }

    fs::rename(quarantine_path, target_path)
        .map_err(|_| SelectedRootExecutorError::LocalDirectoryDeletionRollbackFailed)?;
    sync_parent_directory(parent)
        .map_err(|_| SelectedRootExecutorError::LocalDirectoryDeletionRollbackFailed)
}

fn same_local_directory_identity(before: &fs::Metadata, after: &fs::Metadata) -> bool {
    before.file_type().is_dir()
        && after.file_type().is_dir()
        && !before.file_type().is_symlink()
        && !after.file_type().is_symlink()
        && before.dev() == after.dev()
        && before.ino() == after.ino()
}

pub fn plan_selected_root_remote_deletion(
    storage: &Storage,
    sync_root: &SyncRoot,
) -> Result<SelectedRootRemoteDeletionPlan, SelectedRootExecutorError> {
    let (remote_items, local_entries) = selected_root_materialization_inputs(storage, sync_root)?;
    let materialization = plan_receive_only_materialization(&remote_items, &local_entries)?;

    if materialization.missing_directories != 0
        || materialization.missing_files != 0
        || materialization.matching_directories != materialization.remote_directories
        || materialization.remote_files != 0
        || materialization.existing_files_unverified != 0
        || materialization.local_only_entries != 1
        || materialization.type_conflicts != 0
    {
        return Err(SelectedRootExecutorError::RemoteDeletionPlanPhaseBlocked);
    }

    if storage.sync_root_materialization_receipt_count(&sync_root.id)? != 0 {
        return Err(SelectedRootExecutorError::RemoteDeletionPlanCurrentReceiptPresent);
    }

    let stale_count = storage.sync_root_stale_materialization_receipt_count(&sync_root.id)?;
    let stale_receipts =
        storage.list_sync_root_stale_file_materialization_receipts(&sync_root.id)?;

    if stale_count != 1 || stale_receipts.len() != 1 {
        return Err(SelectedRootExecutorError::RemoteDeletionPlanStaleReceiptCountMismatch);
    }

    let receipt = stale_receipts
        .first()
        .ok_or(SelectedRootExecutorError::RemoteDeletionPlanStaleReceiptCountMismatch)?;

    let remote_id_absent = !remote_items
        .iter()
        .any(|item| item.remote_id == receipt.remote_id);

    if !remote_id_absent {
        return Err(SelectedRootExecutorError::RemoteDeletionPlanRemoteIdentityStillPresent);
    }

    let root_path = validated_selected_root_path(sync_root)?;
    let mut result = SelectedRootRemoteDeletionPlan {
        stale_receipts_total: 1,
        deletion_candidates: 1,
        safe_to_delete: 0,
        local_conflicts: 0,
        files_already_missing: 0,
        type_conflicts: 0,
        bytes_hashed: 0,
        remote_id_absent,
    };

    match inspect_receipt_target(&root_path, receipt)? {
        ReceiptTargetInspection::Missing => {
            result.files_already_missing = 1;
        }
        ReceiptTargetInspection::Conflict => {
            result.type_conflicts = 1;
        }
        ReceiptTargetInspection::File(target_path, observed_size) => {
            if observed_size != receipt.size_bytes {
                result.local_conflicts = 1;
                return Ok(result);
            }

            if receipt.size_bytes > SUPERVISED_FILE_DOWNLOAD_MAX_BYTES {
                return Err(SelectedRootExecutorError::LocalFileTooLarge);
            }

            let (bytes, sha256_hex) =
                hash_local_file(&target_path, SUPERVISED_FILE_DOWNLOAD_MAX_BYTES)?;
            result.bytes_hashed = bytes;

            if bytes == receipt.size_bytes && sha256_hex == receipt.sha256_hex {
                result.safe_to_delete = 1;
            } else {
                result.local_conflicts = 1;
            }
        }
    }

    Ok(result)
}

struct QuarantinedStaleFileDeletion {
    remote_id: String,
    target_path: PathBuf,
    quarantine_path: PathBuf,
    metadata_before: fs::Metadata,
    bytes_verified: u64,
    sha256_hex: String,
}

pub fn delete_selected_root_stale_files(
    storage: &mut Storage,
    sync_root: &SyncRoot,
) -> Result<SelectedRootStaleFileBatchDeletion, SelectedRootExecutorError> {
    let readiness = plan_selected_root_stale_files(storage, sync_root)?;

    if readiness.stale_receipts_total == 0
        || readiness.stale_receipts_total > SUPERVISED_STALE_FILE_PLAN_MAX_ACTIONS
        || readiness.replacement_candidates != 0
        || readiness.safe_to_replace != 0
        || readiness.deletion_candidates != readiness.stale_receipts_total
        || readiness.safe_to_delete != readiness.deletion_candidates
        || readiness.local_conflicts != 0
        || readiness.files_missing != 0
        || readiness.type_conflicts != 0
        || readiness.convergence_replacement_actions != 0
        || readiness.convergence_deletion_actions != readiness.deletion_candidates
    {
        return Err(SelectedRootExecutorError::RemoteDeletionBatchNotReady);
    }

    let stale_receipts =
        storage.list_sync_root_stale_file_materialization_receipts(&sync_root.id)?;
    if stale_receipts.len() != readiness.stale_receipts_total {
        return Err(SelectedRootExecutorError::RemoteDeletionPlanStaleReceiptCountMismatch);
    }

    let remote_items = storage.list_sync_root_remote_items(&sync_root.id)?;
    if stale_receipts.iter().any(|receipt| {
        remote_items
            .iter()
            .any(|item| item.remote_id == receipt.remote_id)
    }) {
        return Err(SelectedRootExecutorError::RemoteDeletionBatchNotReady);
    }

    let root_path = validated_selected_root_path(sync_root)?;
    storage.invalidate_sync_root_local_observation_baseline(&sync_root.id)?;
    let mut quarantined = Vec::with_capacity(stale_receipts.len());
    let mut total_bytes = 0_u64;

    for receipt in &stale_receipts {
        let outcome = match quarantine_verified_stale_local_file(&root_path, receipt) {
            Ok(value) => value,
            Err(error) => {
                rollback_stale_file_deletion_batch(&quarantined)?;
                return Err(error);
            }
        };

        total_bytes = match total_bytes.checked_add(outcome.bytes_verified) {
            Some(value) => value,
            None => {
                rollback_quarantined_stale_file(&outcome)?;
                rollback_stale_file_deletion_batch(&quarantined)?;
                return Err(SelectedRootExecutorError::CountOverflow);
            }
        };

        quarantined.push(outcome);
    }

    let remote_ids = quarantined
        .iter()
        .map(|item| item.remote_id.clone())
        .collect::<Vec<_>>();

    let deleted_receipts = match storage
        .delete_sync_root_stale_file_materialization_receipts(&sync_root.id, &remote_ids)
    {
        Ok(value) => value,
        Err(error) => {
            rollback_stale_file_deletion_batch(&quarantined)?;
            return Err(error.into());
        }
    };

    if deleted_receipts != quarantined.len()
        || storage.sync_root_stale_materialization_receipt_count(&sync_root.id)? != 0
    {
        return Err(SelectedRootExecutorError::LocalFileDeletionBatchPostconditionFailed);
    }

    let removed_quarantines = cleanup_stale_file_deletion_quarantines(&quarantined)?;

    let convergence = plan_selected_root_receive_only_convergence(storage, sync_root)?;
    if convergence.blocked()
        || convergence.create_directories != 0
        || convergence.materialize_missing_files != 0
        || convergence.verify_existing_files != 0
        || convergence.revalidate_stale_file_replacements != 0
        || convergence.revalidate_stale_file_deletions != 0
        || convergence.delete_owned_empty_directories != 0
    {
        return Err(SelectedRootExecutorError::LocalFileDeletionBatchPostconditionFailed);
    }

    Ok(SelectedRootStaleFileBatchDeletion {
        planned_deletion_actions: readiness.deletion_candidates,
        batch_action_limit: SUPERVISED_STALE_FILE_PLAN_MAX_ACTIONS,
        files_deleted: quarantined.len(),
        bytes_verified: total_bytes,
        stale_baselines_verified: quarantined.len(),
        receipts_deleted: deleted_receipts,
        quarantine_renames: quarantined.len(),
        quarantine_files_removed: removed_quarantines,
    })
}

fn quarantine_verified_stale_local_file(
    root_path: &Path,
    receipt: &SyncRootFileMaterializationReceipt,
) -> Result<QuarantinedStaleFileDeletion, SelectedRootExecutorError> {
    let (target_path, observed_size) = match inspect_receipt_target(root_path, receipt)? {
        ReceiptTargetInspection::File(path, size) => (path, size),
        ReceiptTargetInspection::Missing | ReceiptTargetInspection::Conflict => {
            return Err(SelectedRootExecutorError::RemoteDeletionNotReady);
        }
    };

    if observed_size != receipt.size_bytes {
        return Err(SelectedRootExecutorError::RemoteDeletionLocalConflict);
    }
    if receipt.size_bytes > SUPERVISED_FILE_DOWNLOAD_MAX_BYTES {
        return Err(SelectedRootExecutorError::LocalFileTooLarge);
    }

    let metadata_before = fs::symlink_metadata(&target_path)
        .map_err(|_| SelectedRootExecutorError::RemoteDeletionTargetRace)?;
    if metadata_before.file_type().is_symlink() || !metadata_before.is_file() {
        return Err(SelectedRootExecutorError::RemoteDeletionTargetRace);
    }

    let (bytes, sha256_hex) = hash_local_file(&target_path, SUPERVISED_FILE_DOWNLOAD_MAX_BYTES)?;
    if bytes != receipt.size_bytes || sha256_hex != receipt.sha256_hex {
        return Err(SelectedRootExecutorError::RemoteDeletionLocalConflict);
    }

    let metadata_after = fs::symlink_metadata(&target_path)
        .map_err(|_| SelectedRootExecutorError::RemoteDeletionTargetRace)?;
    if !same_local_file_state(&metadata_before, &metadata_after) {
        return Err(SelectedRootExecutorError::RemoteDeletionTargetRace);
    }

    let parent = target_path
        .parent()
        .ok_or(SelectedRootExecutorError::LocalFileParentInvalid)?;
    let quarantine_path = unique_deletion_quarantine_path(parent)?;

    fs::rename(&target_path, &quarantine_path)
        .map_err(|_| SelectedRootExecutorError::LocalFileDeletionRenameFailed)?;

    let quarantine_meta = match fs::symlink_metadata(&quarantine_path) {
        Ok(meta) => meta,
        Err(_) => {
            rollback_deletion_quarantine(&quarantine_path, &target_path, parent)?;
            return Err(SelectedRootExecutorError::RemoteDeletionTargetRace);
        }
    };

    if !same_local_file_identity(&metadata_after, &quarantine_meta) {
        rollback_deletion_quarantine(&quarantine_path, &target_path, parent)?;
        return Err(SelectedRootExecutorError::RemoteDeletionTargetRace);
    }

    let quarantine_before_hash = quarantine_meta;
    let (quarantine_bytes, quarantine_sha256) =
        match hash_local_file(&quarantine_path, SUPERVISED_FILE_DOWNLOAD_MAX_BYTES) {
            Ok(value) => value,
            Err(error) => {
                rollback_deletion_quarantine(&quarantine_path, &target_path, parent)?;
                return Err(error);
            }
        };

    let quarantine_after_hash = match fs::symlink_metadata(&quarantine_path) {
        Ok(meta) => meta,
        Err(_) => {
            rollback_deletion_quarantine(&quarantine_path, &target_path, parent)?;
            return Err(SelectedRootExecutorError::RemoteDeletionTargetRace);
        }
    };

    if !same_local_file_state(&quarantine_before_hash, &quarantine_after_hash)
        || quarantine_bytes != receipt.size_bytes
        || quarantine_sha256 != receipt.sha256_hex
    {
        rollback_deletion_quarantine(&quarantine_path, &target_path, parent)?;
        return Err(SelectedRootExecutorError::RemoteDeletionLocalConflict);
    }

    sync_parent_directory(parent)?;

    match fs::symlink_metadata(&target_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        _ => {
            rollback_deletion_quarantine(&quarantine_path, &target_path, parent)?;
            return Err(SelectedRootExecutorError::LocalFileDeletionPostconditionFailed);
        }
    }

    Ok(QuarantinedStaleFileDeletion {
        remote_id: receipt.remote_id.clone(),
        target_path,
        quarantine_path,
        metadata_before,
        bytes_verified: bytes,
        sha256_hex,
    })
}

fn rollback_quarantined_stale_file(
    item: &QuarantinedStaleFileDeletion,
) -> Result<(), SelectedRootExecutorError> {
    let parent = item
        .target_path
        .parent()
        .ok_or(SelectedRootExecutorError::LocalFileDeletionRollbackFailed)?;

    let quarantine_meta = fs::symlink_metadata(&item.quarantine_path)
        .map_err(|_| SelectedRootExecutorError::LocalFileDeletionRollbackFailed)?;
    if !same_local_file_identity(&item.metadata_before, &quarantine_meta) {
        return Err(SelectedRootExecutorError::LocalFileDeletionRollbackFailed);
    }

    let (bytes, sha256_hex) =
        hash_local_file(&item.quarantine_path, SUPERVISED_FILE_DOWNLOAD_MAX_BYTES)
            .map_err(|_| SelectedRootExecutorError::LocalFileDeletionRollbackFailed)?;
    if bytes != item.bytes_verified || sha256_hex != item.sha256_hex {
        return Err(SelectedRootExecutorError::LocalFileDeletionRollbackFailed);
    }

    rollback_deletion_quarantine(&item.quarantine_path, &item.target_path, parent)
}

fn rollback_stale_file_deletion_batch(
    quarantined: &[QuarantinedStaleFileDeletion],
) -> Result<(), SelectedRootExecutorError> {
    for item in quarantined.iter().rev() {
        rollback_quarantined_stale_file(item)?;
    }
    Ok(())
}

fn cleanup_stale_file_deletion_quarantines(
    quarantined: &[QuarantinedStaleFileDeletion],
) -> Result<usize, SelectedRootExecutorError> {
    let mut removed = 0_usize;

    for item in quarantined {
        let parent = item
            .quarantine_path
            .parent()
            .ok_or(SelectedRootExecutorError::LocalFileDeletionBatchCleanupFailed)?;

        let quarantine_meta = fs::symlink_metadata(&item.quarantine_path)
            .map_err(|_| SelectedRootExecutorError::LocalFileDeletionBatchCleanupFailed)?;
        if !same_local_file_identity(&item.metadata_before, &quarantine_meta) {
            return Err(SelectedRootExecutorError::LocalFileDeletionBatchCleanupFailed);
        }

        let (bytes, sha256_hex) =
            hash_local_file(&item.quarantine_path, SUPERVISED_FILE_DOWNLOAD_MAX_BYTES)
                .map_err(|_| SelectedRootExecutorError::LocalFileDeletionBatchCleanupFailed)?;
        if bytes != item.bytes_verified || sha256_hex != item.sha256_hex {
            return Err(SelectedRootExecutorError::LocalFileDeletionBatchCleanupFailed);
        }

        fs::remove_file(&item.quarantine_path)
            .map_err(|_| SelectedRootExecutorError::LocalFileDeletionBatchCleanupFailed)?;
        sync_parent_directory(parent)
            .map_err(|_| SelectedRootExecutorError::LocalFileDeletionBatchCleanupFailed)?;

        removed = removed
            .checked_add(1)
            .ok_or(SelectedRootExecutorError::CountOverflow)?;
    }

    Ok(removed)
}

pub fn delete_selected_root_existing_file(
    storage: &mut Storage,
    sync_root: &SyncRoot,
) -> Result<SelectedRootFileDeletion, SelectedRootExecutorError> {
    let readiness = plan_selected_root_remote_deletion(storage, sync_root)?;
    if !readiness.ready() {
        return Err(SelectedRootExecutorError::RemoteDeletionNotReady);
    }

    let stale_receipts =
        storage.list_sync_root_stale_file_materialization_receipts(&sync_root.id)?;
    if stale_receipts.len() != 1 {
        return Err(SelectedRootExecutorError::RemoteDeletionPlanStaleReceiptCountMismatch);
    }
    let receipt = stale_receipts
        .first()
        .ok_or(SelectedRootExecutorError::RemoteDeletionPlanStaleReceiptCountMismatch)?;

    let root_path = validated_selected_root_path(sync_root)?;
    storage.invalidate_sync_root_local_observation_baseline(&sync_root.id)?;
    let bytes_verified = delete_verified_stale_local_file(&root_path, receipt)?;

    let receipt_deleted = storage
        .delete_sync_root_stale_file_materialization_receipt(&sync_root.id, &receipt.remote_id)?;
    if !receipt_deleted {
        return Err(SelectedRootExecutorError::LocalFileDeletionReceiptCleanupFailed);
    }

    if storage.sync_root_materialization_receipt_count(&sync_root.id)? != 0
        || storage.sync_root_stale_materialization_receipt_count(&sync_root.id)? != 0
    {
        return Err(SelectedRootExecutorError::LocalFileDeletionReceiptCleanupFailed);
    }

    let (remote_items, local_entries) = selected_root_materialization_inputs(storage, sync_root)?;
    let post = plan_receive_only_materialization(&remote_items, &local_entries)?;
    if post.missing_directories != 0
        || post.missing_files != 0
        || post.local_only_entries != 0
        || post.type_conflicts != 0
        || post.remote_files != 0
        || post.existing_files_unverified != 0
        || post.matching_directories != post.remote_directories
    {
        return Err(SelectedRootExecutorError::LocalFileDeletionPostconditionFailed);
    }

    Ok(SelectedRootFileDeletion {
        files_deleted: 1,
        bytes_verified,
        stale_baseline_match: true,
        receipt_deleted: true,
        quarantine_rename: true,
    })
}

fn delete_verified_stale_local_file(
    root_path: &Path,
    receipt: &SyncRootFileMaterializationReceipt,
) -> Result<u64, SelectedRootExecutorError> {
    let (target_path, observed_size) = match inspect_receipt_target(root_path, receipt)? {
        ReceiptTargetInspection::File(path, size) => (path, size),
        ReceiptTargetInspection::Missing | ReceiptTargetInspection::Conflict => {
            return Err(SelectedRootExecutorError::RemoteDeletionNotReady);
        }
    };

    if observed_size != receipt.size_bytes {
        return Err(SelectedRootExecutorError::RemoteDeletionLocalConflict);
    }
    if receipt.size_bytes > SUPERVISED_FILE_DOWNLOAD_MAX_BYTES {
        return Err(SelectedRootExecutorError::LocalFileTooLarge);
    }

    let metadata_before = fs::symlink_metadata(&target_path)
        .map_err(|_| SelectedRootExecutorError::RemoteDeletionTargetRace)?;
    if metadata_before.file_type().is_symlink() || !metadata_before.is_file() {
        return Err(SelectedRootExecutorError::RemoteDeletionTargetRace);
    }

    let (bytes, sha256_hex) = hash_local_file(&target_path, SUPERVISED_FILE_DOWNLOAD_MAX_BYTES)?;
    if bytes != receipt.size_bytes || sha256_hex != receipt.sha256_hex {
        return Err(SelectedRootExecutorError::RemoteDeletionLocalConflict);
    }

    let metadata_after = fs::symlink_metadata(&target_path)
        .map_err(|_| SelectedRootExecutorError::RemoteDeletionTargetRace)?;
    if !same_local_file_state(&metadata_before, &metadata_after) {
        return Err(SelectedRootExecutorError::RemoteDeletionTargetRace);
    }

    let parent = target_path
        .parent()
        .ok_or(SelectedRootExecutorError::LocalFileParentInvalid)?;
    let quarantine_path = unique_deletion_quarantine_path(parent)?;

    fs::rename(&target_path, &quarantine_path)
        .map_err(|_| SelectedRootExecutorError::LocalFileDeletionRenameFailed)?;

    let quarantine_meta = match fs::symlink_metadata(&quarantine_path) {
        Ok(meta) => meta,
        Err(_) => {
            rollback_deletion_quarantine(&quarantine_path, &target_path, parent)?;
            return Err(SelectedRootExecutorError::RemoteDeletionTargetRace);
        }
    };

    if !same_local_file_identity(&metadata_after, &quarantine_meta) {
        rollback_deletion_quarantine(&quarantine_path, &target_path, parent)?;
        return Err(SelectedRootExecutorError::RemoteDeletionTargetRace);
    }

    let quarantine_before_hash = quarantine_meta;
    let (quarantine_bytes, quarantine_sha256) =
        match hash_local_file(&quarantine_path, SUPERVISED_FILE_DOWNLOAD_MAX_BYTES) {
            Ok(value) => value,
            Err(error) => {
                rollback_deletion_quarantine(&quarantine_path, &target_path, parent)?;
                return Err(error);
            }
        };

    let quarantine_after_hash = match fs::symlink_metadata(&quarantine_path) {
        Ok(meta) => meta,
        Err(_) => {
            rollback_deletion_quarantine(&quarantine_path, &target_path, parent)?;
            return Err(SelectedRootExecutorError::RemoteDeletionTargetRace);
        }
    };

    if !same_local_file_state(&quarantine_before_hash, &quarantine_after_hash)
        || quarantine_bytes != receipt.size_bytes
        || quarantine_sha256 != receipt.sha256_hex
    {
        rollback_deletion_quarantine(&quarantine_path, &target_path, parent)?;
        return Err(SelectedRootExecutorError::RemoteDeletionLocalConflict);
    }

    fs::remove_file(&quarantine_path)
        .map_err(|_| SelectedRootExecutorError::LocalFileDeletionRemoveFailed)?;

    sync_parent_directory(parent)?;

    match fs::symlink_metadata(&target_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        _ => return Err(SelectedRootExecutorError::LocalFileDeletionPostconditionFailed),
    }
    match fs::symlink_metadata(&quarantine_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        _ => return Err(SelectedRootExecutorError::LocalFileDeletionPostconditionFailed),
    }

    Ok(bytes)
}

fn unique_deletion_quarantine_path(parent: &Path) -> Result<PathBuf, SelectedRootExecutorError> {
    for _ in 0..128 {
        let counter = DOWNLOAD_TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let candidate = parent.join(format!(
            ".nubisync-delete-{}-{counter}.tmp",
            std::process::id()
        ));

        match fs::symlink_metadata(&candidate) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(candidate);
            }
            Ok(_) => continue,
            Err(_) => {
                return Err(SelectedRootExecutorError::LocalFilesystemInspectionFailed);
            }
        }
    }

    Err(SelectedRootExecutorError::LocalFileDeletionQuarantineUnavailable)
}

fn rollback_deletion_quarantine(
    quarantine_path: &Path,
    target_path: &Path,
    parent: &Path,
) -> Result<(), SelectedRootExecutorError> {
    if fs::symlink_metadata(target_path).is_ok() {
        return Err(SelectedRootExecutorError::LocalFileDeletionRollbackFailed);
    }

    fs::rename(quarantine_path, target_path)
        .map_err(|_| SelectedRootExecutorError::LocalFileDeletionRollbackFailed)?;
    sync_parent_directory(parent)
        .map_err(|_| SelectedRootExecutorError::LocalFileDeletionRollbackFailed)
}

fn sync_parent_directory(parent: &Path) -> Result<(), SelectedRootExecutorError> {
    let parent_handle =
        fs::File::open(parent).map_err(|_| SelectedRootExecutorError::LocalFileSyncFailed)?;
    parent_handle
        .sync_all()
        .map_err(|_| SelectedRootExecutorError::LocalFileSyncFailed)
}

fn same_local_file_identity(before: &fs::Metadata, after: &fs::Metadata) -> bool {
    before.file_type().is_file()
        && after.file_type().is_file()
        && !before.file_type().is_symlink()
        && !after.file_type().is_symlink()
        && before.dev() == after.dev()
        && before.ino() == after.ino()
        && before.len() == after.len()
}

struct CompletedStaleFileReplacement {
    remote_id: String,
    relative_path: String,
    target_path: PathBuf,
    backup_path: PathBuf,
    original_metadata: fs::Metadata,
    promoted_metadata: fs::Metadata,
    bytes_downloaded: u64,
    sha256_hex: String,
}

pub fn replace_selected_root_stale_files<P: SelectedRootContentProvider>(
    provider: &P,
    storage: &mut Storage,
    sync_root: &SyncRoot,
) -> Result<SelectedRootStaleFileBatchReplacement, SelectedRootExecutorError> {
    let readiness = plan_selected_root_stale_files(storage, sync_root)?;

    if readiness.stale_receipts_total == 0
        || readiness.stale_receipts_total > SUPERVISED_STALE_FILE_PLAN_MAX_ACTIONS
        || readiness.deletion_candidates != 0
        || readiness.safe_to_delete != 0
        || readiness.replacement_candidates != readiness.stale_receipts_total
        || readiness.safe_to_replace != readiness.replacement_candidates
        || readiness.local_conflicts != 0
        || readiness.files_missing != 0
        || readiness.type_conflicts != 0
        || readiness.convergence_deletion_actions != 0
        || readiness.convergence_replacement_actions != readiness.replacement_candidates
    {
        return Err(SelectedRootExecutorError::RemoteReplacementBatchNotReady);
    }

    let current_receipts_before = storage.sync_root_materialization_receipt_count(&sync_root.id)?;
    let stale_receipts =
        storage.list_sync_root_stale_file_materialization_receipts(&sync_root.id)?;
    if stale_receipts.len() != readiness.stale_receipts_total {
        return Err(SelectedRootExecutorError::RemoteReplacementPlanTargetMismatch);
    }

    let (remote_items, local_entries) = selected_root_materialization_inputs(storage, sync_root)?;
    let targets = plan_receive_only_existing_file_targets(&remote_items, &local_entries)?;
    let root_path = validated_selected_root_path(sync_root)?;
    storage.invalidate_sync_root_local_observation_baseline(&sync_root.id)?;

    let mut completed = Vec::with_capacity(stale_receipts.len());
    let mut total_bytes = 0_u64;

    for stale in &stale_receipts {
        let target = targets.iter().find(|target| {
            target.remote_id() == stale.remote_id.as_str()
                && target.relative_path() == stale.relative_path.as_str()
        });

        let Some(target) = target else {
            rollback_stale_file_replacement_batch(&completed)?;
            return Err(SelectedRootExecutorError::RemoteReplacementPlanTargetMismatch);
        };

        let replacement =
            match apply_selected_root_stale_file_replacement(provider, &root_path, target, stale) {
                Ok(value) => value,
                Err(error) => {
                    rollback_stale_file_replacement_batch(&completed)?;
                    return Err(error);
                }
            };

        total_bytes = match total_bytes.checked_add(replacement.bytes_downloaded) {
            Some(value) => value,
            None => {
                rollback_completed_stale_file_replacement(&replacement)?;
                rollback_stale_file_replacement_batch(&completed)?;
                return Err(SelectedRootExecutorError::CountOverflow);
            }
        };

        completed.push(replacement);
    }

    let receipt_rows = completed
        .iter()
        .map(|replacement| {
            (
                replacement.remote_id.clone(),
                replacement.relative_path.clone(),
                replacement.bytes_downloaded,
                replacement.sha256_hex.clone(),
            )
        })
        .collect::<Vec<_>>();

    let recorded = match storage.record_sync_root_file_materializations(
        &sync_root.id,
        &receipt_rows,
        current_unix_time_ms()?,
    ) {
        Ok(value) => value,
        Err(error) => {
            rollback_stale_file_replacement_batch(&completed)?;
            return Err(error.into());
        }
    };

    if recorded != completed.len() {
        return Err(SelectedRootExecutorError::LocalFileReplacementBatchPostconditionFailed);
    }

    let expected_current = current_receipts_before
        .checked_add(
            u64::try_from(completed.len()).map_err(|_| SelectedRootExecutorError::CountOverflow)?,
        )
        .ok_or(SelectedRootExecutorError::CountOverflow)?;
    let current_receipts_after = storage.sync_root_materialization_receipt_count(&sync_root.id)?;
    let stale_receipts_after =
        storage.sync_root_stale_materialization_receipt_count(&sync_root.id)?;

    if current_receipts_after != expected_current || stale_receipts_after != 0 {
        return Err(SelectedRootExecutorError::LocalFileReplacementBatchPostconditionFailed);
    }

    let backups_cleaned = cleanup_stale_file_replacement_backups(&completed)?;

    let convergence = plan_selected_root_receive_only_convergence(storage, sync_root)?;
    if convergence.blocked()
        || convergence.create_directories != 0
        || convergence.materialize_missing_files != 0
        || convergence.verify_existing_files != 0
        || convergence.revalidate_stale_file_replacements != 0
        || convergence.revalidate_stale_file_deletions != 0
        || convergence.delete_owned_empty_directories != 0
    {
        return Err(SelectedRootExecutorError::LocalFileReplacementBatchPostconditionFailed);
    }

    Ok(SelectedRootStaleFileBatchReplacement {
        planned_replacement_actions: readiness.replacement_candidates,
        batch_action_limit: SUPERVISED_STALE_FILE_PLAN_MAX_ACTIONS,
        files_replaced: completed.len(),
        bytes_downloaded: total_bytes,
        max_file_bytes: SUPERVISED_FILE_DOWNLOAD_MAX_BYTES,
        provider_fingerprints_verified: completed.len(),
        stale_baselines_verified: completed.len(),
        receipts_recorded: recorded,
        atomic_replacements: completed.len(),
        replacement_backups_cleaned: backups_cleaned,
    })
}

fn apply_selected_root_stale_file_replacement<P: SelectedRootContentProvider>(
    provider: &P,
    root_path: &Path,
    target: &ReceiveOnlyFileTarget,
    stale: &SyncRootFileMaterializationReceipt,
) -> Result<CompletedStaleFileReplacement, SelectedRootExecutorError> {
    if target.remote_id() != stale.remote_id.as_str()
        || target.relative_path() != stale.relative_path.as_str()
    {
        return Err(SelectedRootExecutorError::RemoteReplacementPlanTargetMismatch);
    }

    let expected_remote_size = target
        .size_bytes()
        .ok_or(SelectedRootExecutorError::LocalFileSizeUnknown)?;
    if expected_remote_size > SUPERVISED_FILE_DOWNLOAD_MAX_BYTES {
        return Err(SelectedRootExecutorError::LocalFileTooLarge);
    }

    let target_path = root_path.join(target.relative_path());
    if !target_path.starts_with(root_path) {
        return Err(SelectedRootExecutorError::LocalFileTargetEscapedRoot);
    }

    let parent = target_path
        .parent()
        .ok_or(SelectedRootExecutorError::LocalFileParentInvalid)?;
    let parent_meta = fs::symlink_metadata(parent)
        .map_err(|_| SelectedRootExecutorError::LocalFileParentInvalid)?;
    if parent_meta.file_type().is_symlink() || !parent_meta.is_dir() {
        return Err(SelectedRootExecutorError::LocalFileParentInvalid);
    }
    let canonical_parent =
        fs::canonicalize(parent).map_err(|_| SelectedRootExecutorError::LocalFileParentInvalid)?;
    if !canonical_parent.starts_with(root_path) {
        return Err(SelectedRootExecutorError::LocalFileParentInvalid);
    }

    let fingerprint_before = provider.content_fingerprint(target.remote_id())?;
    if fingerprint_before.size_bytes != expected_remote_size
        || fingerprint_before.size_bytes > SUPERVISED_FILE_DOWNLOAD_MAX_BYTES
    {
        return Err(SelectedRootExecutorError::RemoteReplacementProviderFingerprintMismatch);
    }

    let (temp_path, mut temp_file) = create_download_temp(parent)?;
    let download = (|| {
        let mut hashing_writer = HashingWriter::new(&mut temp_file);
        let provider_bytes = provider.download_file_content(
            target.remote_id(),
            SUPERVISED_FILE_DOWNLOAD_MAX_BYTES,
            &mut hashing_writer,
        )?;
        let hashed_bytes = hashing_writer.bytes_written();
        let sha256_hex = hashing_writer.finish_hex();

        if provider_bytes != hashed_bytes {
            return Err(SelectedRootExecutorError::LocalFileProviderByteCountMismatch);
        }
        if provider_bytes != expected_remote_size {
            return Err(SelectedRootExecutorError::LocalFileDownloadSizeMismatch);
        }

        Ok((provider_bytes, sha256_hex))
    })();

    let (downloaded_bytes, downloaded_sha256) = match download {
        Ok(value) => value,
        Err(error) => {
            drop(temp_file);
            cleanup_temp_file(&temp_path)?;
            return Err(error);
        }
    };

    if temp_file.sync_all().is_err() {
        drop(temp_file);
        cleanup_temp_file(&temp_path)?;
        return Err(SelectedRootExecutorError::LocalFileSyncFailed);
    }
    drop(temp_file);

    let fingerprint_after = match provider.content_fingerprint(target.remote_id()) {
        Ok(value) => value,
        Err(error) => {
            cleanup_temp_file(&temp_path)?;
            return Err(error);
        }
    };

    if fingerprint_before != fingerprint_after
        || downloaded_bytes != fingerprint_after.size_bytes
        || downloaded_sha256 != fingerprint_after.sha256_hex
    {
        cleanup_temp_file(&temp_path)?;
        return Err(SelectedRootExecutorError::RemoteReplacementProviderFingerprintMismatch);
    }

    let (verified_target_path, observed_size) = match inspect_receipt_target(root_path, stale) {
        Ok(ReceiptTargetInspection::File(path, size)) => (path, size),
        Ok(ReceiptTargetInspection::Missing | ReceiptTargetInspection::Conflict) => {
            cleanup_temp_file(&temp_path)?;
            return Err(SelectedRootExecutorError::RemoteReplacementNotReady);
        }
        Err(error) => {
            cleanup_temp_file(&temp_path)?;
            return Err(error);
        }
    };

    if verified_target_path != target_path || observed_size != stale.size_bytes {
        cleanup_temp_file(&temp_path)?;
        return Err(SelectedRootExecutorError::RemoteReplacementLocalConflict);
    }

    let metadata_before = match fs::symlink_metadata(&target_path) {
        Ok(value) => value,
        Err(_) => {
            cleanup_temp_file(&temp_path)?;
            return Err(SelectedRootExecutorError::RemoteReplacementTargetRace);
        }
    };
    if metadata_before.file_type().is_symlink() || !metadata_before.is_file() {
        cleanup_temp_file(&temp_path)?;
        return Err(SelectedRootExecutorError::RemoteReplacementTargetRace);
    }

    let (local_bytes, local_sha256) =
        match hash_local_file(&target_path, SUPERVISED_FILE_DOWNLOAD_MAX_BYTES) {
            Ok(value) => value,
            Err(error) => {
                cleanup_temp_file(&temp_path)?;
                return Err(error);
            }
        };
    if local_bytes != stale.size_bytes || local_sha256 != stale.sha256_hex {
        cleanup_temp_file(&temp_path)?;
        return Err(SelectedRootExecutorError::RemoteReplacementLocalConflict);
    }

    let metadata_after = match fs::symlink_metadata(&target_path) {
        Ok(value) => value,
        Err(_) => {
            cleanup_temp_file(&temp_path)?;
            return Err(SelectedRootExecutorError::RemoteReplacementTargetRace);
        }
    };
    if !same_local_file_state(&metadata_before, &metadata_after) {
        cleanup_temp_file(&temp_path)?;
        return Err(SelectedRootExecutorError::RemoteReplacementTargetRace);
    }

    let backup_path = unique_replacement_backup_path(parent)?;
    if fs::hard_link(&target_path, &backup_path).is_err() {
        cleanup_temp_file(&temp_path)?;
        return Err(SelectedRootExecutorError::LocalFileReplacementBackupCreateFailed);
    }

    let backup_meta = match fs::symlink_metadata(&backup_path) {
        Ok(value) => value,
        Err(_) => {
            let _ = cleanup_replacement_backup_path(&backup_path, parent);
            cleanup_temp_file(&temp_path)?;
            return Err(SelectedRootExecutorError::LocalFileReplacementBackupCreateFailed);
        }
    };
    if !same_local_file_identity(&metadata_after, &backup_meta) {
        cleanup_replacement_backup_path(&backup_path, parent)?;
        cleanup_temp_file(&temp_path)?;
        return Err(SelectedRootExecutorError::RemoteReplacementTargetRace);
    }

    if let Err(error) = sync_parent_directory(parent) {
        cleanup_replacement_backup_path(&backup_path, parent)?;
        cleanup_temp_file(&temp_path)?;
        return Err(error);
    }

    if fs::rename(&temp_path, &target_path).is_err() {
        cleanup_replacement_backup_path(&backup_path, parent)?;
        cleanup_temp_file(&temp_path)?;
        return Err(SelectedRootExecutorError::LocalFileReplaceFailed);
    }

    let promoted_metadata = match fs::symlink_metadata(&target_path) {
        Ok(value) => value,
        Err(_) => {
            if fs::rename(&backup_path, &target_path).is_err() {
                return Err(SelectedRootExecutorError::LocalFileReplacementRollbackFailed);
            }
            sync_parent_directory(parent)
                .map_err(|_| SelectedRootExecutorError::LocalFileReplacementRollbackFailed)?;
            return Err(SelectedRootExecutorError::LocalFileReplacementPostconditionFailed);
        }
    };

    let completed = CompletedStaleFileReplacement {
        remote_id: target.remote_id().to_owned(),
        relative_path: target.relative_path().to_owned(),
        target_path: target_path.clone(),
        backup_path,
        original_metadata: metadata_after,
        promoted_metadata,
        bytes_downloaded: downloaded_bytes,
        sha256_hex: downloaded_sha256,
    };

    if let Err(error) = sync_parent_directory(parent) {
        rollback_completed_stale_file_replacement(&completed)?;
        return Err(error);
    }

    if completed.promoted_metadata.file_type().is_symlink()
        || !completed.promoted_metadata.is_file()
        || completed.promoted_metadata.len() != completed.bytes_downloaded
    {
        rollback_completed_stale_file_replacement(&completed)?;
        return Err(SelectedRootExecutorError::LocalFileReplacementPostconditionFailed);
    }

    let (promoted_bytes, promoted_sha256) =
        match hash_local_file(&completed.target_path, SUPERVISED_FILE_DOWNLOAD_MAX_BYTES) {
            Ok(value) => value,
            Err(error) => {
                rollback_completed_stale_file_replacement(&completed)?;
                return Err(error);
            }
        };

    if promoted_bytes != completed.bytes_downloaded || promoted_sha256 != completed.sha256_hex {
        rollback_completed_stale_file_replacement(&completed)?;
        return Err(SelectedRootExecutorError::LocalFileReplacementPostconditionFailed);
    }

    Ok(completed)
}

fn unique_replacement_backup_path(parent: &Path) -> Result<PathBuf, SelectedRootExecutorError> {
    for _ in 0..128 {
        let counter = DOWNLOAD_TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let candidate = parent.join(format!(
            ".nubisync-replace-backup-{}-{counter}.tmp",
            std::process::id()
        ));

        match fs::symlink_metadata(&candidate) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(candidate);
            }
            Ok(_) => continue,
            Err(_) => {
                return Err(SelectedRootExecutorError::LocalFilesystemInspectionFailed);
            }
        }
    }

    Err(SelectedRootExecutorError::LocalFileReplacementBackupUnavailable)
}

fn cleanup_replacement_backup_path(
    backup_path: &Path,
    parent: &Path,
) -> Result<(), SelectedRootExecutorError> {
    match fs::remove_file(backup_path) {
        Ok(()) => sync_parent_directory(parent)
            .map_err(|_| SelectedRootExecutorError::LocalFileReplacementBackupCleanupFailed),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(SelectedRootExecutorError::LocalFileReplacementBackupCleanupFailed),
    }
}

fn rollback_completed_stale_file_replacement(
    replacement: &CompletedStaleFileReplacement,
) -> Result<(), SelectedRootExecutorError> {
    let parent = replacement
        .target_path
        .parent()
        .ok_or(SelectedRootExecutorError::LocalFileReplacementRollbackFailed)?;

    let current_meta = fs::symlink_metadata(&replacement.target_path)
        .map_err(|_| SelectedRootExecutorError::LocalFileReplacementRollbackFailed)?;
    if !same_local_file_identity(&replacement.promoted_metadata, &current_meta) {
        return Err(SelectedRootExecutorError::LocalFileReplacementRollbackFailed);
    }

    let (current_bytes, current_sha256) =
        hash_local_file(&replacement.target_path, SUPERVISED_FILE_DOWNLOAD_MAX_BYTES)
            .map_err(|_| SelectedRootExecutorError::LocalFileReplacementRollbackFailed)?;
    if current_bytes != replacement.bytes_downloaded || current_sha256 != replacement.sha256_hex {
        return Err(SelectedRootExecutorError::LocalFileReplacementRollbackFailed);
    }

    let backup_meta = fs::symlink_metadata(&replacement.backup_path)
        .map_err(|_| SelectedRootExecutorError::LocalFileReplacementRollbackFailed)?;
    if !same_local_file_identity(&replacement.original_metadata, &backup_meta) {
        return Err(SelectedRootExecutorError::LocalFileReplacementRollbackFailed);
    }

    fs::rename(&replacement.backup_path, &replacement.target_path)
        .map_err(|_| SelectedRootExecutorError::LocalFileReplacementRollbackFailed)?;
    sync_parent_directory(parent)
        .map_err(|_| SelectedRootExecutorError::LocalFileReplacementRollbackFailed)?;

    let restored = fs::symlink_metadata(&replacement.target_path)
        .map_err(|_| SelectedRootExecutorError::LocalFileReplacementRollbackFailed)?;
    if !same_local_file_identity(&replacement.original_metadata, &restored) {
        return Err(SelectedRootExecutorError::LocalFileReplacementRollbackFailed);
    }

    Ok(())
}

fn rollback_stale_file_replacement_batch(
    completed: &[CompletedStaleFileReplacement],
) -> Result<(), SelectedRootExecutorError> {
    for replacement in completed.iter().rev() {
        rollback_completed_stale_file_replacement(replacement)?;
    }
    Ok(())
}

fn cleanup_stale_file_replacement_backups(
    completed: &[CompletedStaleFileReplacement],
) -> Result<usize, SelectedRootExecutorError> {
    let mut cleaned = 0_usize;

    for replacement in completed {
        let backup_meta = fs::symlink_metadata(&replacement.backup_path)
            .map_err(|_| SelectedRootExecutorError::LocalFileReplacementBackupCleanupFailed)?;
        if !same_local_file_identity(&replacement.original_metadata, &backup_meta) {
            return Err(SelectedRootExecutorError::LocalFileReplacementBackupCleanupFailed);
        }

        let parent = replacement
            .backup_path
            .parent()
            .ok_or(SelectedRootExecutorError::LocalFileReplacementBackupCleanupFailed)?;
        cleanup_replacement_backup_path(&replacement.backup_path, parent)?;
        cleaned = cleaned
            .checked_add(1)
            .ok_or(SelectedRootExecutorError::CountOverflow)?;
    }

    Ok(cleaned)
}

pub fn replace_selected_root_existing_file<P: SelectedRootContentProvider>(
    provider: &P,
    storage: &mut Storage,
    sync_root: &SyncRoot,
) -> Result<SelectedRootFileReplacement, SelectedRootExecutorError> {
    let readiness = plan_selected_root_remote_replacement(storage, sync_root)?;
    if !readiness.ready() {
        return Err(SelectedRootExecutorError::RemoteReplacementNotReady);
    }

    let (remote_items, local_entries) = selected_root_materialization_inputs(storage, sync_root)?;
    let targets = plan_receive_only_existing_file_targets(&remote_items, &local_entries)?;
    let stale_receipts =
        storage.list_sync_root_stale_file_materialization_receipts(&sync_root.id)?;

    if targets.len() != 1 || stale_receipts.len() != 1 {
        return Err(SelectedRootExecutorError::RemoteReplacementPlanTargetMismatch);
    }

    let target = targets
        .first()
        .ok_or(SelectedRootExecutorError::RemoteReplacementPlanTargetMismatch)?;
    let stale = stale_receipts
        .first()
        .ok_or(SelectedRootExecutorError::RemoteReplacementPlanStaleReceiptCountMismatch)?;

    if target.remote_id() != stale.remote_id.as_str()
        || target.relative_path() != stale.relative_path.as_str()
    {
        return Err(SelectedRootExecutorError::RemoteReplacementPlanTargetMismatch);
    }

    let expected_remote_size = target
        .size_bytes()
        .ok_or(SelectedRootExecutorError::LocalFileSizeUnknown)?;
    if expected_remote_size > SUPERVISED_FILE_DOWNLOAD_MAX_BYTES {
        return Err(SelectedRootExecutorError::LocalFileTooLarge);
    }

    let root_path = validated_selected_root_path(sync_root)?;
    storage.invalidate_sync_root_local_observation_baseline(&sync_root.id)?;
    let target_path = root_path.join(target.relative_path());
    if !target_path.starts_with(&root_path) {
        return Err(SelectedRootExecutorError::LocalFileTargetEscapedRoot);
    }

    let parent = target_path
        .parent()
        .ok_or(SelectedRootExecutorError::LocalFileParentInvalid)?;
    let parent_meta = fs::symlink_metadata(parent)
        .map_err(|_| SelectedRootExecutorError::LocalFileParentInvalid)?;
    if parent_meta.file_type().is_symlink() || !parent_meta.is_dir() {
        return Err(SelectedRootExecutorError::LocalFileParentInvalid);
    }
    let canonical_parent =
        fs::canonicalize(parent).map_err(|_| SelectedRootExecutorError::LocalFileParentInvalid)?;
    if !canonical_parent.starts_with(&root_path) {
        return Err(SelectedRootExecutorError::LocalFileParentInvalid);
    }

    let fingerprint_before = provider.content_fingerprint(target.remote_id())?;
    if fingerprint_before.size_bytes != expected_remote_size
        || fingerprint_before.size_bytes > SUPERVISED_FILE_DOWNLOAD_MAX_BYTES
    {
        return Err(SelectedRootExecutorError::RemoteReplacementProviderFingerprintMismatch);
    }

    let (temp_path, mut temp_file) = create_download_temp(parent)?;
    let download = (|| {
        let mut hashing_writer = HashingWriter::new(&mut temp_file);
        let provider_bytes = provider.download_file_content(
            target.remote_id(),
            SUPERVISED_FILE_DOWNLOAD_MAX_BYTES,
            &mut hashing_writer,
        )?;
        let hashed_bytes = hashing_writer.bytes_written();
        let sha256_hex = hashing_writer.finish_hex();

        if provider_bytes != hashed_bytes {
            return Err(SelectedRootExecutorError::LocalFileProviderByteCountMismatch);
        }
        if provider_bytes != expected_remote_size {
            return Err(SelectedRootExecutorError::LocalFileDownloadSizeMismatch);
        }

        Ok((provider_bytes, sha256_hex))
    })();

    let (downloaded_bytes, downloaded_sha256) = match download {
        Ok(value) => value,
        Err(error) => {
            drop(temp_file);
            cleanup_temp_file(&temp_path)?;
            return Err(error);
        }
    };

    if temp_file.sync_all().is_err() {
        drop(temp_file);
        cleanup_temp_file(&temp_path)?;
        return Err(SelectedRootExecutorError::LocalFileSyncFailed);
    }
    drop(temp_file);

    let fingerprint_after = match provider.content_fingerprint(target.remote_id()) {
        Ok(value) => value,
        Err(error) => {
            cleanup_temp_file(&temp_path)?;
            return Err(error);
        }
    };

    if fingerprint_before != fingerprint_after
        || downloaded_bytes != fingerprint_after.size_bytes
        || downloaded_sha256 != fingerprint_after.sha256_hex
    {
        cleanup_temp_file(&temp_path)?;
        return Err(SelectedRootExecutorError::RemoteReplacementProviderFingerprintMismatch);
    }

    let (verified_target_path, observed_size) = match inspect_receipt_target(&root_path, stale)? {
        ReceiptTargetInspection::File(path, size) => (path, size),
        ReceiptTargetInspection::Missing | ReceiptTargetInspection::Conflict => {
            cleanup_temp_file(&temp_path)?;
            return Err(SelectedRootExecutorError::RemoteReplacementNotReady);
        }
    };

    if verified_target_path != target_path || observed_size != stale.size_bytes {
        cleanup_temp_file(&temp_path)?;
        return Err(SelectedRootExecutorError::RemoteReplacementLocalConflict);
    }

    let metadata_before = fs::symlink_metadata(&target_path)
        .map_err(|_| SelectedRootExecutorError::RemoteReplacementTargetRace)?;
    if metadata_before.file_type().is_symlink() || !metadata_before.is_file() {
        cleanup_temp_file(&temp_path)?;
        return Err(SelectedRootExecutorError::RemoteReplacementTargetRace);
    }

    let (local_bytes, local_sha256) =
        hash_local_file(&target_path, SUPERVISED_FILE_DOWNLOAD_MAX_BYTES)?;
    if local_bytes != stale.size_bytes || local_sha256 != stale.sha256_hex {
        cleanup_temp_file(&temp_path)?;
        return Err(SelectedRootExecutorError::RemoteReplacementLocalConflict);
    }

    let metadata_after = fs::symlink_metadata(&target_path)
        .map_err(|_| SelectedRootExecutorError::RemoteReplacementTargetRace)?;
    if !same_local_file_state(&metadata_before, &metadata_after) {
        cleanup_temp_file(&temp_path)?;
        return Err(SelectedRootExecutorError::RemoteReplacementTargetRace);
    }

    if fs::rename(&temp_path, &target_path).is_err() {
        cleanup_temp_file(&temp_path)?;
        return Err(SelectedRootExecutorError::LocalFileReplaceFailed);
    }

    let parent_handle =
        fs::File::open(parent).map_err(|_| SelectedRootExecutorError::LocalFileSyncFailed)?;
    parent_handle
        .sync_all()
        .map_err(|_| SelectedRootExecutorError::LocalFileSyncFailed)?;

    let promoted_meta = fs::symlink_metadata(&target_path)
        .map_err(|_| SelectedRootExecutorError::LocalFileReplacementPostconditionFailed)?;
    if promoted_meta.file_type().is_symlink()
        || !promoted_meta.is_file()
        || promoted_meta.len() != downloaded_bytes
    {
        return Err(SelectedRootExecutorError::LocalFileReplacementPostconditionFailed);
    }

    let (promoted_bytes, promoted_sha256) =
        hash_local_file(&target_path, SUPERVISED_FILE_DOWNLOAD_MAX_BYTES)?;
    if promoted_bytes != downloaded_bytes || promoted_sha256 != downloaded_sha256 {
        return Err(SelectedRootExecutorError::LocalFileReplacementPostconditionFailed);
    }

    storage.record_sync_root_file_materialization(
        &sync_root.id,
        target.remote_id(),
        target.relative_path(),
        downloaded_bytes,
        &downloaded_sha256,
        current_unix_time_ms()?,
    )?;

    let current_receipts = storage.sync_root_materialization_receipt_count(&sync_root.id)?;
    let stale_receipts = storage.sync_root_stale_materialization_receipt_count(&sync_root.id)?;
    if current_receipts != 1 || stale_receipts != 0 {
        return Err(SelectedRootExecutorError::LocalFileReceiptPostconditionFailed);
    }

    Ok(SelectedRootFileReplacement {
        files_replaced: 1,
        bytes_downloaded: downloaded_bytes,
        stale_baseline_match: true,
        provider_fingerprint_match: true,
        receipt_recorded: true,
        atomic_replace: true,
    })
}

fn same_local_file_state(before: &fs::Metadata, after: &fs::Metadata) -> bool {
    before.file_type().is_file()
        && after.file_type().is_file()
        && !before.file_type().is_symlink()
        && !after.file_type().is_symlink()
        && before.dev() == after.dev()
        && before.ino() == after.ino()
        && before.len() == after.len()
        && before.mtime() == after.mtime()
        && before.mtime_nsec() == after.mtime_nsec()
        && before.ctime() == after.ctime()
        && before.ctime_nsec() == after.ctime_nsec()
}

pub fn verify_selected_root_local_receipts(
    storage: &Storage,
    sync_root: &SyncRoot,
) -> Result<SelectedRootLocalReceiptVerification, SelectedRootExecutorError> {
    if sync_root.mode != nubisync_core::SyncMode::ReceiveOnly {
        return Err(SelectedRootExecutorError::LocalPlanModeUnsupported);
    }

    let receipt_count = storage.sync_root_materialization_receipt_count(&sync_root.id)?;
    if receipt_count > 100_000 {
        return Err(SelectedRootExecutorError::LocalReceiptVerificationSafetyLimitExceeded);
    }

    let receipts = storage.list_sync_root_file_materialization_receipts(&sync_root.id)?;
    let receipt_len =
        u64::try_from(receipts.len()).map_err(|_| SelectedRootExecutorError::CountOverflow)?;

    if receipt_len != receipt_count {
        return Err(SelectedRootExecutorError::LocalReceiptVerificationCountMismatch);
    }

    let root_path = validated_selected_root_path(sync_root)?;
    let mut result = SelectedRootLocalReceiptVerification {
        receipts_total: receipts.len(),
        files_matching_receipt: 0,
        files_modified_since_receipt: 0,
        files_missing: 0,
        type_conflicts: 0,
        bytes_hashed: 0,
    };

    for receipt in &receipts {
        match inspect_receipt_target(&root_path, receipt)? {
            ReceiptTargetInspection::Missing => {
                result.files_missing = result
                    .files_missing
                    .checked_add(1)
                    .ok_or(SelectedRootExecutorError::CountOverflow)?;
            }
            ReceiptTargetInspection::Conflict => {
                result.type_conflicts = result
                    .type_conflicts
                    .checked_add(1)
                    .ok_or(SelectedRootExecutorError::CountOverflow)?;
            }
            ReceiptTargetInspection::File(target_path, observed_size) => {
                if observed_size != receipt.size_bytes {
                    result.files_modified_since_receipt = result
                        .files_modified_since_receipt
                        .checked_add(1)
                        .ok_or(SelectedRootExecutorError::CountOverflow)?;
                    continue;
                }

                if receipt.size_bytes > SUPERVISED_FILE_DOWNLOAD_MAX_BYTES {
                    return Err(SelectedRootExecutorError::LocalFileTooLarge);
                }

                let (bytes, sha256_hex) =
                    hash_local_file(&target_path, SUPERVISED_FILE_DOWNLOAD_MAX_BYTES)?;

                result.bytes_hashed = result
                    .bytes_hashed
                    .checked_add(bytes)
                    .ok_or(SelectedRootExecutorError::CountOverflow)?;

                if bytes == receipt.size_bytes && sha256_hex == receipt.sha256_hex {
                    result.files_matching_receipt = result
                        .files_matching_receipt
                        .checked_add(1)
                        .ok_or(SelectedRootExecutorError::CountOverflow)?;
                } else {
                    result.files_modified_since_receipt = result
                        .files_modified_since_receipt
                        .checked_add(1)
                        .ok_or(SelectedRootExecutorError::CountOverflow)?;
                }
            }
        }
    }

    Ok(result)
}

enum ReceiptTargetInspection {
    Missing,
    Conflict,
    File(PathBuf, u64),
}

fn inspect_receipt_target(
    root_path: &Path,
    receipt: &SyncRootFileMaterializationReceipt,
) -> Result<ReceiptTargetInspection, SelectedRootExecutorError> {
    let relative = Path::new(&receipt.relative_path);

    if relative.is_absolute() {
        return Err(SelectedRootExecutorError::LocalReceiptPathInvalid);
    }

    let mut current = root_path.to_path_buf();
    let mut components = relative.components().peekable();

    if components.peek().is_none() {
        return Err(SelectedRootExecutorError::LocalReceiptPathInvalid);
    }

    while let Some(component) = components.next() {
        let Component::Normal(name) = component else {
            return Err(SelectedRootExecutorError::LocalReceiptPathInvalid);
        };

        current.push(name);
        if !current.starts_with(root_path) {
            return Err(SelectedRootExecutorError::LocalReceiptPathInvalid);
        }

        let is_last = components.peek().is_none();
        let metadata = match fs::symlink_metadata(&current) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(ReceiptTargetInspection::Missing);
            }
            Err(_) => {
                return Err(SelectedRootExecutorError::LocalFilesystemInspectionFailed);
            }
        };

        if metadata.file_type().is_symlink() {
            return Ok(ReceiptTargetInspection::Conflict);
        }

        if is_last {
            return if metadata.is_file() {
                Ok(ReceiptTargetInspection::File(current, metadata.len()))
            } else {
                Ok(ReceiptTargetInspection::Conflict)
            };
        }

        if !metadata.is_dir() {
            return Ok(ReceiptTargetInspection::Conflict);
        }

        let canonical = fs::canonicalize(&current)
            .map_err(|_| SelectedRootExecutorError::LocalFilesystemInspectionFailed)?;
        if !canonical.starts_with(root_path) {
            return Err(SelectedRootExecutorError::LocalReceiptPathInvalid);
        }
    }

    Err(SelectedRootExecutorError::LocalReceiptPathInvalid)
}

pub fn verify_selected_root_existing_files<P: SelectedRootContentProvider>(
    provider: &P,
    storage: &mut Storage,
    sync_root: &SyncRoot,
) -> Result<SelectedRootFileBatchVerification, SelectedRootExecutorError> {
    let convergence = plan_selected_root_receive_only_convergence(storage, sync_root)?;

    if convergence.blocked()
        || convergence.verify_existing_files == 0
        || convergence.verify_existing_files > SUPERVISED_FILE_BATCH_MAX_ACTIONS
        || convergence.create_directories != 0
        || convergence.materialize_missing_files != 0
        || convergence.revalidate_stale_file_replacements != 0
        || convergence.revalidate_stale_file_deletions != 0
        || convergence.delete_owned_empty_directories != 0
    {
        return Err(SelectedRootExecutorError::LocalFileVerificationBatchNotReady);
    }

    let (remote_items, local_entries) = selected_root_materialization_inputs(storage, sync_root)?;
    let materialization = plan_receive_only_materialization(&remote_items, &local_entries)?;

    let expected_existing_files = convergence
        .current_owned_files
        .checked_add(convergence.verify_existing_files)
        .ok_or(SelectedRootExecutorError::CountOverflow)?;

    if !materialization.ready_for_directory_phase()
        || materialization.missing_directories != 0
        || materialization.missing_files != 0
        || materialization.matching_directories != materialization.remote_directories
        || materialization.local_only_entries != 0
        || materialization.type_conflicts != 0
        || materialization.existing_files_unverified != expected_existing_files
        || materialization.remote_files != expected_existing_files
    {
        return Err(SelectedRootExecutorError::LocalFileVerificationBatchNotReady);
    }

    let current_receipts_before = storage.sync_root_materialization_receipt_count(&sync_root.id)?;
    if usize::try_from(current_receipts_before)
        .map_err(|_| SelectedRootExecutorError::CountOverflow)?
        != convergence.current_owned_files
        || storage.sync_root_stale_materialization_receipt_count(&sync_root.id)? != 0
    {
        return Err(SelectedRootExecutorError::LocalFileVerificationBatchTargetMismatch);
    }

    let all_targets = plan_receive_only_existing_file_targets(&remote_items, &local_entries)?;
    if all_targets.len() != expected_existing_files {
        return Err(SelectedRootExecutorError::LocalFileVerificationBatchTargetMismatch);
    }

    let mut verification_targets = Vec::with_capacity(convergence.verify_existing_files);
    let mut seen_remote_ids = HashSet::with_capacity(convergence.verify_existing_files);

    for action in convergence
        .actions()
        .iter()
        .filter(|action| action.kind() == ReceiveOnlyConvergenceActionKind::VerifyExistingFile)
    {
        let remote_id = action
            .remote_id()
            .ok_or(SelectedRootExecutorError::LocalFileVerificationBatchTargetMismatch)?;

        if !seen_remote_ids.insert(remote_id.to_owned()) {
            return Err(SelectedRootExecutorError::LocalFileVerificationBatchTargetMismatch);
        }

        let target = all_targets.iter().find(|target| {
            target.remote_id() == remote_id && target.relative_path() == action.relative_path()
        });

        let Some(target) = target else {
            return Err(SelectedRootExecutorError::LocalFileVerificationBatchTargetMismatch);
        };

        verification_targets.push(target.clone());
    }

    if verification_targets.len() != convergence.verify_existing_files
        || verification_targets.len() > SUPERVISED_FILE_BATCH_MAX_ACTIONS
    {
        return Err(SelectedRootExecutorError::LocalFileVerificationBatchTargetMismatch);
    }

    let root_path = validated_selected_root_path(sync_root)?;
    let mut receipt_rows = Vec::with_capacity(verification_targets.len());
    let mut total_bytes = 0_u64;

    for target in &verification_targets {
        let (bytes_verified, sha256_hex) =
            verify_existing_file_target_against_provider(provider, &root_path, target)?;

        total_bytes = total_bytes
            .checked_add(bytes_verified)
            .ok_or(SelectedRootExecutorError::CountOverflow)?;

        receipt_rows.push((
            target.remote_id().to_owned(),
            target.relative_path().to_owned(),
            bytes_verified,
            sha256_hex,
        ));
    }

    let recorded = storage.record_sync_root_file_materializations(
        &sync_root.id,
        &receipt_rows,
        current_unix_time_ms()?,
    )?;

    if recorded != receipt_rows.len() {
        return Err(SelectedRootExecutorError::LocalFileVerificationBatchPostconditionFailed);
    }

    let expected_current_receipts = current_receipts_before
        .checked_add(u64::try_from(recorded).map_err(|_| SelectedRootExecutorError::CountOverflow)?)
        .ok_or(SelectedRootExecutorError::CountOverflow)?;

    if storage.sync_root_materialization_receipt_count(&sync_root.id)? != expected_current_receipts
        || storage.sync_root_stale_materialization_receipt_count(&sync_root.id)? != 0
    {
        return Err(SelectedRootExecutorError::LocalFileVerificationBatchPostconditionFailed);
    }

    let post = plan_selected_root_receive_only_convergence(storage, sync_root)?;
    if post.blocked()
        || post.create_directories != 0
        || post.materialize_missing_files != 0
        || post.verify_existing_files != 0
        || post.revalidate_stale_file_replacements != 0
        || post.revalidate_stale_file_deletions != 0
        || post.delete_owned_empty_directories != 0
    {
        return Err(SelectedRootExecutorError::LocalFileVerificationBatchPostconditionFailed);
    }

    Ok(SelectedRootFileBatchVerification {
        planned_verification_actions: verification_targets.len(),
        batch_action_limit: SUPERVISED_FILE_BATCH_MAX_ACTIONS,
        files_verified: verification_targets.len(),
        bytes_verified: total_bytes,
        max_file_bytes: SUPERVISED_FILE_DOWNLOAD_MAX_BYTES,
        remote_content_hashes_verified: verification_targets.len(),
        receipts_recorded: recorded,
    })
}

fn verify_existing_file_target_against_provider<P: SelectedRootContentProvider>(
    provider: &P,
    root_path: &Path,
    target: &ReceiveOnlyFileTarget,
) -> Result<(u64, String), SelectedRootExecutorError> {
    let expected_size = target
        .size_bytes()
        .ok_or(SelectedRootExecutorError::LocalFileSizeUnknown)?;
    if expected_size > SUPERVISED_FILE_DOWNLOAD_MAX_BYTES {
        return Err(SelectedRootExecutorError::LocalFileTooLarge);
    }

    let target_path = root_path.join(target.relative_path());
    if !target_path.starts_with(root_path) {
        return Err(SelectedRootExecutorError::LocalFileTargetEscapedRoot);
    }

    let parent = target_path
        .parent()
        .ok_or(SelectedRootExecutorError::LocalFileParentInvalid)?;
    let parent_meta = fs::symlink_metadata(parent)
        .map_err(|_| SelectedRootExecutorError::LocalFileParentInvalid)?;
    if parent_meta.file_type().is_symlink() || !parent_meta.is_dir() {
        return Err(SelectedRootExecutorError::LocalFileParentInvalid);
    }

    let canonical_parent =
        fs::canonicalize(parent).map_err(|_| SelectedRootExecutorError::LocalFileParentInvalid)?;
    if !canonical_parent.starts_with(root_path) {
        return Err(SelectedRootExecutorError::LocalFileParentInvalid);
    }

    let metadata_before = fs::symlink_metadata(&target_path)
        .map_err(|_| SelectedRootExecutorError::LocalFileVerifyFailed)?;
    if metadata_before.file_type().is_symlink()
        || !metadata_before.is_file()
        || metadata_before.len() != expected_size
    {
        return Err(SelectedRootExecutorError::LocalFileVerifyFailed);
    }

    let (local_bytes, local_sha256) =
        hash_local_file(&target_path, SUPERVISED_FILE_DOWNLOAD_MAX_BYTES)?;
    if local_bytes != expected_size {
        return Err(SelectedRootExecutorError::LocalFileDownloadSizeMismatch);
    }

    let metadata_after_local = fs::symlink_metadata(&target_path)
        .map_err(|_| SelectedRootExecutorError::LocalFileVerificationTargetRace)?;
    if !same_local_file_state(&metadata_before, &metadata_after_local) {
        return Err(SelectedRootExecutorError::LocalFileVerificationTargetRace);
    }

    let mut remote_sink = HashingWriter::new(std::io::sink());
    let provider_bytes = provider.download_file_content(
        target.remote_id(),
        SUPERVISED_FILE_DOWNLOAD_MAX_BYTES,
        &mut remote_sink,
    )?;
    let hashed_bytes = remote_sink.bytes_written();
    let remote_sha256 = remote_sink.finish_hex();

    if provider_bytes != hashed_bytes {
        return Err(SelectedRootExecutorError::LocalFileProviderByteCountMismatch);
    }
    if provider_bytes != expected_size {
        return Err(SelectedRootExecutorError::LocalFileDownloadSizeMismatch);
    }

    let metadata_after_remote = fs::symlink_metadata(&target_path)
        .map_err(|_| SelectedRootExecutorError::LocalFileVerificationTargetRace)?;
    if !same_local_file_state(&metadata_after_local, &metadata_after_remote) {
        return Err(SelectedRootExecutorError::LocalFileVerificationTargetRace);
    }

    if local_sha256 != remote_sha256 {
        return Err(SelectedRootExecutorError::LocalFileHashMismatch);
    }

    Ok((local_bytes, local_sha256))
}

pub fn verify_selected_root_existing_file<P: SelectedRootContentProvider>(
    provider: &P,
    storage: &mut Storage,
    sync_root: &SyncRoot,
) -> Result<SelectedRootFileVerification, SelectedRootExecutorError> {
    let (remote_items, local_entries) = selected_root_materialization_inputs(storage, sync_root)?;
    let plan = plan_receive_only_materialization(&remote_items, &local_entries)?;

    if !plan.ready_for_directory_phase()
        || plan.missing_directories != 0
        || plan.missing_files != 0
        || plan.matching_directories != plan.remote_directories
        || plan.existing_files_unverified != 1
        || plan.remote_files != 1
    {
        return Err(SelectedRootExecutorError::LocalFileVerificationPhaseBlocked);
    }

    let targets = plan_receive_only_existing_file_targets(&remote_items, &local_entries)?;
    if targets.len() != 1 {
        return Err(SelectedRootExecutorError::LocalFileTargetCountMismatch);
    }
    let target = targets
        .first()
        .ok_or(SelectedRootExecutorError::LocalFileTargetCountMismatch)?;
    let expected_size = target
        .size_bytes()
        .ok_or(SelectedRootExecutorError::LocalFileSizeUnknown)?;
    if expected_size > SUPERVISED_FILE_DOWNLOAD_MAX_BYTES {
        return Err(SelectedRootExecutorError::LocalFileTooLarge);
    }

    let root_path = validated_selected_root_path(sync_root)?;
    let target_path = root_path.join(target.relative_path());
    if !target_path.starts_with(&root_path) {
        return Err(SelectedRootExecutorError::LocalFileTargetEscapedRoot);
    }

    let parent = target_path
        .parent()
        .ok_or(SelectedRootExecutorError::LocalFileParentInvalid)?;
    let canonical_parent =
        fs::canonicalize(parent).map_err(|_| SelectedRootExecutorError::LocalFileParentInvalid)?;
    if !canonical_parent.starts_with(&root_path) {
        return Err(SelectedRootExecutorError::LocalFileParentInvalid);
    }

    let (local_bytes, local_sha256) =
        hash_local_file(&target_path, SUPERVISED_FILE_DOWNLOAD_MAX_BYTES)?;
    if local_bytes != expected_size {
        return Err(SelectedRootExecutorError::LocalFileDownloadSizeMismatch);
    }

    let mut remote_sink = HashingWriter::new(std::io::sink());
    let provider_bytes = provider.download_file_content(
        target.remote_id(),
        SUPERVISED_FILE_DOWNLOAD_MAX_BYTES,
        &mut remote_sink,
    )?;
    let hashed_bytes = remote_sink.bytes_written();
    let remote_sha256 = remote_sink.finish_hex();
    if provider_bytes != hashed_bytes {
        return Err(SelectedRootExecutorError::LocalFileProviderByteCountMismatch);
    }
    if provider_bytes != expected_size {
        return Err(SelectedRootExecutorError::LocalFileDownloadSizeMismatch);
    }
    if local_sha256 != remote_sha256 {
        return Err(SelectedRootExecutorError::LocalFileHashMismatch);
    }

    storage.record_sync_root_file_materialization(
        &sync_root.id,
        target.remote_id(),
        target.relative_path(),
        expected_size,
        &local_sha256,
        current_unix_time_ms()?,
    )?;

    Ok(SelectedRootFileVerification {
        files_verified: 1,
        bytes_verified: expected_size,
        hash_match: true,
        receipt_recorded: true,
    })
}

pub fn plan_selected_root_local_inventory_diff(
    storage: &Storage,
    sync_root: &SyncRoot,
) -> Result<SelectedRootLocalInventoryDiff, SelectedRootExecutorError> {
    if sync_root.mode != SyncMode::ReceiveOnly {
        return Err(SelectedRootExecutorError::LocalDiffModeUnsupported);
    }

    let state = storage.sync_root_local_inventory_state(&sync_root.id)?;
    if !state.snapshot_complete {
        return Err(SelectedRootExecutorError::LocalDiffBaselineMissing);
    }
    if !state.observation_valid {
        return Err(SelectedRootExecutorError::LocalDiffBaselineInvalidated);
    }

    let baseline = storage.list_sync_root_local_items(&sync_root.id)?;
    let baseline_count =
        u64::try_from(baseline.len()).map_err(|_| SelectedRootExecutorError::CountOverflow)?;
    if baseline_count != state.item_count {
        return Err(SelectedRootExecutorError::LocalDiffBaselineCountMismatch);
    }

    let first = scan_selected_root_local_snapshot(sync_root)?;
    let second = scan_selected_root_local_snapshot(sync_root)?;
    if first != second {
        return Err(SelectedRootExecutorError::LocalDiffScanRace);
    }

    build_selected_root_local_inventory_diff(
        &baseline,
        &first,
        state.generation,
        state.snapshot_completed_at_unix_ms,
    )
}

fn build_selected_root_local_inventory_diff(
    baseline: &[LocalItemSnapshot],
    current: &[LocalItemSnapshot],
    baseline_generation: u64,
    baseline_snapshot_completed_at_unix_ms: Option<i64>,
) -> Result<SelectedRootLocalInventoryDiff, SelectedRootExecutorError> {
    let baseline_by_path = baseline
        .iter()
        .map(|item| (item.relative_path(), item))
        .collect::<BTreeMap<_, _>>();
    let current_by_path = current
        .iter()
        .map(|item| (item.relative_path(), item))
        .collect::<BTreeMap<_, _>>();

    if baseline_by_path.len() != baseline.len() || current_by_path.len() != current.len() {
        return Err(SelectedRootExecutorError::LocalDiffDuplicatePath);
    }

    let mut all_paths = baseline_by_path
        .keys()
        .chain(current_by_path.keys())
        .copied()
        .collect::<Vec<_>>();
    all_paths.sort_unstable();
    all_paths.dedup();

    let mut entries = Vec::new();

    for path in all_paths {
        match (baseline_by_path.get(path), current_by_path.get(path)) {
            (None, Some(current_item)) => {
                entries.push(SelectedRootLocalDiffEntry {
                    relative_path: path.to_owned(),
                    kind: SelectedRootLocalDiffKind::Created,
                    baseline_kind: None,
                    current_kind: Some(current_item.kind()),
                });
            }
            (Some(baseline_item), None) => {
                entries.push(SelectedRootLocalDiffEntry {
                    relative_path: path.to_owned(),
                    kind: SelectedRootLocalDiffKind::Deleted,
                    baseline_kind: Some(baseline_item.kind()),
                    current_kind: None,
                });
            }
            (Some(baseline_item), Some(current_item))
                if baseline_item.kind() != current_item.kind() =>
            {
                entries.push(SelectedRootLocalDiffEntry {
                    relative_path: path.to_owned(),
                    kind: SelectedRootLocalDiffKind::TypeChanged,
                    baseline_kind: Some(baseline_item.kind()),
                    current_kind: Some(current_item.kind()),
                });
            }
            (Some(baseline_item), Some(current_item))
                if local_snapshot_metadata_changed(baseline_item, current_item) =>
            {
                entries.push(SelectedRootLocalDiffEntry {
                    relative_path: path.to_owned(),
                    kind: SelectedRootLocalDiffKind::Modified,
                    baseline_kind: Some(baseline_item.kind()),
                    current_kind: Some(current_item.kind()),
                });
            }
            (Some(_), Some(_)) => {}
            (None, None) => unreachable!("path came from the union of baseline and current maps"),
        }
    }

    let created = entries
        .iter()
        .filter(|entry| entry.kind == SelectedRootLocalDiffKind::Created)
        .count();
    let deleted = entries
        .iter()
        .filter(|entry| entry.kind == SelectedRootLocalDiffKind::Deleted)
        .count();
    let modified = entries
        .iter()
        .filter(|entry| entry.kind == SelectedRootLocalDiffKind::Modified)
        .count();
    let type_changed = entries
        .iter()
        .filter(|entry| entry.kind == SelectedRootLocalDiffKind::TypeChanged)
        .count();

    Ok(SelectedRootLocalInventoryDiff {
        baseline_items: baseline.len(),
        baseline_generation,
        baseline_snapshot_completed_at_unix_ms,
        observed_items: current.len(),
        created,
        deleted,
        modified,
        type_changed,
        entries,
    })
}

fn local_snapshot_metadata_changed(
    baseline: &LocalItemSnapshot,
    current: &LocalItemSnapshot,
) -> bool {
    match baseline.kind() {
        LocalItemKind::File => {
            baseline.size_bytes() != current.size_bytes()
                || baseline.modified_unix_ns() != current.modified_unix_ns()
                || baseline.device_id() != current.device_id()
                || baseline.inode() != current.inode()
        }
        LocalItemKind::Directory => {
            baseline.device_id() != current.device_id() || baseline.inode() != current.inode()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedRootLocalJournalResult {
    pub changes_total: usize,
    pub created: usize,
    pub deleted: usize,
    pub modified: usize,
    pub type_changed: usize,
    pub baseline_generation: u64,
    pub pending_events: u64,
    pub superseded_events: u64,
}

pub fn plan_selected_root_confirmed_file_create_settlement(
    storage: &Storage,
    sync_root: &SyncRoot,
    candidate: &RemoteWriteFileCreateCandidate,
    settled_at_unix_ms: i64,
) -> Result<RemoteWriteFileCreateSettlementInput, SelectedRootExecutorError> {
    const SETTLEMENT_HASH_CHUNK_BYTES: usize = 1024 * 1024;

    if sync_root.mode != SyncMode::TwoWay {
        return Err(SelectedRootExecutorError::RemoteWriteFileSettlementModeUnsupported);
    }
    if candidate.status != RemoteWriteIntentStatus::Confirmed || settled_at_unix_ms <= 0 {
        return Err(SelectedRootExecutorError::RemoteWriteFileSettlementNotEligible);
    }

    let evidence = storage
        .sync_root_file_create_content_evidence(candidate.intent_id)?
        .ok_or(SelectedRootExecutorError::RemoteWriteFileSettlementEvidenceMismatch)?;
    let remote_version = evidence
        .remote_version
        .ok_or(SelectedRootExecutorError::RemoteWriteFileSettlementEvidenceMismatch)?;
    if evidence.size_bytes != candidate.local_size_bytes || evidence.size_bytes == 0 {
        return Err(SelectedRootExecutorError::RemoteWriteFileSettlementEvidenceMismatch);
    }

    let state = storage.sync_root_local_inventory_state(&sync_root.id)?;
    if !state.snapshot_complete
        || !state.observation_valid
        || state.generation != candidate.baseline_generation
    {
        return Err(SelectedRootExecutorError::RemoteWriteFileSettlementLocalAuthorityMismatch);
    }

    let baseline = storage.list_sync_root_local_items(&sync_root.id)?;
    let baseline_count =
        u64::try_from(baseline.len()).map_err(|_| SelectedRootExecutorError::CountOverflow)?;
    if baseline_count != state.item_count
        || baseline
            .iter()
            .any(|item| item.relative_path() == candidate.relative_path())
    {
        return Err(SelectedRootExecutorError::RemoteWriteFileSettlementLocalAuthorityMismatch);
    }

    let pending = storage
        .list_pending_sync_root_local_change_events(&sync_root.id, candidate.baseline_generation)?;
    let source_event = pending
        .iter()
        .find(|event| event.id == candidate.source_local_event_id)
        .ok_or(SelectedRootExecutorError::RemoteWriteFileSettlementSourceEventMismatch)?;
    if source_event.relative_path() != candidate.relative_path()
        || source_event.kind != LocalChangeEventKind::Created
        || source_event.baseline_kind.is_some()
        || source_event.current_kind != Some(LocalItemKind::File)
    {
        return Err(SelectedRootExecutorError::RemoteWriteFileSettlementSourceEventMismatch);
    }

    let first = scan_selected_root_local_snapshot(sync_root)?;
    let second = scan_selected_root_local_snapshot(sync_root)?;
    if first != second {
        return Err(SelectedRootExecutorError::LocalDiffScanRace);
    }

    let promoted = LocalItemSnapshot::new(
        candidate.relative_path(),
        LocalItemKind::File,
        Some(candidate.local_size_bytes),
        candidate.local_modified_unix_ns,
        candidate.local_device_id,
        candidate.local_inode,
    )
    .map_err(|_| SelectedRootExecutorError::RemoteWriteFileSettlementSnapshotInvalid)?;

    if let Some(current) = first
        .iter()
        .find(|item| item.relative_path() == candidate.relative_path())
    {
        let metadata_matches_uploaded = current.kind() == LocalItemKind::File
            && current.size_bytes() == Some(candidate.local_size_bytes)
            && current.modified_unix_ns() == candidate.local_modified_unix_ns
            && current.device_id() == candidate.local_device_id
            && current.inode() == candidate.local_inode;

        if metadata_matches_uploaded {
            let mut source = open_selected_root_file_create_local_source(sync_root, candidate)?;
            while source
                .read_next_chunk(SETTLEMENT_HASH_CHUNK_BYTES)?
                .is_some()
            {}
            let current_sha256 = source.completed_sha256_hex()?;
            let finished = source.finish()?;
            if !finished.source_stable || current_sha256 != evidence.sha256_hex() {
                return Err(SelectedRootExecutorError::RemoteWriteFileSettlementContentMismatch);
            }
        }
    }

    let remote = storage
        .sync_root_remote_item(&sync_root.id, candidate.predetermined_remote_id())?
        .ok_or(SelectedRootExecutorError::RemoteWriteFileSettlementRemoteCatalogMismatch)?;
    let expected_name = basename(candidate.relative_path());
    if expected_name.is_empty()
        || remote.name != expected_name
        || remote.kind != RemoteItemKind::File
        || remote.size_bytes != Some(evidence.size_bytes)
        || remote.parent_remote_id.as_deref() != Some(candidate.expected_parent_remote_id())
        || remote.trashed
    {
        return Err(SelectedRootExecutorError::RemoteWriteFileSettlementRemoteCatalogMismatch);
    }

    let mut proposed_baseline = baseline.clone();
    proposed_baseline.push(promoted.clone());
    proposed_baseline.sort_by(|left, right| left.relative_path().cmp(right.relative_path()));

    let next_generation = state
        .generation
        .checked_add(1)
        .ok_or(SelectedRootExecutorError::CountOverflow)?;
    let residual_diff = build_selected_root_local_inventory_diff(
        &proposed_baseline,
        &first,
        next_generation,
        Some(settled_at_unix_ms),
    )?;

    let mut residual_events = Vec::with_capacity(residual_diff.entries().len());
    for entry in residual_diff.entries() {
        residual_events.push(LocalChangeEventInput::new(
            entry.relative_path(),
            local_change_event_kind(entry.kind),
            entry.baseline_kind,
            entry.current_kind,
        )?);
    }

    Ok(RemoteWriteFileCreateSettlementInput::new(
        candidate.intent_id,
        candidate.source_local_event_id,
        candidate.execution_generation,
        state.generation,
        state.item_count,
        state.snapshot_completed_at_unix_ms,
        promoted,
        residual_events,
        candidate.predetermined_remote_id(),
        candidate.expected_parent_remote_id(),
        evidence.sha256_hex(),
        remote_version,
        settled_at_unix_ms,
    )?)
}

pub fn plan_selected_root_confirmed_folder_create_settlement(
    storage: &Storage,
    sync_root: &SyncRoot,
    candidate: &RemoteWriteFolderCreateCandidate,
    settled_at_unix_ms: i64,
) -> Result<RemoteWriteFolderCreateSettlementInput, SelectedRootExecutorError> {
    if sync_root.mode != SyncMode::TwoWay {
        return Err(SelectedRootExecutorError::RemoteWriteFolderSettlementModeUnsupported);
    }
    if candidate.status != RemoteWriteIntentStatus::Confirmed || settled_at_unix_ms <= 0 {
        return Err(SelectedRootExecutorError::RemoteWriteFolderSettlementNotEligible);
    }

    let state = storage.sync_root_local_inventory_state(&sync_root.id)?;
    if !state.snapshot_complete
        || !state.observation_valid
        || state.generation != candidate.baseline_generation
    {
        return Err(SelectedRootExecutorError::RemoteWriteFolderSettlementLocalAuthorityMismatch);
    }
    let baseline = storage.list_sync_root_local_items(&sync_root.id)?;
    let baseline_count =
        u64::try_from(baseline.len()).map_err(|_| SelectedRootExecutorError::CountOverflow)?;
    if baseline_count != state.item_count
        || baseline
            .iter()
            .any(|item| item.relative_path() == candidate.relative_path())
    {
        return Err(SelectedRootExecutorError::RemoteWriteFolderSettlementLocalAuthorityMismatch);
    }

    let first = scan_selected_root_local_snapshot(sync_root)?;
    let second = scan_selected_root_local_snapshot(sync_root)?;
    if first != second {
        return Err(SelectedRootExecutorError::LocalDiffScanRace);
    }
    let current_source = first
        .iter()
        .find(|item| item.relative_path() == candidate.relative_path())
        .ok_or(SelectedRootExecutorError::RemoteWriteFolderSettlementLocalIdentityMismatch)?;
    if current_source.kind() != LocalItemKind::Directory
        || current_source.size_bytes().is_some()
        || current_source.device_id() != candidate.local_device_id
        || current_source.inode() != candidate.local_inode
    {
        return Err(SelectedRootExecutorError::RemoteWriteFolderSettlementLocalIdentityMismatch);
    }

    let old_diff = build_selected_root_local_inventory_diff(
        &baseline,
        &first,
        state.generation,
        state.snapshot_completed_at_unix_ms,
    )?;
    let source_entry = old_diff
        .entries()
        .iter()
        .find(|entry| entry.relative_path() == candidate.relative_path())
        .ok_or(SelectedRootExecutorError::RemoteWriteFolderSettlementSourceDiffMismatch)?;
    if source_entry.kind != SelectedRootLocalDiffKind::Created
        || source_entry.baseline_kind.is_some()
        || source_entry.current_kind != Some(LocalItemKind::Directory)
    {
        return Err(SelectedRootExecutorError::RemoteWriteFolderSettlementSourceDiffMismatch);
    }

    let remote = storage
        .sync_root_remote_item(&sync_root.id, candidate.predetermined_remote_id())?
        .ok_or(SelectedRootExecutorError::RemoteWriteFolderSettlementRemoteCatalogMismatch)?;
    let expected_name = basename(candidate.relative_path());
    if expected_name.is_empty()
        || remote.name != expected_name
        || remote.kind != RemoteItemKind::Folder
        || remote.parent_remote_id.as_deref() != Some(candidate.expected_parent_remote_id())
        || remote.trashed
    {
        return Err(SelectedRootExecutorError::RemoteWriteFolderSettlementRemoteCatalogMismatch);
    }

    let mut proposed_baseline = baseline.clone();
    proposed_baseline.push(current_source.clone());
    proposed_baseline.sort_by(|left, right| left.relative_path().cmp(right.relative_path()));
    let next_generation = state
        .generation
        .checked_add(1)
        .ok_or(SelectedRootExecutorError::CountOverflow)?;
    let residual_diff = build_selected_root_local_inventory_diff(
        &proposed_baseline,
        &first,
        next_generation,
        Some(settled_at_unix_ms),
    )?;
    if residual_diff
        .entries()
        .iter()
        .any(|entry| entry.relative_path() == candidate.relative_path())
    {
        return Err(SelectedRootExecutorError::RemoteWriteFolderSettlementResidualMismatch);
    }
    let mut residual_events = Vec::with_capacity(residual_diff.entries().len());
    for entry in residual_diff.entries() {
        residual_events.push(LocalChangeEventInput::new(
            entry.relative_path(),
            local_change_event_kind(entry.kind),
            entry.baseline_kind,
            entry.current_kind,
        )?);
    }

    Ok(RemoteWriteFolderCreateSettlementInput::new(
        candidate.intent_id,
        candidate.source_local_event_id,
        candidate.execution_generation,
        state.generation,
        state.item_count,
        state.snapshot_completed_at_unix_ms,
        current_source.clone(),
        residual_events,
        candidate.predetermined_remote_id(),
        candidate.expected_parent_remote_id(),
        settled_at_unix_ms,
    )?)
}

pub fn open_selected_root_file_create_local_source(
    sync_root: &SyncRoot,
    candidate: &RemoteWriteFileCreateCandidate,
) -> Result<SelectedRootFileCreateLocalSource, SelectedRootExecutorError> {
    if sync_root.mode != SyncMode::TwoWay {
        return Err(SelectedRootExecutorError::RemoteWriteFileCreateModeUnsupported);
    }
    if candidate.local_size_bytes == 0 {
        return Err(SelectedRootExecutorError::RemoteWriteFileCreateEmptyUnsupported);
    }

    let first = scan_selected_root_local_snapshot(sync_root)?;
    let second = scan_selected_root_local_snapshot(sync_root)?;
    if first != second {
        return Err(SelectedRootExecutorError::LocalDiffScanRace);
    }

    let item = first
        .iter()
        .find(|item| item.relative_path() == candidate.relative_path())
        .ok_or(SelectedRootExecutorError::RemoteWriteFileCreateLocalIdentityMismatch)?;

    if item.kind() != LocalItemKind::File
        || item.size_bytes() != Some(candidate.local_size_bytes)
        || item.modified_unix_ns() != candidate.local_modified_unix_ns
        || item.device_id() != candidate.local_device_id
        || item.inode() != candidate.local_inode
    {
        return Err(SelectedRootExecutorError::RemoteWriteFileCreateLocalIdentityMismatch);
    }

    let root = validated_selected_root_path(sync_root)?;
    let relative = Path::new(candidate.relative_path());
    if relative.is_absolute() {
        return Err(SelectedRootExecutorError::RemoteWriteFileCreateLocalIdentityMismatch);
    }

    let mut absolute = root.clone();
    let mut components = relative.components().peekable();
    if components.peek().is_none() {
        return Err(SelectedRootExecutorError::RemoteWriteFileCreateLocalIdentityMismatch);
    }

    while let Some(component) = components.next() {
        let Component::Normal(name) = component else {
            return Err(SelectedRootExecutorError::RemoteWriteFileCreateLocalIdentityMismatch);
        };
        absolute.push(name);
        if !absolute.starts_with(&root) {
            return Err(SelectedRootExecutorError::RemoteWriteFileCreateLocalIdentityMismatch);
        }

        let metadata = fs::symlink_metadata(&absolute)
            .map_err(|_| SelectedRootExecutorError::RemoteWriteFileCreateLocalIdentityMismatch)?;
        if metadata.file_type().is_symlink() {
            return Err(SelectedRootExecutorError::RemoteWriteFileCreateLocalIdentityMismatch);
        }

        if components.peek().is_some() && !metadata.is_dir() {
            return Err(SelectedRootExecutorError::RemoteWriteFileCreateLocalIdentityMismatch);
        }
        if components.peek().is_none() && !metadata.is_file() {
            return Err(SelectedRootExecutorError::RemoteWriteFileCreateLocalIdentityMismatch);
        }
    }

    let file = fs::File::open(&absolute)
        .map_err(|_| SelectedRootExecutorError::RemoteWriteFileCreateReadFailed)?;
    let opened = file
        .metadata()
        .map_err(|_| SelectedRootExecutorError::RemoteWriteFileCreateReadFailed)?;

    let modified_unix_ns = opened
        .mtime()
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(opened.mtime_nsec()))
        .ok_or(SelectedRootExecutorError::LocalMetadataTimestampOverflow)?;

    if !opened.is_file()
        || opened.len() != candidate.local_size_bytes
        || modified_unix_ns != candidate.local_modified_unix_ns
        || opened.dev() != candidate.local_device_id
        || opened.ino() != candidate.local_inode
    {
        return Err(SelectedRootExecutorError::RemoteWriteFileCreateLocalIdentityMismatch);
    }

    let path_metadata = fs::symlink_metadata(&absolute)
        .map_err(|_| SelectedRootExecutorError::RemoteWriteFileCreateLocalIdentityMismatch)?;
    if path_metadata.file_type().is_symlink()
        || path_metadata.dev() != opened.dev()
        || path_metadata.ino() != opened.ino()
    {
        return Err(SelectedRootExecutorError::RemoteWriteFileCreateLocalIdentityMismatch);
    }

    let leaf_name = basename(candidate.relative_path());
    if leaf_name.is_empty() {
        return Err(SelectedRootExecutorError::RemoteWriteFileCreateLocalIdentityMismatch);
    }

    Ok(SelectedRootFileCreateLocalSource {
        file,
        absolute_path: absolute,
        leaf_name: leaf_name.to_owned(),
        total_bytes: candidate.local_size_bytes,
        initial_metadata: opened,
        bytes_read: 0,
        hasher: Sha256::new(),
    })
}

pub fn validate_selected_root_folder_create_local_identity(
    sync_root: &SyncRoot,
    relative_path: &str,
    expected_modified_unix_ns: i64,
    expected_device_id: u64,
    expected_inode: u64,
) -> Result<SelectedRootFolderCreateLocalValidation, SelectedRootExecutorError> {
    if sync_root.mode != SyncMode::TwoWay {
        return Err(SelectedRootExecutorError::RemoteWriteFolderCreateModeUnsupported);
    }

    let first = scan_selected_root_local_snapshot(sync_root)?;
    let second = scan_selected_root_local_snapshot(sync_root)?;
    if first != second {
        return Err(SelectedRootExecutorError::LocalDiffScanRace);
    }

    let item = first
        .iter()
        .find(|item| item.relative_path() == relative_path)
        .ok_or(SelectedRootExecutorError::RemoteWriteFolderCreateLocalIdentityMismatch)?;

    if item.kind() != LocalItemKind::Directory
        || item.size_bytes().is_some()
        || item.modified_unix_ns() != expected_modified_unix_ns
        || item.device_id() != expected_device_id
        || item.inode() != expected_inode
    {
        return Err(SelectedRootExecutorError::RemoteWriteFolderCreateLocalIdentityMismatch);
    }

    let leaf_name = basename(relative_path);
    if leaf_name.is_empty() {
        return Err(SelectedRootExecutorError::RemoteWriteFolderCreateLocalIdentityMismatch);
    }

    Ok(SelectedRootFolderCreateLocalValidation {
        leaf_name: leaf_name.to_owned(),
    })
}

pub fn journal_selected_root_two_way_local_inventory_diff(
    storage: &mut Storage,
    sync_root: &SyncRoot,
    observed_at_unix_ms: i64,
) -> Result<SelectedRootLocalJournalResult, SelectedRootExecutorError> {
    if sync_root.mode != SyncMode::TwoWay {
        return Err(SelectedRootExecutorError::TwoWayLocalJournalModeUnsupported);
    }

    let state = storage.sync_root_local_inventory_state(&sync_root.id)?;
    if !state.snapshot_complete {
        return Err(SelectedRootExecutorError::LocalDiffBaselineMissing);
    }
    if !state.observation_valid {
        return Err(SelectedRootExecutorError::LocalDiffBaselineInvalidated);
    }

    let baseline = storage.list_sync_root_local_items(&sync_root.id)?;
    let baseline_count =
        u64::try_from(baseline.len()).map_err(|_| SelectedRootExecutorError::CountOverflow)?;
    if baseline_count != state.item_count {
        return Err(SelectedRootExecutorError::LocalDiffBaselineCountMismatch);
    }

    let first = scan_selected_root_local_snapshot(sync_root)?;
    let second = scan_selected_root_local_snapshot(sync_root)?;
    if first != second {
        return Err(SelectedRootExecutorError::LocalDiffScanRace);
    }

    let diff = build_selected_root_local_inventory_diff(
        &baseline,
        &first,
        state.generation,
        state.snapshot_completed_at_unix_ms,
    )?;

    let mut events = Vec::with_capacity(diff.entries().len());
    for entry in diff.entries() {
        events.push(LocalChangeEventInput::new(
            entry.relative_path(),
            local_change_event_kind(entry.kind),
            entry.baseline_kind,
            entry.current_kind,
        )?);
    }

    let baseline_item_count =
        u64::try_from(diff.baseline_items).map_err(|_| SelectedRootExecutorError::CountOverflow)?;

    let commit = storage.reconcile_sync_root_local_change_journal(
        &sync_root.id,
        diff.baseline_generation,
        baseline_item_count,
        diff.baseline_snapshot_completed_at_unix_ms,
        &events,
        observed_at_unix_ms,
    )?;

    Ok(SelectedRootLocalJournalResult {
        changes_total: diff.action_count(),
        created: diff.created,
        deleted: diff.deleted,
        modified: diff.modified,
        type_changed: diff.type_changed,
        baseline_generation: commit.baseline_generation,
        pending_events: commit.pending_events,
        superseded_events: commit.superseded_events,
    })
}

pub fn journal_selected_root_local_inventory_diff(
    storage: &mut Storage,
    sync_root: &SyncRoot,
    observed_at_unix_ms: i64,
) -> Result<SelectedRootLocalJournalResult, SelectedRootExecutorError> {
    let diff = plan_selected_root_local_inventory_diff(storage, sync_root)?;

    let mut events = Vec::with_capacity(diff.entries().len());
    for entry in diff.entries() {
        events.push(LocalChangeEventInput::new(
            entry.relative_path(),
            local_change_event_kind(entry.kind),
            entry.baseline_kind,
            entry.current_kind,
        )?);
    }

    let baseline_item_count =
        u64::try_from(diff.baseline_items).map_err(|_| SelectedRootExecutorError::CountOverflow)?;

    let commit: LocalChangeJournalCommit = storage.reconcile_sync_root_local_change_journal(
        &sync_root.id,
        diff.baseline_generation,
        baseline_item_count,
        diff.baseline_snapshot_completed_at_unix_ms,
        &events,
        observed_at_unix_ms,
    )?;

    Ok(SelectedRootLocalJournalResult {
        changes_total: diff.action_count(),
        created: diff.created,
        deleted: diff.deleted,
        modified: diff.modified,
        type_changed: diff.type_changed,
        baseline_generation: commit.baseline_generation,
        pending_events: commit.pending_events,
        superseded_events: commit.superseded_events,
    })
}

pub fn plan_selected_root_remote_write_intents(
    storage: &Storage,
    sync_root: &SyncRoot,
    full_sync_credential_present: bool,
) -> Result<SelectedRootRemoteWriteIntentPlan, SelectedRootExecutorError> {
    let local_state = storage.sync_root_local_inventory_state(&sync_root.id)?;
    if !local_state.snapshot_complete {
        return Err(SelectedRootExecutorError::RemoteWritePlanLocalBaselineMissing);
    }
    if !local_state.observation_valid {
        return Err(SelectedRootExecutorError::RemoteWritePlanLocalBaselineInvalid);
    }

    let baseline = storage.list_sync_root_local_items(&sync_root.id)?;
    let baseline_count =
        u64::try_from(baseline.len()).map_err(|_| SelectedRootExecutorError::CountOverflow)?;
    if baseline_count != local_state.item_count {
        return Err(SelectedRootExecutorError::RemoteWritePlanLocalBaselineCountMismatch);
    }

    let first = scan_selected_root_local_snapshot(sync_root)?;
    let second = scan_selected_root_local_snapshot(sync_root)?;
    if first != second {
        return Err(SelectedRootExecutorError::RemoteWritePlanLocalScanRace);
    }

    let diff = build_selected_root_local_inventory_diff(
        &baseline,
        &first,
        local_state.generation,
        local_state.snapshot_completed_at_unix_ms,
    )?;
    let events = storage
        .list_pending_sync_root_local_change_events(&sync_root.id, local_state.generation)?;

    if events.len() != diff.entries().len() {
        return Err(SelectedRootExecutorError::RemoteWritePlanLocalJournalMismatch);
    }

    for (event, entry) in events.iter().zip(diff.entries()) {
        if event.relative_path() != entry.relative_path()
            || event.kind != local_change_event_kind(entry.kind)
            || event.baseline_kind != entry.baseline_kind
            || event.current_kind != entry.current_kind
        {
            return Err(SelectedRootExecutorError::RemoteWritePlanLocalJournalMismatch);
        }
    }

    let remote_root_id = sync_root
        .remote_root_id
        .as_deref()
        .ok_or(SelectedRootExecutorError::MissingRemoteRoot)?;

    let remote_state = storage.sync_root_remote_inventory_state(&sync_root.id)?;
    if !remote_state.ready_for_reconciliation() {
        return Err(SelectedRootExecutorError::RemoteWritePlanRemoteCatalogNotReady);
    }
    if storage
        .sync_root_change_window_state(&sync_root.id)?
        .is_some()
    {
        return Err(SelectedRootExecutorError::RemoteWritePlanRemoteChangeWindowPending);
    }
    let remote_cursor = storage
        .sync_root_change_cursor(&sync_root.id)?
        .ok_or(SelectedRootExecutorError::RemoteWritePlanRemoteCursorMissing)?;

    let remote_items = storage.list_sync_root_remote_items(&sync_root.id)?;
    let remote_item_count =
        u64::try_from(remote_items.len()).map_err(|_| SelectedRootExecutorError::CountOverflow)?;
    if remote_item_count != remote_state.item_count {
        return Err(SelectedRootExecutorError::RemoteWritePlanRemoteCatalogCountMismatch);
    }

    let authority_state = storage
        .sync_root_remote_write_authority_state(&sync_root.id)?
        .ok_or(SelectedRootExecutorError::RemoteWritePlanAuthorityStateMissing)?;
    if authority_state.change_cursor != remote_cursor {
        return Err(SelectedRootExecutorError::RemoteWritePlanAuthorityCursorMismatch);
    }

    let authorities = storage.list_sync_root_remote_write_authorities(&sync_root.id)?;
    let expected_authority_count = remote_item_count
        .checked_add(1)
        .ok_or(SelectedRootExecutorError::CountOverflow)?;
    if authority_state.item_count != expected_authority_count
        || u64::try_from(authorities.len()).map_err(|_| SelectedRootExecutorError::CountOverflow)?
            != expected_authority_count
    {
        return Err(SelectedRootExecutorError::RemoteWritePlanAuthorityCountMismatch);
    }

    let expected_ids = std::iter::once(remote_root_id.to_owned())
        .chain(remote_items.iter().map(|item| item.remote_id.clone()))
        .collect::<HashSet<_>>();
    let authority_ids = authorities
        .iter()
        .map(|authority| authority.remote_id().to_owned())
        .collect::<HashSet<_>>();
    if expected_ids != authority_ids {
        return Err(SelectedRootExecutorError::RemoteWritePlanAuthorityCoverageMismatch);
    }

    let file_receipts = storage.list_sync_root_file_materialization_receipts(&sync_root.id)?;
    let directory_receipts =
        storage.list_sync_root_directory_materialization_receipts(&sync_root.id)?;

    let entries = derive_selected_root_remote_write_plan_entries(
        remote_root_id,
        &events,
        &baseline,
        &first,
        &remote_items,
        &file_receipts,
        &directory_receipts,
        &authorities,
    )?;

    let create_file_needs_id = entries
        .iter()
        .filter(|entry| {
            entry.operation == Some(RemoteWriteIntentOperation::CreateFile)
                && entry.disposition
                    == SelectedRootRemoteWritePlanDisposition::NeedsPredeterminedRemoteId
        })
        .count();
    let create_folder_needs_id = entries
        .iter()
        .filter(|entry| {
            entry.operation == Some(RemoteWriteIntentOperation::CreateFolder)
                && entry.disposition
                    == SelectedRootRemoteWritePlanDisposition::NeedsPredeterminedRemoteId
        })
        .count();
    let update_file_ready = entries
        .iter()
        .filter(|entry| {
            entry.operation == Some(RemoteWriteIntentOperation::UpdateFile)
                && entry.disposition == SelectedRootRemoteWritePlanDisposition::Ready
        })
        .count();
    let trash_item_ready = entries
        .iter()
        .filter(|entry| {
            entry.operation == Some(RemoteWriteIntentOperation::TrashItem)
                && entry.disposition == SelectedRootRemoteWritePlanDisposition::Ready
        })
        .count();
    let conflicts = entries
        .iter()
        .filter(|entry| entry.disposition == SelectedRootRemoteWritePlanDisposition::Conflict)
        .count();
    let blocked_identity = entries
        .iter()
        .filter(|entry| {
            entry.disposition == SelectedRootRemoteWritePlanDisposition::BlockedIdentity
        })
        .count();
    let blocked_authority = entries
        .iter()
        .filter(|entry| {
            entry.disposition == SelectedRootRemoteWritePlanDisposition::BlockedAuthority
        })
        .count();

    Ok(SelectedRootRemoteWriteIntentPlan {
        baseline_generation: local_state.generation,
        pending_events: events.len(),
        create_file_needs_id,
        create_folder_needs_id,
        update_file_ready,
        trash_item_ready,
        conflicts,
        blocked_identity,
        blocked_authority,
        root_write_capable: !matches!(sync_root.mode, SyncMode::ReceiveOnly),
        full_sync_credential_present,
        entries,
    })
}

#[allow(clippy::too_many_arguments)]
fn derive_selected_root_remote_write_plan_entries(
    remote_root_id: &str,
    events: &[LocalChangeEventRecord],
    baseline: &[LocalItemSnapshot],
    current: &[LocalItemSnapshot],
    remote_items: &[RemoteItem],
    file_receipts: &[SyncRootFileMaterializationReceipt],
    directory_receipts: &[SyncRootDirectoryMaterializationReceipt],
    authorities: &[RemoteWriteAuthoritySnapshot],
) -> Result<Vec<SelectedRootRemoteWritePlanEntry>, SelectedRootExecutorError> {
    let baseline_by_path = baseline
        .iter()
        .map(|item| (item.relative_path(), item))
        .collect::<BTreeMap<_, _>>();
    let current_by_path = current
        .iter()
        .map(|item| (item.relative_path(), item))
        .collect::<BTreeMap<_, _>>();
    if baseline_by_path.len() != baseline.len() || current_by_path.len() != current.len() {
        return Err(SelectedRootExecutorError::LocalDiffDuplicatePath);
    }

    let remote_by_id = remote_items
        .iter()
        .map(|item| (item.remote_id.clone(), item))
        .collect::<BTreeMap<_, _>>();
    if remote_by_id.len() != remote_items.len() {
        return Err(SelectedRootExecutorError::RemoteWritePlanRemoteCatalogIdentityAmbiguous);
    }

    let authority_by_id = authorities
        .iter()
        .map(|authority| (authority.remote_id().to_owned(), authority))
        .collect::<BTreeMap<_, _>>();
    if authority_by_id.len() != authorities.len() {
        return Err(SelectedRootExecutorError::RemoteWritePlanAuthorityCoverageMismatch);
    }

    let mut ownership = BTreeMap::<String, (String, LocalItemKind)>::new();
    for receipt in file_receipts {
        if ownership
            .insert(
                receipt.relative_path.clone(),
                (receipt.remote_id.clone(), LocalItemKind::File),
            )
            .is_some()
        {
            return Err(SelectedRootExecutorError::RemoteWritePlanOwnershipAmbiguous);
        }
    }
    for receipt in directory_receipts {
        if ownership
            .insert(
                receipt.relative_path.clone(),
                (receipt.remote_id.clone(), LocalItemKind::Directory),
            )
            .is_some()
        {
            return Err(SelectedRootExecutorError::RemoteWritePlanOwnershipAmbiguous);
        }
    }

    let deleted_paths = events
        .iter()
        .filter(|event| event.kind == LocalChangeEventKind::Deleted)
        .map(|event| event.relative_path().to_owned())
        .collect::<HashSet<_>>();

    let mut entries = Vec::with_capacity(events.len());

    for event in events {
        let operation = match (event.kind, event.current_kind, event.baseline_kind) {
            (LocalChangeEventKind::Created, Some(LocalItemKind::File), _) => {
                Some(RemoteWriteIntentOperation::CreateFile)
            }
            (LocalChangeEventKind::Created, Some(LocalItemKind::Directory), _) => {
                Some(RemoteWriteIntentOperation::CreateFolder)
            }
            (LocalChangeEventKind::Modified, Some(LocalItemKind::File), _) => {
                Some(RemoteWriteIntentOperation::UpdateFile)
            }
            (LocalChangeEventKind::Deleted, _, _) => Some(RemoteWriteIntentOperation::TrashItem),
            (LocalChangeEventKind::Modified, Some(LocalItemKind::Directory), _)
            | (LocalChangeEventKind::TypeChanged, _, _) => None,
            _ => None,
        };

        let identity_snapshot = match event.kind {
            LocalChangeEventKind::Created | LocalChangeEventKind::Modified => {
                current_by_path.get(event.relative_path()).copied()
            }
            LocalChangeEventKind::Deleted => baseline_by_path.get(event.relative_path()).copied(),
            LocalChangeEventKind::TypeChanged => {
                current_by_path.get(event.relative_path()).copied()
            }
        };

        let mut entry = plan_entry_from_snapshot(event, operation, identity_snapshot);

        if event.kind == LocalChangeEventKind::TypeChanged
            || (event.kind == LocalChangeEventKind::Modified
                && event.current_kind == Some(LocalItemKind::Directory))
        {
            entry.disposition = SelectedRootRemoteWritePlanDisposition::Conflict;
            entries.push(entry);
            continue;
        }

        let Some(snapshot) = identity_snapshot else {
            entry.disposition = SelectedRootRemoteWritePlanDisposition::BlockedIdentity;
            entries.push(entry);
            continue;
        };

        if event.kind == LocalChangeEventKind::Deleted
            && has_deleted_ancestor(event.relative_path(), &deleted_paths)
        {
            entry.disposition = SelectedRootRemoteWritePlanDisposition::BlockedIdentity;
            entries.push(entry);
            continue;
        }

        match event.kind {
            LocalChangeEventKind::Created => {
                let Some(parent_remote_id) = resolve_owned_parent_remote_id(
                    event.relative_path(),
                    remote_root_id,
                    &ownership,
                    &remote_by_id,
                ) else {
                    entry.disposition = SelectedRootRemoteWritePlanDisposition::BlockedIdentity;
                    entries.push(entry);
                    continue;
                };

                entry.expected_parent_remote_id = Some(parent_remote_id.clone());
                let Some(parent_authority) = authority_by_id.get(&parent_remote_id) else {
                    entry.disposition = SelectedRootRemoteWritePlanDisposition::BlockedAuthority;
                    entries.push(entry);
                    continue;
                };

                if !parent_authority.can_add_children {
                    entry.disposition = SelectedRootRemoteWritePlanDisposition::BlockedAuthority;
                    entries.push(entry);
                    continue;
                }

                entry.local_kind = Some(snapshot.kind());
                entry.disposition =
                    SelectedRootRemoteWritePlanDisposition::NeedsPredeterminedRemoteId;
            }
            LocalChangeEventKind::Modified | LocalChangeEventKind::Deleted => {
                let expected_kind = if event.kind == LocalChangeEventKind::Modified {
                    event.current_kind
                } else {
                    event.baseline_kind
                };
                let Some(expected_kind) = expected_kind else {
                    entry.disposition = SelectedRootRemoteWritePlanDisposition::BlockedIdentity;
                    entries.push(entry);
                    continue;
                };

                let Some((remote_id, receipt_kind)) = ownership.get(event.relative_path()) else {
                    entry.disposition = SelectedRootRemoteWritePlanDisposition::BlockedIdentity;
                    entries.push(entry);
                    continue;
                };
                if *receipt_kind != expected_kind {
                    entry.disposition = SelectedRootRemoteWritePlanDisposition::BlockedIdentity;
                    entries.push(entry);
                    continue;
                }

                let Some(remote_item) = remote_by_id.get(remote_id) else {
                    entry.disposition = SelectedRootRemoteWritePlanDisposition::BlockedIdentity;
                    entries.push(entry);
                    continue;
                };

                let expected_remote_kind = match expected_kind {
                    LocalItemKind::File => RemoteItemKind::File,
                    LocalItemKind::Directory => RemoteItemKind::Folder,
                };
                if remote_item.kind != expected_remote_kind
                    || remote_item.trashed
                    || remote_item.name != basename(event.relative_path())
                {
                    entry.disposition = SelectedRootRemoteWritePlanDisposition::BlockedIdentity;
                    entries.push(entry);
                    continue;
                }

                let Some(expected_parent_remote_id) = resolve_owned_parent_remote_id(
                    event.relative_path(),
                    remote_root_id,
                    &ownership,
                    &remote_by_id,
                ) else {
                    entry.disposition = SelectedRootRemoteWritePlanDisposition::BlockedIdentity;
                    entries.push(entry);
                    continue;
                };
                if remote_item.parent_remote_id.as_deref()
                    != Some(expected_parent_remote_id.as_str())
                {
                    entry.disposition = SelectedRootRemoteWritePlanDisposition::BlockedIdentity;
                    entries.push(entry);
                    continue;
                }

                let Some(authority) = authority_by_id.get(remote_id) else {
                    entry.disposition = SelectedRootRemoteWritePlanDisposition::BlockedAuthority;
                    entries.push(entry);
                    continue;
                };

                let capability_ok = match event.kind {
                    LocalChangeEventKind::Modified => authority.can_edit,
                    LocalChangeEventKind::Deleted => authority.can_trash,
                    LocalChangeEventKind::Created | LocalChangeEventKind::TypeChanged => false,
                };
                if !capability_ok {
                    entry.disposition = SelectedRootRemoteWritePlanDisposition::BlockedAuthority;
                    entries.push(entry);
                    continue;
                }

                entry.target_remote_id = Some(remote_id.clone());
                entry.expected_parent_remote_id = Some(expected_parent_remote_id);
                entry.expected_remote_kind = Some(expected_remote_kind);
                entry.expected_remote_version = Some(authority.remote_version);
                entry.expected_remote_size_bytes = remote_item.size_bytes;
                entry.expected_checksum_algorithm =
                    authority.checksum_algorithm().map(str::to_owned);
                entry.expected_content_checksum = authority.content_checksum().map(str::to_owned);
                entry.disposition = SelectedRootRemoteWritePlanDisposition::Ready;
            }
            LocalChangeEventKind::TypeChanged => unreachable!("handled above"),
        }

        entries.push(entry);
    }

    Ok(entries)
}

fn plan_entry_from_snapshot(
    event: &LocalChangeEventRecord,
    operation: Option<RemoteWriteIntentOperation>,
    snapshot: Option<&LocalItemSnapshot>,
) -> SelectedRootRemoteWritePlanEntry {
    SelectedRootRemoteWritePlanEntry {
        source_local_event_id: event.id,
        relative_path: event.relative_path().to_owned(),
        operation,
        disposition: SelectedRootRemoteWritePlanDisposition::BlockedIdentity,
        local_kind: snapshot.map(LocalItemSnapshot::kind),
        local_size_bytes: snapshot.and_then(LocalItemSnapshot::size_bytes),
        local_modified_unix_ns: snapshot.map(LocalItemSnapshot::modified_unix_ns),
        local_device_id: snapshot.map(LocalItemSnapshot::device_id),
        local_inode: snapshot.map(LocalItemSnapshot::inode),
        target_remote_id: None,
        expected_parent_remote_id: None,
        expected_remote_kind: None,
        expected_remote_version: None,
        expected_remote_size_bytes: None,
        expected_checksum_algorithm: None,
        expected_content_checksum: None,
    }
}

fn resolve_owned_parent_remote_id(
    relative_path: &str,
    remote_root_id: &str,
    ownership: &BTreeMap<String, (String, LocalItemKind)>,
    remote_by_id: &BTreeMap<String, &RemoteItem>,
) -> Option<String> {
    let Some((parent_path, _)) = relative_path.rsplit_once('/') else {
        return Some(remote_root_id.to_owned());
    };
    resolve_owned_directory_remote_id(parent_path, remote_root_id, ownership, remote_by_id)
}

fn resolve_owned_directory_remote_id(
    relative_path: &str,
    remote_root_id: &str,
    ownership: &BTreeMap<String, (String, LocalItemKind)>,
    remote_by_id: &BTreeMap<String, &RemoteItem>,
) -> Option<String> {
    let (remote_id, kind) = ownership.get(relative_path)?;
    if *kind != LocalItemKind::Directory {
        return None;
    }
    let remote = remote_by_id.get(remote_id)?;
    if remote.kind != RemoteItemKind::Folder
        || remote.trashed
        || remote.name != basename(relative_path)
    {
        return None;
    }

    let expected_parent = match relative_path.rsplit_once('/') {
        Some((parent_path, _)) => {
            resolve_owned_directory_remote_id(parent_path, remote_root_id, ownership, remote_by_id)?
        }
        None => remote_root_id.to_owned(),
    };

    if remote.parent_remote_id.as_deref() != Some(expected_parent.as_str()) {
        return None;
    }

    Some(remote_id.clone())
}

fn basename(relative_path: &str) -> &str {
    relative_path
        .rsplit_once('/')
        .map(|(_, name)| name)
        .unwrap_or(relative_path)
}

fn has_deleted_ancestor(relative_path: &str, deleted_paths: &HashSet<String>) -> bool {
    let mut candidate = relative_path;
    while let Some((parent, _)) = candidate.rsplit_once('/') {
        if deleted_paths.contains(parent) {
            return true;
        }
        candidate = parent;
    }
    false
}

fn local_change_event_kind(kind: SelectedRootLocalDiffKind) -> LocalChangeEventKind {
    match kind {
        SelectedRootLocalDiffKind::Created => LocalChangeEventKind::Created,
        SelectedRootLocalDiffKind::Deleted => LocalChangeEventKind::Deleted,
        SelectedRootLocalDiffKind::Modified => LocalChangeEventKind::Modified,
        SelectedRootLocalDiffKind::TypeChanged => LocalChangeEventKind::TypeChanged,
    }
}

pub fn capture_selected_root_local_baseline(
    storage: &mut Storage,
    sync_root: &SyncRoot,
    observed_at_unix_ms: i64,
) -> Result<SelectedRootLocalBaselineCapture, SelectedRootExecutorError> {
    if sync_root.mode != SyncMode::ReceiveOnly {
        return Err(SelectedRootExecutorError::LocalBaselineModeUnsupported);
    }

    let existing = storage.sync_root_local_inventory_state(&sync_root.id)?;
    if existing.snapshot_complete && existing.observation_valid {
        return Err(SelectedRootExecutorError::LocalBaselineAlreadyComplete);
    }

    let pre = plan_selected_root_receive_only_convergence(storage, sync_root)?;
    if pre.blocked() || pre.action_count() != 0 {
        return Err(SelectedRootExecutorError::LocalBaselineConvergenceNotClean);
    }

    let first = scan_selected_root_local_snapshot(sync_root)?;

    storage.begin_sync_root_local_inventory_staging(&sync_root.id)?;
    storage.stage_sync_root_local_inventory_items(&sync_root.id, &first, observed_at_unix_ms)?;

    let second = match scan_selected_root_local_snapshot(sync_root) {
        Ok(value) => value,
        Err(error) => {
            storage.begin_sync_root_local_inventory_staging(&sync_root.id)?;
            return Err(error);
        }
    };

    if first != second {
        storage.begin_sync_root_local_inventory_staging(&sync_root.id)?;
        return Err(SelectedRootExecutorError::LocalBaselineScanRace);
    }

    let post = match plan_selected_root_receive_only_convergence(storage, sync_root) {
        Ok(value) => value,
        Err(error) => {
            storage.begin_sync_root_local_inventory_staging(&sync_root.id)?;
            return Err(error);
        }
    };

    if post.blocked() || post.action_count() != 0 {
        storage.begin_sync_root_local_inventory_staging(&sync_root.id)?;
        return Err(SelectedRootExecutorError::LocalBaselineScanRace);
    }

    let staged = storage.staged_sync_root_local_inventory_count(&sync_root.id)?;
    let expected =
        u64::try_from(first.len()).map_err(|_| SelectedRootExecutorError::CountOverflow)?;
    if staged != expected {
        storage.begin_sync_root_local_inventory_staging(&sync_root.id)?;
        return Err(SelectedRootExecutorError::LocalBaselineStagingMismatch);
    }

    let committed =
        storage.commit_sync_root_local_inventory_snapshot(&sync_root.id, observed_at_unix_ms)?;

    if committed != first.len() {
        return Err(SelectedRootExecutorError::LocalBaselineCommitMismatch);
    }

    let durable = storage.sync_root_local_inventory_state(&sync_root.id)?;
    if !durable.snapshot_complete || durable.item_count != expected {
        return Err(SelectedRootExecutorError::LocalBaselineCommitMismatch);
    }

    let files_captured = first
        .iter()
        .filter(|item| item.kind() == LocalItemKind::File)
        .count();
    let directories_captured = first
        .iter()
        .filter(|item| item.kind() == LocalItemKind::Directory)
        .count();

    Ok(SelectedRootLocalBaselineCapture {
        items_captured: first.len(),
        files_captured,
        directories_captured,
        convergence_actions: 0,
        snapshot_complete: true,
    })
}

fn scan_selected_root_local_snapshot(
    sync_root: &SyncRoot,
) -> Result<Vec<LocalItemSnapshot>, SelectedRootExecutorError> {
    let configured_root = validated_selected_root_path(sync_root)?;
    let mut queue = VecDeque::from([(configured_root, String::new())]);
    let mut items = Vec::new();

    while let Some((absolute_parent, relative_parent)) = queue.pop_front() {
        let directory = fs::read_dir(&absolute_parent)
            .map_err(|_| SelectedRootExecutorError::LocalFilesystemInspectionFailed)?;

        for entry in directory {
            let entry =
                entry.map_err(|_| SelectedRootExecutorError::LocalFilesystemInspectionFailed)?;

            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| SelectedRootExecutorError::LocalEntryNonUtf8)?;

            let relative_path = if relative_parent.is_empty() {
                name
            } else {
                format!("{relative_parent}/{name}")
            };

            let metadata = fs::symlink_metadata(entry.path())
                .map_err(|_| SelectedRootExecutorError::LocalFilesystemInspectionFailed)?;

            if metadata.file_type().is_symlink() {
                return Err(SelectedRootExecutorError::LocalEntrySymlinkUnsupported);
            }

            let (kind, size_bytes) = if metadata.is_dir() {
                (LocalItemKind::Directory, None)
            } else if metadata.is_file() {
                (LocalItemKind::File, Some(metadata.len()))
            } else {
                return Err(SelectedRootExecutorError::LocalEntryTypeUnsupported);
            };

            let modified_unix_ns = metadata
                .mtime()
                .checked_mul(1_000_000_000)
                .and_then(|value| value.checked_add(metadata.mtime_nsec()))
                .ok_or(SelectedRootExecutorError::LocalMetadataTimestampOverflow)?;

            items.push(
                LocalItemSnapshot::new(
                    relative_path.clone(),
                    kind,
                    size_bytes,
                    modified_unix_ns,
                    metadata.dev(),
                    metadata.ino(),
                )
                .map_err(|_| SelectedRootExecutorError::LocalBaselineSnapshotInvalid)?,
            );

            if items.len() > 1_000_000 {
                return Err(SelectedRootExecutorError::LocalScanSafetyLimitExceeded);
            }

            if kind == LocalItemKind::Directory {
                queue.push_back((entry.path(), relative_path));
            }
        }
    }

    let _ = validated_selected_root_path(sync_root)?;

    items.sort_by(|left, right| left.relative_path().cmp(right.relative_path()));
    Ok(items)
}

fn selected_root_materialization_inputs(
    storage: &Storage,
    sync_root: &SyncRoot,
) -> Result<(Vec<RemoteItem>, Vec<LocalTreeEntry>), SelectedRootExecutorError> {
    if sync_root.mode != nubisync_core::SyncMode::ReceiveOnly {
        return Err(SelectedRootExecutorError::LocalPlanModeUnsupported);
    }

    let inventory = storage.sync_root_remote_inventory_state(&sync_root.id)?;

    if !inventory.snapshot_complete {
        return Err(SelectedRootExecutorError::LocalPlanSnapshotMissing);
    }

    if !inventory.catchup_complete {
        return Err(SelectedRootExecutorError::LocalPlanCatchupIncomplete);
    }

    if storage
        .sync_root_change_window_state(&sync_root.id)?
        .is_some()
    {
        return Err(SelectedRootExecutorError::LocalPlanChangeWindowPending);
    }

    if storage.sync_root_change_cursor(&sync_root.id)?.is_none() {
        return Err(SelectedRootExecutorError::LocalPlanCursorMissing);
    }

    let remote_items = storage.list_sync_root_remote_items(&sync_root.id)?;
    let remote_item_count =
        u64::try_from(remote_items.len()).map_err(|_| SelectedRootExecutorError::CountOverflow)?;

    if remote_item_count != inventory.item_count {
        return Err(SelectedRootExecutorError::LocalPlanCatalogItemCountMismatch);
    }

    let local_entries = scan_selected_root_local_tree(sync_root)?;

    Ok((remote_items, local_entries))
}

fn validated_selected_root_path(
    sync_root: &SyncRoot,
) -> Result<PathBuf, SelectedRootExecutorError> {
    let configured_root = PathBuf::from(&sync_root.local_path);

    let root_metadata = fs::symlink_metadata(&configured_root)
        .map_err(|_| SelectedRootExecutorError::LocalRootUnavailable)?;

    if root_metadata.file_type().is_symlink() {
        return Err(SelectedRootExecutorError::LocalRootSymlinkUnsupported);
    }

    if !root_metadata.is_dir() {
        return Err(SelectedRootExecutorError::LocalRootNotDirectory);
    }

    let canonical_root = fs::canonicalize(&configured_root)
        .map_err(|_| SelectedRootExecutorError::LocalRootUnavailable)?;

    if canonical_root != configured_root {
        return Err(SelectedRootExecutorError::LocalRootIdentityChanged);
    }

    Ok(configured_root)
}

fn scan_selected_root_local_tree(
    sync_root: &SyncRoot,
) -> Result<Vec<LocalTreeEntry>, SelectedRootExecutorError> {
    let configured_root = validated_selected_root_path(sync_root)?;
    let mut queue = VecDeque::from([(configured_root, String::new())]);
    let mut entries = Vec::new();

    while let Some((absolute_parent, relative_parent)) = queue.pop_front() {
        let directory = fs::read_dir(&absolute_parent)
            .map_err(|_| SelectedRootExecutorError::LocalFilesystemInspectionFailed)?;

        for entry in directory {
            let entry =
                entry.map_err(|_| SelectedRootExecutorError::LocalFilesystemInspectionFailed)?;

            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| SelectedRootExecutorError::LocalEntryNonUtf8)?;

            let relative_path = if relative_parent.is_empty() {
                name
            } else {
                format!("{relative_parent}/{name}")
            };

            let metadata = fs::symlink_metadata(entry.path())
                .map_err(|_| SelectedRootExecutorError::LocalFilesystemInspectionFailed)?;

            if metadata.file_type().is_symlink() {
                return Err(SelectedRootExecutorError::LocalEntrySymlinkUnsupported);
            }

            let kind = if metadata.is_dir() {
                LocalTreeEntryKind::Directory
            } else if metadata.is_file() {
                LocalTreeEntryKind::File
            } else {
                return Err(SelectedRootExecutorError::LocalEntryTypeUnsupported);
            };

            entries.push(LocalTreeEntry::new(relative_path.clone(), kind)?);

            if entries.len() > 1_000_000 {
                return Err(SelectedRootExecutorError::LocalScanSafetyLimitExceeded);
            }

            if kind == LocalTreeEntryKind::Directory {
                queue.push_back((entry.path(), relative_path));
            }
        }
    }

    Ok(entries)
}

struct DirectoryApplyOutcome {
    created_paths: Vec<PathBuf>,
    created_targets: Vec<ReceiveOnlyDirectoryTarget>,
    existing_directories: usize,
}

fn apply_selected_root_directory_targets(
    root_path: &Path,
    targets: &[ReceiveOnlyDirectoryTarget],
) -> Result<DirectoryApplyOutcome, SelectedRootExecutorError> {
    let mut created_paths = Vec::new();
    let mut created_targets = Vec::new();
    let mut existing_directories = 0_usize;

    let apply_result = (|| {
        for target in targets {
            let target_path = root_path.join(target.relative_path());

            if !target_path.starts_with(root_path) {
                return Err(SelectedRootExecutorError::LocalDirectoryTargetEscapedRoot);
            }

            match fs::symlink_metadata(&target_path) {
                Ok(metadata) => {
                    if metadata.file_type().is_symlink() || !metadata.is_dir() {
                        return Err(SelectedRootExecutorError::LocalDirectoryTargetConflict);
                    }

                    let canonical_target = fs::canonicalize(&target_path)
                        .map_err(|_| SelectedRootExecutorError::LocalFilesystemInspectionFailed)?;
                    if !canonical_target.starts_with(root_path) {
                        return Err(SelectedRootExecutorError::LocalDirectoryTargetEscapedRoot);
                    }

                    existing_directories = existing_directories
                        .checked_add(1)
                        .ok_or(SelectedRootExecutorError::CountOverflow)?;
                    continue;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => {
                    return Err(SelectedRootExecutorError::LocalFilesystemInspectionFailed);
                }
            }

            let parent = target_path
                .parent()
                .ok_or(SelectedRootExecutorError::LocalDirectoryParentInvalid)?;
            let parent_metadata = fs::symlink_metadata(parent)
                .map_err(|_| SelectedRootExecutorError::LocalDirectoryParentInvalid)?;

            if parent_metadata.file_type().is_symlink() || !parent_metadata.is_dir() {
                return Err(SelectedRootExecutorError::LocalDirectoryParentInvalid);
            }

            let canonical_parent = fs::canonicalize(parent)
                .map_err(|_| SelectedRootExecutorError::LocalDirectoryParentInvalid)?;
            if !canonical_parent.starts_with(root_path) {
                return Err(SelectedRootExecutorError::LocalDirectoryParentInvalid);
            }

            fs::create_dir(&target_path)
                .map_err(|_| SelectedRootExecutorError::LocalDirectoryCreateFailed)?;
            created_paths.push(target_path.clone());
            created_targets.push(target.clone());

            let created_metadata = fs::symlink_metadata(&target_path)
                .map_err(|_| SelectedRootExecutorError::LocalDirectoryPostconditionFailed)?;
            if created_metadata.file_type().is_symlink() || !created_metadata.is_dir() {
                return Err(SelectedRootExecutorError::LocalDirectoryPostconditionFailed);
            }

            let canonical_target = fs::canonicalize(&target_path)
                .map_err(|_| SelectedRootExecutorError::LocalDirectoryPostconditionFailed)?;
            if !canonical_target.starts_with(root_path) {
                return Err(SelectedRootExecutorError::LocalDirectoryTargetEscapedRoot);
            }
        }

        Ok(())
    })();

    if let Err(error) = apply_result {
        rollback_created_directories(&created_paths)?;
        return Err(error);
    }

    Ok(DirectoryApplyOutcome {
        created_paths,
        created_targets,
        existing_directories,
    })
}

fn rollback_created_directories(
    created_paths: &[PathBuf],
) -> Result<(), SelectedRootExecutorError> {
    for path in created_paths.iter().rev() {
        match fs::remove_dir(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(SelectedRootExecutorError::LocalDirectoryRollbackFailed),
        }
    }

    Ok(())
}

pub trait SelectedRootProvider {
    type RootIdentity;

    fn resolve_root(
        &self,
        remote_root_id: &str,
    ) -> Result<Self::RootIdentity, SelectedRootExecutorError>;

    fn canonical_root_id<'a>(&self, root: &'a Self::RootIdentity) -> &'a str;

    fn resolve_membership(
        &self,
        item: &RemoteItem,
        root: &Self::RootIdentity,
    ) -> Result<RootChangeMembership, SelectedRootExecutorError>;

    fn hydrate_folder(
        &self,
        item: &RemoteItem,
    ) -> Result<Vec<RemoteItem>, SelectedRootExecutorError>;
}

impl SelectedRootProvider for GoogleDriveApi {
    type RootIdentity = DriveFolderRoot;

    fn resolve_root(
        &self,
        remote_root_id: &str,
    ) -> Result<Self::RootIdentity, SelectedRootExecutorError> {
        GoogleDriveApi::resolve_folder_root(self, remote_root_id)
            .map_err(SelectedRootExecutorError::from)
    }

    fn canonical_root_id<'a>(&self, root: &'a Self::RootIdentity) -> &'a str {
        root.canonical_remote_id()
    }

    fn resolve_membership(
        &self,
        item: &RemoteItem,
        root: &Self::RootIdentity,
    ) -> Result<RootChangeMembership, SelectedRootExecutorError> {
        let membership = GoogleDriveApi::resolve_item_membership(self, item, root)
            .map_err(SelectedRootExecutorError::from)?;

        Ok(match membership {
            DriveRootMembership::Root => RootChangeMembership::Root,
            DriveRootMembership::Descendant => RootChangeMembership::Descendant,
            DriveRootMembership::Outside => RootChangeMembership::Outside,
        })
    }

    fn hydrate_folder(
        &self,
        item: &RemoteItem,
    ) -> Result<Vec<RemoteItem>, SelectedRootExecutorError> {
        GoogleDriveApi::hydrate_folder_subtree(self, item)
            .map(|hydration| hydration.into_items())
            .map_err(SelectedRootExecutorError::from)
    }
}

#[derive(Clone)]
pub struct SelectedRootInventoryPage {
    items: Vec<RemoteItem>,
    continuation: Option<ContinuationToken>,
    unsupported_provider_native: u64,
}

impl SelectedRootInventoryPage {
    pub fn new(
        items: Vec<RemoteItem>,
        continuation: Option<ContinuationToken>,
        unsupported_provider_native: u64,
    ) -> Self {
        Self {
            items,
            continuation,
            unsupported_provider_native,
        }
    }

    fn into_parts(self) -> (Vec<RemoteItem>, Option<ContinuationToken>, u64) {
        (
            self.items,
            self.continuation,
            self.unsupported_provider_native,
        )
    }
}

impl std::fmt::Debug for SelectedRootInventoryPage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SelectedRootInventoryPage")
            .field("item_count", &self.items.len())
            .field("has_continuation", &self.continuation.is_some())
            .field(
                "unsupported_provider_native",
                &self.unsupported_provider_native,
            )
            .finish()
    }
}

pub trait SelectedRootChangeProvider {
    fn list_changes_page(
        &self,
        cursor: &ChangeCursor,
        continuation: Option<&ContinuationToken>,
    ) -> Result<ChangePage, SelectedRootExecutorError>;
}

impl SelectedRootChangeProvider for GoogleDriveApi {
    fn list_changes_page(
        &self,
        cursor: &ChangeCursor,
        continuation: Option<&ContinuationToken>,
    ) -> Result<ChangePage, SelectedRootExecutorError> {
        GoogleDriveApi::list_changes_page(self, cursor, continuation)
            .map_err(SelectedRootExecutorError::from)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedRootChangeWindowCollection {
    pub page_count: u64,
    pub change_count: u64,
    pub complete: bool,
}

pub trait SelectedRootBootstrapProvider: SelectedRootProvider {
    fn current_change_cursor(&self) -> Result<ChangeCursor, SelectedRootExecutorError>;

    fn list_children_page(
        &self,
        parent_remote_id: &str,
        continuation: Option<&ContinuationToken>,
    ) -> Result<SelectedRootInventoryPage, SelectedRootExecutorError>;
}

impl SelectedRootBootstrapProvider for GoogleDriveApi {
    fn current_change_cursor(&self) -> Result<ChangeCursor, SelectedRootExecutorError> {
        GoogleDriveApi::current_change_cursor(self).map_err(SelectedRootExecutorError::from)
    }

    fn list_children_page(
        &self,
        parent_remote_id: &str,
        continuation: Option<&ContinuationToken>,
    ) -> Result<SelectedRootInventoryPage, SelectedRootExecutorError> {
        let page =
            GoogleDriveApi::list_folder_children_page(self, parent_remote_id, continuation, 1000)
                .map_err(SelectedRootExecutorError::from)?;

        Ok(SelectedRootInventoryPage::new(
            page.items,
            page.continuation,
            page.unsupported_provider_native,
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedRootBootstrap {
    pub authoritative_items: u64,
    pub folder_pages: u64,
    pub unsupported_provider_native: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedRootBatchExecution {
    pub provider_changes: usize,
    pub storage_mutations: usize,
    pub authoritative_items: u64,
    pub hydrated_items: usize,
    pub root_revalidations: usize,
    pub completed_initial_catchup: bool,
}

pub fn collect_selected_root_change_window_page<P: SelectedRootChangeProvider>(
    provider: &P,
    storage: &mut Storage,
    sync_root: &SyncRoot,
) -> Result<SelectedRootChangeWindowCollection, SelectedRootExecutorError> {
    if let Some(window) = storage.sync_root_change_window_state(&sync_root.id)? {
        if window.is_complete() {
            return Ok(SelectedRootChangeWindowCollection {
                page_count: window.page_count,
                change_count: window.change_count,
                complete: true,
            });
        }

        let page = provider.list_changes_page(&window.base_cursor, window.continuation.as_ref())?;

        let state = storage.stage_sync_root_change_window_page(
            &sync_root.id,
            &window.base_cursor,
            window.continuation.as_ref(),
            &page,
        )?;

        return Ok(SelectedRootChangeWindowCollection {
            page_count: state.page_count,
            change_count: state.change_count,
            complete: state.is_complete(),
        });
    }

    let inventory = storage.sync_root_remote_inventory_state(&sync_root.id)?;
    if !inventory.snapshot_complete {
        return Err(SelectedRootExecutorError::ChangeWindowSnapshotMissing);
    }

    let base_cursor = if inventory.catchup_complete {
        storage
            .sync_root_change_cursor(&sync_root.id)?
            .ok_or(SelectedRootExecutorError::ChangeWindowCursorMissing)?
    } else {
        inventory
            .catchup_from_cursor
            .ok_or(SelectedRootExecutorError::ChangeWindowCursorMissing)?
    };

    let page = provider.list_changes_page(&base_cursor, None)?;
    let state =
        storage.stage_sync_root_change_window_page(&sync_root.id, &base_cursor, None, &page)?;

    Ok(SelectedRootChangeWindowCollection {
        page_count: state.page_count,
        change_count: state.change_count,
        complete: state.is_complete(),
    })
}

pub fn execute_completed_selected_root_change_window<P: SelectedRootProvider>(
    provider: &P,
    storage: &mut Storage,
    sync_root: &SyncRoot,
    observed_at_unix_ms: i64,
) -> Result<SelectedRootBatchExecution, SelectedRootExecutorError> {
    let window = storage
        .sync_root_change_window_state(&sync_root.id)?
        .ok_or(SelectedRootExecutorError::ChangeWindowMissing)?;

    if !window.is_complete() {
        return Err(SelectedRootExecutorError::ChangeWindowIncomplete);
    }

    let checkpoint = window
        .checkpoint
        .as_ref()
        .ok_or(SelectedRootExecutorError::ChangeWindowIncomplete)?
        .clone();

    let changes = storage.sync_root_change_window_changes(&sync_root.id)?;
    let actual_change_count =
        u64::try_from(changes.len()).map_err(|_| SelectedRootExecutorError::CountOverflow)?;

    if actual_change_count != window.change_count {
        return Err(SelectedRootExecutorError::ChangeWindowChangeCountMismatch);
    }

    let configured_remote_root_id = sync_root
        .remote_root_id
        .as_deref()
        .ok_or(SelectedRootExecutorError::MissingRemoteRoot)?;

    let root_identity = provider.resolve_root(configured_remote_root_id)?;
    let canonical_root_id = provider.canonical_root_id(&root_identity).to_owned();

    if canonical_root_id.trim().is_empty() {
        return Err(SelectedRootExecutorError::InvalidCanonicalRoot);
    }

    let durable_items = storage.list_sync_root_remote_items(&sync_root.id)?;
    let mut projection = RootCatalogProjection::new(&canonical_root_id, &durable_items)?;

    let mut mutation_plans = Vec::new();
    let mut hydrated_items = 0_usize;
    let mut root_revalidations = 0_usize;

    for change in &changes {
        let membership =
            resolve_change_membership(provider, &root_identity, &canonical_root_id, change)?;

        let hydration = match change {
            RemoteChange::Upsert(item)
                if membership == RootChangeMembership::Descendant
                    && item.kind == RemoteItemKind::Folder
                    && !item.trashed
                    && !projection.contains(&item.remote_id) =>
            {
                let items = provider.hydrate_folder(item)?;
                hydrated_items = hydrated_items
                    .checked_add(items.len())
                    .ok_or(SelectedRootExecutorError::CountOverflow)?;
                Some(items)
            }
            _ => None,
        };

        match projection.apply_change(change, membership, hydration)? {
            RootCatalogResolution::Noop => {}
            RootCatalogResolution::Mutations(mut planned) => {
                mutation_plans.append(&mut planned);
            }
            RootCatalogResolution::RevalidateRoot => {
                root_revalidations = root_revalidations
                    .checked_add(1)
                    .ok_or(SelectedRootExecutorError::CountOverflow)?;

                let revalidated = provider.resolve_root(configured_remote_root_id)?;
                if provider.canonical_root_id(&revalidated) != canonical_root_id {
                    return Err(SelectedRootExecutorError::RootIdentityChanged);
                }
            }
        }
    }

    projection.validate_complete()?;

    let storage_mutations = mutation_plans
        .into_iter()
        .map(into_storage_mutation)
        .collect::<Vec<_>>();

    let commit = storage.commit_sync_root_catalog_change_window(
        &sync_root.id,
        &window.base_cursor,
        &checkpoint,
        window.change_count,
        &storage_mutations,
        observed_at_unix_ms,
    )?;

    Ok(execution_result(
        changes.len(),
        storage_mutations.len(),
        hydrated_items,
        root_revalidations,
        commit,
    ))
}

pub fn bootstrap_selected_root_snapshot<P: SelectedRootBootstrapProvider>(
    provider: &P,
    storage: &mut Storage,
    sync_root: &SyncRoot,
    observed_at_unix_ms: i64,
) -> Result<SelectedRootBootstrap, SelectedRootExecutorError> {
    let state = storage.sync_root_remote_inventory_state(&sync_root.id)?;
    if state.snapshot_complete {
        return Err(SelectedRootExecutorError::BootstrapSnapshotAlreadyComplete);
    }

    let configured_remote_root_id = sync_root
        .remote_root_id
        .as_deref()
        .ok_or(SelectedRootExecutorError::MissingRemoteRoot)?;

    let root_identity = provider.resolve_root(configured_remote_root_id)?;
    let canonical_root_id = provider.canonical_root_id(&root_identity).to_owned();

    if canonical_root_id.trim().is_empty() {
        return Err(SelectedRootExecutorError::InvalidCanonicalRoot);
    }

    // The fence must be captured before the full subtree inventory begins.
    let fence = provider.current_change_cursor()?;

    storage.begin_sync_root_remote_inventory_staging(&sync_root.id)?;

    let result = (|| {
        let traversal = stage_selected_root_inventory(
            provider,
            storage,
            &sync_root.id,
            &canonical_root_id,
            observed_at_unix_ms,
        )?;

        // Revalidate the configured root before promoting staging. Any root
        // transition after the fence is still caught by the later change feed,
        // but an unavailable or identity-shifted root must not be promoted.
        let revalidated = provider.resolve_root(configured_remote_root_id)?;
        if provider.canonical_root_id(&revalidated) != canonical_root_id {
            return Err(SelectedRootExecutorError::RootIdentityChanged);
        }

        let staged_items = storage.staged_sync_root_remote_inventory_count(&sync_root.id)?;

        if staged_items != traversal.supported_items {
            return Err(SelectedRootExecutorError::BootstrapItemCountMismatch);
        }

        storage.commit_sync_root_remote_inventory_snapshot(
            &sync_root.id,
            &fence,
            observed_at_unix_ms,
        )?;

        Ok(SelectedRootBootstrap {
            authoritative_items: traversal.supported_items,
            folder_pages: traversal.folder_pages,
            unsupported_provider_native: traversal.unsupported_provider_native,
        })
    })();

    if result.is_err() {
        // Staging is non-authoritative. Best-effort cleanup keeps retries tidy;
        // a future retry also clears staging before writing.
        let _ = storage.clear_sync_root_remote_inventory_staging(&sync_root.id);
    }

    result
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BootstrapTraversal {
    supported_items: u64,
    folder_pages: u64,
    unsupported_provider_native: u64,
}

fn stage_selected_root_inventory<P: SelectedRootBootstrapProvider>(
    provider: &P,
    storage: &mut Storage,
    sync_root_id: &str,
    canonical_root_id: &str,
    observed_at_unix_ms: i64,
) -> Result<BootstrapTraversal, SelectedRootExecutorError> {
    let mut folders = VecDeque::from([canonical_root_id.to_owned()]);
    let mut seen_remote_ids = HashSet::from([canonical_root_id.to_owned()]);
    let mut supported_items = 0_u64;
    let mut folder_pages = 0_u64;
    let mut unsupported_provider_native = 0_u64;

    while let Some(parent_remote_id) = folders.pop_front() {
        let mut continuation = None;
        let mut seen_page_tokens = HashSet::new();

        loop {
            folder_pages = folder_pages
                .checked_add(1)
                .ok_or(SelectedRootExecutorError::BootstrapSafetyLimitExceeded)?;
            if folder_pages > 100_000 {
                return Err(SelectedRootExecutorError::BootstrapSafetyLimitExceeded);
            }

            let page = provider.list_children_page(&parent_remote_id, continuation.as_ref())?;
            let (items, next_continuation, unsupported) = page.into_parts();

            unsupported_provider_native = unsupported_provider_native
                .checked_add(unsupported)
                .ok_or(SelectedRootExecutorError::BootstrapSafetyLimitExceeded)?;

            for item in &items {
                validate_bootstrap_item(&parent_remote_id, item, &mut seen_remote_ids)?;

                supported_items = supported_items
                    .checked_add(1)
                    .ok_or(SelectedRootExecutorError::BootstrapSafetyLimitExceeded)?;
                if supported_items > 1_000_000 {
                    return Err(SelectedRootExecutorError::BootstrapSafetyLimitExceeded);
                }

                if item.kind == RemoteItemKind::Folder {
                    folders.push_back(item.remote_id.clone());
                }
            }

            storage.stage_sync_root_remote_inventory_items(
                sync_root_id,
                &items,
                observed_at_unix_ms,
            )?;

            match next_continuation {
                Some(next) => {
                    if !seen_page_tokens.insert(next.as_str().to_owned()) {
                        return Err(SelectedRootExecutorError::BootstrapPaginationLoop);
                    }
                    continuation = Some(next);
                }
                None => break,
            }
        }
    }

    Ok(BootstrapTraversal {
        supported_items,
        folder_pages,
        unsupported_provider_native,
    })
}

fn validate_bootstrap_item(
    expected_parent_remote_id: &str,
    item: &RemoteItem,
    seen_remote_ids: &mut HashSet<String>,
) -> Result<(), SelectedRootExecutorError> {
    if item.remote_id.trim().is_empty() || item.name.is_empty() || item.trashed {
        return Err(SelectedRootExecutorError::BootstrapInvalidItem);
    }

    if item.parent_remote_id.as_deref() != Some(expected_parent_remote_id) {
        return Err(SelectedRootExecutorError::BootstrapParentMismatch);
    }

    if !seen_remote_ids.insert(item.remote_id.clone()) {
        return Err(SelectedRootExecutorError::BootstrapDuplicateRemoteId);
    }

    Ok(())
}

pub fn execute_selected_root_change_batch<P: SelectedRootProvider>(
    provider: &P,
    storage: &mut Storage,
    sync_root: &SyncRoot,
    expected_cursor: &ChangeCursor,
    changes: &[RemoteChange],
    next_cursor: &ChangeCursor,
    observed_at_unix_ms: i64,
) -> Result<SelectedRootBatchExecution, SelectedRootExecutorError> {
    let configured_remote_root_id = sync_root
        .remote_root_id
        .as_deref()
        .ok_or(SelectedRootExecutorError::MissingRemoteRoot)?;

    let root_identity = provider.resolve_root(configured_remote_root_id)?;
    let canonical_root_id = provider.canonical_root_id(&root_identity).to_owned();

    if canonical_root_id.trim().is_empty() {
        return Err(SelectedRootExecutorError::InvalidCanonicalRoot);
    }

    let durable_items = storage.list_sync_root_remote_items(&sync_root.id)?;
    let mut projection = RootCatalogProjection::new(&canonical_root_id, &durable_items)?;

    let mut mutation_plans = Vec::new();
    let mut hydrated_items = 0_usize;
    let mut root_revalidations = 0_usize;

    for change in changes {
        let membership =
            resolve_change_membership(provider, &root_identity, &canonical_root_id, change)?;

        let hydration = match change {
            RemoteChange::Upsert(item)
                if membership == RootChangeMembership::Descendant
                    && item.kind == RemoteItemKind::Folder
                    && !item.trashed
                    && !projection.contains(&item.remote_id) =>
            {
                let items = provider.hydrate_folder(item)?;
                hydrated_items = hydrated_items
                    .checked_add(items.len())
                    .ok_or(SelectedRootExecutorError::CountOverflow)?;
                Some(items)
            }
            _ => None,
        };

        match projection.apply_change(change, membership, hydration)? {
            RootCatalogResolution::Noop => {}
            RootCatalogResolution::Mutations(mut planned) => {
                mutation_plans.append(&mut planned);
            }
            RootCatalogResolution::RevalidateRoot => {
                root_revalidations = root_revalidations
                    .checked_add(1)
                    .ok_or(SelectedRootExecutorError::CountOverflow)?;

                let revalidated = provider.resolve_root(configured_remote_root_id)?;
                if provider.canonical_root_id(&revalidated) != canonical_root_id {
                    return Err(SelectedRootExecutorError::RootIdentityChanged);
                }
            }
        }
    }

    projection.validate_complete()?;

    let storage_mutations = mutation_plans
        .into_iter()
        .map(into_storage_mutation)
        .collect::<Vec<_>>();

    let commit = storage.commit_sync_root_catalog_batch_and_cursor(
        &sync_root.id,
        expected_cursor,
        &storage_mutations,
        next_cursor,
        observed_at_unix_ms,
    )?;

    Ok(execution_result(
        changes.len(),
        storage_mutations.len(),
        hydrated_items,
        root_revalidations,
        commit,
    ))
}

fn resolve_change_membership<P: SelectedRootProvider>(
    provider: &P,
    root_identity: &P::RootIdentity,
    canonical_root_id: &str,
    change: &RemoteChange,
) -> Result<RootChangeMembership, SelectedRootExecutorError> {
    match change {
        RemoteChange::Delete { remote_id } if remote_id == canonical_root_id => {
            Ok(RootChangeMembership::Root)
        }
        RemoteChange::Delete { .. } => Ok(RootChangeMembership::UnresolvedDelete),
        RemoteChange::Upsert(item) => provider.resolve_membership(item, root_identity),
    }
}

fn into_storage_mutation(plan: RootCatalogMutationPlan) -> SyncRootCatalogMutation {
    match plan {
        RootCatalogMutationPlan::Upsert(item) => SyncRootCatalogMutation::Upsert(item),
        RootCatalogMutationPlan::DeleteSubtree { remote_id } => {
            SyncRootCatalogMutation::DeleteSubtree { remote_id }
        }
    }
}

fn execution_result(
    provider_changes: usize,
    storage_mutations: usize,
    hydrated_items: usize,
    root_revalidations: usize,
    commit: SyncRootCatalogBatchCommit,
) -> SelectedRootBatchExecution {
    SelectedRootBatchExecution {
        provider_changes,
        storage_mutations,
        authoritative_items: commit.authoritative_items,
        hydrated_items,
        root_revalidations,
        completed_initial_catchup: commit.completed_initial_catchup,
    }
}

#[derive(Debug, Error)]
pub enum SelectedRootExecutorError {
    #[error("selected sync root has no configured remote root")]
    MissingRemoteRoot,
    #[error("provider returned an invalid canonical root identity")]
    InvalidCanonicalRoot,
    #[error("selected root canonical identity changed during batch execution")]
    RootIdentityChanged,
    #[error("selected-root batch counter overflowed")]
    CountOverflow,
    #[error("local materialization planning supports only receive_only roots")]
    LocalPlanModeUnsupported,
    #[error("local materialization planning requires an authoritative snapshot")]
    LocalPlanSnapshotMissing,
    #[error("local materialization planning requires completed initial catch-up")]
    LocalPlanCatchupIncomplete,
    #[error("local materialization planning requires a durable change cursor")]
    LocalPlanCursorMissing,
    #[error("local materialization planning requires no pending durable change window")]
    LocalPlanChangeWindowPending,
    #[error("local materialization planning catalog count mismatched durable state")]
    LocalPlanCatalogItemCountMismatch,
    #[error("configured local sync root is unavailable")]
    LocalRootUnavailable,
    #[error("configured local sync root cannot be a symbolic link")]
    LocalRootSymlinkUnsupported,
    #[error("configured local sync root is not a directory")]
    LocalRootNotDirectory,
    #[error("configured local sync root canonical identity changed")]
    LocalRootIdentityChanged,
    #[error("local sync tree contains a non-UTF-8 entry")]
    LocalEntryNonUtf8,
    #[error("local sync tree contains a symbolic link")]
    LocalEntrySymlinkUnsupported,
    #[error("local sync tree contains an unsupported filesystem entry type")]
    LocalEntryTypeUnsupported,
    #[error("local sync tree metadata inspection failed")]
    LocalFilesystemInspectionFailed,
    #[error("local sync tree scan exceeded its safety limit")]
    LocalScanSafetyLimitExceeded,
    #[error("local baseline capture supports only receive_only roots")]
    LocalBaselineModeUnsupported,
    #[error("local baseline snapshot is already complete")]
    LocalBaselineAlreadyComplete,
    #[error("local baseline requires zero receive-only convergence actions")]
    LocalBaselineConvergenceNotClean,
    #[error("local baseline filesystem changed while the supervised scan was running")]
    LocalBaselineScanRace,
    #[error("local baseline staged row count mismatched the scanned snapshot")]
    LocalBaselineStagingMismatch,
    #[error("local baseline durable commit failed its postcondition")]
    LocalBaselineCommitMismatch,
    #[error("local filesystem timestamp does not fit durable nanosecond storage")]
    LocalMetadataTimestampOverflow,
    #[error("local baseline scan produced an invalid durable snapshot item")]
    LocalBaselineSnapshotInvalid,
    #[error("local inventory diff supports only receive_only roots")]
    LocalDiffModeUnsupported,
    #[error("local inventory diff requires a durable baseline")]
    LocalDiffBaselineMissing,
    #[error("local inventory baseline was invalidated by a ReceiveOnly filesystem mutation")]
    LocalDiffBaselineInvalidated,
    #[error("local inventory baseline count mismatched durable state")]
    LocalDiffBaselineCountMismatch,
    #[error("local inventory diff scan changed while planning")]
    LocalDiffScanRace,
    #[error("local inventory diff encountered a duplicate relative path")]
    LocalDiffDuplicatePath,
    #[error("remote-write planning requires a durable local baseline")]
    RemoteWritePlanLocalBaselineMissing,
    #[error("remote-write planning requires a valid local observation baseline")]
    RemoteWritePlanLocalBaselineInvalid,
    #[error("remote-write planning local baseline count mismatched durable state")]
    RemoteWritePlanLocalBaselineCountMismatch,
    #[error("remote-write planning local scan changed while planning")]
    RemoteWritePlanLocalScanRace,
    #[error("remote-write planning pending journal does not match the fresh local diff")]
    RemoteWritePlanLocalJournalMismatch,
    #[error("remote-write planning requires a fully caught-up remote catalog")]
    RemoteWritePlanRemoteCatalogNotReady,
    #[error("remote-write planning requires no open remote change window")]
    RemoteWritePlanRemoteChangeWindowPending,
    #[error("remote-write planning requires a durable remote change cursor")]
    RemoteWritePlanRemoteCursorMissing,
    #[error("remote-write planning remote catalog count mismatched durable state")]
    RemoteWritePlanRemoteCatalogCountMismatch,
    #[error("remote-write planning requires a cursor-bound authority snapshot")]
    RemoteWritePlanAuthorityStateMissing,
    #[error("remote-write planning authority cursor is stale")]
    RemoteWritePlanAuthorityCursorMismatch,
    #[error("remote-write planning authority count mismatched the catalog")]
    RemoteWritePlanAuthorityCountMismatch,
    #[error("remote-write planning authority coverage mismatched the catalog")]
    RemoteWritePlanAuthorityCoverageMismatch,
    #[error("remote-write planning remote catalog identity is ambiguous")]
    RemoteWritePlanRemoteCatalogIdentityAmbiguous,
    #[error("remote-write planning ownership receipts are ambiguous")]
    RemoteWritePlanOwnershipAmbiguous,
    #[error("two-way supervised local journal requires a two_way root")]
    TwoWayLocalJournalModeUnsupported,
    #[error("confirmed ordinary-file create settlement requires a two_way root")]
    RemoteWriteFileSettlementModeUnsupported,
    #[error("confirmed ordinary-file create settlement candidate is not eligible")]
    RemoteWriteFileSettlementNotEligible,
    #[error("confirmed ordinary-file create settlement local authority mismatched")]
    RemoteWriteFileSettlementLocalAuthorityMismatch,
    #[error("confirmed ordinary-file create settlement source event mismatched")]
    RemoteWriteFileSettlementSourceEventMismatch,
    #[error("confirmed ordinary-file create settlement content evidence mismatched")]
    RemoteWriteFileSettlementEvidenceMismatch,
    #[error("confirmed ordinary-file create settlement remote catalog mismatched")]
    RemoteWriteFileSettlementRemoteCatalogMismatch,
    #[error("confirmed ordinary-file create settlement uploaded snapshot is invalid")]
    RemoteWriteFileSettlementSnapshotInvalid,
    #[error("current local content is ambiguous against the uploaded file snapshot")]
    RemoteWriteFileSettlementContentMismatch,
    #[error("confirmed folder-create settlement requires a two_way root")]
    RemoteWriteFolderSettlementModeUnsupported,
    #[error("confirmed folder-create settlement candidate is not eligible")]
    RemoteWriteFolderSettlementNotEligible,
    #[error("confirmed folder-create settlement local authority mismatched")]
    RemoteWriteFolderSettlementLocalAuthorityMismatch,
    #[error("confirmed folder-create settlement local identity mismatched")]
    RemoteWriteFolderSettlementLocalIdentityMismatch,
    #[error("confirmed folder-create settlement source diff mismatched")]
    RemoteWriteFolderSettlementSourceDiffMismatch,
    #[error("confirmed folder-create settlement remote catalog mismatched")]
    RemoteWriteFolderSettlementRemoteCatalogMismatch,
    #[error("confirmed folder-create settlement residual diff contains the source")]
    RemoteWriteFolderSettlementResidualMismatch,
    #[error("ordinary-file create local validation requires a two_way root")]
    RemoteWriteFileCreateModeUnsupported,
    #[error("ordinary-file create local identity no longer matches the durable intent")]
    RemoteWriteFileCreateLocalIdentityMismatch,
    #[error("zero-byte ordinary-file create is not enabled in this resumable phase")]
    RemoteWriteFileCreateEmptyUnsupported,
    #[error("ordinary-file create chunk size is invalid")]
    RemoteWriteFileCreateChunkSizeInvalid,
    #[error("ordinary-file create content read failed")]
    RemoteWriteFileCreateReadFailed,
    #[error("ordinary-file create content stream is incomplete")]
    RemoteWriteFileCreateStreamIncomplete,
    #[error("folder-create local validation requires a two_way root")]
    RemoteWriteFolderCreateModeUnsupported,
    #[error("folder-create local directory identity no longer matches the durable intent")]
    RemoteWriteFolderCreateLocalIdentityMismatch,
    #[error("remote-write plan entry is not eligible for a create intent")]
    RemoteWriteCreateIntentNotEligible,
    #[error("local directory materialization is blocked by local-only entries or type conflicts")]
    LocalDirectoryPhaseBlocked,
    #[error("local directory target count mismatched the remote directory plan")]
    LocalDirectoryTargetCountMismatch,
    #[error("local directory target conflicts with an existing filesystem entry")]
    LocalDirectoryTargetConflict,
    #[error("local directory target escaped the configured sync root")]
    LocalDirectoryTargetEscapedRoot,
    #[error("local directory parent is unavailable, unsafe, or outside the sync root")]
    LocalDirectoryParentInvalid,
    #[error("local directory creation failed")]
    LocalDirectoryCreateFailed,
    #[error("local directory materialization postcondition failed")]
    LocalDirectoryPostconditionFailed,
    #[error("local directory materialization rollback failed")]
    LocalDirectoryRollbackFailed,
    #[error("directory materialization receipt failed its durable postcondition")]
    DirectoryReceiptPostconditionFailed,
    #[error("directory adoption is blocked by the current receive-only state")]
    DirectoryAdoptionPhaseBlocked,
    #[error("directory adoption requires a clean receipt state")]
    DirectoryAdoptionReceiptStateConflict,
    #[error("directory adoption target does not match the authoritative remote catalog")]
    DirectoryAdoptionTargetMismatch,
    #[error("directory adoption target must be empty")]
    DirectoryAdoptionTargetNotEmpty,
    #[error("local file materialization preconditions are not satisfied")]
    LocalFilePhaseBlocked,
    #[error("local file materialization target count did not equal one")]
    LocalFileTargetCountMismatch,
    #[error("local file materialization requires a known remote byte size")]
    LocalFileSizeUnknown,
    #[error("local file exceeds the supervised download byte limit")]
    LocalFileTooLarge,
    #[error("local file target escaped the configured sync root")]
    LocalFileTargetEscapedRoot,
    #[error("local file target appeared before no-overwrite promotion")]
    LocalFileTargetConflict,
    #[error("local file parent is unavailable, unsafe, or outside the sync root")]
    LocalFileParentInvalid,
    #[error("local file temporary download creation failed")]
    LocalFileTempCreateFailed,
    #[error("downloaded byte count did not match durable remote metadata")]
    LocalFileDownloadSizeMismatch,
    #[error("local file durability sync failed")]
    LocalFileSyncFailed,
    #[error("local file atomic no-overwrite promotion failed")]
    LocalFilePromoteFailed,
    #[error("local file temporary download cleanup failed")]
    LocalFileTempCleanupFailed,
    #[error("local file materialization postcondition failed")]
    LocalFilePostconditionFailed,
    #[error("local file materialization rollback failed")]
    LocalFileRollbackFailed,
    #[error("provider byte count disagreed with the bytes hashed locally")]
    LocalFileProviderByteCountMismatch,
    #[error("receive-only supervised cycle supports only receive_only roots")]
    ReceiveOnlyCycleModeUnsupported,
    #[error("receive-only single-flight execution state is unavailable")]
    ReceiveOnlySingleFlightStatePoisoned,
    #[error("cross-process execution lock path is invalid")]
    CrossProcessExecutionLockPathInvalid,
    #[error("cross-process execution lock I/O failed")]
    CrossProcessExecutionLockIo,
    #[error("unified convergence is blocked by fail-closed planner actions")]
    UnifiedConvergenceBlocked,
    #[error("unified convergence requires a readonly content provider for this phase")]
    UnifiedConvergenceProviderRequired,
    #[error("unified convergence delegated phase count did not match the planner")]
    UnifiedConvergencePhaseCountMismatch,
    #[error("local file verification is blocked by the current receive-only state")]
    LocalFileVerificationPhaseBlocked,
    #[error("bounded existing-file verification is not ready for supervised execution")]
    LocalFileVerificationBatchNotReady,
    #[error("bounded existing-file verification targets do not match convergence authority")]
    LocalFileVerificationBatchTargetMismatch,
    #[error("existing local verification target changed during supervised verification")]
    LocalFileVerificationTargetRace,
    #[error("bounded existing-file verification failed its durable postcondition")]
    LocalFileVerificationBatchPostconditionFailed,
    #[error("local file verification failed")]
    LocalFileVerifyFailed,
    #[error("local file SHA-256 does not match current remote content")]
    LocalFileHashMismatch,
    #[error("system clock is before the Unix epoch")]
    ClockBeforeUnixEpoch,
    #[error("local receipt verification exceeded its safety limit")]
    LocalReceiptVerificationSafetyLimitExceeded,
    #[error("local receipt verification durable count did not match loaded receipts")]
    LocalReceiptVerificationCountMismatch,
    #[error("local materialization receipt path is invalid or escaped the root")]
    LocalReceiptPathInvalid,
    #[error("remote replacement planning is blocked by the current receive-only state")]
    RemoteReplacementPlanPhaseBlocked,
    #[error("remote replacement planning requires no current receipt")]
    RemoteReplacementPlanCurrentReceiptPresent,
    #[error("remote replacement planning requires exactly one stale receipt")]
    RemoteReplacementPlanStaleReceiptCountMismatch,
    #[error("remote replacement planning target does not match the stale baseline")]
    RemoteReplacementPlanTargetMismatch,
    #[error("bounded stale replacement is not ready for safe supervised execution")]
    RemoteReplacementBatchNotReady,
    #[error("remote replacement is not ready because the stale local baseline is not clean")]
    RemoteReplacementNotReady,
    #[error("provider fingerprint changed or did not match downloaded content")]
    RemoteReplacementProviderFingerprintMismatch,
    #[error("local file diverged from the stale materialization baseline")]
    RemoteReplacementLocalConflict,
    #[error("local replacement target changed while it was being revalidated")]
    RemoteReplacementTargetRace,
    #[error("atomic replacement of the existing local file failed")]
    LocalFileReplaceFailed,
    #[error("unable to allocate a same-parent replacement backup path")]
    LocalFileReplacementBackupUnavailable,
    #[error("failed to create the same-parent replacement backup")]
    LocalFileReplacementBackupCreateFailed,
    #[error("failed to roll back a bounded stale replacement")]
    LocalFileReplacementRollbackFailed,
    #[error("failed to clean a committed stale replacement backup")]
    LocalFileReplacementBackupCleanupFailed,
    #[error("replaced local file failed post-promotion verification")]
    LocalFileReplacementPostconditionFailed,
    #[error("bounded stale replacement failed its final convergence postcondition")]
    LocalFileReplacementBatchPostconditionFailed,
    #[error("replacement receipt state failed its durable postcondition")]
    LocalFileReceiptPostconditionFailed,
    #[error("remote directory deletion planning is blocked by the current receive-only state")]
    RemoteDirectoryDeletionPlanPhaseBlocked,
    #[error(
        "remote directory deletion is not ready because the stale local directory is not clean"
    )]
    RemoteDirectoryDeletionNotReady,
    #[error("local directory deletion target changed while it was being revalidated")]
    RemoteDirectoryDeletionTargetRace,
    #[error("unable to allocate a same-parent directory deletion quarantine path")]
    LocalDirectoryDeletionQuarantineUnavailable,
    #[error("failed to atomically quarantine the local directory deletion target")]
    LocalDirectoryDeletionRenameFailed,
    #[error("failed to remove the quarantined local directory")]
    LocalDirectoryDeletionRemoveFailed,
    #[error("failed to roll back the quarantined local directory")]
    LocalDirectoryDeletionRollbackFailed,
    #[error("local directory deletion failed its postcondition")]
    LocalDirectoryDeletionPostconditionFailed,
    #[error("stale directory receipt cleanup failed after local deletion")]
    LocalDirectoryDeletionReceiptCleanupFailed,
    #[error("bounded stale directory deletion is not ready for safe supervised execution")]
    RemoteDirectoryDeletionBatchNotReady,
    #[error("bounded stale directory deletion targets do not match durable stale receipts")]
    RemoteDirectoryDeletionBatchTargetMismatch,
    #[error("bounded stale directory deletion quarantine cleanup failed")]
    LocalDirectoryDeletionBatchCleanupFailed,
    #[error("bounded stale directory deletion failed its final convergence postcondition")]
    LocalDirectoryDeletionBatchPostconditionFailed,
    #[error("remote directory deletion planning requires no file receipt")]
    RemoteDirectoryDeletionPlanFileReceiptPresent,
    #[error("remote directory deletion planning requires no current directory receipt")]
    RemoteDirectoryDeletionPlanCurrentReceiptPresent,
    #[error("remote directory deletion planning requires exactly one stale directory receipt")]
    RemoteDirectoryDeletionPlanStaleReceiptCountMismatch,
    #[error("remote directory deletion planning found the stale remote identity still present")]
    RemoteDirectoryDeletionPlanRemoteIdentityStillPresent,
    #[error("remote deletion planning is blocked by the current receive-only state")]
    RemoteDeletionPlanPhaseBlocked,
    #[error("remote deletion planning requires no current receipt")]
    RemoteDeletionPlanCurrentReceiptPresent,
    #[error("remote deletion planning requires exactly one stale receipt")]
    RemoteDeletionPlanStaleReceiptCountMismatch,
    #[error("remote deletion planning found the stale remote identity still present")]
    RemoteDeletionPlanRemoteIdentityStillPresent,
    #[error("bounded stale deletion is not ready for safe supervised execution")]
    RemoteDeletionBatchNotReady,
    #[error("remote deletion is not ready because the stale local baseline is not clean")]
    RemoteDeletionNotReady,
    #[error("local file diverged from the stale deletion baseline")]
    RemoteDeletionLocalConflict,
    #[error("local deletion target changed while it was being revalidated")]
    RemoteDeletionTargetRace,
    #[error("unable to allocate a same-parent deletion quarantine path")]
    LocalFileDeletionQuarantineUnavailable,
    #[error("failed to atomically quarantine the local deletion target")]
    LocalFileDeletionRenameFailed,
    #[error("failed to remove the quarantined local file")]
    LocalFileDeletionRemoveFailed,
    #[error("failed to roll back the quarantined local file")]
    LocalFileDeletionRollbackFailed,
    #[error("local file deletion failed its postcondition")]
    LocalFileDeletionPostconditionFailed,
    #[error("bounded stale deletion quarantine cleanup failed")]
    LocalFileDeletionBatchCleanupFailed,
    #[error("bounded stale deletion failed its final convergence postcondition")]
    LocalFileDeletionBatchPostconditionFailed,
    #[error("stale materialization receipt cleanup failed after local deletion")]
    LocalFileDeletionReceiptCleanupFailed,
    #[error("selected-root provider operation failed")]
    ProviderOperationFailed,
    #[error("selected-root change window requires an authoritative snapshot")]
    ChangeWindowSnapshotMissing,
    #[error("selected-root change window durable cursor is missing")]
    ChangeWindowCursorMissing,
    #[error("selected-root completed change window is missing")]
    ChangeWindowMissing,
    #[error("selected-root change window is not complete")]
    ChangeWindowIncomplete,
    #[error("selected-root durable change count does not match its state")]
    ChangeWindowChangeCountMismatch,
    #[error("selected-root authoritative snapshot is already complete")]
    BootstrapSnapshotAlreadyComplete,
    #[error("selected-root bootstrap returned invalid item metadata")]
    BootstrapInvalidItem,
    #[error("selected-root bootstrap child parent does not match traversal context")]
    BootstrapParentMismatch,
    #[error("selected-root bootstrap returned a duplicate remote identifier")]
    BootstrapDuplicateRemoteId,
    #[error("selected-root bootstrap pagination token repeated")]
    BootstrapPaginationLoop,
    #[error("selected-root bootstrap exceeded a safety limit")]
    BootstrapSafetyLimitExceeded,
    #[error("selected-root bootstrap authoritative item count mismatched traversal")]
    BootstrapItemCountMismatch,
    #[error("Drive provider operation failed")]
    Drive(Box<DriveApiError>),
    #[error("selected-root storage operation failed")]
    Storage(Box<StorageError>),
    #[error("selected-root catalog projection failed: {0:?}")]
    Projection(RootCatalogProjectionError),
    #[error("receive-only local materialization planning failed: {0:?}")]
    MaterializationPlan(ReceiveOnlyMaterializationPlanError),
    #[error("receive-only convergence planning failed: {0:?}")]
    ConvergencePlan(ReceiveOnlyConvergencePlanError),
}

impl From<DriveApiError> for SelectedRootExecutorError {
    fn from(error: DriveApiError) -> Self {
        Self::Drive(Box::new(error))
    }
}

impl From<StorageError> for SelectedRootExecutorError {
    fn from(error: StorageError) -> Self {
        Self::Storage(Box::new(error))
    }
}

impl From<ReceiveOnlyConvergencePlanError> for SelectedRootExecutorError {
    fn from(error: ReceiveOnlyConvergencePlanError) -> Self {
        Self::ConvergencePlan(error)
    }
}

impl From<RootCatalogProjectionError> for SelectedRootExecutorError {
    fn from(error: RootCatalogProjectionError) -> Self {
        Self::Projection(error)
    }
}

impl From<ReceiveOnlyMaterializationPlanError> for SelectedRootExecutorError {
    fn from(error: ReceiveOnlyMaterializationPlanError) -> Self {
        Self::MaterializationPlan(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nubisync_core::{ProviderAccount, ProviderId, SyncMode};
    use std::{
        cell::{Cell, RefCell},
        collections::{HashMap, VecDeque},
    };

    fn local_plan_temp_dir(label: &str) -> PathBuf {
        let unique = format!(
            "nubisync-local-plan-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        );
        std::env::temp_dir().join(unique)
    }

    fn local_plan_test_root(path: &std::path::Path) -> SyncRoot {
        SyncRoot::new(
            "local-plan-root",
            ProviderId::new("google-drive").unwrap(),
            "subject",
            std::fs::canonicalize(path)
                .unwrap()
                .into_os_string()
                .into_string()
                .unwrap(),
            Some("remote-root".into()),
            SyncMode::ReceiveOnly,
            1,
        )
        .unwrap()
    }

    #[test]
    fn local_scan_collects_metadata_without_exposing_paths() {
        let root = local_plan_temp_dir("scan");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(root.join("folder")).unwrap();
        std::fs::write(root.join("file.txt"), b"content-not-read-by-scan").unwrap();

        let sync_root = local_plan_test_root(&root);
        let entries = scan_selected_root_local_tree(&sync_root).unwrap();

        assert_eq!(entries.len(), 2);
        let debug = format!("{entries:?}");
        assert!(!debug.contains("file.txt"));
        assert!(!debug.contains("folder"));

        std::fs::remove_file(root.join("file.txt")).unwrap();
        std::fs::remove_dir(root.join("folder")).unwrap();
        std::fs::remove_dir(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn local_scan_rejects_symlink_entries() {
        use std::os::unix::fs::symlink;

        let root = local_plan_temp_dir("symlink");
        let outside = local_plan_temp_dir("outside");

        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(&outside).unwrap();
        symlink(&outside, root.join("link")).unwrap();

        let sync_root = local_plan_test_root(&root);

        assert!(matches!(
            scan_selected_root_local_tree(&sync_root),
            Err(SelectedRootExecutorError::LocalEntrySymlinkUnsupported)
        ));

        std::fs::remove_file(root.join("link")).unwrap();
        std::fs::remove_dir(root).unwrap();
        std::fs::remove_dir(outside).unwrap();
    }

    fn directory_materialization_item(
        remote_id: &str,
        parent_remote_id: &str,
        name: &str,
        kind: RemoteItemKind,
    ) -> RemoteItem {
        RemoteItem {
            remote_id: remote_id.into(),
            parent_remote_id: Some(parent_remote_id.into()),
            name: name.into(),
            kind,
            size_bytes: None,
            modified_unix_ms: None,
            trashed: false,
        }
    }

    #[test]
    fn directory_targets_create_parent_before_child_and_are_idempotent() {
        let root = local_plan_temp_dir("materialize-directories");
        std::fs::create_dir(&root).unwrap();
        let root = std::fs::canonicalize(root).unwrap();

        let remote_items = vec![
            directory_materialization_item(
                "folder",
                "selected-root",
                "docs",
                RemoteItemKind::Folder,
            ),
            directory_materialization_item("nested", "folder", "nested", RemoteItemKind::Folder),
        ];
        let targets = plan_receive_only_directory_targets(&remote_items).unwrap();

        let first = apply_selected_root_directory_targets(&root, &targets).unwrap();
        assert_eq!(first.created_paths.len(), 2);
        assert_eq!(first.existing_directories, 0);
        assert!(root.join("docs").is_dir());
        assert!(root.join("docs/nested").is_dir());

        let second = apply_selected_root_directory_targets(&root, &targets).unwrap();
        assert_eq!(second.created_paths.len(), 0);
        assert_eq!(second.existing_directories, 2);

        std::fs::remove_dir(root.join("docs/nested")).unwrap();
        std::fs::remove_dir(root.join("docs")).unwrap();
        std::fs::remove_dir(root).unwrap();
    }

    #[test]
    fn directory_target_failure_rolls_back_only_directories_created_by_run() {
        let root = local_plan_temp_dir("materialize-rollback");
        std::fs::create_dir(&root).unwrap();
        let root = std::fs::canonicalize(root).unwrap();
        std::fs::write(root.join("second"), b"existing-local-file").unwrap();

        let remote_items = vec![
            directory_materialization_item(
                "first",
                "selected-root",
                "first",
                RemoteItemKind::Folder,
            ),
            directory_materialization_item(
                "second",
                "selected-root",
                "second",
                RemoteItemKind::Folder,
            ),
        ];
        let targets = plan_receive_only_directory_targets(&remote_items).unwrap();

        assert!(matches!(
            apply_selected_root_directory_targets(&root, &targets),
            Err(SelectedRootExecutorError::LocalDirectoryTargetConflict)
        ));
        assert!(!root.join("first").exists());
        assert!(root.join("second").is_file());

        std::fs::remove_file(root.join("second")).unwrap();
        std::fs::remove_dir(root).unwrap();
    }

    #[test]
    fn receipt_target_inspection_detects_file_missing_and_type_conflict() {
        let root = local_plan_temp_dir("receipt-target-inspection");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(root.join("docs")).unwrap();
        std::fs::write(root.join("docs/file.txt"), b"hello").unwrap();
        let root = std::fs::canonicalize(root).unwrap();

        let receipt = nubisync_storage::SyncRootFileMaterializationReceipt {
            remote_id: "remote".into(),
            relative_path: "docs/file.txt".into(),
            size_bytes: 5,
            sha256_hex: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824".into(),
            materialized_at_unix_ms: 1,
        };

        match inspect_receipt_target(&root, &receipt).unwrap() {
            ReceiptTargetInspection::File(_, size) => assert_eq!(size, 5),
            _ => panic!("expected file"),
        }

        std::fs::remove_file(root.join("docs/file.txt")).unwrap();
        assert!(matches!(
            inspect_receipt_target(&root, &receipt).unwrap(),
            ReceiptTargetInspection::Missing
        ));

        std::fs::create_dir(root.join("docs/file.txt")).unwrap();
        assert!(matches!(
            inspect_receipt_target(&root, &receipt).unwrap(),
            ReceiptTargetInspection::Conflict
        ));

        std::fs::remove_dir(root.join("docs/file.txt")).unwrap();
        std::fs::remove_dir(root.join("docs")).unwrap();
        std::fs::remove_dir(root).unwrap();
    }

    #[test]
    fn local_file_state_detects_in_place_change() {
        let root = local_plan_temp_dir("replacement-state");
        std::fs::create_dir(&root).unwrap();
        let path = root.join("file.txt");
        std::fs::write(&path, b"hello").unwrap();

        let before = std::fs::symlink_metadata(&path).unwrap();
        std::fs::write(&path, b"HELLO!").unwrap();
        let after = std::fs::symlink_metadata(&path).unwrap();

        assert!(!same_local_file_state(&before, &after));

        std::fs::remove_file(path).unwrap();
        std::fs::remove_dir(root).unwrap();
    }

    #[test]
    fn verified_stale_file_deletion_removes_only_matching_baseline() {
        let root = local_plan_temp_dir("safe-delete");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(root.join("docs")).unwrap();
        std::fs::write(root.join("docs/file.txt"), b"hello").unwrap();
        let root = std::fs::canonicalize(root).unwrap();

        let receipt = SyncRootFileMaterializationReceipt {
            remote_id: "remote".into(),
            relative_path: "docs/file.txt".into(),
            size_bytes: 5,
            sha256_hex: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824".into(),
            materialized_at_unix_ms: 1,
        };

        assert_eq!(
            delete_verified_stale_local_file(&root, &receipt).unwrap(),
            5
        );
        assert!(!root.join("docs/file.txt").exists());

        std::fs::write(root.join("docs/file.txt"), b"HELLO").unwrap();
        assert!(matches!(
            delete_verified_stale_local_file(&root, &receipt),
            Err(SelectedRootExecutorError::RemoteDeletionLocalConflict)
        ));
        assert_eq!(std::fs::read(root.join("docs/file.txt")).unwrap(), b"HELLO");

        std::fs::remove_file(root.join("docs/file.txt")).unwrap();
        std::fs::remove_dir(root.join("docs")).unwrap();
        std::fs::remove_dir(root).unwrap();
    }

    struct FakeContentProvider {
        bytes: Vec<u8>,
    }
    impl SelectedRootContentProvider for FakeContentProvider {
        fn content_fingerprint(
            &self,
            _remote_id: &str,
        ) -> Result<SelectedRootContentFingerprint, SelectedRootExecutorError> {
            let mut hasher = Sha256::new();
            hasher.update(&self.bytes);
            Ok(SelectedRootContentFingerprint {
                size_bytes: u64::try_from(self.bytes.len())
                    .map_err(|_| SelectedRootExecutorError::ProviderOperationFailed)?,
                sha256_hex: digest_to_hex(hasher.finalize().as_slice()),
            })
        }

        fn download_file_content(
            &self,
            _remote_id: &str,
            max_bytes: u64,
            writer: &mut dyn Write,
        ) -> Result<u64, SelectedRootExecutorError> {
            let len = u64::try_from(self.bytes.len())
                .map_err(|_| SelectedRootExecutorError::ProviderOperationFailed)?;
            if len > max_bytes {
                return Err(SelectedRootExecutorError::ProviderOperationFailed);
            }
            writer
                .write_all(&self.bytes)
                .map_err(|_| SelectedRootExecutorError::ProviderOperationFailed)?;
            Ok(len)
        }
    }

    fn file_target_for_test(size: u64) -> ReceiveOnlyFileTarget {
        let folder = directory_materialization_item(
            "folder",
            "selected-root",
            "docs",
            RemoteItemKind::Folder,
        );
        let mut file = directory_materialization_item(
            "file-secret",
            "folder",
            "download.txt",
            RemoteItemKind::File,
        );
        file.size_bytes = Some(size);
        let local = vec![LocalTreeEntry::new("docs", LocalTreeEntryKind::Directory).unwrap()];
        plan_receive_only_missing_file_targets(&[folder, file], &local)
            .unwrap()
            .remove(0)
    }

    #[test]
    fn file_materialization_is_no_overwrite_and_size_checked() {
        let root = local_plan_temp_dir("file-download");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(root.join("docs")).unwrap();
        let root = std::fs::canonicalize(root).unwrap();
        let target = file_target_for_test(5);
        let provider = FakeContentProvider {
            bytes: b"hello".to_vec(),
        };
        let out = apply_selected_root_file_target(&provider, &root, &target, 1024).unwrap();
        assert_eq!(out.bytes_downloaded, 5);
        assert_eq!(
            std::fs::read(root.join("docs/download.txt")).unwrap(),
            b"hello"
        );
        std::fs::remove_file(root.join("docs/download.txt")).unwrap();

        std::fs::write(root.join("docs/download.txt"), b"local").unwrap();
        assert!(matches!(
            apply_selected_root_file_target(&provider, &root, &target, 1024),
            Err(SelectedRootExecutorError::LocalFileTargetConflict)
        ));
        assert_eq!(
            std::fs::read(root.join("docs/download.txt")).unwrap(),
            b"local"
        );
        std::fs::remove_file(root.join("docs/download.txt")).unwrap();

        let target = file_target_for_test(6);
        assert!(matches!(
            apply_selected_root_file_target(&provider, &root, &target, 1024),
            Err(SelectedRootExecutorError::LocalFileDownloadSizeMismatch)
        ));
        assert!(!root.join("docs/download.txt").exists());
        std::fs::remove_dir(root.join("docs")).unwrap();
        std::fs::remove_dir(root).unwrap();
    }

    struct FakeProvider {
        canonical_root_id: String,
        hydrations: HashMap<String, Vec<RemoteItem>>,
        fail_revalidation: bool,
        root_resolutions: Cell<usize>,
    }

    impl FakeProvider {
        fn new(canonical_root_id: &str) -> Self {
            Self {
                canonical_root_id: canonical_root_id.into(),
                hydrations: HashMap::new(),
                fail_revalidation: false,
                root_resolutions: Cell::new(0),
            }
        }
    }

    impl SelectedRootProvider for FakeProvider {
        type RootIdentity = String;

        fn resolve_root(
            &self,
            _remote_root_id: &str,
        ) -> Result<Self::RootIdentity, SelectedRootExecutorError> {
            let call = self.root_resolutions.get() + 1;
            self.root_resolutions.set(call);

            if self.fail_revalidation && call > 1 {
                return Err(SelectedRootExecutorError::ProviderOperationFailed);
            }

            Ok(self.canonical_root_id.clone())
        }

        fn canonical_root_id<'a>(&self, root: &'a Self::RootIdentity) -> &'a str {
            root
        }

        fn resolve_membership(
            &self,
            item: &RemoteItem,
            root: &Self::RootIdentity,
        ) -> Result<RootChangeMembership, SelectedRootExecutorError> {
            if item.remote_id == *root {
                Ok(RootChangeMembership::Root)
            } else if item.parent_remote_id.as_deref() == Some("outside") {
                Ok(RootChangeMembership::Outside)
            } else {
                Ok(RootChangeMembership::Descendant)
            }
        }

        fn hydrate_folder(
            &self,
            item: &RemoteItem,
        ) -> Result<Vec<RemoteItem>, SelectedRootExecutorError> {
            self.hydrations
                .get(&item.remote_id)
                .cloned()
                .ok_or(SelectedRootExecutorError::ProviderOperationFailed)
        }
    }

    fn item(remote_id: &str, parent_remote_id: &str, kind: RemoteItemKind) -> RemoteItem {
        RemoteItem {
            remote_id: remote_id.into(),
            parent_remote_id: Some(parent_remote_id.into()),
            name: format!("{remote_id}.item"),
            kind,
            size_bytes: Some(10),
            modified_unix_ms: None,
            trashed: false,
        }
    }

    fn storage_with_uninitialized_root() -> (Storage, SyncRoot) {
        let storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = ProviderAccount::new(provider.clone(), "subject", None, None).unwrap();

        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "root-one",
            provider,
            account.subject,
            "/tmp/root-one",
            Some("configured-root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();

        storage.insert_sync_root(&root).unwrap();
        (storage, root)
    }

    fn storage_with_root(baseline: &[RemoteItem], fence: &str) -> (Storage, SyncRoot) {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = ProviderAccount::new(provider.clone(), "subject", None, None).unwrap();

        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "root-one",
            provider,
            account.subject,
            "/tmp/root-one",
            Some("configured-root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();

        storage.insert_sync_root(&root).unwrap();
        storage
            .begin_sync_root_remote_inventory_staging(&root.id)
            .unwrap();
        storage
            .stage_sync_root_remote_inventory_items(&root.id, baseline, 3)
            .unwrap();
        storage
            .commit_sync_root_remote_inventory_snapshot(
                &root.id,
                &ChangeCursor::new(fence).unwrap(),
                4,
            )
            .unwrap();

        (storage, root)
    }

    struct FakeChangeProvider {
        pages: RefCell<VecDeque<Result<ChangePage, SelectedRootExecutorError>>>,
        calls: Cell<usize>,
    }

    impl FakeChangeProvider {
        fn new(pages: Vec<Result<ChangePage, SelectedRootExecutorError>>) -> Self {
            Self {
                pages: RefCell::new(VecDeque::from(pages)),
                calls: Cell::new(0),
            }
        }
    }

    impl SelectedRootChangeProvider for FakeChangeProvider {
        fn list_changes_page(
            &self,
            _cursor: &ChangeCursor,
            _continuation: Option<&ContinuationToken>,
        ) -> Result<ChangePage, SelectedRootExecutorError> {
            self.calls.set(self.calls.get().saturating_add(1));
            self.pages
                .borrow_mut()
                .pop_front()
                .unwrap_or(Err(SelectedRootExecutorError::ProviderOperationFailed))
        }
    }

    #[test]
    fn change_window_collection_resumes_one_durable_page_at_a_time() {
        let (mut storage, root) = storage_with_root(&[], "fence");

        let first = item("first", "canonical-root", RemoteItemKind::File);
        let provider = FakeChangeProvider::new(vec![
            Ok(ChangePage {
                changes: vec![RemoteChange::Upsert(first.clone())],
                continuation: Some(ContinuationToken::new("page-two").unwrap()),
                checkpoint: None,
            }),
            Ok(ChangePage {
                changes: vec![RemoteChange::Delete {
                    remote_id: first.remote_id.clone(),
                }],
                continuation: None,
                checkpoint: Some(ChangeCursor::new("checkpoint").unwrap()),
            }),
        ]);

        let first_result =
            collect_selected_root_change_window_page(&provider, &mut storage, &root).unwrap();
        assert_eq!(
            first_result,
            SelectedRootChangeWindowCollection {
                page_count: 1,
                change_count: 1,
                complete: false,
            }
        );

        let durable = storage
            .sync_root_change_window_state(&root.id)
            .unwrap()
            .unwrap();
        assert_eq!(durable.page_count, 1);
        assert_eq!(durable.continuation.as_ref().unwrap().as_str(), "page-two");

        let second_result =
            collect_selected_root_change_window_page(&provider, &mut storage, &root).unwrap();
        assert_eq!(
            second_result,
            SelectedRootChangeWindowCollection {
                page_count: 2,
                change_count: 2,
                complete: true,
            }
        );
        assert_eq!(provider.calls.get(), 2);

        let third_result =
            collect_selected_root_change_window_page(&provider, &mut storage, &root).unwrap();
        assert_eq!(third_result, second_result);
        assert_eq!(
            provider.calls.get(),
            2,
            "complete windows must not issue another provider request"
        );

        assert_eq!(
            storage.sync_root_change_window_changes(&root.id).unwrap(),
            vec![
                RemoteChange::Upsert(first.clone()),
                RemoteChange::Delete {
                    remote_id: first.remote_id,
                },
            ]
        );
        assert!(storage.sync_root_change_cursor(&root.id).unwrap().is_none());
        assert!(
            !storage
                .sync_root_remote_inventory_state(&root.id)
                .unwrap()
                .catchup_complete
        );
    }

    #[test]
    fn change_window_provider_failure_preserves_resume_position() {
        let (mut storage, root) = storage_with_root(&[], "fence");

        let provider = FakeChangeProvider::new(vec![
            Ok(ChangePage {
                changes: vec![],
                continuation: Some(ContinuationToken::new("page-two").unwrap()),
                checkpoint: None,
            }),
            Err(SelectedRootExecutorError::ProviderOperationFailed),
        ]);

        collect_selected_root_change_window_page(&provider, &mut storage, &root).unwrap();

        let error =
            collect_selected_root_change_window_page(&provider, &mut storage, &root).unwrap_err();
        assert!(matches!(
            error,
            SelectedRootExecutorError::ProviderOperationFailed
        ));

        let durable = storage
            .sync_root_change_window_state(&root.id)
            .unwrap()
            .unwrap();
        assert_eq!(durable.page_count, 1);
        assert_eq!(durable.change_count, 0);
        assert_eq!(durable.continuation.as_ref().unwrap().as_str(), "page-two");
        assert!(!durable.is_complete());
    }

    #[test]
    fn change_window_collection_requires_snapshot() {
        let (mut storage, root) = storage_with_uninitialized_root();
        let provider = FakeChangeProvider::new(Vec::new());

        let error =
            collect_selected_root_change_window_page(&provider, &mut storage, &root).unwrap_err();

        assert!(matches!(
            error,
            SelectedRootExecutorError::ChangeWindowSnapshotMissing
        ));
        assert_eq!(provider.calls.get(), 0);
    }

    #[test]
    fn completed_change_window_executes_and_disappears_atomically() {
        let (mut storage, root) = storage_with_root(&[], "fence");
        let provider = FakeProvider::new("canonical-root");

        let file = item("file", "canonical-root", RemoteItemKind::File);
        let page = ChangePage {
            changes: vec![RemoteChange::Upsert(file.clone())],
            continuation: None,
            checkpoint: Some(ChangeCursor::new("checkpoint").unwrap()),
        };
        storage
            .stage_sync_root_change_window_page(
                &root.id,
                &ChangeCursor::new("fence").unwrap(),
                None,
                &page,
            )
            .unwrap();

        let result =
            execute_completed_selected_root_change_window(&provider, &mut storage, &root, 20)
                .unwrap();

        assert_eq!(
            result,
            SelectedRootBatchExecution {
                provider_changes: 1,
                storage_mutations: 1,
                authoritative_items: 1,
                hydrated_items: 0,
                root_revalidations: 0,
                completed_initial_catchup: true,
            }
        );
        assert_eq!(
            storage.list_sync_root_remote_items(&root.id).unwrap(),
            vec![file]
        );
        assert_eq!(
            storage
                .sync_root_change_cursor(&root.id)
                .unwrap()
                .unwrap()
                .as_str(),
            "checkpoint"
        );
        assert!(
            storage
                .sync_root_change_window_state(&root.id)
                .unwrap()
                .is_none()
        );
        assert!(
            storage
                .sync_root_change_window_changes(&root.id)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn incomplete_change_window_is_not_executed() {
        let (mut storage, root) = storage_with_root(&[], "fence");
        let provider = FakeProvider::new("canonical-root");

        let page = ChangePage {
            changes: vec![],
            continuation: Some(ContinuationToken::new("page-two").unwrap()),
            checkpoint: None,
        };
        storage
            .stage_sync_root_change_window_page(
                &root.id,
                &ChangeCursor::new("fence").unwrap(),
                None,
                &page,
            )
            .unwrap();

        let error =
            execute_completed_selected_root_change_window(&provider, &mut storage, &root, 20)
                .unwrap_err();

        assert!(matches!(
            error,
            SelectedRootExecutorError::ChangeWindowIncomplete
        ));
        assert_eq!(provider.root_resolutions.get(), 0);
        assert!(
            storage
                .sync_root_change_window_state(&root.id)
                .unwrap()
                .is_some()
        );
        assert!(storage.sync_root_change_cursor(&root.id).unwrap().is_none());
    }

    #[test]
    fn failed_window_projection_preserves_window_and_cursor() {
        let (mut storage, root) = storage_with_root(&[], "fence");
        let provider = FakeProvider::new("canonical-root");

        let orphan = item("orphan", "missing-parent", RemoteItemKind::File);
        let page = ChangePage {
            changes: vec![RemoteChange::Upsert(orphan)],
            continuation: None,
            checkpoint: Some(ChangeCursor::new("checkpoint").unwrap()),
        };
        storage
            .stage_sync_root_change_window_page(
                &root.id,
                &ChangeCursor::new("fence").unwrap(),
                None,
                &page,
            )
            .unwrap();

        let error =
            execute_completed_selected_root_change_window(&provider, &mut storage, &root, 20)
                .unwrap_err();

        assert!(matches!(
            error,
            SelectedRootExecutorError::Projection(
                RootCatalogProjectionError::IncompleteProjectedCatalog
            )
        ));
        assert!(
            storage
                .sync_root_change_window_state(&root.id)
                .unwrap()
                .unwrap()
                .is_complete()
        );
        assert!(storage.sync_root_change_cursor(&root.id).unwrap().is_none());
        assert!(
            storage
                .list_sync_root_remote_items(&root.id)
                .unwrap()
                .is_empty()
        );
    }

    struct FakeBootstrapProvider {
        canonical_root_id: String,
        cursor: ChangeCursor,
        pages: RefCell<VecDeque<Result<SelectedRootInventoryPage, SelectedRootExecutorError>>>,
        events: RefCell<Vec<&'static str>>,
        root_resolutions: Cell<usize>,
    }

    impl FakeBootstrapProvider {
        fn new(
            canonical_root_id: &str,
            cursor: &str,
            pages: Vec<Result<SelectedRootInventoryPage, SelectedRootExecutorError>>,
        ) -> Self {
            Self {
                canonical_root_id: canonical_root_id.into(),
                cursor: ChangeCursor::new(cursor).unwrap(),
                pages: RefCell::new(VecDeque::from(pages)),
                events: RefCell::new(Vec::new()),
                root_resolutions: Cell::new(0),
            }
        }
    }

    impl SelectedRootProvider for FakeBootstrapProvider {
        type RootIdentity = String;

        fn resolve_root(
            &self,
            _remote_root_id: &str,
        ) -> Result<Self::RootIdentity, SelectedRootExecutorError> {
            self.events.borrow_mut().push("resolve_root");
            self.root_resolutions
                .set(self.root_resolutions.get().saturating_add(1));
            Ok(self.canonical_root_id.clone())
        }

        fn canonical_root_id<'a>(&self, root: &'a Self::RootIdentity) -> &'a str {
            root
        }

        fn resolve_membership(
            &self,
            _item: &RemoteItem,
            _root: &Self::RootIdentity,
        ) -> Result<RootChangeMembership, SelectedRootExecutorError> {
            Err(SelectedRootExecutorError::ProviderOperationFailed)
        }

        fn hydrate_folder(
            &self,
            _item: &RemoteItem,
        ) -> Result<Vec<RemoteItem>, SelectedRootExecutorError> {
            Err(SelectedRootExecutorError::ProviderOperationFailed)
        }
    }

    impl SelectedRootBootstrapProvider for FakeBootstrapProvider {
        fn current_change_cursor(&self) -> Result<ChangeCursor, SelectedRootExecutorError> {
            self.events.borrow_mut().push("cursor");
            Ok(self.cursor.clone())
        }

        fn list_children_page(
            &self,
            _parent_remote_id: &str,
            _continuation: Option<&ContinuationToken>,
        ) -> Result<SelectedRootInventoryPage, SelectedRootExecutorError> {
            self.events.borrow_mut().push("list");
            self.pages
                .borrow_mut()
                .pop_front()
                .unwrap_or_else(|| Ok(SelectedRootInventoryPage::new(Vec::new(), None, 0)))
        }
    }

    #[test]
    fn bootstrap_captures_fence_before_inventory_and_promotes_snapshot() {
        let (mut storage, root) = storage_with_uninitialized_root();

        let folder = item("folder", "canonical-root", RemoteItemKind::Folder);
        let direct_file = item("direct-file", "canonical-root", RemoteItemKind::File);
        let nested_file = item("nested-file", "folder", RemoteItemKind::File);

        let provider = FakeBootstrapProvider::new(
            "canonical-root",
            "bootstrap-fence",
            vec![
                Ok(SelectedRootInventoryPage::new(
                    vec![folder.clone(), direct_file.clone()],
                    None,
                    1,
                )),
                Ok(SelectedRootInventoryPage::new(
                    vec![nested_file.clone()],
                    None,
                    0,
                )),
            ],
        );

        let result = bootstrap_selected_root_snapshot(&provider, &mut storage, &root, 10).unwrap();

        assert_eq!(
            result,
            SelectedRootBootstrap {
                authoritative_items: 3,
                folder_pages: 2,
                unsupported_provider_native: 1,
            }
        );

        assert_eq!(
            provider.events.borrow().as_slice(),
            ["resolve_root", "cursor", "list", "list", "resolve_root"]
        );

        let state = storage.sync_root_remote_inventory_state(&root.id).unwrap();
        assert!(state.snapshot_complete);
        assert!(!state.catchup_complete);
        assert_eq!(state.item_count, 3);
        assert_eq!(
            state.catchup_from_cursor.unwrap().as_str(),
            "bootstrap-fence"
        );
        assert!(storage.sync_root_change_cursor(&root.id).unwrap().is_none());
        assert_eq!(
            storage.list_sync_root_remote_items(&root.id).unwrap(),
            vec![direct_file, folder, nested_file]
        );
        assert_eq!(
            storage
                .staged_sync_root_remote_inventory_count(&root.id)
                .unwrap(),
            0
        );
    }

    #[test]
    fn bootstrap_failure_clears_non_authoritative_staging() {
        let (mut storage, root) = storage_with_uninitialized_root();

        let folder = item("folder", "canonical-root", RemoteItemKind::Folder);
        let provider = FakeBootstrapProvider::new(
            "canonical-root",
            "bootstrap-fence",
            vec![
                Ok(SelectedRootInventoryPage::new(vec![folder], None, 0)),
                Err(SelectedRootExecutorError::ProviderOperationFailed),
            ],
        );

        let error =
            bootstrap_selected_root_snapshot(&provider, &mut storage, &root, 10).unwrap_err();

        assert!(matches!(
            error,
            SelectedRootExecutorError::ProviderOperationFailed
        ));
        assert_eq!(
            storage
                .staged_sync_root_remote_inventory_count(&root.id)
                .unwrap(),
            0
        );

        let state = storage.sync_root_remote_inventory_state(&root.id).unwrap();
        assert!(!state.snapshot_complete);
        assert!(!state.catchup_complete);
        assert_eq!(state.item_count, 0);
        assert!(state.catchup_from_cursor.is_none());
        assert!(
            storage
                .list_sync_root_remote_items(&root.id)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn bootstrap_rejects_duplicate_remote_ids_and_clears_staging() {
        let (mut storage, root) = storage_with_uninitialized_root();

        let folder = item("folder", "canonical-root", RemoteItemKind::Folder);
        let duplicate = item("folder", "canonical-root", RemoteItemKind::File);

        let provider = FakeBootstrapProvider::new(
            "canonical-root",
            "bootstrap-fence",
            vec![Ok(SelectedRootInventoryPage::new(
                vec![folder, duplicate],
                None,
                0,
            ))],
        );

        let error =
            bootstrap_selected_root_snapshot(&provider, &mut storage, &root, 10).unwrap_err();

        assert!(matches!(
            error,
            SelectedRootExecutorError::BootstrapDuplicateRemoteId
        ));
        assert_eq!(
            storage
                .staged_sync_root_remote_inventory_count(&root.id)
                .unwrap(),
            0
        );
        assert!(
            !storage
                .sync_root_remote_inventory_state(&root.id)
                .unwrap()
                .snapshot_complete
        );
    }

    #[test]
    fn bootstrap_refuses_to_replace_existing_authoritative_snapshot() {
        let baseline = item("baseline", "canonical-root", RemoteItemKind::File);
        let (mut storage, root) =
            storage_with_root(std::slice::from_ref(&baseline), "existing-fence");

        let provider = FakeBootstrapProvider::new("canonical-root", "new-fence", Vec::new());

        let error =
            bootstrap_selected_root_snapshot(&provider, &mut storage, &root, 10).unwrap_err();

        assert!(matches!(
            error,
            SelectedRootExecutorError::BootstrapSnapshotAlreadyComplete
        ));
        assert!(provider.events.borrow().is_empty());
        assert_eq!(
            storage.list_sync_root_remote_items(&root.id).unwrap(),
            vec![baseline]
        );
        assert_eq!(
            storage
                .sync_root_remote_inventory_state(&root.id)
                .unwrap()
                .catchup_from_cursor
                .unwrap()
                .as_str(),
            "existing-fence"
        );
    }

    #[test]
    fn executor_hydrates_then_observes_later_delete_in_same_batch() {
        let (mut storage, root) = storage_with_root(&[], "fence");

        let folder = item("folder", "canonical-root", RemoteItemKind::Folder);
        let child = item("child", "folder", RemoteItemKind::File);

        let mut provider = FakeProvider::new("canonical-root");
        provider.hydrations.insert(
            folder.remote_id.clone(),
            vec![folder.clone(), child.clone()],
        );

        let changes = vec![
            RemoteChange::Upsert(folder.clone()),
            RemoteChange::Delete {
                remote_id: child.remote_id.clone(),
            },
        ];

        let result = execute_selected_root_change_batch(
            &provider,
            &mut storage,
            &root,
            &ChangeCursor::new("fence").unwrap(),
            &changes,
            &ChangeCursor::new("checkpoint").unwrap(),
            10,
        )
        .unwrap();

        assert_eq!(
            result,
            SelectedRootBatchExecution {
                provider_changes: 2,
                storage_mutations: 3,
                authoritative_items: 1,
                hydrated_items: 2,
                root_revalidations: 0,
                completed_initial_catchup: true,
            }
        );

        assert_eq!(
            storage.list_sync_root_remote_items(&root.id).unwrap(),
            vec![folder]
        );
        assert_eq!(
            storage
                .sync_root_change_cursor(&root.id)
                .unwrap()
                .unwrap()
                .as_str(),
            "checkpoint"
        );
        assert!(
            storage
                .sync_root_remote_inventory_state(&root.id)
                .unwrap()
                .catchup_complete
        );
    }

    #[test]
    fn executor_rejects_incomplete_projection_without_advancing_cursor() {
        let (mut storage, root) = storage_with_root(&[], "fence");
        let provider = FakeProvider::new("canonical-root");

        let orphan = item("orphan", "missing-parent", RemoteItemKind::File);

        let error = execute_selected_root_change_batch(
            &provider,
            &mut storage,
            &root,
            &ChangeCursor::new("fence").unwrap(),
            &[RemoteChange::Upsert(orphan)],
            &ChangeCursor::new("must-not-persist").unwrap(),
            10,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            SelectedRootExecutorError::Projection(
                RootCatalogProjectionError::IncompleteProjectedCatalog
            )
        ));
        assert!(storage.sync_root_change_cursor(&root.id).unwrap().is_none());
        assert!(
            !storage
                .sync_root_remote_inventory_state(&root.id)
                .unwrap()
                .catchup_complete
        );
        assert_eq!(
            storage.list_sync_root_remote_items(&root.id).unwrap(),
            Vec::<RemoteItem>::new()
        );
    }

    #[test]
    fn executor_root_revalidation_failure_prevents_commit() {
        let baseline = item("file", "canonical-root", RemoteItemKind::File);
        let (mut storage, root) = storage_with_root(std::slice::from_ref(&baseline), "fence");

        let mut provider = FakeProvider::new("canonical-root");
        provider.fail_revalidation = true;

        let error = execute_selected_root_change_batch(
            &provider,
            &mut storage,
            &root,
            &ChangeCursor::new("fence").unwrap(),
            &[RemoteChange::Delete {
                remote_id: "canonical-root".into(),
            }],
            &ChangeCursor::new("must-not-persist").unwrap(),
            10,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            SelectedRootExecutorError::ProviderOperationFailed
        ));
        assert!(storage.sync_root_change_cursor(&root.id).unwrap().is_none());
        assert!(
            !storage
                .sync_root_remote_inventory_state(&root.id)
                .unwrap()
                .catchup_complete
        );
        assert_eq!(
            storage.list_sync_root_remote_items(&root.id).unwrap(),
            vec![baseline]
        );
    }

    #[test]
    fn executor_move_out_deletes_existing_subtree_atomically() {
        let folder = item("folder", "canonical-root", RemoteItemKind::Folder);
        let child = item("child", "folder", RemoteItemKind::File);
        let (mut storage, root) = storage_with_root(&[folder.clone(), child], "fence");
        let provider = FakeProvider::new("canonical-root");

        let moved_out = RemoteItem {
            parent_remote_id: Some("outside".into()),
            ..folder
        };

        let result = execute_selected_root_change_batch(
            &provider,
            &mut storage,
            &root,
            &ChangeCursor::new("fence").unwrap(),
            &[RemoteChange::Upsert(moved_out)],
            &ChangeCursor::new("checkpoint").unwrap(),
            10,
        )
        .unwrap();

        assert_eq!(result.authoritative_items, 0);
        assert_eq!(result.storage_mutations, 1);
        assert!(
            storage
                .list_sync_root_remote_items(&root.id)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            storage
                .sync_root_change_cursor(&root.id)
                .unwrap()
                .unwrap()
                .as_str(),
            "checkpoint"
        );
    }
    struct Phase5d5MappedContentProvider {
        blobs: HashMap<String, Vec<u8>>,
        fail_on: Option<String>,
    }

    impl SelectedRootContentProvider for Phase5d5MappedContentProvider {
        fn content_fingerprint(
            &self,
            remote_id: &str,
        ) -> Result<SelectedRootContentFingerprint, SelectedRootExecutorError> {
            if self.fail_on.as_deref() == Some(remote_id) {
                return Err(SelectedRootExecutorError::ProviderOperationFailed);
            }

            let bytes = self
                .blobs
                .get(remote_id)
                .ok_or(SelectedRootExecutorError::ProviderOperationFailed)?;
            let mut hasher = Sha256::new();
            hasher.update(bytes);

            Ok(SelectedRootContentFingerprint {
                size_bytes: u64::try_from(bytes.len())
                    .map_err(|_| SelectedRootExecutorError::CountOverflow)?,
                sha256_hex: digest_to_hex(hasher.finalize().as_slice()),
            })
        }

        fn download_file_content(
            &self,
            remote_id: &str,
            max_bytes: u64,
            writer: &mut dyn Write,
        ) -> Result<u64, SelectedRootExecutorError> {
            if self.fail_on.as_deref() == Some(remote_id) {
                return Err(SelectedRootExecutorError::ProviderOperationFailed);
            }

            let bytes = self
                .blobs
                .get(remote_id)
                .ok_or(SelectedRootExecutorError::ProviderOperationFailed)?;
            let len =
                u64::try_from(bytes.len()).map_err(|_| SelectedRootExecutorError::CountOverflow)?;
            if len > max_bytes {
                return Err(SelectedRootExecutorError::LocalFileTooLarge);
            }

            writer
                .write_all(bytes)
                .map_err(|_| SelectedRootExecutorError::ProviderOperationFailed)?;
            Ok(len)
        }
    }

    fn phase5d5_sha256(bytes: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        digest_to_hex(hasher.finalize().as_slice())
    }

    fn phase5d5_remote_file(remote_id: &str, name: &str, size: usize) -> RemoteItem {
        RemoteItem {
            remote_id: remote_id.into(),
            parent_remote_id: Some("canonical-root".into()),
            name: name.into(),
            kind: RemoteItemKind::File,
            size_bytes: Some(u64::try_from(size).unwrap()),
            modified_unix_ms: None,
            trashed: false,
        }
    }

    fn phase5d5_fixture(label: &str) -> (Storage, SyncRoot, PathBuf, HashMap<String, Vec<u8>>) {
        let local_root = local_plan_temp_dir(label);
        fs::create_dir(&local_root).unwrap();
        let local_root = fs::canonicalize(local_root).unwrap();

        let old_a = b"old-a".to_vec();
        let old_b = b"old-b".to_vec();
        let new_a = b"new-a-content".to_vec();
        let new_b = b"new-b-content".to_vec();

        fs::write(local_root.join("a.txt"), &old_a).unwrap();
        fs::write(local_root.join("b.txt"), &old_b).unwrap();

        let mut storage = Storage::open_in_memory().unwrap();
        let provider_id = ProviderId::new("google-drive").unwrap();
        let account =
            ProviderAccount::new(provider_id.clone(), "phase5d5-subject", None, None).unwrap();
        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "phase5d5-root",
            provider_id,
            account.subject,
            local_root.to_str().unwrap(),
            Some("configured-root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        storage.insert_sync_root(&root).unwrap();

        let old_items = vec![
            phase5d5_remote_file("file-a", "a.txt", old_a.len()),
            phase5d5_remote_file("file-b", "b.txt", old_b.len()),
        ];

        storage
            .begin_sync_root_remote_inventory_staging(&root.id)
            .unwrap();
        storage
            .stage_sync_root_remote_inventory_items(&root.id, &old_items, 3)
            .unwrap();
        storage
            .commit_sync_root_remote_inventory_snapshot(
                &root.id,
                &ChangeCursor::new("phase5d5-fence").unwrap(),
                4,
            )
            .unwrap();

        let root_provider = FakeProvider::new("canonical-root");
        execute_selected_root_change_batch(
            &root_provider,
            &mut storage,
            &root,
            &ChangeCursor::new("phase5d5-fence").unwrap(),
            &[],
            &ChangeCursor::new("phase5d5-ready").unwrap(),
            5,
        )
        .unwrap();

        storage
            .record_sync_root_file_materializations(
                &root.id,
                &[
                    (
                        "file-a".into(),
                        "a.txt".into(),
                        u64::try_from(old_a.len()).unwrap(),
                        phase5d5_sha256(&old_a),
                    ),
                    (
                        "file-b".into(),
                        "b.txt".into(),
                        u64::try_from(old_b.len()).unwrap(),
                        phase5d5_sha256(&old_b),
                    ),
                ],
                6,
            )
            .unwrap();

        let new_items = vec![
            phase5d5_remote_file("file-a", "a.txt", new_a.len()),
            phase5d5_remote_file("file-b", "b.txt", new_b.len()),
        ];
        execute_selected_root_change_batch(
            &root_provider,
            &mut storage,
            &root,
            &ChangeCursor::new("phase5d5-ready").unwrap(),
            &[
                RemoteChange::Upsert(new_items[0].clone()),
                RemoteChange::Upsert(new_items[1].clone()),
            ],
            &ChangeCursor::new("phase5d5-changed").unwrap(),
            7,
        )
        .unwrap();

        assert_eq!(
            storage
                .sync_root_materialization_receipt_count(&root.id)
                .unwrap(),
            0
        );
        assert_eq!(
            storage
                .sync_root_stale_materialization_receipt_count(&root.id)
                .unwrap(),
            2
        );

        let blobs = HashMap::from([("file-a".to_string(), new_a), ("file-b".to_string(), new_b)]);

        (storage, root, local_root, blobs)
    }

    fn phase5d5_assert_no_internal_files(root: &Path) {
        let entries = fs::read_dir(root)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect::<Vec<_>>();

        assert!(
            entries
                .iter()
                .all(|name| !name.starts_with(".nubisync-download-"))
        );
        assert!(
            entries
                .iter()
                .all(|name| !name.starts_with(".nubisync-replace-backup-"))
        );
    }

    #[test]
    fn phase5d5_bounded_replacement_replaces_two_files_and_converges() {
        let (mut storage, root, local_root, blobs) = phase5d5_fixture("phase5d5-success");
        let provider = Phase5d5MappedContentProvider {
            blobs: blobs.clone(),
            fail_on: None,
        };

        let result = replace_selected_root_stale_files(&provider, &mut storage, &root).unwrap();

        assert_eq!(result.planned_replacement_actions, 2);
        assert_eq!(
            result.batch_action_limit,
            SUPERVISED_STALE_FILE_PLAN_MAX_ACTIONS
        );
        assert_eq!(result.files_replaced, 2);
        assert_eq!(
            result.bytes_downloaded,
            u64::try_from(blobs["file-a"].len() + blobs["file-b"].len()).unwrap()
        );
        assert_eq!(result.provider_fingerprints_verified, 2);
        assert_eq!(result.stale_baselines_verified, 2);
        assert_eq!(result.receipts_recorded, 2);
        assert_eq!(result.atomic_replacements, 2);
        assert_eq!(result.replacement_backups_cleaned, 2);

        assert_eq!(fs::read(local_root.join("a.txt")).unwrap(), blobs["file-a"]);
        assert_eq!(fs::read(local_root.join("b.txt")).unwrap(), blobs["file-b"]);
        assert_eq!(
            storage
                .sync_root_materialization_receipt_count(&root.id)
                .unwrap(),
            2
        );
        assert_eq!(
            storage
                .sync_root_stale_materialization_receipt_count(&root.id)
                .unwrap(),
            0
        );

        let convergence = plan_selected_root_receive_only_convergence(&storage, &root).unwrap();
        assert!(!convergence.blocked());
        assert_eq!(convergence.create_directories, 0);
        assert_eq!(convergence.materialize_missing_files, 0);
        assert_eq!(convergence.verify_existing_files, 0);
        assert_eq!(convergence.revalidate_stale_file_replacements, 0);
        assert_eq!(convergence.revalidate_stale_file_deletions, 0);
        assert_eq!(convergence.delete_owned_empty_directories, 0);
        assert_eq!(convergence.current_owned_files, 2);

        phase5d5_assert_no_internal_files(&local_root);

        fs::remove_file(local_root.join("a.txt")).unwrap();
        fs::remove_file(local_root.join("b.txt")).unwrap();
        fs::remove_dir(local_root).unwrap();
    }

    #[test]
    fn phase5d5_provider_failure_rolls_back_prior_replacement_and_receipts() {
        let (mut storage, root, local_root, blobs) = phase5d5_fixture("phase5d5-rollback");
        let provider = Phase5d5MappedContentProvider {
            blobs,
            fail_on: Some("file-b".into()),
        };

        let error = replace_selected_root_stale_files(&provider, &mut storage, &root).unwrap_err();
        assert!(matches!(
            error,
            SelectedRootExecutorError::ProviderOperationFailed
        ));

        assert_eq!(fs::read(local_root.join("a.txt")).unwrap(), b"old-a");
        assert_eq!(fs::read(local_root.join("b.txt")).unwrap(), b"old-b");
        assert_eq!(
            storage
                .sync_root_materialization_receipt_count(&root.id)
                .unwrap(),
            0
        );
        assert_eq!(
            storage
                .sync_root_stale_materialization_receipt_count(&root.id)
                .unwrap(),
            2
        );

        phase5d5_assert_no_internal_files(&local_root);

        fs::remove_file(local_root.join("a.txt")).unwrap();
        fs::remove_file(local_root.join("b.txt")).unwrap();
        fs::remove_dir(local_root).unwrap();
    }
    fn phase5d6_fixture(label: &str) -> (Storage, SyncRoot, PathBuf) {
        let local_root = local_plan_temp_dir(label);
        fs::create_dir(&local_root).unwrap();
        let local_root = fs::canonicalize(local_root).unwrap();

        let first_bytes = b"delete-a".to_vec();
        let second_bytes = b"delete-bb".to_vec();

        fs::write(local_root.join("a.txt"), &first_bytes).unwrap();
        fs::write(local_root.join("b.txt"), &second_bytes).unwrap();

        let mut storage = Storage::open_in_memory().unwrap();
        let provider_id = ProviderId::new("google-drive").unwrap();
        let account =
            ProviderAccount::new(provider_id.clone(), "phase5d6-subject", None, None).unwrap();
        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "phase5d6-root",
            provider_id,
            account.subject,
            local_root.to_str().unwrap(),
            Some("configured-root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        storage.insert_sync_root(&root).unwrap();

        let files = vec![
            phase5d5_remote_file("delete-a", "a.txt", first_bytes.len()),
            phase5d5_remote_file("delete-b", "b.txt", second_bytes.len()),
        ];

        storage
            .begin_sync_root_remote_inventory_staging(&root.id)
            .unwrap();
        storage
            .stage_sync_root_remote_inventory_items(&root.id, &files, 3)
            .unwrap();
        storage
            .commit_sync_root_remote_inventory_snapshot(
                &root.id,
                &ChangeCursor::new("phase5d6-fence").unwrap(),
                4,
            )
            .unwrap();

        let root_provider = FakeProvider::new("canonical-root");
        execute_selected_root_change_batch(
            &root_provider,
            &mut storage,
            &root,
            &ChangeCursor::new("phase5d6-fence").unwrap(),
            &[],
            &ChangeCursor::new("phase5d6-ready").unwrap(),
            5,
        )
        .unwrap();

        storage
            .record_sync_root_file_materializations(
                &root.id,
                &[
                    (
                        "delete-a".into(),
                        "a.txt".into(),
                        u64::try_from(first_bytes.len()).unwrap(),
                        phase5d5_sha256(&first_bytes),
                    ),
                    (
                        "delete-b".into(),
                        "b.txt".into(),
                        u64::try_from(second_bytes.len()).unwrap(),
                        phase5d5_sha256(&second_bytes),
                    ),
                ],
                6,
            )
            .unwrap();

        execute_selected_root_change_batch(
            &root_provider,
            &mut storage,
            &root,
            &ChangeCursor::new("phase5d6-ready").unwrap(),
            &[
                RemoteChange::Delete {
                    remote_id: "delete-a".into(),
                },
                RemoteChange::Delete {
                    remote_id: "delete-b".into(),
                },
            ],
            &ChangeCursor::new("phase5d6-deleted").unwrap(),
            7,
        )
        .unwrap();

        assert_eq!(
            storage
                .sync_root_materialization_receipt_count(&root.id)
                .unwrap(),
            0
        );
        assert_eq!(
            storage
                .sync_root_stale_materialization_receipt_count(&root.id)
                .unwrap(),
            2
        );

        (storage, root, local_root)
    }

    fn phase5d6_assert_no_quarantine_files(root: &Path) {
        let entries = fs::read_dir(root)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect::<Vec<_>>();

        assert!(
            entries
                .iter()
                .all(|name| !name.starts_with(".nubisync-delete-"))
        );
    }

    #[test]
    fn phase5d6_bounded_deletion_removes_two_stale_files_and_converges() {
        let (mut storage, root, local_root) = phase5d6_fixture("phase5d6-success");

        let plan = plan_selected_root_stale_files(&storage, &root).unwrap();
        assert_eq!(plan.replacement_candidates, 0);
        assert_eq!(plan.deletion_candidates, 2);
        assert_eq!(plan.safe_to_delete, 2);
        assert!(plan.all_stale_files_safe());

        let result = delete_selected_root_stale_files(&mut storage, &root).unwrap();

        assert_eq!(result.planned_deletion_actions, 2);
        assert_eq!(
            result.batch_action_limit,
            SUPERVISED_STALE_FILE_PLAN_MAX_ACTIONS
        );
        assert_eq!(result.files_deleted, 2);
        assert_eq!(result.stale_baselines_verified, 2);
        assert_eq!(result.receipts_deleted, 2);
        assert_eq!(result.quarantine_renames, 2);
        assert_eq!(result.quarantine_files_removed, 2);
        assert!(!local_root.join("a.txt").exists());
        assert!(!local_root.join("b.txt").exists());
        assert_eq!(
            storage
                .sync_root_stale_materialization_receipt_count(&root.id)
                .unwrap(),
            0
        );

        let convergence = plan_selected_root_receive_only_convergence(&storage, &root).unwrap();
        assert!(!convergence.blocked());
        assert_eq!(convergence.create_directories, 0);
        assert_eq!(convergence.materialize_missing_files, 0);
        assert_eq!(convergence.verify_existing_files, 0);
        assert_eq!(convergence.revalidate_stale_file_replacements, 0);
        assert_eq!(convergence.revalidate_stale_file_deletions, 0);
        assert_eq!(convergence.delete_owned_empty_directories, 0);

        phase5d6_assert_no_quarantine_files(&local_root);
        fs::remove_dir(local_root).unwrap();
    }

    #[test]
    fn phase5d6_quarantine_failure_path_can_restore_prior_files() {
        let (storage, root, local_root) = phase5d6_fixture("phase5d6-rollback");

        let receipts = storage
            .list_sync_root_stale_file_materialization_receipts(&root.id)
            .unwrap();
        let root_path = validated_selected_root_path(&root).unwrap();

        let first = quarantine_verified_stale_local_file(&root_path, &receipts[0]).unwrap();
        assert!(!first.target_path.exists());
        assert!(first.quarantine_path.exists());

        rollback_stale_file_deletion_batch(&[first]).unwrap();

        assert!(local_root.join("a.txt").exists() || local_root.join("b.txt").exists());
        assert_eq!(
            storage
                .sync_root_stale_materialization_receipt_count(&root.id)
                .unwrap(),
            2
        );
        phase5d6_assert_no_quarantine_files(&local_root);

        fs::remove_file(local_root.join("a.txt")).unwrap();
        fs::remove_file(local_root.join("b.txt")).unwrap();
        fs::remove_dir(local_root).unwrap();
    }
}

#[cfg(test)]
mod phase5c11_directory_deletion_plan_tests {
    use super::*;

    fn temp_root(label: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "nubisync-phase5c11-{label}-{}-{nonce}",
            std::process::id()
        ))
    }

    fn receipt() -> SyncRootDirectoryMaterializationReceipt {
        SyncRootDirectoryMaterializationReceipt {
            remote_id: "remote-folder".into(),
            relative_path: "folder".into(),
            materialized_at_unix_ms: 1,
        }
    }

    #[test]
    fn empty_owned_directory_is_safe_candidate() {
        let root = temp_root("empty");
        fs::create_dir(&root).unwrap();
        fs::create_dir(root.join("folder")).unwrap();
        let root = fs::canonicalize(root).unwrap();

        assert!(matches!(
            inspect_directory_receipt_target(&root, &receipt()).unwrap(),
            DirectoryReceiptTargetInspection::EmptyDirectory
        ));

        fs::remove_dir(root.join("folder")).unwrap();
        fs::remove_dir(root).unwrap();
    }

    #[test]
    fn non_empty_owned_directory_is_not_safe_candidate() {
        let root = temp_root("non-empty");
        fs::create_dir(&root).unwrap();
        fs::create_dir(root.join("folder")).unwrap();
        fs::write(root.join("folder/extra.txt"), b"x").unwrap();
        let root = fs::canonicalize(root).unwrap();

        assert!(matches!(
            inspect_directory_receipt_target(&root, &receipt()).unwrap(),
            DirectoryReceiptTargetInspection::NonEmptyDirectory
        ));

        fs::remove_file(root.join("folder/extra.txt")).unwrap();
        fs::remove_dir(root.join("folder")).unwrap();
        fs::remove_dir(root).unwrap();
    }

    #[test]
    fn directory_deletion_plan_ready_requires_one_clean_absent_candidate() {
        let ready = SelectedRootRemoteDirectoryDeletionPlan {
            stale_receipts_total: 1,
            deletion_candidates: 1,
            safe_to_delete: 1,
            directories_already_missing: 0,
            non_empty_directories: 0,
            type_conflicts: 0,
            remote_id_absent: true,
        };
        assert!(ready.ready());

        let blocked = SelectedRootRemoteDirectoryDeletionPlan {
            non_empty_directories: 1,
            safe_to_delete: 0,
            ..ready
        };
        assert!(!blocked.ready());
    }
}

#[cfg(test)]
mod phase5c12_directory_deletion_executor_tests {
    use super::*;

    fn temp_root(label: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "nubisync-phase5c12-{label}-{}-{nonce}",
            std::process::id()
        ))
    }

    fn receipt() -> SyncRootDirectoryMaterializationReceipt {
        SyncRootDirectoryMaterializationReceipt {
            remote_id: "remote-folder".into(),
            relative_path: "folder".into(),
            materialized_at_unix_ms: 1,
        }
    }

    #[test]
    fn verified_empty_directory_deletion_removes_exact_candidate() {
        let root = temp_root("empty");
        fs::create_dir(&root).unwrap();
        fs::create_dir(root.join("folder")).unwrap();
        let root = fs::canonicalize(root).unwrap();

        delete_verified_stale_local_directory(&root, &receipt()).unwrap();

        assert!(!root.join("folder").exists());
        fs::remove_dir(root).unwrap();
    }

    #[test]
    fn non_empty_directory_is_preserved() {
        let root = temp_root("non-empty");
        fs::create_dir(&root).unwrap();
        fs::create_dir(root.join("folder")).unwrap();
        fs::write(root.join("folder/keep.txt"), b"keep").unwrap();
        let root = fs::canonicalize(root).unwrap();

        assert!(matches!(
            delete_verified_stale_local_directory(&root, &receipt()),
            Err(SelectedRootExecutorError::RemoteDirectoryDeletionNotReady)
        ));

        assert_eq!(fs::read(root.join("folder/keep.txt")).unwrap(), b"keep");

        fs::remove_file(root.join("folder/keep.txt")).unwrap();
        fs::remove_dir(root.join("folder")).unwrap();
        fs::remove_dir(root).unwrap();
    }
}

#[cfg(test)]
mod phase5d7_directory_batch_tests {
    use super::*;
    use nubisync_core::{ProviderAccount, ProviderId, SyncMode};

    struct Phase5d7Provider;

    impl SelectedRootProvider for Phase5d7Provider {
        type RootIdentity = String;

        fn resolve_root(
            &self,
            _remote_root_id: &str,
        ) -> Result<Self::RootIdentity, SelectedRootExecutorError> {
            Ok("canonical-root".into())
        }

        fn canonical_root_id<'a>(&self, root: &'a Self::RootIdentity) -> &'a str {
            root.as_str()
        }

        fn resolve_membership(
            &self,
            item: &RemoteItem,
            root: &Self::RootIdentity,
        ) -> Result<RootChangeMembership, SelectedRootExecutorError> {
            if item.remote_id == *root {
                Ok(RootChangeMembership::Root)
            } else if item.parent_remote_id.as_deref() == Some(root.as_str()) {
                Ok(RootChangeMembership::Descendant)
            } else {
                Ok(RootChangeMembership::Outside)
            }
        }

        fn hydrate_folder(
            &self,
            item: &RemoteItem,
        ) -> Result<Vec<RemoteItem>, SelectedRootExecutorError> {
            Ok(vec![item.clone()])
        }
    }

    fn temp_root(label: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "nubisync-phase5d7-{label}-{}-{nonce}",
            std::process::id()
        ))
    }

    fn folder(remote_id: &str, name: &str) -> RemoteItem {
        RemoteItem {
            remote_id: remote_id.into(),
            parent_remote_id: Some("canonical-root".into()),
            name: name.into(),
            kind: RemoteItemKind::Folder,
            size_bytes: None,
            modified_unix_ms: None,
            trashed: false,
        }
    }

    fn fixture(label: &str) -> (Storage, SyncRoot, PathBuf) {
        let local_root = temp_root(label);
        fs::create_dir(&local_root).unwrap();
        fs::create_dir(local_root.join("dir-a")).unwrap();
        fs::create_dir(local_root.join("dir-b")).unwrap();
        let local_root = fs::canonicalize(local_root).unwrap();

        let mut storage = Storage::open_in_memory().unwrap();
        let provider_id = ProviderId::new("google-drive").unwrap();
        let account =
            ProviderAccount::new(provider_id.clone(), "phase5d7-subject", None, None).unwrap();
        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "phase5d7-root",
            provider_id,
            account.subject,
            local_root.to_str().unwrap(),
            Some("canonical-root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        storage.insert_sync_root(&root).unwrap();

        let folders = vec![folder("dir-a", "dir-a"), folder("dir-b", "dir-b")];

        storage
            .begin_sync_root_remote_inventory_staging(&root.id)
            .unwrap();
        storage
            .stage_sync_root_remote_inventory_items(&root.id, &folders, 3)
            .unwrap();
        storage
            .commit_sync_root_remote_inventory_snapshot(
                &root.id,
                &ChangeCursor::new("phase5d7-fence").unwrap(),
                4,
            )
            .unwrap();

        let provider = Phase5d7Provider;
        execute_selected_root_change_batch(
            &provider,
            &mut storage,
            &root,
            &ChangeCursor::new("phase5d7-fence").unwrap(),
            &[],
            &ChangeCursor::new("phase5d7-ready").unwrap(),
            5,
        )
        .unwrap();

        storage
            .record_sync_root_directory_materializations(
                &root.id,
                &[
                    ("dir-a".into(), "dir-a".into()),
                    ("dir-b".into(), "dir-b".into()),
                ],
                6,
            )
            .unwrap();

        execute_selected_root_change_batch(
            &provider,
            &mut storage,
            &root,
            &ChangeCursor::new("phase5d7-ready").unwrap(),
            &[
                RemoteChange::Delete {
                    remote_id: "dir-a".into(),
                },
                RemoteChange::Delete {
                    remote_id: "dir-b".into(),
                },
            ],
            &ChangeCursor::new("phase5d7-deleted").unwrap(),
            7,
        )
        .unwrap();

        assert_eq!(
            storage
                .sync_root_directory_materialization_receipt_count(&root.id)
                .unwrap(),
            0
        );
        assert_eq!(
            storage
                .sync_root_stale_directory_materialization_receipt_count(&root.id)
                .unwrap(),
            2
        );

        (storage, root, local_root)
    }

    fn assert_no_quarantine_directories(root: &Path) {
        let names = fs::read_dir(root)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect::<Vec<_>>();

        assert!(
            names
                .iter()
                .all(|name| !name.starts_with(".nubisync-delete-dir-"))
        );
    }

    #[test]
    fn phase5d7_bounded_deletion_removes_two_stale_empty_directories_and_converges() {
        let (mut storage, root, local_root) = fixture("success");

        let convergence = plan_selected_root_receive_only_convergence(&storage, &root).unwrap();
        assert!(!convergence.blocked());
        assert_eq!(convergence.delete_owned_empty_directories, 2);
        assert_eq!(convergence.revalidate_stale_file_replacements, 0);
        assert_eq!(convergence.revalidate_stale_file_deletions, 0);

        let result = delete_selected_root_stale_directories(&mut storage, &root).unwrap();

        assert_eq!(result.planned_deletion_actions, 2);
        assert_eq!(
            result.batch_action_limit,
            SUPERVISED_STALE_DIRECTORY_DELETION_MAX_ACTIONS
        );
        assert_eq!(result.directories_deleted, 2);
        assert_eq!(result.empty_directories_verified, 2);
        assert_eq!(result.receipts_deleted, 2);
        assert_eq!(result.quarantine_renames, 2);
        assert_eq!(result.quarantine_directories_removed, 2);

        assert!(!local_root.join("dir-a").exists());
        assert!(!local_root.join("dir-b").exists());
        assert_eq!(
            storage
                .sync_root_stale_directory_materialization_receipt_count(&root.id)
                .unwrap(),
            0
        );

        let convergence = plan_selected_root_receive_only_convergence(&storage, &root).unwrap();
        assert!(!convergence.blocked());
        assert_eq!(convergence.create_directories, 0);
        assert_eq!(convergence.materialize_missing_files, 0);
        assert_eq!(convergence.verify_existing_files, 0);
        assert_eq!(convergence.revalidate_stale_file_replacements, 0);
        assert_eq!(convergence.revalidate_stale_file_deletions, 0);
        assert_eq!(convergence.delete_owned_empty_directories, 0);

        assert_no_quarantine_directories(&local_root);
        fs::remove_dir(local_root).unwrap();
    }

    #[test]
    fn phase5d7_quarantine_failure_path_can_restore_prior_directory() {
        let (storage, root, local_root) = fixture("rollback");
        let receipts = storage
            .list_sync_root_stale_directory_materialization_receipts(&root.id)
            .unwrap();
        assert_eq!(receipts.len(), 2);

        let root_path = validated_selected_root_path(&root).unwrap();
        let first = quarantine_verified_stale_local_directory(&root_path, &receipts[0]).unwrap();

        fs::write(local_root.join("dir-b/blocker.txt"), b"block").unwrap();
        let error =
            quarantine_verified_stale_local_directory(&root_path, &receipts[1]).unwrap_err();
        assert!(matches!(
            error,
            SelectedRootExecutorError::RemoteDirectoryDeletionNotReady
        ));

        rollback_stale_directory_deletion_batch(&[first]).unwrap();

        assert!(local_root.join("dir-a").is_dir());
        assert!(local_root.join("dir-b").is_dir());
        assert_eq!(
            storage
                .sync_root_stale_directory_materialization_receipt_count(&root.id)
                .unwrap(),
            2
        );
        assert_no_quarantine_directories(&local_root);

        fs::remove_file(local_root.join("dir-b/blocker.txt")).unwrap();
        fs::remove_dir(local_root.join("dir-a")).unwrap();
        fs::remove_dir(local_root.join("dir-b")).unwrap();
        fs::remove_dir(local_root).unwrap();
    }
}

#[cfg(test)]
mod phase5d8_file_batch_verification_tests {
    use super::*;
    use nubisync_core::{ProviderAccount, ProviderId, SyncMode};
    use std::collections::HashMap;

    struct Phase5d8RootProvider;

    impl SelectedRootProvider for Phase5d8RootProvider {
        type RootIdentity = String;

        fn resolve_root(
            &self,
            _remote_root_id: &str,
        ) -> Result<Self::RootIdentity, SelectedRootExecutorError> {
            Ok("canonical-root".into())
        }

        fn canonical_root_id<'a>(&self, root: &'a Self::RootIdentity) -> &'a str {
            root.as_str()
        }

        fn resolve_membership(
            &self,
            item: &RemoteItem,
            root: &Self::RootIdentity,
        ) -> Result<RootChangeMembership, SelectedRootExecutorError> {
            if item.remote_id == *root {
                Ok(RootChangeMembership::Root)
            } else if item.parent_remote_id.as_deref() == Some(root.as_str()) {
                Ok(RootChangeMembership::Descendant)
            } else {
                Ok(RootChangeMembership::Outside)
            }
        }

        fn hydrate_folder(
            &self,
            item: &RemoteItem,
        ) -> Result<Vec<RemoteItem>, SelectedRootExecutorError> {
            Ok(vec![item.clone()])
        }
    }

    struct Phase5d8ContentProvider {
        blobs: HashMap<String, Vec<u8>>,
        fail_on: Option<String>,
    }

    impl SelectedRootContentProvider for Phase5d8ContentProvider {
        fn content_fingerprint(
            &self,
            remote_id: &str,
        ) -> Result<SelectedRootContentFingerprint, SelectedRootExecutorError> {
            let bytes = self
                .blobs
                .get(remote_id)
                .ok_or(SelectedRootExecutorError::ProviderOperationFailed)?;
            let mut hasher = Sha256::new();
            hasher.update(bytes);

            Ok(SelectedRootContentFingerprint {
                size_bytes: u64::try_from(bytes.len())
                    .map_err(|_| SelectedRootExecutorError::CountOverflow)?,
                sha256_hex: digest_to_hex(hasher.finalize().as_slice()),
            })
        }

        fn download_file_content(
            &self,
            remote_id: &str,
            max_bytes: u64,
            writer: &mut dyn Write,
        ) -> Result<u64, SelectedRootExecutorError> {
            if self.fail_on.as_deref() == Some(remote_id) {
                return Err(SelectedRootExecutorError::ProviderOperationFailed);
            }

            let bytes = self
                .blobs
                .get(remote_id)
                .ok_or(SelectedRootExecutorError::ProviderOperationFailed)?;
            let len =
                u64::try_from(bytes.len()).map_err(|_| SelectedRootExecutorError::CountOverflow)?;
            if len > max_bytes {
                return Err(SelectedRootExecutorError::LocalFileTooLarge);
            }

            writer
                .write_all(bytes)
                .map_err(|_| SelectedRootExecutorError::ProviderOperationFailed)?;
            Ok(len)
        }
    }

    fn temp_root(label: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "nubisync-phase5d8-{label}-{}-{nonce}",
            std::process::id()
        ))
    }

    fn remote_file(remote_id: &str, name: &str, size: usize) -> RemoteItem {
        RemoteItem {
            remote_id: remote_id.into(),
            parent_remote_id: Some("canonical-root".into()),
            name: name.into(),
            kind: RemoteItemKind::File,
            size_bytes: Some(u64::try_from(size).unwrap()),
            modified_unix_ms: None,
            trashed: false,
        }
    }

    fn fixture(label: &str) -> (Storage, SyncRoot, PathBuf, HashMap<String, Vec<u8>>) {
        let local_root = temp_root(label);
        fs::create_dir(&local_root).unwrap();
        let local_root = fs::canonicalize(local_root).unwrap();

        let a = b"phase-5d8-a".to_vec();
        let b = b"phase-5d8-bb".to_vec();
        fs::write(local_root.join("a.txt"), &a).unwrap();
        fs::write(local_root.join("b.txt"), &b).unwrap();

        let mut storage = Storage::open_in_memory().unwrap();
        let provider_id = ProviderId::new("google-drive").unwrap();
        let account =
            ProviderAccount::new(provider_id.clone(), "phase5d8-subject", None, None).unwrap();
        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "phase5d8-root",
            provider_id,
            account.subject,
            local_root.to_str().unwrap(),
            Some("canonical-root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        storage.insert_sync_root(&root).unwrap();

        let remote_items = vec![
            remote_file("file-a", "a.txt", a.len()),
            remote_file("file-b", "b.txt", b.len()),
        ];

        storage
            .begin_sync_root_remote_inventory_staging(&root.id)
            .unwrap();
        storage
            .stage_sync_root_remote_inventory_items(&root.id, &remote_items, 3)
            .unwrap();
        storage
            .commit_sync_root_remote_inventory_snapshot(
                &root.id,
                &ChangeCursor::new("phase5d8-fence").unwrap(),
                4,
            )
            .unwrap();

        let root_provider = Phase5d8RootProvider;
        execute_selected_root_change_batch(
            &root_provider,
            &mut storage,
            &root,
            &ChangeCursor::new("phase5d8-fence").unwrap(),
            &[],
            &ChangeCursor::new("phase5d8-ready").unwrap(),
            5,
        )
        .unwrap();

        let blobs = HashMap::from([("file-a".to_string(), a), ("file-b".to_string(), b)]);

        (storage, root, local_root, blobs)
    }

    #[test]
    fn phase5d8_bounded_verification_records_two_receipts_and_converges() {
        let (mut storage, root, local_root, blobs) = fixture("success");

        let pre = plan_selected_root_receive_only_convergence(&storage, &root).unwrap();
        assert!(!pre.blocked());
        assert_eq!(pre.verify_existing_files, 2);
        assert_eq!(pre.current_owned_files, 0);
        assert_eq!(pre.action_count(), 2);

        let provider = Phase5d8ContentProvider {
            blobs: blobs.clone(),
            fail_on: None,
        };

        let result = verify_selected_root_existing_files(&provider, &mut storage, &root).unwrap();

        assert_eq!(result.planned_verification_actions, 2);
        assert_eq!(result.batch_action_limit, SUPERVISED_FILE_BATCH_MAX_ACTIONS);
        assert_eq!(result.files_verified, 2);
        assert_eq!(
            result.bytes_verified,
            u64::try_from(blobs["file-a"].len() + blobs["file-b"].len()).unwrap()
        );
        assert_eq!(result.max_file_bytes, SUPERVISED_FILE_DOWNLOAD_MAX_BYTES);
        assert_eq!(result.remote_content_hashes_verified, 2);
        assert_eq!(result.receipts_recorded, 2);

        assert_eq!(fs::read(local_root.join("a.txt")).unwrap(), blobs["file-a"]);
        assert_eq!(fs::read(local_root.join("b.txt")).unwrap(), blobs["file-b"]);
        assert_eq!(
            storage
                .sync_root_materialization_receipt_count(&root.id)
                .unwrap(),
            2
        );
        assert_eq!(
            storage
                .sync_root_stale_materialization_receipt_count(&root.id)
                .unwrap(),
            0
        );

        let post = plan_selected_root_receive_only_convergence(&storage, &root).unwrap();
        assert!(!post.blocked());
        assert_eq!(post.create_directories, 0);
        assert_eq!(post.materialize_missing_files, 0);
        assert_eq!(post.verify_existing_files, 0);
        assert_eq!(post.revalidate_stale_file_replacements, 0);
        assert_eq!(post.revalidate_stale_file_deletions, 0);
        assert_eq!(post.delete_owned_empty_directories, 0);
        assert_eq!(post.current_owned_files, 2);
        assert_eq!(post.action_count(), 0);

        fs::remove_file(local_root.join("a.txt")).unwrap();
        fs::remove_file(local_root.join("b.txt")).unwrap();
        fs::remove_dir(local_root).unwrap();
    }

    #[test]
    fn phase5d8_provider_failure_records_no_partial_receipts() {
        let (mut storage, root, local_root, blobs) = fixture("provider-failure");

        let provider = Phase5d8ContentProvider {
            blobs: blobs.clone(),
            fail_on: Some("file-b".into()),
        };

        let error =
            verify_selected_root_existing_files(&provider, &mut storage, &root).unwrap_err();
        assert!(matches!(
            error,
            SelectedRootExecutorError::ProviderOperationFailed
        ));

        assert_eq!(
            storage
                .sync_root_materialization_receipt_count(&root.id)
                .unwrap(),
            0
        );
        assert_eq!(
            storage
                .sync_root_stale_materialization_receipt_count(&root.id)
                .unwrap(),
            0
        );

        let convergence = plan_selected_root_receive_only_convergence(&storage, &root).unwrap();
        assert!(!convergence.blocked());
        assert_eq!(convergence.verify_existing_files, 2);
        assert_eq!(convergence.action_count(), 2);

        assert_eq!(fs::read(local_root.join("a.txt")).unwrap(), blobs["file-a"]);
        assert_eq!(fs::read(local_root.join("b.txt")).unwrap(), blobs["file-b"]);

        fs::remove_file(local_root.join("a.txt")).unwrap();
        fs::remove_file(local_root.join("b.txt")).unwrap();
        fs::remove_dir(local_root).unwrap();
    }
}

#[cfg(test)]
mod phase5d9_unified_convergence_tests {
    use super::*;
    use nubisync_core::{ProviderAccount, ProviderId, SyncMode};
    use std::collections::HashMap;

    struct Phase5d9RootProvider;

    impl SelectedRootProvider for Phase5d9RootProvider {
        type RootIdentity = String;

        fn resolve_root(
            &self,
            _remote_root_id: &str,
        ) -> Result<Self::RootIdentity, SelectedRootExecutorError> {
            Ok("canonical-root".into())
        }

        fn canonical_root_id<'a>(&self, root: &'a Self::RootIdentity) -> &'a str {
            root.as_str()
        }

        fn resolve_membership(
            &self,
            item: &RemoteItem,
            root: &Self::RootIdentity,
        ) -> Result<RootChangeMembership, SelectedRootExecutorError> {
            if item.remote_id == *root {
                Ok(RootChangeMembership::Root)
            } else {
                Ok(RootChangeMembership::Descendant)
            }
        }

        fn hydrate_folder(
            &self,
            item: &RemoteItem,
        ) -> Result<Vec<RemoteItem>, SelectedRootExecutorError> {
            Ok(vec![item.clone()])
        }
    }

    struct Phase5d9ContentProvider {
        blobs: HashMap<String, Vec<u8>>,
    }

    impl SelectedRootContentProvider for Phase5d9ContentProvider {
        fn content_fingerprint(
            &self,
            remote_id: &str,
        ) -> Result<SelectedRootContentFingerprint, SelectedRootExecutorError> {
            let bytes = self
                .blobs
                .get(remote_id)
                .ok_or(SelectedRootExecutorError::ProviderOperationFailed)?;
            let mut hasher = Sha256::new();
            hasher.update(bytes);

            Ok(SelectedRootContentFingerprint {
                size_bytes: u64::try_from(bytes.len())
                    .map_err(|_| SelectedRootExecutorError::CountOverflow)?,
                sha256_hex: digest_to_hex(hasher.finalize().as_slice()),
            })
        }

        fn download_file_content(
            &self,
            remote_id: &str,
            max_bytes: u64,
            writer: &mut dyn Write,
        ) -> Result<u64, SelectedRootExecutorError> {
            let bytes = self
                .blobs
                .get(remote_id)
                .ok_or(SelectedRootExecutorError::ProviderOperationFailed)?;
            let len =
                u64::try_from(bytes.len()).map_err(|_| SelectedRootExecutorError::CountOverflow)?;
            if len > max_bytes {
                return Err(SelectedRootExecutorError::LocalFileTooLarge);
            }

            writer
                .write_all(bytes)
                .map_err(|_| SelectedRootExecutorError::ProviderOperationFailed)?;
            Ok(len)
        }
    }

    fn temp_root(label: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "nubisync-phase5d9-{label}-{}-{nonce}",
            std::process::id()
        ))
    }

    fn base_storage(label: &str) -> (Storage, SyncRoot, PathBuf) {
        let local_root = temp_root(label);
        fs::create_dir(&local_root).unwrap();
        let local_root = fs::canonicalize(local_root).unwrap();

        let storage = Storage::open_in_memory().unwrap();
        let provider_id = ProviderId::new("google-drive").unwrap();
        let account =
            ProviderAccount::new(provider_id.clone(), "phase5d9-subject", None, None).unwrap();
        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            format!("phase5d9-{label}"),
            provider_id,
            account.subject,
            local_root.to_str().unwrap(),
            Some("canonical-root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        storage.insert_sync_root(&root).unwrap();

        (storage, root, local_root)
    }

    fn finalize_snapshot(storage: &mut Storage, root: &SyncRoot, items: &[RemoteItem]) {
        storage
            .begin_sync_root_remote_inventory_staging(&root.id)
            .unwrap();
        storage
            .stage_sync_root_remote_inventory_items(&root.id, items, 3)
            .unwrap();
        storage
            .commit_sync_root_remote_inventory_snapshot(
                &root.id,
                &ChangeCursor::new("phase5d9-fence").unwrap(),
                4,
            )
            .unwrap();

        execute_selected_root_change_batch(
            &Phase5d9RootProvider,
            storage,
            root,
            &ChangeCursor::new("phase5d9-fence").unwrap(),
            &[],
            &ChangeCursor::new("phase5d9-ready").unwrap(),
            5,
        )
        .unwrap();
    }

    #[test]
    fn phase5d9_dispatches_directory_then_file_across_supervised_invocations() {
        let (mut storage, root, local_root) = base_storage("directory-file");
        let bytes = b"phase-5d9-content".to_vec();

        let folder = RemoteItem {
            remote_id: "folder-a".into(),
            parent_remote_id: Some("canonical-root".into()),
            name: "folder-a".into(),
            kind: RemoteItemKind::Folder,
            size_bytes: None,
            modified_unix_ms: None,
            trashed: false,
        };
        let file = RemoteItem {
            remote_id: "file-a".into(),
            parent_remote_id: Some("folder-a".into()),
            name: "file-a.txt".into(),
            kind: RemoteItemKind::File,
            size_bytes: Some(u64::try_from(bytes.len()).unwrap()),
            modified_unix_ms: None,
            trashed: false,
        };

        finalize_snapshot(&mut storage, &root, &[folder, file]);

        let initial = plan_selected_root_receive_only_convergence(&storage, &root).unwrap();
        assert_eq!(initial.create_directories, 1);
        assert_eq!(initial.materialize_missing_files, 1);
        assert_eq!(initial.action_count(), 2);

        let first = execute_selected_root_unified_convergence_step::<Phase5d9ContentProvider>(
            None,
            &mut storage,
            &root,
        )
        .unwrap();

        assert_eq!(
            first.phase_executed,
            Some(SelectedRootUnifiedConvergencePhase::CreateDirectories)
        );
        assert_eq!(first.directories_created, 1);
        assert_eq!(first.phase_actions_executed, 1);
        assert!(!first.converged);
        assert!(first.requires_another_invocation);
        assert_eq!(
            first.next_phase,
            Some(SelectedRootUnifiedConvergencePhase::MaterializeMissingFiles)
        );
        assert_eq!(
            first.stop_reason,
            SelectedRootUnifiedConvergenceStopReason::PhaseCompleted
        );

        let provider = Phase5d9ContentProvider {
            blobs: HashMap::from([("file-a".to_string(), bytes.clone())]),
        };

        let second =
            execute_selected_root_unified_convergence_step(Some(&provider), &mut storage, &root)
                .unwrap();

        assert_eq!(
            second.phase_executed,
            Some(SelectedRootUnifiedConvergencePhase::MaterializeMissingFiles)
        );
        assert_eq!(second.files_materialized, 1);
        assert_eq!(second.receipts_recorded, 1);
        assert!(second.converged);
        assert!(!second.requires_another_invocation);
        assert_eq!(
            second.stop_reason,
            SelectedRootUnifiedConvergenceStopReason::Converged
        );

        let final_plan = plan_selected_root_receive_only_convergence(&storage, &root).unwrap();
        assert_eq!(final_plan.action_count(), 0);
        assert_eq!(final_plan.current_owned_files, 1);
        assert_eq!(final_plan.current_owned_directories, 1);

        fs::remove_file(local_root.join("folder-a").join("file-a.txt")).unwrap();
        fs::remove_dir(local_root.join("folder-a")).unwrap();
        fs::remove_dir(local_root).unwrap();
    }

    #[test]
    fn phase5d9_mixed_file_classes_safe_stop_without_mutation() {
        let (mut storage, root, local_root) = base_storage("mixed");
        let a = b"existing-content".to_vec();
        let b = b"missing-content".to_vec();

        fs::write(local_root.join("existing.txt"), &a).unwrap();

        let first = RemoteItem {
            remote_id: "existing".into(),
            parent_remote_id: Some("canonical-root".into()),
            name: "existing.txt".into(),
            kind: RemoteItemKind::File,
            size_bytes: Some(u64::try_from(a.len()).unwrap()),
            modified_unix_ms: None,
            trashed: false,
        };
        let second = RemoteItem {
            remote_id: "missing".into(),
            parent_remote_id: Some("canonical-root".into()),
            name: "missing.txt".into(),
            kind: RemoteItemKind::File,
            size_bytes: Some(u64::try_from(b.len()).unwrap()),
            modified_unix_ms: None,
            trashed: false,
        };

        finalize_snapshot(&mut storage, &root, &[first, second]);

        let pre = plan_selected_root_receive_only_convergence(&storage, &root).unwrap();
        assert_eq!(pre.verify_existing_files, 1);
        assert_eq!(pre.materialize_missing_files, 1);
        assert_eq!(pre.action_count(), 2);

        let result = execute_selected_root_unified_convergence_step::<Phase5d9ContentProvider>(
            None,
            &mut storage,
            &root,
        )
        .unwrap();

        assert_eq!(result.phase_executed, None);
        assert_eq!(result.phase_actions_executed, 0);
        assert_eq!(
            result.stop_reason,
            SelectedRootUnifiedConvergenceStopReason::MixedActionClasses
        );
        assert!(result.manual_intervention_required);
        assert!(!result.requires_another_invocation);
        assert_eq!(result.final_actions, 2);
        assert_eq!(
            storage
                .sync_root_materialization_receipt_count(&root.id)
                .unwrap(),
            0
        );
        assert!(!local_root.join("missing.txt").exists());
        assert_eq!(fs::read(local_root.join("existing.txt")).unwrap(), a);

        fs::remove_file(local_root.join("existing.txt")).unwrap();
        fs::remove_dir(local_root).unwrap();
    }

    #[test]
    fn phase5d9_converged_state_is_a_noop() {
        let (mut storage, root, local_root) = base_storage("noop");
        finalize_snapshot(&mut storage, &root, &[]);

        let result = execute_selected_root_unified_convergence_step::<Phase5d9ContentProvider>(
            None,
            &mut storage,
            &root,
        )
        .unwrap();

        assert_eq!(result.initial_actions, 0);
        assert_eq!(result.phase_executed, None);
        assert_eq!(result.final_actions, 0);
        assert!(result.converged);
        assert_eq!(
            result.stop_reason,
            SelectedRootUnifiedConvergenceStopReason::Converged
        );

        fs::remove_dir(local_root).unwrap();
    }
}

#[cfg(test)]
mod phase5e1_receive_only_cycle_tests {
    use super::*;

    #[test]
    fn phase5e1_metadata_phase_state_machine_is_explicit() {
        assert_eq!(
            classify_selected_root_receive_only_cycle_metadata_phase(false, None),
            SelectedRootReceiveOnlyCycleMetadataPhase::Bootstrap
        );
        assert_eq!(
            classify_selected_root_receive_only_cycle_metadata_phase(true, None),
            SelectedRootReceiveOnlyCycleMetadataPhase::CollectChangePage
        );
        assert_eq!(
            classify_selected_root_receive_only_cycle_metadata_phase(true, Some(false)),
            SelectedRootReceiveOnlyCycleMetadataPhase::CollectChangePage
        );
        assert_eq!(
            classify_selected_root_receive_only_cycle_metadata_phase(true, Some(true)),
            SelectedRootReceiveOnlyCycleMetadataPhase::ExecuteWindow
        );
    }

    #[test]
    fn phase5e1_metadata_phase_names_are_private_aggregate_labels() {
        assert_eq!(
            SelectedRootReceiveOnlyCycleMetadataPhase::Bootstrap.as_str(),
            "bootstrap"
        );
        assert_eq!(
            SelectedRootReceiveOnlyCycleMetadataPhase::CollectChangePage.as_str(),
            "collect_change_page"
        );
        assert_eq!(
            SelectedRootReceiveOnlyCycleMetadataPhase::ExecuteWindow.as_str(),
            "execute_window"
        );
    }
}

#[cfg(test)]
mod phase5e2_receive_only_run_tests {
    use super::*;

    #[test]
    fn phase5e2_resumes_existing_convergence_before_new_metadata() {
        assert_eq!(
            classify_selected_root_receive_only_run_start(true, true, false, 1),
            SelectedRootReceiveOnlyRunMode::Convergence
        );
        assert_eq!(
            classify_selected_root_receive_only_run_start(true, true, false, 3),
            SelectedRootReceiveOnlyRunMode::Convergence
        );
    }

    #[test]
    fn phase5e2_metadata_wins_when_remote_state_is_not_stable_or_local_is_idle() {
        assert_eq!(
            classify_selected_root_receive_only_run_start(false, false, false, 0),
            SelectedRootReceiveOnlyRunMode::Metadata
        );
        assert_eq!(
            classify_selected_root_receive_only_run_start(true, false, false, 2),
            SelectedRootReceiveOnlyRunMode::Metadata
        );
        assert_eq!(
            classify_selected_root_receive_only_run_start(true, true, true, 2),
            SelectedRootReceiveOnlyRunMode::Metadata
        );
        assert_eq!(
            classify_selected_root_receive_only_run_start(true, true, false, 0),
            SelectedRootReceiveOnlyRunMode::Metadata
        );
    }

    #[test]
    fn phase5e2_round_budget_and_stop_labels_are_fixed() {
        assert_eq!(SUPERVISED_RECEIVE_ONLY_RUN_MAX_ROUNDS, 8);
        assert_eq!(
            SelectedRootReceiveOnlyRunStopReason::Converged.as_str(),
            "converged"
        );
        assert_eq!(
            SelectedRootReceiveOnlyRunStopReason::RoundBudgetExhausted.as_str(),
            "round_budget_exhausted"
        );
        assert_eq!(
            SelectedRootReceiveOnlyRunStopReason::ManualInterventionRequired.as_str(),
            "manual_intervention_required"
        );
    }
}

#[cfg(test)]
mod phase5e3_single_flight_tests {
    use super::*;

    fn unique_root_id(label: &str) -> String {
        format!(
            "phase5e3-{label}-{}-{}",
            std::process::id(),
            DOWNLOAD_TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        )
    }

    #[test]
    fn phase5e3_same_root_is_busy_until_guard_drops() {
        let root_id = unique_root_id("same-root");

        assert_eq!(
            selected_root_receive_only_single_flight_status(&root_id).unwrap(),
            SelectedRootReceiveOnlySingleFlightStatus::Idle
        );

        let first = try_acquire_selected_root_receive_only_run_slot(&root_id)
            .unwrap()
            .expect("first acquisition must succeed");

        assert_eq!(
            selected_root_receive_only_single_flight_status(&root_id).unwrap(),
            SelectedRootReceiveOnlySingleFlightStatus::Busy
        );
        assert!(
            try_acquire_selected_root_receive_only_run_slot(&root_id)
                .unwrap()
                .is_none()
        );

        drop(first);

        assert_eq!(
            selected_root_receive_only_single_flight_status(&root_id).unwrap(),
            SelectedRootReceiveOnlySingleFlightStatus::Idle
        );

        let second = try_acquire_selected_root_receive_only_run_slot(&root_id)
            .unwrap()
            .expect("slot must be reusable after release");
        drop(second);
    }

    #[test]
    fn phase5e3_different_roots_can_run_independently() {
        let root_a = unique_root_id("root-a");
        let root_b = unique_root_id("root-b");

        let guard_a = try_acquire_selected_root_receive_only_run_slot(&root_a)
            .unwrap()
            .expect("root A acquisition must succeed");
        let guard_b = try_acquire_selected_root_receive_only_run_slot(&root_b)
            .unwrap()
            .expect("root B acquisition must succeed");

        assert_eq!(
            selected_root_receive_only_single_flight_status(&root_a).unwrap(),
            SelectedRootReceiveOnlySingleFlightStatus::Busy
        );
        assert_eq!(
            selected_root_receive_only_single_flight_status(&root_b).unwrap(),
            SelectedRootReceiveOnlySingleFlightStatus::Busy
        );

        drop(guard_a);
        drop(guard_b);
    }

    #[test]
    fn phase5e3_status_labels_are_explicit() {
        assert_eq!(
            SelectedRootReceiveOnlySingleFlightStatus::Idle.as_str(),
            "idle"
        );
        assert_eq!(
            SelectedRootReceiveOnlySingleFlightStatus::Busy.as_str(),
            "busy"
        );
        assert_eq!(
            SelectedRootReceiveOnlySingleFlightResult::Busy.status(),
            SelectedRootReceiveOnlySingleFlightStatus::Busy
        );
    }
}

#[cfg(test)]
mod phase5e4_periodic_scheduler_tests {
    use super::*;

    #[test]
    fn phase5e4_scheduler_is_due_immediately_then_waits_normal_interval() {
        let mut state = SelectedRootReceiveOnlyPeriodicState::new_immediate(1_000);

        assert_eq!(
            state.decision(1_000),
            SelectedRootReceiveOnlyPeriodicDecision::Due
        );

        state.schedule_success(1_000);

        assert_eq!(state.consecutive_failures(), 0);
        assert_eq!(state.next_due_unix_ms(), 31_000);
        assert_eq!(
            state.decision(1_001),
            SelectedRootReceiveOnlyPeriodicDecision::Waiting { delay_ms: 29_999 }
        );
        assert_eq!(
            state.decision(31_000),
            SelectedRootReceiveOnlyPeriodicDecision::Due
        );
    }

    #[test]
    fn phase5e4_busy_uses_short_retry_without_counting_as_failure() {
        let mut state = SelectedRootReceiveOnlyPeriodicState::new_immediate(5_000);

        state.schedule_busy_retry(5_000);

        assert_eq!(state.consecutive_failures(), 0);
        assert_eq!(state.next_due_unix_ms(), 10_000);
        assert_eq!(
            state.decision(9_000),
            SelectedRootReceiveOnlyPeriodicDecision::Waiting { delay_ms: 1_000 }
        );
    }

    #[test]
    fn phase5e4_failures_back_off_and_success_resets_counter() {
        let mut state = SelectedRootReceiveOnlyPeriodicState::new_immediate(0);

        state.schedule_failure(0);
        assert_eq!(state.consecutive_failures(), 1);
        assert_eq!(state.next_due_unix_ms(), 5_000);

        state.schedule_failure(5_000);
        assert_eq!(state.consecutive_failures(), 2);
        assert_eq!(state.next_due_unix_ms(), 15_000);

        state.schedule_failure(15_000);
        assert_eq!(state.consecutive_failures(), 3);
        assert_eq!(state.next_due_unix_ms(), 35_000);

        for _ in 0..16 {
            state.schedule_failure(state.next_due_unix_ms());
        }
        assert_eq!(
            state.error_backoff_ms(),
            RECEIVE_ONLY_PERIODIC_ERROR_BACKOFF_MAX_MS
        );

        state.schedule_success(1_000_000);
        assert_eq!(state.consecutive_failures(), 0);
        assert_eq!(
            state.next_due_unix_ms(),
            1_000_000 + RECEIVE_ONLY_PERIODIC_POLL_INTERVAL_MS
        );
    }

    #[test]
    fn phase5e4_manual_intervention_pause_can_resume_but_shutdown_is_sticky() {
        let mut state = SelectedRootReceiveOnlyPeriodicState::new_immediate(10);

        state.pause_for_manual_intervention();
        assert!(state.paused_for_manual_intervention());
        assert_eq!(
            state.decision(10),
            SelectedRootReceiveOnlyPeriodicDecision::PausedForManualIntervention
        );

        state.resume_after_manual_intervention(20);
        assert!(!state.paused_for_manual_intervention());
        assert_eq!(
            state.decision(20),
            SelectedRootReceiveOnlyPeriodicDecision::Due
        );

        state.request_shutdown();
        assert!(state.shutdown_requested());
        assert_eq!(
            state.decision(20),
            SelectedRootReceiveOnlyPeriodicDecision::Shutdown
        );

        state.resume_after_manual_intervention(30);
        assert_eq!(
            state.decision(30),
            SelectedRootReceiveOnlyPeriodicDecision::Shutdown
        );
    }

    #[test]
    fn phase5e4_policy_constants_are_explicit_and_bounded() {
        assert_eq!(RECEIVE_ONLY_PERIODIC_POLL_INTERVAL_MS, 30_000);
        assert_eq!(RECEIVE_ONLY_PERIODIC_BUSY_RETRY_MS, 5_000);
        assert_eq!(RECEIVE_ONLY_PERIODIC_ERROR_BACKOFF_INITIAL_MS, 5_000);
        assert_eq!(RECEIVE_ONLY_PERIODIC_ERROR_BACKOFF_MAX_MS, 300_000);
    }
}

#[cfg(test)]
mod phase5f2_local_baseline_tests {
    use super::*;
    use std::{
        fs::{self, File},
        os::unix::fs::symlink,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn temp_root(label: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("nubisync-{label}-{stamp}"));
        fs::create_dir_all(&path).unwrap();
        fs::canonicalize(path).unwrap()
    }

    fn sync_root(path: &Path) -> SyncRoot {
        SyncRoot::new(
            "phase5f2-root",
            nubisync_core::ProviderId::new("google-drive").unwrap(),
            "phase5f2-subject",
            path.to_str().unwrap(),
            Some("remote-root".into()),
            SyncMode::ReceiveOnly,
            1,
        )
        .unwrap()
    }

    #[test]
    fn phase5f2_snapshot_scan_is_deterministic_and_metadata_only() {
        let root = temp_root("baseline");
        fs::create_dir(root.join("docs")).unwrap();
        let mut file = File::create(root.join("docs/file.txt")).unwrap();
        file.write_all(b"hello").unwrap();
        drop(file);

        let selected = sync_root(&root);
        let first = scan_selected_root_local_snapshot(&selected).unwrap();
        let second = scan_selected_root_local_snapshot(&selected).unwrap();

        assert_eq!(first, second);
        assert_eq!(first.len(), 2);
        assert_eq!(
            first
                .iter()
                .filter(|item| item.kind() == LocalItemKind::File)
                .count(),
            1
        );
        assert_eq!(
            first
                .iter()
                .find(|item| item.kind() == LocalItemKind::File)
                .unwrap()
                .size_bytes(),
            Some(5)
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn phase5f2_snapshot_scan_rejects_symlinks() {
        let root = temp_root("symlink");
        File::create(root.join("target.txt")).unwrap();
        symlink(root.join("target.txt"), root.join("link.txt")).unwrap();

        let selected = sync_root(&root);
        assert!(matches!(
            scan_selected_root_local_snapshot(&selected),
            Err(SelectedRootExecutorError::LocalEntrySymlinkUnsupported)
        ));

        fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(test)]
mod phase5f3_local_diff_tests {
    use super::*;

    fn file(path: &str, size: u64, mtime: i64, dev: u64, inode: u64) -> LocalItemSnapshot {
        LocalItemSnapshot::new(path, LocalItemKind::File, Some(size), mtime, dev, inode).unwrap()
    }

    fn directory(path: &str, mtime: i64, dev: u64, inode: u64) -> LocalItemSnapshot {
        LocalItemSnapshot::new(path, LocalItemKind::Directory, None, mtime, dev, inode).unwrap()
    }

    #[test]
    fn phase5f3_diff_classifies_create_delete_modify_and_type_change() {
        let baseline = vec![
            directory("docs", 10, 1, 10),
            file("docs/modified.txt", 5, 10, 1, 11),
            file("deleted.txt", 4, 10, 1, 12),
            file("type.txt", 1, 10, 1, 13),
            file("old-name.txt", 8, 10, 1, 99),
        ];

        let current = vec![
            directory("docs", 999, 1, 10),
            file("docs/modified.txt", 6, 11, 1, 11),
            file("created.txt", 3, 12, 1, 14),
            directory("type.txt", 12, 1, 15),
            file("new-name.txt", 8, 10, 1, 99),
        ];

        let diff =
            build_selected_root_local_inventory_diff(&baseline, &current, 1, Some(10)).unwrap();

        assert_eq!(diff.created, 2);
        assert_eq!(diff.deleted, 2);
        assert_eq!(diff.modified, 1);
        assert_eq!(diff.type_changed, 1);
        assert_eq!(diff.action_count(), 6);
        assert!(!diff.clean());

        assert!(diff.entries().iter().any(|entry| {
            entry.relative_path() == "old-name.txt"
                && entry.kind == SelectedRootLocalDiffKind::Deleted
        }));
        assert!(diff.entries().iter().any(|entry| {
            entry.relative_path() == "new-name.txt"
                && entry.kind == SelectedRootLocalDiffKind::Created
        }));

        assert!(!diff.entries().iter().any(|entry| {
            entry.relative_path() == "docs" && entry.kind == SelectedRootLocalDiffKind::Modified
        }));
    }

    #[test]
    fn phase5f3_diff_entry_debug_redacts_relative_path() {
        let entry = SelectedRootLocalDiffEntry {
            relative_path: "private/secret.txt".to_owned(),
            kind: SelectedRootLocalDiffKind::Created,
            baseline_kind: None,
            current_kind: Some(LocalItemKind::File),
        };

        let debug = format!("{entry:?}");
        assert!(!debug.contains("private/secret.txt"));
        assert!(debug.contains("[redacted]"));
    }

    #[test]
    fn phase5f3_clean_diff_has_zero_actions() {
        let baseline = vec![
            directory("docs", 10, 1, 10),
            file("docs/file.txt", 5, 10, 1, 11),
        ];
        let current = baseline.clone();

        let diff =
            build_selected_root_local_inventory_diff(&baseline, &current, 1, Some(10)).unwrap();

        assert!(diff.clean());
        assert_eq!(diff.action_count(), 0);
        assert_eq!(diff.created, 0);
        assert_eq!(diff.deleted, 0);
        assert_eq!(diff.modified, 0);
        assert_eq!(diff.type_changed, 0);
    }
}

#[cfg(test)]
mod phase5f4_local_journal_tests {
    use super::*;

    #[test]
    fn phase5f4_diff_kind_maps_to_durable_event_kind() {
        assert_eq!(
            local_change_event_kind(SelectedRootLocalDiffKind::Created),
            LocalChangeEventKind::Created
        );
        assert_eq!(
            local_change_event_kind(SelectedRootLocalDiffKind::Deleted),
            LocalChangeEventKind::Deleted
        );
        assert_eq!(
            local_change_event_kind(SelectedRootLocalDiffKind::Modified),
            LocalChangeEventKind::Modified
        );
        assert_eq!(
            local_change_event_kind(SelectedRootLocalDiffKind::TypeChanged),
            LocalChangeEventKind::TypeChanged
        );
    }
}

#[cfg(test)]
mod phase5f5_periodic_local_observation_tests {
    use super::*;

    #[test]
    fn phase5f5_local_observation_requires_fully_converged_receive_only_run() {
        assert!(receive_only_execution_ready_for_local_observation(
            true, false, false
        ));
        assert!(!receive_only_execution_ready_for_local_observation(
            false, false, false
        ));
        assert!(!receive_only_execution_ready_for_local_observation(
            true, true, false
        ));
        assert!(!receive_only_execution_ready_for_local_observation(
            true, false, true
        ));
    }

    #[test]
    fn phase5f5_periodic_status_distinguishes_invalidated_baseline() {
        assert_eq!(
            SelectedRootPeriodicLocalObservation::BaselineMissing.as_str(),
            "baseline_missing"
        );
        assert_eq!(
            SelectedRootPeriodicLocalObservation::BaselineInvalidated.as_str(),
            "baseline_invalidated"
        );
        assert_eq!(
            SelectedRootPeriodicLocalObservation::DeferredUntilReceiveOnlyConverged.as_str(),
            "deferred_until_receive_only_converged"
        );
    }
}

#[cfg(test)]
mod phase5g_cross_process_execution_tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_lock_path(label: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "nubisync-phase5g-{label}-{}-{stamp}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir.join("execution.lock")
    }

    #[test]
    fn phase5g_exclusive_lock_is_busy_then_released_by_guard_drop() {
        let path = temp_lock_path("exclusive");

        let first = match try_acquire_selected_root_cross_process_execution_lock(&path).unwrap() {
            SelectedRootCrossProcessExecutionLock::Acquired(guard) => guard,
            SelectedRootCrossProcessExecutionLock::Busy => panic!("first lock must acquire"),
        };

        assert!(matches!(
            try_acquire_selected_root_cross_process_execution_lock(&path).unwrap(),
            SelectedRootCrossProcessExecutionLock::Busy
        ));

        drop(first);

        let second = match try_acquire_selected_root_cross_process_execution_lock(&path).unwrap() {
            SelectedRootCrossProcessExecutionLock::Acquired(guard) => guard,
            SelectedRootCrossProcessExecutionLock::Busy => {
                panic!("lock must be released when guard drops")
            }
        };
        drop(second);

        fs::remove_file(&path).unwrap();
        fs::remove_dir(path.parent().unwrap()).unwrap();
    }

    #[test]
    fn phase5g_lock_rejects_symlink_lockfile() {
        use std::os::unix::fs::symlink;

        let path = temp_lock_path("symlink");
        let target = path.parent().unwrap().join("target");
        fs::write(&target, b"").unwrap();
        symlink(&target, &path).unwrap();

        assert!(matches!(
            try_acquire_selected_root_cross_process_execution_lock(&path),
            Err(SelectedRootExecutorError::CrossProcessExecutionLockPathInvalid)
        ));

        fs::remove_file(&path).unwrap();
        fs::remove_file(&target).unwrap();
        fs::remove_dir(path.parent().unwrap()).unwrap();
    }
}

#[cfg(test)]
mod phase5h3_remote_write_intent_planner_tests {
    use super::*;

    #[test]
    fn phase5h3_derives_create_needing_id_and_exact_update_candidate() {
        let baseline = vec![
            LocalItemSnapshot::new("docs", LocalItemKind::Directory, None, 10, 8, 100).unwrap(),
            LocalItemSnapshot::new(
                "docs/existing.txt",
                LocalItemKind::File,
                Some(4),
                11,
                8,
                101,
            )
            .unwrap(),
        ];
        let current = vec![
            baseline[0].clone(),
            LocalItemSnapshot::new(
                "docs/existing.txt",
                LocalItemKind::File,
                Some(5),
                20,
                8,
                101,
            )
            .unwrap(),
            LocalItemSnapshot::new("docs/new.txt", LocalItemKind::File, Some(3), 21, 8, 102)
                .unwrap(),
        ];
        let events = vec![
            LocalChangeEventRecord::new(
                1,
                1,
                "docs/existing.txt",
                LocalChangeEventKind::Modified,
                Some(LocalItemKind::File),
                Some(LocalItemKind::File),
            )
            .unwrap(),
            LocalChangeEventRecord::new(
                2,
                1,
                "docs/new.txt",
                LocalChangeEventKind::Created,
                None,
                Some(LocalItemKind::File),
            )
            .unwrap(),
        ];
        let remote_items = vec![
            RemoteItem {
                remote_id: "remote-docs".into(),
                parent_remote_id: Some("remote-root".into()),
                name: "docs".into(),
                kind: RemoteItemKind::Folder,
                size_bytes: None,
                modified_unix_ms: None,
                trashed: false,
            },
            RemoteItem {
                remote_id: "remote-existing".into(),
                parent_remote_id: Some("remote-docs".into()),
                name: "existing.txt".into(),
                kind: RemoteItemKind::File,
                size_bytes: Some(4),
                modified_unix_ms: None,
                trashed: false,
            },
        ];
        let file_receipts = vec![SyncRootFileMaterializationReceipt {
            remote_id: "remote-existing".into(),
            relative_path: "docs/existing.txt".into(),
            size_bytes: 4,
            sha256_hex: "a".repeat(64),
            materialized_at_unix_ms: 5,
        }];
        let directory_receipts = vec![SyncRootDirectoryMaterializationReceipt {
            remote_id: "remote-docs".into(),
            relative_path: "docs".into(),
            materialized_at_unix_ms: 5,
        }];
        let authorities = vec![
            RemoteWriteAuthoritySnapshot::new("remote-root", 5, None, None, true, true, true, 30)
                .unwrap(),
            RemoteWriteAuthoritySnapshot::new("remote-docs", 6, None, None, true, true, true, 30)
                .unwrap(),
            RemoteWriteAuthoritySnapshot::new(
                "remote-existing",
                7,
                Some("md5".into()),
                Some("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into()),
                true,
                true,
                false,
                30,
            )
            .unwrap(),
        ];

        let entries = derive_selected_root_remote_write_plan_entries(
            "remote-root",
            &events,
            &baseline,
            &current,
            &remote_items,
            &file_receipts,
            &directory_receipts,
            &authorities,
        )
        .unwrap();

        assert_eq!(entries.len(), 2);
        assert_eq!(
            entries[0].operation,
            Some(RemoteWriteIntentOperation::UpdateFile)
        );
        assert_eq!(
            entries[0].disposition,
            SelectedRootRemoteWritePlanDisposition::Ready
        );
        assert_eq!(entries[0].expected_remote_version, Some(7));
        assert_eq!(
            entries[1].operation,
            Some(RemoteWriteIntentOperation::CreateFile)
        );
        assert_eq!(
            entries[1].disposition,
            SelectedRootRemoteWritePlanDisposition::NeedsPredeterminedRemoteId
        );

        let debug = format!("{entries:?}");
        assert!(!debug.contains("docs/existing.txt"));
        assert!(!debug.contains("docs/new.txt"));
        assert!(!debug.contains("remote-existing"));
        assert!(!debug.contains("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));
    }

    #[test]
    fn phase5h3_type_change_is_conflict_and_nested_delete_is_fail_closed() {
        let baseline = vec![
            LocalItemSnapshot::new("folder", LocalItemKind::Directory, None, 10, 8, 100).unwrap(),
            LocalItemSnapshot::new("folder/child.txt", LocalItemKind::File, Some(4), 11, 8, 101)
                .unwrap(),
            LocalItemSnapshot::new("shape", LocalItemKind::File, Some(1), 12, 8, 102).unwrap(),
        ];
        let current = vec![
            LocalItemSnapshot::new("shape", LocalItemKind::Directory, None, 20, 8, 103).unwrap(),
        ];
        let events = vec![
            LocalChangeEventRecord::new(
                1,
                1,
                "folder",
                LocalChangeEventKind::Deleted,
                Some(LocalItemKind::Directory),
                None,
            )
            .unwrap(),
            LocalChangeEventRecord::new(
                2,
                1,
                "folder/child.txt",
                LocalChangeEventKind::Deleted,
                Some(LocalItemKind::File),
                None,
            )
            .unwrap(),
            LocalChangeEventRecord::new(
                3,
                1,
                "shape",
                LocalChangeEventKind::TypeChanged,
                Some(LocalItemKind::File),
                Some(LocalItemKind::Directory),
            )
            .unwrap(),
        ];
        let remote_items = vec![
            RemoteItem {
                remote_id: "remote-folder".into(),
                parent_remote_id: Some("remote-root".into()),
                name: "folder".into(),
                kind: RemoteItemKind::Folder,
                size_bytes: None,
                modified_unix_ms: None,
                trashed: false,
            },
            RemoteItem {
                remote_id: "remote-child".into(),
                parent_remote_id: Some("remote-folder".into()),
                name: "child.txt".into(),
                kind: RemoteItemKind::File,
                size_bytes: Some(4),
                modified_unix_ms: None,
                trashed: false,
            },
        ];
        let file_receipts = vec![SyncRootFileMaterializationReceipt {
            remote_id: "remote-child".into(),
            relative_path: "folder/child.txt".into(),
            size_bytes: 4,
            sha256_hex: "a".repeat(64),
            materialized_at_unix_ms: 5,
        }];
        let directory_receipts = vec![SyncRootDirectoryMaterializationReceipt {
            remote_id: "remote-folder".into(),
            relative_path: "folder".into(),
            materialized_at_unix_ms: 5,
        }];
        let authorities = vec![
            RemoteWriteAuthoritySnapshot::new("remote-root", 5, None, None, true, true, true, 30)
                .unwrap(),
            RemoteWriteAuthoritySnapshot::new("remote-folder", 6, None, None, true, true, true, 30)
                .unwrap(),
            RemoteWriteAuthoritySnapshot::new("remote-child", 7, None, None, true, true, false, 30)
                .unwrap(),
        ];

        let entries = derive_selected_root_remote_write_plan_entries(
            "remote-root",
            &events,
            &baseline,
            &current,
            &remote_items,
            &file_receipts,
            &directory_receipts,
            &authorities,
        )
        .unwrap();

        assert_eq!(
            entries[0].disposition,
            SelectedRootRemoteWritePlanDisposition::Ready
        );
        assert_eq!(
            entries[1].disposition,
            SelectedRootRemoteWritePlanDisposition::BlockedIdentity
        );
        assert_eq!(
            entries[2].disposition,
            SelectedRootRemoteWritePlanDisposition::Conflict
        );
    }
}

#[cfg(test)]
mod phase5h6_create_id_planner_tests {
    use super::*;

    #[test]
    fn phase5h6_create_plan_entry_builds_redacted_storage_input() {
        let entry = SelectedRootRemoteWritePlanEntry {
            source_local_event_id: 7,
            relative_path: "private/new.txt".into(),
            operation: Some(RemoteWriteIntentOperation::CreateFile),
            disposition: SelectedRootRemoteWritePlanDisposition::NeedsPredeterminedRemoteId,
            local_kind: Some(LocalItemKind::File),
            local_size_bytes: Some(4),
            local_modified_unix_ns: Some(10),
            local_device_id: Some(8),
            local_inode: Some(9),
            target_remote_id: None,
            expected_parent_remote_id: Some("private-parent".into()),
            expected_remote_kind: None,
            expected_remote_version: None,
            expected_remote_size_bytes: None,
            expected_checksum_algorithm: None,
            expected_content_checksum: None,
        };

        let input = entry
            .create_intent_input(2, "generated-private-id".into(), 20)
            .unwrap();

        assert_eq!(input.operation, RemoteWriteIntentOperation::CreateFile);
        assert_eq!(input.baseline_generation, 2);
        assert_eq!(
            input.predetermined_remote_id(),
            Some("generated-private-id")
        );

        let debug = format!("{input:?}");
        assert!(!debug.contains("private/new.txt"));
        assert!(!debug.contains("private-parent"));
        assert!(!debug.contains("generated-private-id"));
    }
}

#[cfg(test)]
mod phase5h9_folder_create_local_validation_tests {
    use super::*;

    #[test]
    fn phase5h9_folder_create_local_identity_uses_safe_double_scan() {
        let root_path =
            std::env::temp_dir().join(format!("nubisync-phase5h9-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root_path);
        fs::create_dir(&root_path).unwrap();
        let child = root_path.join("folder");
        fs::create_dir(&child).unwrap();

        let metadata = fs::symlink_metadata(&child).unwrap();
        let modified_unix_ns = metadata
            .mtime()
            .checked_mul(1_000_000_000)
            .and_then(|value| value.checked_add(metadata.mtime_nsec()))
            .unwrap();

        let root = SyncRoot::new(
            "phase5h9-root",
            nubisync_core::ProviderId::new("google-drive").unwrap(),
            "phase5h9-subject",
            root_path.to_str().unwrap(),
            Some("remote-root".into()),
            SyncMode::TwoWay,
            1,
        )
        .unwrap();

        let validation = validate_selected_root_folder_create_local_identity(
            &root,
            "folder",
            modified_unix_ns,
            metadata.dev(),
            metadata.ino(),
        )
        .unwrap();

        assert_eq!(validation.leaf_name(), "folder");
        assert!(!format!("{validation:?}").contains("folder"));

        fs::remove_dir(&child).unwrap();
        fs::remove_dir(&root_path).unwrap();
    }
}
