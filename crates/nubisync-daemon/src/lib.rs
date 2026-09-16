//! NubiSync synchronization orchestration owned by the daemon layer.

#![forbid(unsafe_code)]

use nubisync_core::{
    ChangeCursor, ChangePage, ContinuationToken, RemoteChange, RemoteItem, RemoteItemKind, SyncRoot,
};
use nubisync_drive::{DriveApiError, DriveFolderRoot, DriveRootMembership, GoogleDriveApi};
use nubisync_storage::{
    Storage, StorageError, SyncRootCatalogBatchCommit, SyncRootCatalogMutation,
};
use nubisync_sync::{
    LocalTreeEntry, LocalTreeEntryKind, ReceiveOnlyDirectoryTarget, ReceiveOnlyFileTarget,
    ReceiveOnlyMaterializationPlan, ReceiveOnlyMaterializationPlanError, RootCatalogMutationPlan,
    RootCatalogProjection, RootCatalogProjectionError, RootCatalogResolution, RootChangeMembership,
    plan_receive_only_directory_targets, plan_receive_only_existing_file_targets,
    plan_receive_only_materialization, plan_receive_only_missing_file_targets,
};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashSet, VecDeque},
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedRootDirectoryMaterialization {
    pub remote_directories: usize,
    pub created_directories: usize,
    pub existing_directories: usize,
    pub pending_files: usize,
}

pub const SUPERVISED_FILE_DOWNLOAD_MAX_BYTES: u64 = 16 * 1024 * 1024;
static DOWNLOAD_TEMP_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedRootFileMaterialization {
    pub files_downloaded: usize,
    pub bytes_downloaded: u64,
    pub max_file_bytes: u64,
    pub size_match_verified: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedRootFileVerification {
    pub files_verified: usize,
    pub bytes_verified: u64,
    pub hash_match: bool,
    pub receipt_recorded: bool,
}

pub trait SelectedRootContentProvider {
    fn download_file_content(
        &self,
        remote_id: &str,
        max_bytes: u64,
        writer: &mut dyn Write,
    ) -> Result<u64, SelectedRootExecutorError>;
}

impl SelectedRootContentProvider for GoogleDriveApi {
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

pub fn materialize_selected_root_directories(
    storage: &Storage,
    sync_root: &SyncRoot,
) -> Result<SelectedRootDirectoryMaterialization, SelectedRootExecutorError> {
    let (remote_items, local_entries) = selected_root_materialization_inputs(storage, sync_root)?;
    let preflight = plan_receive_only_materialization(&remote_items, &local_entries)?;

    if !preflight.ready_for_directory_phase() {
        return Err(SelectedRootExecutorError::LocalDirectoryPhaseBlocked);
    }

    let targets = plan_receive_only_directory_targets(&remote_items)?;
    if targets.len() != preflight.remote_directories {
        return Err(SelectedRootExecutorError::LocalDirectoryTargetCountMismatch);
    }

    let root_path = validated_selected_root_path(sync_root)?;
    let outcome = apply_selected_root_directory_targets(&root_path, &targets)?;

    let post_result = (|| {
        let post_entries = scan_selected_root_local_tree(sync_root)?;
        let post_plan = plan_receive_only_materialization(&remote_items, &post_entries)?;

        if !post_plan.ready_for_directory_phase()
            || post_plan.missing_directories != 0
            || post_plan.matching_directories != post_plan.remote_directories
            || outcome.created_paths.len() + outcome.existing_directories
                != post_plan.remote_directories
        {
            return Err(SelectedRootExecutorError::LocalDirectoryPostconditionFailed);
        }

        Ok(SelectedRootDirectoryMaterialization {
            remote_directories: post_plan.remote_directories,
            created_directories: outcome.created_paths.len(),
            existing_directories: outcome.existing_directories,
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
    existing_directories: usize,
}

fn apply_selected_root_directory_targets(
    root_path: &Path,
    targets: &[ReceiveOnlyDirectoryTarget],
) -> Result<DirectoryApplyOutcome, SelectedRootExecutorError> {
    let mut created_paths = Vec::new();
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
    #[error("local file verification is blocked by the current receive-only state")]
    LocalFileVerificationPhaseBlocked,
    #[error("local file verification failed")]
    LocalFileVerifyFailed,
    #[error("local file SHA-256 does not match current remote content")]
    LocalFileHashMismatch,
    #[error("system clock is before the Unix epoch")]
    ClockBeforeUnixEpoch,
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

    struct FakeContentProvider {
        bytes: Vec<u8>,
    }
    impl SelectedRootContentProvider for FakeContentProvider {
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
}
