//! Transactional local metadata storage for NubiSync.

#![forbid(unsafe_code)]

use nubisync_core::{
    ChangeCursor, ContinuationToken, LocalItemKind, LocalItemSnapshot, ProviderAccount, ProviderId,
    RemoteChange, RemoteItem, RemoteItemKind, SyncMode, SyncRoot,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use std::path::Path;
use thiserror::Error;

const SCHEMA_VERSION: i64 = 16;

type RemoteInventoryStateRow = (i64, i64, i64, Option<i64>, Option<String>);
type SyncRootRemoteItemRow = (Option<String>, String, String, Option<i64>, i64);
type SyncRootRemoteCatalogRow = (String, Option<String>, String, String, Option<i64>, i64);
type SyncRootCursorStateRow = (i64, i64, Option<String>, Option<String>);
type SyncRootChangeWindowStateRow = (String, Option<String>, Option<String>, i64, i64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalInventoryState {
    pub snapshot_complete: bool,
    pub item_count: u64,
    pub snapshot_completed_at_unix_ms: Option<i64>,
    pub generation: u64,
    pub observation_valid: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalChangeEventKind {
    Created,
    Deleted,
    Modified,
    TypeChanged,
}

impl LocalChangeEventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Deleted => "deleted",
            Self::Modified => "modified",
            Self::TypeChanged => "type_changed",
        }
    }

    fn parse(value: &str) -> Result<Self, StorageError> {
        match value {
            "created" => Ok(Self::Created),
            "deleted" => Ok(Self::Deleted),
            "modified" => Ok(Self::Modified),
            "type_changed" => Ok(Self::TypeChanged),
            _ => Err(StorageError::InvalidStoredLocalChangeEventKind),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct LocalChangeEventInput {
    relative_path: String,
    pub kind: LocalChangeEventKind,
    pub baseline_kind: Option<LocalItemKind>,
    pub current_kind: Option<LocalItemKind>,
}

impl LocalChangeEventInput {
    pub fn new(
        relative_path: impl Into<String>,
        kind: LocalChangeEventKind,
        baseline_kind: Option<LocalItemKind>,
        current_kind: Option<LocalItemKind>,
    ) -> Result<Self, StorageError> {
        let relative_path = relative_path.into();
        if !is_safe_local_event_relative_path(&relative_path) {
            return Err(StorageError::InvalidLocalChangePath);
        }

        match kind {
            LocalChangeEventKind::Created if baseline_kind.is_some() || current_kind.is_none() => {
                return Err(StorageError::InvalidLocalChangeShape);
            }
            LocalChangeEventKind::Deleted if baseline_kind.is_none() || current_kind.is_some() => {
                return Err(StorageError::InvalidLocalChangeShape);
            }
            LocalChangeEventKind::Modified
                if baseline_kind.is_none()
                    || current_kind.is_none()
                    || baseline_kind != current_kind =>
            {
                return Err(StorageError::InvalidLocalChangeShape);
            }
            LocalChangeEventKind::TypeChanged
                if baseline_kind.is_none()
                    || current_kind.is_none()
                    || baseline_kind == current_kind =>
            {
                return Err(StorageError::InvalidLocalChangeShape);
            }
            LocalChangeEventKind::Created
            | LocalChangeEventKind::Deleted
            | LocalChangeEventKind::Modified
            | LocalChangeEventKind::TypeChanged => {}
        }

        Ok(Self {
            relative_path,
            kind,
            baseline_kind,
            current_kind,
        })
    }

    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }
}

impl std::fmt::Debug for LocalChangeEventInput {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalChangeEventInput")
            .field("relative_path", &"[redacted]")
            .field("kind", &self.kind)
            .field("baseline_kind", &self.baseline_kind)
            .field("current_kind", &self.current_kind)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct LocalChangeEventRecord {
    pub id: i64,
    pub baseline_generation: u64,
    relative_path: String,
    pub kind: LocalChangeEventKind,
    pub baseline_kind: Option<LocalItemKind>,
    pub current_kind: Option<LocalItemKind>,
}

impl LocalChangeEventRecord {
    pub fn new(
        id: i64,
        baseline_generation: u64,
        relative_path: impl Into<String>,
        kind: LocalChangeEventKind,
        baseline_kind: Option<LocalItemKind>,
        current_kind: Option<LocalItemKind>,
    ) -> Result<Self, StorageError> {
        if id <= 0 || baseline_generation == 0 {
            return Err(StorageError::InvalidStoredLocalChangeEvent);
        }
        let relative_path = relative_path.into();
        let validated =
            LocalChangeEventInput::new(relative_path.clone(), kind, baseline_kind, current_kind)?;
        Ok(Self {
            id,
            baseline_generation,
            relative_path: validated.relative_path,
            kind,
            baseline_kind,
            current_kind,
        })
    }

    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }
}

impl std::fmt::Debug for LocalChangeEventRecord {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalChangeEventRecord")
            .field("id", &self.id)
            .field("baseline_generation", &self.baseline_generation)
            .field("relative_path", &"[redacted]")
            .field("kind", &self.kind)
            .field("baseline_kind", &self.baseline_kind)
            .field("current_kind", &self.current_kind)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteWriteAuthorityState {
    pub change_cursor: ChangeCursor,
    pub item_count: u64,
    pub observed_at_unix_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalChangeJournalCommit {
    pub baseline_generation: u64,
    pub pending_events: u64,
    pub superseded_events: u64,
    pub current_diff_events: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteWriteIntentOperation {
    CreateFile,
    CreateFolder,
    UpdateFile,
    TrashItem,
}

impl RemoteWriteIntentOperation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CreateFile => "create_file",
            Self::CreateFolder => "create_folder",
            Self::UpdateFile => "update_file",
            Self::TrashItem => "trash_item",
        }
    }

    fn parse(value: &str) -> Result<Self, StorageError> {
        match value {
            "create_file" => Ok(Self::CreateFile),
            "create_folder" => Ok(Self::CreateFolder),
            "update_file" => Ok(Self::UpdateFile),
            "trash_item" => Ok(Self::TrashItem),
            _ => Err(StorageError::InvalidStoredRemoteWriteIntentOperation),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteWriteIntentStatus {
    Planned,
    Submitted,
    AwaitingConfirmation,
    Confirmed,
    Conflict,
    Failed,
    Superseded,
}

impl RemoteWriteIntentStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Submitted => "submitted",
            Self::AwaitingConfirmation => "awaiting_confirmation",
            Self::Confirmed => "confirmed",
            Self::Conflict => "conflict",
            Self::Failed => "failed",
            Self::Superseded => "superseded",
        }
    }

    fn parse(value: &str) -> Result<Self, StorageError> {
        match value {
            "planned" => Ok(Self::Planned),
            "submitted" => Ok(Self::Submitted),
            "awaiting_confirmation" => Ok(Self::AwaitingConfirmation),
            "confirmed" => Ok(Self::Confirmed),
            "conflict" => Ok(Self::Conflict),
            "failed" => Ok(Self::Failed),
            "superseded" => Ok(Self::Superseded),
            _ => Err(StorageError::InvalidStoredRemoteWriteIntentStatus),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct RemoteWriteAuthoritySnapshot {
    remote_id: String,
    pub remote_version: u64,
    checksum_algorithm: Option<String>,
    content_checksum: Option<String>,
    pub can_edit: bool,
    pub can_trash: bool,
    pub can_add_children: bool,
    pub observed_at_unix_ms: i64,
}

impl RemoteWriteAuthoritySnapshot {
    pub fn new(
        remote_id: impl Into<String>,
        remote_version: u64,
        checksum_algorithm: Option<String>,
        content_checksum: Option<String>,
        can_edit: bool,
        can_trash: bool,
        can_add_children: bool,
        observed_at_unix_ms: i64,
    ) -> Result<Self, StorageError> {
        let remote_id = remote_id.into();
        validate_remote_write_identifier(&remote_id)?;
        if remote_version == 0 {
            return Err(StorageError::InvalidRemoteWriteAuthority);
        }
        validate_optional_checksum(checksum_algorithm.as_deref(), content_checksum.as_deref())?;

        Ok(Self {
            remote_id,
            remote_version,
            checksum_algorithm,
            content_checksum,
            can_edit,
            can_trash,
            can_add_children,
            observed_at_unix_ms,
        })
    }

    pub fn remote_id(&self) -> &str {
        &self.remote_id
    }

    pub fn checksum_algorithm(&self) -> Option<&str> {
        self.checksum_algorithm.as_deref()
    }

    pub fn content_checksum(&self) -> Option<&str> {
        self.content_checksum.as_deref()
    }
}

impl std::fmt::Debug for RemoteWriteAuthoritySnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RemoteWriteAuthoritySnapshot")
            .field("remote_id", &"[redacted]")
            .field("remote_version", &self.remote_version)
            .field(
                "checksum_algorithm",
                &self.checksum_algorithm.as_deref().map(|_| "[redacted]"),
            )
            .field(
                "content_checksum",
                &self.content_checksum.as_deref().map(|_| "[redacted]"),
            )
            .field("can_edit", &self.can_edit)
            .field("can_trash", &self.can_trash)
            .field("can_add_children", &self.can_add_children)
            .field("observed_at_unix_ms", &self.observed_at_unix_ms)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct RemoteWriteIntentInput {
    pub source_local_event_id: i64,
    pub baseline_generation: u64,
    pub operation: RemoteWriteIntentOperation,
    relative_path: String,
    pub local_kind: LocalItemKind,
    pub local_size_bytes: Option<u64>,
    pub local_modified_unix_ns: Option<i64>,
    pub local_device_id: Option<u64>,
    pub local_inode: Option<u64>,
    target_remote_id: Option<String>,
    predetermined_remote_id: Option<String>,
    expected_parent_remote_id: Option<String>,
    pub expected_remote_kind: Option<RemoteItemKind>,
    pub expected_remote_version: Option<u64>,
    pub expected_remote_size_bytes: Option<u64>,
    expected_checksum_algorithm: Option<String>,
    expected_content_checksum: Option<String>,
    pub planned_at_unix_ms: i64,
}

impl RemoteWriteIntentInput {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source_local_event_id: i64,
        baseline_generation: u64,
        operation: RemoteWriteIntentOperation,
        relative_path: impl Into<String>,
        local_kind: LocalItemKind,
        local_size_bytes: Option<u64>,
        local_modified_unix_ns: Option<i64>,
        local_device_id: Option<u64>,
        local_inode: Option<u64>,
        target_remote_id: Option<String>,
        predetermined_remote_id: Option<String>,
        expected_parent_remote_id: Option<String>,
        expected_remote_kind: Option<RemoteItemKind>,
        expected_remote_version: Option<u64>,
        expected_remote_size_bytes: Option<u64>,
        expected_checksum_algorithm: Option<String>,
        expected_content_checksum: Option<String>,
        planned_at_unix_ms: i64,
    ) -> Result<Self, StorageError> {
        let relative_path = relative_path.into();
        if source_local_event_id <= 0
            || baseline_generation == 0
            || !is_safe_local_event_relative_path(&relative_path)
        {
            return Err(StorageError::InvalidRemoteWriteIntent);
        }

        for value in [
            target_remote_id.as_deref(),
            predetermined_remote_id.as_deref(),
            expected_parent_remote_id.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            validate_remote_write_identifier(value)?;
        }

        validate_optional_checksum(
            expected_checksum_algorithm.as_deref(),
            expected_content_checksum.as_deref(),
        )?;

        let file_identity = local_size_bytes.is_some()
            && local_modified_unix_ns.is_some()
            && local_device_id.is_some()
            && local_inode.is_some();
        let directory_identity = local_size_bytes.is_none()
            && local_modified_unix_ns.is_some()
            && local_device_id.is_some()
            && local_inode.is_some();

        match operation {
            RemoteWriteIntentOperation::CreateFile => {
                if local_kind != LocalItemKind::File
                    || !file_identity
                    || target_remote_id.is_some()
                    || predetermined_remote_id.is_none()
                    || expected_parent_remote_id.is_none()
                    || expected_remote_kind.is_some()
                    || expected_remote_version.is_some()
                {
                    return Err(StorageError::InvalidRemoteWriteIntent);
                }
            }
            RemoteWriteIntentOperation::CreateFolder => {
                if local_kind != LocalItemKind::Directory
                    || !directory_identity
                    || target_remote_id.is_some()
                    || predetermined_remote_id.is_none()
                    || expected_parent_remote_id.is_none()
                    || expected_remote_kind.is_some()
                    || expected_remote_version.is_some()
                {
                    return Err(StorageError::InvalidRemoteWriteIntent);
                }
            }
            RemoteWriteIntentOperation::UpdateFile => {
                if local_kind != LocalItemKind::File
                    || !file_identity
                    || target_remote_id.is_none()
                    || predetermined_remote_id.is_some()
                    || expected_parent_remote_id.is_none()
                    || expected_remote_kind != Some(RemoteItemKind::File)
                    || expected_remote_version.is_none()
                {
                    return Err(StorageError::InvalidRemoteWriteIntent);
                }
            }
            RemoteWriteIntentOperation::TrashItem => {
                if target_remote_id.is_none()
                    || predetermined_remote_id.is_some()
                    || expected_parent_remote_id.is_none()
                    || expected_remote_kind.is_none()
                    || expected_remote_version.is_none()
                    || local_modified_unix_ns.is_none()
                    || local_device_id.is_none()
                    || local_inode.is_none()
                {
                    return Err(StorageError::InvalidRemoteWriteIntent);
                }
            }
        }

        if expected_remote_version == Some(0) {
            return Err(StorageError::InvalidRemoteWriteIntent);
        }

        Ok(Self {
            source_local_event_id,
            baseline_generation,
            operation,
            relative_path,
            local_kind,
            local_size_bytes,
            local_modified_unix_ns,
            local_device_id,
            local_inode,
            target_remote_id,
            predetermined_remote_id,
            expected_parent_remote_id,
            expected_remote_kind,
            expected_remote_version,
            expected_remote_size_bytes,
            expected_checksum_algorithm,
            expected_content_checksum,
            planned_at_unix_ms,
        })
    }

    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }

    pub fn target_remote_id(&self) -> Option<&str> {
        self.target_remote_id.as_deref()
    }

    pub fn predetermined_remote_id(&self) -> Option<&str> {
        self.predetermined_remote_id.as_deref()
    }

    pub fn expected_parent_remote_id(&self) -> Option<&str> {
        self.expected_parent_remote_id.as_deref()
    }

    pub fn expected_checksum_algorithm(&self) -> Option<&str> {
        self.expected_checksum_algorithm.as_deref()
    }

    pub fn expected_content_checksum(&self) -> Option<&str> {
        self.expected_content_checksum.as_deref()
    }
}

impl std::fmt::Debug for RemoteWriteIntentInput {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RemoteWriteIntentInput")
            .field("source_local_event_id", &self.source_local_event_id)
            .field("baseline_generation", &self.baseline_generation)
            .field("operation", &self.operation)
            .field("relative_path", &"[redacted]")
            .field("local_kind", &self.local_kind)
            .field(
                "target_remote_id",
                &self.target_remote_id.as_deref().map(|_| "[redacted]"),
            )
            .field(
                "predetermined_remote_id",
                &self
                    .predetermined_remote_id
                    .as_deref()
                    .map(|_| "[redacted]"),
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
            .field("planned_at_unix_ms", &self.planned_at_unix_ms)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct RemoteWriteIntentRecord {
    pub id: i64,
    pub source_local_event_id: i64,
    pub baseline_generation: u64,
    pub operation: RemoteWriteIntentOperation,
    relative_path: String,
    pub status: RemoteWriteIntentStatus,
}

impl std::fmt::Debug for RemoteWriteIntentRecord {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RemoteWriteIntentRecord")
            .field("id", &self.id)
            .field("source_local_event_id", &self.source_local_event_id)
            .field("baseline_generation", &self.baseline_generation)
            .field("operation", &self.operation)
            .field("relative_path", &"[redacted]")
            .field("status", &self.status)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteInventoryState {
    pub snapshot_complete: bool,
    pub catchup_complete: bool,
    pub item_count: u64,
    pub snapshot_completed_at_unix_ms: Option<i64>,
    pub catchup_from_cursor: Option<ChangeCursor>,
}

impl RemoteInventoryState {
    pub fn ready_for_reconciliation(&self) -> bool {
        self.snapshot_complete && self.catchup_complete
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogCatchupCommit {
    pub changes_applied: usize,
    pub authoritative_items: u64,
    pub remote_events_superseded: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncRootCatalogMutation {
    Upsert(RemoteItem),
    DeleteSubtree { remote_id: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncRootCatalogBatchCommit {
    pub mutations_applied: usize,
    pub authoritative_items: u64,
    pub completed_initial_catchup: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncRootChangeWindowState {
    pub base_cursor: ChangeCursor,
    pub continuation: Option<ContinuationToken>,
    pub checkpoint: Option<ChangeCursor>,
    pub page_count: u64,
    pub change_count: u64,
}

impl SyncRootChangeWindowState {
    pub fn is_complete(&self) -> bool {
        self.continuation.is_none() && self.checkpoint.is_some()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct SyncRootFileMaterializationReceipt {
    pub remote_id: String,
    pub relative_path: String,
    pub size_bytes: u64,
    pub sha256_hex: String,
    pub materialized_at_unix_ms: i64,
}

impl std::fmt::Debug for SyncRootFileMaterializationReceipt {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SyncRootFileMaterializationReceipt")
            .field("remote_id", &"[redacted]")
            .field("relative_path", &"[redacted]")
            .field("size_bytes", &self.size_bytes)
            .field("sha256_hex", &"[redacted]")
            .field("materialized_at_unix_ms", &self.materialized_at_unix_ms)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct SyncRootDirectoryMaterializationReceipt {
    pub remote_id: String,
    pub relative_path: String,
    pub materialized_at_unix_ms: i64,
}

impl std::fmt::Debug for SyncRootDirectoryMaterializationReceipt {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SyncRootDirectoryMaterializationReceipt")
            .field("remote_id", &"[redacted]")
            .field("relative_path", &"[redacted]")
            .field("materialized_at_unix_ms", &self.materialized_at_unix_ms)
            .finish()
    }
}

pub struct Storage {
    connection: Connection,
}

impl Storage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let connection = Connection::open(path)?;
        let mut storage = Self { connection };
        storage.configure()?;
        storage.migrate()?;
        Ok(storage)
    }

    pub fn open_in_memory() -> Result<Self, StorageError> {
        let connection = Connection::open_in_memory()?;
        let mut storage = Self { connection };
        storage.configure()?;
        storage.migrate()?;
        Ok(storage)
    }

    fn configure(&mut self) -> Result<(), StorageError> {
        self.connection.execute_batch(
            "
            PRAGMA foreign_keys = ON;
            PRAGMA busy_timeout = 5000;
            ",
        )?;
        Ok(())
    }

    fn migrate(&mut self) -> Result<(), StorageError> {
        let transaction = self.connection.transaction()?;
        let current_version: i64 =
            transaction.query_row("PRAGMA user_version", [], |row| row.get(0))?;

        if current_version > SCHEMA_VERSION {
            return Err(StorageError::UnsupportedSchemaVersion {
                found: current_version,
                supported: SCHEMA_VERSION,
            });
        }

        transaction.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS accounts (
                provider TEXT NOT NULL,
                subject TEXT NOT NULL,
                email TEXT,
                display_name TEXT,
                created_at_unix_ms INTEGER NOT NULL,
                PRIMARY KEY (provider, subject)
            );

            CREATE TABLE IF NOT EXISTS sync_roots (
                id TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                account_subject TEXT NOT NULL,
                local_path TEXT NOT NULL,
                remote_root_id TEXT,
                mode TEXT NOT NULL CHECK (
                    mode IN ('two_way', 'mirror_local_to_remote', 'receive_only')
                ),
                created_at_unix_ms INTEGER NOT NULL,
                UNIQUE (provider, account_subject, local_path),
                FOREIGN KEY (provider, account_subject)
                    REFERENCES accounts(provider, subject)
                    ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS provider_cursors (
                provider TEXT NOT NULL,
                account_subject TEXT NOT NULL,
                cursor TEXT NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL,
                PRIMARY KEY (provider, account_subject),
                FOREIGN KEY (provider, account_subject)
                    REFERENCES accounts(provider, subject)
                    ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS sync_root_local_inventory_staging (
                sync_root_id TEXT NOT NULL,
                relative_path TEXT NOT NULL,
                item_kind TEXT NOT NULL CHECK (
                    item_kind IN ('file', 'directory')
                ),
                size_bytes INTEGER CHECK (
                    size_bytes IS NULL OR size_bytes >= 0
                ),
                modified_unix_ns INTEGER NOT NULL,
                device_id TEXT NOT NULL,
                inode TEXT NOT NULL,
                observed_at_unix_ms INTEGER NOT NULL,
                PRIMARY KEY (sync_root_id, relative_path),
                FOREIGN KEY (sync_root_id)
                    REFERENCES sync_roots(id)
                    ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS sync_root_local_items (
                sync_root_id TEXT NOT NULL,
                relative_path TEXT NOT NULL,
                item_kind TEXT NOT NULL CHECK (
                    item_kind IN ('file', 'directory')
                ),
                size_bytes INTEGER CHECK (
                    size_bytes IS NULL OR size_bytes >= 0
                ),
                modified_unix_ns INTEGER NOT NULL,
                device_id TEXT NOT NULL,
                inode TEXT NOT NULL,
                observed_at_unix_ms INTEGER NOT NULL,
                PRIMARY KEY (sync_root_id, relative_path),
                FOREIGN KEY (sync_root_id)
                    REFERENCES sync_roots(id)
                    ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS sync_root_local_inventory_state (
                sync_root_id TEXT PRIMARY KEY,
                snapshot_complete INTEGER NOT NULL DEFAULT 0 CHECK (
                    snapshot_complete IN (0, 1)
                ),
                item_count INTEGER NOT NULL DEFAULT 0 CHECK (
                    item_count >= 0
                ),
                snapshot_completed_at_unix_ms INTEGER,
                generation INTEGER NOT NULL DEFAULT 0 CHECK (generation >= 0),
                observation_valid INTEGER NOT NULL DEFAULT 0 CHECK (
                    observation_valid IN (0, 1)
                ),
                FOREIGN KEY (sync_root_id)
                    REFERENCES sync_roots(id)
                    ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS sync_root_local_change_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sync_root_id TEXT NOT NULL,
                baseline_generation INTEGER NOT NULL CHECK (baseline_generation > 0),
                baseline_item_count INTEGER NOT NULL CHECK (baseline_item_count >= 0),
                baseline_snapshot_completed_at_unix_ms INTEGER,
                event_kind TEXT NOT NULL CHECK (
                    event_kind IN ('created', 'deleted', 'modified', 'type_changed')
                ),
                relative_path TEXT NOT NULL,
                baseline_kind TEXT CHECK (
                    baseline_kind IS NULL OR baseline_kind IN ('file', 'directory')
                ),
                current_kind TEXT CHECK (
                    current_kind IS NULL OR current_kind IN ('file', 'directory')
                ),
                observed_at_unix_ms INTEGER NOT NULL,
                status TEXT NOT NULL CHECK (
                    status IN ('pending', 'applied', 'superseded', 'failed')
                ),
                UNIQUE (sync_root_id, baseline_generation, relative_path),
                FOREIGN KEY (sync_root_id)
                    REFERENCES sync_roots(id)
                    ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS sync_root_local_change_events_pending_idx
            ON sync_root_local_change_events(sync_root_id, baseline_generation, status, id);

            CREATE TABLE IF NOT EXISTS sync_root_remote_write_authority (
                sync_root_id TEXT NOT NULL,
                remote_id TEXT NOT NULL,
                remote_version TEXT NOT NULL,
                checksum_algorithm TEXT,
                content_checksum TEXT,
                can_edit INTEGER NOT NULL CHECK (can_edit IN (0, 1)),
                can_trash INTEGER NOT NULL CHECK (can_trash IN (0, 1)),
                can_add_children INTEGER NOT NULL CHECK (can_add_children IN (0, 1)),
                observed_at_unix_ms INTEGER NOT NULL,
                PRIMARY KEY (sync_root_id, remote_id),
                CHECK (
                    (checksum_algorithm IS NULL AND content_checksum IS NULL)
                    OR
                    (checksum_algorithm IS NOT NULL AND content_checksum IS NOT NULL)
                ),
                FOREIGN KEY (sync_root_id) REFERENCES sync_roots(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS sync_root_remote_write_authority_state (
                sync_root_id TEXT PRIMARY KEY,
                change_cursor TEXT NOT NULL,
                item_count INTEGER NOT NULL CHECK (item_count >= 0),
                observed_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY (sync_root_id) REFERENCES sync_roots(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS sync_root_remote_write_intents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sync_root_id TEXT NOT NULL,
                source_local_event_id INTEGER NOT NULL UNIQUE,
                baseline_generation INTEGER NOT NULL CHECK (baseline_generation > 0),
                operation_kind TEXT NOT NULL CHECK (
                    operation_kind IN ('create_file','create_folder','update_file','trash_item')
                ),
                relative_path TEXT NOT NULL,
                local_kind TEXT NOT NULL CHECK (local_kind IN ('file','directory')),
                local_size_bytes INTEGER,
                local_modified_unix_ns INTEGER,
                local_device_id TEXT,
                local_inode TEXT,
                target_remote_id TEXT,
                predetermined_remote_id TEXT,
                expected_parent_remote_id TEXT,
                expected_remote_kind TEXT CHECK (
                    expected_remote_kind IS NULL OR expected_remote_kind IN ('file','folder')
                ),
                expected_remote_version TEXT,
                expected_remote_size_bytes INTEGER,
                expected_checksum_algorithm TEXT,
                expected_content_checksum TEXT,
                planned_at_unix_ms INTEGER NOT NULL,
                status TEXT NOT NULL CHECK (
                    status IN (
                        'planned','submitted','awaiting_confirmation',
                        'confirmed','conflict','failed','superseded'
                    )
                ),
                FOREIGN KEY (sync_root_id) REFERENCES sync_roots(id) ON DELETE CASCADE,
                FOREIGN KEY (source_local_event_id)
                    REFERENCES sync_root_local_change_events(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS sync_root_remote_write_intents_status_idx
            ON sync_root_remote_write_intents(sync_root_id, status, id);

            CREATE TABLE IF NOT EXISTS local_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sync_root_id TEXT NOT NULL,
                event_kind TEXT NOT NULL,
                relative_path TEXT NOT NULL,
                observed_at_unix_ms INTEGER NOT NULL,
                status TEXT NOT NULL CHECK (
                    status IN ('pending', 'applied', 'superseded', 'failed')
                ),
                FOREIGN KEY (sync_root_id)
                    REFERENCES sync_roots(id)
                    ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS remote_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                provider TEXT NOT NULL,
                account_subject TEXT NOT NULL,
                event_kind TEXT NOT NULL CHECK (
                    event_kind IN ('upsert', 'delete')
                ),
                remote_id TEXT NOT NULL,
                parent_remote_id TEXT,
                name TEXT,
                item_kind TEXT CHECK (
                    item_kind IS NULL OR item_kind IN ('file', 'folder')
                ),
                size_bytes INTEGER,
                trashed INTEGER NOT NULL DEFAULT 0 CHECK (
                    trashed IN (0, 1)
                ),
                observed_at_unix_ms INTEGER NOT NULL,
                status TEXT NOT NULL CHECK (
                    status IN ('pending', 'applied', 'superseded', 'failed')
                ),
                FOREIGN KEY (provider, account_subject)
                    REFERENCES accounts(provider, subject)
                    ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS remote_events_pending_idx
            ON remote_events(provider, account_subject, status, id);


            CREATE TABLE IF NOT EXISTS remote_items (
                provider TEXT NOT NULL,
                account_subject TEXT NOT NULL,
                remote_id TEXT NOT NULL,
                parent_remote_id TEXT,
                name TEXT NOT NULL,
                item_kind TEXT NOT NULL CHECK (item_kind IN ('file', 'folder')),
                size_bytes INTEGER,
                trashed INTEGER NOT NULL DEFAULT 0 CHECK (trashed IN (0, 1)),
                observed_at_unix_ms INTEGER NOT NULL,
                PRIMARY KEY (provider, account_subject, remote_id),
                FOREIGN KEY (provider, account_subject)
                    REFERENCES accounts(provider, subject) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS remote_inventory_staging (
                provider TEXT NOT NULL,
                account_subject TEXT NOT NULL,
                remote_id TEXT NOT NULL,
                parent_remote_id TEXT,
                name TEXT NOT NULL,
                item_kind TEXT NOT NULL CHECK (item_kind IN ('file', 'folder')),
                size_bytes INTEGER,
                trashed INTEGER NOT NULL DEFAULT 0 CHECK (trashed IN (0, 1)),
                observed_at_unix_ms INTEGER NOT NULL,
                PRIMARY KEY (provider, account_subject, remote_id),
                FOREIGN KEY (provider, account_subject)
                    REFERENCES accounts(provider, subject) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS remote_inventory_state (
                provider TEXT NOT NULL,
                account_subject TEXT NOT NULL,
                snapshot_complete INTEGER NOT NULL DEFAULT 0 CHECK (
                    snapshot_complete IN (0, 1)
                ),
                catchup_complete INTEGER NOT NULL DEFAULT 0 CHECK (
                    catchup_complete IN (0, 1)
                ),
                item_count INTEGER NOT NULL DEFAULT 0 CHECK (
                    item_count >= 0
                ),
                snapshot_completed_at_unix_ms INTEGER,
                catchup_from_cursor TEXT,
                PRIMARY KEY (provider, account_subject),
                FOREIGN KEY (provider, account_subject)
                    REFERENCES accounts(provider, subject) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS sync_root_remote_items (
                sync_root_id TEXT NOT NULL,
                remote_id TEXT NOT NULL,
                parent_remote_id TEXT,
                name TEXT NOT NULL,
                item_kind TEXT NOT NULL CHECK (item_kind IN ('file', 'folder')),
                size_bytes INTEGER,
                trashed INTEGER NOT NULL DEFAULT 0 CHECK (trashed IN (0, 1)),
                observed_at_unix_ms INTEGER NOT NULL,
                PRIMARY KEY (sync_root_id, remote_id),
                FOREIGN KEY (sync_root_id)
                    REFERENCES sync_roots(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS sync_root_remote_inventory_staging (
                sync_root_id TEXT NOT NULL,
                remote_id TEXT NOT NULL,
                parent_remote_id TEXT,
                name TEXT NOT NULL,
                item_kind TEXT NOT NULL CHECK (item_kind IN ('file', 'folder')),
                size_bytes INTEGER,
                trashed INTEGER NOT NULL DEFAULT 0 CHECK (trashed IN (0, 1)),
                observed_at_unix_ms INTEGER NOT NULL,
                PRIMARY KEY (sync_root_id, remote_id),
                FOREIGN KEY (sync_root_id)
                    REFERENCES sync_roots(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS sync_root_remote_inventory_state (
                sync_root_id TEXT PRIMARY KEY,
                snapshot_complete INTEGER NOT NULL DEFAULT 0 CHECK (
                    snapshot_complete IN (0, 1)
                ),
                catchup_complete INTEGER NOT NULL DEFAULT 0 CHECK (
                    catchup_complete IN (0, 1)
                ),
                item_count INTEGER NOT NULL DEFAULT 0 CHECK (item_count >= 0),
                snapshot_completed_at_unix_ms INTEGER,
                catchup_from_cursor TEXT,
                change_cursor TEXT,
                FOREIGN KEY (sync_root_id)
                    REFERENCES sync_roots(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS sync_root_change_window_state (
                sync_root_id TEXT PRIMARY KEY,
                base_cursor TEXT NOT NULL,
                continuation TEXT,
                checkpoint TEXT,
                page_count INTEGER NOT NULL CHECK (page_count >= 1),
                change_count INTEGER NOT NULL CHECK (change_count >= 0),
                CHECK (
                    (continuation IS NOT NULL AND checkpoint IS NULL)
                    OR
                    (continuation IS NULL AND checkpoint IS NOT NULL)
                ),
                FOREIGN KEY (sync_root_id)
                    REFERENCES sync_roots(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS sync_root_change_window_events (
                sync_root_id TEXT NOT NULL,
                sequence INTEGER NOT NULL CHECK (sequence >= 0),
                event_kind TEXT NOT NULL CHECK (event_kind IN ('upsert', 'delete')),
                remote_id TEXT NOT NULL,
                parent_remote_id TEXT,
                name TEXT,
                item_kind TEXT CHECK (
                    item_kind IS NULL OR item_kind IN ('file', 'folder')
                ),
                size_bytes INTEGER,
                trashed INTEGER NOT NULL DEFAULT 0 CHECK (trashed IN (0, 1)),
                PRIMARY KEY (sync_root_id, sequence),
                FOREIGN KEY (sync_root_id)
                    REFERENCES sync_roots(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS sync_root_change_window_tokens (
                sync_root_id TEXT NOT NULL,
                token TEXT NOT NULL,
                PRIMARY KEY (sync_root_id, token),
                FOREIGN KEY (sync_root_id)
                    REFERENCES sync_roots(id) ON DELETE CASCADE
            );


            CREATE TABLE IF NOT EXISTS sync_root_file_materialization_receipts (
                sync_root_id TEXT NOT NULL,
                remote_id TEXT NOT NULL,
                relative_path TEXT NOT NULL,
                size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
                sha256_hex TEXT NOT NULL CHECK (length(sha256_hex) = 64),
                materialized_at_unix_ms INTEGER NOT NULL,
                receipt_state TEXT NOT NULL DEFAULT 'current' CHECK (
                    receipt_state IN ('current', 'stale')
                ),
                PRIMARY KEY (sync_root_id, remote_id),
                FOREIGN KEY (sync_root_id)
                    REFERENCES sync_roots(id)
                    ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS sync_root_directory_materialization_receipts (
                sync_root_id TEXT NOT NULL,
                remote_id TEXT NOT NULL,
                relative_path TEXT NOT NULL,
                materialized_at_unix_ms INTEGER NOT NULL,
                receipt_state TEXT NOT NULL DEFAULT 'current' CHECK (
                    receipt_state IN ('current', 'stale')
                ),
                PRIMARY KEY (sync_root_id, remote_id),
                FOREIGN KEY (sync_root_id)
                    REFERENCES sync_roots(id)
                    ON DELETE CASCADE
            );
            ",
        )?;

        if current_version == 4 {
            transaction.execute(
                "ALTER TABLE remote_inventory_state ADD COLUMN catchup_from_cursor TEXT",
                [],
            )?;
        }

        if current_version == 6 {
            transaction.execute(
                "ALTER TABLE sync_root_remote_inventory_state ADD COLUMN change_cursor TEXT",
                [],
            )?;
        }

        if current_version == 12 {
            transaction.execute(
                "ALTER TABLE sync_root_local_inventory_state
                 ADD COLUMN generation INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
            transaction.execute(
                "UPDATE sync_root_local_inventory_state
                 SET generation = 1
                 WHERE snapshot_complete = 1",
                [],
            )?;
        }

        if current_version == 13 {
            transaction.execute(
                "ALTER TABLE sync_root_local_inventory_state
                 ADD COLUMN observation_valid INTEGER NOT NULL DEFAULT 0
                 CHECK (observation_valid IN (0, 1))",
                [],
            )?;
        }

        if current_version == 9 {
            transaction.execute_batch(
                "
                ALTER TABLE sync_root_file_materialization_receipts
                    RENAME TO sync_root_file_materialization_receipts_v9;

                CREATE TABLE sync_root_file_materialization_receipts (
                    sync_root_id TEXT NOT NULL,
                    remote_id TEXT NOT NULL,
                    relative_path TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
                    sha256_hex TEXT NOT NULL CHECK (length(sha256_hex) = 64),
                    materialized_at_unix_ms INTEGER NOT NULL,
                    receipt_state TEXT NOT NULL DEFAULT 'current' CHECK (
                        receipt_state IN ('current', 'stale')
                    ),
                    PRIMARY KEY (sync_root_id, remote_id),
                    FOREIGN KEY (sync_root_id)
                        REFERENCES sync_roots(id)
                        ON DELETE CASCADE
                );

                INSERT INTO sync_root_file_materialization_receipts (
                    sync_root_id,
                    remote_id,
                    relative_path,
                    size_bytes,
                    sha256_hex,
                    materialized_at_unix_ms,
                    receipt_state
                )
                SELECT
                    sync_root_id,
                    remote_id,
                    relative_path,
                    size_bytes,
                    sha256_hex,
                    materialized_at_unix_ms,
                    'current'
                FROM sync_root_file_materialization_receipts_v9;

                DROP TABLE sync_root_file_materialization_receipts_v9;
                ",
            )?;
        }

        transaction.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS
                sync_root_file_materialization_current_path_idx
             ON sync_root_file_materialization_receipts (
                sync_root_id,
                relative_path
             )
             WHERE receipt_state = 'current'",
            [],
        )?;

        transaction.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS
                sync_root_directory_materialization_current_path_idx
             ON sync_root_directory_materialization_receipts (
                sync_root_id,
                relative_path
             )
             WHERE receipt_state = 'current'",
            [],
        )?;

        transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn schema_version(&self) -> Result<i64, StorageError> {
        Ok(self
            .connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))?)
    }

    pub fn upsert_account(
        &self,
        account: &ProviderAccount,
        created_at_unix_ms: i64,
    ) -> Result<(), StorageError> {
        self.connection.execute(
            "
            INSERT INTO accounts (
                provider, subject, email, display_name, created_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(provider, subject) DO UPDATE SET
                email = excluded.email,
                display_name = excluded.display_name
            ",
            params![
                account.provider.as_str(),
                account.subject,
                account.email,
                account.display_name,
                created_at_unix_ms
            ],
        )?;

        Ok(())
    }

    pub fn list_accounts(
        &self,
        provider: &ProviderId,
    ) -> Result<Vec<ProviderAccount>, StorageError> {
        let mut statement = self.connection.prepare(
            "
            SELECT subject, email, display_name
            FROM accounts
            WHERE provider = ?1
            ORDER BY created_at_unix_ms ASC, subject ASC
            ",
        )?;

        let rows = statement.query_map(params![provider.as_str()], |row| {
            let subject: String = row.get(0)?;
            let email: Option<String> = row.get(1)?;
            let display_name: Option<String> = row.get(2)?;
            Ok((subject, email, display_name))
        })?;

        let mut accounts = Vec::new();
        for row in rows {
            let (subject, email, display_name) = row?;
            accounts.push(ProviderAccount::new(
                provider.clone(),
                subject,
                email,
                display_name,
            )?);
        }

        Ok(accounts)
    }

    pub fn insert_sync_root(&self, root: &SyncRoot) -> Result<(), StorageError> {
        self.connection.execute(
            "INSERT INTO sync_roots (
                id,
                provider,
                account_subject,
                local_path,
                remote_root_id,
                mode,
                created_at_unix_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                root.id,
                root.provider.as_str(),
                root.account_subject,
                root.local_path,
                root.remote_root_id,
                root.mode.as_str(),
                root.created_at_unix_ms
            ],
        )?;

        Ok(())
    }

    pub fn list_sync_roots(
        &self,
        provider: &ProviderId,
        account_subject: &str,
    ) -> Result<Vec<SyncRoot>, StorageError> {
        let mut statement = self.connection.prepare(
            "SELECT
                id,
                local_path,
                remote_root_id,
                mode,
                created_at_unix_ms
             FROM sync_roots
             WHERE provider = ?1 AND account_subject = ?2
             ORDER BY created_at_unix_ms ASC, id ASC",
        )?;

        let rows = statement.query_map(params![provider.as_str(), account_subject], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })?;

        let mut roots = Vec::new();
        for row in rows {
            let (id, local_path, remote_root_id, mode, created_at_unix_ms) = row?;
            roots.push(SyncRoot::new(
                id,
                provider.clone(),
                account_subject,
                local_path,
                remote_root_id,
                SyncMode::parse(&mode)?,
                created_at_unix_ms,
            )?);
        }

        Ok(roots)
    }

    pub fn sync_root_count(
        &self,
        provider: &ProviderId,
        account_subject: &str,
    ) -> Result<u64, StorageError> {
        let count: i64 = self.connection.query_row(
            "SELECT COUNT(*)
             FROM sync_roots
             WHERE provider = ?1 AND account_subject = ?2",
            params![provider.as_str(), account_subject],
            |row| row.get(0),
        )?;

        u64::try_from(count).map_err(|_| StorageError::NumericOverflow)
    }

    pub fn begin_sync_root_local_inventory_staging(
        &self,
        sync_root_id: &str,
    ) -> Result<(), StorageError> {
        self.connection.execute(
            "DELETE FROM sync_root_local_inventory_staging WHERE sync_root_id = ?1",
            params![sync_root_id],
        )?;
        Ok(())
    }

    pub fn stage_sync_root_local_inventory_items(
        &mut self,
        sync_root_id: &str,
        items: &[LocalItemSnapshot],
        observed_at_unix_ms: i64,
    ) -> Result<usize, StorageError> {
        let transaction = self.connection.transaction()?;
        for item in items {
            insert_sync_root_local_inventory_item(
                &transaction,
                sync_root_id,
                item,
                observed_at_unix_ms,
            )?;
        }
        transaction.commit()?;
        Ok(items.len())
    }

    pub fn staged_sync_root_local_inventory_count(
        &self,
        sync_root_id: &str,
    ) -> Result<u64, StorageError> {
        let count: i64 = self.connection.query_row(
            "SELECT COUNT(*)
             FROM sync_root_local_inventory_staging
             WHERE sync_root_id = ?1",
            params![sync_root_id],
            |row| row.get(0),
        )?;

        u64::try_from(count).map_err(|_| StorageError::NumericOverflow)
    }

    pub fn commit_sync_root_local_inventory_snapshot(
        &mut self,
        sync_root_id: &str,
        completed_at_unix_ms: i64,
    ) -> Result<usize, StorageError> {
        let transaction = self.connection.transaction()?;

        transaction.execute(
            "DELETE FROM sync_root_local_items WHERE sync_root_id = ?1",
            params![sync_root_id],
        )?;

        let inserted = transaction.execute(
            "INSERT INTO sync_root_local_items (
                sync_root_id,
                relative_path,
                item_kind,
                size_bytes,
                modified_unix_ns,
                device_id,
                inode,
                observed_at_unix_ms
             )
             SELECT
                sync_root_id,
                relative_path,
                item_kind,
                size_bytes,
                modified_unix_ns,
                device_id,
                inode,
                observed_at_unix_ms
             FROM sync_root_local_inventory_staging
             WHERE sync_root_id = ?1",
            params![sync_root_id],
        )?;

        let item_count = i64::try_from(inserted).map_err(|_| StorageError::NumericOverflow)?;

        transaction.execute(
            "UPDATE sync_root_local_change_events
             SET status = 'superseded'
             WHERE sync_root_id = ?1
               AND status = 'pending'",
            params![sync_root_id],
        )?;

        transaction.execute(
            "INSERT INTO sync_root_local_inventory_state (
                sync_root_id,
                snapshot_complete,
                item_count,
                snapshot_completed_at_unix_ms,
                generation,
                observation_valid
             ) VALUES (?1, 1, ?2, ?3, 1, 1)
             ON CONFLICT(sync_root_id) DO UPDATE SET
                snapshot_complete = 1,
                item_count = excluded.item_count,
                snapshot_completed_at_unix_ms = excluded.snapshot_completed_at_unix_ms,
                generation = sync_root_local_inventory_state.generation + 1,
                observation_valid = 1",
            params![sync_root_id, item_count, completed_at_unix_ms],
        )?;

        transaction.execute(
            "DELETE FROM sync_root_local_inventory_staging WHERE sync_root_id = ?1",
            params![sync_root_id],
        )?;

        transaction.commit()?;
        Ok(inserted)
    }

    pub fn sync_root_local_inventory_state(
        &self,
        sync_root_id: &str,
    ) -> Result<LocalInventoryState, StorageError> {
        let row: Option<(i64, i64, Option<i64>, i64, i64)> = self
            .connection
            .query_row(
                "SELECT
                    snapshot_complete,
                    item_count,
                    snapshot_completed_at_unix_ms,
                    generation,
                    observation_valid
                 FROM sync_root_local_inventory_state
                 WHERE sync_root_id = ?1",
                params![sync_root_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()?;

        match row {
            Some((snapshot_complete, item_count, completed_at, generation, observation_valid)) => {
                Ok(LocalInventoryState {
                    snapshot_complete: snapshot_complete != 0,
                    item_count: u64::try_from(item_count)
                        .map_err(|_| StorageError::NumericOverflow)?,
                    snapshot_completed_at_unix_ms: completed_at,
                    generation: u64::try_from(generation)
                        .map_err(|_| StorageError::NumericOverflow)?,
                    observation_valid: observation_valid != 0,
                })
            }
            None => Ok(LocalInventoryState {
                snapshot_complete: false,
                item_count: 0,
                snapshot_completed_at_unix_ms: None,
                generation: 0,
                observation_valid: false,
            }),
        }
    }

    pub fn invalidate_sync_root_local_observation_baseline(
        &self,
        sync_root_id: &str,
    ) -> Result<bool, StorageError> {
        let updated = self.connection.execute(
            "UPDATE sync_root_local_inventory_state
             SET observation_valid = 0
             WHERE sync_root_id = ?1
               AND snapshot_complete = 1
               AND observation_valid = 1",
            params![sync_root_id],
        )?;
        Ok(updated != 0)
    }

    pub fn reconcile_sync_root_local_change_journal(
        &mut self,
        sync_root_id: &str,
        expected_generation: u64,
        expected_item_count: u64,
        expected_snapshot_completed_at_unix_ms: Option<i64>,
        events: &[LocalChangeEventInput],
        observed_at_unix_ms: i64,
    ) -> Result<LocalChangeJournalCommit, StorageError> {
        let expected_generation =
            i64::try_from(expected_generation).map_err(|_| StorageError::NumericOverflow)?;
        let expected_item_count =
            i64::try_from(expected_item_count).map_err(|_| StorageError::NumericOverflow)?;

        let transaction = self.connection.transaction()?;

        let state: Option<(i64, i64, Option<i64>, i64, i64)> = transaction
            .query_row(
                "SELECT
                    snapshot_complete,
                    item_count,
                    snapshot_completed_at_unix_ms,
                    generation,
                    observation_valid
                 FROM sync_root_local_inventory_state
                 WHERE sync_root_id = ?1",
                params![sync_root_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()?;

        let Some((snapshot_complete, item_count, completed_at, generation, observation_valid)) =
            state
        else {
            return Err(StorageError::LocalChangeBaselineMismatch);
        };

        if snapshot_complete == 0
            || observation_valid == 0
            || generation != expected_generation
            || item_count != expected_item_count
            || completed_at != expected_snapshot_completed_at_unix_ms
        {
            return Err(StorageError::LocalChangeBaselineMismatch);
        }

        let superseded = transaction.execute(
            "UPDATE sync_root_local_change_events
             SET status = 'superseded'
             WHERE sync_root_id = ?1
               AND baseline_generation = ?2
               AND status = 'pending'",
            params![sync_root_id, expected_generation],
        )?;

        for event in events {
            transaction.execute(
                "INSERT INTO sync_root_local_change_events (
                    sync_root_id,
                    baseline_generation,
                    baseline_item_count,
                    baseline_snapshot_completed_at_unix_ms,
                    event_kind,
                    relative_path,
                    baseline_kind,
                    current_kind,
                    observed_at_unix_ms,
                    status
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'pending')
                 ON CONFLICT(sync_root_id, baseline_generation, relative_path)
                 DO UPDATE SET
                    baseline_item_count = excluded.baseline_item_count,
                    baseline_snapshot_completed_at_unix_ms =
                        excluded.baseline_snapshot_completed_at_unix_ms,
                    event_kind = excluded.event_kind,
                    baseline_kind = excluded.baseline_kind,
                    current_kind = excluded.current_kind,
                    observed_at_unix_ms = excluded.observed_at_unix_ms,
                    status = 'pending'",
                params![
                    sync_root_id,
                    expected_generation,
                    expected_item_count,
                    expected_snapshot_completed_at_unix_ms,
                    event.kind.as_str(),
                    event.relative_path(),
                    event.baseline_kind.map(LocalItemKind::as_str),
                    event.current_kind.map(LocalItemKind::as_str),
                    observed_at_unix_ms
                ],
            )?;
        }

        let pending: i64 = transaction.query_row(
            "SELECT COUNT(*)
             FROM sync_root_local_change_events
             WHERE sync_root_id = ?1
               AND baseline_generation = ?2
               AND status = 'pending'",
            params![sync_root_id, expected_generation],
            |row| row.get(0),
        )?;

        let expected_pending =
            i64::try_from(events.len()).map_err(|_| StorageError::NumericOverflow)?;
        if pending != expected_pending {
            return Err(StorageError::LocalChangeJournalCountMismatch);
        }

        transaction.commit()?;

        Ok(LocalChangeJournalCommit {
            baseline_generation: u64::try_from(expected_generation)
                .map_err(|_| StorageError::NumericOverflow)?,
            pending_events: u64::try_from(pending).map_err(|_| StorageError::NumericOverflow)?,
            superseded_events: u64::try_from(superseded)
                .map_err(|_| StorageError::NumericOverflow)?,
            current_diff_events: u64::try_from(events.len())
                .map_err(|_| StorageError::NumericOverflow)?,
        })
    }

    pub fn upsert_sync_root_remote_write_authority(
        &self,
        sync_root_id: &str,
        authority: &RemoteWriteAuthoritySnapshot,
    ) -> Result<(), StorageError> {
        self.connection.execute(
            "INSERT INTO sync_root_remote_write_authority (
                sync_root_id, remote_id, remote_version,
                checksum_algorithm, content_checksum,
                can_edit, can_trash, can_add_children, observed_at_unix_ms
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)
             ON CONFLICT(sync_root_id, remote_id) DO UPDATE SET
                remote_version=excluded.remote_version,
                checksum_algorithm=excluded.checksum_algorithm,
                content_checksum=excluded.content_checksum,
                can_edit=excluded.can_edit,
                can_trash=excluded.can_trash,
                can_add_children=excluded.can_add_children,
                observed_at_unix_ms=excluded.observed_at_unix_ms",
            params![
                sync_root_id,
                authority.remote_id(),
                authority.remote_version.to_string(),
                authority.checksum_algorithm(),
                authority.content_checksum(),
                i64::from(authority.can_edit),
                i64::from(authority.can_trash),
                i64::from(authority.can_add_children),
                authority.observed_at_unix_ms,
            ],
        )?;
        Ok(())
    }

    pub fn sync_root_remote_write_authority(
        &self,
        sync_root_id: &str,
        remote_id: &str,
    ) -> Result<Option<RemoteWriteAuthoritySnapshot>, StorageError> {
        query_remote_write_authority(&self.connection, sync_root_id, remote_id)
    }

    pub fn list_sync_root_remote_write_authorities(
        &self,
        sync_root_id: &str,
    ) -> Result<Vec<RemoteWriteAuthoritySnapshot>, StorageError> {
        let mut statement = self.connection.prepare(
            "SELECT remote_id, remote_version, checksum_algorithm, content_checksum,
                    can_edit, can_trash, can_add_children, observed_at_unix_ms
             FROM sync_root_remote_write_authority
             WHERE sync_root_id=?1
             ORDER BY remote_id",
        )?;

        let rows = statement.query_map(params![sync_root_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, i64>(7)?,
            ))
        })?;

        let mut authorities = Vec::new();
        for row in rows {
            let (
                remote_id,
                remote_version,
                checksum_algorithm,
                content_checksum,
                can_edit,
                can_trash,
                can_add_children,
                observed_at_unix_ms,
            ) = row?;

            let remote_version = remote_version
                .parse::<u64>()
                .map_err(|_| StorageError::InvalidStoredRemoteWriteAuthority)?;

            authorities.push(RemoteWriteAuthoritySnapshot::new(
                remote_id,
                remote_version,
                checksum_algorithm,
                content_checksum,
                can_edit != 0,
                can_trash != 0,
                can_add_children != 0,
                observed_at_unix_ms,
            )?);
        }

        Ok(authorities)
    }

    pub fn commit_sync_root_remote_write_authority_snapshot(
        &mut self,
        sync_root_id: &str,
        expected_change_cursor: &ChangeCursor,
        authorities: &[RemoteWriteAuthoritySnapshot],
        observed_at_unix_ms: i64,
    ) -> Result<usize, StorageError> {
        let transaction = self.connection.transaction()?;

        let catalog_state: Option<(i64, i64, i64, Option<String>)> = transaction
            .query_row(
                "SELECT snapshot_complete, catchup_complete, item_count, change_cursor
                 FROM sync_root_remote_inventory_state
                 WHERE sync_root_id=?1",
                params![sync_root_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;

        let Some((snapshot_complete, catchup_complete, catalog_items, change_cursor)) =
            catalog_state
        else {
            return Err(StorageError::RemoteWriteAuthorityCatalogNotReady);
        };

        if snapshot_complete == 0 || catchup_complete == 0 {
            return Err(StorageError::RemoteWriteAuthorityCatalogNotReady);
        }

        if change_cursor.as_deref() != Some(expected_change_cursor.as_str()) {
            return Err(StorageError::RemoteWriteAuthorityCursorMismatch);
        }

        let open_window: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM sync_root_change_window_state WHERE sync_root_id=?1",
            params![sync_root_id],
            |row| row.get(0),
        )?;
        if open_window != 0 {
            return Err(StorageError::RemoteWriteAuthorityCatalogNotReady);
        }

        let expected_authorities = catalog_items
            .checked_add(1)
            .ok_or(StorageError::NumericOverflow)?;
        let authority_count =
            i64::try_from(authorities.len()).map_err(|_| StorageError::NumericOverflow)?;
        if authority_count != expected_authorities {
            return Err(StorageError::RemoteWriteAuthorityCountMismatch);
        }

        if authorities
            .iter()
            .any(|authority| authority.observed_at_unix_ms != observed_at_unix_ms)
        {
            return Err(StorageError::InvalidRemoteWriteAuthority);
        }

        transaction.execute(
            "DELETE FROM sync_root_remote_write_authority WHERE sync_root_id=?1",
            params![sync_root_id],
        )?;

        for authority in authorities {
            transaction.execute(
                "INSERT INTO sync_root_remote_write_authority (
                    sync_root_id, remote_id, remote_version,
                    checksum_algorithm, content_checksum,
                    can_edit, can_trash, can_add_children, observed_at_unix_ms
                 ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
                params![
                    sync_root_id,
                    authority.remote_id(),
                    authority.remote_version.to_string(),
                    authority.checksum_algorithm(),
                    authority.content_checksum(),
                    i64::from(authority.can_edit),
                    i64::from(authority.can_trash),
                    i64::from(authority.can_add_children),
                    authority.observed_at_unix_ms,
                ],
            )?;
        }

        transaction.execute(
            "INSERT INTO sync_root_remote_write_authority_state (
                sync_root_id, change_cursor, item_count, observed_at_unix_ms
             ) VALUES (?1,?2,?3,?4)
             ON CONFLICT(sync_root_id) DO UPDATE SET
                change_cursor=excluded.change_cursor,
                item_count=excluded.item_count,
                observed_at_unix_ms=excluded.observed_at_unix_ms",
            params![
                sync_root_id,
                expected_change_cursor.as_str(),
                authority_count,
                observed_at_unix_ms,
            ],
        )?;

        transaction.commit()?;
        Ok(authorities.len())
    }

    pub fn sync_root_remote_write_authority_state(
        &self,
        sync_root_id: &str,
    ) -> Result<Option<RemoteWriteAuthorityState>, StorageError> {
        let row: Option<(String, i64, i64)> = self
            .connection
            .query_row(
                "SELECT change_cursor, item_count, observed_at_unix_ms
                 FROM sync_root_remote_write_authority_state
                 WHERE sync_root_id=?1",
                params![sync_root_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;

        row.map(|(change_cursor, item_count, observed_at_unix_ms)| {
            Ok(RemoteWriteAuthorityState {
                change_cursor: ChangeCursor::new(change_cursor)?,
                item_count: u64::try_from(item_count).map_err(|_| StorageError::NumericOverflow)?,
                observed_at_unix_ms,
            })
        })
        .transpose()
    }

    pub fn replace_sync_root_remote_write_authority_snapshot(
        &mut self,
        sync_root_id: &str,
        authorities: &[RemoteWriteAuthoritySnapshot],
    ) -> Result<usize, StorageError> {
        let transaction = self.connection.transaction()?;

        transaction.execute(
            "DELETE FROM sync_root_remote_write_authority WHERE sync_root_id = ?1",
            params![sync_root_id],
        )?;

        for authority in authorities {
            transaction.execute(
                "INSERT INTO sync_root_remote_write_authority (
                    sync_root_id, remote_id, remote_version,
                    checksum_algorithm, content_checksum,
                    can_edit, can_trash, can_add_children, observed_at_unix_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    sync_root_id,
                    authority.remote_id(),
                    authority.remote_version.to_string(),
                    authority.checksum_algorithm(),
                    authority.content_checksum(),
                    i64::from(authority.can_edit),
                    i64::from(authority.can_trash),
                    i64::from(authority.can_add_children),
                    authority.observed_at_unix_ms,
                ],
            )?;
        }

        transaction.commit()?;
        Ok(authorities.len())
    }

    pub fn sync_root_remote_write_authority_count(
        &self,
        sync_root_id: &str,
    ) -> Result<u64, StorageError> {
        let count: i64 = self.connection.query_row(
            "SELECT COUNT(*)
             FROM sync_root_remote_write_authority
             WHERE sync_root_id = ?1",
            params![sync_root_id],
            |row| row.get(0),
        )?;
        u64::try_from(count).map_err(|_| StorageError::NumericOverflow)
    }

    pub fn create_sync_root_remote_write_intent(
        &mut self,
        sync_root_id: &str,
        input: &RemoteWriteIntentInput,
    ) -> Result<i64, StorageError> {
        let transaction = self.connection.transaction()?;

        let mode: Option<String> = transaction
            .query_row(
                "SELECT mode FROM sync_roots WHERE id=?1",
                params![sync_root_id],
                |row| row.get(0),
            )
            .optional()?;
        let Some(mode) = mode else {
            return Err(StorageError::RemoteWriteIntentSourceMismatch);
        };
        if SyncMode::parse(&mode)? == SyncMode::ReceiveOnly {
            return Err(StorageError::RemoteWriteIntentRootNotWriteCapable);
        }

        let state: Option<(i64, i64)> = transaction
            .query_row(
                "SELECT generation, observation_valid
                 FROM sync_root_local_inventory_state
                 WHERE sync_root_id=?1 AND snapshot_complete=1",
                params![sync_root_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((generation, observation_valid)) = state else {
            return Err(StorageError::RemoteWriteIntentSourceMismatch);
        };
        if u64::try_from(generation).map_err(|_| StorageError::NumericOverflow)?
            != input.baseline_generation
            || observation_valid == 0
        {
            return Err(StorageError::RemoteWriteIntentSourceMismatch);
        }

        let source: Option<(
            String,
            i64,
            String,
            String,
            Option<String>,
            Option<String>,
            String,
        )> = transaction
            .query_row(
                "SELECT sync_root_id, baseline_generation, event_kind, relative_path,
                            baseline_kind, current_kind, status
                     FROM sync_root_local_change_events WHERE id=?1",
                params![input.source_local_event_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )
            .optional()?;

        let Some((
            source_root,
            source_generation,
            source_kind,
            source_path,
            baseline_kind,
            current_kind,
            source_status,
        )) = source
        else {
            return Err(StorageError::RemoteWriteIntentSourceMismatch);
        };

        if source_root != sync_root_id
            || u64::try_from(source_generation).map_err(|_| StorageError::NumericOverflow)?
                != input.baseline_generation
            || source_path != input.relative_path()
            || source_status != "pending"
        {
            return Err(StorageError::RemoteWriteIntentSourceMismatch);
        }

        validate_intent_against_local_event(
            input,
            &source_kind,
            baseline_kind.as_deref(),
            current_kind.as_deref(),
        )?;

        match input.operation {
            RemoteWriteIntentOperation::CreateFile | RemoteWriteIntentOperation::CreateFolder => {
                let parent = input
                    .expected_parent_remote_id()
                    .ok_or(StorageError::InvalidRemoteWriteIntent)?;
                let authority = query_remote_write_authority_in_transaction(
                    &transaction,
                    sync_root_id,
                    parent,
                )?
                .ok_or(StorageError::RemoteWriteIntentAuthorityMismatch)?;
                if !authority.can_add_children {
                    return Err(StorageError::RemoteWriteIntentAuthorityMismatch);
                }
            }
            RemoteWriteIntentOperation::UpdateFile => {
                let target = input
                    .target_remote_id()
                    .ok_or(StorageError::InvalidRemoteWriteIntent)?;
                let authority = query_remote_write_authority_in_transaction(
                    &transaction,
                    sync_root_id,
                    target,
                )?
                .ok_or(StorageError::RemoteWriteIntentAuthorityMismatch)?;
                if !authority.can_edit
                    || Some(authority.remote_version) != input.expected_remote_version
                {
                    return Err(StorageError::RemoteWriteIntentAuthorityMismatch);
                }
            }
            RemoteWriteIntentOperation::TrashItem => {
                let target = input
                    .target_remote_id()
                    .ok_or(StorageError::InvalidRemoteWriteIntent)?;
                let authority = query_remote_write_authority_in_transaction(
                    &transaction,
                    sync_root_id,
                    target,
                )?
                .ok_or(StorageError::RemoteWriteIntentAuthorityMismatch)?;
                if !authority.can_trash
                    || Some(authority.remote_version) != input.expected_remote_version
                {
                    return Err(StorageError::RemoteWriteIntentAuthorityMismatch);
                }
            }
        }

        transaction.execute(
            "INSERT INTO sync_root_remote_write_intents (
                sync_root_id, source_local_event_id, baseline_generation,
                operation_kind, relative_path, local_kind, local_size_bytes,
                local_modified_unix_ns, local_device_id, local_inode,
                target_remote_id, predetermined_remote_id, expected_parent_remote_id,
                expected_remote_kind, expected_remote_version, expected_remote_size_bytes,
                expected_checksum_algorithm, expected_content_checksum,
                planned_at_unix_ms, status
             ) VALUES (
                ?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,
                ?11,?12,?13,?14,?15,?16,?17,?18,?19,'planned'
             )",
            params![
                sync_root_id,
                input.source_local_event_id,
                i64::try_from(input.baseline_generation)
                    .map_err(|_| StorageError::NumericOverflow)?,
                input.operation.as_str(),
                input.relative_path(),
                input.local_kind.as_str(),
                input
                    .local_size_bytes
                    .map(i64::try_from)
                    .transpose()
                    .map_err(|_| StorageError::NumericOverflow)?,
                input.local_modified_unix_ns,
                input.local_device_id.map(|value| value.to_string()),
                input.local_inode.map(|value| value.to_string()),
                input.target_remote_id(),
                input.predetermined_remote_id(),
                input.expected_parent_remote_id(),
                input.expected_remote_kind.map(|kind| match kind {
                    RemoteItemKind::File => "file",
                    RemoteItemKind::Folder => "folder",
                }),
                input.expected_remote_version.map(|value| value.to_string()),
                input
                    .expected_remote_size_bytes
                    .map(i64::try_from)
                    .transpose()
                    .map_err(|_| StorageError::NumericOverflow)?,
                input.expected_checksum_algorithm(),
                input.expected_content_checksum(),
                input.planned_at_unix_ms,
            ],
        )?;

        let id = transaction.last_insert_rowid();
        transaction.commit()?;
        Ok(id)
    }

    pub fn sync_root_remote_write_intent(
        &self,
        intent_id: i64,
    ) -> Result<Option<RemoteWriteIntentRecord>, StorageError> {
        let row: Option<(i64, i64, String, String, String)> = self
            .connection
            .query_row(
                "SELECT source_local_event_id, baseline_generation,
                        operation_kind, relative_path, status
                 FROM sync_root_remote_write_intents WHERE id=?1",
                params![intent_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()?;

        let Some((source_local_event_id, baseline_generation, operation, relative_path, status)) =
            row
        else {
            return Ok(None);
        };

        Ok(Some(RemoteWriteIntentRecord {
            id: intent_id,
            source_local_event_id,
            baseline_generation: u64::try_from(baseline_generation)
                .map_err(|_| StorageError::NumericOverflow)?,
            operation: RemoteWriteIntentOperation::parse(&operation)?,
            relative_path,
            status: RemoteWriteIntentStatus::parse(&status)?,
        }))
    }

    pub fn planned_sync_root_remote_write_intent_count(
        &self,
        sync_root_id: &str,
    ) -> Result<u64, StorageError> {
        let count: i64 = self.connection.query_row(
            "SELECT COUNT(*) FROM sync_root_remote_write_intents
             WHERE sync_root_id=?1 AND status='planned'",
            params![sync_root_id],
            |row| row.get(0),
        )?;
        u64::try_from(count).map_err(|_| StorageError::NumericOverflow)
    }

    pub fn list_pending_sync_root_local_change_events(
        &self,
        sync_root_id: &str,
        baseline_generation: u64,
    ) -> Result<Vec<LocalChangeEventRecord>, StorageError> {
        let baseline_generation_i64 =
            i64::try_from(baseline_generation).map_err(|_| StorageError::NumericOverflow)?;
        let mut statement = self.connection.prepare(
            "SELECT id, event_kind, relative_path, baseline_kind, current_kind
             FROM sync_root_local_change_events
             WHERE sync_root_id=?1
               AND baseline_generation=?2
               AND status='pending'
             ORDER BY relative_path ASC",
        )?;

        let rows = statement.query_map(params![sync_root_id, baseline_generation_i64], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
            ))
        })?;

        let mut events = Vec::new();
        for row in rows {
            let (id, kind, relative_path, baseline_kind, current_kind) = row?;
            let kind = LocalChangeEventKind::parse(&kind)?;
            let baseline_kind = baseline_kind
                .map(|value| {
                    LocalItemKind::parse(&value)
                        .map_err(|_| StorageError::InvalidStoredLocalItemKind)
                })
                .transpose()?;
            let current_kind = current_kind
                .map(|value| {
                    LocalItemKind::parse(&value)
                        .map_err(|_| StorageError::InvalidStoredLocalItemKind)
                })
                .transpose()?;

            events.push(LocalChangeEventRecord::new(
                id,
                baseline_generation,
                relative_path,
                kind,
                baseline_kind,
                current_kind,
            )?);
        }

        Ok(events)
    }

    pub fn pending_sync_root_local_change_event_count(
        &self,
        sync_root_id: &str,
        baseline_generation: u64,
    ) -> Result<u64, StorageError> {
        let baseline_generation =
            i64::try_from(baseline_generation).map_err(|_| StorageError::NumericOverflow)?;
        let count: i64 = self.connection.query_row(
            "SELECT COUNT(*)
             FROM sync_root_local_change_events
             WHERE sync_root_id = ?1
               AND baseline_generation = ?2
               AND status = 'pending'",
            params![sync_root_id, baseline_generation],
            |row| row.get(0),
        )?;

        u64::try_from(count).map_err(|_| StorageError::NumericOverflow)
    }

    pub fn list_sync_root_local_items(
        &self,
        sync_root_id: &str,
    ) -> Result<Vec<LocalItemSnapshot>, StorageError> {
        let mut statement = self.connection.prepare(
            "SELECT
                relative_path,
                item_kind,
                size_bytes,
                modified_unix_ns,
                device_id,
                inode
             FROM sync_root_local_items
             WHERE sync_root_id = ?1
             ORDER BY relative_path ASC",
        )?;

        let rows = statement.query_map(params![sync_root_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<i64>>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
            ))
        })?;

        let mut items = Vec::new();
        for row in rows {
            let (relative_path, item_kind, size_bytes, modified_unix_ns, device_id, inode) = row?;
            let kind = LocalItemKind::parse(&item_kind)
                .map_err(|_| StorageError::InvalidStoredLocalItemKind)?;
            let size_bytes = size_bytes
                .map(u64::try_from)
                .transpose()
                .map_err(|_| StorageError::NumericOverflow)?;
            let device_id = device_id
                .parse::<u64>()
                .map_err(|_| StorageError::InvalidStoredLocalIdentity)?;
            let inode = inode
                .parse::<u64>()
                .map_err(|_| StorageError::InvalidStoredLocalIdentity)?;

            items.push(LocalItemSnapshot::new(
                relative_path,
                kind,
                size_bytes,
                modified_unix_ns,
                device_id,
                inode,
            )?);
        }

        Ok(items)
    }

    pub fn begin_sync_root_remote_inventory_staging(
        &self,
        sync_root_id: &str,
    ) -> Result<(), StorageError> {
        self.connection.execute(
            "DELETE FROM sync_root_remote_inventory_staging WHERE sync_root_id = ?1",
            params![sync_root_id],
        )?;
        Ok(())
    }

    pub fn stage_sync_root_remote_inventory_items(
        &mut self,
        sync_root_id: &str,
        items: &[RemoteItem],
        observed_at_unix_ms: i64,
    ) -> Result<usize, StorageError> {
        let transaction = self.connection.transaction()?;
        for item in items {
            insert_sync_root_inventory_item(&transaction, sync_root_id, item, observed_at_unix_ms)?;
        }
        transaction.commit()?;
        Ok(items.len())
    }

    pub fn staged_sync_root_remote_inventory_count(
        &self,
        sync_root_id: &str,
    ) -> Result<u64, StorageError> {
        let count: i64 = self.connection.query_row(
            "SELECT COUNT(*)
             FROM sync_root_remote_inventory_staging
             WHERE sync_root_id = ?1",
            params![sync_root_id],
            |row| row.get(0),
        )?;

        u64::try_from(count).map_err(|_| StorageError::NumericOverflow)
    }

    pub fn clear_sync_root_remote_inventory_staging(
        &self,
        sync_root_id: &str,
    ) -> Result<(), StorageError> {
        self.connection.execute(
            "DELETE FROM sync_root_remote_inventory_staging WHERE sync_root_id = ?1",
            params![sync_root_id],
        )?;
        Ok(())
    }

    pub fn commit_sync_root_remote_inventory_snapshot(
        &mut self,
        sync_root_id: &str,
        catchup_from_cursor: &ChangeCursor,
        completed_at_unix_ms: i64,
    ) -> Result<usize, StorageError> {
        let transaction = self.connection.transaction()?;

        transaction.execute(
            "DELETE FROM sync_root_remote_items WHERE sync_root_id = ?1",
            params![sync_root_id],
        )?;

        let inserted = transaction.execute(
            "INSERT INTO sync_root_remote_items (
                sync_root_id,
                remote_id,
                parent_remote_id,
                name,
                item_kind,
                size_bytes,
                trashed,
                observed_at_unix_ms
             )
             SELECT
                sync_root_id,
                remote_id,
                parent_remote_id,
                name,
                item_kind,
                size_bytes,
                trashed,
                observed_at_unix_ms
             FROM sync_root_remote_inventory_staging
             WHERE sync_root_id = ?1",
            params![sync_root_id],
        )?;

        let item_count = i64::try_from(inserted).map_err(|_| StorageError::NumericOverflow)?;

        transaction.execute(
            "INSERT INTO sync_root_remote_inventory_state (
                sync_root_id,
                snapshot_complete,
                catchup_complete,
                item_count,
                snapshot_completed_at_unix_ms,
                catchup_from_cursor,
                change_cursor
             ) VALUES (?1, 1, 0, ?2, ?3, ?4, NULL)
             ON CONFLICT(sync_root_id) DO UPDATE SET
                snapshot_complete = 1,
                catchup_complete = 0,
                item_count = excluded.item_count,
                snapshot_completed_at_unix_ms = excluded.snapshot_completed_at_unix_ms,
                catchup_from_cursor = excluded.catchup_from_cursor,
                change_cursor = NULL",
            params![
                sync_root_id,
                item_count,
                completed_at_unix_ms,
                catchup_from_cursor.as_str()
            ],
        )?;

        transaction.execute(
            "DELETE FROM sync_root_remote_inventory_staging WHERE sync_root_id = ?1",
            params![sync_root_id],
        )?;

        transaction.commit()?;
        Ok(inserted)
    }

    pub fn sync_root_remote_inventory_state(
        &self,
        sync_root_id: &str,
    ) -> Result<RemoteInventoryState, StorageError> {
        let row: Option<RemoteInventoryStateRow> = self
            .connection
            .query_row(
                "SELECT
                    snapshot_complete,
                    catchup_complete,
                    item_count,
                    snapshot_completed_at_unix_ms,
                    catchup_from_cursor
                 FROM sync_root_remote_inventory_state
                 WHERE sync_root_id = ?1",
                params![sync_root_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()?;

        match row {
            Some((
                snapshot_complete,
                catchup_complete,
                item_count,
                completed_at,
                catchup_from_cursor,
            )) => Ok(RemoteInventoryState {
                snapshot_complete: snapshot_complete != 0,
                catchup_complete: catchup_complete != 0,
                item_count: u64::try_from(item_count).map_err(|_| StorageError::NumericOverflow)?,
                snapshot_completed_at_unix_ms: completed_at,
                catchup_from_cursor: catchup_from_cursor.map(ChangeCursor::new).transpose()?,
            }),
            None => Ok(RemoteInventoryState {
                snapshot_complete: false,
                catchup_complete: false,
                item_count: 0,
                snapshot_completed_at_unix_ms: None,
                catchup_from_cursor: None,
            }),
        }
    }

    pub fn sync_root_remote_inventory_count(
        &self,
        sync_root_id: &str,
    ) -> Result<u64, StorageError> {
        let count: i64 = self.connection.query_row(
            "SELECT COUNT(*) FROM sync_root_remote_items WHERE sync_root_id = ?1",
            params![sync_root_id],
            |row| row.get(0),
        )?;

        u64::try_from(count).map_err(|_| StorageError::NumericOverflow)
    }

    pub fn list_sync_root_remote_items(
        &self,
        sync_root_id: &str,
    ) -> Result<Vec<RemoteItem>, StorageError> {
        let mut statement = self.connection.prepare(
            "SELECT
                remote_id,
                parent_remote_id,
                name,
                item_kind,
                size_bytes,
                trashed
             FROM sync_root_remote_items
             WHERE sync_root_id = ?1
             ORDER BY remote_id",
        )?;

        let rows = statement.query_map(params![sync_root_id], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            ))
        })?;

        let mut items = Vec::new();

        for row in rows {
            let (
                remote_id,
                parent_remote_id,
                name,
                item_kind,
                size_bytes,
                trashed,
            ): SyncRootRemoteCatalogRow = row?;

            let kind = match item_kind.as_str() {
                "file" => RemoteItemKind::File,
                "folder" => RemoteItemKind::Folder,
                _ => return Err(StorageError::InvalidStoredRemoteItemKind),
            };

            let size_bytes = size_bytes
                .map(u64::try_from)
                .transpose()
                .map_err(|_| StorageError::NumericOverflow)?;

            items.push(RemoteItem {
                remote_id,
                parent_remote_id,
                name,
                kind,
                size_bytes,
                modified_unix_ms: None,
                trashed: trashed != 0,
            });
        }

        Ok(items)
    }

    pub fn sync_root_remote_item(
        &self,
        sync_root_id: &str,
        remote_id: &str,
    ) -> Result<Option<RemoteItem>, StorageError> {
        let row: Option<SyncRootRemoteItemRow> = self
            .connection
            .query_row(
                "SELECT
                    parent_remote_id,
                    name,
                    item_kind,
                    size_bytes,
                    trashed
                 FROM sync_root_remote_items
                 WHERE sync_root_id = ?1 AND remote_id = ?2",
                params![sync_root_id, remote_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()?;

        let Some((parent_remote_id, name, item_kind, size_bytes, trashed)) = row else {
            return Ok(None);
        };

        let kind = match item_kind.as_str() {
            "file" => RemoteItemKind::File,
            "folder" => RemoteItemKind::Folder,
            _ => return Err(StorageError::InvalidStoredRemoteItemKind),
        };

        let size_bytes = size_bytes
            .map(u64::try_from)
            .transpose()
            .map_err(|_| StorageError::NumericOverflow)?;

        Ok(Some(RemoteItem {
            remote_id: remote_id.to_owned(),
            parent_remote_id,
            name,
            kind,
            size_bytes,
            modified_unix_ms: None,
            trashed: trashed != 0,
        }))
    }

    pub fn upsert_sync_root_remote_item(
        &mut self,
        sync_root_id: &str,
        item: &RemoteItem,
        observed_at_unix_ms: i64,
    ) -> Result<(), StorageError> {
        let transaction = self.connection.transaction()?;

        if item.trashed {
            delete_sync_root_subtree_in_transaction(&transaction, sync_root_id, &item.remote_id)?;
        } else {
            upsert_sync_root_catalog_item(&transaction, sync_root_id, item, observed_at_unix_ms)?;
        }

        refresh_sync_root_catalog_count(&transaction, sync_root_id)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn delete_sync_root_remote_subtree(
        &mut self,
        sync_root_id: &str,
        remote_id: &str,
    ) -> Result<u64, StorageError> {
        let transaction = self.connection.transaction()?;

        let deleted =
            delete_sync_root_subtree_in_transaction(&transaction, sync_root_id, remote_id)?;

        refresh_sync_root_catalog_count(&transaction, sync_root_id)?;
        transaction.commit()?;

        u64::try_from(deleted).map_err(|_| StorageError::NumericOverflow)
    }

    pub fn sync_root_change_window_state(
        &self,
        sync_root_id: &str,
    ) -> Result<Option<SyncRootChangeWindowState>, StorageError> {
        let row: Option<SyncRootChangeWindowStateRow> = self
            .connection
            .query_row(
                "SELECT
                    base_cursor,
                    continuation,
                    checkpoint,
                    page_count,
                    change_count
                 FROM sync_root_change_window_state
                 WHERE sync_root_id = ?1",
                params![sync_root_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()?;

        row.map(
            |(base_cursor, continuation, checkpoint, page_count, change_count)| {
                Ok(SyncRootChangeWindowState {
                    base_cursor: ChangeCursor::new(base_cursor)?,
                    continuation: continuation.map(ContinuationToken::new).transpose()?,
                    checkpoint: checkpoint.map(ChangeCursor::new).transpose()?,
                    page_count: u64::try_from(page_count)
                        .map_err(|_| StorageError::NumericOverflow)?,
                    change_count: u64::try_from(change_count)
                        .map_err(|_| StorageError::NumericOverflow)?,
                })
            },
        )
        .transpose()
    }

    pub fn stage_sync_root_change_window_page(
        &mut self,
        sync_root_id: &str,
        base_cursor: &ChangeCursor,
        expected_continuation: Option<&ContinuationToken>,
        page: &nubisync_core::ChangePage,
    ) -> Result<SyncRootChangeWindowState, StorageError> {
        if page.continuation.is_some() == page.checkpoint.is_some() {
            return Err(StorageError::InvalidSyncRootChangePageBoundary);
        }

        let transaction = self.connection.transaction()?;

        let cursor_state: Option<SyncRootCursorStateRow> = transaction
            .query_row(
                "SELECT
                    snapshot_complete,
                    catchup_complete,
                    catchup_from_cursor,
                    change_cursor
                 FROM sync_root_remote_inventory_state
                 WHERE sync_root_id = ?1",
                params![sync_root_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;

        let Some((snapshot_complete, catchup_complete, catchup_from_cursor, change_cursor)) =
            cursor_state
        else {
            return Err(StorageError::SyncRootCatalogSnapshotMissing);
        };

        if snapshot_complete == 0 {
            return Err(StorageError::SyncRootCatalogSnapshotMissing);
        }

        let durable_cursor = if catchup_complete == 0 {
            catchup_from_cursor
                .as_deref()
                .ok_or(StorageError::SyncRootCatalogCatchupCursorMissing)?
        } else {
            change_cursor
                .as_deref()
                .ok_or(StorageError::SyncRootCatalogChangeCursorMissing)?
        };

        if durable_cursor != base_cursor.as_str() {
            return Err(StorageError::SyncRootChangeWindowBaseCursorMismatch);
        }

        let existing: Option<SyncRootChangeWindowStateRow> = transaction
            .query_row(
                "SELECT
                        base_cursor,
                        continuation,
                        checkpoint,
                        page_count,
                        change_count
                     FROM sync_root_change_window_state
                     WHERE sync_root_id = ?1",
                params![sync_root_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()?;

        let (page_count, change_count) = match existing {
            Some((
                stored_base_cursor,
                stored_continuation,
                stored_checkpoint,
                page_count,
                change_count,
            )) => {
                if stored_checkpoint.is_some() {
                    return Err(StorageError::SyncRootChangeWindowAlreadyComplete);
                }

                if stored_base_cursor != base_cursor.as_str() {
                    return Err(StorageError::SyncRootChangeWindowBaseCursorMismatch);
                }

                let expected = expected_continuation.map(ContinuationToken::as_str);
                if stored_continuation.as_deref() != expected {
                    return Err(StorageError::SyncRootChangeWindowContinuationMismatch);
                }

                (page_count, change_count)
            }
            None => {
                if expected_continuation.is_some() {
                    return Err(StorageError::SyncRootChangeWindowContinuationMismatch);
                }
                (0_i64, 0_i64)
            }
        };

        let next_page_count = page_count
            .checked_add(1)
            .ok_or(StorageError::NumericOverflow)?;
        if next_page_count > 1_000_000 {
            return Err(StorageError::SyncRootChangeWindowSafetyLimitExceeded);
        }

        let page_change_count =
            i64::try_from(page.changes.len()).map_err(|_| StorageError::NumericOverflow)?;
        let next_change_count = change_count
            .checked_add(page_change_count)
            .ok_or(StorageError::NumericOverflow)?;
        if next_change_count > 100_000_000 {
            return Err(StorageError::SyncRootChangeWindowSafetyLimitExceeded);
        }

        for (offset, change) in page.changes.iter().enumerate() {
            let offset = i64::try_from(offset).map_err(|_| StorageError::NumericOverflow)?;
            let sequence = change_count
                .checked_add(offset)
                .ok_or(StorageError::NumericOverflow)?;

            match change {
                RemoteChange::Delete { remote_id } => {
                    if remote_id.trim().is_empty() {
                        return Err(StorageError::InvalidSyncRootChangeEvent);
                    }

                    transaction.execute(
                        "INSERT INTO sync_root_change_window_events (
                            sync_root_id,
                            sequence,
                            event_kind,
                            remote_id,
                            parent_remote_id,
                            name,
                            item_kind,
                            size_bytes,
                            trashed
                         ) VALUES (?1, ?2, 'delete', ?3, NULL, NULL, NULL, NULL, 0)",
                        params![sync_root_id, sequence, remote_id],
                    )?;
                }
                RemoteChange::Upsert(item) => {
                    if item.remote_id.trim().is_empty() || item.name.is_empty() {
                        return Err(StorageError::InvalidSyncRootChangeEvent);
                    }

                    let item_kind = match item.kind {
                        RemoteItemKind::File => "file",
                        RemoteItemKind::Folder => "folder",
                    };
                    let size_bytes = item
                        .size_bytes
                        .map(i64::try_from)
                        .transpose()
                        .map_err(|_| StorageError::NumericOverflow)?;

                    transaction.execute(
                        "INSERT INTO sync_root_change_window_events (
                            sync_root_id,
                            sequence,
                            event_kind,
                            remote_id,
                            parent_remote_id,
                            name,
                            item_kind,
                            size_bytes,
                            trashed
                         ) VALUES (?1, ?2, 'upsert', ?3, ?4, ?5, ?6, ?7, ?8)",
                        params![
                            sync_root_id,
                            sequence,
                            item.remote_id,
                            item.parent_remote_id,
                            item.name,
                            item_kind,
                            size_bytes,
                            if item.trashed { 1_i64 } else { 0_i64 }
                        ],
                    )?;
                }
            }
        }

        let next_continuation = page.continuation.as_ref().map(ContinuationToken::as_str);
        let checkpoint = page.checkpoint.as_ref().map(ChangeCursor::as_str);

        if let Some(token) = next_continuation {
            let inserted = transaction.execute(
                "INSERT OR IGNORE INTO sync_root_change_window_tokens (
                    sync_root_id, token
                 ) VALUES (?1, ?2)",
                params![sync_root_id, token],
            )?;

            if inserted == 0 {
                return Err(StorageError::SyncRootChangeWindowPaginationLoop);
            }
        }

        transaction.execute(
            "INSERT INTO sync_root_change_window_state (
                sync_root_id,
                base_cursor,
                continuation,
                checkpoint,
                page_count,
                change_count
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(sync_root_id) DO UPDATE SET
                continuation = excluded.continuation,
                checkpoint = excluded.checkpoint,
                page_count = excluded.page_count,
                change_count = excluded.change_count",
            params![
                sync_root_id,
                base_cursor.as_str(),
                next_continuation,
                checkpoint,
                next_page_count,
                next_change_count
            ],
        )?;

        transaction.commit()?;

        Ok(SyncRootChangeWindowState {
            base_cursor: base_cursor.clone(),
            continuation: page.continuation.clone(),
            checkpoint: page.checkpoint.clone(),
            page_count: u64::try_from(next_page_count)
                .map_err(|_| StorageError::NumericOverflow)?,
            change_count: u64::try_from(next_change_count)
                .map_err(|_| StorageError::NumericOverflow)?,
        })
    }

    pub fn sync_root_change_window_changes(
        &self,
        sync_root_id: &str,
    ) -> Result<Vec<RemoteChange>, StorageError> {
        let mut statement = self.connection.prepare(
            "SELECT
                event_kind,
                remote_id,
                parent_remote_id,
                name,
                item_kind,
                size_bytes,
                trashed
             FROM sync_root_change_window_events
             WHERE sync_root_id = ?1
             ORDER BY sequence",
        )?;

        let rows = statement.query_map(params![sync_root_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<i64>>(5)?,
                row.get::<_, i64>(6)?,
            ))
        })?;

        let mut changes = Vec::new();

        for row in rows {
            let (event_kind, remote_id, parent_remote_id, name, item_kind, size_bytes, trashed) =
                row?;

            match event_kind.as_str() {
                "delete" => {
                    changes.push(RemoteChange::Delete { remote_id });
                }
                "upsert" => {
                    let name = name.ok_or(StorageError::InvalidStoredRemoteChange)?;
                    let item_kind = item_kind.ok_or(StorageError::InvalidStoredRemoteChange)?;
                    let kind = match item_kind.as_str() {
                        "file" => RemoteItemKind::File,
                        "folder" => RemoteItemKind::Folder,
                        _ => return Err(StorageError::InvalidStoredRemoteChange),
                    };
                    let size_bytes = size_bytes
                        .map(u64::try_from)
                        .transpose()
                        .map_err(|_| StorageError::NumericOverflow)?;

                    changes.push(RemoteChange::Upsert(RemoteItem {
                        remote_id,
                        parent_remote_id,
                        name,
                        kind,
                        size_bytes,
                        modified_unix_ms: None,
                        trashed: trashed != 0,
                    }));
                }
                _ => return Err(StorageError::InvalidStoredRemoteChange),
            }
        }

        Ok(changes)
    }

    pub fn commit_sync_root_catalog_batch_and_cursor(
        &mut self,
        sync_root_id: &str,
        expected_cursor: &ChangeCursor,
        mutations: &[SyncRootCatalogMutation],
        next_cursor: &ChangeCursor,
        observed_at_unix_ms: i64,
    ) -> Result<SyncRootCatalogBatchCommit, StorageError> {
        let transaction = self.connection.transaction()?;

        let state: Option<SyncRootCursorStateRow> = transaction
            .query_row(
                "SELECT
                    snapshot_complete,
                    catchup_complete,
                    catchup_from_cursor,
                    change_cursor
                 FROM sync_root_remote_inventory_state
                 WHERE sync_root_id = ?1",
                params![sync_root_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;

        let Some((snapshot_complete, catchup_complete, catchup_from_cursor, change_cursor)) = state
        else {
            return Err(StorageError::SyncRootCatalogSnapshotMissing);
        };

        if snapshot_complete == 0 {
            return Err(StorageError::SyncRootCatalogSnapshotMissing);
        }

        let completed_initial_catchup = catchup_complete == 0;

        let current_cursor = if completed_initial_catchup {
            if change_cursor.is_some() {
                return Err(StorageError::SyncRootCatalogInvalidCursorState);
            }

            catchup_from_cursor
                .as_deref()
                .ok_or(StorageError::SyncRootCatalogCatchupCursorMissing)?
        } else {
            change_cursor
                .as_deref()
                .ok_or(StorageError::SyncRootCatalogChangeCursorMissing)?
        };

        if current_cursor != expected_cursor.as_str() {
            return Err(StorageError::SyncRootCatalogExpectedCursorMismatch);
        }

        for mutation in mutations {
            match mutation {
                SyncRootCatalogMutation::Upsert(item) => {
                    if item.trashed || item.remote_id.trim().is_empty() {
                        return Err(StorageError::InvalidSyncRootCatalogMutation);
                    }

                    upsert_sync_root_catalog_item(
                        &transaction,
                        sync_root_id,
                        item,
                        observed_at_unix_ms,
                    )?;
                }
                SyncRootCatalogMutation::DeleteSubtree { remote_id } => {
                    if remote_id.trim().is_empty() {
                        return Err(StorageError::InvalidSyncRootCatalogMutation);
                    }

                    delete_sync_root_subtree_in_transaction(&transaction, sync_root_id, remote_id)?;
                }
            }
        }

        refresh_sync_root_catalog_count(&transaction, sync_root_id)?;

        let authoritative_items_i64: i64 = transaction.query_row(
            "SELECT item_count
             FROM sync_root_remote_inventory_state
             WHERE sync_root_id = ?1",
            params![sync_root_id],
            |row| row.get(0),
        )?;
        let authoritative_items =
            u64::try_from(authoritative_items_i64).map_err(|_| StorageError::NumericOverflow)?;

        transaction.execute(
            "UPDATE sync_root_remote_inventory_state
             SET
                catchup_complete = 1,
                change_cursor = ?2
             WHERE sync_root_id = ?1",
            params![sync_root_id, next_cursor.as_str()],
        )?;

        transaction.commit()?;

        Ok(SyncRootCatalogBatchCommit {
            mutations_applied: mutations.len(),
            authoritative_items,
            completed_initial_catchup,
        })
    }

    pub fn commit_sync_root_catalog_change_window(
        &mut self,
        sync_root_id: &str,
        expected_base_cursor: &ChangeCursor,
        expected_checkpoint: &ChangeCursor,
        expected_change_count: u64,
        mutations: &[SyncRootCatalogMutation],
        observed_at_unix_ms: i64,
    ) -> Result<SyncRootCatalogBatchCommit, StorageError> {
        let expected_change_count =
            i64::try_from(expected_change_count).map_err(|_| StorageError::NumericOverflow)?;

        let transaction = self.connection.transaction()?;

        let window: Option<SyncRootChangeWindowStateRow> = transaction
            .query_row(
                "SELECT
                    base_cursor,
                    continuation,
                    checkpoint,
                    page_count,
                    change_count
                 FROM sync_root_change_window_state
                 WHERE sync_root_id = ?1",
                params![sync_root_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()?;

        let Some((base_cursor, continuation, checkpoint, _page_count, change_count)) = window
        else {
            return Err(StorageError::SyncRootChangeWindowMissing);
        };

        if continuation.is_some() || checkpoint.is_none() {
            return Err(StorageError::SyncRootChangeWindowIncomplete);
        }

        if base_cursor != expected_base_cursor.as_str() {
            return Err(StorageError::SyncRootChangeWindowBaseCursorMismatch);
        }

        if checkpoint.as_deref() != Some(expected_checkpoint.as_str()) {
            return Err(StorageError::SyncRootChangeWindowCheckpointMismatch);
        }

        if change_count != expected_change_count {
            return Err(StorageError::SyncRootChangeWindowChangeCountMismatch);
        }

        let stored_event_count: i64 = transaction.query_row(
            "SELECT COUNT(*)
             FROM sync_root_change_window_events
             WHERE sync_root_id = ?1",
            params![sync_root_id],
            |row| row.get(0),
        )?;

        if stored_event_count != change_count {
            return Err(StorageError::SyncRootChangeWindowChangeCountMismatch);
        }

        let state: Option<SyncRootCursorStateRow> = transaction
            .query_row(
                "SELECT
                    snapshot_complete,
                    catchup_complete,
                    catchup_from_cursor,
                    change_cursor
                 FROM sync_root_remote_inventory_state
                 WHERE sync_root_id = ?1",
                params![sync_root_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;

        let Some((snapshot_complete, catchup_complete, catchup_from_cursor, change_cursor)) = state
        else {
            return Err(StorageError::SyncRootCatalogSnapshotMissing);
        };

        if snapshot_complete == 0 {
            return Err(StorageError::SyncRootCatalogSnapshotMissing);
        }

        let completed_initial_catchup = catchup_complete == 0;

        let current_cursor = if completed_initial_catchup {
            if change_cursor.is_some() {
                return Err(StorageError::SyncRootCatalogInvalidCursorState);
            }

            catchup_from_cursor
                .as_deref()
                .ok_or(StorageError::SyncRootCatalogCatchupCursorMissing)?
        } else {
            change_cursor
                .as_deref()
                .ok_or(StorageError::SyncRootCatalogChangeCursorMissing)?
        };

        if current_cursor != expected_base_cursor.as_str() {
            return Err(StorageError::SyncRootCatalogExpectedCursorMismatch);
        }

        for mutation in mutations {
            match mutation {
                SyncRootCatalogMutation::Upsert(item) => {
                    if item.trashed || item.remote_id.trim().is_empty() {
                        return Err(StorageError::InvalidSyncRootCatalogMutation);
                    }

                    upsert_sync_root_catalog_item(
                        &transaction,
                        sync_root_id,
                        item,
                        observed_at_unix_ms,
                    )?;
                }
                SyncRootCatalogMutation::DeleteSubtree { remote_id } => {
                    if remote_id.trim().is_empty() {
                        return Err(StorageError::InvalidSyncRootCatalogMutation);
                    }

                    delete_sync_root_subtree_in_transaction(&transaction, sync_root_id, remote_id)?;
                }
            }
        }

        refresh_sync_root_catalog_count(&transaction, sync_root_id)?;

        let authoritative_items_i64: i64 = transaction.query_row(
            "SELECT item_count
             FROM sync_root_remote_inventory_state
             WHERE sync_root_id = ?1",
            params![sync_root_id],
            |row| row.get(0),
        )?;
        let authoritative_items =
            u64::try_from(authoritative_items_i64).map_err(|_| StorageError::NumericOverflow)?;

        transaction.execute(
            "UPDATE sync_root_remote_inventory_state
             SET
                catchup_complete = 1,
                change_cursor = ?2
             WHERE sync_root_id = ?1",
            params![sync_root_id, expected_checkpoint.as_str()],
        )?;

        transaction.execute(
            "DELETE FROM sync_root_change_window_events
             WHERE sync_root_id = ?1",
            params![sync_root_id],
        )?;
        transaction.execute(
            "DELETE FROM sync_root_change_window_tokens
             WHERE sync_root_id = ?1",
            params![sync_root_id],
        )?;
        transaction.execute(
            "DELETE FROM sync_root_change_window_state
             WHERE sync_root_id = ?1",
            params![sync_root_id],
        )?;

        transaction.commit()?;

        Ok(SyncRootCatalogBatchCommit {
            mutations_applied: mutations.len(),
            authoritative_items,
            completed_initial_catchup,
        })
    }

    pub fn record_sync_root_file_materialization(
        &mut self,
        sync_root_id: &str,
        remote_id: &str,
        relative_path: &str,
        size_bytes: u64,
        sha256_hex: &str,
        materialized_at_unix_ms: i64,
    ) -> Result<(), StorageError> {
        validate_materialization_receipt_values(remote_id, relative_path, sha256_hex)?;

        let size_bytes_i64 =
            i64::try_from(size_bytes).map_err(|_| StorageError::NumericOverflow)?;
        let transaction = self.connection.transaction()?;

        let remote: Option<(String, Option<i64>, i64)> = transaction
            .query_row(
                "SELECT item_kind, size_bytes, trashed
                 FROM sync_root_remote_items
                 WHERE sync_root_id = ?1 AND remote_id = ?2",
                params![sync_root_id, remote_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;

        let Some((item_kind, durable_size, trashed)) = remote else {
            return Err(StorageError::MaterializationReceiptRemoteItemMissing);
        };

        if item_kind != "file" || trashed != 0 || durable_size != Some(size_bytes_i64) {
            return Err(StorageError::MaterializationReceiptRemoteItemMismatch);
        }

        transaction.execute(
            "INSERT INTO sync_root_file_materialization_receipts (
                sync_root_id,
                remote_id,
                relative_path,
                size_bytes,
                sha256_hex,
                materialized_at_unix_ms,
                receipt_state
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'current')
             ON CONFLICT(sync_root_id, remote_id) DO UPDATE SET
                relative_path = excluded.relative_path,
                size_bytes = excluded.size_bytes,
                sha256_hex = excluded.sha256_hex,
                materialized_at_unix_ms = excluded.materialized_at_unix_ms,
                receipt_state = 'current'",
            params![
                sync_root_id,
                remote_id,
                relative_path,
                size_bytes_i64,
                sha256_hex,
                materialized_at_unix_ms
            ],
        )?;

        transaction.commit()?;
        Ok(())
    }

    pub fn record_sync_root_file_materializations(
        &mut self,
        sync_root_id: &str,
        files: &[(String, String, u64, String)],
        materialized_at_unix_ms: i64,
    ) -> Result<usize, StorageError> {
        if files.is_empty() {
            return Ok(0);
        }

        let mut remote_ids = std::collections::HashSet::with_capacity(files.len());
        let mut relative_paths = std::collections::HashSet::with_capacity(files.len());

        for (remote_id, relative_path, _, sha256_hex) in files {
            validate_materialization_receipt_values(remote_id, relative_path, sha256_hex)?;
            if !remote_ids.insert(remote_id.as_str())
                || !relative_paths.insert(relative_path.as_str())
            {
                return Err(StorageError::InvalidMaterializationReceipt);
            }
        }

        let transaction = self.connection.transaction()?;

        for (remote_id, relative_path, size_bytes, sha256_hex) in files {
            let size_bytes_i64 =
                i64::try_from(*size_bytes).map_err(|_| StorageError::NumericOverflow)?;

            let remote: Option<(String, Option<i64>, i64)> = transaction
                .query_row(
                    "SELECT item_kind, size_bytes, trashed
                     FROM sync_root_remote_items
                     WHERE sync_root_id = ?1 AND remote_id = ?2",
                    params![sync_root_id, remote_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()?;

            let Some((item_kind, durable_size, trashed)) = remote else {
                return Err(StorageError::MaterializationReceiptRemoteItemMissing);
            };

            if item_kind != "file" || trashed != 0 || durable_size != Some(size_bytes_i64) {
                return Err(StorageError::MaterializationReceiptRemoteItemMismatch);
            }

            transaction.execute(
                "INSERT INTO sync_root_file_materialization_receipts (
                    sync_root_id,
                    remote_id,
                    relative_path,
                    size_bytes,
                    sha256_hex,
                    materialized_at_unix_ms,
                    receipt_state
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'current')
                 ON CONFLICT(sync_root_id, remote_id) DO UPDATE SET
                    relative_path = excluded.relative_path,
                    size_bytes = excluded.size_bytes,
                    sha256_hex = excluded.sha256_hex,
                    materialized_at_unix_ms = excluded.materialized_at_unix_ms,
                    receipt_state = 'current'",
                params![
                    sync_root_id,
                    remote_id,
                    relative_path,
                    size_bytes_i64,
                    sha256_hex,
                    materialized_at_unix_ms
                ],
            )?;
        }

        transaction.commit()?;
        Ok(files.len())
    }

    pub fn list_sync_root_file_materialization_receipts(
        &self,
        sync_root_id: &str,
    ) -> Result<Vec<SyncRootFileMaterializationReceipt>, StorageError> {
        let mut statement = self.connection.prepare(
            "SELECT
                remote_id,
                relative_path,
                size_bytes,
                sha256_hex,
                materialized_at_unix_ms
             FROM sync_root_file_materialization_receipts
             WHERE sync_root_id = ?1
               AND receipt_state = 'current'
             ORDER BY remote_id",
        )?;

        let rows = statement.query_map(params![sync_root_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })?;

        let mut receipts = Vec::new();
        for row in rows {
            let (remote_id, relative_path, size_bytes, sha256_hex, materialized_at_unix_ms) = row?;

            validate_materialization_receipt_values(&remote_id, &relative_path, &sha256_hex)?;

            receipts.push(SyncRootFileMaterializationReceipt {
                remote_id,
                relative_path,
                size_bytes: u64::try_from(size_bytes).map_err(|_| StorageError::NumericOverflow)?,
                sha256_hex,
                materialized_at_unix_ms,
            });
        }

        Ok(receipts)
    }

    pub fn list_sync_root_stale_file_materialization_receipts(
        &self,
        sync_root_id: &str,
    ) -> Result<Vec<SyncRootFileMaterializationReceipt>, StorageError> {
        let mut statement = self.connection.prepare(
            "SELECT
                remote_id,
                relative_path,
                size_bytes,
                sha256_hex,
                materialized_at_unix_ms
             FROM sync_root_file_materialization_receipts
             WHERE sync_root_id = ?1
               AND receipt_state = 'stale'
             ORDER BY remote_id",
        )?;

        let rows = statement.query_map(params![sync_root_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })?;

        let mut receipts = Vec::new();
        for row in rows {
            let (remote_id, relative_path, size_bytes, sha256_hex, materialized_at_unix_ms) = row?;

            validate_materialization_receipt_values(&remote_id, &relative_path, &sha256_hex)?;

            receipts.push(SyncRootFileMaterializationReceipt {
                remote_id,
                relative_path,
                size_bytes: u64::try_from(size_bytes).map_err(|_| StorageError::NumericOverflow)?,
                sha256_hex,
                materialized_at_unix_ms,
            });
        }

        Ok(receipts)
    }

    pub fn sync_root_materialization_receipt_count(
        &self,
        sync_root_id: &str,
    ) -> Result<u64, StorageError> {
        let count: i64 = self.connection.query_row(
            "SELECT COUNT(*)
             FROM sync_root_file_materialization_receipts
             WHERE sync_root_id = ?1
               AND receipt_state = 'current'",
            params![sync_root_id],
            |row| row.get(0),
        )?;
        u64::try_from(count).map_err(|_| StorageError::NumericOverflow)
    }

    pub fn sync_root_stale_materialization_receipt_count(
        &self,
        sync_root_id: &str,
    ) -> Result<u64, StorageError> {
        let count: i64 = self.connection.query_row(
            "SELECT COUNT(*)
             FROM sync_root_file_materialization_receipts
             WHERE sync_root_id = ?1
               AND receipt_state = 'stale'",
            params![sync_root_id],
            |row| row.get(0),
        )?;
        u64::try_from(count).map_err(|_| StorageError::NumericOverflow)
    }

    pub fn delete_sync_root_stale_file_materialization_receipts(
        &mut self,
        sync_root_id: &str,
        remote_ids: &[String],
    ) -> Result<usize, StorageError> {
        if remote_ids.is_empty() {
            return Ok(0);
        }

        let mut unique_ids = std::collections::HashSet::with_capacity(remote_ids.len());
        for remote_id in remote_ids {
            if remote_id.trim().is_empty() || !unique_ids.insert(remote_id.as_str()) {
                return Err(StorageError::InvalidMaterializationReceipt);
            }
        }

        let transaction = self.connection.transaction()?;

        for remote_id in remote_ids {
            let deleted = transaction.execute(
                "DELETE FROM sync_root_file_materialization_receipts
                 WHERE sync_root_id = ?1
                   AND remote_id = ?2
                   AND receipt_state = 'stale'",
                params![sync_root_id, remote_id],
            )?;

            if deleted != 1 {
                return Err(StorageError::StaleMaterializationReceiptBatchMismatch);
            }
        }

        transaction.commit()?;
        Ok(remote_ids.len())
    }

    pub fn delete_sync_root_stale_file_materialization_receipt(
        &mut self,
        sync_root_id: &str,
        remote_id: &str,
    ) -> Result<bool, StorageError> {
        let deleted = self.connection.execute(
            "DELETE FROM sync_root_file_materialization_receipts
             WHERE sync_root_id = ?1
               AND remote_id = ?2
               AND receipt_state = 'stale'",
            params![sync_root_id, remote_id],
        )?;

        Ok(deleted == 1)
    }

    pub fn delete_sync_root_stale_directory_materialization_receipts(
        &mut self,
        sync_root_id: &str,
        remote_ids: &[String],
    ) -> Result<usize, StorageError> {
        if remote_ids.is_empty() {
            return Ok(0);
        }

        let mut unique_ids = std::collections::HashSet::with_capacity(remote_ids.len());
        for remote_id in remote_ids {
            if remote_id.trim().is_empty() || !unique_ids.insert(remote_id.as_str()) {
                return Err(StorageError::InvalidDirectoryMaterializationReceipt);
            }
        }

        let transaction = self.connection.transaction()?;

        for remote_id in remote_ids {
            let deleted = transaction.execute(
                "DELETE FROM sync_root_directory_materialization_receipts
                 WHERE sync_root_id = ?1
                   AND remote_id = ?2
                   AND receipt_state = 'stale'",
                params![sync_root_id, remote_id],
            )?;

            if deleted != 1 {
                return Err(StorageError::StaleDirectoryMaterializationReceiptBatchMismatch);
            }
        }

        transaction.commit()?;
        Ok(remote_ids.len())
    }

    pub fn delete_sync_root_stale_directory_materialization_receipt(
        &mut self,
        sync_root_id: &str,
        remote_id: &str,
    ) -> Result<bool, StorageError> {
        let deleted = self.connection.execute(
            "DELETE FROM sync_root_directory_materialization_receipts
             WHERE sync_root_id = ?1
               AND remote_id = ?2
               AND receipt_state = 'stale'",
            params![sync_root_id, remote_id],
        )?;

        Ok(deleted == 1)
    }

    pub fn record_sync_root_directory_materializations(
        &mut self,
        sync_root_id: &str,
        directories: &[(String, String)],
        materialized_at_unix_ms: i64,
    ) -> Result<usize, StorageError> {
        if directories.is_empty() {
            return Ok(0);
        }

        let mut remote_ids = std::collections::HashSet::with_capacity(directories.len());
        let mut relative_paths = std::collections::HashSet::with_capacity(directories.len());
        for (remote_id, relative_path) in directories {
            validate_directory_materialization_receipt_values(remote_id, relative_path)?;
            if !remote_ids.insert(remote_id.as_str())
                || !relative_paths.insert(relative_path.as_str())
            {
                return Err(StorageError::InvalidDirectoryMaterializationReceipt);
            }
        }

        let transaction = self.connection.transaction()?;

        for (remote_id, relative_path) in directories {
            let remote: Option<(String, i64)> = transaction
                .query_row(
                    "SELECT item_kind, trashed
                     FROM sync_root_remote_items
                     WHERE sync_root_id = ?1 AND remote_id = ?2",
                    params![sync_root_id, remote_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;

            let Some((item_kind, trashed)) = remote else {
                return Err(StorageError::DirectoryMaterializationReceiptRemoteItemMissing);
            };

            if item_kind != "folder" || trashed != 0 {
                return Err(StorageError::DirectoryMaterializationReceiptRemoteItemMismatch);
            }

            transaction.execute(
                "INSERT INTO sync_root_directory_materialization_receipts (
                    sync_root_id,
                    remote_id,
                    relative_path,
                    materialized_at_unix_ms,
                    receipt_state
                 ) VALUES (?1, ?2, ?3, ?4, 'current')
                 ON CONFLICT(sync_root_id, remote_id) DO UPDATE SET
                    relative_path = excluded.relative_path,
                    materialized_at_unix_ms = excluded.materialized_at_unix_ms,
                    receipt_state = 'current'",
                params![
                    sync_root_id,
                    remote_id,
                    relative_path,
                    materialized_at_unix_ms
                ],
            )?;
        }

        transaction.commit()?;
        Ok(directories.len())
    }

    pub fn list_sync_root_directory_materialization_receipts(
        &self,
        sync_root_id: &str,
    ) -> Result<Vec<SyncRootDirectoryMaterializationReceipt>, StorageError> {
        self.list_sync_root_directory_materialization_receipts_by_state(sync_root_id, "current")
    }

    pub fn list_sync_root_stale_directory_materialization_receipts(
        &self,
        sync_root_id: &str,
    ) -> Result<Vec<SyncRootDirectoryMaterializationReceipt>, StorageError> {
        self.list_sync_root_directory_materialization_receipts_by_state(sync_root_id, "stale")
    }

    fn list_sync_root_directory_materialization_receipts_by_state(
        &self,
        sync_root_id: &str,
        state: &str,
    ) -> Result<Vec<SyncRootDirectoryMaterializationReceipt>, StorageError> {
        if !matches!(state, "current" | "stale") {
            return Err(StorageError::InvalidDirectoryMaterializationReceipt);
        }

        let mut statement = self.connection.prepare(
            "SELECT remote_id, relative_path, materialized_at_unix_ms
             FROM sync_root_directory_materialization_receipts
             WHERE sync_root_id = ?1
               AND receipt_state = ?2
             ORDER BY remote_id",
        )?;

        let rows = statement.query_map(params![sync_root_id, state], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?;

        let mut receipts = Vec::new();
        for row in rows {
            let (remote_id, relative_path, materialized_at_unix_ms) = row?;
            validate_directory_materialization_receipt_values(&remote_id, &relative_path)?;
            receipts.push(SyncRootDirectoryMaterializationReceipt {
                remote_id,
                relative_path,
                materialized_at_unix_ms,
            });
        }

        Ok(receipts)
    }

    pub fn sync_root_directory_materialization_receipt_count(
        &self,
        sync_root_id: &str,
    ) -> Result<u64, StorageError> {
        self.sync_root_directory_materialization_receipt_count_by_state(sync_root_id, "current")
    }

    pub fn sync_root_stale_directory_materialization_receipt_count(
        &self,
        sync_root_id: &str,
    ) -> Result<u64, StorageError> {
        self.sync_root_directory_materialization_receipt_count_by_state(sync_root_id, "stale")
    }

    fn sync_root_directory_materialization_receipt_count_by_state(
        &self,
        sync_root_id: &str,
        state: &str,
    ) -> Result<u64, StorageError> {
        if !matches!(state, "current" | "stale") {
            return Err(StorageError::InvalidDirectoryMaterializationReceipt);
        }

        let count: i64 = self.connection.query_row(
            "SELECT COUNT(*)
             FROM sync_root_directory_materialization_receipts
             WHERE sync_root_id = ?1
               AND receipt_state = ?2",
            params![sync_root_id, state],
            |row| row.get(0),
        )?;

        u64::try_from(count).map_err(|_| StorageError::NumericOverflow)
    }

    pub fn sync_root_change_cursor(
        &self,
        sync_root_id: &str,
    ) -> Result<Option<ChangeCursor>, StorageError> {
        let cursor: Option<Option<String>> = self
            .connection
            .query_row(
                "SELECT change_cursor
                 FROM sync_root_remote_inventory_state
                 WHERE sync_root_id = ?1",
                params![sync_root_id],
                |row| row.get(0),
            )
            .optional()?;

        cursor
            .flatten()
            .map(ChangeCursor::new)
            .transpose()
            .map_err(StorageError::from)
    }

    pub fn sync_root_change_cursor_count(
        &self,
        provider: &ProviderId,
        account_subject: &str,
    ) -> Result<u64, StorageError> {
        let count: i64 = self.connection.query_row(
            "SELECT COUNT(*)
             FROM sync_root_remote_inventory_state AS state
             INNER JOIN sync_roots AS root ON root.id = state.sync_root_id
             WHERE root.provider = ?1
               AND root.account_subject = ?2
               AND state.change_cursor IS NOT NULL",
            params![provider.as_str(), account_subject],
            |row| row.get(0),
        )?;

        u64::try_from(count).map_err(|_| StorageError::NumericOverflow)
    }

    pub fn sync_root_catalog_state_count(
        &self,
        provider: &ProviderId,
        account_subject: &str,
    ) -> Result<u64, StorageError> {
        let count: i64 = self.connection.query_row(
            "SELECT COUNT(*)
             FROM sync_root_remote_inventory_state AS state
             INNER JOIN sync_roots AS root ON root.id = state.sync_root_id
             WHERE root.provider = ?1 AND root.account_subject = ?2",
            params![provider.as_str(), account_subject],
            |row| row.get(0),
        )?;

        u64::try_from(count).map_err(|_| StorageError::NumericOverflow)
    }

    pub fn sync_root_catalog_item_count(
        &self,
        provider: &ProviderId,
        account_subject: &str,
    ) -> Result<u64, StorageError> {
        let count: i64 = self.connection.query_row(
            "SELECT COUNT(*)
             FROM sync_root_remote_items AS item
             INNER JOIN sync_roots AS root ON root.id = item.sync_root_id
             WHERE root.provider = ?1 AND root.account_subject = ?2",
            params![provider.as_str(), account_subject],
            |row| row.get(0),
        )?;

        u64::try_from(count).map_err(|_| StorageError::NumericOverflow)
    }

    pub fn save_cursor(
        &self,
        provider: &ProviderId,
        account_subject: &str,
        cursor: &ChangeCursor,
        updated_at_unix_ms: i64,
    ) -> Result<(), StorageError> {
        self.connection.execute(
            "
            INSERT INTO provider_cursors (
                provider, account_subject, cursor, updated_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(provider, account_subject) DO UPDATE SET
                cursor = excluded.cursor,
                updated_at_unix_ms = excluded.updated_at_unix_ms
            ",
            params![
                provider.as_str(),
                account_subject,
                cursor.as_str(),
                updated_at_unix_ms
            ],
        )?;

        Ok(())
    }

    pub fn load_cursor(
        &self,
        provider: &ProviderId,
        account_subject: &str,
    ) -> Result<Option<ChangeCursor>, StorageError> {
        let cursor: Option<String> = self
            .connection
            .query_row(
                "
                SELECT cursor
                FROM provider_cursors
                WHERE provider = ?1 AND account_subject = ?2
                ",
                params![provider.as_str(), account_subject],
                |row| row.get(0),
            )
            .optional()?;

        cursor
            .map(ChangeCursor::new)
            .transpose()
            .map_err(StorageError::from)
    }

    /// Persists a complete remote change batch and advances its provider cursor
    /// in the same SQLite transaction.
    ///
    /// If any event cannot be persisted, the cursor is not advanced and every
    /// insert in this batch is rolled back.
    pub fn commit_remote_changes_and_cursor(
        &mut self,
        provider: &ProviderId,
        account_subject: &str,
        changes: &[RemoteChange],
        cursor: &ChangeCursor,
        observed_at_unix_ms: i64,
    ) -> Result<usize, StorageError> {
        let transaction = self.connection.transaction()?;

        for change in changes {
            insert_remote_event(
                &transaction,
                provider,
                account_subject,
                change,
                observed_at_unix_ms,
            )?;
        }

        transaction.execute(
            "
            INSERT INTO provider_cursors (
                provider, account_subject, cursor, updated_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(provider, account_subject) DO UPDATE SET
                cursor = excluded.cursor,
                updated_at_unix_ms = excluded.updated_at_unix_ms
            ",
            params![
                provider.as_str(),
                account_subject,
                cursor.as_str(),
                observed_at_unix_ms
            ],
        )?;

        transaction.commit()?;
        Ok(changes.len())
    }

    pub fn begin_remote_inventory_staging(
        &self,
        provider: &ProviderId,
        account_subject: &str,
    ) -> Result<(), StorageError> {
        self.connection.execute(
            "DELETE FROM remote_inventory_staging WHERE provider = ?1 AND account_subject = ?2",
            params![provider.as_str(), account_subject],
        )?;
        Ok(())
    }

    pub fn stage_remote_inventory_items(
        &mut self,
        provider: &ProviderId,
        account_subject: &str,
        items: &[RemoteItem],
        observed_at_unix_ms: i64,
    ) -> Result<usize, StorageError> {
        let transaction = self.connection.transaction()?;
        for item in items {
            insert_inventory_item(
                &transaction,
                provider,
                account_subject,
                item,
                observed_at_unix_ms,
            )?;
        }
        transaction.commit()?;
        Ok(items.len())
    }

    pub fn staged_remote_inventory_count(
        &self,
        provider: &ProviderId,
        account_subject: &str,
    ) -> Result<u64, StorageError> {
        count_inventory(
            &self.connection,
            "remote_inventory_staging",
            provider,
            account_subject,
        )
    }

    pub fn clear_remote_inventory_staging(
        &self,
        provider: &ProviderId,
        account_subject: &str,
    ) -> Result<(), StorageError> {
        self.connection.execute(
            "DELETE FROM remote_inventory_staging WHERE provider = ?1 AND account_subject = ?2",
            params![provider.as_str(), account_subject],
        )?;
        Ok(())
    }

    pub fn commit_remote_inventory_snapshot(
        &mut self,
        provider: &ProviderId,
        account_subject: &str,
        catchup_from_cursor: &ChangeCursor,
        completed_at_unix_ms: i64,
    ) -> Result<usize, StorageError> {
        let transaction = self.connection.transaction()?;

        transaction.execute(
            "DELETE FROM remote_items WHERE provider = ?1 AND account_subject = ?2",
            params![provider.as_str(), account_subject],
        )?;

        let inserted = transaction.execute(
            "INSERT INTO remote_items (
                provider,
                account_subject,
                remote_id,
                parent_remote_id,
                name,
                item_kind,
                size_bytes,
                trashed,
                observed_at_unix_ms
             )
             SELECT
                provider,
                account_subject,
                remote_id,
                parent_remote_id,
                name,
                item_kind,
                size_bytes,
                trashed,
                observed_at_unix_ms
             FROM remote_inventory_staging
             WHERE provider = ?1 AND account_subject = ?2",
            params![provider.as_str(), account_subject],
        )?;

        let item_count = i64::try_from(inserted).map_err(|_| StorageError::NumericOverflow)?;

        transaction.execute(
            "INSERT INTO remote_inventory_state (
                provider,
                account_subject,
                snapshot_complete,
                catchup_complete,
                item_count,
                snapshot_completed_at_unix_ms,
                catchup_from_cursor
             ) VALUES (?1, ?2, 1, 0, ?3, ?4, ?5)
             ON CONFLICT(provider, account_subject) DO UPDATE SET
                snapshot_complete = 1,
                catchup_complete = 0,
                item_count = excluded.item_count,
                snapshot_completed_at_unix_ms = excluded.snapshot_completed_at_unix_ms,
                catchup_from_cursor = excluded.catchup_from_cursor",
            params![
                provider.as_str(),
                account_subject,
                item_count,
                completed_at_unix_ms,
                catchup_from_cursor.as_str()
            ],
        )?;

        transaction.execute(
            "DELETE FROM remote_inventory_staging
             WHERE provider = ?1 AND account_subject = ?2",
            params![provider.as_str(), account_subject],
        )?;

        transaction.commit()?;
        Ok(inserted)
    }

    pub fn remote_inventory_state(
        &self,
        provider: &ProviderId,
        account_subject: &str,
    ) -> Result<RemoteInventoryState, StorageError> {
        let row: Option<RemoteInventoryStateRow> = self
            .connection
            .query_row(
                "SELECT
                    snapshot_complete,
                    catchup_complete,
                    item_count,
                    snapshot_completed_at_unix_ms,
                    catchup_from_cursor
                 FROM remote_inventory_state
                 WHERE provider = ?1 AND account_subject = ?2",
                params![provider.as_str(), account_subject],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()?;

        match row {
            Some((
                snapshot_complete,
                catchup_complete,
                item_count,
                completed_at,
                catchup_from_cursor,
            )) => Ok(RemoteInventoryState {
                snapshot_complete: snapshot_complete != 0,
                catchup_complete: catchup_complete != 0,
                item_count: u64::try_from(item_count).map_err(|_| StorageError::NumericOverflow)?,
                snapshot_completed_at_unix_ms: completed_at,
                catchup_from_cursor: catchup_from_cursor.map(ChangeCursor::new).transpose()?,
            }),
            None => Ok(RemoteInventoryState {
                snapshot_complete: false,
                catchup_complete: false,
                item_count: 0,
                snapshot_completed_at_unix_ms: None,
                catchup_from_cursor: None,
            }),
        }
    }

    pub fn commit_remote_catalog_catchup(
        &mut self,
        provider: &ProviderId,
        account_subject: &str,
        from_cursor: &ChangeCursor,
        changes: &[RemoteChange],
        checkpoint: &ChangeCursor,
        observed_at_unix_ms: i64,
    ) -> Result<CatalogCatchupCommit, StorageError> {
        let transaction = self.connection.transaction()?;

        let state: Option<(i64, i64, Option<String>)> = transaction
            .query_row(
                "SELECT snapshot_complete, catchup_complete, catchup_from_cursor
                 FROM remote_inventory_state
                 WHERE provider = ?1 AND account_subject = ?2",
                params![provider.as_str(), account_subject],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;

        let Some((snapshot_complete, catchup_complete, stored_from_cursor)) = state else {
            return Err(StorageError::RemoteCatalogSnapshotMissing);
        };

        if snapshot_complete == 0 {
            return Err(StorageError::RemoteCatalogSnapshotMissing);
        }

        if catchup_complete != 0 {
            return Err(StorageError::RemoteCatalogCatchupAlreadyComplete);
        }

        let stored_from_cursor =
            stored_from_cursor.ok_or(StorageError::RemoteCatalogCatchupCursorMissing)?;

        if stored_from_cursor != from_cursor.as_str() {
            return Err(StorageError::RemoteCatalogCatchupCursorMismatch);
        }

        for change in changes {
            apply_remote_change_to_inventory(
                &transaction,
                provider,
                account_subject,
                change,
                observed_at_unix_ms,
            )?;
        }

        let item_count_i64: i64 = transaction.query_row(
            "SELECT COUNT(*)
             FROM remote_items
             WHERE provider = ?1 AND account_subject = ?2",
            params![provider.as_str(), account_subject],
            |row| row.get(0),
        )?;

        let authoritative_items =
            u64::try_from(item_count_i64).map_err(|_| StorageError::NumericOverflow)?;

        transaction.execute(
            "UPDATE remote_inventory_state
             SET catchup_complete = 1,
                 item_count = ?3
             WHERE provider = ?1
               AND account_subject = ?2
               AND snapshot_complete = 1
               AND catchup_complete = 0
               AND catchup_from_cursor = ?4",
            params![
                provider.as_str(),
                account_subject,
                item_count_i64,
                from_cursor.as_str()
            ],
        )?;

        transaction.execute(
            "INSERT INTO provider_cursors (
                provider, account_subject, cursor, updated_at_unix_ms
             ) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(provider, account_subject) DO UPDATE SET
                cursor = excluded.cursor,
                updated_at_unix_ms = excluded.updated_at_unix_ms",
            params![
                provider.as_str(),
                account_subject,
                checkpoint.as_str(),
                observed_at_unix_ms
            ],
        )?;

        let superseded = transaction.execute(
            "UPDATE remote_events
             SET status = 'superseded'
             WHERE provider = ?1
               AND account_subject = ?2
               AND status = 'pending'",
            params![provider.as_str(), account_subject],
        )?;

        transaction.commit()?;

        Ok(CatalogCatchupCommit {
            changes_applied: changes.len(),
            authoritative_items,
            remote_events_superseded: u64::try_from(superseded)
                .map_err(|_| StorageError::NumericOverflow)?,
        })
    }

    pub fn remote_inventory_count(
        &self,
        provider: &ProviderId,
        account_subject: &str,
    ) -> Result<u64, StorageError> {
        count_inventory(&self.connection, "remote_items", provider, account_subject)
    }

    pub fn pending_remote_event_count(
        &self,
        provider: &ProviderId,
        account_subject: &str,
    ) -> Result<u64, StorageError> {
        let count: i64 = self.connection.query_row(
            "
            SELECT COUNT(*)
            FROM remote_events
            WHERE provider = ?1
              AND account_subject = ?2
              AND status = 'pending'
            ",
            params![provider.as_str(), account_subject],
            |row| row.get(0),
        )?;

        u64::try_from(count).map_err(|_| StorageError::NumericOverflow)
    }
}

fn validate_materialization_receipt_values(
    remote_id: &str,
    relative_path: &str,
    sha256_hex: &str,
) -> Result<(), StorageError> {
    if remote_id.trim().is_empty()
        || relative_path.is_empty()
        || relative_path.starts_with('/')
        || relative_path.ends_with('/')
        || relative_path.contains('\0')
        || relative_path
            .split('/')
            .any(|component| component.is_empty() || matches!(component, "." | ".."))
        || sha256_hex.len() != 64
        || !sha256_hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(StorageError::InvalidMaterializationReceipt);
    }
    Ok(())
}

fn validate_directory_materialization_receipt_values(
    remote_id: &str,
    relative_path: &str,
) -> Result<(), StorageError> {
    if remote_id.trim().is_empty()
        || relative_path.is_empty()
        || relative_path.starts_with('/')
        || relative_path.ends_with('/')
        || relative_path.contains('\0')
        || relative_path
            .split('/')
            .any(|component| component.is_empty() || matches!(component, "." | ".."))
    {
        return Err(StorageError::InvalidDirectoryMaterializationReceipt);
    }
    Ok(())
}

fn invalidate_sync_root_materialization_subtree(
    transaction: &Transaction<'_>,
    sync_root_id: &str,
    remote_id: &str,
) -> Result<(), StorageError> {
    transaction.execute(
        "WITH RECURSIVE subtree(remote_id) AS (
            SELECT ?2
            UNION
            SELECT child.remote_id
            FROM sync_root_remote_items AS child
            INNER JOIN subtree AS parent ON child.parent_remote_id = parent.remote_id
            WHERE child.sync_root_id = ?1
         )
         UPDATE sync_root_file_materialization_receipts
         SET receipt_state = 'stale'
         WHERE sync_root_id = ?1
           AND receipt_state = 'current'
           AND remote_id IN (SELECT remote_id FROM subtree)",
        params![sync_root_id, remote_id],
    )?;

    transaction.execute(
        "WITH RECURSIVE subtree(remote_id) AS (
            SELECT ?2
            UNION
            SELECT child.remote_id
            FROM sync_root_remote_items AS child
            INNER JOIN subtree AS parent ON child.parent_remote_id = parent.remote_id
            WHERE child.sync_root_id = ?1
         )
         UPDATE sync_root_directory_materialization_receipts
         SET receipt_state = 'stale'
         WHERE sync_root_id = ?1
           AND receipt_state = 'current'
           AND remote_id IN (SELECT remote_id FROM subtree)",
        params![sync_root_id, remote_id],
    )?;

    Ok(())
}

fn invalidate_sync_root_file_materialization(
    transaction: &Transaction<'_>,
    sync_root_id: &str,
    remote_id: &str,
) -> Result<(), StorageError> {
    transaction.execute(
        "UPDATE sync_root_file_materialization_receipts
         SET receipt_state = 'stale'
         WHERE sync_root_id = ?1
           AND remote_id = ?2
           AND receipt_state = 'current'",
        params![sync_root_id, remote_id],
    )?;

    Ok(())
}

fn upsert_sync_root_catalog_item(
    transaction: &Transaction<'_>,
    sync_root_id: &str,
    item: &RemoteItem,
    observed_at_unix_ms: i64,
) -> Result<(), StorageError> {
    let existing: Option<(Option<String>, String, String)> = transaction
        .query_row(
            "SELECT parent_remote_id, name, item_kind
             FROM sync_root_remote_items
             WHERE sync_root_id = ?1 AND remote_id = ?2",
            params![sync_root_id, item.remote_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;

    let item_kind = match item.kind {
        RemoteItemKind::File => "file",
        RemoteItemKind::Folder => "folder",
    };

    if let Some((existing_parent, existing_name, existing_kind)) = existing {
        if existing_kind == "folder" {
            let structural_change = item_kind != "folder"
                || existing_parent != item.parent_remote_id
                || existing_name != item.name;

            if structural_change {
                invalidate_sync_root_materialization_subtree(
                    transaction,
                    sync_root_id,
                    &item.remote_id,
                )?;
            }
        } else {
            // A provider file upsert can represent a content revision even when
            // path and size are unchanged. Only that file's content receipt is
            // invalidated; directory ownership is unaffected by file content.
            invalidate_sync_root_file_materialization(transaction, sync_root_id, &item.remote_id)?;
        }
    }

    let size_bytes = item
        .size_bytes
        .map(i64::try_from)
        .transpose()
        .map_err(|_| StorageError::NumericOverflow)?;

    transaction.execute(
        "INSERT INTO sync_root_remote_items (
            sync_root_id,
            remote_id,
            parent_remote_id,
            name,
            item_kind,
            size_bytes,
            trashed,
            observed_at_unix_ms
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, ?7)
         ON CONFLICT(sync_root_id, remote_id) DO UPDATE SET
            parent_remote_id = excluded.parent_remote_id,
            name = excluded.name,
            item_kind = excluded.item_kind,
            size_bytes = excluded.size_bytes,
            trashed = 0,
            observed_at_unix_ms = excluded.observed_at_unix_ms",
        params![
            sync_root_id,
            item.remote_id,
            item.parent_remote_id,
            item.name,
            item_kind,
            size_bytes,
            observed_at_unix_ms
        ],
    )?;

    Ok(())
}

fn delete_sync_root_subtree_in_transaction(
    transaction: &Transaction<'_>,
    sync_root_id: &str,
    remote_id: &str,
) -> Result<usize, StorageError> {
    invalidate_sync_root_materialization_subtree(transaction, sync_root_id, remote_id)?;

    let deleted = transaction.execute(
        "WITH RECURSIVE subtree(remote_id) AS (
            SELECT ?2
            UNION
            SELECT child.remote_id
            FROM sync_root_remote_items AS child
            INNER JOIN subtree AS parent
                ON child.parent_remote_id = parent.remote_id
            WHERE child.sync_root_id = ?1
         )
         DELETE FROM sync_root_remote_items
         WHERE sync_root_id = ?1
           AND remote_id IN (SELECT remote_id FROM subtree)",
        params![sync_root_id, remote_id],
    )?;

    Ok(deleted)
}

fn refresh_sync_root_catalog_count(
    transaction: &Transaction<'_>,
    sync_root_id: &str,
) -> Result<(), StorageError> {
    let item_count: i64 = transaction.query_row(
        "SELECT COUNT(*)
         FROM sync_root_remote_items
         WHERE sync_root_id = ?1",
        params![sync_root_id],
        |row| row.get(0),
    )?;

    transaction.execute(
        "UPDATE sync_root_remote_inventory_state
         SET item_count = ?2
         WHERE sync_root_id = ?1",
        params![sync_root_id, item_count],
    )?;

    Ok(())
}

fn validate_remote_write_identifier(value: &str) -> Result<(), StorageError> {
    if value.is_empty()
        || value != value.trim()
        || value.chars().any(char::is_whitespace)
        || value.len() > 1024
    {
        return Err(StorageError::InvalidRemoteWriteIdentifier);
    }
    Ok(())
}

fn validate_optional_checksum(
    algorithm: Option<&str>,
    checksum: Option<&str>,
) -> Result<(), StorageError> {
    match (algorithm, checksum) {
        (None, None) => Ok(()),
        (Some("md5"), Some(value))
            if value.len() == 32
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()) =>
        {
            Ok(())
        }
        _ => Err(StorageError::InvalidRemoteWriteChecksum),
    }
}

fn parse_optional_local_kind(value: Option<&str>) -> Result<Option<LocalItemKind>, StorageError> {
    value
        .map(|value| {
            LocalItemKind::parse(value).map_err(|_| StorageError::InvalidStoredLocalItemKind)
        })
        .transpose()
}

fn validate_intent_against_local_event(
    input: &RemoteWriteIntentInput,
    source_kind: &str,
    baseline_kind: Option<&str>,
    current_kind: Option<&str>,
) -> Result<(), StorageError> {
    let baseline_kind = parse_optional_local_kind(baseline_kind)?;
    let current_kind = parse_optional_local_kind(current_kind)?;

    let valid = match input.operation {
        RemoteWriteIntentOperation::CreateFile => {
            source_kind == "created"
                && baseline_kind.is_none()
                && current_kind == Some(LocalItemKind::File)
        }
        RemoteWriteIntentOperation::CreateFolder => {
            source_kind == "created"
                && baseline_kind.is_none()
                && current_kind == Some(LocalItemKind::Directory)
        }
        RemoteWriteIntentOperation::UpdateFile => {
            source_kind == "modified"
                && baseline_kind == Some(LocalItemKind::File)
                && current_kind == Some(LocalItemKind::File)
        }
        RemoteWriteIntentOperation::TrashItem => {
            source_kind == "deleted"
                && baseline_kind == Some(input.local_kind)
                && current_kind.is_none()
        }
    };

    if valid {
        Ok(())
    } else {
        Err(StorageError::RemoteWriteIntentSourceMismatch)
    }
}

fn row_to_remote_write_authority(
    remote_id: &str,
    row: (String, Option<String>, Option<String>, i64, i64, i64, i64),
) -> Result<RemoteWriteAuthoritySnapshot, StorageError> {
    let (remote_version, algorithm, checksum, can_edit, can_trash, can_add_children, observed) =
        row;
    let remote_version = remote_version
        .parse::<u64>()
        .map_err(|_| StorageError::InvalidStoredRemoteWriteAuthority)?;
    RemoteWriteAuthoritySnapshot::new(
        remote_id,
        remote_version,
        algorithm,
        checksum,
        can_edit != 0,
        can_trash != 0,
        can_add_children != 0,
        observed,
    )
}

fn query_remote_write_authority(
    connection: &Connection,
    sync_root_id: &str,
    remote_id: &str,
) -> Result<Option<RemoteWriteAuthoritySnapshot>, StorageError> {
    let row = connection
        .query_row(
            "SELECT remote_version, checksum_algorithm, content_checksum,
                    can_edit, can_trash, can_add_children, observed_at_unix_ms
             FROM sync_root_remote_write_authority
             WHERE sync_root_id=?1 AND remote_id=?2",
            params![sync_root_id, remote_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .optional()?;
    row.map(|row| row_to_remote_write_authority(remote_id, row))
        .transpose()
}

fn query_remote_write_authority_in_transaction(
    transaction: &Transaction<'_>,
    sync_root_id: &str,
    remote_id: &str,
) -> Result<Option<RemoteWriteAuthoritySnapshot>, StorageError> {
    let row = transaction
        .query_row(
            "SELECT remote_version, checksum_algorithm, content_checksum,
                    can_edit, can_trash, can_add_children, observed_at_unix_ms
             FROM sync_root_remote_write_authority
             WHERE sync_root_id=?1 AND remote_id=?2",
            params![sync_root_id, remote_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .optional()?;
    row.map(|row| row_to_remote_write_authority(remote_id, row))
        .transpose()
}

fn is_safe_local_event_relative_path(relative_path: &str) -> bool {
    if relative_path.is_empty()
        || relative_path.starts_with('/')
        || relative_path.ends_with('/')
        || relative_path.contains('\0')
    {
        return false;
    }

    relative_path
        .split('/')
        .all(|component| !component.is_empty() && !matches!(component, "." | ".."))
}

fn insert_sync_root_local_inventory_item(
    transaction: &Transaction<'_>,
    sync_root_id: &str,
    item: &LocalItemSnapshot,
    observed_at_unix_ms: i64,
) -> Result<(), StorageError> {
    let size_bytes = item
        .size_bytes()
        .map(i64::try_from)
        .transpose()
        .map_err(|_| StorageError::NumericOverflow)?;

    transaction.execute(
        "INSERT INTO sync_root_local_inventory_staging (
            sync_root_id,
            relative_path,
            item_kind,
            size_bytes,
            modified_unix_ns,
            device_id,
            inode,
            observed_at_unix_ms
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
         ON CONFLICT(sync_root_id, relative_path) DO UPDATE SET
            item_kind = excluded.item_kind,
            size_bytes = excluded.size_bytes,
            modified_unix_ns = excluded.modified_unix_ns,
            device_id = excluded.device_id,
            inode = excluded.inode,
            observed_at_unix_ms = excluded.observed_at_unix_ms",
        params![
            sync_root_id,
            item.relative_path(),
            item.kind().as_str(),
            size_bytes,
            item.modified_unix_ns(),
            item.device_id().to_string(),
            item.inode().to_string(),
            observed_at_unix_ms
        ],
    )?;

    Ok(())
}

fn insert_sync_root_inventory_item(
    transaction: &Transaction<'_>,
    sync_root_id: &str,
    item: &RemoteItem,
    observed_at_unix_ms: i64,
) -> Result<(), StorageError> {
    let item_kind = match item.kind {
        RemoteItemKind::File => "file",
        RemoteItemKind::Folder => "folder",
    };

    let size_bytes = item
        .size_bytes
        .map(i64::try_from)
        .transpose()
        .map_err(|_| StorageError::NumericOverflow)?;

    transaction.execute(
        "INSERT INTO sync_root_remote_inventory_staging (
            sync_root_id,
            remote_id,
            parent_remote_id,
            name,
            item_kind,
            size_bytes,
            trashed,
            observed_at_unix_ms
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
         ON CONFLICT(sync_root_id, remote_id) DO UPDATE SET
            parent_remote_id = excluded.parent_remote_id,
            name = excluded.name,
            item_kind = excluded.item_kind,
            size_bytes = excluded.size_bytes,
            trashed = excluded.trashed,
            observed_at_unix_ms = excluded.observed_at_unix_ms",
        params![
            sync_root_id,
            item.remote_id,
            item.parent_remote_id,
            item.name,
            item_kind,
            size_bytes,
            item.trashed as i64,
            observed_at_unix_ms
        ],
    )?;

    Ok(())
}

fn apply_remote_change_to_inventory(
    transaction: &Transaction<'_>,
    provider: &ProviderId,
    account_subject: &str,
    change: &RemoteChange,
    observed_at_unix_ms: i64,
) -> Result<(), StorageError> {
    match change {
        RemoteChange::Delete { remote_id } => {
            transaction.execute(
                "DELETE FROM remote_items
                 WHERE provider = ?1
                   AND account_subject = ?2
                   AND remote_id = ?3",
                params![provider.as_str(), account_subject, remote_id],
            )?;
        }
        RemoteChange::Upsert(item) if item.trashed => {
            transaction.execute(
                "DELETE FROM remote_items
                 WHERE provider = ?1
                   AND account_subject = ?2
                   AND remote_id = ?3",
                params![provider.as_str(), account_subject, item.remote_id],
            )?;
        }
        RemoteChange::Upsert(item) => {
            let item_kind = match item.kind {
                RemoteItemKind::File => "file",
                RemoteItemKind::Folder => "folder",
            };

            let size_bytes = item
                .size_bytes
                .map(i64::try_from)
                .transpose()
                .map_err(|_| StorageError::NumericOverflow)?;

            transaction.execute(
                "INSERT INTO remote_items (
                    provider,
                    account_subject,
                    remote_id,
                    parent_remote_id,
                    name,
                    item_kind,
                    size_bytes,
                    trashed,
                    observed_at_unix_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0, ?8)
                 ON CONFLICT(provider, account_subject, remote_id) DO UPDATE SET
                    parent_remote_id = excluded.parent_remote_id,
                    name = excluded.name,
                    item_kind = excluded.item_kind,
                    size_bytes = excluded.size_bytes,
                    trashed = 0,
                    observed_at_unix_ms = excluded.observed_at_unix_ms",
                params![
                    provider.as_str(),
                    account_subject,
                    item.remote_id,
                    item.parent_remote_id,
                    item.name,
                    item_kind,
                    size_bytes,
                    observed_at_unix_ms
                ],
            )?;
        }
    }

    Ok(())
}

fn insert_inventory_item(
    transaction: &Transaction<'_>,
    provider: &ProviderId,
    account_subject: &str,
    item: &RemoteItem,
    observed_at_unix_ms: i64,
) -> Result<(), StorageError> {
    let item_kind = match item.kind {
        RemoteItemKind::File => "file",
        RemoteItemKind::Folder => "folder",
    };
    let size_bytes = item
        .size_bytes
        .map(i64::try_from)
        .transpose()
        .map_err(|_| StorageError::NumericOverflow)?;
    transaction.execute(
        "INSERT INTO remote_inventory_staging (provider, account_subject, remote_id, parent_remote_id, name, item_kind, size_bytes, trashed, observed_at_unix_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
         ON CONFLICT(provider, account_subject, remote_id) DO UPDATE SET
           parent_remote_id = excluded.parent_remote_id,
           name = excluded.name,
           item_kind = excluded.item_kind,
           size_bytes = excluded.size_bytes,
           trashed = excluded.trashed,
           observed_at_unix_ms = excluded.observed_at_unix_ms",
        params![provider.as_str(), account_subject, item.remote_id, item.parent_remote_id, item.name, item_kind, size_bytes, i64::from(item.trashed), observed_at_unix_ms],
    )?;
    Ok(())
}

fn count_inventory(
    connection: &Connection,
    table: &'static str,
    provider: &ProviderId,
    account_subject: &str,
) -> Result<u64, StorageError> {
    let sql = match table {
        "remote_items" => {
            "SELECT COUNT(*) FROM remote_items WHERE provider = ?1 AND account_subject = ?2"
        }
        "remote_inventory_staging" => {
            "SELECT COUNT(*) FROM remote_inventory_staging WHERE provider = ?1 AND account_subject = ?2"
        }
        _ => return Err(StorageError::InvalidInternalTable),
    };
    let count: i64 =
        connection.query_row(sql, params![provider.as_str(), account_subject], |row| {
            row.get(0)
        })?;
    u64::try_from(count).map_err(|_| StorageError::NumericOverflow)
}

fn insert_remote_event(
    transaction: &Transaction<'_>,
    provider: &ProviderId,
    account_subject: &str,
    change: &RemoteChange,
    observed_at_unix_ms: i64,
) -> Result<(), StorageError> {
    match change {
        RemoteChange::Delete { remote_id } => {
            transaction.execute(
                "
                INSERT INTO remote_events (
                    provider,
                    account_subject,
                    event_kind,
                    remote_id,
                    parent_remote_id,
                    name,
                    item_kind,
                    size_bytes,
                    trashed,
                    observed_at_unix_ms,
                    status
                ) VALUES (?1, ?2, 'delete', ?3, NULL, NULL, NULL, NULL, 0, ?4, 'pending')
                ",
                params![
                    provider.as_str(),
                    account_subject,
                    remote_id,
                    observed_at_unix_ms
                ],
            )?;
        }
        RemoteChange::Upsert(item) => {
            insert_remote_upsert(
                transaction,
                provider,
                account_subject,
                item,
                observed_at_unix_ms,
            )?;
        }
    }

    Ok(())
}

fn insert_remote_upsert(
    transaction: &Transaction<'_>,
    provider: &ProviderId,
    account_subject: &str,
    item: &RemoteItem,
    observed_at_unix_ms: i64,
) -> Result<(), StorageError> {
    let item_kind = match item.kind {
        RemoteItemKind::File => "file",
        RemoteItemKind::Folder => "folder",
    };

    let size_bytes = item
        .size_bytes
        .map(i64::try_from)
        .transpose()
        .map_err(|_| StorageError::NumericOverflow)?;

    transaction.execute(
        "
        INSERT INTO remote_events (
            provider,
            account_subject,
            event_kind,
            remote_id,
            parent_remote_id,
            name,
            item_kind,
            size_bytes,
            trashed,
            observed_at_unix_ms,
            status
        ) VALUES (?1, ?2, 'upsert', ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'pending')
        ",
        params![
            provider.as_str(),
            account_subject,
            item.remote_id,
            item.parent_remote_id,
            item.name,
            item_kind,
            size_bytes,
            i64::from(item.trashed),
            observed_at_unix_ms
        ],
    )?;

    Ok(())
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("materialization receipt contains invalid values")]
    InvalidMaterializationReceipt,
    #[error("directory materialization receipt contains invalid values")]
    InvalidDirectoryMaterializationReceipt,
    #[error("directory materialization receipt remote item is missing")]
    DirectoryMaterializationReceiptRemoteItemMissing,
    #[error("directory materialization receipt does not match durable remote folder metadata")]
    DirectoryMaterializationReceiptRemoteItemMismatch,
    #[error("materialization receipt remote item is missing")]
    MaterializationReceiptRemoteItemMissing,
    #[error("materialization receipt does not match durable remote file metadata")]
    MaterializationReceiptRemoteItemMismatch,
    #[error("stale materialization receipt batch did not match durable state")]
    StaleMaterializationReceiptBatchMismatch,
    #[error("stale directory materialization receipt batch did not match durable state")]
    StaleDirectoryMaterializationReceiptBatchMismatch,
    #[error("SQLite operation failed")]
    Sqlite(#[from] rusqlite::Error),
    #[error("stored NubiSync domain value is invalid")]
    Core(#[from] nubisync_core::CoreError),
    #[error("numeric value does not fit SQLite storage")]
    NumericOverflow,
    #[error("internal inventory table selection is invalid")]
    InvalidInternalTable,
    #[error("stored root-catalog item kind is invalid")]
    InvalidStoredRemoteItemKind,
    #[error("stored local inventory item kind is invalid")]
    InvalidStoredLocalItemKind,
    #[error("stored local inventory identity is invalid")]
    InvalidStoredLocalIdentity,
    #[error("local change relative path is invalid")]
    InvalidLocalChangePath,
    #[error("local change event shape is invalid")]
    InvalidLocalChangeShape,
    #[error("local change journal baseline does not match durable state")]
    LocalChangeBaselineMismatch,
    #[error("local change journal pending count did not match the supplied diff")]
    LocalChangeJournalCountMismatch,
    #[error("stored local change event kind is invalid")]
    InvalidStoredLocalChangeEventKind,
    #[error("stored local change event is invalid")]
    InvalidStoredLocalChangeEvent,
    #[error("remote-write identifier is invalid")]
    InvalidRemoteWriteIdentifier,
    #[error("remote-write checksum is invalid")]
    InvalidRemoteWriteChecksum,
    #[error("remote-write authority snapshot is invalid")]
    InvalidRemoteWriteAuthority,
    #[error("stored remote-write authority snapshot is invalid")]
    InvalidStoredRemoteWriteAuthority,
    #[error("remote-write intent is invalid")]
    InvalidRemoteWriteIntent,
    #[error("stored remote-write intent operation is invalid")]
    InvalidStoredRemoteWriteIntentOperation,
    #[error("stored remote-write intent status is invalid")]
    InvalidStoredRemoteWriteIntentStatus,
    #[error("remote-write intent source does not match durable local authority")]
    RemoteWriteIntentSourceMismatch,
    #[error("remote-write intent root is not write-capable")]
    RemoteWriteIntentRootNotWriteCapable,
    #[error("remote-write authority does not satisfy intent preconditions")]
    RemoteWriteIntentAuthorityMismatch,
    #[error("remote-write authority requires a fully caught-up catalog with no open window")]
    RemoteWriteAuthorityCatalogNotReady,
    #[error("remote-write authority cursor does not match the durable catalog")]
    RemoteWriteAuthorityCursorMismatch,
    #[error("remote-write authority item count does not match the durable catalog")]
    RemoteWriteAuthorityCountMismatch,
    #[error("SQLite schema version {found} is newer than supported version {supported}")]
    UnsupportedSchemaVersion { found: i64, supported: i64 },
    #[error("remote catalog does not have a complete authoritative snapshot")]
    RemoteCatalogSnapshotMissing,
    #[error("remote catalog catch-up is already complete")]
    RemoteCatalogCatchupAlreadyComplete,
    #[error("remote catalog catch-up cursor is missing")]
    RemoteCatalogCatchupCursorMissing,
    #[error("remote catalog catch-up cursor does not match the stored bootstrap fence")]
    RemoteCatalogCatchupCursorMismatch,
    #[error("sync root catalog mutation is invalid")]
    InvalidSyncRootCatalogMutation,
    #[error("sync root change page boundary is invalid")]
    InvalidSyncRootChangePageBoundary,
    #[error("sync root staged change event is invalid")]
    InvalidSyncRootChangeEvent,
    #[error("stored sync root change event is invalid")]
    InvalidStoredRemoteChange,
    #[error("sync root change window base cursor does not match durable state")]
    SyncRootChangeWindowBaseCursorMismatch,
    #[error("sync root change window continuation does not match durable state")]
    SyncRootChangeWindowContinuationMismatch,
    #[error("sync root change window is already complete")]
    SyncRootChangeWindowAlreadyComplete,
    #[error("sync root change window is missing")]
    SyncRootChangeWindowMissing,
    #[error("sync root change window is incomplete")]
    SyncRootChangeWindowIncomplete,
    #[error("sync root change window checkpoint does not match")]
    SyncRootChangeWindowCheckpointMismatch,
    #[error("sync root change window change count does not match")]
    SyncRootChangeWindowChangeCountMismatch,
    #[error("sync root change window pagination token repeated")]
    SyncRootChangeWindowPaginationLoop,
    #[error("sync root change window exceeded a safety limit")]
    SyncRootChangeWindowSafetyLimitExceeded,
    #[error("sync root catalog does not have a complete authoritative snapshot")]
    SyncRootCatalogSnapshotMissing,
    #[error("sync root catalog bootstrap catch-up cursor is missing")]
    SyncRootCatalogCatchupCursorMissing,
    #[error("sync root catalog incremental change cursor is missing")]
    SyncRootCatalogChangeCursorMissing,
    #[error("sync root catalog expected cursor does not match durable state")]
    SyncRootCatalogExpectedCursorMismatch,
    #[error("sync root catalog cursor state is internally inconsistent")]
    SyncRootCatalogInvalidCursorState,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_account(provider: &ProviderId) -> ProviderAccount {
        ProviderAccount::new(
            provider.clone(),
            "google-subject-123",
            Some("user@example.test".into()),
            Some("Test User".into()),
        )
        .unwrap()
    }

    fn test_remote_item(
        remote_id: &str,
        parent_remote_id: Option<&str>,
        name: &str,
        kind: RemoteItemKind,
    ) -> RemoteItem {
        RemoteItem {
            remote_id: remote_id.into(),
            parent_remote_id: parent_remote_id.map(str::to_owned),
            name: name.into(),
            kind,
            size_bytes: Some(10),
            modified_unix_ms: None,
            trashed: false,
        }
    }

    fn prepare_root_snapshot(
        storage: &mut Storage,
        root: &SyncRoot,
        items: &[RemoteItem],
        fence: &str,
        observed_at_unix_ms: i64,
    ) {
        storage
            .begin_sync_root_remote_inventory_staging(&root.id)
            .unwrap();
        storage
            .stage_sync_root_remote_inventory_items(&root.id, items, observed_at_unix_ms)
            .unwrap();
        storage
            .commit_sync_root_remote_inventory_snapshot(
                &root.id,
                &ChangeCursor::new(fence).unwrap(),
                observed_at_unix_ms + 1,
            )
            .unwrap();
    }

    fn test_upsert(remote_id: &str, size_bytes: Option<u64>) -> RemoteChange {
        RemoteChange::Upsert(RemoteItem {
            remote_id: remote_id.into(),
            parent_remote_id: Some("root".into()),
            name: "example.txt".into(),
            kind: RemoteItemKind::File,
            size_bytes,
            modified_unix_ms: None,
            trashed: false,
        })
    }

    #[test]
    fn migration_is_applied_transactionally() {
        let storage = Storage::open_in_memory().unwrap();
        assert_eq!(storage.schema_version().unwrap(), SCHEMA_VERSION);
    }

    #[test]
    fn schema_v6_migrates_root_change_cursor_column() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "
                CREATE TABLE sync_roots (
                    id TEXT PRIMARY KEY
                );

                CREATE TABLE sync_root_remote_inventory_state (
                    sync_root_id TEXT PRIMARY KEY,
                    snapshot_complete INTEGER NOT NULL DEFAULT 0,
                    catchup_complete INTEGER NOT NULL DEFAULT 0,
                    item_count INTEGER NOT NULL DEFAULT 0,
                    snapshot_completed_at_unix_ms INTEGER,
                    catchup_from_cursor TEXT,
                    FOREIGN KEY (sync_root_id)
                        REFERENCES sync_roots(id) ON DELETE CASCADE
                );

                PRAGMA user_version = 6;
                ",
            )
            .unwrap();

        let mut storage = Storage { connection };
        storage.configure().unwrap();
        storage.migrate().unwrap();

        assert_eq!(storage.schema_version().unwrap(), SCHEMA_VERSION);

        let has_change_cursor: bool = storage
            .connection
            .prepare("PRAGMA table_info(sync_root_remote_inventory_state)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .map(Result::unwrap)
            .any(|name| name == "change_cursor");

        assert!(has_change_cursor);
    }

    #[test]
    fn phase5d6_stale_receipt_batch_delete_is_atomic_and_exact() {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = test_account(&provider);
        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "phase5d6-root",
            provider,
            account.subject,
            "/tmp/phase5d6-root",
            Some("root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        storage.insert_sync_root(&root).unwrap();

        let mut first = test_remote_item("phase5d6-a", Some("root"), "a.txt", RemoteItemKind::File);
        first.size_bytes = Some(5);
        let mut second =
            test_remote_item("phase5d6-b", Some("root"), "b.txt", RemoteItemKind::File);
        second.size_bytes = Some(5);

        prepare_root_snapshot(
            &mut storage,
            &root,
            &[first.clone(), second.clone()],
            "fence",
            3,
        );

        storage
            .record_sync_root_file_materializations(
                &root.id,
                &[
                    (
                        first.remote_id.clone(),
                        first.name.clone(),
                        5,
                        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                    ),
                    (
                        second.remote_id.clone(),
                        second.name.clone(),
                        5,
                        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
                    ),
                ],
                5,
            )
            .unwrap();

        storage
            .delete_sync_root_remote_subtree(&root.id, &first.remote_id)
            .unwrap();
        storage
            .delete_sync_root_remote_subtree(&root.id, &second.remote_id)
            .unwrap();

        assert_eq!(
            storage
                .sync_root_stale_materialization_receipt_count(&root.id)
                .unwrap(),
            2
        );

        let deleted = storage
            .delete_sync_root_stale_file_materialization_receipts(
                &root.id,
                &[first.remote_id.clone(), second.remote_id.clone()],
            )
            .unwrap();
        assert_eq!(deleted, 2);
        assert_eq!(
            storage
                .sync_root_stale_materialization_receipt_count(&root.id)
                .unwrap(),
            0
        );
    }

    #[test]
    fn phase5d6_stale_receipt_batch_delete_rolls_back_on_mismatch() {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = test_account(&provider);
        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "phase5d6-rollback-root",
            provider,
            account.subject,
            "/tmp/phase5d6-rollback-root",
            Some("root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        storage.insert_sync_root(&root).unwrap();

        let mut first = test_remote_item(
            "phase5d6-only",
            Some("root"),
            "only.txt",
            RemoteItemKind::File,
        );
        first.size_bytes = Some(5);

        prepare_root_snapshot(&mut storage, &root, &[first.clone()], "fence", 3);

        storage
            .record_sync_root_file_materialization(
                &root.id,
                &first.remote_id,
                &first.name,
                5,
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                5,
            )
            .unwrap();
        storage
            .delete_sync_root_remote_subtree(&root.id, &first.remote_id)
            .unwrap();

        let error = storage
            .delete_sync_root_stale_file_materialization_receipts(
                &root.id,
                &[first.remote_id.clone(), "missing-stale".into()],
            )
            .unwrap_err();
        assert!(matches!(
            error,
            StorageError::StaleMaterializationReceiptBatchMismatch
        ));

        assert_eq!(
            storage
                .sync_root_stale_materialization_receipt_count(&root.id)
                .unwrap(),
            1
        );
    }

    #[test]
    fn accounts_can_be_loaded_for_persistent_session_discovery() {
        let storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = test_account(&provider);

        storage.upsert_account(&account, 1_700_000_000_000).unwrap();

        let accounts = storage.list_accounts(&provider).unwrap();
        assert_eq!(accounts.len(), 1);
        assert_eq!(accounts[0].subject, "google-subject-123");
        assert_eq!(accounts[0].email.as_deref(), Some("user@example.test"));
    }

    #[test]
    fn sync_root_round_trip_uses_typed_mode() {
        let storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = test_account(&provider);
        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "sync-root-1",
            provider.clone(),
            account.subject.clone(),
            "/tmp/nubisync-test",
            Some("drive-folder-1".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();

        storage.insert_sync_root(&root).unwrap();

        let roots = storage
            .list_sync_roots(&provider, &account.subject)
            .unwrap();

        assert_eq!(roots, vec![root]);
        assert_eq!(
            storage
                .sync_root_count(&provider, &account.subject)
                .unwrap(),
            1
        );
    }

    #[test]
    fn sync_root_registration_is_insert_only() {
        let storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = test_account(&provider);
        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "sync-root-1",
            provider.clone(),
            account.subject.clone(),
            "/tmp/nubisync-one",
            Some("remote-one".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();

        storage.insert_sync_root(&root).unwrap();

        let duplicate_id = SyncRoot::new(
            "sync-root-1",
            provider.clone(),
            account.subject.clone(),
            "/tmp/nubisync-two",
            Some("remote-two".into()),
            SyncMode::ReceiveOnly,
            3,
        )
        .unwrap();

        assert!(storage.insert_sync_root(&duplicate_id).is_err());

        let roots = storage
            .list_sync_roots(&provider, &account.subject)
            .unwrap();
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].local_path, "/tmp/nubisync-one");
        assert_eq!(roots[0].remote_root_id.as_deref(), Some("remote-one"));
    }

    #[test]
    fn root_scoped_catalogs_isolate_identical_remote_ids() {
        let mut storage = Storage::open_in_memory().unwrap();
        assert_eq!(storage.schema_version().unwrap(), SCHEMA_VERSION);

        let provider = ProviderId::new("google-drive").unwrap();
        let account = test_account(&provider);
        storage.upsert_account(&account, 1).unwrap();

        let root_one = SyncRoot::new(
            "root-one",
            provider.clone(),
            account.subject.clone(),
            "/tmp/root-one",
            Some("remote-root-one".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        let root_two = SyncRoot::new(
            "root-two",
            provider.clone(),
            account.subject.clone(),
            "/tmp/root-two",
            Some("remote-root-two".into()),
            SyncMode::ReceiveOnly,
            3,
        )
        .unwrap();

        storage.insert_sync_root(&root_one).unwrap();
        storage.insert_sync_root(&root_two).unwrap();

        let shared = RemoteItem {
            remote_id: "same-remote-id".into(),
            parent_remote_id: Some("parent".into()),
            name: "same.txt".into(),
            kind: RemoteItemKind::File,
            size_bytes: Some(10),
            modified_unix_ms: None,
            trashed: false,
        };

        storage
            .begin_sync_root_remote_inventory_staging(&root_one.id)
            .unwrap();
        storage
            .begin_sync_root_remote_inventory_staging(&root_two.id)
            .unwrap();
        storage
            .stage_sync_root_remote_inventory_items(&root_one.id, std::slice::from_ref(&shared), 4)
            .unwrap();
        storage
            .stage_sync_root_remote_inventory_items(&root_two.id, &[shared], 5)
            .unwrap();

        assert_eq!(
            storage
                .staged_sync_root_remote_inventory_count(&root_one.id)
                .unwrap(),
            1
        );
        assert_eq!(
            storage
                .staged_sync_root_remote_inventory_count(&root_two.id)
                .unwrap(),
            1
        );

        let fence = ChangeCursor::new("root-one-fence").unwrap();
        storage
            .commit_sync_root_remote_inventory_snapshot(&root_one.id, &fence, 6)
            .unwrap();

        assert_eq!(
            storage
                .sync_root_remote_inventory_count(&root_one.id)
                .unwrap(),
            1
        );
        assert_eq!(
            storage
                .sync_root_remote_inventory_count(&root_two.id)
                .unwrap(),
            0
        );
        assert_eq!(
            storage
                .staged_sync_root_remote_inventory_count(&root_two.id)
                .unwrap(),
            1
        );

        let state = storage
            .sync_root_remote_inventory_state(&root_one.id)
            .unwrap();
        assert!(state.snapshot_complete);
        assert!(!state.catchup_complete);
        assert_eq!(state.item_count, 1);
        assert_eq!(state.catchup_from_cursor.unwrap().as_str(), fence.as_str());

        assert_eq!(
            storage
                .sync_root_catalog_state_count(&provider, &account.subject)
                .unwrap(),
            1
        );
        assert_eq!(
            storage
                .sync_root_catalog_item_count(&provider, &account.subject)
                .unwrap(),
            1
        );
    }

    #[test]
    fn root_catalog_can_be_loaded_for_batch_projection() {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = test_account(&provider);
        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "projection-load-root",
            provider,
            account.subject,
            "/tmp/projection-load-root",
            Some("remote-root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        storage.insert_sync_root(&root).unwrap();

        let folder = test_remote_item(
            "folder",
            Some("remote-root"),
            "folder",
            RemoteItemKind::Folder,
        );
        let file = test_remote_item("file", Some("folder"), "file.txt", RemoteItemKind::File);

        prepare_root_snapshot(
            &mut storage,
            &root,
            &[folder.clone(), file.clone()],
            "projection-fence",
            10,
        );

        assert_eq!(
            storage.list_sync_root_remote_items(&root.id).unwrap(),
            vec![file, folder]
        );
    }

    #[test]
    fn root_snapshot_clears_previous_incremental_cursor() {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = test_account(&provider);
        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "root-cursor",
            provider.clone(),
            account.subject.clone(),
            "/tmp/root-cursor",
            Some("remote-root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        storage.insert_sync_root(&root).unwrap();

        storage
            .begin_sync_root_remote_inventory_staging(&root.id)
            .unwrap();
        storage
            .stage_sync_root_remote_inventory_items(
                &root.id,
                &[test_remote_item(
                    "file-one",
                    Some("remote-root"),
                    "one.txt",
                    RemoteItemKind::File,
                )],
                3,
            )
            .unwrap();
        storage
            .commit_sync_root_remote_inventory_snapshot(
                &root.id,
                &ChangeCursor::new("bootstrap-fence-one").unwrap(),
                4,
            )
            .unwrap();

        storage
            .connection
            .execute(
                "UPDATE sync_root_remote_inventory_state
                 SET change_cursor = ?2
                 WHERE sync_root_id = ?1",
                params![root.id, "incremental-checkpoint"],
            )
            .unwrap();

        let cursor = storage.sync_root_change_cursor(&root.id).unwrap().unwrap();
        assert_eq!(cursor.as_str(), "incremental-checkpoint");
        assert_eq!(format!("{cursor:?}"), "ChangeCursor([redacted])");
        assert_eq!(
            storage
                .sync_root_change_cursor_count(&provider, &account.subject)
                .unwrap(),
            1
        );

        storage
            .begin_sync_root_remote_inventory_staging(&root.id)
            .unwrap();
        storage
            .stage_sync_root_remote_inventory_items(
                &root.id,
                &[test_remote_item(
                    "file-two",
                    Some("remote-root"),
                    "two.txt",
                    RemoteItemKind::File,
                )],
                5,
            )
            .unwrap();
        storage
            .commit_sync_root_remote_inventory_snapshot(
                &root.id,
                &ChangeCursor::new("bootstrap-fence-two").unwrap(),
                6,
            )
            .unwrap();

        assert!(storage.sync_root_change_cursor(&root.id).unwrap().is_none());
        assert_eq!(
            storage
                .sync_root_change_cursor_count(&provider, &account.subject)
                .unwrap(),
            0
        );
    }

    #[test]
    fn root_catalog_mutations_are_recursive_isolated_and_counted() {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = test_account(&provider);
        storage.upsert_account(&account, 1).unwrap();

        let root_one = SyncRoot::new(
            "root-one",
            provider.clone(),
            account.subject.clone(),
            "/tmp/root-one",
            Some("remote-root-one".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        let root_two = SyncRoot::new(
            "root-two",
            provider.clone(),
            account.subject.clone(),
            "/tmp/root-two",
            Some("remote-root-two".into()),
            SyncMode::ReceiveOnly,
            3,
        )
        .unwrap();

        storage.insert_sync_root(&root_one).unwrap();
        storage.insert_sync_root(&root_two).unwrap();

        let root_one_items = vec![
            test_remote_item(
                "folder-a",
                Some("remote-root-one"),
                "folder-a",
                RemoteItemKind::Folder,
            ),
            test_remote_item(
                "file-under-a",
                Some("folder-a"),
                "nested.txt",
                RemoteItemKind::File,
            ),
            test_remote_item(
                "sibling",
                Some("remote-root-one"),
                "sibling.txt",
                RemoteItemKind::File,
            ),
        ];
        let root_two_items = vec![
            test_remote_item(
                "folder-a",
                Some("remote-root-two"),
                "folder-a",
                RemoteItemKind::Folder,
            ),
            test_remote_item(
                "file-under-a",
                Some("folder-a"),
                "nested.txt",
                RemoteItemKind::File,
            ),
        ];

        storage
            .begin_sync_root_remote_inventory_staging(&root_one.id)
            .unwrap();
        storage
            .begin_sync_root_remote_inventory_staging(&root_two.id)
            .unwrap();
        storage
            .stage_sync_root_remote_inventory_items(&root_one.id, &root_one_items, 4)
            .unwrap();
        storage
            .stage_sync_root_remote_inventory_items(&root_two.id, &root_two_items, 5)
            .unwrap();

        storage
            .commit_sync_root_remote_inventory_snapshot(
                &root_one.id,
                &ChangeCursor::new("fence-one").unwrap(),
                6,
            )
            .unwrap();
        storage
            .commit_sync_root_remote_inventory_snapshot(
                &root_two.id,
                &ChangeCursor::new("fence-two").unwrap(),
                7,
            )
            .unwrap();

        let deleted = storage
            .delete_sync_root_remote_subtree(&root_one.id, "folder-a")
            .unwrap();
        assert_eq!(deleted, 2);

        assert!(
            storage
                .sync_root_remote_item(&root_one.id, "folder-a")
                .unwrap()
                .is_none()
        );
        assert!(
            storage
                .sync_root_remote_item(&root_one.id, "file-under-a")
                .unwrap()
                .is_none()
        );
        assert!(
            storage
                .sync_root_remote_item(&root_one.id, "sibling")
                .unwrap()
                .is_some()
        );

        // The same remote IDs in another root are untouched.
        assert!(
            storage
                .sync_root_remote_item(&root_two.id, "folder-a")
                .unwrap()
                .is_some()
        );
        assert!(
            storage
                .sync_root_remote_item(&root_two.id, "file-under-a")
                .unwrap()
                .is_some()
        );

        let state = storage
            .sync_root_remote_inventory_state(&root_one.id)
            .unwrap();
        assert_eq!(state.item_count, 1);

        let new_item = test_remote_item(
            "new-file",
            Some("remote-root-one"),
            "new.txt",
            RemoteItemKind::File,
        );
        storage
            .upsert_sync_root_remote_item(&root_one.id, &new_item, 8)
            .unwrap();

        assert_eq!(
            storage
                .sync_root_remote_item(&root_one.id, "new-file")
                .unwrap(),
            Some(new_item)
        );
        assert_eq!(
            storage
                .sync_root_remote_inventory_state(&root_one.id)
                .unwrap()
                .item_count,
            2
        );

        // The selected remote root container is intentionally virtual/not stored.
        // Deleting from that ID still clears every descendant in this root.
        let deleted = storage
            .delete_sync_root_remote_subtree(&root_one.id, "remote-root-one")
            .unwrap();
        assert_eq!(deleted, 2);
        assert_eq!(
            storage
                .sync_root_remote_inventory_count(&root_one.id)
                .unwrap(),
            0
        );
        assert_eq!(
            storage
                .sync_root_remote_inventory_state(&root_one.id)
                .unwrap()
                .item_count,
            0
        );

        assert_eq!(
            storage
                .sync_root_remote_inventory_count(&root_two.id)
                .unwrap(),
            2
        );
    }

    #[test]
    fn completed_change_window_commits_and_clears_atomically() {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = test_account(&provider);
        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "window-commit",
            provider,
            account.subject,
            "/tmp/window-commit",
            Some("remote-root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        storage.insert_sync_root(&root).unwrap();
        prepare_root_snapshot(&mut storage, &root, &[], "fence", 10);

        let item = test_remote_item(
            "file",
            Some("remote-root"),
            "file.txt",
            RemoteItemKind::File,
        );
        let page = nubisync_core::ChangePage {
            changes: vec![RemoteChange::Upsert(item.clone())],
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

        let commit = storage
            .commit_sync_root_catalog_change_window(
                &root.id,
                &ChangeCursor::new("fence").unwrap(),
                &ChangeCursor::new("checkpoint").unwrap(),
                1,
                &[SyncRootCatalogMutation::Upsert(item.clone())],
                20,
            )
            .unwrap();

        assert!(commit.completed_initial_catchup);
        assert_eq!(commit.mutations_applied, 1);
        assert_eq!(commit.authoritative_items, 1);
        assert_eq!(
            storage.list_sync_root_remote_items(&root.id).unwrap(),
            vec![item]
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
    fn failed_change_window_commit_preserves_window_and_catalog() {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = test_account(&provider);
        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "window-commit-rollback",
            provider,
            account.subject,
            "/tmp/window-commit-rollback",
            Some("remote-root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        storage.insert_sync_root(&root).unwrap();
        prepare_root_snapshot(&mut storage, &root, &[], "fence", 10);

        let page = nubisync_core::ChangePage {
            changes: vec![],
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

        let invalid = RemoteItem {
            remote_id: "bad".into(),
            parent_remote_id: Some("remote-root".into()),
            name: "bad.txt".into(),
            kind: RemoteItemKind::File,
            size_bytes: Some(1),
            modified_unix_ms: None,
            trashed: true,
        };

        let error = storage
            .commit_sync_root_catalog_change_window(
                &root.id,
                &ChangeCursor::new("fence").unwrap(),
                &ChangeCursor::new("checkpoint").unwrap(),
                0,
                &[SyncRootCatalogMutation::Upsert(invalid)],
                20,
            )
            .unwrap_err();

        assert!(matches!(
            error,
            StorageError::InvalidSyncRootCatalogMutation
        ));
        assert!(
            storage
                .list_sync_root_remote_items(&root.id)
                .unwrap()
                .is_empty()
        );
        assert!(storage.sync_root_change_cursor(&root.id).unwrap().is_none());
        assert!(
            !storage
                .sync_root_remote_inventory_state(&root.id)
                .unwrap()
                .catchup_complete
        );
        assert!(
            storage
                .sync_root_change_window_state(&root.id)
                .unwrap()
                .unwrap()
                .is_complete()
        );
    }

    #[test]
    fn incomplete_change_window_cannot_be_committed() {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = test_account(&provider);
        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "window-incomplete",
            provider,
            account.subject,
            "/tmp/window-incomplete",
            Some("remote-root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        storage.insert_sync_root(&root).unwrap();
        prepare_root_snapshot(&mut storage, &root, &[], "fence", 10);

        let page = nubisync_core::ChangePage {
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

        let error = storage
            .commit_sync_root_catalog_change_window(
                &root.id,
                &ChangeCursor::new("fence").unwrap(),
                &ChangeCursor::new("never").unwrap(),
                0,
                &[],
                20,
            )
            .unwrap_err();

        assert!(matches!(
            error,
            StorageError::SyncRootChangeWindowIncomplete
        ));
        assert!(
            storage
                .sync_root_change_window_state(&root.id)
                .unwrap()
                .is_some()
        );
        assert!(storage.sync_root_change_cursor(&root.id).unwrap().is_none());
    }

    #[test]
    fn root_change_window_stages_multiple_pages_durably() {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = test_account(&provider);
        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "window-root",
            provider,
            account.subject,
            "/tmp/window-root",
            Some("remote-root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        storage.insert_sync_root(&root).unwrap();
        prepare_root_snapshot(&mut storage, &root, &[], "fence", 10);

        let first_item = test_remote_item(
            "first",
            Some("remote-root"),
            "first.txt",
            RemoteItemKind::File,
        );
        let first_page = nubisync_core::ChangePage {
            changes: vec![RemoteChange::Upsert(first_item.clone())],
            continuation: Some(ContinuationToken::new("page-two").unwrap()),
            checkpoint: None,
        };

        let first_state = storage
            .stage_sync_root_change_window_page(
                &root.id,
                &ChangeCursor::new("fence").unwrap(),
                None,
                &first_page,
            )
            .unwrap();

        assert!(!first_state.is_complete());
        assert_eq!(first_state.page_count, 1);
        assert_eq!(first_state.change_count, 1);

        let second_page = nubisync_core::ChangePage {
            changes: vec![RemoteChange::Delete {
                remote_id: first_item.remote_id.clone(),
            }],
            continuation: None,
            checkpoint: Some(ChangeCursor::new("checkpoint").unwrap()),
        };

        let second_state = storage
            .stage_sync_root_change_window_page(
                &root.id,
                &ChangeCursor::new("fence").unwrap(),
                first_state.continuation.as_ref(),
                &second_page,
            )
            .unwrap();

        assert!(second_state.is_complete());
        assert_eq!(second_state.page_count, 2);
        assert_eq!(second_state.change_count, 2);
        assert_eq!(
            storage.sync_root_change_window_changes(&root.id).unwrap(),
            vec![
                RemoteChange::Upsert(first_item.clone()),
                RemoteChange::Delete {
                    remote_id: first_item.remote_id,
                },
            ]
        );

        let durable = storage
            .sync_root_change_window_state(&root.id)
            .unwrap()
            .unwrap();
        assert!(durable.is_complete());
        assert_eq!(durable.base_cursor.as_str(), "fence");
        assert_eq!(durable.checkpoint.unwrap().as_str(), "checkpoint");
        assert!(storage.sync_root_change_cursor(&root.id).unwrap().is_none());
        assert!(
            !storage
                .sync_root_remote_inventory_state(&root.id)
                .unwrap()
                .catchup_complete
        );
    }

    #[test]
    fn root_change_window_failed_page_preserves_previous_page() {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = test_account(&provider);
        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "window-rollback",
            provider,
            account.subject,
            "/tmp/window-rollback",
            Some("remote-root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        storage.insert_sync_root(&root).unwrap();
        prepare_root_snapshot(&mut storage, &root, &[], "fence", 10);

        let first_page = nubisync_core::ChangePage {
            changes: vec![],
            continuation: Some(ContinuationToken::new("page-two").unwrap()),
            checkpoint: None,
        };

        storage
            .stage_sync_root_change_window_page(
                &root.id,
                &ChangeCursor::new("fence").unwrap(),
                None,
                &first_page,
            )
            .unwrap();

        let invalid_item = RemoteItem {
            remote_id: "invalid-size".into(),
            parent_remote_id: Some("remote-root".into()),
            name: "large.bin".into(),
            kind: RemoteItemKind::File,
            size_bytes: Some(u64::MAX),
            modified_unix_ms: None,
            trashed: false,
        };
        let bad_page = nubisync_core::ChangePage {
            changes: vec![RemoteChange::Upsert(invalid_item)],
            continuation: None,
            checkpoint: Some(ChangeCursor::new("checkpoint").unwrap()),
        };

        let error = storage
            .stage_sync_root_change_window_page(
                &root.id,
                &ChangeCursor::new("fence").unwrap(),
                Some(&ContinuationToken::new("page-two").unwrap()),
                &bad_page,
            )
            .unwrap_err();

        assert!(matches!(error, StorageError::NumericOverflow));

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
    fn root_change_window_detects_pagination_cycles() {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = test_account(&provider);
        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "window-loop",
            provider,
            account.subject,
            "/tmp/window-loop",
            Some("remote-root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        storage.insert_sync_root(&root).unwrap();
        prepare_root_snapshot(&mut storage, &root, &[], "fence", 10);

        let page_one = nubisync_core::ChangePage {
            changes: vec![],
            continuation: Some(ContinuationToken::new("token-a").unwrap()),
            checkpoint: None,
        };
        storage
            .stage_sync_root_change_window_page(
                &root.id,
                &ChangeCursor::new("fence").unwrap(),
                None,
                &page_one,
            )
            .unwrap();

        let page_two = nubisync_core::ChangePage {
            changes: vec![],
            continuation: Some(ContinuationToken::new("token-b").unwrap()),
            checkpoint: None,
        };
        storage
            .stage_sync_root_change_window_page(
                &root.id,
                &ChangeCursor::new("fence").unwrap(),
                Some(&ContinuationToken::new("token-a").unwrap()),
                &page_two,
            )
            .unwrap();

        let cycle = nubisync_core::ChangePage {
            changes: vec![],
            continuation: Some(ContinuationToken::new("token-a").unwrap()),
            checkpoint: None,
        };
        let error = storage
            .stage_sync_root_change_window_page(
                &root.id,
                &ChangeCursor::new("fence").unwrap(),
                Some(&ContinuationToken::new("token-b").unwrap()),
                &cycle,
            )
            .unwrap_err();

        assert!(matches!(
            error,
            StorageError::SyncRootChangeWindowPaginationLoop
        ));

        let durable = storage
            .sync_root_change_window_state(&root.id)
            .unwrap()
            .unwrap();
        assert_eq!(durable.page_count, 2);
        assert_eq!(durable.continuation.as_ref().unwrap().as_str(), "token-b");
    }

    #[test]
    fn root_change_window_rejects_stale_base_cursor() {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = test_account(&provider);
        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "window-stale",
            provider,
            account.subject,
            "/tmp/window-stale",
            Some("remote-root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        storage.insert_sync_root(&root).unwrap();
        prepare_root_snapshot(&mut storage, &root, &[], "actual-fence", 10);

        let page = nubisync_core::ChangePage {
            changes: vec![],
            continuation: None,
            checkpoint: Some(ChangeCursor::new("checkpoint").unwrap()),
        };

        let error = storage
            .stage_sync_root_change_window_page(
                &root.id,
                &ChangeCursor::new("stale-fence").unwrap(),
                None,
                &page,
            )
            .unwrap_err();

        assert!(matches!(
            error,
            StorageError::SyncRootChangeWindowBaseCursorMismatch
        ));
        assert!(
            storage
                .sync_root_change_window_state(&root.id)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn root_batch_commit_completes_catchup_and_advances_incrementally() {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = test_account(&provider);
        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "root-batch",
            provider,
            account.subject,
            "/tmp/root-batch",
            Some("remote-root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        storage.insert_sync_root(&root).unwrap();

        let baseline = test_remote_item(
            "old-file",
            Some("remote-root"),
            "old.txt",
            RemoteItemKind::File,
        );
        prepare_root_snapshot(
            &mut storage,
            &root,
            std::slice::from_ref(&baseline),
            "bootstrap-fence",
            10,
        );

        let new_file = test_remote_item(
            "new-file",
            Some("remote-root"),
            "new.txt",
            RemoteItemKind::File,
        );
        let mutations = vec![
            SyncRootCatalogMutation::DeleteSubtree {
                remote_id: baseline.remote_id.clone(),
            },
            SyncRootCatalogMutation::Upsert(new_file.clone()),
        ];

        let result = storage
            .commit_sync_root_catalog_batch_and_cursor(
                &root.id,
                &ChangeCursor::new("bootstrap-fence").unwrap(),
                &mutations,
                &ChangeCursor::new("checkpoint-one").unwrap(),
                20,
            )
            .unwrap();

        assert_eq!(
            result,
            SyncRootCatalogBatchCommit {
                mutations_applied: 2,
                authoritative_items: 1,
                completed_initial_catchup: true,
            }
        );
        assert!(
            storage
                .sync_root_remote_item(&root.id, "old-file")
                .unwrap()
                .is_none()
        );
        assert_eq!(
            storage.sync_root_remote_item(&root.id, "new-file").unwrap(),
            Some(new_file)
        );

        let state = storage.sync_root_remote_inventory_state(&root.id).unwrap();
        assert!(state.snapshot_complete);
        assert!(state.catchup_complete);
        assert_eq!(state.item_count, 1);
        assert_eq!(
            state.catchup_from_cursor.unwrap().as_str(),
            "bootstrap-fence"
        );
        assert_eq!(
            storage
                .sync_root_change_cursor(&root.id)
                .unwrap()
                .unwrap()
                .as_str(),
            "checkpoint-one"
        );

        let result = storage
            .commit_sync_root_catalog_batch_and_cursor(
                &root.id,
                &ChangeCursor::new("checkpoint-one").unwrap(),
                &[],
                &ChangeCursor::new("checkpoint-two").unwrap(),
                30,
            )
            .unwrap();

        assert_eq!(
            result,
            SyncRootCatalogBatchCommit {
                mutations_applied: 0,
                authoritative_items: 1,
                completed_initial_catchup: false,
            }
        );
        assert_eq!(
            storage
                .sync_root_change_cursor(&root.id)
                .unwrap()
                .unwrap()
                .as_str(),
            "checkpoint-two"
        );
    }

    #[test]
    fn root_batch_failure_rolls_back_catalog_and_cursor() {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = test_account(&provider);
        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "root-rollback",
            provider,
            account.subject,
            "/tmp/root-rollback",
            Some("remote-root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        storage.insert_sync_root(&root).unwrap();

        let baseline = test_remote_item(
            "baseline",
            Some("remote-root"),
            "baseline.txt",
            RemoteItemKind::File,
        );
        prepare_root_snapshot(
            &mut storage,
            &root,
            std::slice::from_ref(&baseline),
            "rollback-fence",
            10,
        );

        let valid = test_remote_item(
            "valid-before-failure",
            Some("remote-root"),
            "valid.txt",
            RemoteItemKind::File,
        );
        let invalid = RemoteItem {
            remote_id: "too-large".into(),
            parent_remote_id: Some("remote-root".into()),
            name: "too-large.bin".into(),
            kind: RemoteItemKind::File,
            size_bytes: Some(u64::MAX),
            modified_unix_ms: None,
            trashed: false,
        };

        let error = storage
            .commit_sync_root_catalog_batch_and_cursor(
                &root.id,
                &ChangeCursor::new("rollback-fence").unwrap(),
                &[
                    SyncRootCatalogMutation::Upsert(valid),
                    SyncRootCatalogMutation::Upsert(invalid),
                ],
                &ChangeCursor::new("must-not-persist").unwrap(),
                20,
            )
            .unwrap_err();

        assert!(matches!(error, StorageError::NumericOverflow));
        assert!(
            storage
                .sync_root_remote_item(&root.id, "valid-before-failure")
                .unwrap()
                .is_none()
        );
        assert_eq!(
            storage.sync_root_remote_item(&root.id, "baseline").unwrap(),
            Some(baseline)
        );
        assert!(storage.sync_root_change_cursor(&root.id).unwrap().is_none());

        let state = storage.sync_root_remote_inventory_state(&root.id).unwrap();
        assert!(state.snapshot_complete);
        assert!(!state.catchup_complete);
        assert_eq!(state.item_count, 1);
        assert_eq!(
            state.catchup_from_cursor.unwrap().as_str(),
            "rollback-fence"
        );
    }

    #[test]
    fn root_batch_rejects_cursor_mismatch_without_mutation() {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = test_account(&provider);
        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "root-mismatch",
            provider,
            account.subject,
            "/tmp/root-mismatch",
            Some("remote-root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        storage.insert_sync_root(&root).unwrap();

        prepare_root_snapshot(&mut storage, &root, &[], "actual-fence", 10);

        let item = test_remote_item(
            "must-not-appear",
            Some("remote-root"),
            "nope.txt",
            RemoteItemKind::File,
        );

        let error = storage
            .commit_sync_root_catalog_batch_and_cursor(
                &root.id,
                &ChangeCursor::new("wrong-fence").unwrap(),
                &[SyncRootCatalogMutation::Upsert(item)],
                &ChangeCursor::new("must-not-persist").unwrap(),
                20,
            )
            .unwrap_err();

        assert!(matches!(
            error,
            StorageError::SyncRootCatalogExpectedCursorMismatch
        ));
        assert_eq!(
            storage.sync_root_remote_inventory_count(&root.id).unwrap(),
            0
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
    fn root_batch_mutations_are_isolated_between_roots() {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = test_account(&provider);
        storage.upsert_account(&account, 1).unwrap();

        let root_one = SyncRoot::new(
            "batch-root-one",
            provider.clone(),
            account.subject.clone(),
            "/tmp/batch-root-one",
            Some("remote-root-one".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        let root_two = SyncRoot::new(
            "batch-root-two",
            provider,
            account.subject,
            "/tmp/batch-root-two",
            Some("remote-root-two".into()),
            SyncMode::ReceiveOnly,
            3,
        )
        .unwrap();
        storage.insert_sync_root(&root_one).unwrap();
        storage.insert_sync_root(&root_two).unwrap();

        let shared_one = test_remote_item(
            "shared-id",
            Some("remote-root-one"),
            "same.txt",
            RemoteItemKind::File,
        );
        let shared_two = test_remote_item(
            "shared-id",
            Some("remote-root-two"),
            "same.txt",
            RemoteItemKind::File,
        );

        prepare_root_snapshot(
            &mut storage,
            &root_one,
            std::slice::from_ref(&shared_one),
            "fence-one",
            10,
        );
        prepare_root_snapshot(
            &mut storage,
            &root_two,
            std::slice::from_ref(&shared_two),
            "fence-two",
            20,
        );

        storage
            .commit_sync_root_catalog_batch_and_cursor(
                &root_one.id,
                &ChangeCursor::new("fence-one").unwrap(),
                &[SyncRootCatalogMutation::DeleteSubtree {
                    remote_id: "shared-id".into(),
                }],
                &ChangeCursor::new("root-one-next").unwrap(),
                30,
            )
            .unwrap();

        assert!(
            storage
                .sync_root_remote_item(&root_one.id, "shared-id")
                .unwrap()
                .is_none()
        );
        assert_eq!(
            storage
                .sync_root_remote_item(&root_two.id, "shared-id")
                .unwrap(),
            Some(shared_two)
        );
        assert_eq!(
            storage
                .sync_root_remote_inventory_state(&root_one.id)
                .unwrap()
                .item_count,
            0
        );
        assert_eq!(
            storage
                .sync_root_remote_inventory_state(&root_two.id)
                .unwrap()
                .item_count,
            1
        );
        assert!(
            storage
                .sync_root_change_cursor(&root_two.id)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn root_batch_requires_authoritative_snapshot() {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = test_account(&provider);
        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "root-without-snapshot",
            provider,
            account.subject,
            "/tmp/root-without-snapshot",
            Some("remote-root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        storage.insert_sync_root(&root).unwrap();

        let error = storage
            .commit_sync_root_catalog_batch_and_cursor(
                &root.id,
                &ChangeCursor::new("missing-fence").unwrap(),
                &[],
                &ChangeCursor::new("next").unwrap(),
                10,
            )
            .unwrap_err();

        assert!(matches!(
            error,
            StorageError::SyncRootCatalogSnapshotMissing
        ));
    }

    #[test]
    fn cursor_round_trip_requires_an_account() {
        let storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = test_account(&provider);

        storage.upsert_account(&account, 1_700_000_000_000).unwrap();

        let cursor = ChangeCursor::new("opaque-drive-change-token").unwrap();
        storage
            .save_cursor(&provider, &account.subject, &cursor, 1_700_000_001_000)
            .unwrap();

        let loaded = storage
            .load_cursor(&provider, &account.subject)
            .unwrap()
            .unwrap();

        assert_eq!(loaded.as_str(), cursor.as_str());
        assert_eq!(format!("{loaded:?}"), "ChangeCursor([redacted])");
    }

    #[test]
    fn bounded_inventory_staging_can_be_discarded() {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = test_account(&provider);
        storage.upsert_account(&account, 1).unwrap();
        let item = match test_upsert("file-1", Some(10)) {
            RemoteChange::Upsert(item) => item,
            RemoteChange::Delete { .. } => unreachable!(),
        };
        storage
            .begin_remote_inventory_staging(&provider, &account.subject)
            .unwrap();
        storage
            .stage_remote_inventory_items(&provider, &account.subject, &[item], 2)
            .unwrap();
        assert_eq!(
            storage
                .staged_remote_inventory_count(&provider, &account.subject)
                .unwrap(),
            1
        );
        assert_eq!(
            storage
                .remote_inventory_count(&provider, &account.subject)
                .unwrap(),
            0
        );
        storage
            .clear_remote_inventory_staging(&provider, &account.subject)
            .unwrap();
        assert_eq!(
            storage
                .staged_remote_inventory_count(&provider, &account.subject)
                .unwrap(),
            0
        );
    }

    #[test]
    fn complete_inventory_promotes_staging() {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = test_account(&provider);
        storage.upsert_account(&account, 1).unwrap();
        let first = match test_upsert("file-1", Some(10)) {
            RemoteChange::Upsert(item) => item,
            _ => unreachable!(),
        };
        let second = match test_upsert("file-2", Some(20)) {
            RemoteChange::Upsert(item) => item,
            _ => unreachable!(),
        };
        storage
            .begin_remote_inventory_staging(&provider, &account.subject)
            .unwrap();
        storage
            .stage_remote_inventory_items(&provider, &account.subject, &[first, second], 2)
            .unwrap();
        assert_eq!(
            storage
                .commit_remote_inventory_snapshot(
                    &provider,
                    &account.subject,
                    &ChangeCursor::new("bootstrap-fence").unwrap(),
                    3,
                )
                .unwrap(),
            2
        );
        assert_eq!(
            storage
                .remote_inventory_count(&provider, &account.subject)
                .unwrap(),
            2
        );
        assert_eq!(
            storage
                .staged_remote_inventory_count(&provider, &account.subject)
                .unwrap(),
            0
        );
    }

    #[test]
    fn inventory_state_defaults_to_not_ready() {
        let storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = test_account(&provider);
        storage.upsert_account(&account, 1).unwrap();

        let state = storage
            .remote_inventory_state(&provider, &account.subject)
            .unwrap();

        assert!(!state.snapshot_complete);
        assert!(!state.catchup_complete);
        assert_eq!(state.item_count, 0);
        assert_eq!(state.snapshot_completed_at_unix_ms, None);
        assert!(state.catchup_from_cursor.is_none());
        assert!(!state.ready_for_reconciliation());
    }

    #[test]
    fn snapshot_promotion_marks_snapshot_complete_but_not_catchup_complete() {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = test_account(&provider);
        storage.upsert_account(&account, 1).unwrap();

        let item = match test_upsert("file-1", Some(10)) {
            RemoteChange::Upsert(item) => item,
            _ => unreachable!(),
        };

        storage
            .begin_remote_inventory_staging(&provider, &account.subject)
            .unwrap();
        storage
            .stage_remote_inventory_items(&provider, &account.subject, &[item], 2)
            .unwrap();
        storage
            .commit_remote_inventory_snapshot(
                &provider,
                &account.subject,
                &ChangeCursor::new("bootstrap-fence").unwrap(),
                3,
            )
            .unwrap();

        let state = storage
            .remote_inventory_state(&provider, &account.subject)
            .unwrap();

        assert!(state.snapshot_complete);
        assert!(!state.catchup_complete);
        assert_eq!(state.item_count, 1);
        assert_eq!(state.snapshot_completed_at_unix_ms, Some(3));
        assert_eq!(
            state.catchup_from_cursor.as_ref().unwrap().as_str(),
            "bootstrap-fence"
        );
        assert_eq!(
            format!("{:?}", state.catchup_from_cursor.as_ref().unwrap()),
            "ChangeCursor([redacted])"
        );
        assert!(!state.ready_for_reconciliation());
    }

    #[test]
    fn catalog_catchup_requires_complete_snapshot() {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = test_account(&provider);
        storage.upsert_account(&account, 1).unwrap();

        let result = storage.commit_remote_catalog_catchup(
            &provider,
            &account.subject,
            &ChangeCursor::new("fence").unwrap(),
            &[],
            &ChangeCursor::new("checkpoint").unwrap(),
            2,
        );

        assert!(matches!(
            result,
            Err(StorageError::RemoteCatalogSnapshotMissing)
        ));
    }

    #[test]
    fn catalog_catchup_applies_changes_advances_cursor_and_supersedes_journal() {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = test_account(&provider);
        storage.upsert_account(&account, 1).unwrap();

        let first = match test_upsert("file-1", Some(10)) {
            RemoteChange::Upsert(item) => item,
            _ => unreachable!(),
        };
        let second = match test_upsert("file-2", Some(20)) {
            RemoteChange::Upsert(item) => item,
            _ => unreachable!(),
        };
        let fence = ChangeCursor::new("bootstrap-fence").unwrap();

        storage
            .begin_remote_inventory_staging(&provider, &account.subject)
            .unwrap();
        storage
            .stage_remote_inventory_items(&provider, &account.subject, &[first, second], 2)
            .unwrap();
        storage
            .commit_remote_inventory_snapshot(&provider, &account.subject, &fence, 3)
            .unwrap();

        storage
            .commit_remote_changes_and_cursor(
                &provider,
                &account.subject,
                &[test_upsert("journal-old", Some(1))],
                &ChangeCursor::new("old-poll-cursor").unwrap(),
                4,
            )
            .unwrap();

        let replacement = RemoteChange::Upsert(RemoteItem {
            remote_id: "file-3".into(),
            parent_remote_id: Some("root".into()),
            name: "replacement.txt".into(),
            kind: RemoteItemKind::File,
            size_bytes: Some(30),
            modified_unix_ms: None,
            trashed: false,
        });

        let checkpoint = ChangeCursor::new("catchup-checkpoint").unwrap();
        let result = storage
            .commit_remote_catalog_catchup(
                &provider,
                &account.subject,
                &fence,
                &[
                    RemoteChange::Delete {
                        remote_id: "file-1".into(),
                    },
                    replacement,
                ],
                &checkpoint,
                5,
            )
            .unwrap();

        assert_eq!(result.changes_applied, 2);
        assert_eq!(result.authoritative_items, 2);
        assert_eq!(result.remote_events_superseded, 1);

        let state = storage
            .remote_inventory_state(&provider, &account.subject)
            .unwrap();
        assert!(state.snapshot_complete);
        assert!(state.catchup_complete);
        assert_eq!(state.item_count, 2);
        assert!(state.ready_for_reconciliation());

        assert_eq!(
            storage
                .load_cursor(&provider, &account.subject)
                .unwrap()
                .unwrap()
                .as_str(),
            checkpoint.as_str()
        );
        assert_eq!(
            storage
                .pending_remote_event_count(&provider, &account.subject)
                .unwrap(),
            0
        );
    }

    #[test]
    fn catalog_catchup_rolls_back_on_invalid_numeric_item() {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = test_account(&provider);
        storage.upsert_account(&account, 1).unwrap();

        let initial = match test_upsert("file-1", Some(10)) {
            RemoteChange::Upsert(item) => item,
            _ => unreachable!(),
        };
        let fence = ChangeCursor::new("bootstrap-fence").unwrap();

        storage
            .begin_remote_inventory_staging(&provider, &account.subject)
            .unwrap();
        storage
            .stage_remote_inventory_items(&provider, &account.subject, &[initial], 2)
            .unwrap();
        storage
            .commit_remote_inventory_snapshot(&provider, &account.subject, &fence, 3)
            .unwrap();

        let old_cursor = ChangeCursor::new("old-poll-cursor").unwrap();
        storage
            .commit_remote_changes_and_cursor(
                &provider,
                &account.subject,
                &[test_upsert("journal-old", Some(1))],
                &old_cursor,
                4,
            )
            .unwrap();

        let valid = test_upsert("file-2", Some(20));
        let overflow = test_upsert("file-overflow", Some(u64::MAX));

        let result = storage.commit_remote_catalog_catchup(
            &provider,
            &account.subject,
            &fence,
            &[valid, overflow],
            &ChangeCursor::new("new-checkpoint").unwrap(),
            5,
        );

        assert!(matches!(result, Err(StorageError::NumericOverflow)));

        let state = storage
            .remote_inventory_state(&provider, &account.subject)
            .unwrap();
        assert!(state.snapshot_complete);
        assert!(!state.catchup_complete);
        assert_eq!(state.item_count, 1);

        assert_eq!(
            storage
                .remote_inventory_count(&provider, &account.subject)
                .unwrap(),
            1
        );
        assert_eq!(
            storage
                .load_cursor(&provider, &account.subject)
                .unwrap()
                .unwrap()
                .as_str(),
            old_cursor.as_str()
        );
        assert_eq!(
            storage
                .pending_remote_event_count(&provider, &account.subject)
                .unwrap(),
            1
        );
    }

    #[test]
    fn remote_events_and_cursor_commit_together() {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = test_account(&provider);
        storage.upsert_account(&account, 1).unwrap();

        let old_cursor = ChangeCursor::new("cursor-old").unwrap();
        storage
            .save_cursor(&provider, &account.subject, &old_cursor, 2)
            .unwrap();

        let new_cursor = ChangeCursor::new("cursor-new").unwrap();
        let changes = vec![
            test_upsert("file-1", Some(123)),
            RemoteChange::Delete {
                remote_id: "file-2".into(),
            },
        ];

        let persisted = storage
            .commit_remote_changes_and_cursor(&provider, &account.subject, &changes, &new_cursor, 3)
            .unwrap();

        assert_eq!(persisted, 2);
        assert_eq!(
            storage
                .pending_remote_event_count(&provider, &account.subject)
                .unwrap(),
            2
        );
        assert_eq!(
            storage
                .load_cursor(&provider, &account.subject)
                .unwrap()
                .unwrap()
                .as_str(),
            "cursor-new"
        );
    }

    #[test]
    fn failed_remote_batch_does_not_advance_cursor_or_leave_partial_events() {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = test_account(&provider);
        storage.upsert_account(&account, 1).unwrap();

        let old_cursor = ChangeCursor::new("cursor-old").unwrap();
        storage
            .save_cursor(&provider, &account.subject, &old_cursor, 2)
            .unwrap();

        let new_cursor = ChangeCursor::new("cursor-new").unwrap();
        let changes = vec![
            test_upsert("file-valid", Some(123)),
            test_upsert("file-overflow", Some(u64::MAX)),
        ];

        assert!(matches!(
            storage.commit_remote_changes_and_cursor(
                &provider,
                &account.subject,
                &changes,
                &new_cursor,
                3,
            ),
            Err(StorageError::NumericOverflow)
        ));

        assert_eq!(
            storage
                .pending_remote_event_count(&provider, &account.subject)
                .unwrap(),
            0
        );
        assert_eq!(
            storage
                .load_cursor(&provider, &account.subject)
                .unwrap()
                .unwrap()
                .as_str(),
            "cursor-old"
        );
    }
}

#[cfg(test)]
mod phase5c5_stale_receipt_tests {
    use super::*;

    fn setup() -> (Storage, SyncRoot) {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = ProviderAccount::new(provider.clone(), "subject", None, None).unwrap();
        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "phase5c5-root",
            provider,
            account.subject,
            "/tmp/phase5c5-root",
            Some("selected-root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        storage.insert_sync_root(&root).unwrap();

        let file = RemoteItem {
            remote_id: "file".into(),
            parent_remote_id: Some("selected-root".into()),
            name: "file.txt".into(),
            kind: RemoteItemKind::File,
            size_bytes: Some(13),
            modified_unix_ms: None,
            trashed: false,
        };

        storage
            .begin_sync_root_remote_inventory_staging(&root.id)
            .unwrap();
        storage
            .stage_sync_root_remote_inventory_items(&root.id, &[file], 3)
            .unwrap();
        storage
            .commit_sync_root_remote_inventory_snapshot(
                &root.id,
                &ChangeCursor::new("fence").unwrap(),
                4,
            )
            .unwrap();

        storage
            .record_sync_root_file_materialization(
                &root.id,
                "file",
                "file.txt",
                13,
                &"a".repeat(64),
                5,
            )
            .unwrap();

        (storage, root)
    }

    #[test]
    fn remote_upsert_preserves_previous_receipt_as_stale() {
        let (mut storage, root) = setup();

        assert_eq!(
            storage
                .sync_root_materialization_receipt_count(&root.id)
                .unwrap(),
            1
        );
        assert_eq!(
            storage
                .sync_root_stale_materialization_receipt_count(&root.id)
                .unwrap(),
            0
        );

        let changed = RemoteItem {
            remote_id: "file".into(),
            parent_remote_id: Some("selected-root".into()),
            name: "file.txt".into(),
            kind: RemoteItemKind::File,
            size_bytes: Some(14),
            modified_unix_ms: None,
            trashed: false,
        };

        storage
            .upsert_sync_root_remote_item(&root.id, &changed, 6)
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
            1
        );

        let stale = storage
            .list_sync_root_stale_file_materialization_receipts(&root.id)
            .unwrap();
        assert_eq!(stale.len(), 1);
        assert_eq!(stale[0].size_bytes, 13);
    }

    #[test]
    fn rematerialization_reactivates_stale_receipt() {
        let (mut storage, root) = setup();

        let changed = RemoteItem {
            remote_id: "file".into(),
            parent_remote_id: Some("selected-root".into()),
            name: "file.txt".into(),
            kind: RemoteItemKind::File,
            size_bytes: Some(14),
            modified_unix_ms: None,
            trashed: false,
        };

        storage
            .upsert_sync_root_remote_item(&root.id, &changed, 6)
            .unwrap();

        storage
            .record_sync_root_file_materialization(
                &root.id,
                "file",
                "file.txt",
                14,
                &"b".repeat(64),
                7,
            )
            .unwrap();

        assert_eq!(
            storage
                .sync_root_materialization_receipt_count(&root.id)
                .unwrap(),
            1
        );
        assert_eq!(
            storage
                .sync_root_stale_materialization_receipt_count(&root.id)
                .unwrap(),
            0
        );
    }
}

#[cfg(test)]
mod phase5c9_deletion_receipt_tests {
    use super::*;

    #[test]
    fn stale_receipt_cleanup_never_deletes_current_receipt() {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account = ProviderAccount::new(provider.clone(), "subject", None, None).unwrap();
        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "phase5c9-root",
            provider,
            account.subject,
            "/tmp/phase5c9-root",
            Some("remote-root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        storage.insert_sync_root(&root).unwrap();

        storage
            .connection
            .execute(
                "INSERT INTO sync_root_file_materialization_receipts (
                    sync_root_id,
                    remote_id,
                    relative_path,
                    size_bytes,
                    sha256_hex,
                    materialized_at_unix_ms,
                    receipt_state
                 ) VALUES (?1, 'gone', 'gone.txt', 5, ?2, 3, 'stale')",
                params![root.id, "a".repeat(64)],
            )
            .unwrap();

        storage
            .connection
            .execute(
                "INSERT INTO sync_root_file_materialization_receipts (
                    sync_root_id,
                    remote_id,
                    relative_path,
                    size_bytes,
                    sha256_hex,
                    materialized_at_unix_ms,
                    receipt_state
                 ) VALUES (?1, 'live', 'live.txt', 5, ?2, 4, 'current')",
                params![root.id, "b".repeat(64)],
            )
            .unwrap();

        assert!(
            storage
                .delete_sync_root_stale_file_materialization_receipt(&root.id, "gone")
                .unwrap()
        );
        assert_eq!(
            storage
                .sync_root_stale_materialization_receipt_count(&root.id)
                .unwrap(),
            0
        );
        assert_eq!(
            storage
                .sync_root_materialization_receipt_count(&root.id)
                .unwrap(),
            1
        );

        assert!(
            !storage
                .delete_sync_root_stale_file_materialization_receipt(&root.id, "live")
                .unwrap()
        );
        assert_eq!(
            storage
                .sync_root_materialization_receipt_count(&root.id)
                .unwrap(),
            1
        );
    }
}

#[cfg(test)]
mod phase5c10_directory_receipt_tests {
    use super::*;

    fn setup() -> (Storage, SyncRoot) {
        let storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account =
            ProviderAccount::new(provider.clone(), "phase5c10-subject", None, None).unwrap();
        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "phase5c10-root",
            provider,
            account.subject,
            "/tmp/phase5c10-root",
            Some("remote-root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        storage.insert_sync_root(&root).unwrap();

        storage
            .connection
            .execute(
                "INSERT INTO sync_root_remote_items (
                    sync_root_id,
                    remote_id,
                    parent_remote_id,
                    name,
                    item_kind,
                    size_bytes,
                    trashed,
                    observed_at_unix_ms
                 ) VALUES (?1, 'folder', 'remote-root', 'folder', 'folder', NULL, 0, 3)",
                params![root.id],
            )
            .unwrap();

        (storage, root)
    }

    #[test]
    fn schema_v16_contains_directory_receipts() {
        let storage = Storage::open_in_memory().unwrap();
        assert_eq!(storage.schema_version().unwrap(), 16);

        let exists: i64 = storage
            .connection
            .query_row(
                "SELECT COUNT(*)
                 FROM sqlite_master
                 WHERE type = 'table'
                   AND name = 'sync_root_directory_materialization_receipts'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(exists, 1);
    }

    #[test]
    fn directory_receipt_records_and_invalidates_to_stale() {
        let (mut storage, root) = setup();

        assert_eq!(
            storage
                .record_sync_root_directory_materializations(
                    &root.id,
                    &[("folder".into(), "folder".into())],
                    4,
                )
                .unwrap(),
            1
        );
        assert_eq!(
            storage
                .sync_root_directory_materialization_receipt_count(&root.id)
                .unwrap(),
            1
        );
        assert_eq!(
            storage
                .sync_root_stale_directory_materialization_receipt_count(&root.id)
                .unwrap(),
            0
        );

        let transaction = storage.connection.transaction().unwrap();
        invalidate_sync_root_materialization_subtree(&transaction, &root.id, "folder").unwrap();
        transaction.commit().unwrap();

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
            1
        );
    }
}

#[cfg(test)]
mod phase5c12_directory_receipt_cleanup_tests {
    use super::*;

    #[test]
    fn stale_directory_receipt_cleanup_never_deletes_current_receipt() {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account =
            ProviderAccount::new(provider.clone(), "phase5c12-subject", None, None).unwrap();
        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "phase5c12-root",
            provider,
            account.subject,
            "/tmp/phase5c12-root",
            Some("remote-root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        storage.insert_sync_root(&root).unwrap();

        storage
            .connection
            .execute(
                "INSERT INTO sync_root_directory_materialization_receipts (
                    sync_root_id,
                    remote_id,
                    relative_path,
                    materialized_at_unix_ms,
                    receipt_state
                 ) VALUES (?1, 'gone', 'gone', 3, 'stale')",
                params![root.id],
            )
            .unwrap();

        storage
            .connection
            .execute(
                "INSERT INTO sync_root_directory_materialization_receipts (
                    sync_root_id,
                    remote_id,
                    relative_path,
                    materialized_at_unix_ms,
                    receipt_state
                 ) VALUES (?1, 'live', 'live', 4, 'current')",
                params![root.id],
            )
            .unwrap();

        assert!(
            storage
                .delete_sync_root_stale_directory_materialization_receipt(&root.id, "gone",)
                .unwrap()
        );
        assert_eq!(
            storage
                .sync_root_stale_directory_materialization_receipt_count(&root.id)
                .unwrap(),
            0
        );
        assert_eq!(
            storage
                .sync_root_directory_materialization_receipt_count(&root.id)
                .unwrap(),
            1
        );

        assert!(
            !storage
                .delete_sync_root_stale_directory_materialization_receipt(&root.id, "live",)
                .unwrap()
        );
        assert_eq!(
            storage
                .sync_root_directory_materialization_receipt_count(&root.id)
                .unwrap(),
            1
        );
    }
}

#[cfg(test)]
mod phase5d4_upsert_receipt_scope_tests {
    use super::*;

    fn setup() -> (Storage, SyncRoot) {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account =
            ProviderAccount::new(provider.clone(), "phase5d4-scope-subject", None, None).unwrap();
        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "phase5d4-scope-root",
            provider,
            account.subject,
            "/tmp/nubisync-phase5d4-scope",
            Some("remote-root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        storage.insert_sync_root(&root).unwrap();

        storage
            .connection
            .execute(
                "INSERT INTO sync_root_remote_items (
                    sync_root_id, remote_id, parent_remote_id, name, item_kind,
                    size_bytes, trashed, observed_at_unix_ms
                 ) VALUES (?1, 'folder', 'remote-root', 'folder', 'folder', NULL, 0, 3)",
                params![root.id],
            )
            .unwrap();
        storage
            .connection
            .execute(
                "INSERT INTO sync_root_remote_items (
                    sync_root_id, remote_id, parent_remote_id, name, item_kind,
                    size_bytes, trashed, observed_at_unix_ms
                 ) VALUES (?1, 'file', 'folder', 'file.txt', 'file', 10, 0, 3)",
                params![root.id],
            )
            .unwrap();

        storage
            .record_sync_root_directory_materializations(
                &root.id,
                &[("folder".into(), "folder".into())],
                4,
            )
            .unwrap();
        storage
            .record_sync_root_file_materialization(
                &root.id,
                "file",
                "folder/file.txt",
                10,
                &"a".repeat(64),
                4,
            )
            .unwrap();

        (storage, root)
    }

    fn apply_upsert(storage: &mut Storage, root: &SyncRoot, item: RemoteItem) {
        let transaction = storage.connection.transaction().unwrap();
        upsert_sync_root_catalog_item(&transaction, &root.id, &item, 5).unwrap();
        transaction.commit().unwrap();
    }

    #[test]
    fn unchanged_folder_upsert_preserves_directory_and_descendant_file_receipts() {
        let (mut storage, root) = setup();
        apply_upsert(
            &mut storage,
            &root,
            RemoteItem {
                remote_id: "folder".into(),
                parent_remote_id: Some("remote-root".into()),
                name: "folder".into(),
                kind: RemoteItemKind::Folder,
                size_bytes: None,
                modified_unix_ms: None,
                trashed: false,
            },
        );

        assert_eq!(
            storage
                .sync_root_directory_materialization_receipt_count(&root.id)
                .unwrap(),
            1
        );
        assert_eq!(
            storage
                .sync_root_stale_directory_materialization_receipt_count(&root.id)
                .unwrap(),
            0
        );
        assert_eq!(
            storage
                .sync_root_materialization_receipt_count(&root.id)
                .unwrap(),
            1
        );
        assert_eq!(
            storage
                .sync_root_stale_materialization_receipt_count(&root.id)
                .unwrap(),
            0
        );
    }

    #[test]
    fn file_upsert_invalidates_only_the_file_receipt() {
        let (mut storage, root) = setup();
        apply_upsert(
            &mut storage,
            &root,
            RemoteItem {
                remote_id: "file".into(),
                parent_remote_id: Some("folder".into()),
                name: "file.txt".into(),
                kind: RemoteItemKind::File,
                size_bytes: Some(20),
                modified_unix_ms: None,
                trashed: false,
            },
        );

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
            1
        );
        assert_eq!(
            storage
                .sync_root_directory_materialization_receipt_count(&root.id)
                .unwrap(),
            1
        );
        assert_eq!(
            storage
                .sync_root_stale_directory_materialization_receipt_count(&root.id)
                .unwrap(),
            0
        );
    }

    #[test]
    fn structural_folder_upsert_still_invalidates_the_owned_subtree() {
        let (mut storage, root) = setup();
        apply_upsert(
            &mut storage,
            &root,
            RemoteItem {
                remote_id: "folder".into(),
                parent_remote_id: Some("remote-root".into()),
                name: "renamed-folder".into(),
                kind: RemoteItemKind::Folder,
                size_bytes: None,
                modified_unix_ms: None,
                trashed: false,
            },
        );

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
            1
        );
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
            1
        );
    }
}

#[cfg(test)]
mod phase5d7_directory_receipt_batch_tests {
    use super::*;

    fn fixture() -> (Storage, SyncRoot) {
        let storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account =
            ProviderAccount::new(provider.clone(), "phase5d7-storage-subject", None, None).unwrap();
        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "phase5d7-storage-root",
            provider,
            account.subject,
            "/tmp/nubisync-phase5d7-storage-root",
            Some("remote-root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        storage.insert_sync_root(&root).unwrap();

        for (remote_id, relative_path) in [("dir-a", "dir-a"), ("dir-b", "dir-b")] {
            storage
                .connection
                .execute(
                    "INSERT INTO sync_root_directory_materialization_receipts (
                        sync_root_id,
                        remote_id,
                        relative_path,
                        materialized_at_unix_ms,
                        receipt_state
                     ) VALUES (?1, ?2, ?3, 3, 'stale')",
                    params![&root.id, remote_id, relative_path],
                )
                .unwrap();
        }

        (storage, root)
    }

    #[test]
    fn phase5d7_stale_directory_receipt_batch_delete_is_atomic_and_exact() {
        let (mut storage, root) = fixture();

        assert_eq!(
            storage
                .sync_root_stale_directory_materialization_receipt_count(&root.id)
                .unwrap(),
            2
        );

        let deleted = storage
            .delete_sync_root_stale_directory_materialization_receipts(
                &root.id,
                &["dir-a".into(), "dir-b".into()],
            )
            .unwrap();

        assert_eq!(deleted, 2);
        assert_eq!(
            storage
                .sync_root_stale_directory_materialization_receipt_count(&root.id)
                .unwrap(),
            0
        );
    }

    #[test]
    fn phase5d7_stale_directory_receipt_batch_delete_rolls_back_on_mismatch() {
        let (mut storage, root) = fixture();

        let error = storage
            .delete_sync_root_stale_directory_materialization_receipts(
                &root.id,
                &["dir-a".into(), "missing-stale-dir".into()],
            )
            .unwrap_err();

        assert!(matches!(
            error,
            StorageError::StaleDirectoryMaterializationReceiptBatchMismatch
        ));
        assert_eq!(
            storage
                .sync_root_stale_directory_materialization_receipt_count(&root.id)
                .unwrap(),
            2
        );
    }
}

#[cfg(test)]
mod phase5f1_local_inventory_tests {
    use super::*;

    fn setup() -> (Storage, SyncRoot) {
        let storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account =
            ProviderAccount::new(provider.clone(), "phase5f1-subject", None, None).unwrap();
        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "phase5f1-root",
            provider,
            account.subject,
            "/tmp/phase5f1-root",
            Some("remote-root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        storage.insert_sync_root(&root).unwrap();
        (storage, root)
    }

    #[test]
    fn phase5f1_local_inventory_snapshot_is_staged_and_committed_atomically() {
        let (mut storage, root) = setup();

        let initial = storage.sync_root_local_inventory_state(&root.id).unwrap();
        assert!(!initial.snapshot_complete);
        assert_eq!(initial.item_count, 0);
        assert_eq!(initial.generation, 0);
        assert!(!initial.observation_valid);

        let items = vec![
            LocalItemSnapshot::new("docs", LocalItemKind::Directory, None, 100, 8, 40).unwrap(),
            LocalItemSnapshot::new("docs/file.txt", LocalItemKind::File, Some(5), 101, 8, 41)
                .unwrap(),
        ];

        storage
            .begin_sync_root_local_inventory_staging(&root.id)
            .unwrap();
        storage
            .stage_sync_root_local_inventory_items(&root.id, &items, 10)
            .unwrap();

        assert_eq!(
            storage
                .staged_sync_root_local_inventory_count(&root.id)
                .unwrap(),
            2
        );
        assert!(
            storage
                .list_sync_root_local_items(&root.id)
                .unwrap()
                .is_empty()
        );

        let committed = storage
            .commit_sync_root_local_inventory_snapshot(&root.id, 11)
            .unwrap();
        assert_eq!(committed, 2);

        let state = storage.sync_root_local_inventory_state(&root.id).unwrap();
        assert!(state.snapshot_complete);
        assert_eq!(state.item_count, 2);
        assert_eq!(state.snapshot_completed_at_unix_ms, Some(11));
        assert_eq!(state.generation, 1);
        assert!(state.observation_valid);

        let current = storage.list_sync_root_local_items(&root.id).unwrap();
        assert_eq!(current.len(), 2);
        assert_eq!(current[0].relative_path(), "docs");
        assert_eq!(current[1].relative_path(), "docs/file.txt");

        let replacement = vec![
            LocalItemSnapshot::new("docs/file.txt", LocalItemKind::File, Some(6), 200, 8, 41)
                .unwrap(),
        ];

        storage
            .begin_sync_root_local_inventory_staging(&root.id)
            .unwrap();
        storage
            .stage_sync_root_local_inventory_items(&root.id, &replacement, 20)
            .unwrap();

        assert_eq!(
            storage.list_sync_root_local_items(&root.id).unwrap().len(),
            2
        );

        storage
            .commit_sync_root_local_inventory_snapshot(&root.id, 21)
            .unwrap();

        let current = storage.list_sync_root_local_items(&root.id).unwrap();
        assert_eq!(current.len(), 1);
        assert_eq!(current[0].size_bytes(), Some(6));

        let state = storage.sync_root_local_inventory_state(&root.id).unwrap();
        assert_eq!(state.item_count, 1);
        assert_eq!(state.snapshot_completed_at_unix_ms, Some(21));
        assert_eq!(state.generation, 2);
        assert!(state.observation_valid);
    }

    #[test]
    fn phase5f1_schema_is_v16() {
        let storage = Storage::open_in_memory().unwrap();
        assert_eq!(storage.schema_version().unwrap(), 16);
    }
}

#[cfg(test)]
mod phase5f4_local_journal_tests {
    use super::*;

    fn fixture() -> (Storage, SyncRoot) {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account =
            ProviderAccount::new(provider.clone(), "phase5f4-subject", None, None).unwrap();
        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "phase5f4-root",
            provider,
            account.subject,
            "/tmp/phase5f4-root",
            Some("remote-root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        storage.insert_sync_root(&root).unwrap();

        let items = vec![
            LocalItemSnapshot::new("docs", LocalItemKind::Directory, None, 100, 8, 40).unwrap(),
            LocalItemSnapshot::new("docs/file.txt", LocalItemKind::File, Some(5), 101, 8, 41)
                .unwrap(),
        ];
        storage
            .begin_sync_root_local_inventory_staging(&root.id)
            .unwrap();
        storage
            .stage_sync_root_local_inventory_items(&root.id, &items, 10)
            .unwrap();
        storage
            .commit_sync_root_local_inventory_snapshot(&root.id, 11)
            .unwrap();

        (storage, root)
    }

    #[test]
    fn phase5f4_journal_is_bound_to_generation_and_idempotently_reconciled() {
        let (mut storage, root) = fixture();
        let state = storage.sync_root_local_inventory_state(&root.id).unwrap();
        assert_eq!(state.generation, 1);

        let events = vec![
            LocalChangeEventInput::new(
                "created.txt",
                LocalChangeEventKind::Created,
                None,
                Some(LocalItemKind::File),
            )
            .unwrap(),
            LocalChangeEventInput::new(
                "docs/file.txt",
                LocalChangeEventKind::Modified,
                Some(LocalItemKind::File),
                Some(LocalItemKind::File),
            )
            .unwrap(),
        ];

        let first = storage
            .reconcile_sync_root_local_change_journal(
                &root.id,
                state.generation,
                state.item_count,
                state.snapshot_completed_at_unix_ms,
                &events,
                20,
            )
            .unwrap();
        assert_eq!(first.pending_events, 2);

        let second = storage
            .reconcile_sync_root_local_change_journal(
                &root.id,
                state.generation,
                state.item_count,
                state.snapshot_completed_at_unix_ms,
                &events,
                21,
            )
            .unwrap();
        assert_eq!(second.pending_events, 2);
        assert_eq!(
            storage
                .pending_sync_root_local_change_event_count(&root.id, state.generation)
                .unwrap(),
            2
        );

        let clean = storage
            .reconcile_sync_root_local_change_journal(
                &root.id,
                state.generation,
                state.item_count,
                state.snapshot_completed_at_unix_ms,
                &[],
                22,
            )
            .unwrap();
        assert_eq!(clean.pending_events, 0);
        assert_eq!(clean.superseded_events, 2);
    }

    #[test]
    fn phase5f4_journal_rejects_stale_baseline_generation_without_mutation() {
        let (mut storage, root) = fixture();
        let state = storage.sync_root_local_inventory_state(&root.id).unwrap();

        let event = LocalChangeEventInput::new(
            "created.txt",
            LocalChangeEventKind::Created,
            None,
            Some(LocalItemKind::File),
        )
        .unwrap();

        let error = storage
            .reconcile_sync_root_local_change_journal(
                &root.id,
                state.generation + 1,
                state.item_count,
                state.snapshot_completed_at_unix_ms,
                &[event],
                20,
            )
            .unwrap_err();

        assert!(matches!(error, StorageError::LocalChangeBaselineMismatch));
        assert_eq!(
            storage
                .pending_sync_root_local_change_event_count(&root.id, state.generation)
                .unwrap(),
            0
        );
    }

    #[test]
    fn phase5f4_schema_is_v16() {
        let storage = Storage::open_in_memory().unwrap();
        assert_eq!(storage.schema_version().unwrap(), 16);
    }
}

#[cfg(test)]
mod phase5f5_observation_fence_tests {
    use super::*;

    fn fixture() -> (Storage, SyncRoot) {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account =
            ProviderAccount::new(provider.clone(), "phase5f5-subject", None, None).unwrap();
        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "phase5f5-root",
            provider,
            account.subject,
            "/tmp/phase5f5-root",
            Some("remote-root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        storage.insert_sync_root(&root).unwrap();

        let items = vec![
            LocalItemSnapshot::new("docs", LocalItemKind::Directory, None, 100, 8, 40).unwrap(),
            LocalItemSnapshot::new("docs/file.txt", LocalItemKind::File, Some(5), 101, 8, 41)
                .unwrap(),
        ];
        storage
            .begin_sync_root_local_inventory_staging(&root.id)
            .unwrap();
        storage
            .stage_sync_root_local_inventory_items(&root.id, &items, 10)
            .unwrap();
        storage
            .commit_sync_root_local_inventory_snapshot(&root.id, 11)
            .unwrap();

        (storage, root)
    }

    #[test]
    fn phase5f5_remote_mutation_fence_invalidates_until_supervised_rebaseline() {
        let (mut storage, root) = fixture();

        let before = storage.sync_root_local_inventory_state(&root.id).unwrap();
        assert!(before.observation_valid);
        assert_eq!(before.generation, 1);

        assert!(
            storage
                .invalidate_sync_root_local_observation_baseline(&root.id)
                .unwrap()
        );
        assert!(
            !storage
                .sync_root_local_inventory_state(&root.id)
                .unwrap()
                .observation_valid
        );
        assert!(
            !storage
                .invalidate_sync_root_local_observation_baseline(&root.id)
                .unwrap()
        );

        let event = LocalChangeEventInput::new(
            "created.txt",
            LocalChangeEventKind::Created,
            None,
            Some(LocalItemKind::File),
        )
        .unwrap();

        assert!(matches!(
            storage.reconcile_sync_root_local_change_journal(
                &root.id,
                before.generation,
                before.item_count,
                before.snapshot_completed_at_unix_ms,
                &[event],
                20,
            ),
            Err(StorageError::LocalChangeBaselineMismatch)
        ));

        let replacement = vec![
            LocalItemSnapshot::new("docs", LocalItemKind::Directory, None, 200, 8, 40).unwrap(),
            LocalItemSnapshot::new("docs/file.txt", LocalItemKind::File, Some(5), 201, 8, 41)
                .unwrap(),
        ];
        storage
            .begin_sync_root_local_inventory_staging(&root.id)
            .unwrap();
        storage
            .stage_sync_root_local_inventory_items(&root.id, &replacement, 30)
            .unwrap();
        storage
            .commit_sync_root_local_inventory_snapshot(&root.id, 31)
            .unwrap();

        let after = storage.sync_root_local_inventory_state(&root.id).unwrap();
        assert_eq!(after.generation, 2);
        assert!(after.observation_valid);
    }

    #[test]
    fn phase5f5_schema_is_v16() {
        let storage = Storage::open_in_memory().unwrap();
        assert_eq!(storage.schema_version().unwrap(), 16);
    }
}

#[cfg(test)]
mod phase5h1_remote_write_intent_foundation_tests {
    use super::*;

    fn setup(mode: SyncMode) -> (Storage, SyncRoot) {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account =
            ProviderAccount::new(provider.clone(), "phase5h1-subject", None, None).unwrap();
        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "phase5h1-root",
            provider,
            account.subject,
            "/tmp/phase5h1-root",
            Some("remote-root".into()),
            mode,
            2,
        )
        .unwrap();
        storage.insert_sync_root(&root).unwrap();

        let baseline = vec![
            LocalItemSnapshot::new("docs", LocalItemKind::Directory, None, 100, 8, 40).unwrap(),
            LocalItemSnapshot::new(
                "docs/existing.txt",
                LocalItemKind::File,
                Some(5),
                101,
                8,
                41,
            )
            .unwrap(),
        ];
        storage
            .begin_sync_root_local_inventory_staging(&root.id)
            .unwrap();
        storage
            .stage_sync_root_local_inventory_items(&root.id, &baseline, 10)
            .unwrap();
        storage
            .commit_sync_root_local_inventory_snapshot(&root.id, 11)
            .unwrap();

        let state = storage.sync_root_local_inventory_state(&root.id).unwrap();
        let events = vec![
            LocalChangeEventInput::new(
                "docs/new.txt",
                LocalChangeEventKind::Created,
                None,
                Some(LocalItemKind::File),
            )
            .unwrap(),
            LocalChangeEventInput::new(
                "docs/existing.txt",
                LocalChangeEventKind::Modified,
                Some(LocalItemKind::File),
                Some(LocalItemKind::File),
            )
            .unwrap(),
        ];
        storage
            .reconcile_sync_root_local_change_journal(
                &root.id,
                state.generation,
                state.item_count,
                state.snapshot_completed_at_unix_ms,
                &events,
                20,
            )
            .unwrap();

        (storage, root)
    }

    fn event_id(storage: &Storage, root_id: &str, path: &str) -> i64 {
        storage
            .connection
            .query_row(
                "SELECT id FROM sync_root_local_change_events
                 WHERE sync_root_id=?1 AND relative_path=?2 AND status='pending'",
                params![root_id, path],
                |row| row.get(0),
            )
            .unwrap()
    }

    #[test]
    fn phase5h1_schema_is_v16_and_tables_exist() {
        let storage = Storage::open_in_memory().unwrap();
        assert_eq!(storage.schema_version().unwrap(), 16);
        for table in [
            "sync_root_remote_write_authority",
            "sync_root_remote_write_intents",
        ] {
            let exists: i64 = storage
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
                    params![table],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(exists, 1);
        }
    }

    #[test]
    fn phase5h1_remote_authority_round_trip_redacts_sensitive_values() {
        let (storage, root) = setup(SyncMode::TwoWay);
        let authority = RemoteWriteAuthoritySnapshot::new(
            "remote-secret-id",
            77,
            Some("md5".into()),
            Some("0123456789abcdef0123456789abcdef".into()),
            true,
            true,
            false,
            30,
        )
        .unwrap();

        storage
            .upsert_sync_root_remote_write_authority(&root.id, &authority)
            .unwrap();
        let loaded = storage
            .sync_root_remote_write_authority(&root.id, "remote-secret-id")
            .unwrap()
            .unwrap();

        assert_eq!(loaded.remote_version, 77);
        assert!(loaded.can_edit);
        let debug = format!("{loaded:?}");
        assert!(!debug.contains("remote-secret-id"));
        assert!(!debug.contains("0123456789abcdef0123456789abcdef"));
    }

    #[test]
    fn phase5h1_create_file_intent_binds_event_generation_and_parent_authority() {
        let (mut storage, root) = setup(SyncMode::TwoWay);
        let state = storage.sync_root_local_inventory_state(&root.id).unwrap();
        storage
            .upsert_sync_root_remote_write_authority(
                &root.id,
                &RemoteWriteAuthoritySnapshot::new(
                    "parent-id",
                    5,
                    None,
                    None,
                    true,
                    false,
                    true,
                    30,
                )
                .unwrap(),
            )
            .unwrap();

        let input = RemoteWriteIntentInput::new(
            event_id(&storage, &root.id, "docs/new.txt"),
            state.generation,
            RemoteWriteIntentOperation::CreateFile,
            "docs/new.txt",
            LocalItemKind::File,
            Some(7),
            Some(200),
            Some(8),
            Some(55),
            None,
            Some("predetermined-id".into()),
            Some("parent-id".into()),
            None,
            None,
            None,
            None,
            None,
            40,
        )
        .unwrap();

        let id = storage
            .create_sync_root_remote_write_intent(&root.id, &input)
            .unwrap();
        let record = storage.sync_root_remote_write_intent(id).unwrap().unwrap();
        assert_eq!(record.operation, RemoteWriteIntentOperation::CreateFile);
        assert_eq!(record.status, RemoteWriteIntentStatus::Planned);
        assert_eq!(
            storage
                .planned_sync_root_remote_write_intent_count(&root.id)
                .unwrap(),
            1
        );

        let debug = format!("{input:?}");
        assert!(!debug.contains("docs/new.txt"));
        assert!(!debug.contains("predetermined-id"));
        assert!(!debug.contains("parent-id"));
        assert!(!format!("{record:?}").contains("docs/new.txt"));
    }

    #[test]
    fn phase5h1_update_requires_matching_remote_version() {
        let (mut storage, root) = setup(SyncMode::TwoWay);
        let state = storage.sync_root_local_inventory_state(&root.id).unwrap();
        storage
            .upsert_sync_root_remote_write_authority(
                &root.id,
                &RemoteWriteAuthoritySnapshot::new(
                    "target-id",
                    9,
                    Some("md5".into()),
                    Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into()),
                    true,
                    true,
                    false,
                    30,
                )
                .unwrap(),
            )
            .unwrap();

        let source_id = event_id(&storage, &root.id, "docs/existing.txt");

        let stale = RemoteWriteIntentInput::new(
            source_id,
            state.generation,
            RemoteWriteIntentOperation::UpdateFile,
            "docs/existing.txt",
            LocalItemKind::File,
            Some(6),
            Some(202),
            Some(8),
            Some(41),
            Some("target-id".into()),
            None,
            Some("parent-id".into()),
            Some(RemoteItemKind::File),
            Some(8),
            Some(5),
            Some("md5".into()),
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into()),
            40,
        )
        .unwrap();

        assert!(matches!(
            storage.create_sync_root_remote_write_intent(&root.id, &stale),
            Err(StorageError::RemoteWriteIntentAuthorityMismatch)
        ));

        let fresh = RemoteWriteIntentInput::new(
            source_id,
            state.generation,
            RemoteWriteIntentOperation::UpdateFile,
            "docs/existing.txt",
            LocalItemKind::File,
            Some(6),
            Some(202),
            Some(8),
            Some(41),
            Some("target-id".into()),
            None,
            Some("parent-id".into()),
            Some(RemoteItemKind::File),
            Some(9),
            Some(5),
            Some("md5".into()),
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into()),
            41,
        )
        .unwrap();

        assert!(
            storage
                .create_sync_root_remote_write_intent(&root.id, &fresh)
                .is_ok()
        );
    }

    #[test]
    fn phase5h1_receive_only_root_rejects_intent_creation() {
        let (mut storage, root) = setup(SyncMode::ReceiveOnly);
        let state = storage.sync_root_local_inventory_state(&root.id).unwrap();
        storage
            .upsert_sync_root_remote_write_authority(
                &root.id,
                &RemoteWriteAuthoritySnapshot::new(
                    "parent-id",
                    5,
                    None,
                    None,
                    true,
                    false,
                    true,
                    30,
                )
                .unwrap(),
            )
            .unwrap();

        let input = RemoteWriteIntentInput::new(
            event_id(&storage, &root.id, "docs/new.txt"),
            state.generation,
            RemoteWriteIntentOperation::CreateFile,
            "docs/new.txt",
            LocalItemKind::File,
            Some(7),
            Some(200),
            Some(8),
            Some(55),
            None,
            Some("predetermined-id".into()),
            Some("parent-id".into()),
            None,
            None,
            None,
            None,
            None,
            40,
        )
        .unwrap();

        assert!(matches!(
            storage.create_sync_root_remote_write_intent(&root.id, &input),
            Err(StorageError::RemoteWriteIntentRootNotWriteCapable)
        ));
    }
}

#[cfg(test)]
mod phase5h2_remote_write_authority_observation_tests {
    use super::*;

    fn fixture(label: &str) -> (Storage, SyncRoot) {
        let storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account =
            ProviderAccount::new(provider.clone(), format!("{label}-subject"), None, None).unwrap();
        storage.upsert_account(&account, 1).unwrap();
        let root = SyncRoot::new(
            format!("{label}-root"),
            provider,
            account.subject,
            format!("/tmp/{label}-root"),
            Some("remote-root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        storage.insert_sync_root(&root).unwrap();
        (storage, root)
    }

    #[test]
    fn phase5h2_authority_snapshot_replacement_is_atomic_and_exact() {
        let (mut storage, root) = fixture("phase5h2");
        let first = vec![
            RemoteWriteAuthoritySnapshot::new("remote-root", 10, None, None, true, true, true, 20)
                .unwrap(),
            RemoteWriteAuthoritySnapshot::new(
                "file-1",
                11,
                Some("md5".into()),
                Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into()),
                true,
                true,
                false,
                20,
            )
            .unwrap(),
        ];

        assert_eq!(
            storage
                .replace_sync_root_remote_write_authority_snapshot(&root.id, &first)
                .unwrap(),
            2
        );
        assert_eq!(
            storage
                .sync_root_remote_write_authority_count(&root.id)
                .unwrap(),
            2
        );

        let replacement = vec![
            RemoteWriteAuthoritySnapshot::new("remote-root", 12, None, None, true, true, true, 30)
                .unwrap(),
        ];
        storage
            .replace_sync_root_remote_write_authority_snapshot(&root.id, &replacement)
            .unwrap();

        assert_eq!(
            storage
                .sync_root_remote_write_authority_count(&root.id)
                .unwrap(),
            1
        );
        assert!(
            storage
                .sync_root_remote_write_authority(&root.id, "file-1")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn phase5h2_duplicate_authority_rolls_back_replacement() {
        let (mut storage, root) = fixture("phase5h2-rollback");
        let original =
            RemoteWriteAuthoritySnapshot::new("original", 5, None, None, true, true, false, 10)
                .unwrap();
        storage
            .replace_sync_root_remote_write_authority_snapshot(&root.id, &[original])
            .unwrap();

        let duplicate =
            RemoteWriteAuthoritySnapshot::new("duplicate", 6, None, None, true, true, false, 11)
                .unwrap();

        assert!(
            storage
                .replace_sync_root_remote_write_authority_snapshot(
                    &root.id,
                    &[duplicate.clone(), duplicate],
                )
                .is_err()
        );
        assert_eq!(
            storage
                .sync_root_remote_write_authority_count(&root.id)
                .unwrap(),
            1
        );
        assert!(
            storage
                .sync_root_remote_write_authority(&root.id, "original")
                .unwrap()
                .is_some()
        );
    }
}

#[cfg(test)]
mod phase5h3_remote_write_planner_storage_tests {
    use super::*;

    fn ready_root() -> (Storage, SyncRoot, ChangeCursor) {
        let mut storage = Storage::open_in_memory().unwrap();
        let provider = ProviderId::new("google-drive").unwrap();
        let account =
            ProviderAccount::new(provider.clone(), "phase5h3-subject", None, None).unwrap();
        storage.upsert_account(&account, 1).unwrap();

        let root = SyncRoot::new(
            "phase5h3-root",
            provider,
            account.subject,
            "/tmp/phase5h3-root",
            Some("remote-root".into()),
            SyncMode::ReceiveOnly,
            2,
        )
        .unwrap();
        storage.insert_sync_root(&root).unwrap();

        let item = RemoteItem {
            remote_id: "remote-file".into(),
            parent_remote_id: Some("remote-root".into()),
            name: "file.txt".into(),
            kind: RemoteItemKind::File,
            size_bytes: Some(4),
            modified_unix_ms: None,
            trashed: false,
        };

        storage
            .begin_sync_root_remote_inventory_staging(&root.id)
            .unwrap();
        storage
            .stage_sync_root_remote_inventory_items(&root.id, &[item], 3)
            .unwrap();
        let bootstrap = ChangeCursor::new("phase5h3-bootstrap").unwrap();
        storage
            .commit_sync_root_remote_inventory_snapshot(&root.id, &bootstrap, 4)
            .unwrap();

        let current = ChangeCursor::new("phase5h3-current").unwrap();
        storage
            .commit_sync_root_catalog_batch_and_cursor(&root.id, &bootstrap, &[], &current, 5)
            .unwrap();

        (storage, root, current)
    }

    #[test]
    fn phase5h3_schema_is_v16_and_authority_state_is_cursor_bound() {
        let (mut storage, root, cursor) = ready_root();
        assert_eq!(storage.schema_version().unwrap(), 16);

        let authorities = vec![
            RemoteWriteAuthoritySnapshot::new("remote-root", 10, None, None, true, true, true, 20)
                .unwrap(),
            RemoteWriteAuthoritySnapshot::new(
                "remote-file",
                11,
                Some("md5".into()),
                Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into()),
                true,
                true,
                false,
                20,
            )
            .unwrap(),
        ];

        assert_eq!(
            storage
                .commit_sync_root_remote_write_authority_snapshot(
                    &root.id,
                    &cursor,
                    &authorities,
                    20,
                )
                .unwrap(),
            2
        );

        let state = storage
            .sync_root_remote_write_authority_state(&root.id)
            .unwrap()
            .unwrap();
        assert_eq!(state.change_cursor, cursor);
        assert_eq!(state.item_count, 2);
        assert_eq!(
            storage
                .list_sync_root_remote_write_authorities(&root.id)
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn phase5h3_pending_event_records_round_trip_with_redacted_debug() {
        let (mut storage, root, _) = ready_root();

        let baseline = vec![
            LocalItemSnapshot::new("private.txt", LocalItemKind::File, Some(4), 10, 8, 9).unwrap(),
        ];
        storage
            .begin_sync_root_local_inventory_staging(&root.id)
            .unwrap();
        storage
            .stage_sync_root_local_inventory_items(&root.id, &baseline, 10)
            .unwrap();
        storage
            .commit_sync_root_local_inventory_snapshot(&root.id, 11)
            .unwrap();

        let state = storage.sync_root_local_inventory_state(&root.id).unwrap();
        storage
            .reconcile_sync_root_local_change_journal(
                &root.id,
                state.generation,
                state.item_count,
                state.snapshot_completed_at_unix_ms,
                &[LocalChangeEventInput::new(
                    "private.txt",
                    LocalChangeEventKind::Modified,
                    Some(LocalItemKind::File),
                    Some(LocalItemKind::File),
                )
                .unwrap()],
                12,
            )
            .unwrap();

        let events = storage
            .list_pending_sync_root_local_change_events(&root.id, state.generation)
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, LocalChangeEventKind::Modified);
        assert_eq!(events[0].relative_path(), "private.txt");
        assert!(!format!("{:?}", events[0]).contains("private.txt"));
    }
}
