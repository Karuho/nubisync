//! Transactional local metadata storage for NubiSync.

#![forbid(unsafe_code)]

use nubisync_core::{
    ChangeCursor, ContinuationToken, ProviderAccount, ProviderId, RemoteChange, RemoteItem,
    RemoteItemKind, SyncMode, SyncRoot,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use std::path::Path;
use thiserror::Error;

const SCHEMA_VERSION: i64 = 10;

type RemoteInventoryStateRow = (i64, i64, i64, Option<i64>, Option<String>);
type SyncRootRemoteItemRow = (Option<String>, String, String, Option<i64>, i64);
type SyncRootRemoteCatalogRow = (String, Option<String>, String, String, Option<i64>, i64);
type SyncRootCursorStateRow = (i64, i64, Option<String>, Option<String>);
type SyncRootChangeWindowStateRow = (String, Option<String>, Option<String>, i64, i64);

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
    Ok(())
}

fn upsert_sync_root_catalog_item(
    transaction: &Transaction<'_>,
    sync_root_id: &str,
    item: &RemoteItem,
    observed_at_unix_ms: i64,
) -> Result<(), StorageError> {
    invalidate_sync_root_materialization_subtree(transaction, sync_root_id, &item.remote_id)?;

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
    #[error("materialization receipt remote item is missing")]
    MaterializationReceiptRemoteItemMissing,
    #[error("materialization receipt does not match durable remote file metadata")]
    MaterializationReceiptRemoteItemMismatch,
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
