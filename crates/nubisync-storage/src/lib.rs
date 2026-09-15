//! Transactional local metadata storage for NubiSync.

#![forbid(unsafe_code)]

use nubisync_core::{
    ChangeCursor, ProviderAccount, ProviderId, RemoteChange, RemoteItem, RemoteItemKind, SyncMode,
    SyncRoot,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use std::path::Path;
use thiserror::Error;

const SCHEMA_VERSION: i64 = 6;

type RemoteInventoryStateRow = (i64, i64, i64, Option<i64>, Option<String>);

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
                FOREIGN KEY (sync_root_id)
                    REFERENCES sync_roots(id) ON DELETE CASCADE
            );
            ",
        )?;

        if current_version == 4 {
            transaction.execute(
                "ALTER TABLE remote_inventory_state ADD COLUMN catchup_from_cursor TEXT",
                [],
            )?;
        }

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
                catchup_from_cursor
             ) VALUES (?1, 1, 0, ?2, ?3, ?4)
             ON CONFLICT(sync_root_id) DO UPDATE SET
                snapshot_complete = 1,
                catchup_complete = 0,
                item_count = excluded.item_count,
                snapshot_completed_at_unix_ms = excluded.snapshot_completed_at_unix_ms,
                catchup_from_cursor = excluded.catchup_from_cursor",
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
    #[error("SQLite operation failed")]
    Sqlite(#[from] rusqlite::Error),
    #[error("stored NubiSync domain value is invalid")]
    Core(#[from] nubisync_core::CoreError),
    #[error("numeric value does not fit SQLite storage")]
    NumericOverflow,
    #[error("internal inventory table selection is invalid")]
    InvalidInternalTable,
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
        assert_eq!(storage.schema_version().unwrap(), 6);

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
