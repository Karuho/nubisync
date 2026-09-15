//! Transactional local metadata storage for NubiSync.

#![forbid(unsafe_code)]

use nubisync_core::{
    ChangeCursor, ProviderAccount, ProviderId, RemoteChange, RemoteItem, RemoteItemKind,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use std::path::Path;
use thiserror::Error;

const SCHEMA_VERSION: i64 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemoteInventoryState {
    pub snapshot_complete: bool,
    pub catchup_complete: bool,
    pub item_count: u64,
    pub snapshot_completed_at_unix_ms: Option<i64>,
}

impl RemoteInventoryState {
    pub fn ready_for_reconciliation(self) -> bool {
        self.snapshot_complete && self.catchup_complete
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
                PRIMARY KEY (provider, account_subject),
                FOREIGN KEY (provider, account_subject)
                    REFERENCES accounts(provider, subject) ON DELETE CASCADE
            );
            ",
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
                snapshot_completed_at_unix_ms
             ) VALUES (?1, ?2, 1, 0, ?3, ?4)
             ON CONFLICT(provider, account_subject) DO UPDATE SET
                snapshot_complete = 1,
                catchup_complete = 0,
                item_count = excluded.item_count,
                snapshot_completed_at_unix_ms = excluded.snapshot_completed_at_unix_ms",
            params![
                provider.as_str(),
                account_subject,
                item_count,
                completed_at_unix_ms
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
        let row: Option<(i64, i64, i64, Option<i64>)> = self
            .connection
            .query_row(
                "SELECT
                    snapshot_complete,
                    catchup_complete,
                    item_count,
                    snapshot_completed_at_unix_ms
                 FROM remote_inventory_state
                 WHERE provider = ?1 AND account_subject = ?2",
                params![provider.as_str(), account_subject],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;

        match row {
            Some((snapshot_complete, catchup_complete, item_count, completed_at)) => {
                Ok(RemoteInventoryState {
                    snapshot_complete: snapshot_complete != 0,
                    catchup_complete: catchup_complete != 0,
                    item_count: u64::try_from(item_count)
                        .map_err(|_| StorageError::NumericOverflow)?,
                    snapshot_completed_at_unix_ms: completed_at,
                })
            }
            None => Ok(RemoteInventoryState {
                snapshot_complete: false,
                catchup_complete: false,
                item_count: 0,
                snapshot_completed_at_unix_ms: None,
            }),
        }
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
                .commit_remote_inventory_snapshot(&provider, &account.subject, 3)
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
            .commit_remote_inventory_snapshot(&provider, &account.subject, 3)
            .unwrap();

        let state = storage
            .remote_inventory_state(&provider, &account.subject)
            .unwrap();

        assert!(state.snapshot_complete);
        assert!(!state.catchup_complete);
        assert_eq!(state.item_count, 1);
        assert_eq!(state.snapshot_completed_at_unix_ms, Some(3));
        assert!(!state.ready_for_reconciliation());
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
