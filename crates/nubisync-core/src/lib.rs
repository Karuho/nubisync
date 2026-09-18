//! Provider-neutral domain contracts for NubiSync.

#![forbid(unsafe_code)]

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

/// Stable identifier for a cloud provider implementation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProviderId(String);

impl ProviderId {
    pub fn new(value: impl Into<String>) -> Result<Self, CoreError> {
        let value = value.into();

        if value.is_empty() || value.len() > 64 {
            return Err(CoreError::InvalidProviderId);
        }

        if !value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
        }) {
            return Err(CoreError::InvalidProviderId);
        }

        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Provider account identity used by the local synchronization engine.
///
/// This is application state, not telemetry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderAccount {
    pub provider: ProviderId,
    pub subject: String,
    pub email: Option<String>,
    pub display_name: Option<String>,
}

impl ProviderAccount {
    pub fn new(
        provider: ProviderId,
        subject: impl Into<String>,
        email: Option<String>,
        display_name: Option<String>,
    ) -> Result<Self, CoreError> {
        let subject = subject.into();
        if subject.trim().is_empty() {
            return Err(CoreError::InvalidAccountSubject);
        }

        Ok(Self {
            provider,
            subject,
            email,
            display_name,
        })
    }
}

/// Durable provider checkpoint for an incremental remote change stream.
///
/// Cursor contents are intentionally redacted from `Debug`.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ChangeCursor(String);

impl ChangeCursor {
    pub fn new(value: impl Into<String>) -> Result<Self, CoreError> {
        let value = value.into();
        if value.is_empty() {
            return Err(CoreError::InvalidCursor);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ChangeCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ChangeCursor([redacted])")
    }
}

/// Short-lived provider pagination token.
///
/// Token contents are intentionally redacted from `Debug`.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ContinuationToken(String);

impl ContinuationToken {
    pub fn new(value: impl Into<String>) -> Result<Self, CoreError> {
        let value = value.into();
        if value.is_empty() {
            return Err(CoreError::InvalidContinuationToken);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ContinuationToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ContinuationToken([redacted])")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncMode {
    TwoWay,
    MirrorLocalToRemote,
    ReceiveOnly,
}

impl SyncMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TwoWay => "two_way",
            Self::MirrorLocalToRemote => "mirror_local_to_remote",
            Self::ReceiveOnly => "receive_only",
        }
    }

    pub fn parse(value: &str) -> Result<Self, CoreError> {
        match value {
            "two_way" => Ok(Self::TwoWay),
            "mirror_local_to_remote" => Ok(Self::MirrorLocalToRemote),
            "receive_only" => Ok(Self::ReceiveOnly),
            _ => Err(CoreError::InvalidSyncMode),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncRoot {
    pub id: String,
    pub provider: ProviderId,
    pub account_subject: String,
    pub local_path: String,
    pub remote_root_id: Option<String>,
    pub mode: SyncMode,
    pub created_at_unix_ms: i64,
}

impl SyncRoot {
    pub fn new(
        id: impl Into<String>,
        provider: ProviderId,
        account_subject: impl Into<String>,
        local_path: impl Into<String>,
        remote_root_id: Option<String>,
        mode: SyncMode,
        created_at_unix_ms: i64,
    ) -> Result<Self, CoreError> {
        let id = id.into();
        let account_subject = account_subject.into();
        let local_path = local_path.into();

        if id.trim().is_empty() || id.len() > 128 {
            return Err(CoreError::InvalidSyncRootId);
        }

        if account_subject.trim().is_empty() {
            return Err(CoreError::InvalidAccountSubject);
        }

        if local_path.trim().is_empty() {
            return Err(CoreError::InvalidSyncRootLocalPath);
        }

        if remote_root_id
            .as_ref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err(CoreError::InvalidRemoteRootId);
        }

        Ok(Self {
            id,
            provider,
            account_subject,
            local_path,
            remote_root_id,
            mode,
            created_at_unix_ms,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LocalItemKind {
    File,
    Directory,
}

impl LocalItemKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Directory => "directory",
        }
    }

    pub fn parse(value: &str) -> Result<Self, CoreError> {
        match value {
            "file" => Ok(Self::File),
            "directory" => Ok(Self::Directory),
            _ => Err(CoreError::InvalidLocalItemKind),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalItemSnapshot {
    relative_path: String,
    kind: LocalItemKind,
    size_bytes: Option<u64>,
    modified_unix_ns: i64,
    device_id: u64,
    inode: u64,
}

impl LocalItemSnapshot {
    pub fn new(
        relative_path: impl Into<String>,
        kind: LocalItemKind,
        size_bytes: Option<u64>,
        modified_unix_ns: i64,
        device_id: u64,
        inode: u64,
    ) -> Result<Self, CoreError> {
        let relative_path = relative_path.into();
        validate_local_snapshot_relative_path(&relative_path)?;

        match kind {
            LocalItemKind::File if size_bytes.is_none() => {
                return Err(CoreError::InvalidLocalItemSize);
            }
            LocalItemKind::Directory if size_bytes.is_some() => {
                return Err(CoreError::InvalidLocalItemSize);
            }
            LocalItemKind::File | LocalItemKind::Directory => {}
        }

        Ok(Self {
            relative_path,
            kind,
            size_bytes,
            modified_unix_ns,
            device_id,
            inode,
        })
    }

    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }

    pub fn kind(&self) -> LocalItemKind {
        self.kind
    }

    pub fn size_bytes(&self) -> Option<u64> {
        self.size_bytes
    }

    pub fn modified_unix_ns(&self) -> i64 {
        self.modified_unix_ns
    }

    pub fn device_id(&self) -> u64 {
        self.device_id
    }

    pub fn inode(&self) -> u64 {
        self.inode
    }
}

impl fmt::Debug for LocalItemSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalItemSnapshot")
            .field("relative_path", &"[redacted]")
            .field("kind", &self.kind)
            .field("size_bytes", &self.size_bytes)
            .field("modified_unix_ns", &self.modified_unix_ns)
            .field("device_id", &self.device_id)
            .field("inode", &self.inode)
            .finish()
    }
}

fn validate_local_snapshot_relative_path(relative_path: &str) -> Result<(), CoreError> {
    if relative_path.is_empty()
        || relative_path.starts_with('/')
        || relative_path.ends_with('/')
        || relative_path.contains('\0')
    {
        return Err(CoreError::InvalidLocalItemRelativePath);
    }

    for component in relative_path.split('/') {
        if component.is_empty() || matches!(component, "." | "..") {
            return Err(CoreError::InvalidLocalItemRelativePath);
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RemoteItemKind {
    File,
    Folder,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteItem {
    pub remote_id: String,
    pub parent_remote_id: Option<String>,
    pub name: String,
    pub kind: RemoteItemKind,
    pub size_bytes: Option<u64>,
    pub modified_unix_ms: Option<i64>,
    pub trashed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RemoteChange {
    Upsert(RemoteItem),
    Delete { remote_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangePage {
    pub changes: Vec<RemoteChange>,
    pub continuation: Option<ContinuationToken>,
    pub checkpoint: Option<ChangeCursor>,
}

/// Minimum provider interface needed for incremental synchronization.
///
/// Transfer operations will be added only when their semantics are specified
/// and covered by tests.
#[async_trait]
pub trait CloudProvider: Send + Sync {
    fn provider_id(&self) -> &ProviderId;

    async fn account(&self) -> Result<ProviderAccount, ProviderError>;

    async fn initial_change_cursor(&self) -> Result<ChangeCursor, ProviderError>;

    async fn list_changes(
        &self,
        cursor: &ChangeCursor,
        continuation: Option<&ContinuationToken>,
    ) -> Result<ChangePage, ProviderError>;
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CoreError {
    #[error("provider id is invalid")]
    InvalidProviderId,
    #[error("provider account subject is invalid")]
    InvalidAccountSubject,
    #[error("change cursor is invalid")]
    InvalidCursor,
    #[error("continuation token is invalid")]
    InvalidContinuationToken,
    #[error("sync mode is invalid")]
    InvalidSyncMode,
    #[error("sync root id is invalid")]
    InvalidSyncRootId,
    #[error("sync root local path is invalid")]
    InvalidSyncRootLocalPath,
    #[error("sync root remote id is invalid")]
    InvalidRemoteRootId,
    #[error("local snapshot relative path is invalid")]
    InvalidLocalItemRelativePath,
    #[error("local snapshot item kind is invalid")]
    InvalidLocalItemKind,
    #[error("local snapshot item size is invalid for its kind")]
    InvalidLocalItemSize,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ProviderError {
    #[error("provider authentication is required")]
    AuthenticationRequired,
    #[error("provider permission was denied")]
    PermissionDenied,
    #[error("provider rate limit reached")]
    RateLimited { retry_after_seconds: Option<u64> },
    #[error("temporary provider failure: {code}")]
    Temporary { code: String },
    #[error("invalid provider response: {code}")]
    InvalidResponse { code: String },
    #[error("provider feature is unsupported: {feature}")]
    Unsupported { feature: &'static str },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_id_rejects_unsafe_shapes() {
        assert!(ProviderId::new("google-drive").is_ok());
        assert_eq!(
            ProviderId::new("Google Drive"),
            Err(CoreError::InvalidProviderId)
        );
    }

    #[test]
    fn sync_mode_storage_names_round_trip() {
        for mode in [
            SyncMode::TwoWay,
            SyncMode::MirrorLocalToRemote,
            SyncMode::ReceiveOnly,
        ] {
            assert_eq!(SyncMode::parse(mode.as_str()).unwrap(), mode);
        }
        assert_eq!(SyncMode::parse("invalid"), Err(CoreError::InvalidSyncMode));
    }

    #[test]
    fn sync_root_rejects_empty_local_path_and_remote_id() {
        let provider = ProviderId::new("google-drive").unwrap();

        assert_eq!(
            SyncRoot::new(
                "root-1",
                provider.clone(),
                "subject",
                "   ",
                Some("remote".into()),
                SyncMode::ReceiveOnly,
                1,
            ),
            Err(CoreError::InvalidSyncRootLocalPath)
        );

        assert_eq!(
            SyncRoot::new(
                "root-1",
                provider,
                "subject",
                "/tmp/nubisync",
                Some("   ".into()),
                SyncMode::ReceiveOnly,
                1,
            ),
            Err(CoreError::InvalidRemoteRootId)
        );
    }

    #[test]
    fn change_cursor_debug_is_redacted() {
        let cursor = ChangeCursor::new("secret-ish-provider-token").unwrap();
        assert_eq!(format!("{cursor:?}"), "ChangeCursor([redacted])");
        assert!(!format!("{cursor:?}").contains(cursor.as_str()));
    }
}

#[cfg(test)]
mod phase5f1_local_snapshot_tests {
    use super::*;

    #[test]
    fn phase5f1_local_snapshot_validates_shape_and_redacts_path() {
        let file = LocalItemSnapshot::new(
            "docs/private.txt",
            LocalItemKind::File,
            Some(12),
            123,
            8,
            42,
        )
        .unwrap();

        assert_eq!(file.kind(), LocalItemKind::File);
        assert_eq!(file.size_bytes(), Some(12));
        assert_eq!(file.relative_path(), "docs/private.txt");

        let debug = format!("{file:?}");
        assert!(!debug.contains("docs/private.txt"));
        assert!(debug.contains("[redacted]"));

        assert!(
            LocalItemSnapshot::new("../escape", LocalItemKind::File, Some(1), 1, 1, 1,).is_err()
        );

        assert!(
            LocalItemSnapshot::new("directory", LocalItemKind::Directory, Some(1), 1, 1, 1,)
                .is_err()
        );

        assert!(LocalItemSnapshot::new("file", LocalItemKind::File, None, 1, 1, 1,).is_err());
    }

    #[test]
    fn phase5f1_local_item_kind_storage_names_round_trip() {
        for kind in [LocalItemKind::File, LocalItemKind::Directory] {
            assert_eq!(LocalItemKind::parse(kind.as_str()).unwrap(), kind);
        }
        assert!(LocalItemKind::parse("other").is_err());
    }
}
