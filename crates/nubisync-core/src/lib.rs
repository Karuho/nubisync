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
    fn change_cursor_debug_is_redacted() {
        let cursor = ChangeCursor::new("secret-ish-provider-token").unwrap();
        assert_eq!(format!("{cursor:?}"), "ChangeCursor([redacted])");
        assert!(!format!("{cursor:?}").contains(cursor.as_str()));
    }
}
