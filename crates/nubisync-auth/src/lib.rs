//! Secret-storage boundary for OAuth credentials.
//!
//! Phase 1 provides only the interface and an in-memory test implementation.
//! A Linux Secret Service/libsecret implementation will follow later.

#![forbid(unsafe_code)]

use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, RwLock},
};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SecretKey {
    pub provider: String,
    pub account_subject: String,
    pub purpose: String,
}

impl SecretKey {
    pub fn new(
        provider: impl Into<String>,
        account_subject: impl Into<String>,
        purpose: impl Into<String>,
    ) -> Result<Self, SecretStoreError> {
        let key = Self {
            provider: provider.into(),
            account_subject: account_subject.into(),
            purpose: purpose.into(),
        };

        if key.provider.trim().is_empty()
            || key.account_subject.trim().is_empty()
            || key.purpose.trim().is_empty()
        {
            return Err(SecretStoreError::InvalidKey);
        }

        Ok(key)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct SecretValue(Vec<u8>);

impl SecretValue {
    pub fn new(value: impl Into<Vec<u8>>) -> Result<Self, SecretStoreError> {
        let value = value.into();
        if value.is_empty() {
            return Err(SecretStoreError::EmptySecret);
        }
        Ok(Self(value))
    }

    pub fn expose_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for SecretValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretValue([redacted])")
    }
}

pub trait SecretStore: Send + Sync {
    fn put(&self, key: &SecretKey, value: SecretValue) -> Result<(), SecretStoreError>;
    fn get(&self, key: &SecretKey) -> Result<Option<SecretValue>, SecretStoreError>;
    fn delete(&self, key: &SecretKey) -> Result<(), SecretStoreError>;
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SecretStoreError {
    #[error("secret key is invalid")]
    InvalidKey,
    #[error("secret value must not be empty")]
    EmptySecret,
    #[error("secret store lock is poisoned")]
    Poisoned,
}

#[derive(Clone, Default)]
pub struct MemorySecretStore {
    inner: Arc<RwLock<HashMap<SecretKey, SecretValue>>>,
}

impl SecretStore for MemorySecretStore {
    fn put(&self, key: &SecretKey, value: SecretValue) -> Result<(), SecretStoreError> {
        let mut guard = self.inner.write().map_err(|_| SecretStoreError::Poisoned)?;
        guard.insert(key.clone(), value);
        Ok(())
    }

    fn get(&self, key: &SecretKey) -> Result<Option<SecretValue>, SecretStoreError> {
        let guard = self.inner.read().map_err(|_| SecretStoreError::Poisoned)?;
        Ok(guard.get(key).cloned())
    }

    fn delete(&self, key: &SecretKey) -> Result<(), SecretStoreError> {
        let mut guard = self.inner.write().map_err(|_| SecretStoreError::Poisoned)?;
        guard.remove(key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_values_are_redacted_from_debug() {
        let secret = SecretValue::new(b"refresh-token".to_vec()).unwrap();
        assert_eq!(format!("{secret:?}"), "SecretValue([redacted])");
    }

    #[test]
    fn memory_store_round_trip() {
        let store = MemorySecretStore::default();
        let key = SecretKey::new("google-drive", "subject-1", "refresh-token").unwrap();
        let value = SecretValue::new(b"test-only-token".to_vec()).unwrap();

        store.put(&key, value.clone()).unwrap();
        assert_eq!(store.get(&key).unwrap(), Some(value));
        store.delete(&key).unwrap();
        assert_eq!(store.get(&key).unwrap(), None);
    }
}
