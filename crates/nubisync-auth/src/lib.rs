//! Secret-storage boundary for OAuth credentials.
//!
//! Production credentials belong in the operating-system credential store.
//! SQLite and telemetry are not credential stores.

#![forbid(unsafe_code)]

use keyring::Entry;
use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, RwLock},
};
use thiserror::Error;

pub const NUBISYNC_KEYRING_SERVICE: &str = "dev.dynadev.nubisync";

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

        if [&key.provider, &key.account_subject, &key.purpose]
            .iter()
            .any(|value| value.contains('\0'))
        {
            return Err(SecretStoreError::InvalidKey);
        }

        Ok(key)
    }

    fn storage_username(&self) -> String {
        format!(
            "{}:{}:{}",
            self.provider, self.account_subject, self.purpose
        )
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
    #[error("secret store service name is invalid")]
    InvalidService,
    #[error("secret store lock is poisoned")]
    Poisoned,
    #[error("operating-system credential store is unavailable")]
    BackendUnavailable,
    #[error("operating-system credential operation failed")]
    BackendFailure,
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

/// OS-backed credential storage used by official desktop builds.
///
/// On Linux, keyring-rs selects the Secret Service backend for its v1 API.
#[derive(Debug, Clone)]
pub struct KeyringSecretStore {
    service: String,
}

impl Default for KeyringSecretStore {
    fn default() -> Self {
        Self {
            service: NUBISYNC_KEYRING_SERVICE.to_owned(),
        }
    }
}

impl KeyringSecretStore {
    pub fn new(service: impl Into<String>) -> Result<Self, SecretStoreError> {
        let service = service.into();
        if service.trim().is_empty() || service.contains('\0') {
            return Err(SecretStoreError::InvalidService);
        }
        Ok(Self { service })
    }

    pub fn is_available() -> bool {
        Entry::store_status().is_ok()
    }

    fn entry(&self, key: &SecretKey) -> Result<Entry, SecretStoreError> {
        Entry::new(&self.service, &key.storage_username()).map_err(map_keyring_error)
    }
}

impl SecretStore for KeyringSecretStore {
    fn put(&self, key: &SecretKey, value: SecretValue) -> Result<(), SecretStoreError> {
        self.entry(key)?
            .set_secret(value.expose_bytes())
            .map_err(map_keyring_error)
    }

    fn get(&self, key: &SecretKey) -> Result<Option<SecretValue>, SecretStoreError> {
        match self.entry(key)?.get_secret() {
            Ok(secret) => SecretValue::new(secret).map(Some),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(error) => Err(map_keyring_error(error)),
        }
    }

    fn delete(&self, key: &SecretKey) -> Result<(), SecretStoreError> {
        match self.entry(key)?.delete_credential() {
            Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(error) => Err(map_keyring_error(error)),
        }
    }
}

fn map_keyring_error(error: keyring::Error) -> SecretStoreError {
    match error {
        keyring::Error::NoDefaultStore | keyring::Error::NoStorageAccess(_) => {
            SecretStoreError::BackendUnavailable
        }
        _ => SecretStoreError::BackendFailure,
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

    #[test]
    fn storage_username_is_namespaced() {
        let key = SecretKey::new("google-drive", "12345", "refresh-token").unwrap();
        assert_eq!(key.storage_username(), "google-drive:12345:refresh-token");
    }
}
