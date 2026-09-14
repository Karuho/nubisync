//! Typed, privacy-constrained telemetry schema.
//!
//! The schema deliberately avoids arbitrary metadata maps in order to reduce
//! accidental collection of paths, filenames or provider identifiers.

#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

pub const TELEMETRY_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TelemetryLevel {
    Off,
    Basic,
    Enhanced,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiagnosticCode(String);

impl DiagnosticCode {
    pub fn new(value: impl Into<String>) -> Result<Self, TelemetryError> {
        let value = value.into();

        if value.is_empty()
            || value.len() > 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(TelemetryError::InvalidDiagnosticCode);
        }

        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DurationBucket {
    UnderOneSecond,
    OneToFiveSeconds,
    FiveToThirtySeconds,
    ThirtySecondsToFiveMinutes,
    OverFiveMinutes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CountBucket {
    Zero,
    OneToTen,
    ElevenToOneHundred,
    OneHundredOneToOneThousand,
    OverOneThousand,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TelemetryEvent {
    SyncCompleted {
        duration: DurationBucket,
        items: CountBucket,
    },
    SyncFailed {
        code: DiagnosticCode,
    },
    ConflictDetected,
    ProviderRateLimited,
    RecoveryCompleted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelemetryEnvelope {
    pub schema_version: u16,
    pub installation_id: Uuid,
    pub app_version: String,
    pub event: TelemetryEvent,
}

impl TelemetryEnvelope {
    pub fn new(
        installation_id: Uuid,
        app_version: impl Into<String>,
        event: TelemetryEvent,
    ) -> Self {
        Self {
            schema_version: TELEMETRY_SCHEMA_VERSION,
            installation_id,
            app_version: app_version.into(),
            event,
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TelemetryError {
    #[error("diagnostic code must contain only A-Z, 0-9 and underscore")]
    InvalidDiagnosticCode,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostic_code_rejects_free_form_sensitive_text() {
        assert!(DiagnosticCode::new("DRIVE_RATE_LIMITED").is_ok());
        assert_eq!(
            DiagnosticCode::new("/home/user/private/file.txt"),
            Err(TelemetryError::InvalidDiagnosticCode)
        );
        assert_eq!(
            DiagnosticCode::new("user@example.com"),
            Err(TelemetryError::InvalidDiagnosticCode)
        );
    }
}
