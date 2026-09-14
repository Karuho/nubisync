use crate::OAuthAccessToken;
use nubisync_core::ChangeCursor;
use reqwest::blocking::Client;
use serde::Deserialize;
use thiserror::Error;

const GOOGLE_USERINFO_ENDPOINT: &str = "https://openidconnect.googleapis.com/v1/userinfo";
const GOOGLE_DRIVE_ABOUT_ENDPOINT: &str = "https://www.googleapis.com/drive/v3/about";
const GOOGLE_DRIVE_START_PAGE_TOKEN_ENDPOINT: &str =
    "https://www.googleapis.com/drive/v3/changes/startPageToken";

pub struct GoogleDriveApi {
    client: Client,
    access_token: OAuthAccessToken,
}

impl GoogleDriveApi {
    pub fn new(access_token: OAuthAccessToken) -> Result<Self, DriveApiError> {
        let client = Client::builder()
            .user_agent(concat!("NubiSync/", env!("CARGO_PKG_VERSION")))
            .build()?;

        Ok(Self {
            client,
            access_token,
        })
    }

    pub fn user_info(&self) -> Result<GoogleUserInfo, DriveApiError> {
        let response = self
            .client
            .get(GOOGLE_USERINFO_ENDPOINT)
            .bearer_auth(self.access_token.as_str())
            .send()?
            .error_for_status()?;

        Ok(response.json()?)
    }

    /// Performs a non-destructive metadata-only Drive probe.
    ///
    /// This does not list filenames and performs no write operation.
    pub fn probe(&self) -> Result<DriveProbe, DriveApiError> {
        let about: AboutResponse = self
            .client
            .get(GOOGLE_DRIVE_ABOUT_ENDPOINT)
            .bearer_auth(self.access_token.as_str())
            .query(&[(
                "fields",
                "user(displayName,emailAddress),storageQuota(limit,usageInDrive),maxUploadSize",
            )])
            .send()?
            .error_for_status()?
            .json()?;

        let token_response: StartPageTokenResponse = self
            .client
            .get(GOOGLE_DRIVE_START_PAGE_TOKEN_ENDPOINT)
            .bearer_auth(self.access_token.as_str())
            .send()?
            .error_for_status()?
            .json()?;

        let change_cursor = ChangeCursor::new(token_response.start_page_token)?;

        let storage_quota = about.storage_quota.as_ref();

        Ok(DriveProbe {
            display_name: about.user.display_name,
            email_address: about.user.email_address,
            storage_limit_bytes: parse_optional_u64(
                storage_quota.and_then(|quota| quota.limit.as_deref()),
                "storageQuota.limit",
            )?,
            usage_in_drive_bytes: parse_optional_u64(
                storage_quota.and_then(|quota| quota.usage_in_drive.as_deref()),
                "storageQuota.usageInDrive",
            )?,
            max_upload_size_bytes: parse_optional_u64(
                about.max_upload_size.as_deref(),
                "maxUploadSize",
            )?,
            change_cursor,
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct GoogleUserInfo {
    pub sub: String,
    pub email: Option<String>,
    pub name: Option<String>,
    pub locale: Option<String>,
}

#[derive(Debug)]
pub struct DriveProbe {
    pub display_name: Option<String>,
    pub email_address: Option<String>,
    pub storage_limit_bytes: Option<u64>,
    pub usage_in_drive_bytes: Option<u64>,
    pub max_upload_size_bytes: Option<u64>,
    pub change_cursor: ChangeCursor,
}

#[derive(Debug, Deserialize)]
struct AboutResponse {
    user: AboutUser,
    #[serde(rename = "storageQuota")]
    storage_quota: Option<StorageQuota>,
    #[serde(rename = "maxUploadSize")]
    max_upload_size: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AboutUser {
    #[serde(rename = "displayName")]
    display_name: Option<String>,
    #[serde(rename = "emailAddress")]
    email_address: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StorageQuota {
    limit: Option<String>,
    #[serde(rename = "usageInDrive")]
    usage_in_drive: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StartPageTokenResponse {
    #[serde(rename = "startPageToken")]
    start_page_token: String,
}

fn parse_optional_u64(
    value: Option<&str>,
    field: &'static str,
) -> Result<Option<u64>, DriveApiError> {
    value
        .map(|raw| {
            raw.parse::<u64>()
                .map_err(|_| DriveApiError::InvalidNumericField { field })
        })
        .transpose()
}

#[derive(Debug, Error)]
pub enum DriveApiError {
    #[error("Google Drive HTTP request failed")]
    Http(#[from] reqwest::Error),
    #[error("Google Drive response contained an invalid NubiSync domain value")]
    Core(#[from] nubisync_core::CoreError),
    #[error("Google Drive numeric field is invalid: {field}")]
    InvalidNumericField { field: &'static str },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn optional_numeric_fields_parse_without_guessing() {
        assert_eq!(
            parse_optional_u64(Some("12345"), "test").unwrap(),
            Some(12345)
        );
        assert_eq!(parse_optional_u64(None, "test").unwrap(), None);
        assert!(matches!(
            parse_optional_u64(Some("not-a-number"), "test"),
            Err(DriveApiError::InvalidNumericField { field: "test" })
        ));
    }
}
