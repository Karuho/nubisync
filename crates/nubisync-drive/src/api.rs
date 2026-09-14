use crate::OAuthAccessToken;
use nubisync_core::{
    ChangeCursor, ChangePage, ContinuationToken, RemoteChange, RemoteItem, RemoteItemKind,
};
use reqwest::blocking::Client;
use serde::Deserialize;
use thiserror::Error;

const GOOGLE_USERINFO_ENDPOINT: &str = "https://openidconnect.googleapis.com/v1/userinfo";
const GOOGLE_DRIVE_ABOUT_ENDPOINT: &str = "https://www.googleapis.com/drive/v3/about";
const GOOGLE_DRIVE_START_PAGE_TOKEN_ENDPOINT: &str =
    "https://www.googleapis.com/drive/v3/changes/startPageToken";
const GOOGLE_DRIVE_CHANGES_ENDPOINT: &str = "https://www.googleapis.com/drive/v3/changes";
const GOOGLE_DRIVE_FOLDER_MIME_TYPE: &str = "application/vnd.google-apps.folder";
const GOOGLE_DRIVE_CHANGES_FIELDS: &str = concat!(
    "nextPageToken,newStartPageToken,",
    "changes(changeType,fileId,removed,",
    "file(id,name,mimeType,parents,size,trashed))"
);

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

    /// Reads one page of the user's My Drive change stream.
    ///
    /// The caller supplies the durable cursor for the first page and the
    /// short-lived continuation token for subsequent pages. This method is
    /// metadata-only and performs no Drive write or file-content request.
    pub fn list_changes_page(
        &self,
        cursor: &ChangeCursor,
        continuation: Option<&ContinuationToken>,
    ) -> Result<ChangePage, DriveApiError> {
        let page_token = continuation
            .map(ContinuationToken::as_str)
            .unwrap_or_else(|| cursor.as_str());

        let response: ChangeListResponse = self
            .client
            .get(GOOGLE_DRIVE_CHANGES_ENDPOINT)
            .bearer_auth(self.access_token.as_str())
            .query(&[
                ("pageToken", page_token),
                ("pageSize", "100"),
                ("spaces", "drive"),
                ("includeRemoved", "true"),
                ("restrictToMyDrive", "true"),
                ("fields", GOOGLE_DRIVE_CHANGES_FIELDS),
            ])
            .send()?
            .error_for_status()?
            .json()?;

        response.into_change_page()
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

#[derive(Debug, Deserialize)]
struct ChangeListResponse {
    #[serde(rename = "nextPageToken")]
    next_page_token: Option<String>,
    #[serde(rename = "newStartPageToken")]
    new_start_page_token: Option<String>,
    #[serde(default)]
    changes: Vec<GoogleChange>,
}

impl ChangeListResponse {
    fn into_change_page(self) -> Result<ChangePage, DriveApiError> {
        let continuation = self
            .next_page_token
            .map(ContinuationToken::new)
            .transpose()?;
        let checkpoint = self
            .new_start_page_token
            .map(ChangeCursor::new)
            .transpose()?;

        match (&continuation, &checkpoint) {
            (Some(_), None) | (None, Some(_)) => {}
            (Some(_), Some(_)) => {
                return Err(DriveApiError::InvalidChangePage {
                    code: "both_page_and_checkpoint_tokens",
                });
            }
            (None, None) => {
                return Err(DriveApiError::InvalidChangePage {
                    code: "missing_page_or_checkpoint_token",
                });
            }
        }

        let changes = self
            .changes
            .into_iter()
            .map(GoogleChange::into_remote_change)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(ChangePage {
            changes,
            continuation,
            checkpoint,
        })
    }
}

#[derive(Debug, Deserialize)]
struct GoogleChange {
    #[serde(rename = "changeType")]
    change_type: String,
    #[serde(rename = "fileId")]
    file_id: String,
    #[serde(default)]
    removed: bool,
    file: Option<GoogleFile>,
}

impl GoogleChange {
    fn into_remote_change(self) -> Result<RemoteChange, DriveApiError> {
        if self.change_type != "file" {
            return Err(DriveApiError::InvalidChangePage {
                code: "non_file_change",
            });
        }

        if self.file_id.trim().is_empty() {
            return Err(DriveApiError::InvalidChangePage {
                code: "empty_file_id",
            });
        }

        if self.removed {
            return Ok(RemoteChange::Delete {
                remote_id: self.file_id,
            });
        }

        let file = self.file.ok_or(DriveApiError::InvalidChangePage {
            code: "missing_file_metadata",
        })?;

        if file.id != self.file_id {
            return Err(DriveApiError::InvalidChangePage {
                code: "file_id_mismatch",
            });
        }

        if file.name.is_empty() || file.mime_type.is_empty() {
            return Err(DriveApiError::InvalidChangePage {
                code: "missing_file_identity_metadata",
            });
        }

        let size_bytes = parse_optional_u64(file.size.as_deref(), "file.size")?;
        let parent_remote_id = file.parents.into_iter().next();
        let kind = if file.mime_type == GOOGLE_DRIVE_FOLDER_MIME_TYPE {
            RemoteItemKind::Folder
        } else {
            RemoteItemKind::File
        };

        Ok(RemoteChange::Upsert(RemoteItem {
            remote_id: file.id,
            parent_remote_id,
            name: file.name,
            kind,
            size_bytes,
            modified_unix_ms: None,
            trashed: file.trashed,
        }))
    }
}

#[derive(Debug, Deserialize)]
struct GoogleFile {
    id: String,
    name: String,
    #[serde(rename = "mimeType")]
    mime_type: String,
    #[serde(default)]
    parents: Vec<String>,
    size: Option<String>,
    #[serde(default)]
    trashed: bool,
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
    #[error("Google Drive change page is invalid: {code}")]
    InvalidChangePage { code: &'static str },
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

    #[test]
    fn final_change_page_maps_file_and_removed_entry() {
        let response = ChangeListResponse {
            next_page_token: None,
            new_start_page_token: Some("checkpoint-2".into()),
            changes: vec![
                GoogleChange {
                    change_type: "file".into(),
                    file_id: "folder-1".into(),
                    removed: false,
                    file: Some(GoogleFile {
                        id: "folder-1".into(),
                        name: "Folder".into(),
                        mime_type: GOOGLE_DRIVE_FOLDER_MIME_TYPE.into(),
                        parents: vec!["root".into()],
                        size: None,
                        trashed: false,
                    }),
                },
                GoogleChange {
                    change_type: "file".into(),
                    file_id: "removed-1".into(),
                    removed: true,
                    file: None,
                },
            ],
        };

        let page = response.into_change_page().unwrap();

        assert!(page.continuation.is_none());
        assert_eq!(page.checkpoint.unwrap().as_str(), "checkpoint-2");
        assert_eq!(page.changes.len(), 2);

        match &page.changes[0] {
            RemoteChange::Upsert(item) => {
                assert_eq!(item.remote_id, "folder-1");
                assert_eq!(item.kind, RemoteItemKind::Folder);
                assert!(!item.trashed);
            }
            RemoteChange::Delete { .. } => panic!("expected upsert"),
        }

        assert!(matches!(
            &page.changes[1],
            RemoteChange::Delete { remote_id } if remote_id == "removed-1"
        ));
    }

    #[test]
    fn intermediate_change_page_uses_redacted_continuation_token() {
        let response = ChangeListResponse {
            next_page_token: Some("opaque-next-page".into()),
            new_start_page_token: None,
            changes: Vec::new(),
        };

        let page = response.into_change_page().unwrap();
        let continuation = page.continuation.unwrap();

        assert_eq!(format!("{continuation:?}"), "ContinuationToken([redacted])");
        assert!(page.checkpoint.is_none());
    }

    #[test]
    fn change_page_rejects_ambiguous_checkpoint_state() {
        let response = ChangeListResponse {
            next_page_token: Some("opaque-next-page".into()),
            new_start_page_token: Some("checkpoint".into()),
            changes: Vec::new(),
        };

        assert!(matches!(
            response.into_change_page(),
            Err(DriveApiError::InvalidChangePage {
                code: "both_page_and_checkpoint_tokens"
            })
        ));
    }
}
