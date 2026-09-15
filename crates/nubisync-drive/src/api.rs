use crate::OAuthAccessToken;
use nubisync_core::{
    ChangeCursor, ChangePage, ContinuationToken, RemoteChange, RemoteItem, RemoteItemKind,
};
use reqwest::blocking::Client;
use serde::Deserialize;
use std::{collections::HashSet, fmt, time::Duration};
use thiserror::Error;

const GOOGLE_USERINFO_ENDPOINT: &str = "https://openidconnect.googleapis.com/v1/userinfo";
const GOOGLE_DRIVE_ABOUT_ENDPOINT: &str = "https://www.googleapis.com/drive/v3/about";
const GOOGLE_DRIVE_START_PAGE_TOKEN_ENDPOINT: &str =
    "https://www.googleapis.com/drive/v3/changes/startPageToken";
const GOOGLE_DRIVE_CHANGES_ENDPOINT: &str = "https://www.googleapis.com/drive/v3/changes";
const GOOGLE_DRIVE_FILES_ENDPOINT: &str = "https://www.googleapis.com/drive/v3/files";
const GOOGLE_DRIVE_FOLDER_MIME_TYPE: &str = "application/vnd.google-apps.folder";
const GOOGLE_DRIVE_INVENTORY_FIELDS: &str =
    "nextPageToken,incompleteSearch,files(id,name,mimeType,parents,size,trashed)";
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
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(60))
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

    /// Returns a fresh cursor representing the current Drive change boundary.
    ///
    /// The opaque cursor is never printed by this provider.
    pub fn current_change_cursor(&self) -> Result<ChangeCursor, DriveApiError> {
        let response: StartPageTokenResponse = self
            .client
            .get(GOOGLE_DRIVE_START_PAGE_TOKEN_ENDPOINT)
            .bearer_auth(self.access_token.as_str())
            .send()?
            .error_for_status()?
            .json()?;

        ChangeCursor::new(response.start_page_token).map_err(DriveApiError::from)
    }

    /// Validates one candidate My Drive sync root using metadata only.
    ///
    /// The provider fetches no file content and does not expose the remote ID
    /// in the returned value or logs.
    pub fn resolve_folder_root(
        &self,
        remote_root_id: &str,
    ) -> Result<DriveFolderRoot, DriveApiError> {
        validate_drive_file_id(remote_root_id)?;

        let metadata: DriveFolderRootMetadata = self
            .client
            .get(format!("{GOOGLE_DRIVE_FILES_ENDPOINT}/{remote_root_id}"))
            .bearer_auth(self.access_token.as_str())
            .query(&[("fields", "id,mimeType,trashed,ownedByMe")])
            .send()?
            .error_for_status()?
            .json()?;

        validate_folder_root_metadata(&metadata)?;
        validate_ancestry_id(&metadata.id)?;

        Ok(DriveFolderRoot {
            canonical_remote_id: metadata.id,
        })
    }

    pub fn validate_folder_root(&self, remote_root_id: &str) -> Result<(), DriveApiError> {
        self.resolve_folder_root(remote_root_id).map(|_| ())
    }

    pub fn resolve_item_membership(
        &self,
        item: &RemoteItem,
        root: &DriveFolderRoot,
    ) -> Result<DriveRootMembership, DriveApiError> {
        validate_ancestry_id(&item.remote_id)?;
        validate_ancestry_id(root.canonical_remote_id())?;

        if item.remote_id == root.canonical_remote_id() {
            return Ok(DriveRootMembership::Root);
        }

        let mut current_parent = item.parent_remote_id.clone();
        let mut seen = HashSet::new();

        for _ in 0..128 {
            let Some(parent_remote_id) = current_parent else {
                return Ok(DriveRootMembership::Outside);
            };

            validate_ancestry_id(&parent_remote_id)?;

            if parent_remote_id == root.canonical_remote_id() {
                return Ok(DriveRootMembership::Descendant);
            }

            if !seen.insert(parent_remote_id.clone()) {
                return Err(DriveApiError::AncestryCycleDetected);
            }

            let metadata = self.fetch_ancestry_metadata(&parent_remote_id)?;

            match ancestry_step(&parent_remote_id, root.canonical_remote_id(), &metadata)? {
                DriveAncestryStep::ReachedRoot => {
                    return Ok(DriveRootMembership::Descendant);
                }
                DriveAncestryStep::Continue(next_parent) => {
                    current_parent = next_parent;
                }
                DriveAncestryStep::Outside => {
                    return Ok(DriveRootMembership::Outside);
                }
            }
        }

        Err(DriveApiError::AncestryHopLimitExceeded)
    }

    fn fetch_ancestry_metadata(
        &self,
        remote_id: &str,
    ) -> Result<DriveAncestryMetadata, DriveApiError> {
        validate_ancestry_id(remote_id)?;

        let metadata: DriveAncestryMetadata = self
            .client
            .get(format!("{GOOGLE_DRIVE_FILES_ENDPOINT}/{remote_id}"))
            .bearer_auth(self.access_token.as_str())
            .query(&[("fields", "id,mimeType,parents,trashed,ownedByMe")])
            .send()?
            .error_for_status()?
            .json()?;

        Ok(metadata)
    }

    /// Lists one metadata-only inventory page for ordinary Drive items
    /// owned by the current user.
    ///
    /// Provider-native Google Workspace items and shortcuts are counted but
    /// not exposed as supported ordinary files in this phase.
    pub fn list_inventory_page(
        &self,
        continuation: Option<&ContinuationToken>,
        page_size: u16,
    ) -> Result<DriveInventoryPage, DriveApiError> {
        if !(1..=1000).contains(&page_size) {
            return Err(DriveApiError::InvalidInventoryPageSize);
        }

        let page_size = page_size.to_string();
        let mut request = self
            .client
            .get(GOOGLE_DRIVE_FILES_ENDPOINT)
            .bearer_auth(self.access_token.as_str())
            .query(&[
                ("q", "'me' in owners and trashed = false"),
                ("corpora", "user"),
                ("spaces", "drive"),
                ("pageSize", page_size.as_str()),
                ("fields", GOOGLE_DRIVE_INVENTORY_FIELDS),
            ]);

        if let Some(token) = continuation {
            request = request.query(&[("pageToken", token.as_str())]);
        }

        let response: FileListResponse = request.send()?.error_for_status()?.json()?;
        response.into_inventory_page()
    }

    /// Lists one metadata-only page of direct children for a Drive folder.
    ///
    /// This is intentionally non-recursive. The caller decides whether and how
    /// to traverse child folders in a later phase.
    pub fn list_folder_children_page(
        &self,
        parent_remote_id: &str,
        continuation: Option<&ContinuationToken>,
        page_size: u16,
    ) -> Result<DriveInventoryPage, DriveApiError> {
        if !(1..=1000).contains(&page_size) {
            return Err(DriveApiError::InvalidInventoryPageSize);
        }

        let query = folder_children_query(parent_remote_id)?;
        let page_size = page_size.to_string();

        let mut request = self
            .client
            .get(GOOGLE_DRIVE_FILES_ENDPOINT)
            .bearer_auth(self.access_token.as_str())
            .query(&[
                ("q", query.as_str()),
                ("corpora", "user"),
                ("spaces", "drive"),
                ("pageSize", page_size.as_str()),
                ("fields", GOOGLE_DRIVE_INVENTORY_FIELDS),
            ]);

        if let Some(token) = continuation {
            request = request.query(&[("pageToken", token.as_str())]);
        }

        let response: FileListResponse = request.send()?.error_for_status()?.json()?;
        response.into_inventory_page()
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

#[derive(Clone, PartialEq, Eq)]
pub struct DriveFolderRoot {
    canonical_remote_id: String,
}

impl DriveFolderRoot {
    pub fn canonical_remote_id(&self) -> &str {
        &self.canonical_remote_id
    }
}

impl fmt::Debug for DriveFolderRoot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DriveFolderRoot([redacted])")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriveRootMembership {
    Root,
    Descendant,
    Outside,
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
struct DriveFolderRootMetadata {
    id: String,
    #[serde(rename = "mimeType")]
    mime_type: String,
    #[serde(default)]
    trashed: bool,
    #[serde(rename = "ownedByMe", default)]
    owned_by_me: bool,
}

#[derive(Debug, Deserialize)]
struct DriveAncestryMetadata {
    id: String,
    #[serde(rename = "mimeType")]
    mime_type: String,
    #[serde(default)]
    parents: Vec<String>,
    #[serde(default)]
    trashed: bool,
    #[serde(rename = "ownedByMe", default)]
    owned_by_me: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DriveAncestryStep {
    ReachedRoot,
    Continue(Option<String>),
    Outside,
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

#[derive(Debug)]
pub struct DriveInventoryPage {
    pub items: Vec<RemoteItem>,
    pub continuation: Option<ContinuationToken>,
    pub supported_items: u64,
    pub file_count: u64,
    pub folder_count: u64,
    pub unsupported_provider_native: u64,
}

#[derive(Debug, Deserialize)]
struct FileListResponse {
    #[serde(rename = "nextPageToken")]
    next_page_token: Option<String>,
    #[serde(rename = "incompleteSearch", default)]
    incomplete_search: bool,
    #[serde(default)]
    files: Vec<GoogleInventoryFile>,
}

impl FileListResponse {
    fn into_inventory_page(self) -> Result<DriveInventoryPage, DriveApiError> {
        if self.incomplete_search {
            return Err(DriveApiError::IncompleteInventorySearch);
        }

        let continuation = self
            .next_page_token
            .map(ContinuationToken::new)
            .transpose()?;

        let mut items = Vec::new();
        let mut file_count = 0_u64;
        let mut folder_count = 0_u64;
        let mut unsupported_provider_native = 0_u64;

        for file in self.files {
            if file.id.trim().is_empty() || file.name.is_empty() || file.mime_type.is_empty() {
                return Err(DriveApiError::InvalidInventoryPage {
                    code: "missing_file_identity_metadata",
                });
            }
            if file.trashed {
                return Err(DriveApiError::InvalidInventoryPage {
                    code: "trashed_item_returned",
                });
            }

            if file.mime_type == GOOGLE_DRIVE_FOLDER_MIME_TYPE {
                folder_count += 1;
                items.push(file.into_remote_item(RemoteItemKind::Folder)?);
            } else if file.mime_type.starts_with("application/vnd.google-apps.") {
                unsupported_provider_native += 1;
            } else {
                file_count += 1;
                items.push(file.into_remote_item(RemoteItemKind::File)?);
            }
        }

        Ok(DriveInventoryPage {
            items,
            continuation,
            supported_items: file_count + folder_count,
            file_count,
            folder_count,
            unsupported_provider_native,
        })
    }
}

#[derive(Debug, Deserialize)]
struct GoogleInventoryFile {
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

impl GoogleInventoryFile {
    fn into_remote_item(self, kind: RemoteItemKind) -> Result<RemoteItem, DriveApiError> {
        Ok(RemoteItem {
            remote_id: self.id,
            parent_remote_id: self.parents.into_iter().next(),
            name: self.name,
            kind,
            size_bytes: parse_optional_u64(self.size.as_deref(), "file.size")?,
            modified_unix_ms: None,
            trashed: self.trashed,
        })
    }
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

        let mut changes = Vec::new();

        for change in self.changes {
            if let Some(change) = change.into_remote_change()? {
                changes.push(change);
            }
        }

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
    fn into_remote_change(self) -> Result<Option<RemoteChange>, DriveApiError> {
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
            return Ok(Some(RemoteChange::Delete {
                remote_id: self.file_id,
            }));
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

        if file.mime_type != GOOGLE_DRIVE_FOLDER_MIME_TYPE
            && file.mime_type.starts_with("application/vnd.google-apps.")
        {
            return Ok(None);
        }

        let size_bytes = parse_optional_u64(file.size.as_deref(), "file.size")?;
        let parent_remote_id = file.parents.into_iter().next();
        let kind = if file.mime_type == GOOGLE_DRIVE_FOLDER_MIME_TYPE {
            RemoteItemKind::Folder
        } else {
            RemoteItemKind::File
        };

        Ok(Some(RemoteChange::Upsert(RemoteItem {
            remote_id: file.id,
            parent_remote_id,
            name: file.name,
            kind,
            size_bytes,
            modified_unix_ms: None,
            trashed: file.trashed,
        })))
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

fn ancestry_step(
    expected_remote_id: &str,
    root_remote_id: &str,
    metadata: &DriveAncestryMetadata,
) -> Result<DriveAncestryStep, DriveApiError> {
    validate_ancestry_id(expected_remote_id)?;
    validate_ancestry_id(root_remote_id)?;
    validate_ancestry_id(&metadata.id)?;

    if metadata.id != expected_remote_id {
        return Err(DriveApiError::AncestryMetadataIdMismatch);
    }

    if metadata.trashed || !metadata.owned_by_me {
        return Ok(DriveAncestryStep::Outside);
    }

    if metadata.mime_type != GOOGLE_DRIVE_FOLDER_MIME_TYPE {
        return Err(DriveApiError::AncestryParentNotFolder);
    }

    if metadata.parents.len() > 1 {
        return Err(DriveApiError::AncestryMultipleParents);
    }

    let next_parent = metadata.parents.first().cloned();

    if next_parent.as_deref() == Some(root_remote_id) {
        Ok(DriveAncestryStep::ReachedRoot)
    } else {
        Ok(DriveAncestryStep::Continue(next_parent))
    }
}

fn validate_ancestry_id(value: &str) -> Result<(), DriveApiError> {
    if value.is_empty()
        || value.len() > 256
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(DriveApiError::InvalidAncestryIdentifier);
    }

    Ok(())
}

fn validate_drive_file_id(value: &str) -> Result<(), DriveApiError> {
    if value.is_empty()
        || value.len() > 256
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(DriveApiError::InvalidRemoteRootId);
    }

    Ok(())
}

fn validate_folder_root_metadata(metadata: &DriveFolderRootMetadata) -> Result<(), DriveApiError> {
    if metadata.mime_type != GOOGLE_DRIVE_FOLDER_MIME_TYPE {
        return Err(DriveApiError::RemoteRootNotFolder);
    }

    if metadata.trashed {
        return Err(DriveApiError::RemoteRootTrashed);
    }

    if !metadata.owned_by_me {
        return Err(DriveApiError::RemoteRootNotOwnedByUser);
    }

    Ok(())
}

fn escape_drive_query_literal(value: &str) -> Result<String, DriveApiError> {
    if value.trim().is_empty() {
        return Err(DriveApiError::InvalidInventoryParentId);
    }

    Ok(value.replace('\\', "\\\\").replace('\'', "\\'"))
}

fn folder_children_query(parent_remote_id: &str) -> Result<String, DriveApiError> {
    let parent_remote_id = escape_drive_query_literal(parent_remote_id)?;
    Ok(format!(
        "'{parent_remote_id}' in parents and 'me' in owners and trashed = false"
    ))
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
    #[error("Google Drive inventory search was incomplete")]
    IncompleteInventorySearch,
    #[error("Google Drive inventory page size must be between 1 and 1000")]
    InvalidInventoryPageSize,
    #[error("Google Drive inventory page is invalid: {code}")]
    InvalidInventoryPage { code: &'static str },
    #[error("Google Drive inventory parent identifier is invalid")]
    InvalidInventoryParentId,
    #[error("Google Drive remote root identifier is invalid")]
    InvalidRemoteRootId,
    #[error("Google Drive remote root is not a folder")]
    RemoteRootNotFolder,
    #[error("Google Drive remote root is trashed")]
    RemoteRootTrashed,
    #[error("Google Drive remote root is outside the supported My Drive ownership scope")]
    RemoteRootNotOwnedByUser,
    #[error("Google Drive ancestry identifier is invalid")]
    InvalidAncestryIdentifier,
    #[error("Google Drive ancestry metadata ID does not match the requested item")]
    AncestryMetadataIdMismatch,
    #[error("Google Drive ancestry parent is not a folder")]
    AncestryParentNotFolder,
    #[error("Google Drive ancestry metadata unexpectedly contains multiple parents")]
    AncestryMultipleParents,
    #[error("Google Drive ancestry traversal detected a cycle")]
    AncestryCycleDetected,
    #[error("Google Drive ancestry traversal exceeded the safety hop limit")]
    AncestryHopLimitExceeded,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_root_id_accepts_root_alias_and_drive_id_shape() {
        assert!(validate_drive_file_id("root").is_ok());
        assert!(validate_drive_file_id("1AbC_def-123").is_ok());
        assert!(matches!(
            validate_drive_file_id("bad/id"),
            Err(DriveApiError::InvalidRemoteRootId)
        ));
        assert!(matches!(
            validate_drive_file_id(""),
            Err(DriveApiError::InvalidRemoteRootId)
        ));
    }

    #[test]
    fn remote_root_metadata_requires_owned_live_folder() {
        let valid = DriveFolderRootMetadata {
            id: "canonical-root".into(),
            mime_type: GOOGLE_DRIVE_FOLDER_MIME_TYPE.into(),
            trashed: false,
            owned_by_me: true,
        };
        assert!(validate_folder_root_metadata(&valid).is_ok());

        let file = DriveFolderRootMetadata {
            id: "file-id".into(),
            mime_type: "text/plain".into(),
            trashed: false,
            owned_by_me: true,
        };
        assert!(matches!(
            validate_folder_root_metadata(&file),
            Err(DriveApiError::RemoteRootNotFolder)
        ));

        let trashed = DriveFolderRootMetadata {
            id: "trashed-root".into(),
            mime_type: GOOGLE_DRIVE_FOLDER_MIME_TYPE.into(),
            trashed: true,
            owned_by_me: true,
        };
        assert!(matches!(
            validate_folder_root_metadata(&trashed),
            Err(DriveApiError::RemoteRootTrashed)
        ));

        let not_owned = DriveFolderRootMetadata {
            id: "foreign-root".into(),
            mime_type: GOOGLE_DRIVE_FOLDER_MIME_TYPE.into(),
            trashed: false,
            owned_by_me: false,
        };
        assert!(matches!(
            validate_folder_root_metadata(&not_owned),
            Err(DriveApiError::RemoteRootNotOwnedByUser)
        ));
    }

    #[test]
    fn canonical_root_identity_redacts_debug_output() {
        let root = DriveFolderRoot {
            canonical_remote_id: "real-drive-root-id".into(),
        };

        assert_eq!(root.canonical_remote_id(), "real-drive-root-id");
        assert_eq!(format!("{root:?}"), "DriveFolderRoot([redacted])");
    }

    #[test]
    fn ancestry_step_reaches_root_or_continues_without_guessing() {
        let reaches_root = DriveAncestryMetadata {
            id: "folder-a".into(),
            mime_type: GOOGLE_DRIVE_FOLDER_MIME_TYPE.into(),
            parents: vec!["canonical-root".into()],
            trashed: false,
            owned_by_me: true,
        };

        assert_eq!(
            ancestry_step("folder-a", "canonical-root", &reaches_root).unwrap(),
            DriveAncestryStep::ReachedRoot
        );

        let continues = DriveAncestryMetadata {
            id: "folder-b".into(),
            mime_type: GOOGLE_DRIVE_FOLDER_MIME_TYPE.into(),
            parents: vec!["folder-a".into()],
            trashed: false,
            owned_by_me: true,
        };

        assert_eq!(
            ancestry_step("folder-b", "canonical-root", &continues).unwrap(),
            DriveAncestryStep::Continue(Some("folder-a".into()))
        );
    }

    #[test]
    fn ancestry_step_fails_closed_for_invalid_parent_metadata() {
        let mismatch = DriveAncestryMetadata {
            id: "different-id".into(),
            mime_type: GOOGLE_DRIVE_FOLDER_MIME_TYPE.into(),
            parents: vec!["canonical-root".into()],
            trashed: false,
            owned_by_me: true,
        };
        assert!(matches!(
            ancestry_step("expected-id", "canonical-root", &mismatch),
            Err(DriveApiError::AncestryMetadataIdMismatch)
        ));

        let non_folder = DriveAncestryMetadata {
            id: "file-parent".into(),
            mime_type: "text/plain".into(),
            parents: vec!["canonical-root".into()],
            trashed: false,
            owned_by_me: true,
        };
        assert!(matches!(
            ancestry_step("file-parent", "canonical-root", &non_folder),
            Err(DriveApiError::AncestryParentNotFolder)
        ));

        let multiple_parents = DriveAncestryMetadata {
            id: "folder-many".into(),
            mime_type: GOOGLE_DRIVE_FOLDER_MIME_TYPE.into(),
            parents: vec!["one".into(), "two".into()],
            trashed: false,
            owned_by_me: true,
        };
        assert!(matches!(
            ancestry_step("folder-many", "canonical-root", &multiple_parents),
            Err(DriveApiError::AncestryMultipleParents)
        ));
    }

    #[test]
    fn ancestry_step_treats_trashed_or_not_owned_parent_as_outside() {
        let trashed = DriveAncestryMetadata {
            id: "folder-a".into(),
            mime_type: GOOGLE_DRIVE_FOLDER_MIME_TYPE.into(),
            parents: vec!["canonical-root".into()],
            trashed: true,
            owned_by_me: true,
        };
        assert_eq!(
            ancestry_step("folder-a", "canonical-root", &trashed).unwrap(),
            DriveAncestryStep::Outside
        );

        let not_owned = DriveAncestryMetadata {
            id: "folder-b".into(),
            mime_type: GOOGLE_DRIVE_FOLDER_MIME_TYPE.into(),
            parents: vec!["canonical-root".into()],
            trashed: false,
            owned_by_me: false,
        };
        assert_eq!(
            ancestry_step("folder-b", "canonical-root", &not_owned).unwrap(),
            DriveAncestryStep::Outside
        );
    }

    #[test]
    fn folder_children_query_is_scoped_and_escapes_literals() {
        assert_eq!(
            folder_children_query("root").unwrap(),
            "'root' in parents and 'me' in owners and trashed = false"
        );
        assert_eq!(
            folder_children_query("folder'with\\chars").unwrap(),
            "'folder\\'with\\\\chars' in parents and 'me' in owners and trashed = false"
        );
    }

    #[test]
    fn folder_children_query_rejects_empty_parent() {
        assert!(matches!(
            folder_children_query("   "),
            Err(DriveApiError::InvalidInventoryParentId)
        ));
    }

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
    fn inventory_page_counts_supported_and_native_items() {
        let response = FileListResponse {
            next_page_token: Some("next-inventory-page".into()),
            incomplete_search: false,
            files: vec![
                GoogleInventoryFile {
                    id: "file-1".into(),
                    name: "example.txt".into(),
                    mime_type: "text/plain".into(),
                    parents: vec!["root".into()],
                    size: Some("12".into()),
                    trashed: false,
                },
                GoogleInventoryFile {
                    id: "folder-1".into(),
                    name: "Folder".into(),
                    mime_type: GOOGLE_DRIVE_FOLDER_MIME_TYPE.into(),
                    parents: vec!["root".into()],
                    size: None,
                    trashed: false,
                },
                GoogleInventoryFile {
                    id: "native-1".into(),
                    name: "Native Doc".into(),
                    mime_type: "application/vnd.google-apps.document".into(),
                    parents: vec!["root".into()],
                    size: None,
                    trashed: false,
                },
            ],
        };

        let page = response.into_inventory_page().unwrap();

        assert_eq!(page.items.len(), 2);
        assert_eq!(page.supported_items, 2);
        assert_eq!(page.file_count, 1);
        assert_eq!(page.folder_count, 1);
        assert_eq!(page.unsupported_provider_native, 1);
        assert_eq!(
            format!("{:?}", page.continuation.unwrap()),
            "ContinuationToken([redacted])"
        );
    }

    #[test]
    fn inventory_page_rejects_incomplete_search() {
        let response = FileListResponse {
            next_page_token: None,
            incomplete_search: true,
            files: Vec::new(),
        };

        assert!(matches!(
            response.into_inventory_page(),
            Err(DriveApiError::IncompleteInventorySearch)
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
                GoogleChange {
                    change_type: "file".into(),
                    file_id: "native-1".into(),
                    removed: false,
                    file: Some(GoogleFile {
                        id: "native-1".into(),
                        name: "Native Doc".into(),
                        mime_type: "application/vnd.google-apps.document".into(),
                        parents: vec!["root".into()],
                        size: None,
                        trashed: false,
                    }),
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
    fn change_page_ignores_google_native_and_shortcut_upserts() {
        let response = ChangeListResponse {
            next_page_token: None,
            new_start_page_token: Some("checkpoint-native".into()),
            changes: vec![
                GoogleChange {
                    change_type: "file".into(),
                    file_id: "native-doc".into(),
                    removed: false,
                    file: Some(GoogleFile {
                        id: "native-doc".into(),
                        name: "Document".into(),
                        mime_type: "application/vnd.google-apps.document".into(),
                        parents: vec!["root".into()],
                        size: None,
                        trashed: false,
                    }),
                },
                GoogleChange {
                    change_type: "file".into(),
                    file_id: "shortcut-1".into(),
                    removed: false,
                    file: Some(GoogleFile {
                        id: "shortcut-1".into(),
                        name: "Shortcut".into(),
                        mime_type: "application/vnd.google-apps.shortcut".into(),
                        parents: vec!["root".into()],
                        size: None,
                        trashed: false,
                    }),
                },
            ],
        };

        let page = response.into_change_page().unwrap();

        assert!(page.changes.is_empty());
        assert_eq!(page.checkpoint.unwrap().as_str(), "checkpoint-native");
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
