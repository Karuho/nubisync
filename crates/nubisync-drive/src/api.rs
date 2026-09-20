use crate::OAuthAccessToken;
use nubisync_core::{
    ChangeCursor, ChangePage, ContinuationToken, RemoteChange, RemoteItem, RemoteItemKind,
};
use reqwest::{StatusCode, blocking::Client};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashSet, VecDeque},
    fmt,
    io::{Read, Write},
    time::Duration,
};
use thiserror::Error;
use url::Url;

const GOOGLE_USERINFO_ENDPOINT: &str = "https://openidconnect.googleapis.com/v1/userinfo";
const GOOGLE_DRIVE_ABOUT_ENDPOINT: &str = "https://www.googleapis.com/drive/v3/about";
const GOOGLE_DRIVE_START_PAGE_TOKEN_ENDPOINT: &str =
    "https://www.googleapis.com/drive/v3/changes/startPageToken";
const GOOGLE_DRIVE_CHANGES_ENDPOINT: &str = "https://www.googleapis.com/drive/v3/changes";
const GOOGLE_DRIVE_FILES_ENDPOINT: &str = "https://www.googleapis.com/drive/v3/files";
const GOOGLE_DRIVE_UPLOAD_FILES_ENDPOINT: &str = "https://www.googleapis.com/upload/drive/v3/files";
const GOOGLE_DRIVE_GENERATE_IDS_ENDPOINT: &str =
    "https://www.googleapis.com/drive/v3/files/generateIds";
const GOOGLE_DRIVE_FOLDER_MIME_TYPE: &str = "application/vnd.google-apps.folder";
const GOOGLE_DRIVE_FILE_UPLOAD_FIELDS: &str =
    "id,name,mimeType,parents,size,trashed,version,md5Checksum,sha256Checksum";

pub const DRIVE_RESUMABLE_CHUNK_ALIGNMENT_BYTES: u64 = 256 * 1024;
pub const DRIVE_DEFAULT_RESUMABLE_CHUNK_BYTES: u64 = 8 * 1024 * 1024;
const GOOGLE_DRIVE_INVENTORY_FIELDS: &str =
    "nextPageToken,incompleteSearch,files(id,name,mimeType,parents,size,trashed)";
const GOOGLE_DRIVE_CHANGES_FIELDS: &str = concat!(
    "nextPageToken,newStartPageToken,",
    "changes(changeType,fileId,removed,",
    "file(id,name,mimeType,parents,size,trashed))"
);
const GOOGLE_DRIVE_WRITE_AUTHORITY_FIELDS: &str = concat!(
    "id,mimeType,trashed,version,md5Checksum,",
    "capabilities(canEdit,canTrash,canAddChildren)"
);
const GOOGLE_DRIVE_FOLDER_CREATE_FIELDS: &str = "id,name,mimeType,parents,trashed,version";

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

    /// Generates predetermined Drive IDs for ordinary file/folder create intents.
    ///
    /// This is the only FullSync provider primitive admitted in phase 5H6.
    /// It creates no Drive object and reads no file content.
    pub fn generate_file_ids(&self, count: u16) -> Result<DriveGeneratedIds, DriveApiError> {
        if !(1..=64).contains(&count) {
            return Err(DriveApiError::InvalidGeneratedIdCount);
        }

        let count_text = count.to_string();
        let response: DriveGeneratedIdsResponse = self
            .client
            .get(GOOGLE_DRIVE_GENERATE_IDS_ENDPOINT)
            .bearer_auth(self.access_token.as_str())
            .query(&[
                ("count", count_text.as_str()),
                ("space", "drive"),
                ("type", "files"),
            ])
            .send()?
            .error_for_status()?
            .json()?;

        validate_generated_ids_response(count, response)
    }

    /// Creates exactly one Drive folder with a predetermined ID.
    ///
    /// This is the first remote-object mutation primitive admitted by NubiSync.
    pub fn create_folder_with_predetermined_id(
        &self,
        remote_id: &str,
        name: &str,
        parent_remote_id: &str,
    ) -> Result<DriveFolderCreateSubmission, DriveApiError> {
        validate_drive_file_id(remote_id)?;
        validate_drive_file_id(parent_remote_id)?;
        validate_folder_create_name(name)?;

        let body = DriveFolderCreateRequest {
            id: remote_id,
            name,
            mime_type: GOOGLE_DRIVE_FOLDER_MIME_TYPE,
            parents: [parent_remote_id],
        };

        let response = self
            .client
            .post(GOOGLE_DRIVE_FILES_ENDPOINT)
            .bearer_auth(self.access_token.as_str())
            .query(&[("fields", GOOGLE_DRIVE_FOLDER_CREATE_FIELDS)])
            .json(&body)
            .send()?;

        if response.status() == StatusCode::CONFLICT {
            return Ok(DriveFolderCreateSubmission::Conflict);
        }

        let metadata: DriveFolderMutationResponse = response.error_for_status()?.json()?;
        if !folder_create_response_matches(remote_id, name, parent_remote_id, &metadata) {
            return Err(DriveApiError::FolderCreatePostconditionMismatch);
        }

        Ok(DriveFolderCreateSubmission::Created)
    }

    /// Inspects a predetermined folder ID without mutating Drive.
    pub fn inspect_expected_folder(
        &self,
        remote_id: &str,
        expected_name: &str,
        expected_parent_remote_id: &str,
    ) -> Result<DriveExpectedFolderLookup, DriveApiError> {
        validate_drive_file_id(remote_id)?;
        validate_drive_file_id(expected_parent_remote_id)?;
        validate_folder_create_name(expected_name)?;

        let response = self
            .client
            .get(format!("{GOOGLE_DRIVE_FILES_ENDPOINT}/{remote_id}"))
            .bearer_auth(self.access_token.as_str())
            .query(&[("fields", GOOGLE_DRIVE_FOLDER_CREATE_FIELDS)])
            .send()?;

        if response.status() == StatusCode::NOT_FOUND {
            return Ok(DriveExpectedFolderLookup::Missing);
        }

        let metadata: DriveFolderMutationResponse = response.error_for_status()?.json()?;
        Ok(
            if folder_create_response_matches(
                remote_id,
                expected_name,
                expected_parent_remote_id,
                &metadata,
            ) {
                DriveExpectedFolderLookup::Exact
            } else {
                DriveExpectedFolderLookup::Mismatch
            },
        )
    }

    /// Inspects one predetermined ordinary-file ID without mutating Drive.
    pub fn inspect_expected_file(
        &self,
        remote_id: &str,
        expected_name: &str,
        expected_parent_remote_id: &str,
        expected_mime_type: &str,
        expected_size_bytes: u64,
        expected_sha256_hex: &str,
    ) -> Result<DriveExpectedFileLookup, DriveApiError> {
        validate_drive_file_id(remote_id)?;
        validate_drive_file_id(expected_parent_remote_id)?;
        validate_ordinary_file_create_name(expected_name)?;
        validate_ordinary_upload_mime_type(expected_mime_type)?;
        validate_expected_sha256(expected_sha256_hex)?;
        let response = self
            .client
            .get(format!("{GOOGLE_DRIVE_FILES_ENDPOINT}/{remote_id}"))
            .bearer_auth(self.access_token.as_str())
            .query(&[("fields", GOOGLE_DRIVE_FILE_UPLOAD_FIELDS)])
            .send()?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(DriveExpectedFileLookup::Missing);
        }
        let metadata: DriveOrdinaryFileUploadResponse = response.error_for_status()?.json()?;
        if !expected_ordinary_file_matches(
            remote_id,
            expected_name,
            expected_parent_remote_id,
            expected_mime_type,
            expected_size_bytes,
            expected_sha256_hex,
            &metadata,
        )? {
            return Ok(DriveExpectedFileLookup::Mismatch);
        }

        let remote_version = metadata
            .version
            .as_deref()
            .ok_or(DriveApiError::OrdinaryFileUploadVersionMissing)?
            .parse::<u64>()
            .map_err(|_| DriveApiError::OrdinaryFileUploadVersionInvalid)?;
        if remote_version == 0 {
            return Err(DriveApiError::OrdinaryFileUploadVersionInvalid);
        }

        Ok(DriveExpectedFileLookup::Exact { remote_version })
    }

    /// Observes metadata-only remote authority for later write planning.
    pub fn observe_write_authority(
        &self,
        remote_id: &str,
    ) -> Result<DriveWriteAuthorityObservation, DriveApiError> {
        validate_drive_file_id(remote_id)?;

        let metadata: DriveWriteAuthorityResponse = self
            .client
            .get(format!("{GOOGLE_DRIVE_FILES_ENDPOINT}/{remote_id}"))
            .bearer_auth(self.access_token.as_str())
            .query(&[("fields", GOOGLE_DRIVE_WRITE_AUTHORITY_FIELDS)])
            .send()?
            .error_for_status()?
            .json()?;

        validate_write_authority_response(remote_id, metadata)
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

    /// Hydrates one supported folder and every supported descendant.
    ///
    /// The supplied folder metadata must already come from a provider change
    /// that the caller has resolved as a descendant of the selected sync root.
    ///
    /// This operation is metadata-only. It performs no storage mutation, file
    /// content read, Drive write, cursor advance, or local filesystem action.
    pub fn hydrate_folder_subtree(
        &self,
        root_item: &RemoteItem,
    ) -> Result<DriveSubtreeHydration, DriveApiError> {
        validate_hydration_root(root_item)?;

        let mut items = vec![root_item.clone()];
        let mut folders = VecDeque::from([root_item.remote_id.clone()]);
        let mut seen_remote_ids = HashSet::from([root_item.remote_id.clone()]);
        let mut unsupported_provider_native = 0_u64;
        let mut page_count = 0_u64;

        while let Some(parent_remote_id) = folders.pop_front() {
            let mut continuation = None;
            let mut seen_page_tokens = HashSet::new();

            loop {
                page_count = page_count
                    .checked_add(1)
                    .ok_or(DriveApiError::HydrationSafetyLimitExceeded)?;

                if page_count > 100_000 {
                    return Err(DriveApiError::HydrationSafetyLimitExceeded);
                }

                let page =
                    self.list_folder_children_page(&parent_remote_id, continuation.as_ref(), 1000)?;

                unsupported_provider_native = unsupported_provider_native
                    .checked_add(page.unsupported_provider_native)
                    .ok_or(DriveApiError::HydrationSafetyLimitExceeded)?;

                for item in page.items {
                    validate_hydration_child(&parent_remote_id, &item, &mut seen_remote_ids)?;

                    if item.kind == RemoteItemKind::Folder {
                        folders.push_back(item.remote_id.clone());
                    }

                    items.push(item);

                    if items.len() > 1_000_000 {
                        return Err(DriveApiError::HydrationSafetyLimitExceeded);
                    }
                }

                match page.continuation {
                    Some(next) => {
                        if !seen_page_tokens.insert(next.as_str().to_owned()) {
                            return Err(DriveApiError::HydrationPaginationLoop);
                        }
                        continuation = Some(next);
                    }
                    None => break,
                }
            }
        }

        Ok(DriveSubtreeHydration {
            items,
            unsupported_provider_native,
            page_count,
        })
    }

    /// Fetches the current provider fingerprint for one ordinary Drive blob.
    ///
    /// This is metadata-only. The SHA-256 is returned by Drive and is never
    /// printed by this provider.
    pub fn fetch_blob_fingerprint(
        &self,
        remote_id: &str,
    ) -> Result<DriveBlobFingerprint, DriveApiError> {
        validate_drive_file_id(remote_id)?;

        let metadata: DriveBlobFingerprintResponse = self
            .client
            .get(format!("{GOOGLE_DRIVE_FILES_ENDPOINT}/{remote_id}"))
            .bearer_auth(self.access_token.as_str())
            .query(&[(
                "fields",
                "id,mimeType,size,sha256Checksum,trashed,ownedByMe",
            )])
            .send()?
            .error_for_status()?
            .json()?;

        validate_blob_fingerprint_response(remote_id, metadata)
    }

    /// Streams one ordinary Drive blob into a caller-owned writer.
    pub fn download_blob_to_writer(
        &self,
        remote_id: &str,
        max_bytes: u64,
        writer: &mut dyn Write,
    ) -> Result<u64, DriveApiError> {
        validate_drive_file_id(remote_id)?;
        if max_bytes == 0 {
            return Err(DriveApiError::InvalidDownloadLimit);
        }

        let mut response = self
            .client
            .get(format!("{GOOGLE_DRIVE_FILES_ENDPOINT}/{remote_id}"))
            .bearer_auth(self.access_token.as_str())
            .query(&[("alt", "media")])
            .send()?
            .error_for_status()?;

        if response.content_length().is_some_and(|n| n > max_bytes) {
            return Err(DriveApiError::DownloadSafetyLimitExceeded);
        }
        copy_bounded_download(&mut response, writer, max_bytes)
    }

    /// Starts an ordinary-file resumable create with a predetermined Drive ID.
    ///
    /// The returned session URI is capability-sensitive: it is retained only
    /// inside the opaque session object and is redacted from Debug output.
    pub fn initiate_resumable_file_create(
        &self,
        remote_id: &str,
        name: &str,
        parent_remote_id: &str,
        mime_type: &str,
        total_bytes: u64,
    ) -> Result<DriveResumableUploadSession, DriveApiError> {
        validate_drive_file_id(remote_id)?;
        validate_drive_file_id(parent_remote_id)?;
        validate_ordinary_file_create_name(name)?;
        validate_ordinary_upload_mime_type(mime_type)?;
        if total_bytes == 0 {
            return Err(DriveApiError::ResumableUploadEmptyFileUnsupported);
        }

        let body = DriveResumableFileCreateRequest {
            id: remote_id,
            name,
            mime_type,
            parents: [parent_remote_id],
        };

        let response = self
            .client
            .post(GOOGLE_DRIVE_UPLOAD_FILES_ENDPOINT)
            .bearer_auth(self.access_token.as_str())
            .query(&[
                ("uploadType", "resumable"),
                ("fields", GOOGLE_DRIVE_FILE_UPLOAD_FIELDS),
            ])
            .header("X-Upload-Content-Type", mime_type)
            .header("X-Upload-Content-Length", total_bytes.to_string())
            .json(&body)
            .send()?
            .error_for_status()?;

        let location = response
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|value| value.to_str().ok())
            .ok_or(DriveApiError::ResumableUploadLocationMissing)?;
        let session_uri = validate_resumable_session_uri(location)?;

        Ok(DriveResumableUploadSession {
            session_uri,
            remote_id: remote_id.to_owned(),
            expected_name: name.to_owned(),
            expected_parent_remote_id: parent_remote_id.to_owned(),
            expected_mime_type: mime_type.to_owned(),
            total_bytes,
        })
    }

    /// Sends one provider-confirmed resumable chunk.
    pub fn upload_resumable_file_chunk(
        &self,
        session: &DriveResumableUploadSession,
        start_offset: u64,
        chunk: Vec<u8>,
    ) -> Result<DriveResumableUploadProgress, DriveApiError> {
        let content_range =
            build_resumable_chunk_content_range(session.total_bytes, start_offset, chunk.len())?;

        let response = self
            .client
            .put(session.session_uri.as_str())
            .bearer_auth(self.access_token.as_str())
            .header(
                reqwest::header::CONTENT_TYPE,
                session.expected_mime_type.as_str(),
            )
            .header(reqwest::header::CONTENT_LENGTH, chunk.len().to_string())
            .header(reqwest::header::CONTENT_RANGE, content_range)
            .body(chunk)
            .send()?;

        parse_resumable_upload_response(session, response)
    }

    /// Queries an interrupted resumable session without uploading content.
    pub fn query_resumable_file_upload_status(
        &self,
        session: &DriveResumableUploadSession,
    ) -> Result<DriveResumableUploadProgress, DriveApiError> {
        let response = self
            .client
            .put(session.session_uri.as_str())
            .bearer_auth(self.access_token.as_str())
            .header(reqwest::header::CONTENT_LENGTH, "0")
            .header(
                reqwest::header::CONTENT_RANGE,
                format!("bytes */{}", session.total_bytes),
            )
            .send()?;

        parse_resumable_upload_response(session, response)
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

#[derive(Clone, PartialEq, Eq)]
pub struct DriveBlobFingerprint {
    pub size_bytes: u64,
    sha256_hex: String,
}

impl DriveBlobFingerprint {
    pub fn sha256_hex(&self) -> &str {
        &self.sha256_hex
    }
}

impl fmt::Debug for DriveBlobFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DriveBlobFingerprint")
            .field("size_bytes", &self.size_bytes)
            .field("sha256_hex", &"[redacted]")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct DriveGeneratedIds {
    ids: Vec<String>,
}

impl DriveGeneratedIds {
    pub fn len(&self) -> usize {
        self.ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    pub fn into_ids(self) -> Vec<String> {
        self.ids
    }
}

impl fmt::Debug for DriveGeneratedIds {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DriveGeneratedIds")
            .field("count", &self.ids.len())
            .field("ids", &"[redacted]")
            .finish()
    }
}

#[derive(Debug, Deserialize)]
struct DriveGeneratedIdsResponse {
    ids: Vec<String>,
    space: String,
    kind: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriveFolderCreateSubmission {
    Created,
    Conflict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriveExpectedFolderLookup {
    Exact,
    Missing,
    Mismatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriveExpectedFileLookup {
    Exact { remote_version: u64 },
    Missing,
    Mismatch,
}

#[derive(Clone, PartialEq, Eq)]
pub struct DriveResumableUploadSession {
    session_uri: String,
    remote_id: String,
    expected_name: String,
    expected_parent_remote_id: String,
    expected_mime_type: String,
    total_bytes: u64,
}

impl DriveResumableUploadSession {
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
}

impl fmt::Debug for DriveResumableUploadSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DriveResumableUploadSession")
            .field("session_uri", &"[redacted]")
            .field("remote_id", &"[redacted]")
            .field("expected_name", &"[redacted]")
            .field("expected_parent_remote_id", &"[redacted]")
            .field("expected_mime_type", &self.expected_mime_type)
            .field("total_bytes", &self.total_bytes)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct DriveOrdinaryFileUploadCompletion {
    pub size_bytes: u64,
    pub remote_version: u64,
    md5_checksum: Option<String>,
    sha256_checksum: Option<String>,
}

impl DriveOrdinaryFileUploadCompletion {
    pub fn md5_checksum(&self) -> Option<&str> {
        self.md5_checksum.as_deref()
    }

    pub fn sha256_checksum(&self) -> Option<&str> {
        self.sha256_checksum.as_deref()
    }
}

impl fmt::Debug for DriveOrdinaryFileUploadCompletion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DriveOrdinaryFileUploadCompletion")
            .field("size_bytes", &self.size_bytes)
            .field("remote_version", &self.remote_version)
            .field(
                "md5_checksum",
                &self.md5_checksum.as_deref().map(|_| "[redacted]"),
            )
            .field(
                "sha256_checksum",
                &self.sha256_checksum.as_deref().map(|_| "[redacted]"),
            )
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriveResumableUploadProgress {
    Incomplete { next_offset: u64 },
    Complete(DriveOrdinaryFileUploadCompletion),
    Expired,
}

#[derive(Serialize)]
struct DriveResumableFileCreateRequest<'a> {
    id: &'a str,
    name: &'a str,
    #[serde(rename = "mimeType")]
    mime_type: &'a str,
    parents: [&'a str; 1],
}

#[derive(Deserialize)]
struct DriveOrdinaryFileUploadResponse {
    id: String,
    name: String,
    #[serde(rename = "mimeType")]
    mime_type: String,
    #[serde(default)]
    parents: Vec<String>,
    size: Option<String>,
    #[serde(default)]
    trashed: bool,
    version: Option<String>,
    #[serde(rename = "md5Checksum")]
    md5_checksum: Option<String>,
    #[serde(rename = "sha256Checksum")]
    sha256_checksum: Option<String>,
}

#[derive(Serialize)]
struct DriveFolderCreateRequest<'a> {
    id: &'a str,
    name: &'a str,
    #[serde(rename = "mimeType")]
    mime_type: &'static str,
    parents: [&'a str; 1],
}

#[derive(Deserialize)]
struct DriveFolderMutationResponse {
    id: String,
    name: String,
    #[serde(rename = "mimeType")]
    mime_type: String,
    #[serde(default)]
    parents: Vec<String>,
    #[serde(default)]
    trashed: bool,
    version: Option<String>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct DriveWriteAuthorityObservation {
    remote_id: String,
    pub kind: RemoteItemKind,
    pub remote_version: u64,
    md5_checksum: Option<String>,
    pub can_edit: bool,
    pub can_trash: bool,
    pub can_add_children: bool,
}

impl DriveWriteAuthorityObservation {
    pub fn remote_id(&self) -> &str {
        &self.remote_id
    }

    pub fn md5_checksum(&self) -> Option<&str> {
        self.md5_checksum.as_deref()
    }
}

impl fmt::Debug for DriveWriteAuthorityObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DriveWriteAuthorityObservation")
            .field("remote_id", &"[redacted]")
            .field("kind", &self.kind)
            .field("remote_version", &self.remote_version)
            .field(
                "md5_checksum",
                &self.md5_checksum.as_deref().map(|_| "[redacted]"),
            )
            .field("can_edit", &self.can_edit)
            .field("can_trash", &self.can_trash)
            .field("can_add_children", &self.can_add_children)
            .finish()
    }
}

#[derive(Debug, Deserialize)]
struct DriveWriteAuthorityResponse {
    id: String,
    #[serde(rename = "mimeType")]
    mime_type: String,
    #[serde(default)]
    trashed: bool,
    version: Option<String>,
    #[serde(rename = "md5Checksum")]
    md5_checksum: Option<String>,
    #[serde(default)]
    capabilities: DriveWriteCapabilitiesResponse,
}

#[derive(Debug, Default, Deserialize)]
struct DriveWriteCapabilitiesResponse {
    #[serde(rename = "canEdit", default)]
    can_edit: bool,
    #[serde(rename = "canTrash", default)]
    can_trash: bool,
    #[serde(rename = "canAddChildren", default)]
    can_add_children: bool,
}

#[derive(Debug, Deserialize)]
struct DriveBlobFingerprintResponse {
    id: String,
    #[serde(rename = "mimeType")]
    mime_type: String,
    size: Option<String>,
    #[serde(rename = "sha256Checksum")]
    sha256_checksum: Option<String>,
    #[serde(default)]
    trashed: bool,
    #[serde(rename = "ownedByMe", default)]
    owned_by_me: bool,
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

pub struct DriveSubtreeHydration {
    items: Vec<RemoteItem>,
    unsupported_provider_native: u64,
    page_count: u64,
}

impl DriveSubtreeHydration {
    pub fn item_count(&self) -> usize {
        self.items.len()
    }

    pub fn unsupported_provider_native(&self) -> u64 {
        self.unsupported_provider_native
    }

    pub fn page_count(&self) -> u64 {
        self.page_count
    }

    pub fn into_items(self) -> Vec<RemoteItem> {
        self.items
    }
}

impl fmt::Debug for DriveSubtreeHydration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DriveSubtreeHydration")
            .field("item_count", &self.items.len())
            .field(
                "unsupported_provider_native",
                &self.unsupported_provider_native,
            )
            .field("page_count", &self.page_count)
            .finish()
    }
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

fn validate_ordinary_file_create_name(name: &str) -> Result<(), DriveApiError> {
    if name.is_empty() || name.contains('/') || name.contains('\0') {
        return Err(DriveApiError::InvalidOrdinaryFileCreateName);
    }
    Ok(())
}

fn validate_ordinary_upload_mime_type(mime_type: &str) -> Result<(), DriveApiError> {
    if mime_type.is_empty()
        || mime_type.len() > 255
        || mime_type.chars().any(char::is_whitespace)
        || mime_type.chars().any(char::is_control)
        || !mime_type.contains('/')
        || mime_type == GOOGLE_DRIVE_FOLDER_MIME_TYPE
        || mime_type.starts_with("application/vnd.google-apps.")
    {
        return Err(DriveApiError::InvalidOrdinaryFileUploadMimeType);
    }
    Ok(())
}

fn validate_resumable_session_uri(value: &str) -> Result<String, DriveApiError> {
    let parsed = Url::parse(value).map_err(|_| DriveApiError::InvalidResumableSessionUri)?;
    if parsed.scheme() != "https"
        || parsed.host_str() != Some("www.googleapis.com")
        || parsed.port_or_known_default() != Some(443)
        || parsed.fragment().is_some()
        || !parsed.path().starts_with("/upload/drive/v3/files")
    {
        return Err(DriveApiError::InvalidResumableSessionUri);
    }
    Ok(parsed.to_string())
}

fn build_resumable_chunk_content_range(
    total_bytes: u64,
    start_offset: u64,
    chunk_len: usize,
) -> Result<String, DriveApiError> {
    if total_bytes == 0 || chunk_len == 0 {
        return Err(DriveApiError::InvalidResumableUploadChunk);
    }

    let chunk_len =
        u64::try_from(chunk_len).map_err(|_| DriveApiError::InvalidResumableUploadChunk)?;
    let end_exclusive = start_offset
        .checked_add(chunk_len)
        .ok_or(DriveApiError::InvalidResumableUploadChunk)?;
    if start_offset >= total_bytes || end_exclusive > total_bytes {
        return Err(DriveApiError::InvalidResumableUploadChunk);
    }

    let final_chunk = end_exclusive == total_bytes;
    if !final_chunk && chunk_len % DRIVE_RESUMABLE_CHUNK_ALIGNMENT_BYTES != 0 {
        return Err(DriveApiError::InvalidResumableUploadChunkAlignment);
    }

    Ok(format!(
        "bytes {start_offset}-{}/{}",
        end_exclusive - 1,
        total_bytes
    ))
}

fn parse_resumable_next_offset(
    range_header: Option<&str>,
    total_bytes: u64,
) -> Result<u64, DriveApiError> {
    let Some(value) = range_header else {
        return Ok(0);
    };
    let suffix = value
        .strip_prefix("bytes=0-")
        .ok_or(DriveApiError::InvalidResumableUploadRange)?;
    let last = suffix
        .parse::<u64>()
        .map_err(|_| DriveApiError::InvalidResumableUploadRange)?;
    let next = last
        .checked_add(1)
        .ok_or(DriveApiError::InvalidResumableUploadRange)?;
    if next >= total_bytes {
        return Err(DriveApiError::InvalidResumableUploadRange);
    }
    Ok(next)
}

fn normalize_optional_hex_checksum(
    value: Option<String>,
    expected_len: usize,
) -> Result<Option<String>, DriveApiError> {
    value
        .map(|value| {
            if value.len() != expected_len || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(DriveApiError::InvalidOrdinaryFileUploadChecksum);
            }
            Ok(value.to_ascii_lowercase())
        })
        .transpose()
}

fn validate_ordinary_file_upload_completion(
    session: &DriveResumableUploadSession,
    metadata: DriveOrdinaryFileUploadResponse,
) -> Result<DriveOrdinaryFileUploadCompletion, DriveApiError> {
    if metadata.id != session.remote_id
        || metadata.name != session.expected_name
        || metadata.mime_type != session.expected_mime_type
        || metadata.parents.len() != 1
        || metadata.parents.first().map(String::as_str)
            != Some(session.expected_parent_remote_id.as_str())
        || metadata.trashed
    {
        return Err(DriveApiError::OrdinaryFileUploadPostconditionMismatch);
    }

    let size_bytes = metadata
        .size
        .as_deref()
        .ok_or(DriveApiError::OrdinaryFileUploadSizeMissing)?
        .parse::<u64>()
        .map_err(|_| DriveApiError::OrdinaryFileUploadSizeInvalid)?;
    if size_bytes != session.total_bytes {
        return Err(DriveApiError::OrdinaryFileUploadPostconditionMismatch);
    }

    let remote_version = metadata
        .version
        .as_deref()
        .ok_or(DriveApiError::OrdinaryFileUploadVersionMissing)?
        .parse::<u64>()
        .map_err(|_| DriveApiError::OrdinaryFileUploadVersionInvalid)?;
    if remote_version == 0 {
        return Err(DriveApiError::OrdinaryFileUploadVersionInvalid);
    }

    let md5_checksum = normalize_optional_hex_checksum(metadata.md5_checksum, 32)?;
    let sha256_checksum = normalize_optional_hex_checksum(metadata.sha256_checksum, 64)?;

    Ok(DriveOrdinaryFileUploadCompletion {
        size_bytes,
        remote_version,
        md5_checksum,
        sha256_checksum,
    })
}

fn parse_resumable_upload_response(
    session: &DriveResumableUploadSession,
    response: reqwest::blocking::Response,
) -> Result<DriveResumableUploadProgress, DriveApiError> {
    match response.status() {
        StatusCode::OK | StatusCode::CREATED => {
            let metadata: DriveOrdinaryFileUploadResponse = response.json()?;
            Ok(DriveResumableUploadProgress::Complete(
                validate_ordinary_file_upload_completion(session, metadata)?,
            ))
        }
        StatusCode::PERMANENT_REDIRECT => {
            let range = response
                .headers()
                .get(reqwest::header::RANGE)
                .and_then(|value| value.to_str().ok());
            Ok(DriveResumableUploadProgress::Incomplete {
                next_offset: parse_resumable_next_offset(range, session.total_bytes)?,
            })
        }
        StatusCode::NOT_FOUND => Ok(DriveResumableUploadProgress::Expired),
        _ => {
            response.error_for_status()?;
            Err(DriveApiError::UnexpectedResumableUploadStatus)
        }
    }
}

fn validate_expected_sha256(value: &str) -> Result<(), DriveApiError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return Err(DriveApiError::InvalidExpectedOrdinaryFileSha256);
    }
    Ok(())
}
fn expected_ordinary_file_matches(
    expected_remote_id: &str,
    expected_name: &str,
    expected_parent_remote_id: &str,
    expected_mime_type: &str,
    expected_size_bytes: u64,
    expected_sha256_hex: &str,
    metadata: &DriveOrdinaryFileUploadResponse,
) -> Result<bool, DriveApiError> {
    validate_expected_sha256(expected_sha256_hex)?;
    if metadata.id != expected_remote_id
        || metadata.name != expected_name
        || metadata.mime_type != expected_mime_type
        || metadata.parents.len() != 1
        || metadata.parents.first().map(String::as_str) != Some(expected_parent_remote_id)
        || metadata.trashed
    {
        return Ok(false);
    }
    let Some(size) = metadata.size.as_deref() else {
        return Ok(false);
    };
    if size
        .parse::<u64>()
        .map_err(|_| DriveApiError::OrdinaryFileUploadSizeInvalid)?
        != expected_size_bytes
    {
        return Ok(false);
    }
    let Some(version) = metadata.version.as_deref() else {
        return Ok(false);
    };
    if version
        .parse::<u64>()
        .map_err(|_| DriveApiError::OrdinaryFileUploadVersionInvalid)?
        == 0
    {
        return Ok(false);
    }
    let Some(remote_sha) = normalize_optional_hex_checksum(metadata.sha256_checksum.clone(), 64)?
    else {
        return Ok(false);
    };
    Ok(remote_sha == expected_sha256_hex)
}

fn validate_folder_create_name(name: &str) -> Result<(), DriveApiError> {
    if name.is_empty() || name.contains('/') || name.contains('\0') {
        return Err(DriveApiError::InvalidFolderCreateName);
    }
    Ok(())
}

fn folder_create_response_matches(
    expected_remote_id: &str,
    expected_name: &str,
    expected_parent_remote_id: &str,
    metadata: &DriveFolderMutationResponse,
) -> bool {
    metadata.id == expected_remote_id
        && metadata.name == expected_name
        && metadata.mime_type == GOOGLE_DRIVE_FOLDER_MIME_TYPE
        && metadata.parents.len() == 1
        && metadata.parents.first().map(String::as_str) == Some(expected_parent_remote_id)
        && !metadata.trashed
        && metadata
            .version
            .as_deref()
            .and_then(|value| value.parse::<u64>().ok())
            .is_some_and(|value| value > 0)
}

fn validate_generated_ids_response(
    expected_count: u16,
    response: DriveGeneratedIdsResponse,
) -> Result<DriveGeneratedIds, DriveApiError> {
    if response.kind != "drive#generatedIds" || response.space != "drive" {
        return Err(DriveApiError::InvalidGeneratedIdsResponse);
    }

    if response.ids.len() != usize::from(expected_count) {
        return Err(DriveApiError::GeneratedIdCountMismatch);
    }

    let mut seen = HashSet::new();
    for remote_id in &response.ids {
        validate_drive_file_id(remote_id)?;
        if !seen.insert(remote_id.clone()) {
            return Err(DriveApiError::DuplicateGeneratedId);
        }
    }

    Ok(DriveGeneratedIds { ids: response.ids })
}

fn validate_write_authority_response(
    expected_remote_id: &str,
    metadata: DriveWriteAuthorityResponse,
) -> Result<DriveWriteAuthorityObservation, DriveApiError> {
    if metadata.id != expected_remote_id {
        return Err(DriveApiError::WriteAuthorityMetadataIdMismatch);
    }

    if metadata.trashed {
        return Err(DriveApiError::WriteAuthorityItemTrashed);
    }

    if metadata.mime_type != GOOGLE_DRIVE_FOLDER_MIME_TYPE
        && metadata
            .mime_type
            .starts_with("application/vnd.google-apps.")
    {
        return Err(DriveApiError::WriteAuthorityProviderNativeUnsupported);
    }

    let kind = if metadata.mime_type == GOOGLE_DRIVE_FOLDER_MIME_TYPE {
        RemoteItemKind::Folder
    } else {
        RemoteItemKind::File
    };

    let remote_version = metadata
        .version
        .as_deref()
        .ok_or(DriveApiError::WriteAuthorityVersionMissing)?
        .parse::<u64>()
        .map_err(|_| DriveApiError::WriteAuthorityVersionInvalid)?;

    if remote_version == 0 {
        return Err(DriveApiError::WriteAuthorityVersionInvalid);
    }

    let md5_checksum = metadata
        .md5_checksum
        .map(|value| {
            if value.len() != 32 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(DriveApiError::WriteAuthorityMd5Invalid);
            }
            Ok(value.to_ascii_lowercase())
        })
        .transpose()?;

    if metadata.mime_type == GOOGLE_DRIVE_FOLDER_MIME_TYPE && md5_checksum.is_some() {
        return Err(DriveApiError::WriteAuthorityMd5Invalid);
    }

    Ok(DriveWriteAuthorityObservation {
        remote_id: metadata.id,
        kind,
        remote_version,
        md5_checksum,
        can_edit: metadata.capabilities.can_edit,
        can_trash: metadata.capabilities.can_trash,
        can_add_children: metadata.capabilities.can_add_children,
    })
}

fn validate_blob_fingerprint_response(
    expected_remote_id: &str,
    metadata: DriveBlobFingerprintResponse,
) -> Result<DriveBlobFingerprint, DriveApiError> {
    if metadata.id != expected_remote_id {
        return Err(DriveApiError::InvalidBlobFingerprintMetadata);
    }

    if metadata.trashed
        || !metadata.owned_by_me
        || metadata.mime_type == GOOGLE_DRIVE_FOLDER_MIME_TYPE
        || metadata
            .mime_type
            .starts_with("application/vnd.google-apps.")
    {
        return Err(DriveApiError::BlobFingerprintNotOrdinaryFile);
    }

    let size_bytes = parse_optional_u64(metadata.size.as_deref(), "file.size")?
        .ok_or(DriveApiError::BlobFingerprintSizeMissing)?;

    let sha256_hex = metadata
        .sha256_checksum
        .ok_or(DriveApiError::BlobFingerprintSha256Missing)?;

    if sha256_hex.len() != 64 || !sha256_hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(DriveApiError::BlobFingerprintSha256Invalid);
    }

    Ok(DriveBlobFingerprint {
        size_bytes,
        sha256_hex: sha256_hex.to_ascii_lowercase(),
    })
}

fn validate_hydration_root(item: &RemoteItem) -> Result<(), DriveApiError> {
    validate_ancestry_id(&item.remote_id)?;

    if item.kind != RemoteItemKind::Folder {
        return Err(DriveApiError::HydrationRootNotFolder);
    }

    if item.trashed {
        return Err(DriveApiError::HydrationRootTrashed);
    }

    if item.name.is_empty() {
        return Err(DriveApiError::HydrationInvalidItem);
    }

    Ok(())
}

fn validate_hydration_child(
    expected_parent_remote_id: &str,
    item: &RemoteItem,
    seen_remote_ids: &mut HashSet<String>,
) -> Result<(), DriveApiError> {
    validate_ancestry_id(expected_parent_remote_id)?;
    validate_ancestry_id(&item.remote_id)?;

    if item.name.is_empty() || item.trashed {
        return Err(DriveApiError::HydrationInvalidItem);
    }

    if item.parent_remote_id.as_deref() != Some(expected_parent_remote_id) {
        return Err(DriveApiError::HydrationParentMismatch);
    }

    if !seen_remote_ids.insert(item.remote_id.clone()) {
        return Err(DriveApiError::HydrationDuplicateRemoteId);
    }

    Ok(())
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

fn copy_bounded_download(
    reader: &mut dyn Read,
    writer: &mut dyn Write,
    max_bytes: u64,
) -> Result<u64, DriveApiError> {
    if max_bytes == 0 {
        return Err(DriveApiError::InvalidDownloadLimit);
    }
    let mut total = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let remaining = max_bytes.saturating_sub(total);
        let probe = remaining.saturating_add(1).min(buffer.len() as u64) as usize;
        let read = reader.read(&mut buffer[..probe])?;
        if read == 0 {
            return Ok(total);
        }
        let read = u64::try_from(read).map_err(|_| DriveApiError::DownloadSafetyLimitExceeded)?;
        let next = total
            .checked_add(read)
            .ok_or(DriveApiError::DownloadSafetyLimitExceeded)?;
        if next > max_bytes {
            return Err(DriveApiError::DownloadSafetyLimitExceeded);
        }
        writer.write_all(&buffer[..read as usize])?;
        total = next;
    }
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
    #[error("Google Drive file download byte limit must be non-zero")]
    InvalidDownloadLimit,
    #[error("Google Drive file download exceeded the supervised byte limit")]
    DownloadSafetyLimitExceeded,
    #[error("Google Drive file download stream I/O failed")]
    ContentIo(#[from] std::io::Error),
    #[error("Google Drive generated-ID count must be between 1 and 64")]
    InvalidGeneratedIdCount,
    #[error("Google Drive generated-ID response metadata is invalid")]
    InvalidGeneratedIdsResponse,
    #[error("Google Drive generated-ID response count mismatched the request")]
    GeneratedIdCountMismatch,
    #[error("Google Drive generated-ID response contained a duplicate ID")]
    DuplicateGeneratedId,
    #[error("Google Drive folder-create name is invalid")]
    InvalidFolderCreateName,
    #[error("Google Drive folder-create response did not match the durable intent")]
    FolderCreatePostconditionMismatch,
    #[error("Google Drive ordinary-file create name is invalid")]
    InvalidOrdinaryFileCreateName,
    #[error("Google Drive ordinary-file upload MIME type is invalid")]
    InvalidOrdinaryFileUploadMimeType,
    #[error("zero-byte ordinary files are not yet admitted by the resumable provider primitive")]
    ResumableUploadEmptyFileUnsupported,
    #[error("Google Drive resumable upload response is missing the session Location header")]
    ResumableUploadLocationMissing,
    #[error("Google Drive resumable session URI is invalid")]
    InvalidResumableSessionUri,
    #[error("Google Drive resumable upload chunk is invalid")]
    InvalidResumableUploadChunk,
    #[error("Google Drive non-final resumable chunks must align to 256 KiB")]
    InvalidResumableUploadChunkAlignment,
    #[error("Google Drive resumable upload Range header is invalid")]
    InvalidResumableUploadRange,
    #[error("Google Drive resumable upload returned an unexpected successful status")]
    UnexpectedResumableUploadStatus,
    #[error("Google Drive ordinary-file upload response did not match the session intent")]
    OrdinaryFileUploadPostconditionMismatch,
    #[error("Google Drive ordinary-file upload response is missing byte size")]
    OrdinaryFileUploadSizeMissing,
    #[error("Google Drive ordinary-file upload response byte size is invalid")]
    OrdinaryFileUploadSizeInvalid,
    #[error("Google Drive ordinary-file upload response is missing version")]
    OrdinaryFileUploadVersionMissing,
    #[error("Google Drive ordinary-file upload response version is invalid")]
    OrdinaryFileUploadVersionInvalid,
    #[error("Google Drive ordinary-file upload checksum metadata is invalid")]
    InvalidOrdinaryFileUploadChecksum,
    #[error("expected ordinary-file SHA-256 is invalid")]
    InvalidExpectedOrdinaryFileSha256,
    #[error("Google Drive write-authority metadata ID does not match the requested item")]
    WriteAuthorityMetadataIdMismatch,
    #[error("Google Drive write-authority target is trashed")]
    WriteAuthorityItemTrashed,
    #[error("Google Drive write-authority target is a provider-native item")]
    WriteAuthorityProviderNativeUnsupported,
    #[error("Google Drive write-authority version is missing")]
    WriteAuthorityVersionMissing,
    #[error("Google Drive write-authority version is invalid")]
    WriteAuthorityVersionInvalid,
    #[error("Google Drive write-authority MD5 metadata is invalid")]
    WriteAuthorityMd5Invalid,
    #[error("Google Drive blob fingerprint metadata is invalid")]
    InvalidBlobFingerprintMetadata,
    #[error("Google Drive blob fingerprint target is not a supported ordinary file")]
    BlobFingerprintNotOrdinaryFile,
    #[error("Google Drive blob fingerprint is missing durable byte size")]
    BlobFingerprintSizeMissing,
    #[error("Google Drive blob fingerprint is missing SHA-256")]
    BlobFingerprintSha256Missing,
    #[error("Google Drive blob fingerprint SHA-256 is invalid")]
    BlobFingerprintSha256Invalid,
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
    #[error("Drive subtree hydration root is not a folder")]
    HydrationRootNotFolder,
    #[error("Drive subtree hydration root is trashed")]
    HydrationRootTrashed,
    #[error("Drive subtree hydration returned invalid item metadata")]
    HydrationInvalidItem,
    #[error("Drive subtree hydration child parent does not match traversal context")]
    HydrationParentMismatch,
    #[error("Drive subtree hydration returned a duplicate remote identifier")]
    HydrationDuplicateRemoteId,
    #[error("Drive subtree hydration pagination token repeated")]
    HydrationPaginationLoop,
    #[error("Drive subtree hydration exceeded a safety limit")]
    HydrationSafetyLimitExceeded,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn phase5h16_session() -> DriveResumableUploadSession {
        DriveResumableUploadSession {
            session_uri: "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&upload_id=secret-session".into(),
            remote_id: "generated-file-id".into(),
            expected_name: "example.bin".into(),
            expected_parent_remote_id: "parent-id".into(),
            expected_mime_type: "application/octet-stream".into(),
            total_bytes: DRIVE_DEFAULT_RESUMABLE_CHUNK_BYTES + 7,
        }
    }

    #[test]
    fn phase5h16_resumable_constants_are_aligned() {
        assert_eq!(DRIVE_RESUMABLE_CHUNK_ALIGNMENT_BYTES, 256 * 1024);
        assert_eq!(DRIVE_DEFAULT_RESUMABLE_CHUNK_BYTES, 8 * 1024 * 1024);
        assert_eq!(
            DRIVE_DEFAULT_RESUMABLE_CHUNK_BYTES % DRIVE_RESUMABLE_CHUNK_ALIGNMENT_BYTES,
            0
        );
    }

    #[test]
    fn phase5h16_session_uri_is_google_https_only_and_debug_redacts_capability() {
        let valid = "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&upload_id=secret-session";
        assert_eq!(validate_resumable_session_uri(valid).unwrap(), valid);

        for invalid in [
            "http://www.googleapis.com/upload/drive/v3/files?upload_id=x",
            "https://evil.example/upload/drive/v3/files?upload_id=x",
            "https://www.googleapis.com/drive/v3/files?upload_id=x",
            "https://www.googleapis.com/upload/drive/v3/files?upload_id=x#fragment",
        ] {
            assert!(matches!(
                validate_resumable_session_uri(invalid),
                Err(DriveApiError::InvalidResumableSessionUri)
            ));
        }

        let session = phase5h16_session();
        let debug = format!("{session:?}");
        assert!(!debug.contains("secret-session"));
        assert!(!debug.contains("generated-file-id"));
        assert!(!debug.contains("example.bin"));
        assert!(!debug.contains("parent-id"));
        assert!(debug.contains("[redacted]"));
    }

    #[test]
    fn phase5h16_chunk_ranges_require_alignment_except_final_chunk() {
        let total = DRIVE_DEFAULT_RESUMABLE_CHUNK_BYTES + 7;
        assert_eq!(
            build_resumable_chunk_content_range(
                total,
                0,
                DRIVE_DEFAULT_RESUMABLE_CHUNK_BYTES as usize,
            )
            .unwrap(),
            format!(
                "bytes 0-{}/{}",
                DRIVE_DEFAULT_RESUMABLE_CHUNK_BYTES - 1,
                total
            )
        );
        assert_eq!(
            build_resumable_chunk_content_range(total, DRIVE_DEFAULT_RESUMABLE_CHUNK_BYTES, 7,)
                .unwrap(),
            format!(
                "bytes {}-{}/{}",
                DRIVE_DEFAULT_RESUMABLE_CHUNK_BYTES,
                total - 1,
                total
            )
        );

        assert!(matches!(
            build_resumable_chunk_content_range(total, 0, 1),
            Err(DriveApiError::InvalidResumableUploadChunkAlignment)
        ));
        assert!(matches!(
            build_resumable_chunk_content_range(total, 0, 0),
            Err(DriveApiError::InvalidResumableUploadChunk)
        ));
        assert!(matches!(
            build_resumable_chunk_content_range(total, total, 1),
            Err(DriveApiError::InvalidResumableUploadChunk)
        ));
    }

    #[test]
    fn phase5h16_range_header_controls_resume_offset() {
        let total = 1_000;
        assert_eq!(parse_resumable_next_offset(None, total).unwrap(), 0);
        assert_eq!(
            parse_resumable_next_offset(Some("bytes=0-42"), total).unwrap(),
            43
        );
        for invalid in ["bytes=1-42", "bytes=0-x", "0-42", "bytes=0-999"] {
            assert!(matches!(
                parse_resumable_next_offset(Some(invalid), total),
                Err(DriveApiError::InvalidResumableUploadRange)
            ));
        }
    }

    #[test]
    fn phase5h16_completion_requires_exact_identity_size_and_redacts_hashes() {
        let session = phase5h16_session();
        let completion = validate_ordinary_file_upload_completion(
            &session,
            DriveOrdinaryFileUploadResponse {
                id: "generated-file-id".into(),
                name: "example.bin".into(),
                mime_type: "application/octet-stream".into(),
                parents: vec!["parent-id".into()],
                size: Some(session.total_bytes.to_string()),
                trashed: false,
                version: Some("7".into()),
                md5_checksum: Some("A".repeat(32)),
                sha256_checksum: Some("B".repeat(64)),
            },
        )
        .unwrap();

        assert_eq!(completion.size_bytes, session.total_bytes);
        assert_eq!(completion.remote_version, 7);
        let expected_md5 = "a".repeat(32);
        let expected_sha256 = "b".repeat(64);
        assert_eq!(completion.md5_checksum(), Some(expected_md5.as_str()));
        assert_eq!(completion.sha256_checksum(), Some(expected_sha256.as_str()));

        let debug = format!("{completion:?}");
        assert!(!debug.contains(&expected_md5));
        assert!(!debug.contains(&expected_sha256));
        assert!(debug.contains("[redacted]"));
    }

    #[test]
    fn phase5h16_completion_fails_closed_on_mismatch_and_bad_checksum() {
        let session = phase5h16_session();
        let mismatched = DriveOrdinaryFileUploadResponse {
            id: "different-id".into(),
            name: "example.bin".into(),
            mime_type: "application/octet-stream".into(),
            parents: vec!["parent-id".into()],
            size: Some(session.total_bytes.to_string()),
            trashed: false,
            version: Some("7".into()),
            md5_checksum: None,
            sha256_checksum: None,
        };
        assert!(matches!(
            validate_ordinary_file_upload_completion(&session, mismatched),
            Err(DriveApiError::OrdinaryFileUploadPostconditionMismatch)
        ));

        let bad_hash = DriveOrdinaryFileUploadResponse {
            id: "generated-file-id".into(),
            name: "example.bin".into(),
            mime_type: "application/octet-stream".into(),
            parents: vec!["parent-id".into()],
            size: Some(session.total_bytes.to_string()),
            trashed: false,
            version: Some("7".into()),
            md5_checksum: Some("not-a-valid-md5".into()),
            sha256_checksum: None,
        };
        assert!(matches!(
            validate_ordinary_file_upload_completion(&session, bad_hash),
            Err(DriveApiError::InvalidOrdinaryFileUploadChecksum)
        ));
    }

    #[test]
    fn phase5h16_file_name_and_mime_are_fail_closed() {
        assert!(validate_ordinary_file_create_name("example.bin").is_ok());
        assert!(validate_ordinary_upload_mime_type("application/octet-stream").is_ok());
        assert!(matches!(
            validate_ordinary_file_create_name("bad/name"),
            Err(DriveApiError::InvalidOrdinaryFileCreateName)
        ));
        assert!(matches!(
            validate_ordinary_upload_mime_type("application/vnd.google-apps.document"),
            Err(DriveApiError::InvalidOrdinaryFileUploadMimeType)
        ));
    }

    #[test]
    fn phase5h17a_expected_file_recovery_requires_exact_content_identity() {
        let m = DriveOrdinaryFileUploadResponse {
            id: "generated-file-id".into(),
            name: "example.bin".into(),
            mime_type: "application/octet-stream".into(),
            parents: vec!["parent-id".into()],
            size: Some("7".into()),
            trashed: false,
            version: Some("9".into()),
            md5_checksum: None,
            sha256_checksum: Some("a".repeat(64)),
        };
        assert!(
            expected_ordinary_file_matches(
                "generated-file-id",
                "example.bin",
                "parent-id",
                "application/octet-stream",
                7,
                &"a".repeat(64),
                &m
            )
            .unwrap()
        );
        assert!(
            !expected_ordinary_file_matches(
                "generated-file-id",
                "example.bin",
                "parent-id",
                "application/octet-stream",
                8,
                &"a".repeat(64),
                &m
            )
            .unwrap()
        );
        assert!(
            !expected_ordinary_file_matches(
                "generated-file-id",
                "example.bin",
                "parent-id",
                "application/octet-stream",
                7,
                &"b".repeat(64),
                &m
            )
            .unwrap()
        );
    }
    #[test]
    fn phase5h17a_expected_file_recovery_fails_closed_without_sha256() {
        let m = DriveOrdinaryFileUploadResponse {
            id: "generated-file-id".into(),
            name: "example.bin".into(),
            mime_type: "application/octet-stream".into(),
            parents: vec!["parent-id".into()],
            size: Some("7".into()),
            trashed: false,
            version: Some("9".into()),
            md5_checksum: None,
            sha256_checksum: None,
        };
        assert!(
            !expected_ordinary_file_matches(
                "generated-file-id",
                "example.bin",
                "parent-id",
                "application/octet-stream",
                7,
                &"a".repeat(64),
                &m
            )
            .unwrap()
        );
        assert!(matches!(
            validate_expected_sha256(&"A".repeat(64)),
            Err(DriveApiError::InvalidExpectedOrdinaryFileSha256)
        ));
    }

    #[test]
    fn phase5h9_folder_create_postcondition_requires_exact_identity() {
        let exact = DriveFolderMutationResponse {
            id: "generated-folder-id".into(),
            name: "folder".into(),
            mime_type: GOOGLE_DRIVE_FOLDER_MIME_TYPE.into(),
            parents: vec!["parent-id".into()],
            trashed: false,
            version: Some("7".into()),
        };
        assert!(folder_create_response_matches(
            "generated-folder-id",
            "folder",
            "parent-id",
            &exact,
        ));

        let wrong_parent = DriveFolderMutationResponse {
            parents: vec!["different-parent".into()],
            ..exact
        };
        assert!(!folder_create_response_matches(
            "generated-folder-id",
            "folder",
            "parent-id",
            &wrong_parent,
        ));
    }

    #[test]
    fn phase5h9_folder_create_name_validation_is_fail_closed() {
        assert!(validate_folder_create_name("folder").is_ok());
        assert!(matches!(
            validate_folder_create_name(""),
            Err(DriveApiError::InvalidFolderCreateName)
        ));
        assert!(matches!(
            validate_folder_create_name("bad/name"),
            Err(DriveApiError::InvalidFolderCreateName)
        ));
    }

    #[test]
    fn phase5h6_generated_ids_validate_count_uniqueness_and_redaction() {
        let generated = validate_generated_ids_response(
            2,
            DriveGeneratedIdsResponse {
                ids: vec!["generated-id-1".into(), "generated-id-2".into()],
                space: "drive".into(),
                kind: "drive#generatedIds".into(),
            },
        )
        .unwrap();

        assert_eq!(generated.len(), 2);
        assert!(!generated.is_empty());

        let debug = format!("{generated:?}");
        assert!(!debug.contains("generated-id-1"));
        assert!(!debug.contains("generated-id-2"));
        assert!(debug.contains("[redacted]"));
    }

    #[test]
    fn phase5h6_generated_ids_fail_closed_on_bad_response() {
        assert!(matches!(
            validate_generated_ids_response(
                2,
                DriveGeneratedIdsResponse {
                    ids: vec!["generated-id-1".into()],
                    space: "drive".into(),
                    kind: "drive#generatedIds".into(),
                },
            ),
            Err(DriveApiError::GeneratedIdCountMismatch)
        ));

        assert!(matches!(
            validate_generated_ids_response(
                2,
                DriveGeneratedIdsResponse {
                    ids: vec!["generated-id-1".into(), "generated-id-1".into()],
                    space: "drive".into(),
                    kind: "drive#generatedIds".into(),
                },
            ),
            Err(DriveApiError::DuplicateGeneratedId)
        ));

        assert!(matches!(
            validate_generated_ids_response(
                1,
                DriveGeneratedIdsResponse {
                    ids: vec!["generated-id-1".into()],
                    space: "appDataFolder".into(),
                    kind: "drive#generatedIds".into(),
                },
            ),
            Err(DriveApiError::InvalidGeneratedIdsResponse)
        ));
    }

    #[test]
    fn phase5h2_write_authority_parses_version_capabilities_and_redacts_metadata() {
        let observation = validate_write_authority_response(
            "file-id",
            DriveWriteAuthorityResponse {
                id: "file-id".into(),
                mime_type: "text/plain".into(),
                trashed: false,
                version: Some("42".into()),
                md5_checksum: Some("ABCDEFABCDEFABCDEFABCDEFABCDEFAB".into()),
                capabilities: DriveWriteCapabilitiesResponse {
                    can_edit: true,
                    can_trash: true,
                    can_add_children: false,
                },
            },
        )
        .unwrap();

        assert_eq!(observation.remote_version, 42);
        assert_eq!(
            observation.md5_checksum(),
            Some("abcdefabcdefabcdefabcdefabcdefab")
        );
        assert!(observation.can_edit);
        assert!(observation.can_trash);
        assert!(!observation.can_add_children);

        let debug = format!("{observation:?}");
        assert!(!debug.contains("file-id"));
        assert!(!debug.contains("abcdefabcdefabcdefabcdefabcdefab"));
    }

    #[test]
    fn phase5h2_write_authority_rejects_native_trashed_and_invalid_version() {
        let native = DriveWriteAuthorityResponse {
            id: "native".into(),
            mime_type: "application/vnd.google-apps.document".into(),
            trashed: false,
            version: Some("1".into()),
            md5_checksum: None,
            capabilities: DriveWriteCapabilitiesResponse::default(),
        };
        assert!(matches!(
            validate_write_authority_response("native", native),
            Err(DriveApiError::WriteAuthorityProviderNativeUnsupported)
        ));

        let trashed = DriveWriteAuthorityResponse {
            id: "trashed".into(),
            mime_type: "text/plain".into(),
            trashed: true,
            version: Some("2".into()),
            md5_checksum: None,
            capabilities: DriveWriteCapabilitiesResponse::default(),
        };
        assert!(matches!(
            validate_write_authority_response("trashed", trashed),
            Err(DriveApiError::WriteAuthorityItemTrashed)
        ));

        let invalid_version = DriveWriteAuthorityResponse {
            id: "bad-version".into(),
            mime_type: "text/plain".into(),
            trashed: false,
            version: Some("0".into()),
            md5_checksum: None,
            capabilities: DriveWriteCapabilitiesResponse::default(),
        };
        assert!(matches!(
            validate_write_authority_response("bad-version", invalid_version),
            Err(DriveApiError::WriteAuthorityVersionInvalid)
        ));
    }

    #[test]
    fn bounded_download_copy_enforces_limit() {
        let mut input = &b"hello"[..];
        let mut output = Vec::new();
        assert_eq!(
            copy_bounded_download(&mut input, &mut output, 10).unwrap(),
            5
        );
        assert_eq!(output, b"hello");
        let mut input = &b"hello"[..];
        let mut output = Vec::new();
        assert!(matches!(
            copy_bounded_download(&mut input, &mut output, 4),
            Err(DriveApiError::DownloadSafetyLimitExceeded)
        ));
        assert!(output.is_empty());
    }

    #[test]
    fn blob_fingerprint_requires_supported_sha256_metadata() {
        let valid = DriveBlobFingerprintResponse {
            id: "file-id".into(),
            mime_type: "text/plain".into(),
            size: Some("13".into()),
            sha256_checksum: Some("a".repeat(64)),
            trashed: false,
            owned_by_me: true,
        };

        let fingerprint = validate_blob_fingerprint_response("file-id", valid).unwrap();
        assert_eq!(fingerprint.size_bytes, 13);
        assert_eq!(fingerprint.sha256_hex(), "a".repeat(64));

        let missing_hash = DriveBlobFingerprintResponse {
            id: "file-id".into(),
            mime_type: "text/plain".into(),
            size: Some("13".into()),
            sha256_checksum: None,
            trashed: false,
            owned_by_me: true,
        };
        assert!(matches!(
            validate_blob_fingerprint_response("file-id", missing_hash),
            Err(DriveApiError::BlobFingerprintSha256Missing)
        ));

        let native = DriveBlobFingerprintResponse {
            id: "file-id".into(),
            mime_type: "application/vnd.google-apps.document".into(),
            size: None,
            sha256_checksum: None,
            trashed: false,
            owned_by_me: true,
        };
        assert!(matches!(
            validate_blob_fingerprint_response("file-id", native),
            Err(DriveApiError::BlobFingerprintNotOrdinaryFile)
        ));
    }

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
    fn hydration_root_requires_live_folder_metadata() {
        let file = RemoteItem {
            remote_id: "file-root".into(),
            parent_remote_id: Some("parent".into()),
            name: "file.txt".into(),
            kind: RemoteItemKind::File,
            size_bytes: Some(1),
            modified_unix_ms: None,
            trashed: false,
        };
        assert!(matches!(
            validate_hydration_root(&file),
            Err(DriveApiError::HydrationRootNotFolder)
        ));

        let trashed = RemoteItem {
            remote_id: "folder-root".into(),
            parent_remote_id: Some("parent".into()),
            name: "Folder".into(),
            kind: RemoteItemKind::Folder,
            size_bytes: None,
            modified_unix_ms: None,
            trashed: true,
        };
        assert!(matches!(
            validate_hydration_root(&trashed),
            Err(DriveApiError::HydrationRootTrashed)
        ));
    }

    #[test]
    fn hydration_child_requires_exact_parent_and_unique_id() {
        let mut seen = HashSet::from(["hydration-root".to_owned()]);
        let child = RemoteItem {
            remote_id: "child-one".into(),
            parent_remote_id: Some("hydration-root".into()),
            name: "child.txt".into(),
            kind: RemoteItemKind::File,
            size_bytes: Some(1),
            modified_unix_ms: None,
            trashed: false,
        };

        validate_hydration_child("hydration-root", &child, &mut seen).unwrap();

        assert!(matches!(
            validate_hydration_child("hydration-root", &child, &mut seen),
            Err(DriveApiError::HydrationDuplicateRemoteId)
        ));

        let wrong_parent = RemoteItem {
            remote_id: "child-two".into(),
            parent_remote_id: Some("somewhere-else".into()),
            name: "other.txt".into(),
            kind: RemoteItemKind::File,
            size_bytes: Some(1),
            modified_unix_ms: None,
            trashed: false,
        };

        assert!(matches!(
            validate_hydration_child("hydration-root", &wrong_parent, &mut seen),
            Err(DriveApiError::HydrationParentMismatch)
        ));
    }

    #[test]
    fn hydration_result_debug_redacts_remote_metadata() {
        let hydration = DriveSubtreeHydration {
            items: vec![RemoteItem {
                remote_id: "secret-remote-id".into(),
                parent_remote_id: Some("secret-parent-id".into()),
                name: "private-name.txt".into(),
                kind: RemoteItemKind::File,
                size_bytes: Some(1),
                modified_unix_ms: None,
                trashed: false,
            }],
            unsupported_provider_native: 2,
            page_count: 3,
        };

        let debug = format!("{hydration:?}");
        assert!(debug.contains("item_count"));
        assert!(debug.contains("unsupported_provider_native"));
        assert!(debug.contains("page_count"));
        assert!(!debug.contains("secret-remote-id"));
        assert!(!debug.contains("secret-parent-id"));
        assert!(!debug.contains("private-name.txt"));
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
