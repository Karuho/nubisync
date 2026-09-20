//! Google Drive provider foundations.

#![forbid(unsafe_code)]

mod api;
mod oauth;

pub use api::{
    DRIVE_DEFAULT_RESUMABLE_CHUNK_BYTES, DRIVE_RESUMABLE_CHUNK_ALIGNMENT_BYTES, DriveApiError,
    DriveBlobFingerprint, DriveExpectedFileLookup, DriveExpectedFolderLookup,
    DriveFolderCreateSubmission, DriveFolderRoot, DriveGeneratedIds, DriveInventoryPage,
    DriveOrdinaryFileUploadCompletion, DriveProbe, DriveResumableUploadProgress,
    DriveResumableUploadSession, DriveRootMembership, DriveWriteAuthorityObservation,
    GoogleDriveApi, GoogleUserInfo,
};
pub use oauth::{
    GOOGLE_DRIVE_FULL_SCOPE, GOOGLE_DRIVE_METADATA_READONLY_SCOPE, GOOGLE_DRIVE_READONLY_SCOPE,
    GOOGLE_OAUTH_AUTH_ENDPOINT, GOOGLE_OAUTH_TOKEN_ENDPOINT, GoogleDriveAccess, GoogleOAuthConfig,
    OAuthAccessToken, OAuthAuthorization, OAuthError, OAuthRefreshToken, OAuthTokens,
};
