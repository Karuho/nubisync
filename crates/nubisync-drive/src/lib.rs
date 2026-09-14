//! Google Drive provider foundations.

#![forbid(unsafe_code)]

mod oauth;

pub use oauth::{
    GOOGLE_DRIVE_FULL_SCOPE, GOOGLE_OAUTH_AUTH_ENDPOINT, GoogleOAuthConfig, OAuthAuthorization,
    OAuthError,
};
