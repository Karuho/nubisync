use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use rand::{RngCore, rngs::OsRng};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::fmt;
use thiserror::Error;
use url::Url;

pub const GOOGLE_OAUTH_AUTH_ENDPOINT: &str = "https://accounts.google.com/o/oauth2/v2/auth";
pub const GOOGLE_OAUTH_TOKEN_ENDPOINT: &str = "https://oauth2.googleapis.com/token";

pub const GOOGLE_DRIVE_METADATA_READONLY_SCOPE: &str =
    "https://www.googleapis.com/auth/drive.metadata.readonly";
pub const GOOGLE_DRIVE_READONLY_SCOPE: &str = "https://www.googleapis.com/auth/drive.readonly";
pub const GOOGLE_DRIVE_FULL_SCOPE: &str = "https://www.googleapis.com/auth/drive";

const IDENTITY_SCOPES: [&str; 3] = ["openid", "email", "profile"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GoogleDriveAccess {
    MetadataReadOnly,
    ReadOnly,
    FullSync,
}

impl GoogleDriveAccess {
    pub fn scope(self) -> &'static str {
        match self {
            Self::MetadataReadOnly => GOOGLE_DRIVE_METADATA_READONLY_SCOPE,
            Self::ReadOnly => GOOGLE_DRIVE_READONLY_SCOPE,
            Self::FullSync => GOOGLE_DRIVE_FULL_SCOPE,
        }
    }
}

#[derive(Debug, Clone)]
pub struct GoogleOAuthConfig {
    client_id: String,
}

impl GoogleOAuthConfig {
    pub fn new(client_id: impl Into<String>) -> Result<Self, OAuthError> {
        let client_id = client_id.into();
        let normalized = client_id.trim();
        let lowercase = normalized.to_ascii_lowercase();

        if normalized.is_empty()
            || normalized.len() != client_id.len()
            || normalized.chars().any(char::is_whitespace)
            || !normalized.ends_with(".apps.googleusercontent.com")
            || lowercase.contains("tu_client_id")
            || lowercase.contains("your_client_id")
            || lowercase.contains("example")
        {
            return Err(OAuthError::InvalidClientId);
        }

        Ok(Self { client_id })
    }

    pub fn begin_authorization(
        &self,
        loopback_port: u16,
        access: GoogleDriveAccess,
    ) -> Result<OAuthAuthorization, OAuthError> {
        if loopback_port == 0 {
            return Err(OAuthError::InvalidLoopbackPort);
        }

        let code_verifier = random_base64url_32_bytes();
        let code_challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(code_verifier.as_bytes()));
        let state = random_base64url_32_bytes();

        let redirect_uri = Url::parse(&format!("http://127.0.0.1:{loopback_port}"))?;
        let mut authorization_url = Url::parse(GOOGLE_OAUTH_AUTH_ENDPOINT)?;

        let mut scopes = IDENTITY_SCOPES.to_vec();
        scopes.push(access.scope());
        let joined_scopes = scopes.join(" ");

        authorization_url
            .query_pairs_mut()
            .append_pair("client_id", &self.client_id)
            .append_pair("redirect_uri", redirect_uri.as_str())
            .append_pair("response_type", "code")
            .append_pair("scope", &joined_scopes)
            .append_pair("state", &state)
            .append_pair("code_challenge", &code_challenge)
            .append_pair("code_challenge_method", "S256")
            .append_pair("access_type", "offline")
            .append_pair("prompt", "consent");

        Ok(OAuthAuthorization {
            authorization_url,
            redirect_uri,
            state,
            code_verifier,
        })
    }

    pub fn exchange_code(
        &self,
        authorization: &OAuthAuthorization,
        code: &str,
    ) -> Result<OAuthTokens, OAuthError> {
        if code.trim().is_empty() {
            return Err(OAuthError::MissingAuthorizationCode);
        }

        let client = reqwest::blocking::Client::builder()
            .user_agent(concat!("NubiSync/", env!("CARGO_PKG_VERSION")))
            .build()?;

        let response = client
            .post(GOOGLE_OAUTH_TOKEN_ENDPOINT)
            .form(&[
                ("client_id", self.client_id.as_str()),
                ("code", code),
                ("code_verifier", authorization.code_verifier()),
                ("grant_type", "authorization_code"),
                ("redirect_uri", authorization.redirect_uri().as_str()),
            ])
            .send()?;

        let status = response.status();
        if !status.is_success() {
            let error_code = response
                .json::<TokenErrorResponse>()
                .ok()
                .and_then(|body| sanitize_oauth_error_code(&body.error))
                .unwrap_or_else(|| "unknown_error".to_owned());

            return Err(OAuthError::TokenEndpointRejected {
                status: status.as_u16(),
                code: error_code,
            });
        }

        let response: TokenResponse = response.json()?;

        Ok(OAuthTokens {
            access_token: OAuthAccessToken(response.access_token),
            refresh_token: response.refresh_token.map(OAuthRefreshToken),
            expires_in_seconds: response.expires_in,
            token_type: response.token_type,
            scope: response.scope,
        })
    }
}

pub struct OAuthAuthorization {
    authorization_url: Url,
    redirect_uri: Url,
    state: String,
    code_verifier: String,
}

impl OAuthAuthorization {
    pub fn authorization_url(&self) -> &Url {
        &self.authorization_url
    }

    pub fn redirect_uri(&self) -> &Url {
        &self.redirect_uri
    }

    pub fn expected_state(&self) -> &str {
        &self.state
    }

    pub fn code_verifier(&self) -> &str {
        &self.code_verifier
    }

    pub fn state_matches(&self, received_state: &str) -> bool {
        self.state == received_state
    }

    pub fn accept_callback(&self, callback: &Url) -> Result<String, OAuthError> {
        if callback.scheme() != self.redirect_uri.scheme()
            || callback.host_str() != self.redirect_uri.host_str()
            || callback.port_or_known_default() != self.redirect_uri.port_or_known_default()
            || callback.path() != self.redirect_uri.path()
        {
            return Err(OAuthError::InvalidCallback);
        }

        let mut code = None;
        let mut state = None;
        let mut provider_error = None;

        for (key, value) in callback.query_pairs() {
            match key.as_ref() {
                "code" => code = Some(value.into_owned()),
                "state" => state = Some(value.into_owned()),
                "error" => provider_error = Some(value.into_owned()),
                _ => {}
            }
        }

        if provider_error.is_some() {
            return Err(OAuthError::AuthorizationDenied);
        }

        let state = state.ok_or(OAuthError::MissingState)?;
        if !self.state_matches(&state) {
            return Err(OAuthError::StateMismatch);
        }

        let code = code.ok_or(OAuthError::MissingAuthorizationCode)?;
        if code.trim().is_empty() {
            return Err(OAuthError::MissingAuthorizationCode);
        }

        Ok(code)
    }
}

impl fmt::Debug for OAuthAuthorization {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OAuthAuthorization")
            .field("authorization_endpoint", &GOOGLE_OAUTH_AUTH_ENDPOINT)
            .field("redirect_uri", &self.redirect_uri)
            .field("state", &"[redacted]")
            .field("code_verifier", &"[redacted]")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct OAuthAccessToken(String);

impl OAuthAccessToken {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for OAuthAccessToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OAuthAccessToken([redacted])")
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct OAuthRefreshToken(String);

impl OAuthRefreshToken {
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl fmt::Debug for OAuthRefreshToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OAuthRefreshToken([redacted])")
    }
}

pub struct OAuthTokens {
    access_token: OAuthAccessToken,
    refresh_token: Option<OAuthRefreshToken>,
    expires_in_seconds: u64,
    token_type: String,
    scope: Option<String>,
}

impl OAuthTokens {
    pub fn access_token(&self) -> &OAuthAccessToken {
        &self.access_token
    }

    pub fn refresh_token(&self) -> Option<&OAuthRefreshToken> {
        self.refresh_token.as_ref()
    }

    pub fn expires_in_seconds(&self) -> u64 {
        self.expires_in_seconds
    }

    pub fn token_type(&self) -> &str {
        &self.token_type
    }

    pub fn scope(&self) -> Option<&str> {
        self.scope.as_deref()
    }
}

impl fmt::Debug for OAuthTokens {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OAuthTokens")
            .field("access_token", &"[redacted]")
            .field(
                "refresh_token",
                &self.refresh_token.as_ref().map(|_| "[redacted]"),
            )
            .field("expires_in_seconds", &self.expires_in_seconds)
            .field("token_type", &self.token_type)
            .field("scope", &self.scope)
            .finish()
    }
}

#[derive(Debug, Deserialize)]
struct TokenErrorResponse {
    error: String,
}

fn sanitize_oauth_error_code(value: &str) -> Option<String> {
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return None;
    }

    Some(value.to_owned())
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
    refresh_token: Option<String>,
    scope: Option<String>,
    token_type: String,
}

fn random_base64url_32_bytes() -> String {
    let mut random = [0_u8; 32];
    OsRng.fill_bytes(&mut random);
    URL_SAFE_NO_PAD.encode(random)
}

#[derive(Debug, Error)]
pub enum OAuthError {
    #[error("Google OAuth client id is invalid")]
    InvalidClientId,
    #[error("loopback port must be a non-zero local port")]
    InvalidLoopbackPort,
    #[error("OAuth callback origin or path is invalid")]
    InvalidCallback,
    #[error("OAuth callback did not include state")]
    MissingState,
    #[error("OAuth callback state mismatch")]
    StateMismatch,
    #[error("Google authorization was denied")]
    AuthorizationDenied,
    #[error("OAuth callback did not include an authorization code")]
    MissingAuthorizationCode,
    #[error("OAuth URL construction failed")]
    Url(#[from] url::ParseError),
    #[error("Google OAuth token endpoint rejected request (HTTP {status}): {code}")]
    TokenEndpointRejected { status: u16, code: String },
    #[error("OAuth HTTP transport or response parsing failed: {0}")]
    Http(#[from] reqwest::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn client_id_rejects_placeholders_and_malformed_values() {
        assert!(GoogleOAuthConfig::new("123.apps.googleusercontent.com").is_ok());
        assert!(matches!(
            GoogleOAuthConfig::new("TU_CLIENT_ID.apps.googleusercontent.com"),
            Err(OAuthError::InvalidClientId)
        ));
        assert!(matches!(
            GoogleOAuthConfig::new("not-a-google-client-id"),
            Err(OAuthError::InvalidClientId)
        ));
    }

    #[test]
    fn oauth_error_code_sanitizer_rejects_free_form_text() {
        assert_eq!(
            sanitize_oauth_error_code("invalid_grant").as_deref(),
            Some("invalid_grant")
        );
        assert!(sanitize_oauth_error_code("bad token: secret-ish text").is_none());
    }

    #[test]
    fn metadata_flow_uses_loopback_pkce_and_least_privilege_scope() {
        let config = GoogleOAuthConfig::new("test.apps.googleusercontent.com").unwrap();
        let request = config
            .begin_authorization(45123, GoogleDriveAccess::MetadataReadOnly)
            .unwrap();

        assert_eq!(request.redirect_uri().scheme(), "http");
        assert_eq!(request.redirect_uri().host_str(), Some("127.0.0.1"));
        assert_eq!(request.redirect_uri().port(), Some(45123));
        assert_eq!(request.redirect_uri().path(), "/");

        let params: HashMap<_, _> = request
            .authorization_url()
            .query_pairs()
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();

        assert_eq!(
            params.get("response_type").map(String::as_str),
            Some("code")
        );
        assert_eq!(
            params.get("code_challenge_method").map(String::as_str),
            Some("S256")
        );
        assert_eq!(
            params.get("access_type").map(String::as_str),
            Some("offline")
        );

        assert!(!params.contains_key("include_granted_scopes"));

        let scopes = params.get("scope").unwrap();
        assert!(scopes.contains("openid"));
        assert!(scopes.contains("email"));
        assert!(scopes.contains("profile"));
        assert!(scopes.contains(GOOGLE_DRIVE_METADATA_READONLY_SCOPE));
        assert!(
            !scopes
                .split(' ')
                .any(|scope| scope == GOOGLE_DRIVE_FULL_SCOPE)
        );

        assert!(!params.contains_key("client_secret"));
        assert!(request.code_verifier().len() >= 43);
        assert!(request.state_matches(request.expected_state()));
    }

    #[test]
    fn callback_requires_exact_state_and_loopback_origin() {
        let config = GoogleOAuthConfig::new("test.apps.googleusercontent.com").unwrap();
        let request = config
            .begin_authorization(45123, GoogleDriveAccess::MetadataReadOnly)
            .unwrap();

        let valid = Url::parse(&format!(
            "http://127.0.0.1:45123/?code=test-code&state={}",
            request.expected_state()
        ))
        .unwrap();

        assert_eq!(request.accept_callback(&valid).unwrap(), "test-code");

        let wrong_state = Url::parse("http://127.0.0.1:45123/?code=test-code&state=wrong").unwrap();
        assert!(matches!(
            request.accept_callback(&wrong_state),
            Err(OAuthError::StateMismatch)
        ));

        let wrong_host = Url::parse(&format!(
            "http://localhost:45123/?code=test-code&state={}",
            request.expected_state()
        ))
        .unwrap();
        assert!(matches!(
            request.accept_callback(&wrong_host),
            Err(OAuthError::InvalidCallback)
        ));
    }

    #[test]
    fn debug_does_not_expose_pkce_or_state() {
        let config = GoogleOAuthConfig::new("test.apps.googleusercontent.com").unwrap();
        let request = config
            .begin_authorization(45123, GoogleDriveAccess::MetadataReadOnly)
            .unwrap();
        let debug = format!("{request:?}");

        assert!(!debug.contains(request.code_verifier()));
        assert!(!debug.contains(request.expected_state()));
    }

    #[test]
    fn token_debug_is_redacted() {
        let token = OAuthAccessToken("access-secret".into());
        let refresh = OAuthRefreshToken("refresh-secret".into());

        assert!(!format!("{token:?}").contains("access-secret"));
        assert!(!format!("{refresh:?}").contains("refresh-secret"));
    }
}
