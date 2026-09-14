use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use rand::{RngCore, rngs::OsRng};
use sha2::{Digest, Sha256};
use std::fmt;
use thiserror::Error;
use url::Url;

pub const GOOGLE_OAUTH_AUTH_ENDPOINT: &str = "https://accounts.google.com/o/oauth2/v2/auth";
pub const GOOGLE_DRIVE_FULL_SCOPE: &str = "https://www.googleapis.com/auth/drive";

const IDENTITY_SCOPES: [&str; 3] = ["openid", "email", "profile"];

#[derive(Debug, Clone)]
pub struct GoogleOAuthConfig {
    client_id: String,
}

impl GoogleOAuthConfig {
    pub fn new(client_id: impl Into<String>) -> Result<Self, OAuthError> {
        let client_id = client_id.into();
        if client_id.trim().is_empty() || client_id.chars().any(char::is_whitespace) {
            return Err(OAuthError::InvalidClientId);
        }

        Ok(Self { client_id })
    }

    /// Creates an installed-desktop-app authorization request.
    ///
    /// This method performs no network access.
    pub fn begin_authorization(
        &self,
        loopback_port: u16,
    ) -> Result<OAuthAuthorization, OAuthError> {
        if loopback_port == 0 {
            return Err(OAuthError::InvalidLoopbackPort);
        }

        let code_verifier = random_base64url_32_bytes();
        let code_challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(code_verifier.as_bytes()));
        let state = random_base64url_32_bytes();

        let redirect_uri = Url::parse(&format!("http://127.0.0.1:{loopback_port}/oauth/callback"))?;
        let mut authorization_url = Url::parse(GOOGLE_OAUTH_AUTH_ENDPOINT)?;

        let mut scopes = IDENTITY_SCOPES.to_vec();
        scopes.push(GOOGLE_DRIVE_FULL_SCOPE);
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
            .append_pair("include_granted_scopes", "true");

        Ok(OAuthAuthorization {
            authorization_url,
            redirect_uri,
            state,
            code_verifier,
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
    #[error("OAuth URL construction failed")]
    Url(#[from] url::ParseError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn desktop_flow_uses_loopback_pkce_and_required_scopes() {
        let config = GoogleOAuthConfig::new("test.apps.googleusercontent.com").unwrap();
        let request = config.begin_authorization(45123).unwrap();

        assert_eq!(request.redirect_uri().scheme(), "http");
        assert_eq!(request.redirect_uri().host_str(), Some("127.0.0.1"));
        assert_eq!(request.redirect_uri().port(), Some(45123));

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

        let scopes = params.get("scope").unwrap();
        assert!(scopes.contains("openid"));
        assert!(scopes.contains("email"));
        assert!(scopes.contains("profile"));
        assert!(scopes.contains(GOOGLE_DRIVE_FULL_SCOPE));

        assert!(!params.contains_key("client_secret"));
        assert!(request.code_verifier().len() >= 43);
        assert!(request.state_matches(request.expected_state()));
    }

    #[test]
    fn debug_does_not_expose_pkce_or_state() {
        let config = GoogleOAuthConfig::new("test.apps.googleusercontent.com").unwrap();
        let request = config.begin_authorization(45123).unwrap();
        let debug = format!("{request:?}");

        assert!(!debug.contains(request.code_verifier()));
        assert!(!debug.contains(request.expected_state()));
    }
}
