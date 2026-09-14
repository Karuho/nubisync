//! Development CLI for NubiSync.
//!
//! The desktop UI will eventually call the same application services. The CLI
//! exists now so OAuth and provider behavior can be tested deterministically.

#![forbid(unsafe_code)]

use nubisync_auth::{KeyringSecretStore, SecretKey, SecretStore, SecretValue};
use nubisync_core::{ProviderAccount, ProviderId};
use nubisync_drive::{GoogleDriveAccess, GoogleDriveApi, GoogleOAuthConfig};
use nubisync_storage::Storage;
use std::{
    env, fs,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use tiny_http::{Method, Response, Server};
use url::Url;

const GOOGLE_CLIENT_ID_ENV: &str = "NUBISYNC_GOOGLE_CLIENT_ID";
const GOOGLE_CLIENT_SECRET_ENV: &str = "NUBISYNC_GOOGLE_CLIENT_SECRET";
const REFRESH_TOKEN_PURPOSE: &str = "refresh-token";

fn main() {
    if let Err(error) = run() {
        eprintln!("NubiSync error: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), CliError> {
    let args: Vec<String> = env::args().skip(1).collect();

    match args.as_slice() {
        [] => {
            print_help();
            Ok(())
        }
        [single] if single == "help" || single == "--help" || single == "-h" => {
            print_help();
            Ok(())
        }
        [single] if single == "--version" || single == "-V" => {
            println!("nubisync {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        [auth, google, login] if auth == "auth" && google == "google" && login == "login" => {
            google_login()
        }
        [auth, keyring, check] if auth == "auth" && keyring == "keyring" && check == "check" => {
            keyring_check()
        }
        _ => {
            print_help();
            Err(CliError::InvalidArguments)
        }
    }
}

fn print_help() {
    println!(
        "\
NubiSync — Native cloud sync for Linux

USAGE:
  nubisync help
  nubisync --version
  nubisync auth keyring check
  nubisync auth google login

GOOGLE DEVELOPMENT LOGIN:
  Set the public OAuth Desktop client ID in the environment:

    export NUBISYNC_GOOGLE_CLIENT_ID='...apps.googleusercontent.com'
    export NUBISYNC_GOOGLE_CLIENT_SECRET='...'
    cargo run -p nubisync-cli -- auth google login

SECURITY:
  - the Desktop OAuth client secret is supplied at runtime and is never logged
  - refresh tokens are stored in the OS credential store
  - this Phase 2 command requests metadata-only Drive access
  - it does not list filenames, download files, or write to Drive
"
    );
}

fn keyring_check() -> Result<(), CliError> {
    if KeyringSecretStore::is_available() {
        println!("KEYRING_STATUS=AVAILABLE");
        Ok(())
    } else {
        Err(CliError::KeyringUnavailable)
    }
}

fn google_login() -> Result<(), CliError> {
    if !KeyringSecretStore::is_available() {
        return Err(CliError::KeyringUnavailable);
    }

    let client_id = env::var(GOOGLE_CLIENT_ID_ENV).map_err(|_| CliError::MissingGoogleClientId)?;
    let client_secret =
        env::var(GOOGLE_CLIENT_SECRET_ENV).map_err(|_| CliError::MissingGoogleClientSecret)?;
    let oauth = GoogleOAuthConfig::new(client_id)?;

    let server = Server::http("127.0.0.1:0").map_err(|_| CliError::LoopbackBindFailed)?;
    let listen_addr = server
        .server_addr()
        .to_ip()
        .ok_or(CliError::LoopbackBindFailed)?;

    if !listen_addr.ip().is_loopback() {
        return Err(CliError::LoopbackBindFailed);
    }

    let authorization =
        oauth.begin_authorization(listen_addr.port(), GoogleDriveAccess::MetadataReadOnly)?;

    println!("GOOGLE_AUTH_MODE=METADATA_READONLY");
    println!("LOOPBACK_PORT={}", listen_addr.port());
    println!("Opening Google authorization in your default browser...");

    webbrowser::open(authorization.authorization_url().as_str())
        .map_err(|_| CliError::BrowserOpenFailed)?;

    let request = server
        .recv_timeout(Duration::from_secs(180))
        .map_err(|_| CliError::CallbackReceiveFailed)?
        .ok_or(CliError::CallbackTimeout)?;

    if request.method() != &Method::Get {
        let _ = request.respond(
            Response::from_string("NubiSync rejected this callback method.").with_status_code(405),
        );
        return Err(CliError::InvalidCallbackMethod);
    }

    let callback = Url::parse(&format!(
        "http://127.0.0.1:{}{}",
        listen_addr.port(),
        request.url()
    ))?;

    let code = match authorization.accept_callback(&callback) {
        Ok(code) => {
            let _ = request.respond(Response::from_string(
                "NubiSync received the Google authorization. You can close this tab.",
            ));
            code
        }
        Err(error) => {
            let _ = request.respond(
                Response::from_string(
                    "NubiSync rejected the OAuth callback. Return to the terminal.",
                )
                .with_status_code(400),
            );
            return Err(error.into());
        }
    };

    let tokens = oauth.exchange_code(&authorization, &code, &client_secret)?;
    let api = GoogleDriveApi::new(tokens.access_token().clone())?;
    let user = api.user_info()?;

    if user.sub.trim().is_empty() {
        return Err(CliError::InvalidGoogleSubject);
    }

    let provider = ProviderId::new("google-drive")?;
    let account = ProviderAccount::new(
        provider.clone(),
        user.sub.clone(),
        user.email.clone(),
        user.name.clone(),
    )?;

    let keyring = KeyringSecretStore::default();
    let refresh_key = SecretKey::new("google-drive", &user.sub, REFRESH_TOKEN_PURPOSE)?;

    if let Some(refresh_token) = tokens.refresh_token() {
        keyring.put(
            &refresh_key,
            SecretValue::new(refresh_token.as_bytes().to_vec())?,
        )?;
    } else if keyring.get(&refresh_key)?.is_none() {
        return Err(CliError::MissingRefreshToken);
    }

    let probe = api.probe()?;

    let data_dir = nubisync_data_dir()?;
    fs::create_dir_all(&data_dir)?;
    let db_path = data_dir.join("nubisync.db");
    let storage = Storage::open(&db_path)?;

    let now = unix_time_ms()?;
    storage.upsert_account(&account, now)?;
    storage.save_cursor(&provider, &user.sub, &probe.change_cursor, now)?;

    println!("GOOGLE_AUTH=PASS");
    println!("ACCOUNT_SUBJECT_STORED=yes");
    println!(
        "ACCOUNT_EMAIL={}",
        user.email.as_deref().unwrap_or("(not returned)")
    );
    println!(
        "ACCOUNT_NAME={}",
        user.name.as_deref().unwrap_or("(not returned)")
    );
    println!("REFRESH_TOKEN_STORAGE=OS_KEYRING");
    println!("DRIVE_WRITE_ACCESS=no");
    println!("DRIVE_FILE_LISTING_PERFORMED=no");
    println!("DRIVE_CHANGE_CURSOR_STORED=yes");
    println!("DATABASE={}", db_path.display());

    if let Some(usage) = probe.usage_in_drive_bytes {
        println!("DRIVE_USAGE_BYTES={usage}");
    }

    if let Some(limit) = probe.storage_limit_bytes {
        println!("DRIVE_STORAGE_LIMIT_BYTES={limit}");
    }

    if let Some(max_upload_size) = probe.max_upload_size_bytes {
        println!("DRIVE_MAX_UPLOAD_SIZE_BYTES={max_upload_size}");
    }

    Ok(())
}

fn nubisync_data_dir() -> Result<PathBuf, CliError> {
    if let Some(path) = env::var_os("XDG_DATA_HOME") {
        return Ok(PathBuf::from(path).join("nubisync"));
    }

    let home = env::var_os("HOME").ok_or(CliError::HomeDirectoryUnavailable)?;
    Ok(PathBuf::from(home)
        .join(".local")
        .join("share")
        .join("nubisync"))
}

fn unix_time_ms() -> Result<i64, CliError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| CliError::ClockBeforeUnixEpoch)?;

    i64::try_from(duration.as_millis()).map_err(|_| CliError::ClockOverflow)
}

#[derive(Debug, Error)]
enum CliError {
    #[error("invalid command; run `nubisync help`")]
    InvalidArguments,
    #[error("NUBISYNC_GOOGLE_CLIENT_ID is not set")]
    MissingGoogleClientId,
    #[error("NUBISYNC_GOOGLE_CLIENT_SECRET is not set")]
    MissingGoogleClientSecret,
    #[error("the operating-system credential store is unavailable")]
    KeyringUnavailable,
    #[error("failed to bind the OAuth callback to loopback")]
    LoopbackBindFailed,
    #[error("failed to open the system browser")]
    BrowserOpenFailed,
    #[error("OAuth callback timed out")]
    CallbackTimeout,
    #[error("failed while waiting for the OAuth callback")]
    CallbackReceiveFailed,
    #[error("OAuth callback used an unexpected HTTP method")]
    InvalidCallbackMethod,
    #[error("Google did not return a stable account subject")]
    InvalidGoogleSubject,
    #[error(
        "Google did not return a refresh token and NubiSync has no existing token for this account"
    )]
    MissingRefreshToken,
    #[error("HOME is unavailable")]
    HomeDirectoryUnavailable,
    #[error("system clock is before the Unix epoch")]
    ClockBeforeUnixEpoch,
    #[error("system time does not fit in NubiSync timestamp storage")]
    ClockOverflow,
    #[error(transparent)]
    OAuth(#[from] nubisync_drive::OAuthError),
    #[error(transparent)]
    Drive(#[from] nubisync_drive::DriveApiError),
    #[error(transparent)]
    Core(#[from] nubisync_core::CoreError),
    #[error(transparent)]
    Secrets(#[from] nubisync_auth::SecretStoreError),
    #[error(transparent)]
    Storage(#[from] nubisync_storage::StorageError),
    #[error("local filesystem operation failed")]
    Io(#[from] std::io::Error),
    #[error("OAuth callback URL was invalid")]
    Url(#[from] url::ParseError),
}
