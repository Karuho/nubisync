//! Development CLI for NubiSync.
//!
//! The desktop UI will eventually call the same application services. The CLI
//! exists now so OAuth and provider behavior can be tested deterministically.

#![forbid(unsafe_code)]

use nubisync_auth::{KeyringSecretStore, SecretKey, SecretStore, SecretValue};
use nubisync_core::{ProviderAccount, ProviderId, RemoteChange, RemoteItemKind};
use nubisync_drive::{GoogleDriveAccess, GoogleDriveApi, GoogleOAuthConfig};
use nubisync_storage::Storage;
use std::{
    collections::HashSet,
    env, fs,
    io::{self, Write},
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use tiny_http::{Method, Response, Server};
use url::Url;

const REFRESH_TOKEN_PURPOSE: &str = "refresh-token";
const OAUTH_CLIENT_SUBJECT: &str = "oauth-desktop-client";
const OAUTH_CLIENT_ID_PURPOSE: &str = "client-id";
const OAUTH_CLIENT_SECRET_PURPOSE: &str = "client-secret";

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
        [auth, google, configure]
            if auth == "auth" && google == "google" && configure == "configure" =>
        {
            google_configure()
        }
        [auth, google, status] if auth == "auth" && google == "google" && status == "status" => {
            google_status()
        }
        [auth, google, refresh] if auth == "auth" && google == "google" && refresh == "refresh" => {
            google_refresh()
        }
        [auth, google, logout] if auth == "auth" && google == "google" && logout == "logout" => {
            google_logout()
        }
        [drive, changes] if drive == "drive" && changes == "changes" => drive_changes(),
        [drive, catalog, status]
            if drive == "drive" && catalog == "catalog" && status == "status" =>
        {
            drive_catalog_status()
        }
        [drive, inventory] if drive == "drive" && inventory == "inventory" => {
            drive_inventory(Some(20))
        }
        [drive, inventory, full]
            if drive == "drive" && inventory == "inventory" && full == "--full" =>
        {
            drive_inventory(None)
        }
        [drive, inventory, limit, value]
            if drive == "drive" && inventory == "inventory" && limit == "--limit" =>
        {
            drive_inventory(Some(parse_inventory_limit(value)?))
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
  nubisync auth google configure
  nubisync auth google status
  nubisync auth google refresh
  nubisync auth google logout
  nubisync drive changes
  nubisync drive catalog status
  nubisync drive inventory
  nubisync drive inventory --limit <1-10000>
  nubisync drive inventory --full

GOOGLE DEVELOPMENT CLIENT CONFIG:
  Store the development Desktop OAuth client directly in the OS credential store:

    cargo run -p nubisync-cli -- auth google configure

  The command prompts for the Client ID and hides the Client Secret while typing.
  Login, refresh, and status then use the persistent OS-keyring configuration.

SECURITY:
  - Desktop OAuth client configuration is never printed
  - user refresh tokens are stored in the OS credential store
  - access tokens remain memory-only
  - auth status performs no network request
  - logout removes the user refresh token but retains local sync metadata
"
    );
}

fn keyring_check() -> Result<(), CliError> {
    ensure_keyring_available()?;
    println!("KEYRING_STATUS=AVAILABLE");
    Ok(())
}

fn google_configure() -> Result<(), CliError> {
    ensure_keyring_available()?;

    let client_id = prompt_line("Google Client ID: ")?;
    let client_secret = rpassword::prompt_password("Google Client Secret: ")?;

    let _ = GoogleOAuthConfig::new(client_id.clone())?;
    validate_client_secret_text(&client_secret)?;

    let keyring = KeyringSecretStore::default();
    store_google_client_config(&keyring, &client_id, &client_secret)?;

    let (stored_client_id, stored_client_secret) = load_google_client_config(&keyring)?;
    if stored_client_id.as_bytes() != client_id.as_bytes()
        || stored_client_secret.as_bytes() != client_secret.as_bytes()
    {
        return Err(CliError::StoredGoogleClientConfigMismatch);
    }

    println!("GOOGLE_CLIENT_CONFIG=PASS");
    println!("CLIENT_ID_STORAGE=OS_KEYRING");
    println!("CLIENT_SECRET_STORAGE=OS_KEYRING");
    println!("CLIENT_ID_ROUNDTRIP_MATCH=yes");
    println!("CLIENT_SECRET_ROUNDTRIP_MATCH=yes");
    println!("CLIENT_VALUES_PRINTED=no");

    Ok(())
}

fn google_status() -> Result<(), CliError> {
    ensure_keyring_available()?;

    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        println!("GOOGLE_STATUS=NOT_CONNECTED");
        println!("DATABASE_PRESENT=no");
        println!("NETWORK_CHECK=not_performed");
        return Ok(());
    }

    let storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let accounts = storage.list_accounts(&provider)?;

    if accounts.is_empty() {
        println!("GOOGLE_STATUS=NOT_CONNECTED");
        println!("DATABASE_PRESENT=yes");
        println!("NETWORK_CHECK=not_performed");
        return Ok(());
    }

    let account = single_google_account(accounts)?;
    let keyring = KeyringSecretStore::default();

    let refresh_present = keyring
        .get(&refresh_token_key(&account.subject)?)?
        .is_some();
    let (client_id_key, client_secret_key) = google_client_config_keys()?;
    let client_id_present = keyring.get(&client_id_key)?.is_some();
    let client_secret_present = keyring.get(&client_secret_key)?.is_some();
    let client_config_present = client_id_present && client_secret_present;

    let status = if refresh_present && client_config_present {
        "CONNECTED"
    } else {
        "INCOMPLETE"
    };

    println!("GOOGLE_STATUS={status}");
    println!(
        "ACCOUNT_EMAIL={}",
        account.email.as_deref().unwrap_or("(not returned)")
    );
    println!("REFRESH_TOKEN_PRESENT={}", yes_no(refresh_present));
    println!("CLIENT_CONFIG_PRESENT={}", yes_no(client_config_present));
    println!("DATABASE_PRESENT=yes");
    println!("NETWORK_CHECK=not_performed");

    Ok(())
}

fn google_refresh() -> Result<(), CliError> {
    ensure_keyring_available()?;

    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;

    let keyring = KeyringSecretStore::default();
    let refresh_key = refresh_token_key(&account.subject)?;
    let refresh_token = required_secret_utf8(
        keyring.get(&refresh_key)?,
        CliError::MissingStoredRefreshToken,
    )?;

    let (client_id, client_secret) = load_google_client_config(&keyring)?;

    let oauth = GoogleOAuthConfig::new(client_id)?;
    let tokens = oauth.refresh_access_token(&refresh_token, &client_secret)?;

    if let Some(rotated_refresh_token) = tokens.refresh_token() {
        keyring.put(
            &refresh_key,
            SecretValue::new(rotated_refresh_token.as_bytes().to_vec())?,
        )?;
    }

    let api = GoogleDriveApi::new(tokens.access_token().clone())?;
    let user = api.user_info()?;

    if user.sub != account.subject {
        return Err(CliError::GoogleAccountMismatch);
    }

    println!("GOOGLE_REFRESH=PASS");
    println!("ACCOUNT_SUBJECT_MATCH=yes");
    println!("ACCESS_TOKEN_STORAGE=memory_only");
    println!("REFRESH_TOKEN_STORAGE=OS_KEYRING");
    println!(
        "REFRESH_TOKEN_ROTATED={}",
        yes_no(tokens.refresh_token().is_some())
    );
    println!("DRIVE_WRITE_ACCESS=no");

    Ok(())
}

fn google_logout() -> Result<(), CliError> {
    ensure_keyring_available()?;

    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        println!("GOOGLE_LOGOUT=NOT_CONNECTED");
        return Ok(());
    }

    let storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let accounts = storage.list_accounts(&provider)?;

    if accounts.is_empty() {
        println!("GOOGLE_LOGOUT=NOT_CONNECTED");
        return Ok(());
    }

    let account = single_google_account(accounts)?;
    let keyring = KeyringSecretStore::default();
    keyring.delete(&refresh_token_key(&account.subject)?)?;

    println!("GOOGLE_LOGOUT=PASS");
    println!("REFRESH_TOKEN_REMOVED=yes");
    println!("CLIENT_CONFIG_RETAINED=yes");
    println!("LOCAL_METADATA_RETAINED=yes");

    Ok(())
}

fn drive_catalog_status() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;

    let state = storage.remote_inventory_state(&provider, &account.subject)?;
    let authoritative_items = storage.remote_inventory_count(&provider, &account.subject)?;
    let staging_items = storage.staged_remote_inventory_count(&provider, &account.subject)?;
    let pending_remote_events = storage.pending_remote_event_count(&provider, &account.subject)?;

    let state_consistent = state.item_count == authoritative_items;
    let ready_for_reconciliation =
        state.ready_for_reconciliation() && state_consistent && staging_items == 0;

    println!("DRIVE_CATALOG_STATUS=PASS");
    println!("SNAPSHOT_COMPLETE={}", yes_no(state.snapshot_complete));
    println!("CATCHUP_COMPLETE={}", yes_no(state.catchup_complete));
    println!("STATE_ITEM_COUNT={}", state.item_count);
    println!("AUTHORITATIVE_ITEMS={authoritative_items}");
    println!("STAGING_ITEMS={staging_items}");
    println!("PENDING_REMOTE_EVENTS={pending_remote_events}");
    println!("STATE_CONSISTENT={}", yes_no(state_consistent));
    println!(
        "READY_FOR_RECONCILIATION={}",
        yes_no(ready_for_reconciliation)
    );
    println!("NETWORK_CHECK=not_performed");
    println!("REMOTE_METADATA_PRINTED=no");

    Ok(())
}

fn parse_inventory_limit(value: &str) -> Result<u64, CliError> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| CliError::InvalidInventoryLimit)?;

    if !(1..=10_000).contains(&parsed) {
        return Err(CliError::InvalidInventoryLimit);
    }

    Ok(parsed)
}

fn drive_inventory(max_items: Option<u64>) -> Result<(), CliError> {
    println!("DRIVE_INVENTORY_STAGE=local_session");
    ensure_keyring_available()?;

    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let mut storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;

    let keyring = KeyringSecretStore::default();
    let refresh_key = refresh_token_key(&account.subject)?;
    let refresh_token = required_secret_utf8(
        keyring.get(&refresh_key)?,
        CliError::MissingStoredRefreshToken,
    )?;
    let (client_id, client_secret) = load_google_client_config(&keyring)?;

    println!("DRIVE_INVENTORY_STAGE=refresh_access_token");
    let oauth = GoogleOAuthConfig::new(client_id)?;
    let tokens = oauth.refresh_access_token(&refresh_token, &client_secret)?;

    if let Some(rotated_refresh_token) = tokens.refresh_token() {
        keyring.put(
            &refresh_key,
            SecretValue::new(rotated_refresh_token.as_bytes().to_vec())?,
        )?;
    }

    println!("DRIVE_INVENTORY_STAGE=verify_account");
    let api = GoogleDriveApi::new(tokens.access_token().clone())?;
    let user = api.user_info()?;
    if user.sub != account.subject {
        return Err(CliError::GoogleAccountMismatch);
    }

    storage.begin_remote_inventory_staging(&provider, &account.subject)?;
    let observed_at_unix_ms = unix_time_ms()?;

    let mut continuation = None;
    let mut seen_continuations = HashSet::new();
    let mut pages_fetched = 0_u64;
    let mut observed_items = 0_u64;
    let mut supported_items = 0_u64;
    let mut files = 0_u64;
    let mut folders = 0_u64;
    let mut unsupported_provider_native = 0_u64;
    let mut inventory_complete = false;
    let mut limit_reached = false;

    loop {
        if pages_fetched >= 10_000 {
            return Err(CliError::DriveInventoryPageLimitExceeded);
        }

        let page_size = match max_items {
            Some(limit) => {
                if observed_items >= limit {
                    limit_reached = true;
                    break;
                }

                u16::try_from((limit - observed_items).min(1000))
                    .map_err(|_| CliError::InvalidInventoryLimit)?
            }
            None => 1000,
        };

        let page_number = pages_fetched + 1;
        println!("DRIVE_INVENTORY_FETCH_PAGE={page_number}");

        let page = api.list_inventory_page(continuation.as_ref(), page_size)?;
        pages_fetched += 1;
        storage.stage_remote_inventory_items(
            &provider,
            &account.subject,
            &page.items,
            observed_at_unix_ms,
        )?;

        let page_items = page.supported_items + page.unsupported_provider_native;
        observed_items += page_items;
        supported_items += page.supported_items;
        files += page.file_count;
        folders += page.folder_count;
        unsupported_provider_native += page.unsupported_provider_native;

        println!(
            "DRIVE_INVENTORY_PAGE_COMPLETE={} OBSERVED_SO_FAR={} SUPPORTED_SO_FAR={} FILES_SO_FAR={} FOLDERS_SO_FAR={} UNSUPPORTED_NATIVE_SO_FAR={}",
            pages_fetched,
            observed_items,
            supported_items,
            files,
            folders,
            unsupported_provider_native
        );

        match page.continuation {
            Some(next) => {
                if !seen_continuations.insert(next.as_str().to_owned()) {
                    return Err(CliError::DriveInventoryPaginationLoop);
                }

                continuation = Some(next);

                if max_items.is_some_and(|limit| observed_items >= limit) {
                    limit_reached = true;
                    break;
                }
            }
            None => {
                inventory_complete = true;
                break;
            }
        }
    }

    let staged_items = storage.staged_remote_inventory_count(&provider, &account.subject)?;
    let authoritative_snapshot_committed = inventory_complete && max_items.is_none();
    let authoritative_items = if authoritative_snapshot_committed {
        u64::try_from(storage.commit_remote_inventory_snapshot(
            &provider,
            &account.subject,
            unix_time_ms()?,
        )?)
        .map_err(|_| CliError::NumericOverflow)?
    } else {
        storage.clear_remote_inventory_staging(&provider, &account.subject)?;
        storage.remote_inventory_count(&provider, &account.subject)?
    };

    println!("DRIVE_INVENTORY=PASS");
    println!(
        "MODE={}",
        if max_items.is_some() {
            "bounded"
        } else {
            "full"
        }
    );
    match max_items {
        Some(limit) => println!("MAX_ITEMS={limit}"),
        None => println!("MAX_ITEMS=unlimited"),
    }
    println!("INVENTORY_COMPLETE={}", yes_no(inventory_complete));
    println!("LIMIT_REACHED={}", yes_no(limit_reached));
    println!("PAGES_FETCHED={pages_fetched}");
    println!("OBSERVED_ITEMS={observed_items}");
    println!("SUPPORTED_ITEMS={supported_items}");
    println!("FILES={files}");
    println!("FOLDERS={folders}");
    println!("UNSUPPORTED_PROVIDER_NATIVE={unsupported_provider_native}");
    println!("STAGED_ITEMS={staged_items}");
    println!(
        "AUTHORITATIVE_SNAPSHOT_COMMITTED={}",
        yes_no(authoritative_snapshot_committed)
    );
    println!("AUTHORITATIVE_ITEMS={authoritative_items}");
    println!(
        "STAGING_CLEARED={}",
        yes_no(!authoritative_snapshot_committed)
    );
    println!(
        "INVENTORY_PERSISTED={}",
        yes_no(authoritative_snapshot_committed)
    );
    println!("PROVIDER_CURSOR_MODIFIED=no");
    println!("REMOTE_EVENTS_MODIFIED=no");
    println!("REMOTE_METADATA_PRINTED=no");
    println!("FILE_CONTENT_ACCESSED=no");
    println!("DRIVE_WRITE_ACCESS=no");

    Ok(())
}

fn drive_changes() -> Result<(), CliError> {
    ensure_keyring_available()?;

    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let mut storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;

    let keyring = KeyringSecretStore::default();
    let refresh_key = refresh_token_key(&account.subject)?;
    let refresh_token = required_secret_utf8(
        keyring.get(&refresh_key)?,
        CliError::MissingStoredRefreshToken,
    )?;
    let (client_id, client_secret) = load_google_client_config(&keyring)?;

    let oauth = GoogleOAuthConfig::new(client_id)?;
    let tokens = oauth.refresh_access_token(&refresh_token, &client_secret)?;

    if let Some(rotated_refresh_token) = tokens.refresh_token() {
        keyring.put(
            &refresh_key,
            SecretValue::new(rotated_refresh_token.as_bytes().to_vec())?,
        )?;
    }

    let api = GoogleDriveApi::new(tokens.access_token().clone())?;
    let user = api.user_info()?;
    if user.sub != account.subject {
        return Err(CliError::GoogleAccountMismatch);
    }

    let cursor = storage
        .load_cursor(&provider, &account.subject)?
        .ok_or(CliError::MissingStoredDriveCursor)?;

    let mut continuation = None;
    let mut seen_continuations = HashSet::new();
    let mut pages_fetched = 0_u64;
    let mut changes_total = 0_u64;
    let mut upserts = 0_u64;
    let mut deletes = 0_u64;
    let mut files = 0_u64;
    let mut folders = 0_u64;
    let mut trashed = 0_u64;
    let mut collected_changes = Vec::new();

    let checkpoint = loop {
        if pages_fetched >= 10_000 {
            return Err(CliError::DriveChangePageLimitExceeded);
        }

        let page = api.list_changes_page(&cursor, continuation.as_ref())?;
        pages_fetched += 1;

        for change in page.changes {
            changes_total += 1;

            match &change {
                RemoteChange::Delete { .. } => {
                    deletes += 1;
                }
                RemoteChange::Upsert(item) => {
                    upserts += 1;

                    if item.trashed {
                        trashed += 1;
                    }

                    match item.kind {
                        RemoteItemKind::File => files += 1,
                        RemoteItemKind::Folder => folders += 1,
                    }
                }
            }

            collected_changes.push(change);
        }

        match (page.continuation, page.checkpoint) {
            (Some(next), None) => {
                if !seen_continuations.insert(next.as_str().to_owned()) {
                    return Err(CliError::DriveChangePaginationLoop);
                }
                continuation = Some(next);
            }
            (None, Some(checkpoint)) => break checkpoint,
            _ => return Err(CliError::DriveChangeStreamMissingCheckpoint),
        }
    };

    let cursor_changed = checkpoint != cursor;
    let observed_at_unix_ms = unix_time_ms()?;

    let persisted = storage.commit_remote_changes_and_cursor(
        &provider,
        &account.subject,
        &collected_changes,
        &checkpoint,
        observed_at_unix_ms,
    )?;

    let pending_total = storage.pending_remote_event_count(&provider, &account.subject)?;

    println!("DRIVE_CHANGES=PASS");
    println!("PAGES_FETCHED={pages_fetched}");
    println!("CHANGES_TOTAL={changes_total}");
    println!("UPSERTS={upserts}");
    println!("DELETES={deletes}");
    println!("FILES={files}");
    println!("FOLDERS={folders}");
    println!("TRASHED={trashed}");
    println!("REMOTE_EVENTS_PERSISTED={persisted}");
    println!("REMOTE_EVENTS_PENDING_TOTAL={pending_total}");
    println!("CURSOR_AND_EVENTS_ATOMIC=yes");
    println!("CHECKPOINT_COMMITTED=yes");
    println!("CURSOR_CHANGED={}", yes_no(cursor_changed));
    println!("REMOTE_METADATA_PRINTED=no");
    println!("FILE_CONTENT_ACCESSED=no");
    println!("DRIVE_WRITE_ACCESS=no");

    Ok(())
}

fn google_login() -> Result<(), CliError> {
    ensure_keyring_available()?;

    let keyring = KeyringSecretStore::default();
    let (client_id, client_secret) = load_google_client_config(&keyring)?;
    let oauth = GoogleOAuthConfig::new(client_id.clone())?;
    validate_client_secret_text(&client_secret)?;

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

    let refresh_key = refresh_token_key(&user.sub)?;

    if let Some(refresh_token) = tokens.refresh_token() {
        keyring.put(
            &refresh_key,
            SecretValue::new(refresh_token.as_bytes().to_vec())?,
        )?;
    } else if keyring.get(&refresh_key)?.is_none() {
        return Err(CliError::MissingRefreshToken);
    }

    store_google_client_config(&keyring, &client_id, &client_secret)?;

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
    println!("CLIENT_CONFIG_STORAGE=OS_KEYRING");
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

fn ensure_keyring_available() -> Result<(), CliError> {
    if KeyringSecretStore::is_available() {
        Ok(())
    } else {
        Err(CliError::KeyringUnavailable)
    }
}

fn validate_client_secret_text(value: &str) -> Result<(), CliError> {
    if value.trim().is_empty()
        || value.len() != value.trim().len()
        || value.chars().any(char::is_whitespace)
    {
        return Err(CliError::InvalidGoogleClientSecret);
    }

    Ok(())
}

fn prompt_line(prompt: &str) -> Result<String, CliError> {
    print!("{prompt}");
    io::stdout().flush()?;

    let mut value = String::new();
    io::stdin().read_line(&mut value)?;

    while value.ends_with(['\n', '\r']) {
        value.pop();
    }

    Ok(value)
}

fn store_google_client_config(
    keyring: &KeyringSecretStore,
    client_id: &str,
    client_secret: &str,
) -> Result<(), CliError> {
    let (client_id_key, client_secret_key) = google_client_config_keys()?;
    keyring.put(
        &client_id_key,
        SecretValue::new(client_id.as_bytes().to_vec())?,
    )?;
    keyring.put(
        &client_secret_key,
        SecretValue::new(client_secret.as_bytes().to_vec())?,
    )?;
    Ok(())
}

fn load_google_client_config(keyring: &KeyringSecretStore) -> Result<(String, String), CliError> {
    let (client_id_key, client_secret_key) = google_client_config_keys()?;
    let client_id = required_secret_utf8(
        keyring.get(&client_id_key)?,
        CliError::MissingStoredGoogleClientConfig,
    )?;
    let client_secret = required_secret_utf8(
        keyring.get(&client_secret_key)?,
        CliError::MissingStoredGoogleClientConfig,
    )?;

    Ok((client_id, client_secret))
}

fn google_client_config_keys() -> Result<(SecretKey, SecretKey), CliError> {
    Ok((
        SecretKey::new(
            "google-drive",
            OAUTH_CLIENT_SUBJECT,
            OAUTH_CLIENT_ID_PURPOSE,
        )?,
        SecretKey::new(
            "google-drive",
            OAUTH_CLIENT_SUBJECT,
            OAUTH_CLIENT_SECRET_PURPOSE,
        )?,
    ))
}

fn refresh_token_key(account_subject: &str) -> Result<SecretKey, CliError> {
    Ok(SecretKey::new(
        "google-drive",
        account_subject,
        REFRESH_TOKEN_PURPOSE,
    )?)
}

fn required_secret_utf8(
    secret: Option<SecretValue>,
    missing_error: CliError,
) -> Result<String, CliError> {
    let secret = secret.ok_or(missing_error)?;
    String::from_utf8(secret.expose_bytes().to_vec()).map_err(|_| CliError::InvalidStoredSecret)
}

fn single_google_account(accounts: Vec<ProviderAccount>) -> Result<ProviderAccount, CliError> {
    match accounts.len() {
        0 => Err(CliError::NoLocalGoogleAccount),
        1 => Ok(accounts.into_iter().next().expect("length checked")),
        _ => Err(CliError::MultipleGoogleAccountsUnsupported),
    }
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn nubisync_database_path() -> Result<PathBuf, CliError> {
    Ok(nubisync_data_dir()?.join("nubisync.db"))
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
    #[error("Google OAuth Desktop client secret is invalid")]
    InvalidGoogleClientSecret,
    #[error("the operating-system credential store is unavailable")]
    KeyringUnavailable,
    #[error("no local Google account is configured")]
    NoLocalGoogleAccount,
    #[error("multiple Google accounts are not supported in this NubiSync alpha")]
    MultipleGoogleAccountsUnsupported,
    #[error("stored Google refresh token is missing")]
    MissingStoredRefreshToken,
    #[error("stored Google Drive change cursor is missing")]
    MissingStoredDriveCursor,
    #[error("Google Drive change pagination repeated a continuation token")]
    DriveChangePaginationLoop,
    #[error("Google Drive change pagination exceeded the safety limit")]
    DriveChangePageLimitExceeded,
    #[error("Google Drive inventory pagination repeated a continuation token")]
    DriveInventoryPaginationLoop,
    #[error("Google Drive inventory pagination exceeded the safety limit")]
    DriveInventoryPageLimitExceeded,
    #[error("inventory limit must be an integer between 1 and 10000")]
    InvalidInventoryLimit,
    #[error("numeric value does not fit CLI counters")]
    NumericOverflow,
    #[error("Google Drive change stream ended without a durable checkpoint")]
    DriveChangeStreamMissingCheckpoint,
    #[error("stored Google OAuth client configuration is missing")]
    MissingStoredGoogleClientConfig,
    #[error("OS keyring changed Google OAuth client configuration during round-trip")]
    StoredGoogleClientConfigMismatch,
    #[error("stored credential contains invalid UTF-8")]
    InvalidStoredSecret,
    #[error("refreshed Google identity does not match the stored account")]
    GoogleAccountMismatch,
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
