//! Development CLI for NubiSync.
//!
//! The desktop UI will eventually call the same application services. The CLI
//! exists now so OAuth and provider behavior can be tested deterministically.

#![forbid(unsafe_code)]

use nubisync_auth::{KeyringSecretStore, SecretKey, SecretStore, SecretValue};
use nubisync_core::{
    ProviderAccount, ProviderId, RemoteChange, RemoteItemKind, SyncMode, SyncRoot,
};
use nubisync_daemon::{
    SUPERVISED_FILE_DOWNLOAD_MAX_BYTES, bootstrap_selected_root_snapshot,
    collect_selected_root_change_window_page, execute_completed_selected_root_change_window,
    materialize_selected_root_directories, materialize_selected_root_missing_file,
    plan_selected_root_local_materialization, plan_selected_root_remote_replacement,
    replace_selected_root_existing_file, verify_selected_root_existing_file,
    verify_selected_root_local_receipts,
};
use nubisync_drive::{
    GOOGLE_DRIVE_READONLY_SCOPE, GoogleDriveAccess, GoogleDriveApi, GoogleOAuthConfig,
};
use nubisync_storage::Storage;
use std::{
    collections::{HashSet, VecDeque},
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
        [auth, google, upgrade_readonly, approve]
            if auth == "auth"
                && google == "google"
                && upgrade_readonly == "upgrade-readonly"
                && approve == "--approve" =>
        {
            google_upgrade_readonly()
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
        [sync, roots, status] if sync == "sync" && roots == "roots" && status == "status" => {
            sync_roots_status()
        }
        [sync, roots, metadata_step, approve]
            if sync == "sync"
                && roots == "roots"
                && metadata_step == "metadata-step"
                && approve == "--approve" =>
        {
            sync_roots_metadata_step()
        }
        [sync, roots, reconcile_plan, approve]
            if sync == "sync"
                && roots == "roots"
                && reconcile_plan == "reconcile-plan"
                && approve == "--approve" =>
        {
            sync_roots_reconcile_plan()
        }
        [sync, roots, materialize_directories, approve]
            if sync == "sync"
                && roots == "roots"
                && materialize_directories == "materialize-directories"
                && approve == "--approve" =>
        {
            sync_roots_materialize_directories()
        }
        [sync, roots, materialize_file, approve]
            if sync == "sync"
                && roots == "roots"
                && materialize_file == "materialize-file"
                && approve == "--approve" =>
        {
            sync_roots_materialize_file()
        }
        [sync, roots, verify_file, approve]
            if sync == "sync"
                && roots == "roots"
                && verify_file == "verify-file"
                && approve == "--approve" =>
        {
            sync_roots_verify_file()
        }
        [sync, roots, verify_local, approve]
            if sync == "sync"
                && roots == "roots"
                && verify_local == "verify-local"
                && approve == "--approve" =>
        {
            sync_roots_verify_local()
        }
        [sync, roots, replacement_plan, approve]
            if sync == "sync"
                && roots == "roots"
                && replacement_plan == "replacement-plan"
                && approve == "--approve" =>
        {
            sync_roots_replacement_plan()
        }
        [sync, roots, replace_file, approve]
            if sync == "sync"
                && roots == "roots"
                && replace_file == "replace-file"
                && approve == "--approve" =>
        {
            sync_roots_replace_file()
        }
        [sync, roots, inventory, limit, value]
            if sync == "sync"
                && roots == "roots"
                && inventory == "inventory"
                && limit == "--limit" =>
        {
            sync_root_inventory(parse_inventory_limit(value)?)
        }
        [sync, roots, add, mode_flag, mode_value]
            if sync == "sync" && roots == "roots" && add == "add" && mode_flag == "--mode" =>
        {
            sync_roots_add(parse_sync_root_mode(mode_value)?, false)
        }
        [sync, roots, add, mode_flag, mode_value, dry_run]
            if sync == "sync"
                && roots == "roots"
                && add == "add"
                && mode_flag == "--mode"
                && dry_run == "--dry-run" =>
        {
            sync_roots_add(parse_sync_root_mode(mode_value)?, true)
        }
        [drive, changes] if drive == "drive" && changes == "changes" => drive_changes(),
        [drive, catalog, status]
            if drive == "drive" && catalog == "catalog" && status == "status" =>
        {
            drive_catalog_status()
        }
        [drive, catalog, catchup]
            if drive == "drive" && catalog == "catalog" && catchup == "catchup" =>
        {
            drive_catalog_catchup()
        }
        [drive, inventory] if drive == "drive" && inventory == "inventory" => {
            drive_inventory(Some(20))
        }
        [drive, folder, validate, remote_root_id]
            if drive == "drive" && folder == "folder" && validate == "validate" =>
        {
            drive_folder_validate(remote_root_id)
        }
        [drive, folder, probe, parent_id, limit, value]
            if drive == "drive" && folder == "folder" && probe == "probe" && limit == "--limit" =>
        {
            drive_folder_probe(parent_id, parse_folder_probe_limit(value)?)
        }
        [drive, folder, tree, root_id, limit, value]
            if drive == "drive" && folder == "folder" && tree == "tree" && limit == "--limit" =>
        {
            drive_folder_tree(root_id, parse_folder_tree_limit(value)?)
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
  nubisync auth google upgrade-readonly --approve
  nubisync auth google configure
  nubisync auth google status
  nubisync auth google refresh
  nubisync auth google logout
  nubisync sync roots status
  nubisync sync roots metadata-step --approve
  nubisync sync roots reconcile-plan --approve
  nubisync sync roots materialize-directories --approve
  nubisync sync roots materialize-file --approve
  nubisync sync roots verify-file --approve
  nubisync sync roots verify-local --approve
  nubisync sync roots replacement-plan --approve
  nubisync sync roots replace-file --approve
  nubisync sync roots inventory --limit <1-10000>
  nubisync sync roots add --mode receive_only
  nubisync sync roots add --mode receive_only --dry-run
  nubisync drive changes
  nubisync drive catalog status
  nubisync drive catalog catchup
  nubisync drive folder validate <remote-folder-id>
  nubisync drive folder probe <remote-folder-id> --limit <1-1000>
  nubisync drive folder tree <remote-folder-id> --limit <1-10000>
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SyncRootMetadataStep {
    Bootstrap,
    CollectChangePage,
    ExecuteWindow,
}

fn classify_sync_root_metadata_step(
    snapshot_complete: bool,
    window_complete: Option<bool>,
) -> SyncRootMetadataStep {
    if !snapshot_complete {
        return SyncRootMetadataStep::Bootstrap;
    }

    match window_complete {
        Some(true) => SyncRootMetadataStep::ExecuteWindow,
        Some(false) | None => SyncRootMetadataStep::CollectChangePage,
    }
}

fn sync_roots_metadata_step() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let mut storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;

    if roots.is_empty() {
        println!("SYNC_ROOT_METADATA_STEP=SKIPPED");
        println!("REASON=no_configured_root");
        println!("NETWORK_CHECK=not_performed");
        println!("DATABASE_MUTATION=no");
        println!("FILESYSTEM_MUTATION=no");
        println!("FILE_CONTENT_ACCESSED=no");
        println!("DRIVE_WRITE_ACCESS=no");
        return Ok(());
    }

    if roots.len() != 1 {
        println!("SYNC_ROOT_METADATA_STEP=SKIPPED");
        println!("REASON=multiple_roots_require_selector");
        println!("NETWORK_CHECK=not_performed");
        println!("DATABASE_MUTATION=no");
        println!("FILESYSTEM_MUTATION=no");
        println!("FILE_CONTENT_ACCESSED=no");
        println!("DRIVE_WRITE_ACCESS=no");
        return Ok(());
    }

    let root = roots
        .into_iter()
        .next()
        .ok_or(CliError::SyncRootMetadataStepSelectionFailed)?;

    if root.mode != SyncMode::ReceiveOnly {
        return Err(CliError::SyncRootMetadataStepModeUnsupported);
    }

    let inventory = storage.sync_root_remote_inventory_state(&root.id)?;
    let window = storage.sync_root_change_window_state(&root.id)?;
    let step = classify_sync_root_metadata_step(
        inventory.snapshot_complete,
        window.as_ref().map(|state| state.is_complete()),
    );

    ensure_keyring_available()?;
    let keyring = KeyringSecretStore::default();
    let refresh_key = refresh_token_key(&account.subject)?;
    let refresh_token = required_secret_utf8(
        keyring.get(&refresh_key)?,
        CliError::MissingStoredRefreshToken,
    )?;
    let (client_id, client_secret) = load_google_client_config(&keyring)?;

    println!("SYNC_ROOT_METADATA_STEP_STAGE=refresh_access_token");
    let oauth = GoogleOAuthConfig::new(client_id)?;
    let tokens = oauth.refresh_access_token(&refresh_token, &client_secret)?;

    if let Some(rotated_refresh_token) = tokens.refresh_token() {
        keyring.put(
            &refresh_key,
            SecretValue::new(rotated_refresh_token.as_bytes().to_vec())?,
        )?;
    }

    println!("SYNC_ROOT_METADATA_STEP_STAGE=verify_account");
    let api = GoogleDriveApi::new(tokens.access_token().clone())?;
    let user = api.user_info()?;

    if user.sub != account.subject {
        return Err(CliError::GoogleAccountMismatch);
    }

    let observed_at_unix_ms = unix_time_ms()?;

    match step {
        SyncRootMetadataStep::Bootstrap => {
            println!("SYNC_ROOT_METADATA_STEP_ACTION=bootstrap");
            let result =
                bootstrap_selected_root_snapshot(&api, &mut storage, &root, observed_at_unix_ms)?;

            println!("SYNC_ROOT_METADATA_STEP=PASS");
            println!("ACTION=bootstrap");
            println!("AUTHORITATIVE_ITEMS={}", result.authoritative_items);
            println!("FOLDER_PAGES={}", result.folder_pages);
            println!(
                "UNSUPPORTED_PROVIDER_NATIVE={}",
                result.unsupported_provider_native
            );
            println!("NEXT_ACTION=collect_change_page");
        }
        SyncRootMetadataStep::CollectChangePage => {
            println!("SYNC_ROOT_METADATA_STEP_ACTION=collect_change_page");
            let result = collect_selected_root_change_window_page(&api, &mut storage, &root)?;

            println!("SYNC_ROOT_METADATA_STEP=PASS");
            println!("ACTION=collect_change_page");
            println!("WINDOW_PAGE_COUNT={}", result.page_count);
            println!("WINDOW_CHANGE_COUNT={}", result.change_count);
            println!("WINDOW_COMPLETE={}", yes_no(result.complete));
            println!(
                "NEXT_ACTION={}",
                if result.complete {
                    "execute_window"
                } else {
                    "collect_change_page"
                }
            );
        }
        SyncRootMetadataStep::ExecuteWindow => {
            println!("SYNC_ROOT_METADATA_STEP_ACTION=execute_window");
            let result = execute_completed_selected_root_change_window(
                &api,
                &mut storage,
                &root,
                observed_at_unix_ms,
            )?;

            println!("SYNC_ROOT_METADATA_STEP=PASS");
            println!("ACTION=execute_window");
            println!("PROVIDER_CHANGES={}", result.provider_changes);
            println!("CATALOG_MUTATIONS={}", result.storage_mutations);
            println!("AUTHORITATIVE_ITEMS={}", result.authoritative_items);
            println!("HYDRATED_ITEMS={}", result.hydrated_items);
            println!("ROOT_REVALIDATIONS={}", result.root_revalidations);
            println!(
                "COMPLETED_INITIAL_CATCHUP={}",
                yes_no(result.completed_initial_catchup)
            );
            println!("NEXT_ACTION=collect_change_page");
        }
    }

    println!("NETWORK_CHECK=performed");
    println!("REMOTE_ROOT_ID_PRINTED=no");
    println!("REMOTE_METADATA_PRINTED=no");
    println!("FILE_CONTENT_ACCESSED=no");
    println!("FILESYSTEM_MUTATION=no");
    println!("DRIVE_WRITE_ACCESS=no");

    Ok(())
}

fn sync_roots_reconcile_plan() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;

    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;

    if roots.is_empty() {
        println!("SYNC_ROOT_RECONCILE_PLAN=SKIPPED");
        println!("REASON=no_configured_root");
        println!("NETWORK_CHECK=not_performed");
        println!("DATABASE_MUTATION=no");
        println!("FILESYSTEM_MUTATION=no");
        println!("FILE_CONTENT_ACCESSED=no");
        println!("DRIVE_WRITE_ACCESS=no");
        return Ok(());
    }

    if roots.len() != 1 {
        println!("SYNC_ROOT_RECONCILE_PLAN=SKIPPED");
        println!("REASON=multiple_roots_require_selector");
        println!("NETWORK_CHECK=not_performed");
        println!("DATABASE_MUTATION=no");
        println!("FILESYSTEM_MUTATION=no");
        println!("FILE_CONTENT_ACCESSED=no");
        println!("DRIVE_WRITE_ACCESS=no");
        return Ok(());
    }

    let root = roots
        .into_iter()
        .next()
        .ok_or(CliError::SyncRootReconcilePlanSelectionFailed)?;

    let plan = plan_selected_root_local_materialization(&storage, &root)?;
    let receipt_count = storage.sync_root_materialization_receipt_count(&root.id)?;
    let stale_receipt_count = storage.sync_root_stale_materialization_receipt_count(&root.id)?;

    println!("SYNC_ROOT_RECONCILE_PLAN=PASS");
    println!("MODE=receive_only");
    println!("REMOTE_ITEMS={}", plan.remote_items);
    println!("REMOTE_DIRECTORIES={}", plan.remote_directories);
    println!("REMOTE_FILES={}", plan.remote_files);
    println!("LOCAL_ENTRIES={}", plan.local_entries);
    println!("MISSING_DIRECTORIES={}", plan.missing_directories);
    println!("MISSING_FILES={}", plan.missing_files);
    println!("MATCHING_DIRECTORIES={}", plan.matching_directories);
    println!(
        "EXISTING_FILES_UNVERIFIED={}",
        plan.existing_files_unverified
    );
    println!("LOCAL_ONLY_ENTRIES={}", plan.local_only_entries);
    println!("TYPE_CONFLICTS={}", plan.type_conflicts);
    println!("DURABLE_MATERIALIZATION_RECEIPTS={receipt_count}");
    println!("STALE_MATERIALIZATION_RECEIPTS={stale_receipt_count}");
    println!(
        "READY_FOR_DIRECTORY_PHASE={}",
        yes_no(plan.ready_for_directory_phase())
    );
    println!("NETWORK_CHECK=not_performed");
    println!("DATABASE_MUTATION=no");
    println!("FILESYSTEM_READ=metadata_only");
    println!("FILESYSTEM_MUTATION=no");
    println!("FILE_CONTENT_ACCESSED=no");
    println!("ROOT_PATH_PRINTED=no");
    println!("LOCAL_NAMES_PRINTED=no");
    println!("REMOTE_ROOT_ID_PRINTED=no");
    println!("REMOTE_METADATA_PRINTED=no");
    println!("DRIVE_WRITE_ACCESS=no");

    Ok(())
}

fn sync_roots_materialize_directories() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;

    if roots.is_empty() {
        println!("SYNC_ROOT_DIRECTORY_MATERIALIZATION=SKIPPED");
        println!("REASON=no_configured_root");
        println!("NETWORK_CHECK=not_performed");
        println!("DATABASE_MUTATION=no");
        println!("FILESYSTEM_MUTATION=no");
        println!("FILE_CONTENT_ACCESSED=no");
        println!("DRIVE_WRITE_ACCESS=no");
        return Ok(());
    }

    if roots.len() != 1 {
        println!("SYNC_ROOT_DIRECTORY_MATERIALIZATION=SKIPPED");
        println!("REASON=multiple_roots_require_selector");
        println!("NETWORK_CHECK=not_performed");
        println!("DATABASE_MUTATION=no");
        println!("FILESYSTEM_MUTATION=no");
        println!("FILE_CONTENT_ACCESSED=no");
        println!("DRIVE_WRITE_ACCESS=no");
        return Ok(());
    }

    let root = roots
        .first()
        .ok_or(CliError::SyncRootReconcilePlanSelectionFailed)?;
    let result = materialize_selected_root_directories(&storage, root)?;

    println!("SYNC_ROOT_DIRECTORY_MATERIALIZATION=PASS");
    println!("MODE=receive_only");
    println!("REMOTE_DIRECTORIES={}", result.remote_directories);
    println!("DIRECTORIES_CREATED={}", result.created_directories);
    println!(
        "DIRECTORIES_ALREADY_PRESENT={}",
        result.existing_directories
    );
    println!("PENDING_FILES={}", result.pending_files);
    println!("NETWORK_CHECK=not_performed");
    println!("DATABASE_MUTATION=no");
    println!("FILESYSTEM_READ=metadata_only");
    println!(
        "FILESYSTEM_MUTATION={}",
        yes_no(result.created_directories > 0)
    );
    println!("FILE_CONTENT_ACCESSED=no");
    println!("FILES_CREATED=0");
    println!("FILES_DELETED=0");
    println!("DIRECTORIES_REMOVED=0");
    println!("ROOT_PATH_PRINTED=no");
    println!("LOCAL_NAMES_PRINTED=no");
    println!("REMOTE_ROOT_ID_PRINTED=no");
    println!("REMOTE_METADATA_PRINTED=no");
    println!("DRIVE_WRITE_ACCESS=no");

    Ok(())
}

fn sync_roots_materialize_file() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let mut storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;

    if roots.is_empty() {
        println!("SYNC_ROOT_FILE_MATERIALIZATION=SKIPPED");
        println!("REASON=no_configured_root");
        println!("NETWORK_CHECK=not_performed");
        println!("DATABASE_MUTATION=no");
        println!("FILESYSTEM_MUTATION=no");
        println!("FILE_CONTENT_ACCESSED=no");
        println!("DRIVE_WRITE_ACCESS=no");
        return Ok(());
    }
    if roots.len() != 1 {
        println!("SYNC_ROOT_FILE_MATERIALIZATION=SKIPPED");
        println!("REASON=multiple_roots_require_selector");
        println!("NETWORK_CHECK=not_performed");
        println!("DATABASE_MUTATION=no");
        println!("FILESYSTEM_MUTATION=no");
        println!("FILE_CONTENT_ACCESSED=no");
        println!("DRIVE_WRITE_ACCESS=no");
        return Ok(());
    }
    let root = roots
        .first()
        .ok_or(CliError::SyncRootReconcilePlanSelectionFailed)?;

    ensure_keyring_available()?;
    let keyring = KeyringSecretStore::default();
    let refresh_key = refresh_token_key(&account.subject)?;
    let refresh_token = required_secret_utf8(
        keyring.get(&refresh_key)?,
        CliError::MissingStoredRefreshToken,
    )?;
    let (client_id, client_secret) = load_google_client_config(&keyring)?;

    println!("SYNC_ROOT_FILE_MATERIALIZATION_STAGE=refresh_access_token");
    let oauth = GoogleOAuthConfig::new(client_id)?;
    let tokens = oauth.refresh_access_token(&refresh_token, &client_secret)?;
    if let Some(scope) = tokens.scope()
        && !oauth_scope_contains(Some(scope), GOOGLE_DRIVE_READONLY_SCOPE)
    {
        return Err(CliError::GoogleReadonlyScopeNotGranted);
    }
    if let Some(rotated) = tokens.refresh_token() {
        keyring.put(&refresh_key, SecretValue::new(rotated.as_bytes().to_vec())?)?;
    }

    println!("SYNC_ROOT_FILE_MATERIALIZATION_STAGE=verify_account");
    let api = GoogleDriveApi::new(tokens.access_token().clone())?;
    let user = api.user_info()?;
    if user.sub != account.subject {
        return Err(CliError::GoogleAccountMismatch);
    }

    println!("SYNC_ROOT_FILE_MATERIALIZATION_STAGE=download_one_file");
    let result = materialize_selected_root_missing_file(&api, &mut storage, root)?;

    println!("SYNC_ROOT_FILE_MATERIALIZATION=PASS");
    println!("MODE=receive_only");
    println!("FILES_DOWNLOADED={}", result.files_downloaded);
    println!("BYTES_DOWNLOADED={}", result.bytes_downloaded);
    println!("MAX_FILE_BYTES={}", result.max_file_bytes);
    println!("SIZE_MATCH_VERIFIED={}", yes_no(result.size_match_verified));
    println!("NETWORK_CHECK=performed");
    println!("DATABASE_MUTATION=yes");
    println!("FILESYSTEM_MUTATION=yes");
    println!("FILE_CONTENT_ACCESSED=yes");
    println!("FILES_CREATED=1");
    println!("FILES_OVERWRITTEN=0");
    println!("FILES_DELETED=0");
    println!("DIRECTORIES_CREATED=0");
    println!("DIRECTORIES_REMOVED=0");
    println!("ATOMIC_NO_OVERWRITE_PROMOTION=yes");
    println!("TEMP_FILES_RETAINED=0");
    println!("ROOT_PATH_PRINTED=no");
    println!("LOCAL_NAMES_PRINTED=no");
    println!("REMOTE_ROOT_ID_PRINTED=no");
    println!("REMOTE_METADATA_PRINTED=no");
    println!("TOKEN_VALUES_PRINTED=no");
    println!("DRIVE_WRITE_ACCESS=no");
    println!("CONFIGURED_MAX_FILE_BYTES={SUPERVISED_FILE_DOWNLOAD_MAX_BYTES}");
    Ok(())
}

fn sync_roots_verify_file() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let mut storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;

    if roots.len() != 1 {
        println!("SYNC_ROOT_FILE_VERIFICATION=SKIPPED");
        println!(
            "REASON={}",
            if roots.is_empty() {
                "no_configured_root"
            } else {
                "multiple_roots_require_selector"
            }
        );
        println!("NETWORK_CHECK=not_performed");
        println!("DATABASE_MUTATION=no");
        println!("FILESYSTEM_MUTATION=no");
        println!("LOCAL_FILE_CONTENT_ACCESSED=no");
        println!("REMOTE_FILE_CONTENT_ACCESSED=no");
        println!("DRIVE_WRITE_ACCESS=no");
        return Ok(());
    }

    let root = roots
        .first()
        .ok_or(CliError::SyncRootReconcilePlanSelectionFailed)?;

    ensure_keyring_available()?;
    let keyring = KeyringSecretStore::default();
    let refresh_key = refresh_token_key(&account.subject)?;
    let refresh_token = required_secret_utf8(
        keyring.get(&refresh_key)?,
        CliError::MissingStoredRefreshToken,
    )?;
    let (client_id, client_secret) = load_google_client_config(&keyring)?;

    println!("SYNC_ROOT_FILE_VERIFICATION_STAGE=refresh_access_token");
    let oauth = GoogleOAuthConfig::new(client_id)?;
    let tokens = oauth.refresh_access_token(&refresh_token, &client_secret)?;
    if let Some(scope) = tokens.scope()
        && !oauth_scope_contains(Some(scope), GOOGLE_DRIVE_READONLY_SCOPE)
    {
        return Err(CliError::GoogleReadonlyScopeNotGranted);
    }
    if let Some(rotated_refresh_token) = tokens.refresh_token() {
        keyring.put(
            &refresh_key,
            SecretValue::new(rotated_refresh_token.as_bytes().to_vec())?,
        )?;
    }

    println!("SYNC_ROOT_FILE_VERIFICATION_STAGE=verify_account");
    let api = GoogleDriveApi::new(tokens.access_token().clone())?;
    let user = api.user_info()?;
    if user.sub != account.subject {
        return Err(CliError::GoogleAccountMismatch);
    }

    println!("SYNC_ROOT_FILE_VERIFICATION_STAGE=compare_sha256");
    let result = verify_selected_root_existing_file(&api, &mut storage, root)?;
    let receipt_count = storage.sync_root_materialization_receipt_count(&root.id)?;

    println!("SYNC_ROOT_FILE_VERIFICATION=PASS");
    println!("MODE=receive_only");
    println!("FILES_VERIFIED={}", result.files_verified);
    println!("BYTES_VERIFIED={}", result.bytes_verified);
    println!("HASH_ALGORITHM=sha256");
    println!("HASH_MATCH={}", yes_no(result.hash_match));
    println!("RECEIPT_RECORDED={}", yes_no(result.receipt_recorded));
    println!("DURABLE_MATERIALIZATION_RECEIPTS={receipt_count}");
    println!("NETWORK_CHECK=performed");
    println!("DATABASE_MUTATION=yes");
    println!("FILESYSTEM_MUTATION=no");
    println!("LOCAL_FILE_CONTENT_ACCESSED=yes");
    println!("REMOTE_FILE_CONTENT_ACCESSED=yes");
    println!("FILES_OVERWRITTEN=0");
    println!("FILES_DELETED=0");
    println!("ROOT_PATH_PRINTED=no");
    println!("LOCAL_NAMES_PRINTED=no");
    println!("REMOTE_ROOT_ID_PRINTED=no");
    println!("REMOTE_METADATA_PRINTED=no");
    println!("HASH_VALUE_PRINTED=no");
    println!("TOKEN_VALUES_PRINTED=no");
    println!("DRIVE_WRITE_ACCESS=no");
    Ok(())
}

fn sync_roots_verify_local() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;

    if roots.len() != 1 {
        println!("SYNC_ROOT_LOCAL_RECEIPT_VERIFICATION=SKIPPED");
        println!(
            "REASON={}",
            if roots.is_empty() {
                "no_configured_root"
            } else {
                "multiple_roots_require_selector"
            }
        );
        println!("NETWORK_CHECK=not_performed");
        println!("DATABASE_MUTATION=no");
        println!("FILESYSTEM_MUTATION=no");
        println!("LOCAL_FILE_CONTENT_ACCESSED=no");
        println!("REMOTE_FILE_CONTENT_ACCESSED=no");
        println!("DRIVE_WRITE_ACCESS=no");
        return Ok(());
    }

    let root = roots
        .first()
        .ok_or(CliError::SyncRootReconcilePlanSelectionFailed)?;

    let result = verify_selected_root_local_receipts(&storage, root)?;

    println!("SYNC_ROOT_LOCAL_RECEIPT_VERIFICATION=PASS");
    println!("MODE=receive_only");
    println!("RECEIPTS_TOTAL={}", result.receipts_total);
    println!("FILES_MATCHING_RECEIPT={}", result.files_matching_receipt);
    println!(
        "FILES_MODIFIED_SINCE_RECEIPT={}",
        result.files_modified_since_receipt
    );
    println!("FILES_MISSING={}", result.files_missing);
    println!("TYPE_CONFLICTS={}", result.type_conflicts);
    println!("BYTES_HASHED={}", result.bytes_hashed);
    println!("ALL_RECEIPTS_MATCH={}", yes_no(result.all_receipts_match()));
    println!("NETWORK_CHECK=not_performed");
    println!("DATABASE_MUTATION=no");
    println!("FILESYSTEM_READ=file_content");
    println!("FILESYSTEM_MUTATION=no");
    println!("LOCAL_FILE_CONTENT_ACCESSED=yes");
    println!("REMOTE_FILE_CONTENT_ACCESSED=no");
    println!("ROOT_PATH_PRINTED=no");
    println!("LOCAL_NAMES_PRINTED=no");
    println!("REMOTE_ROOT_ID_PRINTED=no");
    println!("REMOTE_METADATA_PRINTED=no");
    println!("HASH_VALUE_PRINTED=no");
    println!("TOKEN_VALUES_PRINTED=no");
    println!("DRIVE_WRITE_ACCESS=no");

    Ok(())
}

fn sync_roots_replacement_plan() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;

    if roots.len() != 1 {
        println!("SYNC_ROOT_REMOTE_REPLACEMENT_PLAN=SKIPPED");
        println!(
            "REASON={}",
            if roots.is_empty() {
                "no_configured_root"
            } else {
                "multiple_roots_require_selector"
            }
        );
        println!("NETWORK_CHECK=not_performed");
        println!("DATABASE_MUTATION=no");
        println!("FILESYSTEM_MUTATION=no");
        println!("LOCAL_FILE_CONTENT_ACCESSED=no");
        println!("REMOTE_FILE_CONTENT_ACCESSED=no");
        println!("DRIVE_WRITE_ACCESS=no");
        return Ok(());
    }

    let root = roots
        .first()
        .ok_or(CliError::SyncRootReconcilePlanSelectionFailed)?;

    let result = plan_selected_root_remote_replacement(&storage, root)?;

    println!("SYNC_ROOT_REMOTE_REPLACEMENT_PLAN=PASS");
    println!("MODE=receive_only");
    println!("STALE_RECEIPTS_TOTAL={}", result.stale_receipts_total);
    println!("REPLACEMENT_CANDIDATES={}", result.replacement_candidates);
    println!("SAFE_TO_REPLACE={}", result.safe_to_replace);
    println!("LOCAL_CONFLICTS={}", result.local_conflicts);
    println!("FILES_MISSING={}", result.files_missing);
    println!("TYPE_CONFLICTS={}", result.type_conflicts);
    println!("BYTES_HASHED={}", result.bytes_hashed);
    println!("READY_TO_REPLACE={}", yes_no(result.ready()));
    println!("NETWORK_CHECK=not_performed");
    println!("DATABASE_MUTATION=no");
    println!("FILESYSTEM_READ=file_content");
    println!("FILESYSTEM_MUTATION=no");
    println!("LOCAL_FILE_CONTENT_ACCESSED=yes");
    println!("REMOTE_FILE_CONTENT_ACCESSED=no");
    println!("ROOT_PATH_PRINTED=no");
    println!("LOCAL_NAMES_PRINTED=no");
    println!("REMOTE_ROOT_ID_PRINTED=no");
    println!("REMOTE_METADATA_PRINTED=no");
    println!("HASH_VALUE_PRINTED=no");
    println!("TOKEN_VALUES_PRINTED=no");
    println!("DRIVE_WRITE_ACCESS=no");

    Ok(())
}

fn sync_roots_replace_file() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let mut storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;

    if roots.len() != 1 {
        println!("SYNC_ROOT_FILE_REPLACEMENT=SKIPPED");
        println!(
            "REASON={}",
            if roots.is_empty() {
                "no_configured_root"
            } else {
                "multiple_roots_require_selector"
            }
        );
        println!("NETWORK_CHECK=not_performed");
        println!("DATABASE_MUTATION=no");
        println!("FILESYSTEM_MUTATION=no");
        println!("DRIVE_WRITE_ACCESS=no");
        return Ok(());
    }

    let root = roots
        .first()
        .ok_or(CliError::SyncRootReconcilePlanSelectionFailed)?;

    let readiness = plan_selected_root_remote_replacement(&storage, root)?;
    if !readiness.ready() {
        return Err(CliError::SyncRootReplacementNotReady);
    }

    ensure_keyring_available()?;
    let keyring = KeyringSecretStore::default();
    let refresh_key = refresh_token_key(&account.subject)?;
    let refresh_token = required_secret_utf8(
        keyring.get(&refresh_key)?,
        CliError::MissingStoredRefreshToken,
    )?;
    let (client_id, client_secret) = load_google_client_config(&keyring)?;

    println!("SYNC_ROOT_FILE_REPLACEMENT_STAGE=refresh_access_token");
    let oauth = GoogleOAuthConfig::new(client_id)?;
    let tokens = oauth.refresh_access_token(&refresh_token, &client_secret)?;
    if let Some(scope) = tokens.scope()
        && !oauth_scope_contains(Some(scope), GOOGLE_DRIVE_READONLY_SCOPE)
    {
        return Err(CliError::GoogleReadonlyScopeNotGranted);
    }
    if let Some(rotated_refresh_token) = tokens.refresh_token() {
        keyring.put(
            &refresh_key,
            SecretValue::new(rotated_refresh_token.as_bytes().to_vec())?,
        )?;
    }

    println!("SYNC_ROOT_FILE_REPLACEMENT_STAGE=verify_account");
    let api = GoogleDriveApi::new(tokens.access_token().clone())?;
    let user = api.user_info()?;
    if user.sub != account.subject {
        return Err(CliError::GoogleAccountMismatch);
    }

    println!("SYNC_ROOT_FILE_REPLACEMENT_STAGE=download_and_replace");
    let result = replace_selected_root_existing_file(&api, &mut storage, root)?;

    let current_receipts = storage.sync_root_materialization_receipt_count(&root.id)?;
    let stale_receipts = storage.sync_root_stale_materialization_receipt_count(&root.id)?;

    println!("SYNC_ROOT_FILE_REPLACEMENT=PASS");
    println!("MODE=receive_only");
    println!("FILES_REPLACED={}", result.files_replaced);
    println!("BYTES_DOWNLOADED={}", result.bytes_downloaded);
    println!(
        "STALE_BASELINE_MATCH={}",
        yes_no(result.stale_baseline_match)
    );
    println!(
        "PROVIDER_FINGERPRINT_MATCH={}",
        yes_no(result.provider_fingerprint_match)
    );
    println!("RECEIPT_RECORDED={}", yes_no(result.receipt_recorded));
    println!("ATOMIC_REPLACE={}", yes_no(result.atomic_replace));
    println!("DURABLE_MATERIALIZATION_RECEIPTS={current_receipts}");
    println!("STALE_MATERIALIZATION_RECEIPTS={stale_receipts}");
    println!("MAX_FILE_BYTES={SUPERVISED_FILE_DOWNLOAD_MAX_BYTES}");
    println!("NETWORK_CHECK=performed");
    println!("DATABASE_MUTATION=yes");
    println!("FILESYSTEM_MUTATION=yes");
    println!("LOCAL_FILE_CONTENT_ACCESSED=yes");
    println!("REMOTE_FILE_CONTENT_ACCESSED=yes");
    println!("FILES_CREATED=0");
    println!("FILES_OVERWRITTEN=1");
    println!("FILES_DELETED=0");
    println!("DIRECTORIES_CREATED=0");
    println!("DIRECTORIES_REMOVED=0");
    println!("ROOT_PATH_PRINTED=no");
    println!("LOCAL_NAMES_PRINTED=no");
    println!("REMOTE_ROOT_ID_PRINTED=no");
    println!("REMOTE_METADATA_PRINTED=no");
    println!("HASH_VALUE_PRINTED=no");
    println!("TOKEN_VALUES_PRINTED=no");
    println!("DRIVE_WRITE_ACCESS=no");

    Ok(())
}

fn parse_sync_root_mode(value: &str) -> Result<SyncMode, CliError> {
    let mode = SyncMode::parse(value)?;

    if mode != SyncMode::ReceiveOnly {
        return Err(CliError::SyncRootModeNotYetSupported);
    }

    Ok(mode)
}

fn validate_local_sync_directory(value: &str) -> Result<String, CliError> {
    if value.trim().is_empty() {
        return Err(CliError::InvalidLocalSyncDirectory);
    }

    let path = PathBuf::from(value);
    if !path.is_absolute() {
        return Err(CliError::LocalSyncDirectoryMustBeAbsolute);
    }

    let metadata =
        fs::symlink_metadata(&path).map_err(|_| CliError::LocalSyncDirectoryUnavailable)?;

    if metadata.file_type().is_symlink() {
        return Err(CliError::LocalSyncDirectorySymlinkUnsupported);
    }

    if !metadata.is_dir() {
        return Err(CliError::LocalSyncDirectoryNotDirectory);
    }

    let canonical = fs::canonicalize(path).map_err(|_| CliError::LocalSyncDirectoryUnavailable)?;

    if canonical.parent().is_none() {
        return Err(CliError::LocalSyncDirectoryFilesystemRootUnsupported);
    }

    if let Some(home) = env::var_os("HOME")
        && let Ok(home) = fs::canonicalize(home)
        && canonical == home
    {
        return Err(CliError::LocalSyncDirectoryHomeUnsupported);
    }

    let mut entries =
        fs::read_dir(&canonical).map_err(|_| CliError::LocalSyncDirectoryUnavailable)?;

    if entries.next().transpose()?.is_some() {
        return Err(CliError::LocalSyncDirectoryNotEmpty);
    }

    canonical
        .into_os_string()
        .into_string()
        .map_err(|_| CliError::LocalSyncDirectoryNonUtf8)
}

fn sync_roots_add(mode: SyncMode, dry_run: bool) -> Result<(), CliError> {
    let local_path_input = prompt_line("Local sync directory (absolute, existing): ")?;
    let remote_root_id = prompt_line("Google Drive folder ID (or root): ")?;

    let local_path = validate_local_sync_directory(&local_path_input)?;

    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;

    let existing_roots = storage.list_sync_roots(&provider, &account.subject)?;

    if existing_roots
        .iter()
        .any(|root| root.local_path == local_path)
    {
        return Err(CliError::LocalSyncDirectoryAlreadyRegistered);
    }

    if existing_roots
        .iter()
        .any(|root| root.remote_root_id.as_deref() == Some(remote_root_id.as_str()))
    {
        return Err(CliError::RemoteSyncRootAlreadyRegistered);
    }

    ensure_keyring_available()?;
    let keyring = KeyringSecretStore::default();
    let refresh_key = refresh_token_key(&account.subject)?;
    let refresh_token = required_secret_utf8(
        keyring.get(&refresh_key)?,
        CliError::MissingStoredRefreshToken,
    )?;
    let (client_id, client_secret) = load_google_client_config(&keyring)?;

    println!("SYNC_ROOT_ADD_STAGE=refresh_access_token");
    let oauth = GoogleOAuthConfig::new(client_id)?;
    let tokens = oauth.refresh_access_token(&refresh_token, &client_secret)?;

    if let Some(rotated_refresh_token) = tokens.refresh_token() {
        keyring.put(
            &refresh_key,
            SecretValue::new(rotated_refresh_token.as_bytes().to_vec())?,
        )?;
    }

    println!("SYNC_ROOT_ADD_STAGE=verify_account");
    let api = GoogleDriveApi::new(tokens.access_token().clone())?;
    let user = api.user_info()?;
    if user.sub != account.subject {
        return Err(CliError::GoogleAccountMismatch);
    }

    println!("SYNC_ROOT_ADD_STAGE=validate_remote_root");
    api.validate_folder_root(&remote_root_id)?;

    let created_at_unix_ms = unix_time_ms()?;
    let root = SyncRoot::new(
        format!("sync-root-{created_at_unix_ms}"),
        provider.clone(),
        account.subject.clone(),
        local_path,
        Some(remote_root_id),
        mode,
        created_at_unix_ms,
    )?;

    if dry_run {
        let configured_roots = storage.sync_root_count(&provider, &account.subject)?;

        println!("SYNC_ROOT_ADD=DRY_RUN_PASS");
        println!("MODE={}", mode.as_str());
        println!("LOCAL_DIRECTORY_VERIFIED=yes");
        println!("LOCAL_DIRECTORY_EMPTY=yes");
        println!("REMOTE_ROOT_VERIFIED=yes");
        println!("SYNC_ROOT_PERSISTED=no");
        println!("CONFIGURED_ROOTS={configured_roots}");
        println!("ROOT_PATH_PRINTED=no");
        println!("REMOTE_ROOT_ID_PRINTED=no");
        println!("INVENTORY_PERSISTED=no");
        println!("FILESYSTEM_MUTATION=no");
        println!("FILE_CONTENT_ACCESSED=no");
        println!("DRIVE_WRITE_ACCESS=no");

        return Ok(());
    }

    storage.insert_sync_root(&root)?;

    let configured_roots = storage.sync_root_count(&provider, &account.subject)?;

    println!("SYNC_ROOT_ADD=PASS");
    println!("MODE={}", mode.as_str());
    println!("LOCAL_DIRECTORY_VERIFIED=yes");
    println!("LOCAL_DIRECTORY_EMPTY=yes");
    println!("REMOTE_ROOT_VERIFIED=yes");
    println!("SYNC_ROOT_PERSISTED=yes");
    println!("CONFIGURED_ROOTS={configured_roots}");
    println!("ROOT_PATH_PRINTED=no");
    println!("REMOTE_ROOT_ID_PRINTED=no");
    println!("INVENTORY_PERSISTED=no");
    println!("FILESYSTEM_MUTATION=no");
    println!("FILE_CONTENT_ACCESSED=no");
    println!("DRIVE_WRITE_ACCESS=no");

    Ok(())
}

#[derive(Debug)]
struct SyncRootInventoryStats {
    pages_fetched: u64,
    folders_visited: u64,
    observed_items: u64,
    supported_items: u64,
    files: u64,
    folders: u64,
    unsupported_provider_native: u64,
    limit_reached: bool,
    traversal_complete: bool,
    staged_items_before_clear: u64,
}

fn sync_root_inventory(max_items: u64) -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let mut storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;

    if roots.is_empty() {
        println!("SYNC_ROOT_INVENTORY=SKIPPED");
        println!("REASON=no_configured_root");
        println!("NETWORK_CHECK=not_performed");
        println!("STAGING_MODIFIED=no");
        println!("AUTHORITATIVE_CATALOG_MODIFIED=no");
        println!("ROOT_CATALOG_STATE_MODIFIED=no");
        println!("PROVIDER_CURSOR_MODIFIED=no");
        println!("REMOTE_EVENTS_MODIFIED=no");
        return Ok(());
    }

    if roots.len() != 1 {
        println!("SYNC_ROOT_INVENTORY=SKIPPED");
        println!("REASON=multiple_roots_require_selector");
        println!("NETWORK_CHECK=not_performed");
        println!("STAGING_MODIFIED=no");
        println!("AUTHORITATIVE_CATALOG_MODIFIED=no");
        println!("ROOT_CATALOG_STATE_MODIFIED=no");
        println!("PROVIDER_CURSOR_MODIFIED=no");
        println!("REMOTE_EVENTS_MODIFIED=no");
        return Ok(());
    }

    let root = roots
        .into_iter()
        .next()
        .ok_or(CliError::SyncRootInventorySelectionFailed)?;

    let remote_root_id = root
        .remote_root_id
        .clone()
        .ok_or(CliError::SyncRootInventoryRemoteRootMissing)?;

    ensure_keyring_available()?;
    let keyring = KeyringSecretStore::default();
    let refresh_key = refresh_token_key(&account.subject)?;
    let refresh_token = required_secret_utf8(
        keyring.get(&refresh_key)?,
        CliError::MissingStoredRefreshToken,
    )?;
    let (client_id, client_secret) = load_google_client_config(&keyring)?;

    println!("SYNC_ROOT_INVENTORY_STAGE=refresh_access_token");
    let oauth = GoogleOAuthConfig::new(client_id)?;
    let tokens = oauth.refresh_access_token(&refresh_token, &client_secret)?;

    if let Some(rotated_refresh_token) = tokens.refresh_token() {
        keyring.put(
            &refresh_key,
            SecretValue::new(rotated_refresh_token.as_bytes().to_vec())?,
        )?;
    }

    println!("SYNC_ROOT_INVENTORY_STAGE=verify_account");
    let api = GoogleDriveApi::new(tokens.access_token().clone())?;
    let user = api.user_info()?;
    if user.sub != account.subject {
        return Err(CliError::GoogleAccountMismatch);
    }

    println!("SYNC_ROOT_INVENTORY_STAGE=validate_remote_root");
    api.validate_folder_root(&remote_root_id)?;

    storage.begin_sync_root_remote_inventory_staging(&root.id)?;

    let observed_at_unix_ms = unix_time_ms()?;

    let scan_result = (|| -> Result<SyncRootInventoryStats, CliError> {
        let mut folder_queue = VecDeque::new();
        let mut seen_folders = HashSet::new();

        seen_folders.insert(remote_root_id.clone());
        folder_queue.push_back(remote_root_id);

        let mut pages_fetched = 0_u64;
        let mut folders_visited = 0_u64;
        let mut observed_items = 0_u64;
        let mut supported_items = 0_u64;
        let mut files = 0_u64;
        let mut folders = 0_u64;
        let mut unsupported_provider_native = 0_u64;
        let mut limit_reached = false;

        'folders: while let Some(parent_remote_id) = folder_queue.pop_front() {
            folders_visited += 1;

            let mut continuation = None;
            let mut seen_continuations = HashSet::new();

            loop {
                if observed_items >= max_items {
                    limit_reached = true;
                    break 'folders;
                }

                if pages_fetched >= 10_000 {
                    return Err(CliError::SyncRootInventoryPageLimitExceeded);
                }

                let remaining = (max_items - observed_items).min(1000);
                let page_size =
                    u16::try_from(remaining).map_err(|_| CliError::InvalidInventoryLimit)?;

                let page_number = pages_fetched + 1;
                println!("SYNC_ROOT_INVENTORY_FETCH_PAGE={page_number}");

                let page = api.list_folder_children_page(
                    &parent_remote_id,
                    continuation.as_ref(),
                    page_size,
                )?;
                pages_fetched += 1;

                storage.stage_sync_root_remote_inventory_items(
                    &root.id,
                    &page.items,
                    observed_at_unix_ms,
                )?;

                for item in &page.items {
                    if item.kind == RemoteItemKind::Folder
                        && seen_folders.insert(item.remote_id.clone())
                    {
                        folder_queue.push_back(item.remote_id.clone());
                    }
                }

                let page_items = page.supported_items + page.unsupported_provider_native;
                observed_items += page_items;
                supported_items += page.supported_items;
                files += page.file_count;
                folders += page.folder_count;
                unsupported_provider_native += page.unsupported_provider_native;

                println!(
                    "SYNC_ROOT_INVENTORY_PAGE_COMPLETE={} OBSERVED_SO_FAR={} FILES_SO_FAR={} FOLDERS_SO_FAR={} QUEUED_FOLDERS={}",
                    pages_fetched,
                    observed_items,
                    files,
                    folders,
                    folder_queue.len()
                );

                match page.continuation {
                    Some(next) => {
                        if !seen_continuations.insert(next.as_str().to_owned()) {
                            return Err(CliError::SyncRootInventoryPaginationLoop);
                        }

                        continuation = Some(next);

                        if observed_items >= max_items {
                            limit_reached = true;
                            break 'folders;
                        }
                    }
                    None => break,
                }
            }
        }

        let traversal_complete = !limit_reached && folder_queue.is_empty();
        let staged_items_before_clear =
            storage.staged_sync_root_remote_inventory_count(&root.id)?;

        Ok(SyncRootInventoryStats {
            pages_fetched,
            folders_visited,
            observed_items,
            supported_items,
            files,
            folders,
            unsupported_provider_native,
            limit_reached,
            traversal_complete,
            staged_items_before_clear,
        })
    })();

    // Bounded inventory is never authoritative. Always attempt to remove staging,
    // including when the provider scan itself failed.
    storage.clear_sync_root_remote_inventory_staging(&root.id)?;

    let stats = scan_result?;

    println!("SYNC_ROOT_INVENTORY=PASS");
    println!("MODE=bounded_recursive");
    println!("MAX_ITEMS={max_items}");
    println!("TRAVERSAL_COMPLETE={}", yes_no(stats.traversal_complete));
    println!("LIMIT_REACHED={}", yes_no(stats.limit_reached));
    println!("PAGES_FETCHED={}", stats.pages_fetched);
    println!("FOLDERS_VISITED={}", stats.folders_visited);
    println!("OBSERVED_ITEMS={}", stats.observed_items);
    println!("SUPPORTED_ITEMS={}", stats.supported_items);
    println!("FILES={}", stats.files);
    println!("FOLDERS={}", stats.folders);
    println!(
        "UNSUPPORTED_PROVIDER_NATIVE={}",
        stats.unsupported_provider_native
    );
    println!(
        "STAGED_ITEMS_BEFORE_CLEAR={}",
        stats.staged_items_before_clear
    );
    println!("STAGING_CLEARED=yes");
    println!("REMOTE_ROOT_CONTAINER_INCLUDED=no");
    println!("AUTHORITATIVE_CATALOG_MODIFIED=no");
    println!("ROOT_CATALOG_STATE_MODIFIED=no");
    println!("PROVIDER_CURSOR_MODIFIED=no");
    println!("REMOTE_EVENTS_MODIFIED=no");
    println!("ROOT_PATH_PRINTED=no");
    println!("REMOTE_ROOT_ID_PRINTED=no");
    println!("REMOTE_METADATA_PRINTED=no");
    println!("FILE_CONTENT_ACCESSED=no");
    println!("FILESYSTEM_MUTATION=no");
    println!("DRIVE_WRITE_ACCESS=no");

    Ok(())
}

fn sync_roots_status() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;

    let roots = storage.list_sync_roots(&provider, &account.subject)?;
    let with_remote_root = roots
        .iter()
        .filter(|root| root.remote_root_id.is_some())
        .count();
    let root_catalog_states = storage.sync_root_catalog_state_count(&provider, &account.subject)?;
    let root_catalog_items = storage.sync_root_catalog_item_count(&provider, &account.subject)?;
    let root_change_cursors = storage.sync_root_change_cursor_count(&provider, &account.subject)?;

    println!("SYNC_ROOTS_STATUS=PASS");
    println!("CONFIGURED_ROOTS={}", roots.len());
    println!("REMOTE_ROOTS_CONFIGURED={with_remote_root}");
    println!("ROOT_CATALOG_STATES={root_catalog_states}");
    println!("ROOT_CATALOG_ITEMS={root_catalog_items}");
    println!("ROOT_CHANGE_CURSORS_PRESENT={root_change_cursors}");
    println!("ROOT_PATHS_PRINTED=no");
    println!("REMOTE_ROOT_IDS_PRINTED=no");
    println!("NETWORK_CHECK=not_performed");
    println!("FILESYSTEM_MUTATION=no");
    println!("DRIVE_WRITE_ACCESS=no");

    Ok(())
}

fn drive_catalog_catchup() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let mut storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let state = storage.remote_inventory_state(&provider, &account.subject)?;

    if !state.snapshot_complete {
        println!("DRIVE_CATALOG_CATCHUP=SKIPPED");
        println!("REASON=snapshot_missing");
        println!("NETWORK_CHECK=not_performed");
        println!("REMOTE_EVENTS_MODIFIED=no");
        println!("PROVIDER_CURSOR_MODIFIED=no");
        return Ok(());
    }

    if state.catchup_complete {
        println!("DRIVE_CATALOG_CATCHUP=SKIPPED");
        println!("REASON=already_complete");
        println!("NETWORK_CHECK=not_performed");
        println!("REMOTE_EVENTS_MODIFIED=no");
        println!("PROVIDER_CURSOR_MODIFIED=no");
        return Ok(());
    }

    let from_cursor = state
        .catchup_from_cursor
        .ok_or(CliError::MissingCatalogCatchupCursor)?;

    ensure_keyring_available()?;
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

    let mut continuation = None;
    let mut seen_continuations = HashSet::new();
    let mut pages_fetched = 0_u64;
    let mut collected_changes = Vec::new();

    let checkpoint = loop {
        if pages_fetched >= 10_000 {
            return Err(CliError::DriveChangePageLimitExceeded);
        }

        let page = api.list_changes_page(&from_cursor, continuation.as_ref())?;
        pages_fetched += 1;
        collected_changes.extend(page.changes);

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

    let result = storage.commit_remote_catalog_catchup(
        &provider,
        &account.subject,
        &from_cursor,
        &collected_changes,
        &checkpoint,
        unix_time_ms()?,
    )?;

    let final_state = storage.remote_inventory_state(&provider, &account.subject)?;
    let staging_items = storage.staged_remote_inventory_count(&provider, &account.subject)?;
    let state_consistent = final_state.item_count == result.authoritative_items;
    let ready = final_state.ready_for_reconciliation() && state_consistent && staging_items == 0;

    println!("DRIVE_CATALOG_CATCHUP=PASS");
    println!("PAGES_FETCHED={pages_fetched}");
    println!("CHANGES_TOTAL={}", collected_changes.len());
    println!("CATALOG_CHANGES_APPLIED={}", result.changes_applied);
    println!("AUTHORITATIVE_ITEMS={}", result.authoritative_items);
    println!(
        "REMOTE_EVENTS_SUPERSEDED={}",
        result.remote_events_superseded
    );
    println!("CATCHUP_COMPLETE={}", yes_no(final_state.catchup_complete));
    println!("STATE_CONSISTENT={}", yes_no(state_consistent));
    println!("READY_FOR_RECONCILIATION={}", yes_no(ready));
    println!("PROVIDER_CURSOR_ADVANCED=yes");
    println!("FILE_CONTENT_ACCESSED=no");
    println!("DRIVE_WRITE_ACCESS=no");
    println!("REMOTE_METADATA_PRINTED=no");

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
    println!(
        "CATCHUP_CURSOR_PRESENT={}",
        yes_no(state.catchup_from_cursor.is_some())
    );
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

fn drive_folder_validate(remote_root_id: &str) -> Result<(), CliError> {
    println!("DRIVE_FOLDER_VALIDATE_STAGE=local_session");
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

    println!("DRIVE_FOLDER_VALIDATE_STAGE=refresh_access_token");
    let oauth = GoogleOAuthConfig::new(client_id)?;
    let tokens = oauth.refresh_access_token(&refresh_token, &client_secret)?;

    if let Some(rotated_refresh_token) = tokens.refresh_token() {
        keyring.put(
            &refresh_key,
            SecretValue::new(rotated_refresh_token.as_bytes().to_vec())?,
        )?;
    }

    println!("DRIVE_FOLDER_VALIDATE_STAGE=verify_account");
    let api = GoogleDriveApi::new(tokens.access_token().clone())?;
    let user = api.user_info()?;
    if user.sub != account.subject {
        return Err(CliError::GoogleAccountMismatch);
    }

    println!("DRIVE_FOLDER_VALIDATE_STAGE=validate_remote_root");
    api.validate_folder_root(remote_root_id)?;

    println!("DRIVE_FOLDER_VALIDATE=PASS");
    println!("REMOTE_ROOT_KIND=folder");
    println!("REMOTE_ROOT_OWNERSHIP=my_drive");
    println!("REMOTE_ROOT_TRASHED=no");
    println!("REMOTE_ROOT_ID_PRINTED=no");
    println!("REMOTE_METADATA_PRINTED=no");
    println!("INVENTORY_PERSISTED=no");
    println!("SYNC_ROOT_PERSISTED=no");
    println!("FILE_CONTENT_ACCESSED=no");
    println!("DRIVE_WRITE_ACCESS=no");

    Ok(())
}

fn parse_folder_tree_limit(value: &str) -> Result<u64, CliError> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| CliError::InvalidFolderTreeLimit)?;

    if !(1..=10_000).contains(&parsed) {
        return Err(CliError::InvalidFolderTreeLimit);
    }

    Ok(parsed)
}

fn drive_folder_tree(root_remote_id: &str, max_items: u64) -> Result<(), CliError> {
    println!("DRIVE_FOLDER_TREE_STAGE=local_session");
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

    println!("DRIVE_FOLDER_TREE_STAGE=refresh_access_token");
    let oauth = GoogleOAuthConfig::new(client_id)?;
    let tokens = oauth.refresh_access_token(&refresh_token, &client_secret)?;

    if let Some(rotated_refresh_token) = tokens.refresh_token() {
        keyring.put(
            &refresh_key,
            SecretValue::new(rotated_refresh_token.as_bytes().to_vec())?,
        )?;
    }

    println!("DRIVE_FOLDER_TREE_STAGE=verify_account");
    let api = GoogleDriveApi::new(tokens.access_token().clone())?;
    let user = api.user_info()?;
    if user.sub != account.subject {
        return Err(CliError::GoogleAccountMismatch);
    }

    let mut folder_queue = VecDeque::new();
    let mut seen_folders = HashSet::new();

    seen_folders.insert(root_remote_id.to_owned());
    folder_queue.push_back(root_remote_id.to_owned());

    let mut pages_fetched = 0_u64;
    let mut folders_visited = 0_u64;
    let mut observed_items = 0_u64;
    let mut supported_items = 0_u64;
    let mut files = 0_u64;
    let mut folders = 0_u64;
    let mut unsupported_provider_native = 0_u64;
    let mut limit_reached = false;

    'folders: while let Some(parent_remote_id) = folder_queue.pop_front() {
        folders_visited += 1;

        let mut continuation = None;
        let mut seen_continuations = HashSet::new();

        loop {
            if observed_items >= max_items {
                limit_reached = true;
                break 'folders;
            }

            if pages_fetched >= 10_000 {
                return Err(CliError::DriveFolderTreePageLimitExceeded);
            }

            let remaining = (max_items - observed_items).min(1000);
            let page_size =
                u16::try_from(remaining).map_err(|_| CliError::InvalidFolderTreeLimit)?;

            let page_number = pages_fetched + 1;
            println!("DRIVE_FOLDER_TREE_FETCH_PAGE={page_number}");

            let page =
                api.list_folder_children_page(&parent_remote_id, continuation.as_ref(), page_size)?;
            pages_fetched += 1;

            for item in &page.items {
                if item.kind == RemoteItemKind::Folder
                    && seen_folders.insert(item.remote_id.clone())
                {
                    folder_queue.push_back(item.remote_id.clone());
                }
            }

            let page_items = page.supported_items + page.unsupported_provider_native;
            observed_items += page_items;
            supported_items += page.supported_items;
            files += page.file_count;
            folders += page.folder_count;
            unsupported_provider_native += page.unsupported_provider_native;

            println!(
                "DRIVE_FOLDER_TREE_PAGE_COMPLETE={} OBSERVED_SO_FAR={} FILES_SO_FAR={} FOLDERS_SO_FAR={} QUEUED_FOLDERS={}",
                pages_fetched,
                observed_items,
                files,
                folders,
                folder_queue.len()
            );

            match page.continuation {
                Some(next) => {
                    if !seen_continuations.insert(next.as_str().to_owned()) {
                        return Err(CliError::DriveFolderTreePaginationLoop);
                    }

                    continuation = Some(next);

                    if observed_items >= max_items {
                        limit_reached = true;
                        break 'folders;
                    }
                }
                None => break,
            }
        }
    }

    let traversal_complete = !limit_reached && folder_queue.is_empty();

    println!("DRIVE_FOLDER_TREE=PASS");
    println!("MODE=bounded_recursive");
    println!("MAX_ITEMS={max_items}");
    println!("TRAVERSAL_COMPLETE={}", yes_no(traversal_complete));
    println!("LIMIT_REACHED={}", yes_no(limit_reached));
    println!("PAGES_FETCHED={pages_fetched}");
    println!("FOLDERS_VISITED={folders_visited}");
    println!("OBSERVED_ITEMS={observed_items}");
    println!("SUPPORTED_ITEMS={supported_items}");
    println!("FILES={files}");
    println!("FOLDERS={folders}");
    println!("UNSUPPORTED_PROVIDER_NATIVE={unsupported_provider_native}");
    println!("REMOTE_ROOT_ID_PRINTED=no");
    println!("REMOTE_ITEM_IDS_PRINTED=no");
    println!("INVENTORY_PERSISTED=no");
    println!("PROVIDER_CURSOR_MODIFIED=no");
    println!("REMOTE_EVENTS_MODIFIED=no");
    println!("REMOTE_METADATA_PRINTED=no");
    println!("FILE_CONTENT_ACCESSED=no");
    println!("DRIVE_WRITE_ACCESS=no");

    Ok(())
}

fn parse_folder_probe_limit(value: &str) -> Result<u16, CliError> {
    let parsed = value
        .parse::<u16>()
        .map_err(|_| CliError::InvalidFolderProbeLimit)?;

    if !(1..=1000).contains(&parsed) {
        return Err(CliError::InvalidFolderProbeLimit);
    }

    Ok(parsed)
}

fn drive_folder_probe(parent_remote_id: &str, max_items: u16) -> Result<(), CliError> {
    println!("DRIVE_FOLDER_PROBE_STAGE=local_session");
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

    println!("DRIVE_FOLDER_PROBE_STAGE=refresh_access_token");
    let oauth = GoogleOAuthConfig::new(client_id)?;
    let tokens = oauth.refresh_access_token(&refresh_token, &client_secret)?;

    if let Some(rotated_refresh_token) = tokens.refresh_token() {
        keyring.put(
            &refresh_key,
            SecretValue::new(rotated_refresh_token.as_bytes().to_vec())?,
        )?;
    }

    println!("DRIVE_FOLDER_PROBE_STAGE=verify_account");
    let api = GoogleDriveApi::new(tokens.access_token().clone())?;
    let user = api.user_info()?;
    if user.sub != account.subject {
        return Err(CliError::GoogleAccountMismatch);
    }

    println!("DRIVE_FOLDER_PROBE_FETCH_PAGE=1");
    let page = api.list_folder_children_page(parent_remote_id, None, max_items)?;

    let observed_items = page.supported_items + page.unsupported_provider_native;

    println!("DRIVE_FOLDER_PROBE=PASS");
    println!("MAX_ITEMS={max_items}");
    println!("OBSERVED_ITEMS={observed_items}");
    println!("SUPPORTED_ITEMS={}", page.supported_items);
    println!("FILES={}", page.file_count);
    println!("FOLDERS={}", page.folder_count);
    println!(
        "UNSUPPORTED_PROVIDER_NATIVE={}",
        page.unsupported_provider_native
    );
    println!(
        "MORE_CHILDREN_AVAILABLE={}",
        yes_no(page.continuation.is_some())
    );
    println!("REMOTE_ROOT_ID_PRINTED=no");
    println!("INVENTORY_PERSISTED=no");
    println!("PROVIDER_CURSOR_MODIFIED=no");
    println!("REMOTE_EVENTS_MODIFIED=no");
    println!("REMOTE_METADATA_PRINTED=no");
    println!("FILE_CONTENT_ACCESSED=no");
    println!("DRIVE_WRITE_ACCESS=no");

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

    let bootstrap_fence = if max_items.is_none() {
        println!("DRIVE_INVENTORY_STAGE=capture_bootstrap_fence");
        Some(api.current_change_cursor()?)
    } else {
        None
    };

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
        let bootstrap_fence = bootstrap_fence
            .as_ref()
            .ok_or(CliError::MissingInventoryBootstrapFence)?;

        u64::try_from(storage.commit_remote_inventory_snapshot(
            &provider,
            &account.subject,
            bootstrap_fence,
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

fn oauth_scope_contains(scope: Option<&str>, required_scope: &str) -> bool {
    scope.is_some_and(|value| {
        value
            .split_ascii_whitespace()
            .any(|candidate| candidate == required_scope)
    })
}

fn google_upgrade_readonly() -> Result<(), CliError> {
    ensure_keyring_available()?;

    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;

    let keyring = KeyringSecretStore::default();
    let (client_id, client_secret) = load_google_client_config(&keyring)?;
    let oauth = GoogleOAuthConfig::new(client_id)?;
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
        oauth.begin_authorization(listen_addr.port(), GoogleDriveAccess::ReadOnly)?;

    println!("GOOGLE_READONLY_UPGRADE_STAGE=authorize");
    println!("REQUESTED_DRIVE_ACCESS=read_only");
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
                "NubiSync received the Google read-only authorization. You can close this tab.",
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

    println!("GOOGLE_READONLY_UPGRADE_STAGE=exchange_code");
    let tokens = oauth.exchange_code(&authorization, &code, &client_secret)?;

    if !oauth_scope_contains(tokens.scope(), GOOGLE_DRIVE_READONLY_SCOPE) {
        return Err(CliError::GoogleReadonlyScopeNotGranted);
    }

    println!("GOOGLE_READONLY_UPGRADE_STAGE=verify_account");
    let api = GoogleDriveApi::new(tokens.access_token().clone())?;
    let user = api.user_info()?;

    if user.sub != account.subject {
        return Err(CliError::GoogleAccountMismatch);
    }

    let refresh_token = tokens
        .refresh_token()
        .ok_or(CliError::GoogleReadonlyRefreshTokenMissing)?;

    keyring.put(
        &refresh_token_key(&account.subject)?,
        SecretValue::new(refresh_token.as_bytes().to_vec())?,
    )?;

    println!("GOOGLE_READONLY_UPGRADE=PASS");
    println!("ACCOUNT_SUBJECT_MATCH=yes");
    println!("REQUESTED_SCOPE=drive.readonly");
    println!("GRANTED_SCOPE_VERIFIED=yes");
    println!("REFRESH_TOKEN_REPLACED=yes");
    println!("REFRESH_TOKEN_STORAGE=OS_KEYRING");
    println!("ACCESS_TOKEN_STORAGE=memory_only");
    println!("DATABASE_MUTATION=no");
    println!("FILESYSTEM_MUTATION=no");
    println!("DRIVE_FILE_CONTENT_ACCESSED=no");
    println!("DRIVE_WRITE_ACCESS=no");
    println!("TOKEN_VALUES_PRINTED=no");

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

#[cfg(test)]
mod sync_root_cli_tests {
    use super::*;

    fn temp_test_dir(label: &str) -> PathBuf {
        let unique = format!(
            "nubisync-{label}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        );
        std::env::temp_dir().join(unique)
    }

    #[test]
    fn oauth_scope_matching_is_exact_and_fail_closed() {
        assert!(oauth_scope_contains(
            Some("openid email https://www.googleapis.com/auth/drive.readonly profile"),
            GOOGLE_DRIVE_READONLY_SCOPE,
        ));
        assert!(!oauth_scope_contains(
            Some("openid email https://www.googleapis.com/auth/drive.metadata.readonly profile"),
            GOOGLE_DRIVE_READONLY_SCOPE,
        ));
        assert!(!oauth_scope_contains(
            Some("https://www.googleapis.com/auth/drive.readonly.extra"),
            GOOGLE_DRIVE_READONLY_SCOPE,
        ));
        assert!(!oauth_scope_contains(None, GOOGLE_DRIVE_READONLY_SCOPE));
    }

    #[test]
    fn metadata_step_state_machine_is_explicit() {
        assert_eq!(
            classify_sync_root_metadata_step(false, None),
            SyncRootMetadataStep::Bootstrap
        );
        assert_eq!(
            classify_sync_root_metadata_step(true, None),
            SyncRootMetadataStep::CollectChangePage
        );
        assert_eq!(
            classify_sync_root_metadata_step(true, Some(false)),
            SyncRootMetadataStep::CollectChangePage
        );
        assert_eq!(
            classify_sync_root_metadata_step(true, Some(true)),
            SyncRootMetadataStep::ExecuteWindow
        );
    }

    #[test]
    fn sync_root_mode_is_fail_closed_to_receive_only() {
        assert_eq!(
            parse_sync_root_mode("receive_only").unwrap(),
            SyncMode::ReceiveOnly
        );
        assert!(matches!(
            parse_sync_root_mode("two_way"),
            Err(CliError::SyncRootModeNotYetSupported)
        ));
        assert!(matches!(
            parse_sync_root_mode("mirror_local_to_remote"),
            Err(CliError::SyncRootModeNotYetSupported)
        ));
    }

    #[test]
    fn local_sync_directory_must_exist_and_be_absolute_directory() {
        let path = temp_test_dir("root");
        fs::create_dir(&path).unwrap();

        let validated = validate_local_sync_directory(path.to_str().unwrap()).unwrap();
        assert_eq!(PathBuf::from(validated), fs::canonicalize(&path).unwrap());

        fs::remove_dir(&path).unwrap();

        assert!(matches!(
            validate_local_sync_directory("relative/path"),
            Err(CliError::LocalSyncDirectoryMustBeAbsolute)
        ));
    }

    #[test]
    fn local_sync_directory_rejects_filesystem_root_and_non_empty_directory() {
        assert!(matches!(
            validate_local_sync_directory("/"),
            Err(CliError::LocalSyncDirectoryFilesystemRootUnsupported)
        ));

        let path = temp_test_dir("non-empty");
        fs::create_dir(&path).unwrap();
        fs::write(path.join("existing.txt"), b"existing").unwrap();

        assert!(matches!(
            validate_local_sync_directory(path.to_str().unwrap()),
            Err(CliError::LocalSyncDirectoryNotEmpty)
        ));

        fs::remove_file(path.join("existing.txt")).unwrap();
        fs::remove_dir(&path).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn local_sync_directory_rejects_symlink_root() {
        use std::os::unix::fs::symlink;

        let target = temp_test_dir("target");
        let link = temp_test_dir("link");
        fs::create_dir(&target).unwrap();
        symlink(&target, &link).unwrap();

        assert!(matches!(
            validate_local_sync_directory(link.to_str().unwrap()),
            Err(CliError::LocalSyncDirectorySymlinkUnsupported)
        ));

        fs::remove_file(&link).unwrap();
        fs::remove_dir(&target).unwrap();
    }
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
    #[error("Google did not grant the requested Drive read-only scope")]
    GoogleReadonlyScopeNotGranted,
    #[error("Google did not return a new refresh token for the Drive read-only upgrade")]
    GoogleReadonlyRefreshTokenMissing,
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
    Daemon(#[from] nubisync_daemon::SelectedRootExecutorError),
    #[error(transparent)]
    Storage(#[from] nubisync_storage::StorageError),
    #[error("local filesystem operation failed")]
    Io(#[from] std::io::Error),
    #[error("OAuth callback URL was invalid")]
    Url(#[from] url::ParseError),
    #[error("full inventory completed without its bootstrap change fence")]
    MissingInventoryBootstrapFence,
    #[error("remote catalog catch-up cursor is missing")]
    MissingCatalogCatchupCursor,
    #[error("folder probe limit must be an integer between 1 and 1000")]
    InvalidFolderProbeLimit,
    #[error("folder tree limit must be an integer between 1 and 10000")]
    InvalidFolderTreeLimit,
    #[error("Drive folder tree pagination repeated a continuation token")]
    DriveFolderTreePaginationLoop,
    #[error("Drive folder tree pagination exceeded the safety limit")]
    DriveFolderTreePageLimitExceeded,
    #[error("only receive_only sync roots are supported in this alpha phase")]
    SyncRootModeNotYetSupported,
    #[error("local sync directory is invalid")]
    InvalidLocalSyncDirectory,
    #[error("local sync directory must be an absolute path")]
    LocalSyncDirectoryMustBeAbsolute,
    #[error("local sync directory does not exist or cannot be inspected")]
    LocalSyncDirectoryUnavailable,
    #[error("local sync directory cannot be a symbolic link")]
    LocalSyncDirectorySymlinkUnsupported,
    #[error("local sync path is not a directory")]
    LocalSyncDirectoryNotDirectory,
    #[error("local sync directory is not representable as UTF-8")]
    LocalSyncDirectoryNonUtf8,
    #[error("local sync directory is already registered")]
    LocalSyncDirectoryAlreadyRegistered,
    #[error("remote Drive sync root is already registered")]
    RemoteSyncRootAlreadyRegistered,
    #[error("filesystem root cannot be used as a sync root")]
    LocalSyncDirectoryFilesystemRootUnsupported,
    #[error("the user's home directory cannot be used directly as a sync root")]
    LocalSyncDirectoryHomeUnsupported,
    #[error("local sync directory must be empty for initial receive-only registration")]
    LocalSyncDirectoryNotEmpty,
    #[error("sync root inventory selection failed")]
    SyncRootInventorySelectionFailed,
    #[error("sync root metadata-step selection failed")]
    SyncRootMetadataStepSelectionFailed,
    #[error("sync root reconciliation-plan selection failed")]
    SyncRootReconcilePlanSelectionFailed,
    #[error("selected receive-only file is not ready for safe replacement")]
    SyncRootReplacementNotReady,
    #[error("sync root metadata-step currently supports only receive_only roots")]
    SyncRootMetadataStepModeUnsupported,
    #[error("configured sync root does not have a remote root identifier")]
    SyncRootInventoryRemoteRootMissing,
    #[error("sync root inventory pagination repeated a continuation token")]
    SyncRootInventoryPaginationLoop,
    #[error("sync root inventory pagination exceeded the safety limit")]
    SyncRootInventoryPageLimitExceeded,
}
