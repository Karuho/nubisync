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
    SUPERVISED_FILE_BATCH_MAX_ACTIONS, SUPERVISED_FILE_DOWNLOAD_MAX_BYTES,
    SUPERVISED_RECEIVE_ONLY_RUN_MAX_ROUNDS, SelectedRootCrossProcessExecutionLock,
    SelectedRootReceiveOnlySingleFlightResult, SelectedRootRemoteWritePlanDisposition,
    adopt_selected_root_existing_directory, bootstrap_selected_root_snapshot,
    capture_selected_root_local_baseline, collect_selected_root_change_window_page,
    delete_selected_root_existing_directory, delete_selected_root_existing_file,
    delete_selected_root_stale_directories, delete_selected_root_stale_files,
    execute_completed_selected_root_change_window, execute_selected_root_receive_only_cycle,
    execute_selected_root_receive_only_single_flight,
    execute_selected_root_unified_convergence_step, journal_selected_root_local_inventory_diff,
    journal_selected_root_two_way_local_inventory_diff, materialize_selected_root_directories,
    materialize_selected_root_missing_file, materialize_selected_root_missing_files,
    plan_selected_root_confirmed_folder_create_settlement, plan_selected_root_local_inventory_diff,
    plan_selected_root_local_materialization, plan_selected_root_receive_only_convergence,
    plan_selected_root_remote_deletion, plan_selected_root_remote_directory_deletion,
    plan_selected_root_remote_replacement, plan_selected_root_remote_write_intents,
    plan_selected_root_stale_files, plan_selected_root_unified_convergence_step,
    replace_selected_root_existing_file, replace_selected_root_stale_files,
    try_acquire_selected_root_cross_process_execution_lock,
    validate_selected_root_folder_create_local_identity, verify_selected_root_existing_file,
    verify_selected_root_existing_files, verify_selected_root_local_receipts,
};
use nubisync_drive::{
    DriveExpectedFolderLookup, DriveFolderCreateSubmission, DriveRootMembership,
    GOOGLE_DRIVE_FULL_SCOPE, GOOGLE_DRIVE_READONLY_SCOPE, GoogleDriveAccess, GoogleDriveApi,
    GoogleOAuthConfig,
};
use nubisync_storage::{
    REMOTE_WRITE_INTENT_BATCH_MAX, RemoteWriteAuthoritySnapshot, RemoteWriteIntentStatus, Storage,
};
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
const FULLSYNC_REFRESH_TOKEN_PURPOSE: &str = "full-sync-refresh-token";
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

    let _cross_process_guard = if cli_requires_cross_process_execution_lock(&args) {
        match try_acquire_selected_root_cross_process_execution_lock(
            &nubisync_execution_lock_path()?,
        )? {
            SelectedRootCrossProcessExecutionLock::Acquired(guard) => {
                println!("CROSS_PROCESS_LOCK=acquired");
                println!("CROSS_PROCESS_LOCK_SCOPE=user_global_sync_execution");
                println!("EXECUTION_LOCK_PATH_PRINTED=no");
                Some(guard)
            }
            SelectedRootCrossProcessExecutionLock::Busy => {
                println!("SYNC_EXECUTION=BUSY");
                println!("CROSS_PROCESS_LOCK=busy");
                println!("CROSS_PROCESS_LOCK_SCOPE=user_global_sync_execution");
                println!("DATABASE_MUTATION=no");
                println!("FILESYSTEM_MUTATION=no");
                println!("ROOT_PATH_PRINTED=no");
                println!("LOCAL_NAMES_PRINTED=no");
                println!("EXECUTION_LOCK_PATH_PRINTED=no");
                println!("DRIVE_WRITE_ACCESS=no");
                return Ok(());
            }
        }
    } else {
        None
    };

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
        [auth, google, upgrade_full_sync, approve]
            if auth == "auth"
                && google == "google"
                && upgrade_full_sync == "upgrade-full-sync"
                && approve == "--approve" =>
        {
            google_upgrade_full_sync()
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
        [sync, roots, cycle, approve]
            if sync == "sync" && roots == "roots" && cycle == "cycle" && approve == "--approve" =>
        {
            sync_roots_cycle()
        }
        [sync, roots, run_to_idle, approve]
            if sync == "sync"
                && roots == "roots"
                && run_to_idle == "run-to-idle"
                && approve == "--approve" =>
        {
            sync_roots_run_to_idle()
        }
        [sync, roots, local_baseline, approve]
            if sync == "sync"
                && roots == "roots"
                && local_baseline == "local-baseline"
                && approve == "--approve" =>
        {
            sync_roots_local_baseline()
        }
        [sync, roots, local_diff, approve]
            if sync == "sync"
                && roots == "roots"
                && local_diff == "local-diff"
                && approve == "--approve" =>
        {
            sync_roots_local_diff()
        }
        [sync, roots, local_journal, approve]
            if sync == "sync"
                && roots == "roots"
                && local_journal == "local-journal"
                && approve == "--approve" =>
        {
            sync_roots_local_journal()
        }
        [sync, roots, refresh_two_way_metadata, approve]
            if sync == "sync"
                && roots == "roots"
                && refresh_two_way_metadata == "refresh-two-way-metadata"
                && approve == "--approve" =>
        {
            sync_roots_refresh_two_way_metadata()
        }
        [sync, roots, observe_write_authority, approve]
            if sync == "sync"
                && roots == "roots"
                && observe_write_authority == "observe-write-authority"
                && approve == "--approve" =>
        {
            sync_roots_observe_write_authority()
        }
        [sync, roots, remote_write_plan, approve]
            if sync == "sync"
                && roots == "roots"
                && remote_write_plan == "remote-write-plan"
                && approve == "--approve" =>
        {
            sync_roots_remote_write_plan()
        }
        [sync, roots, allocate_create_ids, approve]
            if sync == "sync"
                && roots == "roots"
                && allocate_create_ids == "allocate-create-ids"
                && approve == "--approve" =>
        {
            sync_roots_allocate_create_ids()
        }
        [sync, roots, folder_create_recovery_plan, approve]
            if sync == "sync"
                && roots == "roots"
                && folder_create_recovery_plan == "folder-create-recovery-plan"
                && approve == "--approve" =>
        {
            sync_roots_folder_create_recovery_plan()
        }
        [sync, roots, submit_folder_create, approve]
            if sync == "sync"
                && roots == "roots"
                && submit_folder_create == "submit-folder-create"
                && approve == "--approve" =>
        {
            sync_roots_submit_folder_create()
        }
        [sync, roots, recover_folder_create, approve]
            if sync == "sync"
                && roots == "roots"
                && recover_folder_create == "recover-folder-create-submission"
                && approve == "--approve" =>
        {
            sync_roots_recover_folder_create_submission()
        }
        [sync, roots, confirm_folder_create, approve]
            if sync == "sync"
                && roots == "roots"
                && confirm_folder_create == "confirm-folder-create"
                && approve == "--approve" =>
        {
            sync_roots_confirm_folder_create()
        }
        [sync, roots, settle_confirmed_folder_create, approve]
            if sync == "sync"
                && roots == "roots"
                && settle_confirmed_folder_create == "settle-confirmed-folder-create"
                && approve == "--approve" =>
        {
            sync_roots_settle_confirmed_folder_create()
        }
        [sync, roots, activate_two_way, approve]
            if sync == "sync"
                && roots == "roots"
                && activate_two_way == "activate-two-way"
                && approve == "--approve" =>
        {
            sync_roots_activate_two_way()
        }
        [sync, roots, deactivate_two_way, approve]
            if sync == "sync"
                && roots == "roots"
                && deactivate_two_way == "deactivate-two-way"
                && approve == "--approve" =>
        {
            sync_roots_deactivate_two_way()
        }
        [sync, roots, convergence_plan, approve]
            if sync == "sync"
                && roots == "roots"
                && convergence_plan == "convergence-plan"
                && approve == "--approve" =>
        {
            sync_roots_convergence_plan()
        }
        [sync, roots, converge, approve]
            if sync == "sync"
                && roots == "roots"
                && converge == "converge"
                && approve == "--approve" =>
        {
            sync_roots_converge()
        }
        [sync, roots, stale_files_plan, approve]
            if sync == "sync"
                && roots == "roots"
                && stale_files_plan == "stale-files-plan"
                && approve == "--approve" =>
        {
            sync_roots_stale_files_plan()
        }
        [sync, roots, replace_stale_files, approve]
            if sync == "sync"
                && roots == "roots"
                && replace_stale_files == "replace-stale-files"
                && approve == "--approve" =>
        {
            sync_roots_replace_stale_files()
        }
        [sync, roots, delete_stale_files, approve]
            if sync == "sync"
                && roots == "roots"
                && delete_stale_files == "delete-stale-files"
                && approve == "--approve" =>
        {
            sync_roots_delete_stale_files()
        }
        [sync, roots, delete_stale_directories, approve]
            if sync == "sync"
                && roots == "roots"
                && delete_stale_directories == "delete-stale-directories"
                && approve == "--approve" =>
        {
            sync_roots_delete_stale_directories()
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
        [sync, roots, adopt_directory, approve]
            if sync == "sync"
                && roots == "roots"
                && adopt_directory == "adopt-directory"
                && approve == "--approve" =>
        {
            sync_roots_adopt_directory()
        }
        [sync, roots, materialize_files, approve]
            if sync == "sync"
                && roots == "roots"
                && materialize_files == "materialize-files"
                && approve == "--approve" =>
        {
            sync_roots_materialize_files()
        }
        [sync, roots, materialize_file, approve]
            if sync == "sync"
                && roots == "roots"
                && materialize_file == "materialize-file"
                && approve == "--approve" =>
        {
            sync_roots_materialize_file()
        }
        [sync, roots, verify_files, approve]
            if sync == "sync"
                && roots == "roots"
                && verify_files == "verify-files"
                && approve == "--approve" =>
        {
            sync_roots_verify_files()
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
        [sync, roots, directory_deletion_plan, approve]
            if sync == "sync"
                && roots == "roots"
                && directory_deletion_plan == "directory-deletion-plan"
                && approve == "--approve" =>
        {
            sync_roots_directory_deletion_plan()
        }
        [sync, roots, delete_directory, approve]
            if sync == "sync"
                && roots == "roots"
                && delete_directory == "delete-directory"
                && approve == "--approve" =>
        {
            sync_roots_delete_directory()
        }
        [sync, roots, deletion_plan, approve]
            if sync == "sync"
                && roots == "roots"
                && deletion_plan == "deletion-plan"
                && approve == "--approve" =>
        {
            sync_roots_deletion_plan()
        }
        [sync, roots, delete_file, approve]
            if sync == "sync"
                && roots == "roots"
                && delete_file == "delete-file"
                && approve == "--approve" =>
        {
            sync_roots_delete_file()
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
  nubisync auth google upgrade-full-sync --approve
  nubisync auth google configure
  nubisync auth google status
  nubisync auth google refresh
  nubisync auth google logout
  nubisync sync roots status
  nubisync sync roots metadata-step --approve
  nubisync sync roots cycle --approve
  nubisync sync roots run-to-idle --approve
  nubisync sync roots local-baseline --approve
  nubisync sync roots local-diff --approve
  nubisync sync roots local-journal --approve
  nubisync sync roots refresh-two-way-metadata --approve
  nubisync sync roots observe-write-authority --approve
  nubisync sync roots remote-write-plan --approve
  nubisync sync roots allocate-create-ids --approve
  nubisync sync roots folder-create-recovery-plan --approve
  nubisync sync roots submit-folder-create --approve
  nubisync sync roots recover-folder-create-submission --approve
  nubisync sync roots confirm-folder-create --approve
  nubisync sync roots settle-confirmed-folder-create --approve
  nubisync sync roots activate-two-way --approve
  nubisync sync roots deactivate-two-way --approve
  nubisync sync roots convergence-plan --approve
  nubisync sync roots converge --approve
  nubisync sync roots stale-files-plan --approve
  nubisync sync roots replace-stale-files --approve
  nubisync sync roots delete-stale-files --approve
  nubisync sync roots delete-stale-directories --approve
  nubisync sync roots reconcile-plan --approve
  nubisync sync roots materialize-directories --approve
  nubisync sync roots adopt-directory --approve
  nubisync sync roots materialize-files --approve
  nubisync sync roots materialize-file --approve
  nubisync sync roots verify-files --approve
  nubisync sync roots verify-file --approve
  nubisync sync roots verify-local --approve
  nubisync sync roots replacement-plan --approve
  nubisync sync roots replace-file --approve
  nubisync sync roots directory-deletion-plan --approve
  nubisync sync roots delete-directory --approve
  nubisync sync roots deletion-plan --approve
  nubisync sync roots delete-file --approve
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
    let fullsync_refresh_present = keyring
        .get(&fullsync_refresh_token_key(&account.subject)?)?
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
    println!(
        "FULLSYNC_REFRESH_TOKEN_PRESENT={}",
        yes_no(fullsync_refresh_present)
    );
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
    keyring.delete(&fullsync_refresh_token_key(&account.subject)?)?;

    println!("GOOGLE_LOGOUT=PASS");
    println!("REFRESH_TOKEN_REMOVED=yes");
    println!("FULLSYNC_REFRESH_TOKEN_REMOVED=yes");
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

fn sync_roots_refresh_two_way_metadata() -> Result<(), CliError> {
    const MAX_PAGES: usize = 64;

    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let mut storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;
    if roots.len() != 1 {
        return Err(CliError::SyncRootTwoWayMetadataRefreshSelectionFailed);
    }
    let root = roots
        .into_iter()
        .next()
        .ok_or(CliError::SyncRootTwoWayMetadataRefreshSelectionFailed)?;
    if root.mode != SyncMode::TwoWay {
        return Err(CliError::SyncRootTwoWayMetadataRefreshModeUnsupported);
    }
    if storage.sync_root_remote_write_intent_count(&root.id)? != 0 {
        return Err(CliError::SyncRootTwoWayMetadataRefreshExistingIntents);
    }

    let local_before = storage.sync_root_local_inventory_state(&root.id)?;
    if !local_before.snapshot_complete || !local_before.observation_valid {
        return Err(CliError::SyncRootTwoWayMetadataRefreshLocalStateNotReady);
    }
    let pending_before =
        storage.pending_sync_root_local_change_event_count(&root.id, local_before.generation)?;

    let remote_before = storage.sync_root_remote_inventory_state(&root.id)?;
    if !remote_before.ready_for_reconciliation() {
        return Err(CliError::SyncRootTwoWayMetadataRefreshRemoteStateNotReady);
    }
    let cursor_before = storage
        .sync_root_change_cursor(&root.id)?
        .ok_or(CliError::SyncRootTwoWayMetadataRefreshRemoteStateNotReady)?;

    ensure_keyring_available()?;
    let keyring = KeyringSecretStore::default();
    let refresh_key = refresh_token_key(&account.subject)?;
    let refresh_token = required_secret_utf8(
        keyring.get(&refresh_key)?,
        CliError::MissingStoredRefreshToken,
    )?;
    let (client_id, client_secret) = load_google_client_config(&keyring)?;

    println!("SYNC_ROOT_TWO_WAY_METADATA_REFRESH_STAGE=refresh_readonly_access_token");
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

    let api = GoogleDriveApi::new(tokens.access_token().clone())?;
    if api.user_info()?.sub != account.subject {
        return Err(CliError::GoogleAccountMismatch);
    }

    println!("SYNC_ROOT_TWO_WAY_METADATA_REFRESH_STAGE=collect_change_window");
    let mut pages = 0usize;
    loop {
        let result = collect_selected_root_change_window_page(&api, &mut storage, &root)?;
        pages = pages.checked_add(1).ok_or(CliError::NumericOverflow)?;
        if result.complete {
            break;
        }
        if pages >= MAX_PAGES {
            return Err(CliError::SyncRootTwoWayMetadataRefreshPageLimitExceeded);
        }
    }

    println!("SYNC_ROOT_TWO_WAY_METADATA_REFRESH_STAGE=commit_change_window");
    let execution =
        execute_completed_selected_root_change_window(&api, &mut storage, &root, unix_time_ms()?)?;

    let local_after = storage.sync_root_local_inventory_state(&root.id)?;
    let pending_after =
        storage.pending_sync_root_local_change_event_count(&root.id, local_after.generation)?;
    if local_after.generation != local_before.generation
        || local_after.item_count != local_before.item_count
        || local_after.snapshot_completed_at_unix_ms != local_before.snapshot_completed_at_unix_ms
        || local_after.observation_valid != local_before.observation_valid
        || pending_after != pending_before
    {
        return Err(CliError::SyncRootTwoWayMetadataRefreshLocalStateChanged);
    }
    if storage.sync_root_change_window_state(&root.id)?.is_some() {
        return Err(CliError::SyncRootTwoWayMetadataRefreshWindowNotCleared);
    }

    let remote_after = storage.sync_root_remote_inventory_state(&root.id)?;
    if !remote_after.ready_for_reconciliation() {
        return Err(CliError::SyncRootTwoWayMetadataRefreshRemoteStateNotReady);
    }
    let cursor_after = storage
        .sync_root_change_cursor(&root.id)?
        .ok_or(CliError::SyncRootTwoWayMetadataRefreshRemoteStateNotReady)?;

    println!("SYNC_ROOT_TWO_WAY_METADATA_REFRESH=PASS");
    println!("MODE=two_way");
    println!("PAGES_COLLECTED={pages}");
    println!("PROVIDER_CHANGES={}", execution.provider_changes);
    println!("CATALOG_MUTATIONS={}", execution.storage_mutations);
    println!("AUTHORITATIVE_ITEMS={}", execution.authoritative_items);
    println!("CURSOR_ADVANCED={}", yes_no(cursor_after != cursor_before));
    println!("LOCAL_BASELINE_UNCHANGED=yes");
    println!("LOCAL_PENDING_EVENTS_PRESERVED=yes");
    println!("LOCAL_PENDING_EVENTS={pending_after}");
    println!("NETWORK_CHECK=performed");
    println!("DATABASE_MUTATION=yes");
    println!("FILESYSTEM_READ=not_performed");
    println!("FILESYSTEM_MUTATION=no");
    println!("FILE_CONTENT_ACCESSED=no");
    println!("PROVIDER_WRITE_METHOD_CALLED=no");
    println!("REMOTE_OBJECT_MUTATION=no");
    println!("ROOT_PATH_PRINTED=no");
    println!("LOCAL_NAMES_PRINTED=no");
    println!("REMOTE_IDS_PRINTED=no");
    println!("CURSOR_VALUES_PRINTED=no");
    println!("TOKEN_VALUES_PRINTED=no");
    println!("DRIVE_WRITE_ACCESS=readonly_metadata_refresh");
    Ok(())
}

fn sync_roots_observe_write_authority() -> Result<(), CliError> {
    const MAX_OBSERVED_ITEMS: usize = 10_000;

    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let mut storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;

    if roots.len() != 1 {
        return Err(CliError::SyncRootWriteAuthoritySelectionFailed);
    }

    let root = roots
        .into_iter()
        .next()
        .ok_or(CliError::SyncRootWriteAuthoritySelectionFailed)?;
    let remote_root_id = root
        .remote_root_id
        .as_deref()
        .ok_or(CliError::SyncRootInventoryRemoteRootMissing)?;

    let state = storage.sync_root_remote_inventory_state(&root.id)?;
    let durable_cursor = storage
        .sync_root_change_cursor(&root.id)?
        .ok_or(CliError::SyncRootWriteAuthorityCatalogNotReady)?;

    if !state.ready_for_reconciliation()
        || storage.sync_root_change_window_state(&root.id)?.is_some()
    {
        return Err(CliError::SyncRootWriteAuthorityCatalogNotReady);
    }

    let items = storage.list_sync_root_remote_items(&root.id)?;
    let expected_item_count =
        usize::try_from(state.item_count).map_err(|_| CliError::NumericOverflow)?;
    if items.len() != expected_item_count {
        return Err(CliError::SyncRootWriteAuthorityCatalogNotReady);
    }

    let observation_count = items
        .len()
        .checked_add(1)
        .ok_or(CliError::NumericOverflow)?;
    if observation_count > MAX_OBSERVED_ITEMS {
        return Err(CliError::SyncRootWriteAuthorityObservationLimitExceeded);
    }

    ensure_keyring_available()?;
    let keyring = KeyringSecretStore::default();
    let refresh_key = refresh_token_key(&account.subject)?;
    let refresh_token = required_secret_utf8(
        keyring.get(&refresh_key)?,
        CliError::MissingStoredRefreshToken,
    )?;
    let (client_id, client_secret) = load_google_client_config(&keyring)?;

    println!("SYNC_ROOT_WRITE_AUTHORITY_STAGE=refresh_access_token");
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

    println!("SYNC_ROOT_WRITE_AUTHORITY_STAGE=verify_account");
    let api = GoogleDriveApi::new(tokens.access_token().clone())?;
    let user = api.user_info()?;
    if user.sub != account.subject {
        return Err(CliError::GoogleAccountMismatch);
    }

    println!("SYNC_ROOT_WRITE_AUTHORITY_STAGE=establish_remote_fence");
    let before_cursor = api.current_change_cursor()?;
    if before_cursor != durable_cursor {
        return Err(CliError::SyncRootWriteAuthorityCatalogNotCurrent);
    }

    let observed_at_unix_ms = unix_time_ms()?;
    let mut authorities = Vec::with_capacity(observation_count);

    println!("SYNC_ROOT_WRITE_AUTHORITY_STAGE=observe_metadata");
    let root_observation = api.observe_write_authority(remote_root_id)?;
    authorities.push(RemoteWriteAuthoritySnapshot::new(
        root_observation.remote_id(),
        root_observation.remote_version,
        root_observation.md5_checksum().map(|_| "md5".to_owned()),
        root_observation.md5_checksum().map(str::to_owned),
        root_observation.can_edit,
        root_observation.can_trash,
        root_observation.can_add_children,
        observed_at_unix_ms,
    )?);

    let mut seen = HashSet::from([remote_root_id.to_owned()]);
    for item in &items {
        if !seen.insert(item.remote_id.clone()) {
            return Err(CliError::SyncRootWriteAuthorityDuplicateRemoteId);
        }

        let observation = api.observe_write_authority(&item.remote_id)?;
        authorities.push(RemoteWriteAuthoritySnapshot::new(
            observation.remote_id(),
            observation.remote_version,
            observation.md5_checksum().map(|_| "md5".to_owned()),
            observation.md5_checksum().map(str::to_owned),
            observation.can_edit,
            observation.can_trash,
            observation.can_add_children,
            observed_at_unix_ms,
        )?);
    }

    let after_cursor = api.current_change_cursor()?;
    if after_cursor != before_cursor {
        return Err(CliError::SyncRootWriteAuthorityRemoteChangedDuringObservation);
    }

    if storage.sync_root_change_cursor(&root.id)?.as_ref() != Some(&durable_cursor)
        || storage.sync_root_change_window_state(&root.id)?.is_some()
    {
        return Err(CliError::SyncRootWriteAuthorityCatalogNotReady);
    }

    println!("SYNC_ROOT_WRITE_AUTHORITY_STAGE=commit_snapshot");
    let persisted = storage.commit_sync_root_remote_write_authority_snapshot(
        &root.id,
        &durable_cursor,
        &authorities,
        observed_at_unix_ms,
    )?;
    let durable_count = storage.sync_root_remote_write_authority_count(&root.id)?;
    let authority_state = storage
        .sync_root_remote_write_authority_state(&root.id)?
        .ok_or(CliError::SyncRootWriteAuthorityPersistenceMismatch)?;

    if persisted != authorities.len()
        || durable_count
            != u64::try_from(authorities.len()).map_err(|_| CliError::NumericOverflow)?
        || authority_state.change_cursor != durable_cursor
        || authority_state.item_count != durable_count
    {
        return Err(CliError::SyncRootWriteAuthorityPersistenceMismatch);
    }

    let checksummed = authorities
        .iter()
        .filter(|authority| authority.content_checksum().is_some())
        .count();
    let editable = authorities
        .iter()
        .filter(|authority| authority.can_edit)
        .count();
    let trashable = authorities
        .iter()
        .filter(|authority| authority.can_trash)
        .count();
    let add_children = authorities
        .iter()
        .filter(|authority| authority.can_add_children)
        .count();

    println!("SYNC_ROOT_WRITE_AUTHORITY=PASS");
    println!("MODE={}", root.mode.as_str());
    println!("CATALOG_ITEMS={}", items.len());
    println!("ROOT_AUTHORITY_OBSERVED=yes");
    println!("AUTHORITIES_PERSISTED={persisted}");
    println!("MD5_PRESENT={checksummed}");
    println!("CAN_EDIT={editable}");
    println!("CAN_TRASH={trashable}");
    println!("CAN_ADD_CHILDREN={add_children}");
    println!("REMOTE_CURSOR_STABLE=yes");
    println!("CATALOG_CURSOR_MATCH=yes");
    println!("AUTHORITY_CURSOR_BOUND=yes");
    println!("NETWORK_CHECK=performed");
    println!("DATABASE_MUTATION=yes");
    println!("FILESYSTEM_MUTATION=no");
    println!("FILE_CONTENT_ACCESSED=no");
    println!("ROOT_PATH_PRINTED=no");
    println!("LOCAL_NAMES_PRINTED=no");
    println!("REMOTE_IDS_PRINTED=no");
    println!("REMOTE_METADATA_PRINTED=no");
    println!("HASH_VALUES_PRINTED=no");
    println!("TOKEN_VALUES_PRINTED=no");
    println!("OAUTH_FULLSYNC_ACTIVATED=no");
    println!("DRIVE_WRITE_ACCESS=no");

    Ok(())
}

fn sync_roots_remote_write_plan() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;
    if roots.len() != 1 {
        return Err(CliError::SyncRootRemoteWritePlanSelectionFailed);
    }

    let root = roots
        .into_iter()
        .next()
        .ok_or(CliError::SyncRootRemoteWritePlanSelectionFailed)?;

    ensure_keyring_available()?;
    let keyring = KeyringSecretStore::default();
    let full_sync_credential_present = keyring
        .get(&fullsync_refresh_token_key(&account.subject)?)?
        .is_some();

    println!("SYNC_ROOT_REMOTE_WRITE_PLAN_STAGE=scan_and_validate");
    let plan =
        plan_selected_root_remote_write_intents(&storage, &root, full_sync_credential_present)?;

    println!("SYNC_ROOT_REMOTE_WRITE_PLAN=PASS");
    println!("MODE={}", root.mode.as_str());
    println!("BASELINE_GENERATION={}", plan.baseline_generation);
    println!("PENDING_EVENTS={}", plan.pending_events);
    println!("CREATE_FILE_NEEDS_ID={}", plan.create_file_needs_id);
    println!("CREATE_FOLDER_NEEDS_ID={}", plan.create_folder_needs_id);
    println!("UPDATE_FILE_READY={}", plan.update_file_ready);
    println!("TRASH_ITEM_READY={}", plan.trash_item_ready);
    println!("CONFLICTS={}", plan.conflicts);
    println!("BLOCKED_IDENTITY={}", plan.blocked_identity);
    println!("BLOCKED_AUTHORITY={}", plan.blocked_authority);
    println!("ROOT_WRITE_CAPABLE={}", yes_no(plan.root_write_capable));
    println!(
        "FULLSYNC_CREDENTIAL_PRESENT={}",
        yes_no(plan.full_sync_credential_present)
    );
    println!(
        "WRITE_GATES_SATISFIED={}",
        yes_no(plan.write_gates_satisfied())
    );
    println!(
        "PERSISTABLE_EXISTING_INTENTS={}",
        plan.persistable_existing_intents()
    );
    println!("PREDETERMINED_CREATE_IDS_ALLOCATED=no");
    println!("INTENTS_PERSISTED=0");
    println!("NETWORK_CHECK=not_performed");
    println!("DATABASE_MUTATION=no");
    println!("FILESYSTEM_READ=metadata_only");
    println!("FILESYSTEM_MUTATION=no");
    println!("FILE_CONTENT_ACCESSED=no");
    println!("ROOT_PATH_PRINTED=no");
    println!("LOCAL_NAMES_PRINTED=no");
    println!("REMOTE_IDS_PRINTED=no");
    println!("REMOTE_METADATA_PRINTED=no");
    println!("HASH_VALUES_PRINTED=no");
    println!("TOKEN_VALUES_PRINTED=no");
    println!("OAUTH_FULLSYNC_ACTIVATED=no");
    println!("DRIVE_WRITE_ACCESS=no");

    Ok(())
}

fn sync_roots_allocate_create_ids() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let mut storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;
    if roots.len() != 1 {
        return Err(CliError::SyncRootCreateIdAllocationSelectionFailed);
    }

    let root = roots
        .into_iter()
        .next()
        .ok_or(CliError::SyncRootCreateIdAllocationSelectionFailed)?;
    if root.mode != SyncMode::TwoWay {
        return Err(CliError::SyncRootCreateIdAllocationModeUnsupported);
    }

    ensure_keyring_available()?;
    let keyring = KeyringSecretStore::default();
    let fullsync_key = fullsync_refresh_token_key(&account.subject)?;
    let fullsync_present = keyring.get(&fullsync_key)?.is_some();
    if !fullsync_present {
        return Err(CliError::MissingStoredFullSyncRefreshToken);
    }

    println!("SYNC_ROOT_CREATE_ID_ALLOCATION_STAGE=journal_local_metadata");
    let journal =
        journal_selected_root_two_way_local_inventory_diff(&mut storage, &root, unix_time_ms()?)?;

    println!("SYNC_ROOT_CREATE_ID_ALLOCATION_STAGE=plan");
    let plan = plan_selected_root_remote_write_intents(&storage, &root, true)?;
    if !plan.write_gates_satisfied() {
        return Err(CliError::SyncRootCreateIdAllocationWriteGatesNotSatisfied);
    }
    if plan.conflicts != 0 || plan.blocked_identity != 0 || plan.blocked_authority != 0 {
        return Err(CliError::SyncRootCreateIdAllocationPlanBlocked);
    }

    let mut eligible_source_ids = Vec::new();
    let mut existing_intents = 0usize;

    for entry in plan.entries() {
        if entry.disposition != SelectedRootRemoteWritePlanDisposition::NeedsPredeterminedRemoteId {
            continue;
        }

        if storage
            .sync_root_remote_write_intent_for_source_event(&root.id, entry.source_local_event_id)?
            .is_some()
        {
            existing_intents = existing_intents
                .checked_add(1)
                .ok_or(CliError::NumericOverflow)?;
            continue;
        }

        eligible_source_ids.push(entry.source_local_event_id);
    }

    eligible_source_ids.truncate(REMOTE_WRITE_INTENT_BATCH_MAX);

    if eligible_source_ids.is_empty() {
        println!("SYNC_ROOT_CREATE_ID_ALLOCATION=PASS");
        println!("MODE=two_way");
        println!("LOCAL_OBSERVATION=journaled");
        println!("PENDING_EVENTS={}", journal.pending_events);
        println!("ELIGIBLE_CREATE_CANDIDATES=0");
        println!("EXISTING_CREATE_INTENTS={existing_intents}");
        println!("IDS_REQUESTED=0");
        println!("IDS_RETURNED=0");
        println!("INTENTS_PERSISTED=0");
        println!("GENERATE_IDS_CALL=not_performed");
        println!("NETWORK_CHECK=not_performed");
        println!("DATABASE_MUTATION=yes");
        println!("FILESYSTEM_READ=metadata_only");
        println!("FILESYSTEM_MUTATION=no");
        println!("FILE_CONTENT_ACCESSED=no");
        println!("LOCAL_EVENT_APPLIED=no");
        println!("BASELINE_ADVANCED=no");
        println!("REMOTE_IDS_PRINTED=no");
        println!("TOKEN_VALUES_PRINTED=no");
        println!("REMOTE_OBJECT_MUTATION=no");
        println!("DRIVE_WRITE_ACCESS=no_remote_call");
        println!("DRIVE_WRITE_EXECUTION_ENABLED=no");
        return Ok(());
    }

    let fullsync_refresh_token = required_secret_utf8(
        keyring.get(&fullsync_key)?,
        CliError::MissingStoredFullSyncRefreshToken,
    )?;
    let (client_id, client_secret) = load_google_client_config(&keyring)?;
    let oauth = GoogleOAuthConfig::new(client_id)?;

    println!("SYNC_ROOT_CREATE_ID_ALLOCATION_STAGE=refresh_fullsync_access_token");
    let tokens = oauth.refresh_access_token(&fullsync_refresh_token, &client_secret)?;
    if let Some(scope) = tokens.scope()
        && !oauth_scope_contains(Some(scope), GOOGLE_DRIVE_FULL_SCOPE)
    {
        return Err(CliError::GoogleFullSyncScopeNotGranted);
    }

    let api = GoogleDriveApi::new(tokens.access_token().clone())?;
    let user = api.user_info()?;
    if user.sub != account.subject {
        return Err(CliError::GoogleAccountMismatch);
    }

    if let Some(rotated_refresh_token) = tokens.refresh_token() {
        keyring.put(
            &fullsync_key,
            SecretValue::new(rotated_refresh_token.as_bytes().to_vec())?,
        )?;
    }

    let durable_cursor = storage
        .sync_root_change_cursor(&root.id)?
        .ok_or(CliError::SyncRootCreateIdAllocationRemoteFenceMissing)?;
    let authority_state = storage
        .sync_root_remote_write_authority_state(&root.id)?
        .ok_or(CliError::SyncRootCreateIdAllocationRemoteFenceMissing)?;
    if authority_state.change_cursor != durable_cursor {
        return Err(CliError::SyncRootCreateIdAllocationRemoteFenceMismatch);
    }

    println!("SYNC_ROOT_CREATE_ID_ALLOCATION_STAGE=establish_remote_fence");
    let before_cursor = api.current_change_cursor()?;
    if before_cursor != durable_cursor {
        return Err(CliError::SyncRootCreateIdAllocationRemoteFenceMismatch);
    }

    let requested =
        u16::try_from(eligible_source_ids.len()).map_err(|_| CliError::NumericOverflow)?;

    println!("SYNC_ROOT_CREATE_ID_ALLOCATION_STAGE=generate_ids");
    let generated = api.generate_file_ids(requested)?;
    let returned = generated.len();
    if returned != eligible_source_ids.len() {
        return Err(CliError::SyncRootCreateIdAllocationCountMismatch);
    }

    let after_cursor = api.current_change_cursor()?;
    if after_cursor != before_cursor {
        return Err(CliError::SyncRootCreateIdAllocationRemoteChanged);
    }

    println!("SYNC_ROOT_CREATE_ID_ALLOCATION_STAGE=revalidate_local_plan");
    let journal_after =
        journal_selected_root_two_way_local_inventory_diff(&mut storage, &root, unix_time_ms()?)?;
    let plan_after = plan_selected_root_remote_write_intents(&storage, &root, true)?;

    if plan_after.conflicts != 0
        || plan_after.blocked_identity != 0
        || plan_after.blocked_authority != 0
    {
        return Err(CliError::SyncRootCreateIdAllocationPlanChanged);
    }

    if storage.sync_root_change_cursor(&root.id)?.as_ref() != Some(&durable_cursor) {
        return Err(CliError::SyncRootCreateIdAllocationRemoteFenceMismatch);
    }

    let generated_ids = generated.into_ids();
    let mut inputs = Vec::with_capacity(generated_ids.len());
    let planned_at_unix_ms = unix_time_ms()?;

    for (source_id, generated_id) in eligible_source_ids.iter().zip(generated_ids) {
        if storage
            .sync_root_remote_write_intent_for_source_event(&root.id, *source_id)?
            .is_some()
        {
            return Err(CliError::SyncRootCreateIdAllocationPlanChanged);
        }

        let entry = plan_after
            .entries()
            .iter()
            .find(|entry| {
                entry.source_local_event_id == *source_id
                    && entry.disposition
                        == SelectedRootRemoteWritePlanDisposition::NeedsPredeterminedRemoteId
            })
            .ok_or(CliError::SyncRootCreateIdAllocationPlanChanged)?;

        inputs.push(entry.create_intent_input(
            plan_after.baseline_generation,
            generated_id,
            planned_at_unix_ms,
        )?);
    }

    println!("SYNC_ROOT_CREATE_ID_ALLOCATION_STAGE=persist_intents");
    let intent_ids = storage.create_sync_root_remote_write_intents_batch(&root.id, &inputs)?;

    if intent_ids.len() != inputs.len() {
        return Err(CliError::SyncRootCreateIdAllocationPersistenceMismatch);
    }

    for input in &inputs {
        let record = storage
            .sync_root_remote_write_intent_for_source_event(&root.id, input.source_local_event_id)?
            .ok_or(CliError::SyncRootCreateIdAllocationPersistenceMismatch)?;
        if record.status.as_str() != "planned" {
            return Err(CliError::SyncRootCreateIdAllocationPersistenceMismatch);
        }
    }

    println!("SYNC_ROOT_CREATE_ID_ALLOCATION=PASS");
    println!("MODE=two_way");
    println!("LOCAL_OBSERVATION=journaled");
    println!("PENDING_EVENTS={}", journal_after.pending_events);
    println!("ELIGIBLE_CREATE_CANDIDATES={}", eligible_source_ids.len());
    println!("EXISTING_CREATE_INTENTS={existing_intents}");
    println!("IDS_REQUESTED={requested}");
    println!("IDS_RETURNED={returned}");
    println!("INTENTS_PERSISTED={}", intent_ids.len());
    println!("GENERATE_IDS_CALL=performed");
    println!("GENERATE_IDS_SPACE=drive");
    println!("GENERATE_IDS_TYPE=files");
    println!("REMOTE_CURSOR_STABLE=yes");
    println!("AUTHORITY_CURSOR_MATCH=yes");
    println!("NETWORK_CHECK=performed");
    println!("DATABASE_MUTATION=yes");
    println!("FILESYSTEM_READ=metadata_only");
    println!("FILESYSTEM_MUTATION=no");
    println!("FILE_CONTENT_ACCESSED=no");
    println!("LOCAL_EVENT_APPLIED=no");
    println!("BASELINE_ADVANCED=no");
    println!("REMOTE_IDS_PRINTED=no");
    println!("TOKEN_VALUES_PRINTED=no");
    println!("REMOTE_OBJECT_MUTATION=no");
    println!("DRIVE_WRITE_ACCESS=generate_ids_only");
    println!("DRIVE_WRITE_EXECUTION_ENABLED=no");

    Ok(())
}

fn sync_roots_submit_folder_create() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let mut storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;
    if roots.len() != 1 {
        return Err(CliError::SyncRootFolderCreateSubmissionSelectionFailed);
    }

    let root = roots
        .into_iter()
        .next()
        .ok_or(CliError::SyncRootFolderCreateSubmissionSelectionFailed)?;
    if root.mode != SyncMode::TwoWay {
        return Err(CliError::SyncRootFolderCreateSubmissionModeUnsupported);
    }

    let candidates = storage
        .list_sync_root_folder_create_candidates(&root.id, RemoteWriteIntentStatus::Planned)?;

    if candidates.is_empty() {
        println!("SYNC_ROOT_FOLDER_CREATE_SUBMISSION=PASS");
        println!("MODE=two_way");
        println!("PLANNED_CREATE_FOLDER_INTENTS=0");
        println!("SELECTED_INTENTS=0");
        println!("SUBMISSION_ATTEMPTED=no");
        println!("PROVIDER_POST=not_performed");
        println!("INTENT_STATUS_AFTER=none");
        println!("NETWORK_CHECK=not_performed");
        println!("DATABASE_MUTATION=no");
        println!("FILESYSTEM_READ=not_performed");
        println!("FILESYSTEM_MUTATION=no");
        println!("FILE_CONTENT_ACCESSED=no");
        println!("REMOTE_OBJECT_MUTATION=none");
        println!("CONFIRMED=no");
        println!("LOCAL_EVENT_APPLIED=no");
        println!("BASELINE_ADVANCED=no");
        println!("LOCAL_NAMES_PRINTED=no");
        println!("REMOTE_IDS_PRINTED=no");
        println!("CURSOR_VALUES_PRINTED=no");
        println!("TOKEN_VALUES_PRINTED=no");
        println!("DRIVE_WRITE_ACCESS=no_remote_call");
        return Ok(());
    }

    let candidate = candidates
        .first()
        .cloned()
        .ok_or(CliError::SyncRootFolderCreateSubmissionSelectionFailed)?;

    println!("SYNC_ROOT_FOLDER_CREATE_SUBMISSION_STAGE=revalidate_local_identity");
    let local = validate_selected_root_folder_create_local_identity(
        &root,
        candidate.relative_path(),
        candidate.local_modified_unix_ns,
        candidate.local_device_id,
        candidate.local_inode,
    )?;

    ensure_keyring_available()?;
    let keyring = KeyringSecretStore::default();
    let fullsync_key = fullsync_refresh_token_key(&account.subject)?;
    let fullsync_refresh_token = required_secret_utf8(
        keyring.get(&fullsync_key)?,
        CliError::MissingStoredFullSyncRefreshToken,
    )?;
    let (client_id, client_secret) = load_google_client_config(&keyring)?;
    let oauth = GoogleOAuthConfig::new(client_id)?;

    println!("SYNC_ROOT_FOLDER_CREATE_SUBMISSION_STAGE=refresh_fullsync_access_token");
    let tokens = oauth.refresh_access_token(&fullsync_refresh_token, &client_secret)?;
    if let Some(scope) = tokens.scope()
        && !oauth_scope_contains(Some(scope), GOOGLE_DRIVE_FULL_SCOPE)
    {
        return Err(CliError::GoogleFullSyncScopeNotGranted);
    }

    let api = GoogleDriveApi::new(tokens.access_token().clone())?;
    let user = api.user_info()?;
    if user.sub != account.subject {
        return Err(CliError::GoogleAccountMismatch);
    }

    if let Some(rotated_refresh_token) = tokens.refresh_token() {
        keyring.put(
            &fullsync_key,
            SecretValue::new(rotated_refresh_token.as_bytes().to_vec())?,
        )?;
    }

    let durable_cursor = storage
        .sync_root_change_cursor(&root.id)?
        .ok_or(CliError::SyncRootFolderCreateRemoteFenceMismatch)?;
    let authority_state = storage
        .sync_root_remote_write_authority_state(&root.id)?
        .ok_or(CliError::SyncRootFolderCreateRemoteFenceMismatch)?;
    if authority_state.change_cursor != durable_cursor {
        return Err(CliError::SyncRootFolderCreateRemoteFenceMismatch);
    }

    let parent_remote_id = candidate.expected_parent_remote_id();
    let durable_parent_authority = storage
        .sync_root_remote_write_authority(&root.id, parent_remote_id)?
        .ok_or(CliError::SyncRootFolderCreateParentAuthorityMismatch)?;
    if !durable_parent_authority.can_add_children {
        return Err(CliError::SyncRootFolderCreateParentAuthorityMismatch);
    }

    println!("SYNC_ROOT_FOLDER_CREATE_SUBMISSION_STAGE=verify_fresh_parent");
    let fresh_parent = api.observe_write_authority(parent_remote_id)?;
    if fresh_parent.kind != RemoteItemKind::Folder
        || !fresh_parent.can_add_children
        || fresh_parent.remote_version != durable_parent_authority.remote_version
    {
        return Err(CliError::SyncRootFolderCreateParentAuthorityMismatch);
    }

    let root_remote_id = root
        .remote_root_id
        .as_deref()
        .ok_or(CliError::SyncRootFolderCreateRemoteRootMissing)?;
    let drive_root = api.resolve_folder_root(root_remote_id)?;

    if parent_remote_id != root_remote_id {
        let parent_item = storage
            .sync_root_remote_item(&root.id, parent_remote_id)?
            .ok_or(CliError::SyncRootFolderCreateParentTopologyMismatch)?;
        if parent_item.kind != RemoteItemKind::Folder || parent_item.trashed {
            return Err(CliError::SyncRootFolderCreateParentTopologyMismatch);
        }
        if api.resolve_item_membership(&parent_item, &drive_root)?
            != DriveRootMembership::Descendant
        {
            return Err(CliError::SyncRootFolderCreateParentTopologyMismatch);
        }
    }

    println!("SYNC_ROOT_FOLDER_CREATE_SUBMISSION_STAGE=establish_pre_submit_fence");
    let provider_cursor = api.current_change_cursor()?;
    if provider_cursor != durable_cursor {
        return Err(CliError::SyncRootFolderCreateRemoteFenceMismatch);
    }

    println!("SYNC_ROOT_FOLDER_CREATE_SUBMISSION_STAGE=commit_submitted");
    let submitted = storage.begin_sync_root_folder_create_submission(
        &root.id,
        candidate.intent_id,
        candidate.execution_generation,
        &provider_cursor,
        unix_time_ms()?,
    )?;

    println!("SYNC_ROOT_FOLDER_CREATE_SUBMISSION_STAGE=provider_create");
    let create_result = api.create_folder_with_predetermined_id(
        candidate.predetermined_remote_id(),
        local.leaf_name(),
        parent_remote_id,
    )?;

    let (status_after, remote_mutation, recovery_path) = match create_result {
        DriveFolderCreateSubmission::Created => {
            let next = storage.transition_sync_root_folder_create_intent(
                candidate.intent_id,
                RemoteWriteIntentStatus::Submitted,
                submitted.execution_generation,
                RemoteWriteIntentStatus::AwaitingConfirmation,
                unix_time_ms()?,
            )?;
            (next.status.as_str(), "created", "not_needed")
        }
        DriveFolderCreateSubmission::Conflict => {
            println!("SYNC_ROOT_FOLDER_CREATE_SUBMISSION_STAGE=verify_409_predetermined_id");
            match api.inspect_expected_folder(
                candidate.predetermined_remote_id(),
                local.leaf_name(),
                parent_remote_id,
            )? {
                DriveExpectedFolderLookup::Exact => {
                    let next = storage.transition_sync_root_folder_create_intent(
                        candidate.intent_id,
                        RemoteWriteIntentStatus::Submitted,
                        submitted.execution_generation,
                        RemoteWriteIntentStatus::AwaitingConfirmation,
                        unix_time_ms()?,
                    )?;
                    (
                        next.status.as_str(),
                        "previously_created_or_existing_exact",
                        "exact_id_match",
                    )
                }
                DriveExpectedFolderLookup::Mismatch => {
                    let next = storage.transition_sync_root_folder_create_intent(
                        candidate.intent_id,
                        RemoteWriteIntentStatus::Submitted,
                        submitted.execution_generation,
                        RemoteWriteIntentStatus::Conflict,
                        unix_time_ms()?,
                    )?;
                    (next.status.as_str(), "none_conflict", "exact_id_mismatch")
                }
                DriveExpectedFolderLookup::Missing => (
                    RemoteWriteIntentStatus::Submitted.as_str(),
                    "ambiguous",
                    "exact_id_missing_no_retry",
                ),
            }
        }
    };

    println!("SYNC_ROOT_FOLDER_CREATE_SUBMISSION=PASS");
    println!("MODE=two_way");
    println!("PLANNED_CREATE_FOLDER_INTENTS={}", candidates.len());
    println!("SELECTED_INTENTS=1");
    println!("SUBMISSION_ATTEMPTED=yes");
    println!("LOCAL_IDENTITY_VERIFIED=yes");
    println!("FULLSYNC_CREDENTIAL_VERIFIED=yes");
    println!("ACCOUNT_SUBJECT_MATCH=yes");
    println!("PARENT_AUTHORITY_VERIFIED=yes");
    println!("PARENT_TOPOLOGY_VERIFIED=yes");
    println!("PRE_SUBMIT_CURSOR_MATCH=yes");
    println!("DURABLE_SUBMITTED_BEFORE_POST=yes");
    println!("PROVIDER_POST=performed");
    println!("HTTP_409_RECOVERY={recovery_path}");
    println!("INTENT_STATUS_AFTER={status_after}");
    println!("NETWORK_CHECK=performed");
    println!("DATABASE_MUTATION=yes");
    println!("FILESYSTEM_READ=metadata_only");
    println!("FILESYSTEM_MUTATION=no");
    println!("FILE_CONTENT_ACCESSED=no");
    println!("REMOTE_OBJECT_MUTATION={remote_mutation}");
    println!("CONFIRMED=no");
    println!("CHANGE_STREAM_CONFIRMATION_REQUIRED=yes");
    println!("LOCAL_EVENT_APPLIED=no");
    println!("BASELINE_ADVANCED=no");
    println!("LOCAL_NAMES_PRINTED=no");
    println!("REMOTE_IDS_PRINTED=no");
    println!("CURSOR_VALUES_PRINTED=no");
    println!("TOKEN_VALUES_PRINTED=no");
    println!("DRIVE_WRITE_ACCESS=folder_create_only");

    Ok(())
}

fn sync_roots_recover_folder_create_submission() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;
    if roots.len() != 1 {
        return Err(CliError::SyncRootFolderCreateSubmissionSelectionFailed);
    }
    let root = roots
        .into_iter()
        .next()
        .ok_or(CliError::SyncRootFolderCreateSubmissionSelectionFailed)?;
    if root.mode != SyncMode::TwoWay {
        return Err(CliError::SyncRootFolderCreateSubmissionModeUnsupported);
    }

    let candidates = storage
        .list_sync_root_folder_create_candidates(&root.id, RemoteWriteIntentStatus::Submitted)?;

    if candidates.is_empty() {
        println!("SYNC_ROOT_FOLDER_CREATE_RECOVERY=PASS");
        println!("MODE=two_way");
        println!("SUBMITTED_CREATE_FOLDER_INTENTS=0");
        println!("SELECTED_INTENTS=0");
        println!("RECOVERY_OUTCOME=none");
        println!("PROVIDER_POST=not_performed");
        println!("NETWORK_CHECK=not_performed");
        println!("DATABASE_MUTATION=no");
        println!("FILESYSTEM_READ=not_performed");
        println!("REMOTE_OBJECT_MUTATION=none");
        println!("CONFIRMED=no");
        println!("LOCAL_EVENT_APPLIED=no");
        println!("BASELINE_ADVANCED=no");
        println!("REMOTE_IDS_PRINTED=no");
        println!("TOKEN_VALUES_PRINTED=no");
        println!("DRIVE_WRITE_ACCESS=no_remote_call");
        return Ok(());
    }

    let candidate = candidates
        .first()
        .cloned()
        .ok_or(CliError::SyncRootFolderCreateSubmissionSelectionFailed)?;

    let local = validate_selected_root_folder_create_local_identity(
        &root,
        candidate.relative_path(),
        candidate.local_modified_unix_ns,
        candidate.local_device_id,
        candidate.local_inode,
    )?;

    ensure_keyring_available()?;
    let keyring = KeyringSecretStore::default();
    let fullsync_key = fullsync_refresh_token_key(&account.subject)?;
    let fullsync_refresh_token = required_secret_utf8(
        keyring.get(&fullsync_key)?,
        CliError::MissingStoredFullSyncRefreshToken,
    )?;
    let (client_id, client_secret) = load_google_client_config(&keyring)?;
    let oauth = GoogleOAuthConfig::new(client_id)?;
    let tokens = oauth.refresh_access_token(&fullsync_refresh_token, &client_secret)?;
    if let Some(scope) = tokens.scope()
        && !oauth_scope_contains(Some(scope), GOOGLE_DRIVE_FULL_SCOPE)
    {
        return Err(CliError::GoogleFullSyncScopeNotGranted);
    }

    let api = GoogleDriveApi::new(tokens.access_token().clone())?;
    let user = api.user_info()?;
    if user.sub != account.subject {
        return Err(CliError::GoogleAccountMismatch);
    }

    if let Some(rotated_refresh_token) = tokens.refresh_token() {
        keyring.put(
            &fullsync_key,
            SecretValue::new(rotated_refresh_token.as_bytes().to_vec())?,
        )?;
    }

    let lookup = api.inspect_expected_folder(
        candidate.predetermined_remote_id(),
        local.leaf_name(),
        candidate.expected_parent_remote_id(),
    )?;

    let (status_after, recovery_outcome, database_mutation) = match lookup {
        DriveExpectedFolderLookup::Exact => {
            let next = storage.transition_sync_root_folder_create_intent(
                candidate.intent_id,
                RemoteWriteIntentStatus::Submitted,
                candidate.execution_generation,
                RemoteWriteIntentStatus::AwaitingConfirmation,
                unix_time_ms()?,
            )?;
            (next.status.as_str(), "exact_id_match", "yes")
        }
        DriveExpectedFolderLookup::Mismatch => {
            let next = storage.transition_sync_root_folder_create_intent(
                candidate.intent_id,
                RemoteWriteIntentStatus::Submitted,
                candidate.execution_generation,
                RemoteWriteIntentStatus::Conflict,
                unix_time_ms()?,
            )?;
            (next.status.as_str(), "exact_id_mismatch", "yes")
        }
        DriveExpectedFolderLookup::Missing => (
            RemoteWriteIntentStatus::Submitted.as_str(),
            "exact_id_missing_no_retry",
            "no",
        ),
    };

    println!("SYNC_ROOT_FOLDER_CREATE_RECOVERY=PASS");
    println!("MODE=two_way");
    println!("SUBMITTED_CREATE_FOLDER_INTENTS={}", candidates.len());
    println!("SELECTED_INTENTS=1");
    println!("RECOVERY_OUTCOME={recovery_outcome}");
    println!("INTENT_STATUS_AFTER={status_after}");
    println!("PROVIDER_POST=not_performed");
    println!("NETWORK_CHECK=performed");
    println!("DATABASE_MUTATION={database_mutation}");
    println!("FILESYSTEM_READ=metadata_only");
    println!("FILESYSTEM_MUTATION=no");
    println!("FILE_CONTENT_ACCESSED=no");
    println!("REMOTE_OBJECT_MUTATION=none");
    println!("CONFIRMED=no");
    println!("LOCAL_EVENT_APPLIED=no");
    println!("BASELINE_ADVANCED=no");
    println!("LOCAL_NAMES_PRINTED=no");
    println!("REMOTE_IDS_PRINTED=no");
    println!("CURSOR_VALUES_PRINTED=no");
    println!("TOKEN_VALUES_PRINTED=no");
    println!("DRIVE_WRITE_ACCESS=metadata_recovery_only");

    Ok(())
}

fn sync_roots_settle_confirmed_folder_create() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }
    let mut storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;
    if roots.len() != 1 {
        return Err(CliError::SyncRootFolderCreateSettlementSelectionFailed);
    }
    let root = roots
        .into_iter()
        .next()
        .ok_or(CliError::SyncRootFolderCreateSettlementSelectionFailed)?;
    if root.mode != SyncMode::TwoWay {
        return Err(CliError::SyncRootFolderCreateSettlementModeUnsupported);
    }

    let confirmed = storage
        .list_sync_root_folder_create_candidates(&root.id, RemoteWriteIntentStatus::Confirmed)?;
    let mut candidates = Vec::new();
    for candidate in confirmed {
        if !storage.sync_root_remote_write_settlement_exists(candidate.intent_id)? {
            candidates.push(candidate);
        }
    }

    if candidates.is_empty() {
        println!("SYNC_ROOT_CONFIRMED_FOLDER_CREATE_SETTLEMENT=PASS");
        println!("MODE=two_way");
        println!("UNSETTLED_CONFIRMED_FOLDER_INTENTS=0");
        println!("SELECTED_INTENTS=0");
        println!("LOCAL_DOUBLE_SCAN=not_performed");
        println!("SELECTIVE_BASELINE_PROMOTION=not_performed");
        println!("RESIDUAL_DIFF_REBASED=not_performed");
        println!("GENERATION_ADVANCED=no");
        println!("SOURCE_EVENT_APPLIED=no");
        println!("OWNERSHIP_RECEIPT_CREATED=no");
        println!("SETTLEMENT_EVIDENCE_RECORDED=no");
        println!("SETTLEMENT_DATABASE_MUTATION=no");
        println!("NETWORK_CHECK=not_performed");
        println!("PROVIDER_METHOD_CALLED=no");
        println!("REMOTE_OBJECT_MUTATION=no");
        println!("FILESYSTEM_READ=not_performed");
        println!("FILESYSTEM_MUTATION=no");
        println!("FILE_CONTENT_ACCESSED=no");
        println!("LOCAL_NAMES_PRINTED=no");
        println!("REMOTE_IDS_PRINTED=no");
        println!("TOKEN_VALUES_PRINTED=no");
        println!("DRIVE_WRITE_ACCESS=no_remote_call");
        return Ok(());
    }

    let candidate = candidates
        .first()
        .ok_or(CliError::SyncRootFolderCreateSettlementSelectionFailed)?;
    let settled_at_unix_ms = unix_time_ms()?;
    println!("SYNC_ROOT_CONFIRMED_FOLDER_CREATE_SETTLEMENT_STAGE=derive_selective_baseline");
    let plan = plan_selected_root_confirmed_folder_create_settlement(
        &storage,
        &root,
        candidate,
        settled_at_unix_ms,
    )?;
    println!("SYNC_ROOT_CONFIRMED_FOLDER_CREATE_SETTLEMENT_STAGE=atomic_commit");
    let result = storage.settle_confirmed_sync_root_folder_create(&root.id, &plan)?;

    let intent = storage
        .sync_root_remote_write_intent_execution_state(candidate.intent_id)?
        .ok_or(CliError::SyncRootFolderCreateSettlementPostconditionFailed)?;
    if intent.status != RemoteWriteIntentStatus::Confirmed
        || !storage.sync_root_remote_write_settlement_exists(candidate.intent_id)?
    {
        return Err(CliError::SyncRootFolderCreateSettlementPostconditionFailed);
    }

    println!("SYNC_ROOT_CONFIRMED_FOLDER_CREATE_SETTLEMENT=PASS");
    println!("MODE=two_way");
    println!("UNSETTLED_CONFIRMED_FOLDER_INTENTS={}", candidates.len());
    println!("SELECTED_INTENTS=1");
    println!("LOCAL_DOUBLE_SCAN=performed");
    println!("SOURCE_IDENTITY=device_inode");
    println!("SOURCE_MTIME_IDENTITY_REQUIRED=no");
    println!("SELECTIVE_BASELINE_PROMOTION=performed");
    println!("RESIDUAL_DIFF_REBASED=yes");
    println!("RESIDUAL_PENDING_EVENTS={}", result.residual_pending_events);
    println!(
        "OLD_PENDING_EVENTS_SUPERSEDED={}",
        result.superseded_old_events
    );
    println!("GENERATION_FROM={}", result.settled_from_generation);
    println!("GENERATION_TO={}", result.settled_to_generation);
    println!("GENERATION_ADVANCED=yes");
    println!("SOURCE_EVENT_APPLIED=yes");
    println!("OWNERSHIP_RECEIPT_CREATED=yes");
    println!("SETTLEMENT_EVIDENCE_RECORDED=yes");
    println!("REMOTE_WRITE_INTENT_STATUS=confirmed");
    println!("SETTLEMENT_DATABASE_MUTATION=yes");
    println!("NETWORK_CHECK=not_performed");
    println!("PROVIDER_METHOD_CALLED=no");
    println!("REMOTE_OBJECT_MUTATION=no");
    println!("FILESYSTEM_READ=metadata_only");
    println!("FILESYSTEM_MUTATION=no");
    println!("FILE_CONTENT_ACCESSED=no");
    println!("LOCAL_NAMES_PRINTED=no");
    println!("REMOTE_IDS_PRINTED=no");
    println!("TOKEN_VALUES_PRINTED=no");
    println!("DRIVE_WRITE_ACCESS=no_remote_call");
    Ok(())
}

fn sync_roots_confirm_folder_create() -> Result<(), CliError> {
    const MAX_CONFIRMATION_PAGES_PER_APPROVAL: usize = 64;

    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let mut storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;
    if roots.len() != 1 {
        return Err(CliError::SyncRootFolderCreateConfirmationSelectionFailed);
    }

    let root = roots
        .into_iter()
        .next()
        .ok_or(CliError::SyncRootFolderCreateConfirmationSelectionFailed)?;
    if root.mode != SyncMode::TwoWay {
        return Err(CliError::SyncRootFolderCreateConfirmationModeUnsupported);
    }

    let candidates = storage.list_sync_root_folder_create_candidates(
        &root.id,
        RemoteWriteIntentStatus::AwaitingConfirmation,
    )?;

    if candidates.is_empty() {
        println!("SYNC_ROOT_FOLDER_CREATE_CONFIRMATION=PASS");
        println!("MODE=two_way");
        println!("AWAITING_CONFIRMATION_INTENTS=0");
        println!("SELECTED_INTENTS=0");
        println!("CHANGE_STREAM_SCAN=not_performed");
        println!("TARGET_CHANGE_OBSERVED=no");
        println!("CURSOR_ADVANCED=no");
        println!("INTENT_STATUS_AFTER=none");
        println!("NETWORK_CHECK=not_performed");
        println!("DATABASE_MUTATION=no");
        println!("FILESYSTEM_READ=not_performed");
        println!("FILESYSTEM_MUTATION=no");
        println!("FILE_CONTENT_ACCESSED=no");
        println!("PROVIDER_WRITE_METHOD_CALLED=no");
        println!("REMOTE_OBJECT_MUTATION=no");
        println!("LOCAL_EVENT_APPLIED=no");
        println!("BASELINE_ADVANCED=no");
        println!("LOCAL_NAMES_PRINTED=no");
        println!("REMOTE_IDS_PRINTED=no");
        println!("REMOTE_METADATA_PRINTED=no");
        println!("CURSOR_VALUES_PRINTED=no");
        println!("TOKEN_VALUES_PRINTED=no");
        println!("DRIVE_WRITE_ACCESS=no_remote_call");
        return Ok(());
    }

    let candidate = candidates
        .first()
        .cloned()
        .ok_or(CliError::SyncRootFolderCreateConfirmationSelectionFailed)?;

    let execution_state = storage
        .sync_root_remote_write_intent_execution_state(candidate.intent_id)?
        .ok_or(CliError::SyncRootFolderCreateConfirmationStateMismatch)?;
    if execution_state.status != RemoteWriteIntentStatus::AwaitingConfirmation
        || execution_state.execution_generation != candidate.execution_generation
    {
        return Err(CliError::SyncRootFolderCreateConfirmationStateMismatch);
    }

    let pre_submit_cursor = execution_state
        .pre_submit_change_cursor()
        .cloned()
        .ok_or(CliError::SyncRootFolderCreateConfirmationFenceMissing)?;

    let local_state_before = storage.sync_root_local_inventory_state(&root.id)?;
    if !local_state_before.snapshot_complete
        || !local_state_before.observation_valid
        || local_state_before.generation != candidate.baseline_generation
    {
        return Err(CliError::SyncRootFolderCreateConfirmationLocalStateChanged);
    }

    let pending_before = storage
        .list_pending_sync_root_local_change_events(&root.id, candidate.baseline_generation)?;
    if !pending_before
        .iter()
        .any(|event| event.id == candidate.source_local_event_id)
    {
        return Err(CliError::SyncRootFolderCreateConfirmationLocalStateChanged);
    }

    match storage.sync_root_change_window_state(&root.id)? {
        Some(window) => {
            if window.base_cursor != pre_submit_cursor {
                return Err(CliError::SyncRootFolderCreateConfirmationFenceMismatch);
            }
        }
        None => {
            let durable_cursor = storage
                .sync_root_change_cursor(&root.id)?
                .ok_or(CliError::SyncRootFolderCreateConfirmationFenceMissing)?;
            if durable_cursor != pre_submit_cursor {
                return Err(CliError::SyncRootFolderCreateConfirmationFenceMismatch);
            }
        }
    }

    ensure_keyring_available()?;
    let keyring = KeyringSecretStore::default();
    let refresh_key = refresh_token_key(&account.subject)?;
    let refresh_token = required_secret_utf8(
        keyring.get(&refresh_key)?,
        CliError::MissingStoredRefreshToken,
    )?;
    let (client_id, client_secret) = load_google_client_config(&keyring)?;
    let oauth = GoogleOAuthConfig::new(client_id)?;

    println!("SYNC_ROOT_FOLDER_CREATE_CONFIRMATION_STAGE=refresh_readonly_access_token");
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

    let api = GoogleDriveApi::new(tokens.access_token().clone())?;
    let user = api.user_info()?;
    if user.sub != account.subject {
        return Err(CliError::GoogleAccountMismatch);
    }

    println!("SYNC_ROOT_FOLDER_CREATE_CONFIRMATION_STAGE=collect_change_window");
    let mut pages_collected = 0usize;
    loop {
        let result = collect_selected_root_change_window_page(&api, &mut storage, &root)?;
        pages_collected = pages_collected
            .checked_add(1)
            .ok_or(CliError::NumericOverflow)?;
        if result.complete {
            break;
        }
        if pages_collected >= MAX_CONFIRMATION_PAGES_PER_APPROVAL {
            return Err(CliError::SyncRootFolderCreateConfirmationPageLimitExceeded);
        }
    }

    let window = storage
        .sync_root_change_window_state(&root.id)?
        .ok_or(CliError::SyncRootFolderCreateConfirmationWindowMismatch)?;
    if !window.is_complete() || window.base_cursor != pre_submit_cursor {
        return Err(CliError::SyncRootFolderCreateConfirmationWindowMismatch);
    }

    let expected_name = candidate
        .relative_path()
        .rsplit_once('/')
        .map(|(_, name)| name)
        .unwrap_or(candidate.relative_path());
    if expected_name.is_empty() {
        return Err(CliError::SyncRootFolderCreateConfirmationStateMismatch);
    }

    let target_id = candidate.predetermined_remote_id();
    let expected_parent = candidate.expected_parent_remote_id();
    let changes = storage.sync_root_change_window_changes(&root.id)?;

    let mut target_changes = 0usize;
    let mut last_target_exact = None;
    for change in &changes {
        match change {
            RemoteChange::Upsert(item) if item.remote_id == target_id => {
                target_changes = target_changes
                    .checked_add(1)
                    .ok_or(CliError::NumericOverflow)?;
                last_target_exact = Some(
                    item.name == expected_name
                        && item.kind == RemoteItemKind::Folder
                        && item.parent_remote_id.as_deref() == Some(expected_parent)
                        && !item.trashed,
                );
            }
            RemoteChange::Delete { remote_id } if remote_id == target_id => {
                target_changes = target_changes
                    .checked_add(1)
                    .ok_or(CliError::NumericOverflow)?;
                last_target_exact = Some(false);
            }
            _ => {}
        }
    }

    if target_changes == 0 {
        println!("SYNC_ROOT_FOLDER_CREATE_CONFIRMATION_STAGE=discard_unobserved_window");
        if !storage.discard_sync_root_change_window(&root.id, &pre_submit_cursor)? {
            return Err(CliError::SyncRootFolderCreateConfirmationWindowMismatch);
        }

        let local_state_after = storage.sync_root_local_inventory_state(&root.id)?;
        let pending_after = storage
            .list_pending_sync_root_local_change_events(&root.id, candidate.baseline_generation)?;
        if local_state_after.generation != local_state_before.generation
            || local_state_after.item_count != local_state_before.item_count
            || local_state_after.snapshot_completed_at_unix_ms
                != local_state_before.snapshot_completed_at_unix_ms
            || local_state_after.observation_valid != local_state_before.observation_valid
            || !pending_after
                .iter()
                .any(|event| event.id == candidate.source_local_event_id)
        {
            return Err(CliError::SyncRootFolderCreateConfirmationLocalStateChanged);
        }

        println!("SYNC_ROOT_FOLDER_CREATE_CONFIRMATION=PASS");
        println!("MODE=two_way");
        println!("AWAITING_CONFIRMATION_INTENTS={}", candidates.len());
        println!("SELECTED_INTENTS=1");
        println!("CHANGE_STREAM_SCAN=performed");
        println!("WINDOW_PAGES_COLLECTED={pages_collected}");
        println!("TARGET_CHANGE_OBSERVED=no");
        println!("STAGED_WINDOW_DISCARDED=yes");
        println!("CURSOR_ADVANCED=no");
        println!("INTENT_STATUS_AFTER=awaiting_confirmation");
        println!("NETWORK_CHECK=performed");
        println!("DATABASE_MUTATION=yes");
        println!("FILESYSTEM_READ=not_performed");
        println!("FILESYSTEM_MUTATION=no");
        println!("FILE_CONTENT_ACCESSED=no");
        println!("PROVIDER_WRITE_METHOD_CALLED=no");
        println!("REMOTE_OBJECT_MUTATION=no");
        println!("LOCAL_EVENT_APPLIED=no");
        println!("BASELINE_ADVANCED=no");
        println!("LOCAL_NAMES_PRINTED=no");
        println!("REMOTE_IDS_PRINTED=no");
        println!("REMOTE_METADATA_PRINTED=no");
        println!("CURSOR_VALUES_PRINTED=no");
        println!("TOKEN_VALUES_PRINTED=no");
        println!("DRIVE_WRITE_ACCESS=readonly_confirmation_only");
        return Ok(());
    }

    println!("SYNC_ROOT_FOLDER_CREATE_CONFIRMATION_STAGE=commit_change_window");
    let execution =
        execute_completed_selected_root_change_window(&api, &mut storage, &root, unix_time_ms()?)?;

    let final_item = storage.sync_root_remote_item(&root.id, target_id)?;
    let final_exact = final_item.as_ref().is_some_and(|item| {
        item.name == expected_name
            && item.kind == RemoteItemKind::Folder
            && item.parent_remote_id.as_deref() == Some(expected_parent)
            && !item.trashed
    });

    let exact_change = last_target_exact == Some(true);
    let new_status = if exact_change && final_exact {
        RemoteWriteIntentStatus::Confirmed
    } else {
        RemoteWriteIntentStatus::Conflict
    };

    println!("SYNC_ROOT_FOLDER_CREATE_CONFIRMATION_STAGE=transition_intent");
    let transitioned = storage.transition_sync_root_folder_create_intent(
        candidate.intent_id,
        RemoteWriteIntentStatus::AwaitingConfirmation,
        candidate.execution_generation,
        new_status,
        unix_time_ms()?,
    )?;

    let local_state_after = storage.sync_root_local_inventory_state(&root.id)?;
    let pending_after = storage
        .list_pending_sync_root_local_change_events(&root.id, candidate.baseline_generation)?;

    if local_state_after.generation != local_state_before.generation
        || local_state_after.item_count != local_state_before.item_count
        || local_state_after.snapshot_completed_at_unix_ms
            != local_state_before.snapshot_completed_at_unix_ms
        || local_state_after.observation_valid != local_state_before.observation_valid
        || !pending_after
            .iter()
            .any(|event| event.id == candidate.source_local_event_id)
    {
        return Err(CliError::SyncRootFolderCreateConfirmationLocalStateChanged);
    }

    println!("SYNC_ROOT_FOLDER_CREATE_CONFIRMATION=PASS");
    println!("MODE=two_way");
    println!("AWAITING_CONFIRMATION_INTENTS={}", candidates.len());
    println!("SELECTED_INTENTS=1");
    println!("CHANGE_STREAM_SCAN=performed");
    println!("WINDOW_PAGES_COLLECTED={pages_collected}");
    println!("TARGET_CHANGE_OBSERVED=yes");
    println!("TARGET_LAST_CHANGE_EXACT={}", yes_no(exact_change));
    println!("FINAL_CATALOG_ITEM_EXACT={}", yes_no(final_exact));
    println!("CURSOR_ADVANCED=yes");
    println!("CATALOG_MUTATIONS={}", execution.storage_mutations);
    println!("INTENT_STATUS_AFTER={}", transitioned.status.as_str());
    println!(
        "CONFIRMED={}",
        yes_no(transitioned.status == RemoteWriteIntentStatus::Confirmed)
    );
    println!("NETWORK_CHECK=performed");
    println!("DATABASE_MUTATION=yes");
    println!("FILESYSTEM_READ=not_performed");
    println!("FILESYSTEM_MUTATION=no");
    println!("FILE_CONTENT_ACCESSED=no");
    println!("PROVIDER_WRITE_METHOD_CALLED=no");
    println!("REMOTE_OBJECT_MUTATION=no");
    println!("LOCAL_EVENT_APPLIED=no");
    println!("BASELINE_ADVANCED=no");
    println!("LOCAL_NAMES_PRINTED=no");
    println!("REMOTE_IDS_PRINTED=no");
    println!("REMOTE_METADATA_PRINTED=no");
    println!("CURSOR_VALUES_PRINTED=no");
    println!("TOKEN_VALUES_PRINTED=no");
    println!("DRIVE_WRITE_ACCESS=readonly_confirmation_only");

    Ok(())
}

fn sync_roots_folder_create_recovery_plan() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;
    if roots.len() != 1 {
        return Err(CliError::SyncRootFolderCreateRecoverySelectionFailed);
    }

    let root = roots
        .into_iter()
        .next()
        .ok_or(CliError::SyncRootFolderCreateRecoverySelectionFailed)?;
    if root.mode != SyncMode::TwoWay {
        return Err(CliError::SyncRootFolderCreateRecoveryModeUnsupported);
    }

    let states = storage.list_sync_root_folder_create_execution_states(&root.id)?;
    let mut planned = 0usize;
    let mut submitted = 0usize;
    let mut awaiting_confirmation = 0usize;
    let mut confirmed = 0usize;
    let mut conflict = 0usize;
    let mut failed = 0usize;
    let mut superseded = 0usize;
    let mut attempted = 0usize;

    for state in &states {
        if state.attempt_count > 0 {
            attempted = attempted.checked_add(1).ok_or(CliError::NumericOverflow)?;
        }
        match state.status {
            RemoteWriteIntentStatus::Planned => planned += 1,
            RemoteWriteIntentStatus::Submitted => submitted += 1,
            RemoteWriteIntentStatus::AwaitingConfirmation => awaiting_confirmation += 1,
            RemoteWriteIntentStatus::Confirmed => confirmed += 1,
            RemoteWriteIntentStatus::Conflict => conflict += 1,
            RemoteWriteIntentStatus::Failed => failed += 1,
            RemoteWriteIntentStatus::Superseded => superseded += 1,
        }
    }

    println!("SYNC_ROOT_FOLDER_CREATE_RECOVERY_PLAN=PASS");
    println!("MODE=two_way");
    println!("FOLDER_CREATE_INTENTS={}", states.len());
    println!("PLANNED={planned}");
    println!("SUBMITTED={submitted}");
    println!("AWAITING_CONFIRMATION={awaiting_confirmation}");
    println!("CONFIRMED={confirmed}");
    println!("CONFLICT={conflict}");
    println!("FAILED={failed}");
    println!("SUPERSEDED={superseded}");
    println!("ATTEMPTED_INTENTS={attempted}");
    println!(
        "RECOVERY_REQUIRED={}",
        yes_no(submitted > 0 || awaiting_confirmation > 0)
    );
    println!("NETWORK_CHECK=not_performed");
    println!("FILESYSTEM_READ=not_performed");
    println!("FILESYSTEM_MUTATION=no");
    println!("FILE_CONTENT_ACCESSED=no");
    println!("INTENT_STATUS_MUTATION=no");
    println!("LOCAL_EVENT_APPLIED=no");
    println!("BASELINE_ADVANCED=no");
    println!("REMOTE_IDS_PRINTED=no");
    println!("CURSOR_VALUES_PRINTED=no");
    println!("TOKEN_VALUES_PRINTED=no");
    println!("REMOTE_OBJECT_MUTATION=no");
    println!("DRIVE_WRITE_ACCESS=no");

    Ok(())
}

fn sync_roots_activate_two_way() -> Result<(), CliError> {
    ensure_keyring_available()?;

    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;
    if roots.len() != 1 {
        return Err(CliError::SyncRootTwoWayActivationSelectionFailed);
    }
    let root = roots
        .into_iter()
        .next()
        .ok_or(CliError::SyncRootTwoWayActivationSelectionFailed)?;

    if root.mode == SyncMode::TwoWay {
        println!("SYNC_ROOT_TWO_WAY_ACTIVATION=ALREADY_ACTIVE");
        println!("MODE=two_way");
        println!("DATABASE_MUTATION=no");
        println!("PROVIDER_WRITE_METHOD_CALLED=no");
        println!("REMOTE_WRITE_EXECUTION_ENABLED=no");
        println!("DRIVE_WRITE_ACCESS=credential_only");
        return Ok(());
    }
    if root.mode != SyncMode::ReceiveOnly {
        return Err(CliError::SyncRootTwoWayActivationModeUnsupported);
    }

    println!("SYNC_ROOT_TWO_WAY_ACTIVATION_STAGE=validate_receive_only_state");
    let convergence = plan_selected_root_receive_only_convergence(&storage, &root)?;
    if convergence.blocked() || convergence.action_count() != 0 {
        return Err(CliError::SyncRootTwoWayActivationReceiveOnlyNotConverged);
    }

    let local_state = storage.sync_root_local_inventory_state(&root.id)?;
    if !local_state.snapshot_complete || !local_state.observation_valid {
        return Err(CliError::SyncRootTwoWayActivationLocalStateNotReady);
    }
    if storage.pending_sync_root_local_change_event_count(&root.id, local_state.generation)? != 0 {
        return Err(CliError::SyncRootTwoWayActivationLocalJournalNotClean);
    }

    let remote_state = storage.sync_root_remote_inventory_state(&root.id)?;
    if !remote_state.ready_for_reconciliation()
        || storage.sync_root_change_window_state(&root.id)?.is_some()
    {
        return Err(CliError::SyncRootTwoWayActivationRemoteStateNotReady);
    }
    let remote_cursor = storage
        .sync_root_change_cursor(&root.id)?
        .ok_or(CliError::SyncRootTwoWayActivationRemoteStateNotReady)?;

    let authority_state = storage
        .sync_root_remote_write_authority_state(&root.id)?
        .ok_or(CliError::SyncRootTwoWayActivationAuthorityNotReady)?;
    if authority_state.change_cursor != remote_cursor {
        return Err(CliError::SyncRootTwoWayActivationAuthorityNotReady);
    }
    let expected_authority_count = remote_state
        .item_count
        .checked_add(1)
        .ok_or(CliError::NumericOverflow)?;
    if authority_state.item_count != expected_authority_count
        || storage.sync_root_remote_write_authority_count(&root.id)? != expected_authority_count
    {
        return Err(CliError::SyncRootTwoWayActivationAuthorityNotReady);
    }

    if storage.sync_root_remote_write_intent_count(&root.id)? != 0 {
        return Err(CliError::SyncRootTwoWayActivationExistingIntents);
    }

    let keyring = KeyringSecretStore::default();
    let fullsync_key = fullsync_refresh_token_key(&account.subject)?;
    let fullsync_refresh_token = required_secret_utf8(
        keyring.get(&fullsync_key)?,
        CliError::MissingStoredFullSyncRefreshToken,
    )?;
    let (client_id, client_secret) = load_google_client_config(&keyring)?;
    let oauth = GoogleOAuthConfig::new(client_id)?;

    println!("SYNC_ROOT_TWO_WAY_ACTIVATION_STAGE=verify_fullsync_credential");
    let tokens = oauth.refresh_access_token(&fullsync_refresh_token, &client_secret)?;
    if let Some(scope) = tokens.scope()
        && !oauth_scope_contains(Some(scope), GOOGLE_DRIVE_FULL_SCOPE)
    {
        return Err(CliError::GoogleFullSyncScopeNotGranted);
    }

    let api = GoogleDriveApi::new(tokens.access_token().clone())?;
    let user = api.user_info()?;
    if user.sub != account.subject {
        return Err(CliError::GoogleAccountMismatch);
    }

    if let Some(rotated_refresh_token) = tokens.refresh_token() {
        keyring.put(
            &fullsync_key,
            SecretValue::new(rotated_refresh_token.as_bytes().to_vec())?,
        )?;
    }

    println!("SYNC_ROOT_TWO_WAY_ACTIVATION_STAGE=validate_write_plan");
    let plan = plan_selected_root_remote_write_intents(&storage, &root, true)?;
    if plan.pending_events != 0
        || plan.create_file_needs_id != 0
        || plan.create_folder_needs_id != 0
        || plan.update_file_ready != 0
        || plan.trash_item_ready != 0
        || plan.conflicts != 0
        || plan.blocked_identity != 0
        || plan.blocked_authority != 0
    {
        return Err(CliError::SyncRootTwoWayActivationPlannerNotClean);
    }

    println!("SYNC_ROOT_TWO_WAY_ACTIVATION_STAGE=activate_mode_gate");
    if !storage.update_sync_root_mode_if_expected(
        &root.id,
        SyncMode::ReceiveOnly,
        SyncMode::TwoWay,
    )? {
        return Err(CliError::SyncRootTwoWayActivationCompareAndSetFailed);
    }

    let roots = storage.list_sync_roots(&provider, &account.subject)?;
    if roots.len() != 1 || roots[0].mode != SyncMode::TwoWay {
        return Err(CliError::SyncRootTwoWayActivationPostconditionFailed);
    }

    println!("SYNC_ROOT_TWO_WAY_ACTIVATION=PASS");
    println!("PREVIOUS_MODE=receive_only");
    println!("MODE=two_way");
    println!("FULLSYNC_CREDENTIAL_VERIFIED=yes");
    println!("ACCOUNT_SUBJECT_MATCH=yes");
    println!("RECEIVE_ONLY_CONVERGED=yes");
    println!("LOCAL_JOURNAL_PENDING=0");
    println!("REMOTE_CATALOG_CURRENT=yes");
    println!("AUTHORITY_CURSOR_MATCH=yes");
    println!("REMOTE_WRITE_INTENTS_EXISTING=0");
    println!("DATABASE_MUTATION=yes");
    println!("FILESYSTEM_MUTATION=no");
    println!("FILE_CONTENT_ACCESSED=no");
    println!("PROVIDER_WRITE_METHOD_CALLED=no");
    println!("REMOTE_WRITE_INTENT_PERSISTED=no");
    println!("REMOTE_WRITE_EXECUTION_ENABLED=no");
    println!("ROOT_PATH_PRINTED=no");
    println!("LOCAL_NAMES_PRINTED=no");
    println!("REMOTE_IDS_PRINTED=no");
    println!("TOKEN_VALUES_PRINTED=no");
    println!("DRIVE_WRITE_ACCESS=credential_and_root_gate_only");

    Ok(())
}

fn sync_roots_deactivate_two_way() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;
    if roots.len() != 1 {
        return Err(CliError::SyncRootTwoWayActivationSelectionFailed);
    }
    let root = roots
        .into_iter()
        .next()
        .ok_or(CliError::SyncRootTwoWayActivationSelectionFailed)?;

    if root.mode == SyncMode::ReceiveOnly {
        println!("SYNC_ROOT_TWO_WAY_DEACTIVATION=ALREADY_RECEIVE_ONLY");
        println!("MODE=receive_only");
        println!("DATABASE_MUTATION=no");
        println!("DRIVE_WRITE_ACCESS=no");
        return Ok(());
    }
    if root.mode != SyncMode::TwoWay {
        return Err(CliError::SyncRootTwoWayActivationModeUnsupported);
    }

    if !storage.update_sync_root_mode_if_expected(
        &root.id,
        SyncMode::TwoWay,
        SyncMode::ReceiveOnly,
    )? {
        return Err(CliError::SyncRootTwoWayActivationCompareAndSetFailed);
    }

    println!("SYNC_ROOT_TWO_WAY_DEACTIVATION=PASS");
    println!("PREVIOUS_MODE=two_way");
    println!("MODE=receive_only");
    println!("DATABASE_MUTATION=yes");
    println!("PROVIDER_WRITE_METHOD_CALLED=no");
    println!("REMOTE_WRITE_EXECUTION_ENABLED=no");
    println!("DRIVE_WRITE_ACCESS=no");
    Ok(())
}

fn sync_roots_cycle() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let mut storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;

    if roots.len() != 1 {
        println!("SYNC_ROOT_RECEIVE_ONLY_CYCLE=SKIPPED");
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

    if root.mode != SyncMode::ReceiveOnly {
        return Err(CliError::SyncRootMetadataStepModeUnsupported);
    }

    ensure_keyring_available()?;
    let keyring = KeyringSecretStore::default();
    let refresh_key = refresh_token_key(&account.subject)?;
    let refresh_token = required_secret_utf8(
        keyring.get(&refresh_key)?,
        CliError::MissingStoredRefreshToken,
    )?;
    let (client_id, client_secret) = load_google_client_config(&keyring)?;

    println!("SYNC_ROOT_RECEIVE_ONLY_CYCLE_STAGE=refresh_access_token");
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

    println!("SYNC_ROOT_RECEIVE_ONLY_CYCLE_STAGE=verify_account");
    let api = GoogleDriveApi::new(tokens.access_token().clone())?;
    let user = api.user_info()?;
    if user.sub != account.subject {
        return Err(CliError::GoogleAccountMismatch);
    }

    println!("SYNC_ROOT_RECEIVE_ONLY_CYCLE_STAGE=execute_bounded_cycle");
    let result =
        execute_selected_root_receive_only_cycle(&api, &mut storage, root, unix_time_ms()?)?;

    let filesystem_mutation = result.directories_created != 0
        || result.files_materialized != 0
        || result.files_replaced != 0
        || result.files_deleted != 0
        || result.directories_deleted != 0;

    println!("SYNC_ROOT_RECEIVE_ONLY_CYCLE=PASS");
    println!("MODE=receive_only");
    println!("BOUNDARY=one_metadata_step_plus_at_most_one_convergence_class");
    println!("METADATA_PHASE={}", result.metadata_phase.as_str());
    println!(
        "METADATA_AUTHORITATIVE_ITEMS={}",
        result.metadata_authoritative_items
    );
    println!("METADATA_PAGE_COUNT={}", result.metadata_page_count);
    println!("METADATA_CHANGE_COUNT={}", result.metadata_change_count);
    println!(
        "METADATA_CATALOG_MUTATIONS={}",
        result.metadata_catalog_mutations
    );
    println!(
        "METADATA_WINDOW_COMPLETE={}",
        yes_no(result.metadata_window_complete)
    );
    println!(
        "INITIAL_CATCHUP_COMPLETE={}",
        yes_no(result.initial_catchup_complete)
    );
    println!(
        "CONVERGENCE_EXECUTED={}",
        yes_no(result.convergence_executed)
    );
    println!(
        "CONVERGENCE_PHASE={}",
        result
            .convergence_phase
            .map(|phase| phase.as_str())
            .unwrap_or("none")
    );
    println!(
        "CONVERGENCE_ACTIONS_EXECUTED={}",
        result.convergence_actions_executed
    );
    println!("DIRECTORIES_CREATED={}", result.directories_created);
    println!("FILES_MATERIALIZED={}", result.files_materialized);
    println!("FILES_VERIFIED={}", result.files_verified);
    println!("FILES_REPLACED={}", result.files_replaced);
    println!("FILES_DELETED={}", result.files_deleted);
    println!("DIRECTORIES_DELETED={}", result.directories_deleted);
    println!("BYTES_DOWNLOADED={}", result.bytes_downloaded);
    println!("BYTES_VERIFIED={}", result.bytes_verified);
    println!("RECEIPTS_RECORDED={}", result.receipts_recorded);
    println!("RECEIPTS_DELETED={}", result.receipts_deleted);
    println!("FINAL_ACTIONS={}", result.final_actions);
    println!("FINAL_BLOCKED_ACTIONS={}", result.final_blocked_actions);
    println!(
        "NEXT_METADATA_PHASE={}",
        result.next_metadata_phase.as_str()
    );
    println!(
        "NEXT_CONVERGENCE_PHASE={}",
        result
            .next_convergence_phase
            .map(|phase| phase.as_str())
            .unwrap_or("none")
    );
    println!("CONVERGED={}", yes_no(result.converged));
    println!(
        "REQUIRES_ANOTHER_INVOCATION={}",
        yes_no(result.requires_another_invocation)
    );
    println!(
        "MANUAL_INTERVENTION_REQUIRED={}",
        yes_no(result.manual_intervention_required)
    );
    println!("STOP_REASON={}", result.stop_reason.as_str());
    println!("NETWORK_CHECK=performed");
    println!("DATABASE_MUTATION=yes");
    println!("FILESYSTEM_MUTATION={}", yes_no(filesystem_mutation));
    println!("ROOT_PATH_PRINTED=no");
    println!("LOCAL_NAMES_PRINTED=no");
    println!("REMOTE_ROOT_ID_PRINTED=no");
    println!("REMOTE_METADATA_PRINTED=no");
    println!("HASH_VALUE_PRINTED=no");
    println!("TOKEN_VALUES_PRINTED=no");
    println!("DRIVE_WRITE_ACCESS=no");

    Ok(())
}

fn sync_roots_run_to_idle() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let mut storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;

    if roots.len() != 1 {
        println!("SYNC_ROOT_RECEIVE_ONLY_RUN_TO_IDLE=SKIPPED");
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

    if root.mode != SyncMode::ReceiveOnly {
        return Err(CliError::SyncRootMetadataStepModeUnsupported);
    }

    ensure_keyring_available()?;
    let keyring = KeyringSecretStore::default();
    let refresh_key = refresh_token_key(&account.subject)?;
    let refresh_token = required_secret_utf8(
        keyring.get(&refresh_key)?,
        CliError::MissingStoredRefreshToken,
    )?;
    let (client_id, client_secret) = load_google_client_config(&keyring)?;

    println!("SYNC_ROOT_RECEIVE_ONLY_RUN_TO_IDLE_STAGE=refresh_access_token");
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

    println!("SYNC_ROOT_RECEIVE_ONLY_RUN_TO_IDLE_STAGE=verify_account");
    let api = GoogleDriveApi::new(tokens.access_token().clone())?;
    let user = api.user_info()?;
    if user.sub != account.subject {
        return Err(CliError::GoogleAccountMismatch);
    }

    println!("SYNC_ROOT_RECEIVE_ONLY_RUN_TO_IDLE_STAGE=execute_single_flight");
    let single_flight = execute_selected_root_receive_only_single_flight(
        &api,
        &mut storage,
        root,
        unix_time_ms()?,
    )?;

    let result = match single_flight {
        SelectedRootReceiveOnlySingleFlightResult::Busy => {
            println!("SYNC_ROOT_RECEIVE_ONLY_RUN_TO_IDLE=BUSY");
            println!("MODE=receive_only");
            println!("SINGLE_FLIGHT=busy");
            println!("SINGLE_FLIGHT_SCOPE=in_process");
            println!("CROSS_PROCESS_LOCK=no");
            println!("SYNC_EXECUTION=not_started");
            println!("REQUIRES_ANOTHER_INVOCATION=yes");
            println!("MANUAL_INTERVENTION_REQUIRED=no");
            println!("NETWORK_CHECK=performed");
            println!("DATABASE_MUTATION=no");
            println!("FILESYSTEM_MUTATION=no");
            println!("ROOT_PATH_PRINTED=no");
            println!("LOCAL_NAMES_PRINTED=no");
            println!("REMOTE_ROOT_ID_PRINTED=no");
            println!("REMOTE_METADATA_PRINTED=no");
            println!("HASH_VALUE_PRINTED=no");
            println!("TOKEN_VALUES_PRINTED=no");
            println!("DRIVE_WRITE_ACCESS=no");
            return Ok(());
        }
        SelectedRootReceiveOnlySingleFlightResult::Executed(result) => result,
    };

    let filesystem_mutation = result.directories_created != 0
        || result.files_materialized != 0
        || result.files_replaced != 0
        || result.files_deleted != 0
        || result.directories_deleted != 0;

    println!("SYNC_ROOT_RECEIVE_ONLY_RUN_TO_IDLE=PASS");
    println!("MODE=receive_only");
    println!("SINGLE_FLIGHT=acquired");
    println!("SINGLE_FLIGHT_SCOPE=in_process");
    println!("CROSS_PROCESS_LOCK=no");
    println!("BOUNDARY=bounded_run_to_observed_idle");
    println!("MAX_ROUNDS={}", SUPERVISED_RECEIVE_ONLY_RUN_MAX_ROUNDS);
    println!("ROUNDS_EXECUTED={}", result.rounds_executed);
    println!("METADATA_ROUNDS={}", result.metadata_rounds);
    println!("CONVERGENCE_ONLY_ROUNDS={}", result.convergence_only_rounds);
    println!("BOOTSTRAP_ROUNDS={}", result.bootstrap_rounds);
    println!(
        "COLLECT_CHANGE_PAGE_ROUNDS={}",
        result.collect_change_page_rounds
    );
    println!("EXECUTE_WINDOW_ROUNDS={}", result.execute_window_rounds);
    println!(
        "CONVERGENCE_PHASES_EXECUTED={}",
        result.convergence_phases_executed
    );
    println!("DIRECTORIES_CREATED={}", result.directories_created);
    println!("FILES_MATERIALIZED={}", result.files_materialized);
    println!("FILES_VERIFIED={}", result.files_verified);
    println!("FILES_REPLACED={}", result.files_replaced);
    println!("FILES_DELETED={}", result.files_deleted);
    println!("DIRECTORIES_DELETED={}", result.directories_deleted);
    println!("BYTES_DOWNLOADED={}", result.bytes_downloaded);
    println!("BYTES_VERIFIED={}", result.bytes_verified);
    println!("RECEIPTS_RECORDED={}", result.receipts_recorded);
    println!("RECEIPTS_DELETED={}", result.receipts_deleted);
    println!(
        "FINAL_CONVERGENCE_KNOWN={}",
        yes_no(result.final_convergence_known)
    );
    println!("FINAL_ACTIONS={}", result.final_actions);
    println!("FINAL_BLOCKED_ACTIONS={}", result.final_blocked_actions);
    println!(
        "NEXT_METADATA_PHASE={}",
        result.next_metadata_phase.as_str()
    );
    println!(
        "NEXT_CONVERGENCE_PHASE={}",
        result
            .next_convergence_phase
            .map(|phase| phase.as_str())
            .unwrap_or("none")
    );
    println!("CONVERGED={}", yes_no(result.converged));
    println!(
        "REQUIRES_ANOTHER_INVOCATION={}",
        yes_no(result.requires_another_invocation)
    );
    println!(
        "MANUAL_INTERVENTION_REQUIRED={}",
        yes_no(result.manual_intervention_required)
    );
    println!("STOP_REASON={}", result.stop_reason.as_str());
    println!("NETWORK_CHECK=performed");
    println!("DATABASE_MUTATION=yes");
    println!("FILESYSTEM_MUTATION={}", yes_no(filesystem_mutation));
    println!("ROOT_PATH_PRINTED=no");
    println!("LOCAL_NAMES_PRINTED=no");
    println!("REMOTE_ROOT_ID_PRINTED=no");
    println!("REMOTE_METADATA_PRINTED=no");
    println!("HASH_VALUE_PRINTED=no");
    println!("TOKEN_VALUES_PRINTED=no");
    println!("DRIVE_WRITE_ACCESS=no");

    Ok(())
}

fn sync_roots_local_baseline() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let mut storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;

    if roots.len() != 1 {
        println!("SYNC_ROOT_LOCAL_BASELINE=SKIPPED");
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
        println!("FILE_CONTENT_ACCESSED=no");
        println!("DRIVE_WRITE_ACCESS=no");
        return Ok(());
    }

    let root = roots
        .first()
        .ok_or(CliError::SyncRootReconcilePlanSelectionFailed)?;

    if root.mode != SyncMode::ReceiveOnly {
        return Err(CliError::SyncRootMetadataStepModeUnsupported);
    }

    let existing = storage.sync_root_local_inventory_state(&root.id)?;
    if existing.snapshot_complete && existing.observation_valid {
        println!("SYNC_ROOT_LOCAL_BASELINE=ALREADY_CAPTURED");
        println!("MODE=receive_only");
        println!("ITEMS_CAPTURED={}", existing.item_count);
        println!("SNAPSHOT_COMPLETE=yes");
        println!("OBSERVATION_VALID=yes");
        println!("NETWORK_CHECK=not_performed");
        println!("DATABASE_MUTATION=no");
        println!("FILESYSTEM_READ=not_performed");
        println!("FILESYSTEM_MUTATION=no");
        println!("FILE_CONTENT_ACCESSED=no");
        println!("ROOT_PATH_PRINTED=no");
        println!("LOCAL_NAMES_PRINTED=no");
        println!("REMOTE_ROOT_ID_PRINTED=no");
        println!("REMOTE_METADATA_PRINTED=no");
        println!("HASH_VALUE_PRINTED=no");
        println!("TOKEN_VALUES_PRINTED=no");
        println!("DRIVE_WRITE_ACCESS=no");
        return Ok(());
    }

    println!(
        "SYNC_ROOT_LOCAL_BASELINE_STAGE={}",
        if existing.snapshot_complete {
            "recapture_invalidated_baseline"
        } else {
            "capture_metadata_snapshot"
        }
    );
    let result = capture_selected_root_local_baseline(&mut storage, root, unix_time_ms()?)?;

    println!("SYNC_ROOT_LOCAL_BASELINE=PASS");
    println!("MODE=receive_only");
    println!("ITEMS_CAPTURED={}", result.items_captured);
    println!("FILES_CAPTURED={}", result.files_captured);
    println!("DIRECTORIES_CAPTURED={}", result.directories_captured);
    println!("CONVERGENCE_ACTIONS={}", result.convergence_actions);
    println!("SNAPSHOT_COMPLETE={}", yes_no(result.snapshot_complete));
    println!("OBSERVATION_VALID=yes");
    println!("NETWORK_CHECK=not_performed");
    println!("DATABASE_MUTATION=yes");
    println!("FILESYSTEM_READ=metadata_only");
    println!("FILESYSTEM_MUTATION=no");
    println!("FILE_CONTENT_ACCESSED=no");
    println!("ROOT_PATH_PRINTED=no");
    println!("LOCAL_NAMES_PRINTED=no");
    println!("REMOTE_ROOT_ID_PRINTED=no");
    println!("REMOTE_METADATA_PRINTED=no");
    println!("HASH_VALUE_PRINTED=no");
    println!("TOKEN_VALUES_PRINTED=no");
    println!("DRIVE_WRITE_ACCESS=no");

    Ok(())
}

fn sync_roots_local_diff() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;

    if roots.len() != 1 {
        println!("SYNC_ROOT_LOCAL_DIFF=SKIPPED");
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
        println!("FILE_CONTENT_ACCESSED=no");
        println!("DRIVE_WRITE_ACCESS=no");
        return Ok(());
    }

    let root = roots
        .first()
        .ok_or(CliError::SyncRootReconcilePlanSelectionFailed)?;

    if root.mode != SyncMode::ReceiveOnly {
        return Err(CliError::SyncRootMetadataStepModeUnsupported);
    }

    println!("SYNC_ROOT_LOCAL_DIFF_STAGE=scan_metadata");
    let result = plan_selected_root_local_inventory_diff(&storage, root)?;

    println!("SYNC_ROOT_LOCAL_DIFF=PASS");
    println!("MODE=receive_only");
    println!("BASELINE_ITEMS={}", result.baseline_items);
    println!("OBSERVED_ITEMS={}", result.observed_items);
    println!("CHANGES_TOTAL={}", result.action_count());
    println!("CREATED={}", result.created);
    println!("DELETED={}", result.deleted);
    println!("MODIFIED={}", result.modified);
    println!("TYPE_CHANGED={}", result.type_changed);
    println!("CLEAN={}", yes_no(result.clean()));
    println!("RENAME_COALESCING=no");
    println!("NETWORK_CHECK=not_performed");
    println!("DATABASE_MUTATION=no");
    println!("FILESYSTEM_READ=metadata_only");
    println!("FILESYSTEM_MUTATION=no");
    println!("FILE_CONTENT_ACCESSED=no");
    println!("ROOT_PATH_PRINTED=no");
    println!("LOCAL_NAMES_PRINTED=no");
    println!("REMOTE_ROOT_ID_PRINTED=no");
    println!("REMOTE_METADATA_PRINTED=no");
    println!("HASH_VALUE_PRINTED=no");
    println!("TOKEN_VALUES_PRINTED=no");
    println!("DRIVE_WRITE_ACCESS=no");

    Ok(())
}

fn sync_roots_local_journal() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let mut storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;

    if roots.len() != 1 {
        println!("SYNC_ROOT_LOCAL_JOURNAL=SKIPPED");
        println!(
            "REASON={}",
            if roots.is_empty() {
                "no_configured_root"
            } else {
                "multiple_roots_require_selector"
            }
        );
        println!("NETWORK_CHECK=not_performed");
        println!("FILESYSTEM_MUTATION=no");
        println!("FILE_CONTENT_ACCESSED=no");
        println!("DRIVE_WRITE_ACCESS=no");
        return Ok(());
    }

    let root = roots
        .first()
        .ok_or(CliError::SyncRootReconcilePlanSelectionFailed)?;

    if root.mode != SyncMode::ReceiveOnly {
        return Err(CliError::SyncRootMetadataStepModeUnsupported);
    }

    println!("SYNC_ROOT_LOCAL_JOURNAL_STAGE=reconcile_durable_events");
    let result = journal_selected_root_local_inventory_diff(&mut storage, root, unix_time_ms()?)?;

    println!("SYNC_ROOT_LOCAL_JOURNAL=PASS");
    println!("MODE=receive_only");
    println!("CHANGES_TOTAL={}", result.changes_total);
    println!("CREATED={}", result.created);
    println!("DELETED={}", result.deleted);
    println!("MODIFIED={}", result.modified);
    println!("TYPE_CHANGED={}", result.type_changed);
    println!("BASELINE_GENERATION={}", result.baseline_generation);
    println!("PENDING_EVENTS={}", result.pending_events);
    println!("SUPERSEDED_EVENTS={}", result.superseded_events);
    println!("RENAME_COALESCING=no");
    println!("BASELINE_MUTATED=no");
    println!("NETWORK_CHECK=not_performed");
    println!("DATABASE_MUTATION=yes");
    println!("FILESYSTEM_READ=metadata_only");
    println!("FILESYSTEM_MUTATION=no");
    println!("FILE_CONTENT_ACCESSED=no");
    println!("ROOT_PATH_PRINTED=no");
    println!("LOCAL_NAMES_PRINTED=no");
    println!("REMOTE_ROOT_ID_PRINTED=no");
    println!("REMOTE_METADATA_PRINTED=no");
    println!("HASH_VALUE_PRINTED=no");
    println!("TOKEN_VALUES_PRINTED=no");
    println!("DRIVE_WRITE_ACCESS=no");

    Ok(())
}

fn sync_roots_convergence_plan() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;

    if roots.len() != 1 {
        println!("SYNC_ROOT_CONVERGENCE_PLAN=SKIPPED");
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
        println!("FILE_CONTENT_ACCESSED=no");
        println!("DRIVE_WRITE_ACCESS=no");
        return Ok(());
    }

    let root = roots
        .first()
        .ok_or(CliError::SyncRootReconcilePlanSelectionFailed)?;

    let plan = plan_selected_root_receive_only_convergence(&storage, root)?;

    println!("SYNC_ROOT_CONVERGENCE_PLAN=PASS");
    println!("MODE=receive_only");
    println!("MAX_ACTIONS=10000");
    println!("REMOTE_ITEMS={}", plan.remote_items);
    println!("LOCAL_ENTRIES={}", plan.local_entries);
    println!("OWNERSHIP_RECEIPTS={}", plan.receipt_count);
    println!("ACTIONS_TOTAL={}", plan.action_count());
    println!("CREATE_DIRECTORIES={}", plan.create_directories);
    println!(
        "MATERIALIZE_MISSING_FILES={}",
        plan.materialize_missing_files
    );
    println!("VERIFY_EXISTING_FILES={}", plan.verify_existing_files);
    println!(
        "REVALIDATE_STALE_FILE_REPLACEMENTS={}",
        plan.revalidate_stale_file_replacements
    );
    println!(
        "REVALIDATE_STALE_FILE_DELETIONS={}",
        plan.revalidate_stale_file_deletions
    );
    println!(
        "DELETE_OWNED_EMPTY_DIRECTORIES={}",
        plan.delete_owned_empty_directories
    );
    println!("BLOCKED_ACTIONS={}", plan.blocked_actions);
    println!("CURRENT_OWNED_FILES={}", plan.current_owned_files);
    println!(
        "CURRENT_OWNED_DIRECTORIES={}",
        plan.current_owned_directories
    );
    println!(
        "UNOWNED_MATCHING_DIRECTORIES={}",
        plan.unowned_matching_directories
    );
    println!("BATCH_EXECUTION_AVAILABLE=no");
    println!("NETWORK_CHECK=not_performed");
    println!("DATABASE_MUTATION=no");
    println!("FILESYSTEM_READ=metadata_only");
    println!("FILESYSTEM_MUTATION=no");
    println!("FILE_CONTENT_ACCESSED=no");
    println!("ROOT_PATH_PRINTED=no");
    println!("LOCAL_NAMES_PRINTED=no");
    println!("REMOTE_ROOT_ID_PRINTED=no");
    println!("REMOTE_METADATA_PRINTED=no");
    println!("HASH_VALUE_PRINTED=no");
    println!("TOKEN_VALUES_PRINTED=no");
    println!("DRIVE_WRITE_ACCESS=no");

    Ok(())
}

fn sync_roots_converge() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let mut storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;

    if roots.len() != 1 {
        println!("SYNC_ROOT_UNIFIED_CONVERGENCE=SKIPPED");
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

    let decision = plan_selected_root_unified_convergence_step(&storage, root)?;
    let provider_required = decision.requires_content_provider();

    let result = if provider_required {
        ensure_keyring_available()?;
        let keyring = KeyringSecretStore::default();
        let refresh_key = refresh_token_key(&account.subject)?;
        let refresh_token = required_secret_utf8(
            keyring.get(&refresh_key)?,
            CliError::MissingStoredRefreshToken,
        )?;
        let (client_id, client_secret) = load_google_client_config(&keyring)?;

        println!("SYNC_ROOT_UNIFIED_CONVERGENCE_STAGE=refresh_access_token");
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

        println!("SYNC_ROOT_UNIFIED_CONVERGENCE_STAGE=verify_account");
        let api = GoogleDriveApi::new(tokens.access_token().clone())?;
        let user = api.user_info()?;
        if user.sub != account.subject {
            return Err(CliError::GoogleAccountMismatch);
        }

        println!("SYNC_ROOT_UNIFIED_CONVERGENCE_STAGE=execute_one_phase");
        execute_selected_root_unified_convergence_step(Some(&api), &mut storage, root)?
    } else {
        println!("SYNC_ROOT_UNIFIED_CONVERGENCE_STAGE=execute_one_phase");
        execute_selected_root_unified_convergence_step::<GoogleDriveApi>(None, &mut storage, root)?
    };

    let filesystem_mutation = result.directories_created != 0
        || result.files_materialized != 0
        || result.files_replaced != 0
        || result.files_deleted != 0
        || result.directories_deleted != 0;

    println!("SYNC_ROOT_UNIFIED_CONVERGENCE=PASS");
    println!("MODE=receive_only");
    println!("DISPATCH_POLICY=one_action_class_per_approval");
    println!("INITIAL_ACTIONS={}", result.initial_actions);
    println!(
        "PHASE_EXECUTED={}",
        result
            .phase_executed
            .map(|phase| phase.as_str())
            .unwrap_or("none")
    );
    println!("PHASE_ACTIONS_PLANNED={}", result.phase_actions_planned);
    println!("PHASE_ACTIONS_EXECUTED={}", result.phase_actions_executed);
    println!("DIRECTORIES_CREATED={}", result.directories_created);
    println!("FILES_MATERIALIZED={}", result.files_materialized);
    println!("FILES_VERIFIED={}", result.files_verified);
    println!("FILES_REPLACED={}", result.files_replaced);
    println!("FILES_DELETED={}", result.files_deleted);
    println!("DIRECTORIES_DELETED={}", result.directories_deleted);
    println!("BYTES_DOWNLOADED={}", result.bytes_downloaded);
    println!("BYTES_VERIFIED={}", result.bytes_verified);
    println!("RECEIPTS_RECORDED={}", result.receipts_recorded);
    println!("RECEIPTS_DELETED={}", result.receipts_deleted);
    println!("FINAL_ACTIONS={}", result.final_actions);
    println!("FINAL_BLOCKED_ACTIONS={}", result.final_blocked_actions);
    println!(
        "NEXT_PHASE={}",
        result
            .next_phase
            .map(|phase| phase.as_str())
            .unwrap_or("none")
    );
    println!("CONVERGED={}", yes_no(result.converged));
    println!(
        "REQUIRES_ANOTHER_INVOCATION={}",
        yes_no(result.requires_another_invocation)
    );
    println!(
        "MANUAL_INTERVENTION_REQUIRED={}",
        yes_no(result.manual_intervention_required)
    );
    println!("STOP_REASON={}", result.stop_reason.as_str());
    println!(
        "NETWORK_CHECK={}",
        if provider_required {
            "performed"
        } else {
            "not_performed"
        }
    );
    println!(
        "DATABASE_MUTATION={}",
        yes_no(result.receipts_recorded != 0 || result.receipts_deleted != 0)
    );
    println!("FILESYSTEM_MUTATION={}", yes_no(filesystem_mutation));
    println!("ROOT_PATH_PRINTED=no");
    println!("LOCAL_NAMES_PRINTED=no");
    println!("REMOTE_ROOT_ID_PRINTED=no");
    println!("REMOTE_METADATA_PRINTED=no");
    println!("HASH_VALUE_PRINTED=no");
    println!("TOKEN_VALUES_PRINTED=no");
    println!("DRIVE_WRITE_ACCESS=no");

    Ok(())
}

fn sync_roots_stale_files_plan() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;

    if roots.len() != 1 {
        println!("SYNC_ROOT_STALE_FILE_BATCH_PLAN=SKIPPED");
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
        println!("FILE_CONTENT_ACCESSED=no");
        println!("DRIVE_WRITE_ACCESS=no");
        return Ok(());
    }

    let root = roots
        .first()
        .ok_or(CliError::SyncRootReconcilePlanSelectionFailed)?;

    let result = plan_selected_root_stale_files(&storage, root)?;

    println!("SYNC_ROOT_STALE_FILE_BATCH_PLAN=PASS");
    println!("MODE=receive_only");
    println!("MAX_ACTIONS={}", result.max_actions);
    println!("CURRENT_FILE_RECEIPTS={}", result.current_receipts);
    println!("STALE_FILE_RECEIPTS={}", result.stale_receipts_total);
    println!("REPLACEMENT_CANDIDATES={}", result.replacement_candidates);
    println!("DELETION_CANDIDATES={}", result.deletion_candidates);
    println!("SAFE_TO_REPLACE={}", result.safe_to_replace);
    println!("SAFE_TO_DELETE={}", result.safe_to_delete);
    println!("LOCAL_CONFLICTS={}", result.local_conflicts);
    println!("FILES_MISSING={}", result.files_missing);
    println!("TYPE_CONFLICTS={}", result.type_conflicts);
    println!("BYTES_HASHED={}", result.bytes_hashed);
    println!(
        "CONVERGENCE_REPLACEMENT_ACTIONS={}",
        result.convergence_replacement_actions
    );
    println!(
        "CONVERGENCE_DELETION_ACTIONS={}",
        result.convergence_deletion_actions
    );
    println!(
        "ALL_STALE_FILES_SAFE={}",
        yes_no(result.all_stale_files_safe())
    );
    println!("NETWORK_CHECK=not_performed");
    println!("DATABASE_MUTATION=no");
    println!("FILESYSTEM_READ=content_hashing");
    println!("FILESYSTEM_MUTATION=no");
    println!("FILE_CONTENT_ACCESSED={}", yes_no(result.bytes_hashed > 0));
    println!("ROOT_PATH_PRINTED=no");
    println!("LOCAL_NAMES_PRINTED=no");
    println!("REMOTE_ROOT_ID_PRINTED=no");
    println!("REMOTE_METADATA_PRINTED=no");
    println!("HASH_VALUE_PRINTED=no");
    println!("TOKEN_VALUES_PRINTED=no");
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

    let mut storage = Storage::open(&db_path)?;
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
    let result = materialize_selected_root_directories(&mut storage, root)?;
    let current_directory_receipts =
        storage.sync_root_directory_materialization_receipt_count(&root.id)?;
    let stale_directory_receipts =
        storage.sync_root_stale_directory_materialization_receipt_count(&root.id)?;

    println!("SYNC_ROOT_DIRECTORY_MATERIALIZATION=PASS");
    println!("MODE=receive_only");
    println!("REMOTE_DIRECTORIES={}", result.remote_directories);
    println!("BATCH_ACTION_LIMIT={}", result.batch_action_limit);
    println!(
        "DIRECTORY_ACTIONS_PLANNED={}",
        result.planned_directory_actions
    );
    println!("DIRECTORIES_CREATED={}", result.created_directories);
    println!(
        "DIRECTORIES_ALREADY_PRESENT={}",
        result.existing_directories
    );
    println!("PENDING_FILES={}", result.pending_files);
    println!("CURRENT_DIRECTORY_RECEIPTS={current_directory_receipts}");
    println!("STALE_DIRECTORY_RECEIPTS={stale_directory_receipts}");
    println!("BATCH_MODE=bounded_supervised");
    println!("FILE_BATCH_EXECUTION_AVAILABLE=no");
    println!("NETWORK_CHECK=not_performed");
    println!(
        "DATABASE_MUTATION={}",
        yes_no(result.created_directories > 0)
    );
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

fn sync_roots_adopt_directory() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let mut storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;

    if roots.len() != 1 {
        println!("SYNC_ROOT_DIRECTORY_ADOPTION=SKIPPED");
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

    let result = adopt_selected_root_existing_directory(&mut storage, root)?;
    let schema_version = storage.schema_version()?;

    println!("SYNC_ROOT_DIRECTORY_ADOPTION=PASS");
    println!("MODE=receive_only");
    println!("DIRECTORIES_ADOPTED={}", result.directories_adopted);
    println!(
        "CURRENT_DIRECTORY_RECEIPTS={}",
        result.current_directory_receipts
    );
    println!(
        "STALE_DIRECTORY_RECEIPTS={}",
        result.stale_directory_receipts
    );
    println!("SCHEMA_VERSION={schema_version}");
    println!("NETWORK_CHECK=not_performed");
    println!("DATABASE_MUTATION=yes");
    println!("FILESYSTEM_READ=metadata_only");
    println!("FILESYSTEM_MUTATION=no");
    println!("FILE_CONTENT_ACCESSED=no");
    println!("ROOT_PATH_PRINTED=no");
    println!("LOCAL_NAMES_PRINTED=no");
    println!("REMOTE_ROOT_ID_PRINTED=no");
    println!("REMOTE_METADATA_PRINTED=no");
    println!("TOKEN_VALUES_PRINTED=no");
    println!("DRIVE_WRITE_ACCESS=no");

    Ok(())
}

fn sync_roots_materialize_files() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let mut storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;

    if roots.len() != 1 {
        println!("SYNC_ROOT_FILE_BATCH_MATERIALIZATION=SKIPPED");
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

    println!("SYNC_ROOT_FILE_BATCH_MATERIALIZATION_STAGE=refresh_access_token");
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

    println!("SYNC_ROOT_FILE_BATCH_MATERIALIZATION_STAGE=verify_account");
    let api = GoogleDriveApi::new(tokens.access_token().clone())?;
    let user = api.user_info()?;
    if user.sub != account.subject {
        return Err(CliError::GoogleAccountMismatch);
    }

    println!("SYNC_ROOT_FILE_BATCH_MATERIALIZATION_STAGE=download_file_batch");
    let result = materialize_selected_root_missing_files(&api, &mut storage, root)?;

    let current_file_receipts = storage.sync_root_materialization_receipt_count(&root.id)?;
    let stale_file_receipts = storage.sync_root_stale_materialization_receipt_count(&root.id)?;

    println!("SYNC_ROOT_FILE_BATCH_MATERIALIZATION=PASS");
    println!("MODE=receive_only");
    println!("BATCH_ACTION_LIMIT={}", result.batch_action_limit);
    println!("FILE_ACTIONS_PLANNED={}", result.planned_file_actions);
    println!("FILES_DOWNLOADED={}", result.files_downloaded);
    println!("BYTES_DOWNLOADED={}", result.bytes_downloaded);
    println!("MAX_FILE_BYTES={}", result.max_file_bytes);
    println!(
        "PROVIDER_FINGERPRINTS_VERIFIED={}",
        result.provider_fingerprints_verified
    );
    println!("RECEIPTS_RECORDED={}", result.receipts_recorded);
    println!("CURRENT_FILE_RECEIPTS={current_file_receipts}");
    println!("STALE_FILE_RECEIPTS={stale_file_receipts}");
    println!("BATCH_MODE=bounded_supervised");
    println!("NETWORK_CHECK=performed");
    println!("DATABASE_MUTATION={}", yes_no(result.files_downloaded > 0));
    println!(
        "FILESYSTEM_MUTATION={}",
        yes_no(result.files_downloaded > 0)
    );
    println!(
        "FILE_CONTENT_ACCESSED={}",
        yes_no(result.files_downloaded > 0)
    );
    println!("FILES_CREATED={}", result.files_downloaded);
    println!("FILES_OVERWRITTEN=0");
    println!("FILES_DELETED=0");
    println!("DIRECTORIES_CREATED=0");
    println!("DIRECTORIES_REMOVED=0");
    println!("ATOMIC_NO_OVERWRITE_PROMOTION=yes");
    println!("PROVIDER_FINGERPRINT_BEFORE_AFTER=yes");
    println!("PROMOTED_SHA256_VERIFIED=yes");
    println!("TEMP_FILES_RETAINED=0");
    println!("ROOT_PATH_PRINTED=no");
    println!("LOCAL_NAMES_PRINTED=no");
    println!("REMOTE_ROOT_ID_PRINTED=no");
    println!("REMOTE_METADATA_PRINTED=no");
    println!("TOKEN_VALUES_PRINTED=no");
    println!("DRIVE_WRITE_ACCESS=no");
    println!("CONFIGURED_MAX_FILE_BYTES={SUPERVISED_FILE_DOWNLOAD_MAX_BYTES}");
    println!("CONFIGURED_MAX_FILE_ACTIONS={SUPERVISED_FILE_BATCH_MAX_ACTIONS}");

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

fn sync_roots_verify_files() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let mut storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;

    if roots.len() != 1 {
        println!("SYNC_ROOT_FILE_BATCH_VERIFICATION=SKIPPED");
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

    let readiness = plan_selected_root_receive_only_convergence(&storage, root)?;
    if readiness.blocked()
        || readiness.verify_existing_files == 0
        || readiness.verify_existing_files > SUPERVISED_FILE_BATCH_MAX_ACTIONS
        || readiness.create_directories != 0
        || readiness.materialize_missing_files != 0
        || readiness.revalidate_stale_file_replacements != 0
        || readiness.revalidate_stale_file_deletions != 0
        || readiness.delete_owned_empty_directories != 0
    {
        return Err(
            nubisync_daemon::SelectedRootExecutorError::LocalFileVerificationBatchNotReady.into(),
        );
    }

    ensure_keyring_available()?;
    let keyring = KeyringSecretStore::default();
    let refresh_key = refresh_token_key(&account.subject)?;
    let refresh_token = required_secret_utf8(
        keyring.get(&refresh_key)?,
        CliError::MissingStoredRefreshToken,
    )?;
    let (client_id, client_secret) = load_google_client_config(&keyring)?;

    println!("SYNC_ROOT_FILE_BATCH_VERIFICATION_STAGE=refresh_access_token");
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

    println!("SYNC_ROOT_FILE_BATCH_VERIFICATION_STAGE=verify_account");
    let api = GoogleDriveApi::new(tokens.access_token().clone())?;
    let user = api.user_info()?;
    if user.sub != account.subject {
        return Err(CliError::GoogleAccountMismatch);
    }

    println!("SYNC_ROOT_FILE_BATCH_VERIFICATION_STAGE=compare_sha256_batch");
    let result = verify_selected_root_existing_files(&api, &mut storage, root)?;

    let current_receipts = storage.sync_root_materialization_receipt_count(&root.id)?;
    let stale_receipts = storage.sync_root_stale_materialization_receipt_count(&root.id)?;

    println!("SYNC_ROOT_FILE_BATCH_VERIFICATION=PASS");
    println!("MODE=receive_only");
    println!("BATCH_ACTION_LIMIT={}", result.batch_action_limit);
    println!(
        "VERIFICATION_ACTIONS_PLANNED={}",
        result.planned_verification_actions
    );
    println!("FILES_VERIFIED={}", result.files_verified);
    println!("BYTES_VERIFIED={}", result.bytes_verified);
    println!("MAX_FILE_BYTES={}", result.max_file_bytes);
    println!("HASH_ALGORITHM=sha256");
    println!(
        "REMOTE_CONTENT_HASHES_VERIFIED={}",
        result.remote_content_hashes_verified
    );
    println!("RECEIPTS_RECORDED={}", result.receipts_recorded);
    println!("CURRENT_FILE_RECEIPTS={current_receipts}");
    println!("STALE_FILE_RECEIPTS={stale_receipts}");
    println!("BATCH_MODE=bounded_supervised");
    println!("NETWORK_CHECK=performed");
    println!("DATABASE_MUTATION=yes");
    println!("FILESYSTEM_READ=file_content");
    println!("FILESYSTEM_MUTATION=no");
    println!("LOCAL_FILE_CONTENT_ACCESSED=yes");
    println!("REMOTE_FILE_CONTENT_ACCESSED=yes");
    println!("FILES_CREATED=0");
    println!("FILES_OVERWRITTEN=0");
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

fn sync_roots_replace_stale_files() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let mut storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;

    if roots.len() != 1 {
        println!("SYNC_ROOT_STALE_FILE_BATCH_REPLACEMENT=SKIPPED");
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

    let readiness = plan_selected_root_stale_files(&storage, root)?;
    if !readiness.all_stale_files_safe()
        || readiness.replacement_candidates == 0
        || readiness.deletion_candidates != 0
    {
        return Err(
            nubisync_daemon::SelectedRootExecutorError::RemoteReplacementBatchNotReady.into(),
        );
    }

    ensure_keyring_available()?;
    let keyring = KeyringSecretStore::default();
    let refresh_key = refresh_token_key(&account.subject)?;
    let refresh_token = required_secret_utf8(
        keyring.get(&refresh_key)?,
        CliError::MissingStoredRefreshToken,
    )?;
    let (client_id, client_secret) = load_google_client_config(&keyring)?;

    println!("SYNC_ROOT_STALE_FILE_BATCH_REPLACEMENT_STAGE=refresh_access_token");
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

    println!("SYNC_ROOT_STALE_FILE_BATCH_REPLACEMENT_STAGE=verify_account");
    let api = GoogleDriveApi::new(tokens.access_token().clone())?;
    let user = api.user_info()?;
    if user.sub != account.subject {
        return Err(CliError::GoogleAccountMismatch);
    }

    println!("SYNC_ROOT_STALE_FILE_BATCH_REPLACEMENT_STAGE=download_and_replace_batch");
    let result = replace_selected_root_stale_files(&api, &mut storage, root)?;

    let current_receipts = storage.sync_root_materialization_receipt_count(&root.id)?;
    let stale_receipts = storage.sync_root_stale_materialization_receipt_count(&root.id)?;

    println!("SYNC_ROOT_STALE_FILE_BATCH_REPLACEMENT=PASS");
    println!("MODE=receive_only");
    println!("BATCH_ACTION_LIMIT={}", result.batch_action_limit);
    println!(
        "REPLACEMENT_ACTIONS_PLANNED={}",
        result.planned_replacement_actions
    );
    println!("FILES_REPLACED={}", result.files_replaced);
    println!("BYTES_DOWNLOADED={}", result.bytes_downloaded);
    println!("MAX_FILE_BYTES={}", result.max_file_bytes);
    println!(
        "PROVIDER_FINGERPRINTS_VERIFIED={}",
        result.provider_fingerprints_verified
    );
    println!(
        "STALE_BASELINES_VERIFIED={}",
        result.stale_baselines_verified
    );
    println!("RECEIPTS_RECORDED={}", result.receipts_recorded);
    println!("ATOMIC_REPLACEMENTS={}", result.atomic_replacements);
    println!(
        "REPLACEMENT_BACKUPS_CLEANED={}",
        result.replacement_backups_cleaned
    );
    println!("CURRENT_FILE_RECEIPTS={current_receipts}");
    println!("STALE_FILE_RECEIPTS={stale_receipts}");
    println!("BATCH_MODE=bounded_supervised");
    println!("NETWORK_CHECK=performed");
    println!("DATABASE_MUTATION=yes");
    println!("FILESYSTEM_MUTATION=yes");
    println!("LOCAL_FILE_CONTENT_ACCESSED=yes");
    println!("REMOTE_FILE_CONTENT_ACCESSED=yes");
    println!("FILES_CREATED=0");
    println!("FILES_OVERWRITTEN={}", result.files_replaced);
    println!("FILES_DELETED=0");
    println!("DIRECTORIES_CREATED=0");
    println!("DIRECTORIES_REMOVED=0");
    println!("ATOMIC_REPLACE=yes");
    println!("PROVIDER_FINGERPRINT_BEFORE_AFTER=yes");
    println!("PROMOTED_SHA256_VERIFIED=yes");
    println!("STALE_BASELINE_REVALIDATED=yes");
    println!("REPLACEMENT_BACKUPS_RETAINED=0");
    println!("TEMP_FILES_RETAINED=0");
    println!("ROOT_PATH_PRINTED=no");
    println!("LOCAL_NAMES_PRINTED=no");
    println!("REMOTE_ROOT_ID_PRINTED=no");
    println!("REMOTE_METADATA_PRINTED=no");
    println!("HASH_VALUE_PRINTED=no");
    println!("TOKEN_VALUES_PRINTED=no");
    println!("DRIVE_WRITE_ACCESS=no");

    Ok(())
}

fn sync_roots_delete_stale_files() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let mut storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;

    if roots.len() != 1 {
        println!("SYNC_ROOT_STALE_FILE_BATCH_DELETION=SKIPPED");
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

    let readiness = plan_selected_root_stale_files(&storage, root)?;
    if !readiness.all_stale_files_safe()
        || readiness.deletion_candidates == 0
        || readiness.replacement_candidates != 0
    {
        return Err(nubisync_daemon::SelectedRootExecutorError::RemoteDeletionBatchNotReady.into());
    }

    println!("SYNC_ROOT_STALE_FILE_BATCH_DELETION_STAGE=quarantine_and_delete_batch");
    let result = delete_selected_root_stale_files(&mut storage, root)?;

    let current_receipts = storage.sync_root_materialization_receipt_count(&root.id)?;
    let stale_receipts = storage.sync_root_stale_materialization_receipt_count(&root.id)?;

    println!("SYNC_ROOT_STALE_FILE_BATCH_DELETION=PASS");
    println!("MODE=receive_only");
    println!("BATCH_ACTION_LIMIT={}", result.batch_action_limit);
    println!(
        "DELETION_ACTIONS_PLANNED={}",
        result.planned_deletion_actions
    );
    println!("FILES_DELETED={}", result.files_deleted);
    println!("BYTES_VERIFIED={}", result.bytes_verified);
    println!(
        "STALE_BASELINES_VERIFIED={}",
        result.stale_baselines_verified
    );
    println!("RECEIPTS_DELETED={}", result.receipts_deleted);
    println!("QUARANTINE_RENAMES={}", result.quarantine_renames);
    println!(
        "QUARANTINE_FILES_REMOVED={}",
        result.quarantine_files_removed
    );
    println!("CURRENT_FILE_RECEIPTS={current_receipts}");
    println!("STALE_FILE_RECEIPTS={stale_receipts}");
    println!("BATCH_MODE=bounded_supervised");
    println!("NETWORK_CHECK=not_performed");
    println!("DATABASE_MUTATION=yes");
    println!("FILESYSTEM_MUTATION=yes");
    println!("LOCAL_FILE_CONTENT_ACCESSED=yes");
    println!("REMOTE_FILE_CONTENT_ACCESSED=no");
    println!("FILES_CREATED=0");
    println!("FILES_OVERWRITTEN=0");
    println!("DIRECTORIES_CREATED=0");
    println!("DIRECTORIES_REMOVED=0");
    println!("STALE_BASELINE_REVALIDATED=yes");
    println!("SAME_PARENT_QUARANTINE=yes");
    println!("QUARANTINE_FILES_RETAINED=0");
    println!("ROOT_PATH_PRINTED=no");
    println!("LOCAL_NAMES_PRINTED=no");
    println!("REMOTE_ROOT_ID_PRINTED=no");
    println!("REMOTE_METADATA_PRINTED=no");
    println!("HASH_VALUE_PRINTED=no");
    println!("TOKEN_VALUES_PRINTED=no");
    println!("DRIVE_WRITE_ACCESS=no");

    Ok(())
}

fn sync_roots_delete_stale_directories() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let mut storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;

    if roots.len() != 1 {
        println!("SYNC_ROOT_STALE_DIRECTORY_BATCH_DELETION=SKIPPED");
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
        println!("FILE_CONTENT_ACCESSED=no");
        println!("DRIVE_WRITE_ACCESS=no");
        return Ok(());
    }

    let root = roots
        .first()
        .ok_or(CliError::SyncRootReconcilePlanSelectionFailed)?;

    println!("SYNC_ROOT_STALE_DIRECTORY_BATCH_DELETION_STAGE=quarantine_and_delete_batch");
    let result = delete_selected_root_stale_directories(&mut storage, root)?;

    let current_directory_receipts =
        storage.sync_root_directory_materialization_receipt_count(&root.id)?;
    let stale_directory_receipts =
        storage.sync_root_stale_directory_materialization_receipt_count(&root.id)?;

    println!("SYNC_ROOT_STALE_DIRECTORY_BATCH_DELETION=PASS");
    println!("MODE=receive_only");
    println!("BATCH_ACTION_LIMIT={}", result.batch_action_limit);
    println!(
        "DELETION_ACTIONS_PLANNED={}",
        result.planned_deletion_actions
    );
    println!("DIRECTORIES_DELETED={}", result.directories_deleted);
    println!(
        "EMPTY_DIRECTORIES_VERIFIED={}",
        result.empty_directories_verified
    );
    println!("RECEIPTS_DELETED={}", result.receipts_deleted);
    println!("QUARANTINE_RENAMES={}", result.quarantine_renames);
    println!(
        "QUARANTINE_DIRECTORIES_REMOVED={}",
        result.quarantine_directories_removed
    );
    println!("CURRENT_DIRECTORY_RECEIPTS={current_directory_receipts}");
    println!("STALE_DIRECTORY_RECEIPTS={stale_directory_receipts}");
    println!("BATCH_MODE=bounded_supervised");
    println!("NETWORK_CHECK=not_performed");
    println!("DATABASE_MUTATION=yes");
    println!("FILESYSTEM_MUTATION=yes");
    println!("FILE_CONTENT_ACCESSED=no");
    println!("FILES_CREATED=0");
    println!("FILES_OVERWRITTEN=0");
    println!("FILES_DELETED=0");
    println!("DIRECTORIES_CREATED=0");
    println!("DIRECTORIES_REMOVED={}", result.directories_deleted);
    println!("EMPTY_DIRECTORY_REVALIDATED=yes");
    println!("SAME_PARENT_QUARANTINE=yes");
    println!("QUARANTINE_DIRECTORIES_RETAINED=0");
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

fn sync_roots_directory_deletion_plan() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;

    if roots.len() != 1 {
        println!("SYNC_ROOT_REMOTE_DIRECTORY_DELETION_PLAN=SKIPPED");
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
        println!("FILE_CONTENT_ACCESSED=no");
        println!("DRIVE_WRITE_ACCESS=no");
        return Ok(());
    }

    let root = roots
        .first()
        .ok_or(CliError::SyncRootReconcilePlanSelectionFailed)?;

    let result = plan_selected_root_remote_directory_deletion(&storage, root)?;

    println!("SYNC_ROOT_REMOTE_DIRECTORY_DELETION_PLAN=PASS");
    println!("MODE=receive_only");
    println!(
        "STALE_DIRECTORY_RECEIPTS_TOTAL={}",
        result.stale_receipts_total
    );
    println!("DELETION_CANDIDATES={}", result.deletion_candidates);
    println!("SAFE_TO_DELETE={}", result.safe_to_delete);
    println!(
        "DIRECTORIES_ALREADY_MISSING={}",
        result.directories_already_missing
    );
    println!("NON_EMPTY_DIRECTORIES={}", result.non_empty_directories);
    println!("TYPE_CONFLICTS={}", result.type_conflicts);
    println!("REMOTE_ID_ABSENT={}", yes_no(result.remote_id_absent));
    println!("READY_TO_DELETE={}", yes_no(result.ready()));
    println!("NETWORK_CHECK=not_performed");
    println!("DATABASE_MUTATION=no");
    println!("FILESYSTEM_READ=metadata_only");
    println!("FILESYSTEM_MUTATION=no");
    println!("FILE_CONTENT_ACCESSED=no");
    println!("ROOT_PATH_PRINTED=no");
    println!("LOCAL_NAMES_PRINTED=no");
    println!("REMOTE_ROOT_ID_PRINTED=no");
    println!("REMOTE_METADATA_PRINTED=no");
    println!("TOKEN_VALUES_PRINTED=no");
    println!("DRIVE_WRITE_ACCESS=no");

    Ok(())
}

fn sync_roots_delete_directory() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let mut storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;

    if roots.len() != 1 {
        println!("SYNC_ROOT_DIRECTORY_DELETION=SKIPPED");
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
        println!("FILE_CONTENT_ACCESSED=no");
        println!("DRIVE_WRITE_ACCESS=no");
        return Ok(());
    }

    let root = roots
        .first()
        .ok_or(CliError::SyncRootReconcilePlanSelectionFailed)?;

    let readiness = plan_selected_root_remote_directory_deletion(&storage, root)?;
    if !readiness.ready() {
        return Err(CliError::SyncRootDirectoryDeletionNotReady);
    }

    let result = delete_selected_root_existing_directory(&mut storage, root)?;
    let current_directory_receipts =
        storage.sync_root_directory_materialization_receipt_count(&root.id)?;
    let stale_directory_receipts =
        storage.sync_root_stale_directory_materialization_receipt_count(&root.id)?;

    println!("SYNC_ROOT_DIRECTORY_DELETION=PASS");
    println!("MODE=receive_only");
    println!("DIRECTORIES_DELETED={}", result.directories_deleted);
    println!(
        "EMPTY_DIRECTORY_VERIFIED={}",
        yes_no(result.empty_directory_verified)
    );
    println!("RECEIPT_DELETED={}", yes_no(result.receipt_deleted));
    println!("QUARANTINE_RENAME={}", yes_no(result.quarantine_rename));
    println!("CURRENT_DIRECTORY_RECEIPTS={current_directory_receipts}");
    println!("STALE_DIRECTORY_RECEIPTS={stale_directory_receipts}");
    println!("NETWORK_CHECK=not_performed");
    println!("DATABASE_MUTATION=yes");
    println!("FILESYSTEM_READ=metadata_only");
    println!("FILESYSTEM_MUTATION=yes");
    println!("FILE_CONTENT_ACCESSED=no");
    println!("FILES_CREATED=0");
    println!("FILES_DELETED=0");
    println!("DIRECTORIES_CREATED=0");
    println!("DIRECTORIES_REMOVED=1");
    println!("ROOT_PATH_PRINTED=no");
    println!("LOCAL_NAMES_PRINTED=no");
    println!("REMOTE_ROOT_ID_PRINTED=no");
    println!("REMOTE_METADATA_PRINTED=no");
    println!("TOKEN_VALUES_PRINTED=no");
    println!("DRIVE_WRITE_ACCESS=no");

    Ok(())
}

fn sync_roots_deletion_plan() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;

    if roots.len() != 1 {
        println!("SYNC_ROOT_REMOTE_DELETION_PLAN=SKIPPED");
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

    let result = plan_selected_root_remote_deletion(&storage, root)?;

    println!("SYNC_ROOT_REMOTE_DELETION_PLAN=PASS");
    println!("MODE=receive_only");
    println!("STALE_RECEIPTS_TOTAL={}", result.stale_receipts_total);
    println!("DELETION_CANDIDATES={}", result.deletion_candidates);
    println!("SAFE_TO_DELETE={}", result.safe_to_delete);
    println!("LOCAL_CONFLICTS={}", result.local_conflicts);
    println!("FILES_ALREADY_MISSING={}", result.files_already_missing);
    println!("TYPE_CONFLICTS={}", result.type_conflicts);
    println!("BYTES_HASHED={}", result.bytes_hashed);
    println!("REMOTE_ID_ABSENT={}", yes_no(result.remote_id_absent));
    println!("READY_TO_DELETE={}", yes_no(result.ready()));
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

fn sync_roots_delete_file() -> Result<(), CliError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let mut storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;

    if roots.len() != 1 {
        println!("SYNC_ROOT_FILE_DELETION=SKIPPED");
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

    let readiness = plan_selected_root_remote_deletion(&storage, root)?;
    if !readiness.ready() {
        return Err(CliError::SyncRootDeletionNotReady);
    }

    let result = delete_selected_root_existing_file(&mut storage, root)?;
    let current_receipts = storage.sync_root_materialization_receipt_count(&root.id)?;
    let stale_receipts = storage.sync_root_stale_materialization_receipt_count(&root.id)?;

    println!("SYNC_ROOT_FILE_DELETION=PASS");
    println!("MODE=receive_only");
    println!("FILES_DELETED={}", result.files_deleted);
    println!("BYTES_VERIFIED={}", result.bytes_verified);
    println!(
        "STALE_BASELINE_MATCH={}",
        yes_no(result.stale_baseline_match)
    );
    println!("RECEIPT_DELETED={}", yes_no(result.receipt_deleted));
    println!("QUARANTINE_RENAME={}", yes_no(result.quarantine_rename));
    println!("DURABLE_MATERIALIZATION_RECEIPTS={current_receipts}");
    println!("STALE_MATERIALIZATION_RECEIPTS={stale_receipts}");
    println!("NETWORK_CHECK=not_performed");
    println!("DATABASE_MUTATION=yes");
    println!("FILESYSTEM_READ=file_content");
    println!("FILESYSTEM_MUTATION=yes");
    println!("LOCAL_FILE_CONTENT_ACCESSED=yes");
    println!("REMOTE_FILE_CONTENT_ACCESSED=no");
    println!("FILES_CREATED=0");
    println!("FILES_OVERWRITTEN=0");
    println!("FILES_DELETED=1");
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

fn google_upgrade_full_sync() -> Result<(), CliError> {
    ensure_keyring_available()?;

    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(CliError::NoLocalGoogleAccount);
    }

    let storage = Storage::open(&db_path)?;
    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;

    let keyring = KeyringSecretStore::default();
    let readonly_key = refresh_token_key(&account.subject)?;
    let fullsync_key = fullsync_refresh_token_key(&account.subject)?;

    let readonly_before = keyring
        .get(&readonly_key)?
        .ok_or(CliError::MissingStoredRefreshToken)?;
    let previous_fullsync = keyring.get(&fullsync_key)?;

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
        oauth.begin_authorization(listen_addr.port(), GoogleDriveAccess::FullSync)?;

    println!("GOOGLE_FULLSYNC_UPGRADE_STAGE=authorize");
    println!("REQUESTED_DRIVE_ACCESS=full_sync");
    println!("REQUESTED_SCOPE=drive");
    println!("LOOPBACK_PORT={}", listen_addr.port());
    println!("READONLY_REFRESH_TOKEN_PRESENT=yes");
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
                "NubiSync received the Google FullSync authorization. You can close this tab.",
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

    println!("GOOGLE_FULLSYNC_UPGRADE_STAGE=exchange_code");
    let tokens = oauth.exchange_code(&authorization, &code, &client_secret)?;

    if !oauth_scope_contains(tokens.scope(), GOOGLE_DRIVE_FULL_SCOPE) {
        return Err(CliError::GoogleFullSyncScopeNotGranted);
    }

    println!("GOOGLE_FULLSYNC_UPGRADE_STAGE=verify_account");
    let api = GoogleDriveApi::new(tokens.access_token().clone())?;
    let user = api.user_info()?;
    if user.sub != account.subject {
        return Err(CliError::GoogleAccountMismatch);
    }

    let fresh_refresh_token = tokens
        .refresh_token()
        .ok_or(CliError::GoogleFullSyncRefreshTokenMissing)?;

    // The existing ReceiveOnly credential is an independent authority lane.
    // It must still be byte-for-byte unchanged before promotion.
    let readonly_pre_promotion = keyring
        .get(&readonly_key)?
        .ok_or(CliError::MissingStoredRefreshToken)?;
    if readonly_pre_promotion != readonly_before {
        return Err(CliError::GoogleReadonlyCredentialChangedDuringFullSyncUpgrade);
    }

    println!("GOOGLE_FULLSYNC_UPGRADE_STAGE=promote_separate_credential");
    keyring.put(
        &fullsync_key,
        SecretValue::new(fresh_refresh_token.as_bytes().to_vec())?,
    )?;

    let stored_fullsync = keyring
        .get(&fullsync_key)?
        .ok_or(CliError::GoogleFullSyncPromotionFailed)?;

    let readonly_after = keyring
        .get(&readonly_key)?
        .ok_or(CliError::MissingStoredRefreshToken)?;

    if stored_fullsync.expose_bytes() != fresh_refresh_token.as_bytes()
        || readonly_after != readonly_before
    {
        match previous_fullsync {
            Some(previous) => keyring.put(&fullsync_key, previous)?,
            None => keyring.delete(&fullsync_key)?,
        }

        if readonly_after != readonly_before {
            return Err(CliError::GoogleReadonlyCredentialChangedDuringFullSyncUpgrade);
        }
        return Err(CliError::GoogleFullSyncPromotionFailed);
    }

    println!("GOOGLE_FULLSYNC_UPGRADE=PASS");
    println!("ACCOUNT_SUBJECT_MATCH=yes");
    println!("REQUESTED_SCOPE=drive");
    println!("GRANTED_SCOPE_VERIFIED=yes");
    println!("FRESH_REFRESH_TOKEN_REQUIRED=yes");
    println!("FULLSYNC_REFRESH_TOKEN_STORAGE=OS_KEYRING");
    println!("FULLSYNC_KEY_PURPOSE=separate");
    println!("READONLY_REFRESH_TOKEN_UNCHANGED=yes");
    println!("ACCESS_TOKEN_STORAGE=memory_only");
    println!("ROOT_MODE_CHANGED=no");
    println!("DATABASE_MUTATION=no");
    println!("FILESYSTEM_MUTATION=no");
    println!("DRIVE_FILE_CONTENT_ACCESSED=no");
    println!("PROVIDER_WRITE_METHOD_CALLED=no");
    println!("REMOTE_WRITE_INTENT_PERSISTED=no");
    println!("TOKEN_VALUES_PRINTED=no");
    println!("DRIVE_WRITE_CREDENTIAL_PRESENT=yes");
    println!("DRIVE_WRITE_EXECUTION_ENABLED=no");

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

fn fullsync_refresh_token_key(account_subject: &str) -> Result<SecretKey, CliError> {
    Ok(SecretKey::new(
        "google-drive",
        account_subject,
        FULLSYNC_REFRESH_TOKEN_PURPOSE,
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

fn cli_requires_cross_process_execution_lock(args: &[String]) -> bool {
    if args.len() >= 3 && args[0] == "sync" && args[1] == "roots" {
        return matches!(
            args[2].as_str(),
            "metadata-step"
                | "cycle"
                | "run-to-idle"
                | "local-baseline"
                | "local-diff"
                | "local-journal"
                | "refresh-two-way-metadata"
                | "observe-write-authority"
                | "remote-write-plan"
                | "allocate-create-ids"
                | "folder-create-recovery-plan"
                | "submit-folder-create"
                | "recover-folder-create-submission"
                | "confirm-folder-create"
                | "settle-confirmed-folder-create"
                | "activate-two-way"
                | "deactivate-two-way"
                | "convergence-plan"
                | "converge"
                | "stale-files-plan"
                | "replace-stale-files"
                | "delete-stale-files"
                | "delete-stale-directories"
                | "reconcile-plan"
                | "materialize-directories"
                | "adopt-directory"
                | "materialize-files"
                | "materialize-file"
                | "verify-files"
                | "verify-file"
                | "verify-local"
                | "replacement-plan"
                | "replace-file"
                | "directory-deletion-plan"
                | "delete-directory"
                | "deletion-plan"
                | "delete-file"
                | "add"
        );
    }

    if args.len() >= 3 && args[0] == "auth" && args[1] == "google" && args[2] == "upgrade-full-sync"
    {
        return true;
    }

    args.len() >= 3 && args[0] == "drive" && args[1] == "catalog" && args[2] == "catchup"
}

fn nubisync_database_path() -> Result<PathBuf, CliError> {
    Ok(nubisync_data_dir()?.join("nubisync.db"))
}

fn nubisync_execution_lock_path() -> Result<PathBuf, CliError> {
    Ok(nubisync_data_dir()?.join("execution.lock"))
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
    fn phase5g_execution_lock_policy_covers_sync_mutation_surfaces() {
        let locked = [
            vec!["sync", "roots", "run-to-idle", "--approve"],
            vec!["sync", "roots", "local-baseline", "--approve"],
            vec!["sync", "roots", "local-journal", "--approve"],
            vec!["sync", "roots", "materialize-file", "--approve"],
            vec!["sync", "roots", "delete-file", "--approve"],
            vec!["sync", "roots", "add", "--mode", "receive_only"],
            vec!["drive", "catalog", "catchup"],
        ];

        for args in locked {
            let args = args.into_iter().map(str::to_owned).collect::<Vec<_>>();
            assert!(cli_requires_cross_process_execution_lock(&args));
        }

        for args in [
            vec!["sync", "roots", "status"],
            vec!["sync", "roots", "inventory", "--limit", "10"],
            vec!["drive", "catalog", "status"],
            vec!["auth", "google", "status"],
        ] {
            let args = args.into_iter().map(str::to_owned).collect::<Vec<_>>();
            assert!(!cli_requires_cross_process_execution_lock(&args));
        }
    }

    #[test]
    fn phase5h14a_execution_lock_covers_two_way_metadata_refresh() {
        let args = ["sync", "roots", "refresh-two-way-metadata", "--approve"]
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        assert!(cli_requires_cross_process_execution_lock(&args));
    }

    #[test]
    fn phase5h12_execution_lock_covers_confirmed_folder_create_settlement() {
        let args = [
            "sync",
            "roots",
            "settle-confirmed-folder-create",
            "--approve",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
        assert!(cli_requires_cross_process_execution_lock(&args));
    }

    #[test]
    fn phase5h10_execution_lock_covers_folder_create_confirmation() {
        let args = ["sync", "roots", "confirm-folder-create", "--approve"]
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        assert!(cli_requires_cross_process_execution_lock(&args));
    }

    #[test]
    fn phase5h9_execution_lock_covers_folder_create_submission_and_recovery() {
        for args in [
            ["sync", "roots", "submit-folder-create", "--approve"],
            [
                "sync",
                "roots",
                "recover-folder-create-submission",
                "--approve",
            ],
        ] {
            let args = args.into_iter().map(str::to_owned).collect::<Vec<_>>();
            assert!(cli_requires_cross_process_execution_lock(&args));
        }
    }

    #[test]
    fn phase5h8_execution_lock_covers_folder_create_recovery_plan() {
        let args = ["sync", "roots", "folder-create-recovery-plan", "--approve"]
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        assert!(cli_requires_cross_process_execution_lock(&args));
    }

    #[test]
    fn phase5h6_execution_lock_covers_create_id_allocation() {
        let args = ["sync", "roots", "allocate-create-ids", "--approve"]
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        assert!(cli_requires_cross_process_execution_lock(&args));
    }

    #[test]
    fn phase5h5_execution_lock_covers_two_way_mode_gate_changes() {
        for args in [
            ["sync", "roots", "activate-two-way", "--approve"],
            ["sync", "roots", "deactivate-two-way", "--approve"],
        ] {
            let args = args.into_iter().map(str::to_owned).collect::<Vec<_>>();
            assert!(cli_requires_cross_process_execution_lock(&args));
        }
    }

    #[test]
    fn phase5h4_fullsync_scope_matching_is_exact_and_fail_closed() {
        assert!(oauth_scope_contains(
            Some("openid email https://www.googleapis.com/auth/drive profile"),
            GOOGLE_DRIVE_FULL_SCOPE,
        ));
        assert!(!oauth_scope_contains(
            Some("openid email https://www.googleapis.com/auth/drive.readonly profile"),
            GOOGLE_DRIVE_FULL_SCOPE,
        ));
        assert!(!oauth_scope_contains(
            Some("https://www.googleapis.com/auth/drive.file"),
            GOOGLE_DRIVE_FULL_SCOPE,
        ));
        assert!(!oauth_scope_contains(None, GOOGLE_DRIVE_FULL_SCOPE));
    }

    #[test]
    fn phase5h4_fullsync_refresh_key_is_distinct_from_receive_only_key() {
        let readonly = refresh_token_key("subject").unwrap();
        let fullsync = fullsync_refresh_token_key("subject").unwrap();
        assert_ne!(readonly, fullsync);
        assert_eq!(readonly.purpose, REFRESH_TOKEN_PURPOSE);
        assert_eq!(fullsync.purpose, FULLSYNC_REFRESH_TOKEN_PURPOSE);
    }

    #[test]
    fn phase5h4_execution_lock_covers_fullsync_upgrade() {
        let args = ["auth", "google", "upgrade-full-sync", "--approve"]
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        assert!(cli_requires_cross_process_execution_lock(&args));
    }

    #[test]
    fn phase5h3_execution_lock_policy_covers_remote_write_planner() {
        let args = ["sync", "roots", "remote-write-plan", "--approve"]
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        assert!(cli_requires_cross_process_execution_lock(&args));
    }

    #[test]
    fn phase5h2_execution_lock_policy_covers_write_authority_observation() {
        let args = ["sync", "roots", "observe-write-authority", "--approve"]
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        assert!(cli_requires_cross_process_execution_lock(&args));
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
    #[error("Google did not grant the exact Drive FullSync scope")]
    GoogleFullSyncScopeNotGranted,
    #[error("Google did not return a fresh refresh token for the FullSync upgrade")]
    GoogleFullSyncRefreshTokenMissing,
    #[error("the ReceiveOnly refresh credential changed during FullSync upgrade")]
    GoogleReadonlyCredentialChangedDuringFullSyncUpgrade,
    #[error("the FullSync refresh credential failed its keyring promotion postcondition")]
    GoogleFullSyncPromotionFailed,
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
    #[error("sync root two-way metadata refresh selection failed")]
    SyncRootTwoWayMetadataRefreshSelectionFailed,
    #[error("sync root two-way metadata refresh requires two_way mode")]
    SyncRootTwoWayMetadataRefreshModeUnsupported,
    #[error("existing remote-write intents block two-way metadata refresh")]
    SyncRootTwoWayMetadataRefreshExistingIntents,
    #[error("local baseline is not ready for two-way metadata refresh")]
    SyncRootTwoWayMetadataRefreshLocalStateNotReady,
    #[error("remote catalog is not ready for two-way metadata refresh")]
    SyncRootTwoWayMetadataRefreshRemoteStateNotReady,
    #[error("two-way metadata refresh exceeded the supervised page limit")]
    SyncRootTwoWayMetadataRefreshPageLimitExceeded,
    #[error("two-way metadata refresh changed local baseline or pending events")]
    SyncRootTwoWayMetadataRefreshLocalStateChanged,
    #[error("two-way metadata refresh left an open change window")]
    SyncRootTwoWayMetadataRefreshWindowNotCleared,
    #[error("sync root write-authority observation selection failed")]
    SyncRootWriteAuthoritySelectionFailed,
    #[error("sync root catalog is not ready for write-authority observation")]
    SyncRootWriteAuthorityCatalogNotReady,
    #[error("sync root catalog is not current with the provider change boundary")]
    SyncRootWriteAuthorityCatalogNotCurrent,
    #[error("sync root write-authority observation exceeded the safety item limit")]
    SyncRootWriteAuthorityObservationLimitExceeded,
    #[error("sync root write-authority observation found a duplicate remote identifier")]
    SyncRootWriteAuthorityDuplicateRemoteId,
    #[error("Google Drive changed during write-authority observation")]
    SyncRootWriteAuthorityRemoteChangedDuringObservation,
    #[error("sync root write-authority snapshot persistence did not match observation")]
    SyncRootWriteAuthorityPersistenceMismatch,
    #[error("sync root remote-write planner selection failed")]
    SyncRootRemoteWritePlanSelectionFailed,
    #[error("sync root create-ID allocation selection failed")]
    SyncRootCreateIdAllocationSelectionFailed,
    #[error("sync root folder-create recovery-plan selection failed")]
    SyncRootFolderCreateRecoverySelectionFailed,
    #[error("sync root folder-create submission selection failed")]
    SyncRootFolderCreateSubmissionSelectionFailed,
    #[error("sync root confirmed folder-create settlement selection failed")]
    SyncRootFolderCreateSettlementSelectionFailed,
    #[error("sync root confirmed folder-create settlement requires two_way mode")]
    SyncRootFolderCreateSettlementModeUnsupported,
    #[error("sync root confirmed folder-create settlement postcondition failed")]
    SyncRootFolderCreateSettlementPostconditionFailed,
    #[error("sync root folder-create confirmation selection failed")]
    SyncRootFolderCreateConfirmationSelectionFailed,
    #[error("sync root folder-create confirmation requires two_way mode")]
    SyncRootFolderCreateConfirmationModeUnsupported,
    #[error("sync root folder-create confirmation execution state mismatched")]
    SyncRootFolderCreateConfirmationStateMismatch,
    #[error("sync root folder-create confirmation pre-submit fence is missing")]
    SyncRootFolderCreateConfirmationFenceMissing,
    #[error("sync root folder-create confirmation fence mismatched")]
    SyncRootFolderCreateConfirmationFenceMismatch,
    #[error("sync root folder-create confirmation local authority changed")]
    SyncRootFolderCreateConfirmationLocalStateChanged,
    #[error("sync root folder-create confirmation exceeded the supervised page limit")]
    SyncRootFolderCreateConfirmationPageLimitExceeded,
    #[error("sync root folder-create confirmation change window mismatched")]
    SyncRootFolderCreateConfirmationWindowMismatch,
    #[error("sync root folder-create submission requires two_way mode")]
    SyncRootFolderCreateSubmissionModeUnsupported,
    #[error("sync root folder-create durable remote root is missing")]
    SyncRootFolderCreateRemoteRootMissing,
    #[error("sync root folder-create parent authority no longer matches")]
    SyncRootFolderCreateParentAuthorityMismatch,
    #[error("sync root folder-create parent topology is no longer valid")]
    SyncRootFolderCreateParentTopologyMismatch,
    #[error("sync root folder-create remote cursor fence no longer matches")]
    SyncRootFolderCreateRemoteFenceMismatch,
    #[error("sync root folder-create recovery-plan requires two_way mode")]
    SyncRootFolderCreateRecoveryModeUnsupported,
    #[error("sync root create-ID allocation requires two_way mode")]
    SyncRootCreateIdAllocationModeUnsupported,
    #[error("sync root create-ID allocation write gates are not satisfied")]
    SyncRootCreateIdAllocationWriteGatesNotSatisfied,
    #[error("sync root create-ID allocation plan contains blocked/conflict entries")]
    SyncRootCreateIdAllocationPlanBlocked,
    #[error("sync root create-ID allocation requires a durable remote fence")]
    SyncRootCreateIdAllocationRemoteFenceMissing,
    #[error("sync root create-ID allocation remote fence is stale")]
    SyncRootCreateIdAllocationRemoteFenceMismatch,
    #[error("Google Drive changed during create-ID allocation")]
    SyncRootCreateIdAllocationRemoteChanged,
    #[error("Google Drive generated-ID count mismatched eligible candidates")]
    SyncRootCreateIdAllocationCountMismatch,
    #[error("local create-intent plan changed during ID allocation")]
    SyncRootCreateIdAllocationPlanChanged,
    #[error("create-intent persistence failed its durable postcondition")]
    SyncRootCreateIdAllocationPersistenceMismatch,
    #[error("sync root two-way activation selection failed")]
    SyncRootTwoWayActivationSelectionFailed,
    #[error("sync root mode is not eligible for two-way activation")]
    SyncRootTwoWayActivationModeUnsupported,
    #[error("receive-only state is not fully converged")]
    SyncRootTwoWayActivationReceiveOnlyNotConverged,
    #[error("local baseline is not ready for two-way activation")]
    SyncRootTwoWayActivationLocalStateNotReady,
    #[error("local change journal is not clean for two-way activation")]
    SyncRootTwoWayActivationLocalJournalNotClean,
    #[error("remote catalog is not ready for two-way activation")]
    SyncRootTwoWayActivationRemoteStateNotReady,
    #[error("remote write-authority snapshot is not current for two-way activation")]
    SyncRootTwoWayActivationAuthorityNotReady,
    #[error("existing remote-write intents block two-way activation")]
    SyncRootTwoWayActivationExistingIntents,
    #[error("stored FullSync refresh token is missing")]
    MissingStoredFullSyncRefreshToken,
    #[error("remote-write planner is not clean for two-way activation")]
    SyncRootTwoWayActivationPlannerNotClean,
    #[error("two-way mode compare-and-set failed")]
    SyncRootTwoWayActivationCompareAndSetFailed,
    #[error("two-way mode activation postcondition failed")]
    SyncRootTwoWayActivationPostconditionFailed,
    #[error("sync root reconciliation-plan selection failed")]
    SyncRootReconcilePlanSelectionFailed,
    #[error("selected receive-only file is not ready for safe replacement")]
    SyncRootReplacementNotReady,
    #[error("selected receive-only file is not ready for safe deletion")]
    SyncRootDeletionNotReady,
    #[error("selected receive-only directory is not ready for safe deletion")]
    SyncRootDirectoryDeletionNotReady,
    #[error("sync root metadata-step currently supports only receive_only roots")]
    SyncRootMetadataStepModeUnsupported,
    #[error("configured sync root does not have a remote root identifier")]
    SyncRootInventoryRemoteRootMissing,
    #[error("sync root inventory pagination repeated a continuation token")]
    SyncRootInventoryPaginationLoop,
    #[error("sync root inventory pagination exceeded the safety limit")]
    SyncRootInventoryPageLimitExceeded,
}
