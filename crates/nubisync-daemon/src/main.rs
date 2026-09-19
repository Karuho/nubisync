//! NubiSync background daemon entry point.

#![forbid(unsafe_code)]

use nubisync_auth::{KeyringSecretStore, SecretKey, SecretStore, SecretValue};
use nubisync_core::{ProviderAccount, ProviderId, SyncMode, SyncRoot};
use nubisync_daemon::{
    SelectedRootExecutionBusyScope, SelectedRootPeriodicLocalObservation,
    SelectedRootReceiveOnlyPeriodicDecision, SelectedRootReceiveOnlyPeriodicState,
    SelectedRootReceiveOnlyPeriodicTick, execute_selected_root_receive_only_periodic_tick,
};
use nubisync_drive::{GOOGLE_DRIVE_READONLY_SCOPE, GoogleDriveApi, GoogleOAuthConfig, OAuthError};
use nubisync_storage::Storage;
use std::{
    env,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;

const REFRESH_TOKEN_PURPOSE: &str = "refresh-token";
const OAUTH_CLIENT_SUBJECT: &str = "oauth-desktop-client";
const OAUTH_CLIENT_ID_PURPOSE: &str = "client-id";
const OAUTH_CLIENT_SECRET_PURPOSE: &str = "client-secret";
const ACCESS_TOKEN_REFRESH_SAFETY_SECONDS: u64 = 60;
const INTERRUPTIBLE_SLEEP_SLICE_MS: u64 = 250;
const MAX_TEST_TICKS: usize = 10_000;

fn main() {
    if let Err(error) = run() {
        eprintln!("NUBISYNCD=FAIL");
        eprintln!("ERROR_CLASS={}", error.code());
        eprintln!("ROOT_PATH_PRINTED=no");
        eprintln!("LOCAL_NAMES_PRINTED=no");
        eprintln!("REMOTE_ROOT_ID_PRINTED=no");
        eprintln!("REMOTE_METADATA_PRINTED=no");
        eprintln!("TOKEN_VALUES_PRINTED=no");
        eprintln!("DRIVE_WRITE_ACCESS=no");
        std::process::exit(1);
    }
}

fn run() -> Result<(), DaemonError> {
    match parse_command(env::args().skip(1))? {
        DaemonCommand::Version => {
            println!("nubisyncd {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        DaemonCommand::Help => {
            print_help();
            Ok(())
        }
        DaemonCommand::Run { max_ticks } => run_periodic_daemon(max_ticks),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DaemonCommand {
    Help,
    Version,
    Run { max_ticks: Option<usize> },
}

fn parse_command(args: impl Iterator<Item = String>) -> Result<DaemonCommand, DaemonError> {
    let args = args.collect::<Vec<_>>();
    match args.as_slice() {
        [] => Ok(DaemonCommand::Help),
        [arg] if matches!(arg.as_str(), "help" | "--help" | "-h") => Ok(DaemonCommand::Help),
        [arg] if matches!(arg.as_str(), "--version" | "-V") => Ok(DaemonCommand::Version),
        [run] if run == "run" => Ok(DaemonCommand::Run { max_ticks: None }),
        [run, flag, value] if run == "run" && flag == "--max-ticks" => {
            let ticks = value
                .parse::<usize>()
                .map_err(|_| DaemonError::InvalidMaxTicks)?;
            if ticks == 0 || ticks > MAX_TEST_TICKS {
                return Err(DaemonError::InvalidMaxTicks);
            }
            Ok(DaemonCommand::Run {
                max_ticks: Some(ticks),
            })
        }
        _ => Err(DaemonError::InvalidArguments),
    }
}

fn print_help() {
    println!("NubiSync daemon");
    println!();
    println!("USAGE:");
    println!("  nubisyncd run");
    println!("  nubisyncd run --max-ticks <1..10000>");
    println!("  nubisyncd --version");
    println!();
    println!("MODE=receive_only");
    println!("POLL_INTERVAL_SECONDS=30");
    println!("SINGLE_FLIGHT_SCOPE=in_process");
    println!("CROSS_PROCESS_LOCK_SCOPE=user_global_sync_execution");
    println!("CROSS_PROCESS_LOCK_LIFETIME=per_tick");
    println!("EXECUTION_LOCK_PATH_PRINTED=no");
    println!("DRIVE_WRITE_ACCESS=no");
}

struct DriveSession {
    api: GoogleDriveApi,
    refresh_at_unix_ms: i64,
}

struct DaemonRuntimeConfig {
    account: ProviderAccount,
    root: SyncRoot,
    oauth: GoogleOAuthConfig,
    client_secret: String,
    refresh_key: SecretKey,
    refresh_token: String,
    keyring: KeyringSecretStore,
}

fn run_periodic_daemon(max_ticks: Option<usize>) -> Result<(), DaemonError> {
    let db_path = nubisync_database_path()?;
    if !db_path.exists() {
        return Err(DaemonError::NoLocalGoogleAccount);
    }

    let mut storage = Storage::open(&db_path)?;
    let mut config = load_runtime_config(&storage)?;
    let mut session: Option<DriveSession> = None;
    let now = unix_time_ms()?;
    let mut scheduler = SelectedRootReceiveOnlyPeriodicState::new_immediate(now);
    let execution_lock_path = nubisync_execution_lock_path()?;
    let shutdown_requested = install_shutdown_handler()?;
    let mut executed_ticks = 0usize;

    println!("NUBISYNCD=STARTED");
    println!("MODE=receive_only");
    println!("PERIODIC_SCHEDULER=yes");
    println!("POLL_INTERVAL_SECONDS=30");
    println!("SINGLE_FLIGHT_SCOPE=in_process");
    println!("CROSS_PROCESS_LOCK_SCOPE=user_global_sync_execution");
    println!("CROSS_PROCESS_LOCK_LIFETIME=per_tick");
    println!("EXECUTION_LOCK_PATH_PRINTED=no");
    println!("ACCESS_TOKEN_STORAGE=memory_only");
    println!("REFRESH_TOKEN_STORAGE=OS_KEYRING");
    println!("ROOT_PATH_PRINTED=no");
    println!("LOCAL_NAMES_PRINTED=no");
    println!("REMOTE_ROOT_ID_PRINTED=no");
    println!("REMOTE_METADATA_PRINTED=no");
    println!("TOKEN_VALUES_PRINTED=no");
    println!("DRIVE_WRITE_ACCESS=no");

    loop {
        if shutdown_requested.load(Ordering::SeqCst) {
            scheduler.request_shutdown();
        }

        if max_ticks.is_some_and(|limit| executed_ticks >= limit) {
            scheduler.request_shutdown();
        }

        let now = unix_time_ms()?;
        match scheduler.decision(now) {
            SelectedRootReceiveOnlyPeriodicDecision::Waiting { delay_ms } => {
                if sleep_interruptibly(delay_ms, &shutdown_requested)? {
                    scheduler.request_shutdown();
                }
                continue;
            }
            SelectedRootReceiveOnlyPeriodicDecision::PausedForManualIntervention => {
                println!("NUBISYNCD=PAUSED");
                println!("REASON=manual_intervention_required");
                println!("EXECUTED_TICKS={executed_ticks}");
                println!("DRIVE_WRITE_ACCESS=no");
                return Ok(());
            }
            SelectedRootReceiveOnlyPeriodicDecision::Shutdown => {
                println!("NUBISYNCD=STOPPED");
                println!(
                    "STOP_REASON={}",
                    shutdown_stop_reason(shutdown_requested.load(Ordering::SeqCst))
                );
                println!("EXECUTED_TICKS={executed_ticks}");
                println!("DRIVE_WRITE_ACCESS=no");
                return Ok(());
            }
            SelectedRootReceiveOnlyPeriodicDecision::Due => {}
        }

        if session
            .as_ref()
            .is_none_or(|current| now >= current.refresh_at_unix_ms)
        {
            match refresh_drive_session(&mut config, now) {
                Ok(refreshed) => {
                    session = Some(refreshed);
                    println!("NUBISYNCD_AUTH=REFRESHED");
                    println!("ACCOUNT_SUBJECT_MATCH=yes");
                    println!("TOKEN_VALUES_PRINTED=no");
                }
                Err(_) => {
                    scheduler.record_external_failure(now);
                    println!("NUBISYNCD_TICK=RETRY");
                    println!("REASON=oauth_or_identity_refresh_failed");
                    println!("TOKEN_VALUES_PRINTED=no");
                    println!("DRIVE_WRITE_ACCESS=no");
                    continue;
                }
            }
        }

        let api = &session
            .as_ref()
            .ok_or(DaemonError::DriveSessionUnavailable)?
            .api;

        match execute_selected_root_receive_only_periodic_tick(
            &mut scheduler,
            api,
            &mut storage,
            &config.root,
            &execution_lock_path,
            now,
        ) {
            Ok(SelectedRootReceiveOnlyPeriodicTick::Executed {
                execution,
                local_observation,
                next_delay_ms,
            }) => {
                executed_ticks = executed_ticks
                    .checked_add(1)
                    .ok_or(DaemonError::NumericOverflow)?;

                println!("NUBISYNCD_TICK=PASS");
                println!("CROSS_PROCESS_LOCK=acquired");
                println!("EXECUTION_LOCK_PATH_PRINTED=no");
                println!("EXECUTED_TICKS={executed_ticks}");
                println!("ROUNDS_EXECUTED={}", execution.rounds_executed);
                println!("METADATA_ROUNDS={}", execution.metadata_rounds);
                println!(
                    "CONVERGENCE_ONLY_ROUNDS={}",
                    execution.convergence_only_rounds
                );
                println!(
                    "CONVERGENCE_PHASES_EXECUTED={}",
                    execution.convergence_phases_executed
                );
                println!("FINAL_ACTIONS={}", execution.final_actions);
                println!("FINAL_BLOCKED_ACTIONS={}", execution.final_blocked_actions);
                println!("CONVERGED={}", yes_no(execution.converged));
                println!(
                    "REQUIRES_ANOTHER_INVOCATION={}",
                    yes_no(execution.requires_another_invocation)
                );
                println!(
                    "MANUAL_INTERVENTION_REQUIRED={}",
                    yes_no(execution.manual_intervention_required)
                );
                println!("STOP_REASON={}", execution.stop_reason.as_str());
                println!(
                    "NEXT_DELAY_MS={}",
                    next_delay_ms
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "none".to_owned())
                );

                match local_observation {
                    SelectedRootPeriodicLocalObservation::Journaled(observation) => {
                        println!("LOCAL_OBSERVATION=PASS");
                        println!("LOCAL_OBSERVATION_STATUS=journaled");
                        println!("LOCAL_BASELINE_READY=yes");
                        println!(
                            "LOCAL_BASELINE_GENERATION={}",
                            observation.baseline_generation
                        );
                        println!("LOCAL_CHANGES_TOTAL={}", observation.changes_total);
                        println!("LOCAL_CREATED={}", observation.created);
                        println!("LOCAL_DELETED={}", observation.deleted);
                        println!("LOCAL_MODIFIED={}", observation.modified);
                        println!("LOCAL_TYPE_CHANGED={}", observation.type_changed);
                        println!("LOCAL_PENDING_EVENTS={}", observation.pending_events);
                        println!("LOCAL_SUPERSEDED_EVENTS={}", observation.superseded_events);
                        println!("LOCAL_DATABASE_MUTATION=yes");
                        println!("LOCAL_FILESYSTEM_READ=metadata_only");
                        println!("LOCAL_BASELINE_MUTATED=no");
                    }
                    SelectedRootPeriodicLocalObservation::BaselineMissing => {
                        println!("LOCAL_OBSERVATION=SKIPPED");
                        println!("LOCAL_OBSERVATION_STATUS=baseline_missing");
                        println!("LOCAL_BASELINE_READY=no");
                        println!("LOCAL_DATABASE_MUTATION=no");
                        println!("LOCAL_FILESYSTEM_READ=not_performed");
                        println!("LOCAL_BASELINE_MUTATED=no");
                    }
                    SelectedRootPeriodicLocalObservation::BaselineInvalidated => {
                        println!("LOCAL_OBSERVATION=SKIPPED");
                        println!("LOCAL_OBSERVATION_STATUS=baseline_invalidated");
                        println!("LOCAL_BASELINE_READY=no");
                        println!("LOCAL_REBASELINE_REQUIRED=yes");
                        println!("LOCAL_DATABASE_MUTATION=no");
                        println!("LOCAL_FILESYSTEM_READ=not_performed");
                        println!("LOCAL_BASELINE_MUTATED=no");
                    }
                    SelectedRootPeriodicLocalObservation::DeferredUntilReceiveOnlyConverged => {
                        println!("LOCAL_OBSERVATION=SKIPPED");
                        println!("LOCAL_OBSERVATION_STATUS=deferred_until_receive_only_converged");
                        println!("LOCAL_BASELINE_READY=unknown");
                        println!("LOCAL_DATABASE_MUTATION=no");
                        println!("LOCAL_FILESYSTEM_READ=not_performed");
                        println!("LOCAL_BASELINE_MUTATED=no");
                    }
                }

                println!("ROOT_PATH_PRINTED=no");
                println!("LOCAL_NAMES_PRINTED=no");
                println!("REMOTE_ROOT_ID_PRINTED=no");
                println!("REMOTE_METADATA_PRINTED=no");
                println!("TOKEN_VALUES_PRINTED=no");
                println!("DRIVE_WRITE_ACCESS=no");
            }
            Ok(SelectedRootReceiveOnlyPeriodicTick::Busy {
                retry_after_ms,
                scope,
            }) => {
                println!("NUBISYNCD_TICK=BUSY");
                println!("BUSY_SCOPE={}", scope.as_str());
                println!(
                    "CROSS_PROCESS_LOCK={}",
                    if scope == SelectedRootExecutionBusyScope::CrossProcess {
                        "busy"
                    } else {
                        "acquired"
                    }
                );
                println!("EXECUTION_LOCK_PATH_PRINTED=no");
                println!("RETRY_AFTER_MS={retry_after_ms}");
                println!("SYNC_EXECUTION=not_started");
                println!("DRIVE_WRITE_ACCESS=no");
            }
            Ok(SelectedRootReceiveOnlyPeriodicTick::Waiting { delay_ms }) => {
                if sleep_interruptibly(delay_ms, &shutdown_requested)? {
                    scheduler.request_shutdown();
                }
            }
            Ok(SelectedRootReceiveOnlyPeriodicTick::PausedForManualIntervention) => {
                println!("NUBISYNCD=PAUSED");
                println!("REASON=manual_intervention_required");
                println!("EXECUTED_TICKS={executed_ticks}");
                println!("DRIVE_WRITE_ACCESS=no");
                return Ok(());
            }
            Ok(SelectedRootReceiveOnlyPeriodicTick::Shutdown) => {
                println!("NUBISYNCD=STOPPED");
                println!("STOP_REASON=scheduler_shutdown");
                println!("EXECUTED_TICKS={executed_ticks}");
                println!("DRIVE_WRITE_ACCESS=no");
                return Ok(());
            }
            Err(_) => {
                println!("NUBISYNCD_TICK=RETRY");
                println!("REASON=receive_only_executor_failed");
                println!("ROOT_PATH_PRINTED=no");
                println!("LOCAL_NAMES_PRINTED=no");
                println!("REMOTE_ROOT_ID_PRINTED=no");
                println!("REMOTE_METADATA_PRINTED=no");
                println!("TOKEN_VALUES_PRINTED=no");
                println!("DRIVE_WRITE_ACCESS=no");
            }
        }
    }
}

fn load_runtime_config(storage: &Storage) -> Result<DaemonRuntimeConfig, DaemonError> {
    if !KeyringSecretStore::is_available() {
        return Err(DaemonError::KeyringUnavailable);
    }

    let provider = ProviderId::new("google-drive")?;
    let account = single_google_account(storage.list_accounts(&provider)?)?;
    let roots = storage.list_sync_roots(&provider, &account.subject)?;

    let root = match roots.len() {
        0 => return Err(DaemonError::NoConfiguredRoot),
        1 => roots
            .into_iter()
            .next()
            .ok_or(DaemonError::NoConfiguredRoot)?,
        _ => return Err(DaemonError::MultipleRootsUnsupported),
    };

    if root.mode != SyncMode::ReceiveOnly {
        return Err(DaemonError::SyncRootModeUnsupported);
    }

    let keyring = KeyringSecretStore::default();
    let (client_id_key, client_secret_key) = google_client_config_keys()?;
    let client_id = required_secret_utf8(
        keyring.get(&client_id_key)?,
        DaemonError::MissingStoredGoogleClientConfig,
    )?;
    let client_secret = required_secret_utf8(
        keyring.get(&client_secret_key)?,
        DaemonError::MissingStoredGoogleClientConfig,
    )?;

    let refresh_key = refresh_token_key(&account.subject)?;
    let refresh_token = required_secret_utf8(
        keyring.get(&refresh_key)?,
        DaemonError::MissingStoredRefreshToken,
    )?;

    Ok(DaemonRuntimeConfig {
        account,
        root,
        oauth: GoogleOAuthConfig::new(client_id)?,
        client_secret,
        refresh_key,
        refresh_token,
        keyring,
    })
}

fn refresh_drive_session(
    config: &mut DaemonRuntimeConfig,
    now_unix_ms: i64,
) -> Result<DriveSession, DaemonError> {
    let tokens = config
        .oauth
        .refresh_access_token(&config.refresh_token, &config.client_secret)?;

    if let Some(scope) = tokens.scope()
        && !oauth_scope_contains(Some(scope), GOOGLE_DRIVE_READONLY_SCOPE)
    {
        return Err(DaemonError::GoogleReadonlyScopeNotGranted);
    }

    if let Some(rotated) = tokens.refresh_token() {
        let rotated_bytes = rotated.as_bytes().to_vec();
        let rotated_text = String::from_utf8(rotated_bytes.clone())
            .map_err(|_| DaemonError::InvalidStoredSecret)?;
        config
            .keyring
            .put(&config.refresh_key, SecretValue::new(rotated_bytes)?)?;
        config.refresh_token = rotated_text;
    }

    let api = GoogleDriveApi::new(tokens.access_token().clone())?;
    let user = api.user_info()?;
    if user.sub != config.account.subject {
        return Err(DaemonError::GoogleAccountMismatch);
    }

    let refresh_after_seconds = safe_access_token_refresh_after(tokens.expires_in_seconds());
    let refresh_after_ms = i64::try_from(refresh_after_seconds)
        .map_err(|_| DaemonError::ClockOverflow)?
        .checked_mul(1_000)
        .ok_or(DaemonError::ClockOverflow)?;

    Ok(DriveSession {
        api,
        refresh_at_unix_ms: now_unix_ms
            .checked_add(refresh_after_ms)
            .ok_or(DaemonError::ClockOverflow)?,
    })
}

fn safe_access_token_refresh_after(expires_in_seconds: u64) -> u64 {
    if expires_in_seconds > ACCESS_TOKEN_REFRESH_SAFETY_SECONDS * 2 {
        expires_in_seconds - ACCESS_TOKEN_REFRESH_SAFETY_SECONDS
    } else {
        (expires_in_seconds / 2).max(1)
    }
}

fn oauth_scope_contains(scope: Option<&str>, required_scope: &str) -> bool {
    scope.is_some_and(|value| {
        value
            .split_ascii_whitespace()
            .any(|candidate| candidate == required_scope)
    })
}

fn google_client_config_keys() -> Result<(SecretKey, SecretKey), DaemonError> {
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

fn refresh_token_key(account_subject: &str) -> Result<SecretKey, DaemonError> {
    Ok(SecretKey::new(
        "google-drive",
        account_subject,
        REFRESH_TOKEN_PURPOSE,
    )?)
}

fn required_secret_utf8(
    secret: Option<SecretValue>,
    missing_error: DaemonError,
) -> Result<String, DaemonError> {
    let secret = secret.ok_or(missing_error)?;
    String::from_utf8(secret.expose_bytes().to_vec()).map_err(|_| DaemonError::InvalidStoredSecret)
}

fn single_google_account(accounts: Vec<ProviderAccount>) -> Result<ProviderAccount, DaemonError> {
    match accounts.len() {
        0 => Err(DaemonError::NoLocalGoogleAccount),
        1 => accounts
            .into_iter()
            .next()
            .ok_or(DaemonError::NoLocalGoogleAccount),
        _ => Err(DaemonError::MultipleGoogleAccountsUnsupported),
    }
}

fn nubisync_database_path() -> Result<PathBuf, DaemonError> {
    Ok(nubisync_data_dir()?.join("nubisync.db"))
}

fn nubisync_execution_lock_path() -> Result<PathBuf, DaemonError> {
    Ok(nubisync_data_dir()?.join("execution.lock"))
}

fn nubisync_data_dir() -> Result<PathBuf, DaemonError> {
    if let Some(path) = env::var_os("XDG_DATA_HOME") {
        return Ok(PathBuf::from(path).join("nubisync"));
    }

    let home = env::var_os("HOME").ok_or(DaemonError::HomeDirectoryUnavailable)?;
    Ok(PathBuf::from(home)
        .join(".local")
        .join("share")
        .join("nubisync"))
}

fn unix_time_ms() -> Result<i64, DaemonError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| DaemonError::ClockBeforeUnixEpoch)?;

    i64::try_from(duration.as_millis()).map_err(|_| DaemonError::ClockOverflow)
}

fn install_shutdown_handler() -> Result<Arc<AtomicBool>, DaemonError> {
    let shutdown_requested = Arc::new(AtomicBool::new(false));
    let handler_flag = Arc::clone(&shutdown_requested);

    ctrlc::set_handler(move || {
        handler_flag.store(true, Ordering::SeqCst);
    })?;

    Ok(shutdown_requested)
}

fn sleep_interruptibly(
    delay_ms: i64,
    shutdown_requested: &AtomicBool,
) -> Result<bool, DaemonError> {
    let mut remaining = u64::try_from(delay_ms).map_err(|_| DaemonError::InvalidSchedulerDelay)?;

    while remaining != 0 {
        if shutdown_requested.load(Ordering::SeqCst) {
            return Ok(true);
        }

        let slice = remaining.min(INTERRUPTIBLE_SLEEP_SLICE_MS);
        thread::sleep(Duration::from_millis(slice));
        remaining -= slice;
    }

    Ok(shutdown_requested.load(Ordering::SeqCst))
}

fn shutdown_stop_reason(signal_requested: bool) -> &'static str {
    if signal_requested {
        "signal_requested"
    } else {
        "max_ticks_reached"
    }
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

#[derive(Debug, Error)]
enum DaemonError {
    #[error("invalid daemon arguments")]
    InvalidArguments,
    #[error("max ticks is invalid")]
    InvalidMaxTicks,
    #[error("HOME is unavailable")]
    HomeDirectoryUnavailable,
    #[error("system clock is before Unix epoch")]
    ClockBeforeUnixEpoch,
    #[error("clock value overflowed")]
    ClockOverflow,
    #[error("scheduler delay is invalid")]
    InvalidSchedulerDelay,
    #[error("numeric counter overflowed")]
    NumericOverflow,
    #[error("keyring is unavailable")]
    KeyringUnavailable,
    #[error("no local Google account is configured")]
    NoLocalGoogleAccount,
    #[error("multiple Google accounts are unsupported")]
    MultipleGoogleAccountsUnsupported,
    #[error("no sync root is configured")]
    NoConfiguredRoot,
    #[error("multiple sync roots are unsupported by this daemon phase")]
    MultipleRootsUnsupported,
    #[error("configured sync root mode is unsupported")]
    SyncRootModeUnsupported,
    #[error("stored OAuth client configuration is missing")]
    MissingStoredGoogleClientConfig,
    #[error("stored refresh token is missing")]
    MissingStoredRefreshToken,
    #[error("stored secret is invalid")]
    InvalidStoredSecret,
    #[error("Google did not grant Drive read-only scope")]
    GoogleReadonlyScopeNotGranted,
    #[error("refreshed Google identity does not match configured account")]
    GoogleAccountMismatch,
    #[error("Drive session is unavailable")]
    DriveSessionUnavailable,
    #[error("process signal handler is unavailable")]
    SignalHandler(#[from] ctrlc::Error),
    #[error("core operation failed")]
    Core(#[from] nubisync_core::CoreError),
    #[error("secret-store operation failed")]
    Secrets(#[from] nubisync_auth::SecretStoreError),
    #[error("storage operation failed")]
    Storage(#[from] nubisync_storage::StorageError),
    #[error("OAuth operation failed")]
    OAuth(#[from] OAuthError),
    #[error("Drive operation failed")]
    Drive(#[from] nubisync_drive::DriveApiError),
    #[error("receive-only executor failed")]
    Executor(#[from] nubisync_daemon::SelectedRootExecutorError),
}

impl DaemonError {
    fn code(&self) -> &'static str {
        match self {
            Self::InvalidArguments => "invalid_arguments",
            Self::InvalidMaxTicks => "invalid_max_ticks",
            Self::HomeDirectoryUnavailable => "home_unavailable",
            Self::ClockBeforeUnixEpoch | Self::ClockOverflow => "clock_error",
            Self::InvalidSchedulerDelay => "invalid_scheduler_delay",
            Self::NumericOverflow => "numeric_overflow",
            Self::KeyringUnavailable => "keyring_unavailable",
            Self::NoLocalGoogleAccount => "no_local_google_account",
            Self::MultipleGoogleAccountsUnsupported => "multiple_google_accounts_unsupported",
            Self::NoConfiguredRoot => "no_configured_root",
            Self::MultipleRootsUnsupported => "multiple_roots_unsupported",
            Self::SyncRootModeUnsupported => "sync_root_mode_unsupported",
            Self::MissingStoredGoogleClientConfig => "oauth_client_config_missing",
            Self::MissingStoredRefreshToken => "refresh_token_missing",
            Self::InvalidStoredSecret => "stored_secret_invalid",
            Self::GoogleReadonlyScopeNotGranted => "drive_readonly_scope_missing",
            Self::GoogleAccountMismatch => "google_account_mismatch",
            Self::DriveSessionUnavailable => "drive_session_unavailable",
            Self::SignalHandler(_) => "signal_handler_unavailable",
            Self::Core(_) => "core_error",
            Self::Secrets(_) => "secret_store_error",
            Self::Storage(_) => "storage_error",
            Self::OAuth(_) => "oauth_error",
            Self::Drive(_) => "drive_error",
            Self::Executor(_) => "receive_only_executor_error",
        }
    }
}

#[cfg(test)]
mod phase5e5_daemon_wiring_tests {
    use super::*;

    #[test]
    fn phase5e5_command_parser_supports_continuous_and_bounded_run() {
        assert_eq!(
            parse_command(["run".to_owned()].into_iter()).unwrap(),
            DaemonCommand::Run { max_ticks: None }
        );
        assert_eq!(
            parse_command(["run".to_owned(), "--max-ticks".to_owned(), "1".to_owned()].into_iter())
                .unwrap(),
            DaemonCommand::Run { max_ticks: Some(1) }
        );
        assert!(matches!(
            parse_command(["run".to_owned(), "--max-ticks".to_owned(), "0".to_owned()].into_iter()),
            Err(DaemonError::InvalidMaxTicks)
        ));
    }

    #[test]
    fn phase5e5_token_refresh_margin_never_extends_beyond_short_token_lifetime() {
        assert_eq!(safe_access_token_refresh_after(3_600), 3_540);
        assert_eq!(safe_access_token_refresh_after(120), 60);
        assert_eq!(safe_access_token_refresh_after(20), 10);
        assert_eq!(safe_access_token_refresh_after(1), 1);
    }

    #[test]
    fn phase5e6_interruptible_sleep_observes_preexisting_shutdown() {
        let flag = AtomicBool::new(true);
        assert!(sleep_interruptibly(30_000, &flag).unwrap());
    }

    #[test]
    fn phase5e6_shutdown_reason_distinguishes_signal_from_bounded_test_stop() {
        assert_eq!(shutdown_stop_reason(true), "signal_requested");
        assert_eq!(shutdown_stop_reason(false), "max_ticks_reached");
    }

    #[test]
    fn phase5e5_scope_matching_is_exact() {
        assert!(oauth_scope_contains(
            Some("openid https://www.googleapis.com/auth/drive.readonly profile"),
            GOOGLE_DRIVE_READONLY_SCOPE,
        ));
        assert!(!oauth_scope_contains(
            Some("https://www.googleapis.com/auth/drive.metadata.readonly"),
            GOOGLE_DRIVE_READONLY_SCOPE,
        ));
        assert!(!oauth_scope_contains(None, GOOGLE_DRIVE_READONLY_SCOPE));
    }
}
