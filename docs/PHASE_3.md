# Phase 3A — Persistent Google OAuth Session

## Objective

Allow NubiSync to restart without forcing the user through browser authorization again.

This subphase deliberately does not implement Drive file synchronization yet.

## Credential model

NubiSync stores:

- Google user refresh token → OS credential store
- development Desktop OAuth client ID → OS credential store
- development Desktop OAuth client secret → OS credential store
- short-lived access token → memory only
- account metadata → SQLite
- Drive change cursor → SQLite

No OAuth credential is stored in telemetry.

## Commands

`nubisync auth google configure`

Stores the development Desktop OAuth client configuration in the OS credential store.

`nubisync auth google status`

Inspects local session state without a network request and without printing credentials.

`nubisync auth google refresh`

Uses the stored refresh token to obtain a new short-lived access token and verifies that OpenID Connect UserInfo returns the same stable Google `sub` stored in SQLite.

`nubisync auth google logout`

Removes the user refresh token while deliberately retaining local SQLite synchronization metadata and the Desktop client configuration.

## Testing-mode limitation

Google OAuth projects with an External consent screen in Testing status can issue refresh tokens that expire after seven days when scopes beyond basic profile identity are requested.

That is acceptable for NubiSync development, but production persistence must be validated again after the OAuth app moves through the appropriate production and verification process.

## Phase 3A exit criteria

- client configuration can be moved from environment variables into the OS credential store
- environment variables can then be unset
- `auth google status` reports a locally connected account without network access
- `auth google refresh` succeeds without browser interaction
- refreshed UserInfo `sub` matches the stored account
- access token remains memory-only
- full workspace tests and Clippy pass

## Persistent client configuration

Phase 3A no longer depends on shell environment variables for the Google Desktop OAuth client.

`nubisync auth google configure` prompts for the Client ID and Client Secret directly. The Client Secret input is hidden. Both values are written to the OS credential store and immediately read back; NubiSync verifies exact byte-for-byte round-trip equality without printing either value.

`auth google login` and `auth google refresh` use only this persisted keyring configuration.

## Phase 3B — Incremental Google Drive change journal

NubiSync now consumes the Google Drive `changes` collection using the durable start-page token already stored in SQLite.

The command is:

`nubisync drive changes`

Behavior:

- refreshes the short-lived Google access token from the OS credential store
- verifies the refreshed Google `sub` matches the SQLite account
- loads the durable Drive cursor from SQLite
- calls `changes.list` with `spaces=drive`, `includeRemoved=true`, and `restrictToMyDrive=true`
- follows every `nextPageToken`
- treats `nextPageToken` as short-lived pagination state only
- accepts `newStartPageToken` only on the final page
- commits the new durable cursor to SQLite only after every page has been parsed successfully
- does not request file content
- does not write to Google Drive
- does not print filenames, Drive IDs, page tokens, or cursor values

The current implementation maps visible file metadata changes into the provider-neutral `RemoteChange` model. Removed entries become `Delete`; current file/folder states become `Upsert`.

For this subphase, `modified_unix_ms` remains unset because timestamp normalization belongs in a later metadata-normalization step.

### Failure behavior

A malformed change page, repeated continuation token, excessive pagination, account mismatch, refresh failure, or missing final checkpoint aborts the run without advancing the durable cursor.

This makes a retry re-read the uncommitted change range instead of silently skipping it.

### Exit criteria

- `nubisync drive changes` succeeds against the real Google account
- a no-change poll safely commits the returned checkpoint
- a manually created or renamed My Drive item produces at least one incremental change on the next poll
- no file content is downloaded
- no Drive write is performed
- no filename, remote ID, OAuth credential, page token, or cursor is printed

## Phase 3C — Durable remote journal and atomic checkpoint

Phase 3B proved that NubiSync can consume real Google Drive changes incrementally. Phase 3C makes that stream crash-safe before any filesystem application logic is introduced.

SQLite schema version 2 adds `remote_events`.

Each Google Drive `RemoteChange` is persisted locally with:

- provider and account ownership
- event kind (`upsert` or `delete`)
- remote item identifier
- parent identifier when available
- local synchronization metadata required later by the reconciler
- file/folder kind
- size when available
- trashed state
- observation timestamp
- lifecycle status, initially `pending`

This metadata is local synchronization state. It is not diagnostics telemetry and is not emitted by `nubisync drive changes`.

### Atomicity invariant

The complete fetched change batch and the final provider cursor are committed in one SQLite transaction.

Therefore:

- if every event insert succeeds, all events and the new cursor become durable together
- if any event insert fails, SQLite rolls back the complete batch and leaves the previous cursor untouched
- a retry cannot silently skip a remote change because of a partially advanced checkpoint

The storage tests include an intentional numeric-overflow failure after a valid first event. The test verifies that the first insert is rolled back and that the previous cursor remains active.

### Phase 3C exit criteria

- schema migrates from version 1 to version 2 on the existing local database
- a real Drive change is persisted as a pending `remote_event`
- the cursor and remote-event batch commit atomically
- no remote filenames, IDs, page tokens, or cursor values are printed
- no file content is downloaded
- no Google Drive write occurs
