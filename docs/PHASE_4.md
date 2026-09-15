# Phase 4 — Remote baseline before local filesystem reconciliation

## Phase 4A — Metadata-only Drive inventory probe

The incremental Drive change feed only contains items that changed after a checkpoint.
It cannot reconstruct the complete pre-existing remote hierarchy by itself.

Before NubiSync creates or modifies local files, Phase 4A deliberately exercises
`files.list` for the first time as a metadata-only inventory probe.

Command:

`nubisync drive inventory`

The probe:

- uses the existing `drive.metadata.readonly` scope
- searches the Drive space for non-trashed items owned by the current user
- follows `nextPageToken` until the inventory is complete
- counts ordinary files and folders
- counts provider-native Google Workspace items and shortcuts separately
- does not persist filenames or Drive IDs yet
- does not change the existing Drive change cursor
- does not modify the durable remote-event journal
- does not request file contents
- performs no Drive write

Provider-native Google Workspace files remain outside the initial ordinary-file
sync scope and will require explicit export/import semantics in a later phase.

Phase 4B will persist the validated ordinary-file/folder inventory through a
crash-safe staging snapshot before any local filesystem reconciliation is enabled.

### Inventory request bounds

The initial Phase 4A runtime probe exposed an observability issue: HTTP requests had no explicit timeout and the CLI emitted no progress until the complete inventory finished.

The Drive API client now uses:

- 10-second connection timeout
- 60-second total request timeout
- 1000 items per metadata inventory page during the probe

`drive inventory` also emits privacy-safe stage/page counters. It still does not print filenames, Drive IDs, cursor values, page tokens, or OAuth credentials.

### Inventory page sizing

The runtime probe showed that the account can legitimately span many inventory pages.
Phase 4A therefore uses Drive's maximum documented `files.list` page size of 1000
to reduce request count while preserving pagination correctness. Progress output
includes cumulative counts so long inventories are visibly advancing rather than
appearing stalled.

### Bounded development probe

Large Drive accounts can contain tens of thousands of objects, so routine
development validation must not require a complete inventory.

The default command is bounded to 20 returned Drive objects:

`nubisync drive inventory`

A custom bounded probe can be requested with:

`nubisync drive inventory --limit <1-10000>`

A complete scan is explicit:

`nubisync drive inventory --full`

A bounded probe reports `INVENTORY_COMPLETE=no` and `LIMIT_REACHED=yes` when
the cap is reached. It does not persist inventory metadata, alter the durable
change cursor, or modify the pending remote-event journal.


## Phase 4B — Durable inventory staging

SQLite schema version 3 separates temporary inventory collection from the
complete authoritative remote snapshot.

Bounded probes stage supported file/folder metadata and then discard that
staging data. They never replace the authoritative snapshot.

Only an explicit `drive inventory --full` run that reaches the end of Drive
pagination may atomically promote staging into `remote_items`.

This keeps routine development tests short while preventing a partial inventory
from ever masquerading as complete state. Provider cursors and pending remote
change events remain untouched.

## Phase 4C — Explicit remote baseline state

An empty `remote_items` table is ambiguous by itself. It can mean either that no
complete inventory has been built yet or that a completed inventory is genuinely
empty.

Schema version 4 adds `remote_inventory_state` with two independent readiness
gates:

- `snapshot_complete`: a full inventory reached the end of pagination and was
  promoted atomically into `remote_items`
- `catchup_complete`: post-snapshot Drive changes have been reconciled onto that
  baseline

A full snapshot promotion sets `snapshot_complete=yes` and deliberately resets
`catchup_complete=no`.

Phase 4C does not mark the catalog ready for filesystem reconciliation.

Offline aggregate status is available with:

`nubisync drive catalog status`

The status command performs no network request and prints no remote metadata.

## Phase 4D — Bootstrap change fence

A full remote inventory is not instantaneous. Files can change while inventory
pages are being collected.

Before an explicit `drive inventory --full` starts its metadata scan, NubiSync
requests a fresh Drive start page token and keeps it as an opaque bootstrap fence.

Bounded development probes do not request or persist this fence.

If and only if the full inventory reaches the end of pagination, snapshot
promotion stores the authoritative snapshot, `snapshot_complete=yes`,
`catchup_complete=no`, the opaque catch-up cursor and the completion timestamp
in the same SQLite transaction.

The normal provider polling cursor is not modified by snapshot promotion.

The bootstrap cursor is local synchronization metadata. It is never printed or
sent as telemetry. `drive catalog status` reports only whether it exists.

## Phase 4E — Inventory/change classification parity

The Drive inventory and incremental change stream now apply the same support
boundary for Google-native objects.

Supported:

- ordinary files
- folders

Ignored on upsert:

- Google Docs/Sheets/Slides and other `application/vnd.google-apps.*` native
  objects
- Drive shortcuts

Removed change records are still represented as provider-neutral deletes because
Drive can omit the metadata needed to classify a removed object. Deleting an ID
that was never present in the supported baseline is idempotent and harmless.

This prevents a later bootstrap catch-up from introducing provider-native objects
that the authoritative inventory intentionally excluded.

## Phase 4F — Transactional catalog catch-up

After a complete inventory snapshot exists, `drive catalog catchup` consumes the
Drive change stream beginning at the bootstrap fence captured before that
inventory.

Catch-up changes are applied directly to the authoritative `remote_items`
catalog. Deletes and trashed items remove entries; ordinary file/folder upserts
replace their metadata idempotently.

The following transition is committed in one SQLite transaction:

- apply the complete catch-up change batch to `remote_items`
- recompute the authoritative item count
- set `catchup_complete=yes`
- advance the normal provider cursor to the catch-up checkpoint
- supersede older pending `remote_events` already represented by the
  snapshot-plus-catch-up baseline

If any catalog mutation fails, none of those transitions commit.

The command checks local catalog state before authentication or network access.
Without a complete snapshot it exits as `SKIPPED` with
`NETWORK_CHECK=not_performed`.

Phase 4F still performs no filesystem mutation and no Drive write.

## Phase 4G — Folder-scoped metadata probe

NubiSync can now perform a bounded metadata-only probe of the direct children of
one Drive folder.

The command is:

`nubisync drive folder probe <remote-folder-id> --limit <1-1000>`

For My Drive's top-level folder, Google Drive accepts the special identifier
`root`, so routine validation can use:

`nubisync drive folder probe root --limit 10`

The provider query is scoped to:

- the selected parent folder
- items owned by the current user
- non-trashed Drive-space items

The probe is intentionally non-recursive and does not persist any inventory,
change cursor, journal event, filename, path, or Drive ID to CLI output.

This is the foundation for a later recursive inventory rooted at a user-selected
remote folder instead of requiring an account-wide My Drive scan.
