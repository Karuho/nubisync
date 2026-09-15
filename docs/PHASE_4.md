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

## Phase 4H — Bounded recursive folder traversal

NubiSync can now walk a selected Drive folder tree recursively while enforcing a
hard total-object limit.

Development command:

`nubisync drive folder tree <remote-folder-id> --limit <1-10000>`

The traversal is breadth-first. Every discovered supported folder is queued for
inspection, while ordinary files are counted and Google-native objects remain
unsupported.

The limit applies to total returned Drive objects, not only supported files.
This keeps routine development probes predictable even for very large accounts.

The recursive probe:

- does not persist inventory
- does not modify the provider cursor
- does not modify the remote event journal
- does not print the selected root ID, child IDs, names, or paths
- does not access file contents
- performs no Drive write

A bounded traversal can report `TRAVERSAL_COMPLETE=no`; that is expected whenever
the hard limit is reached before the selected tree has been exhausted.

This traversal is the provider-side foundation for a durable sync root.

## Phase 4I — Durable sync-root contract

The existing `sync_roots` SQLite table now has a typed provider-neutral domain
contract and storage API.

`SyncMode` lives in `nubisync-core` so persistence and reconciliation share the
same semantics. `nubisync-sync` re-exports it to preserve the existing planner
API.

A `SyncRoot` binds:

- one provider/account
- one local path
- an optional remote root identifier
- one sync mode
- one durable root identifier

Phase 4I adds the offline aggregate-only command:

`nubisync sync roots status`

It does not print local paths or Drive IDs and performs no network or filesystem
mutation.

This phase intentionally does not create a sync root yet. Remote-folder
validation and explicit root registration remain separate operations.

## Phase 4J — Remote sync-root validation

Before a remote folder can become a durable sync root, NubiSync validates that
the selected Drive identifier resolves to a live folder within the currently
supported My Drive ownership scope.

Development command:

`nubisync drive folder validate <remote-folder-id>`

The validation performs one metadata-only `files.get` request and requests only:

- MIME type
- trashed state
- ownership state

It does not request the folder name, path, children, file content, or write
permissions.

The special Drive identifier `root` is accepted as a valid candidate identifier
for My Drive's root folder.

Phase 4J intentionally does not persist a sync root. Registration remains an
explicit later operation once both a remote folder and local directory have been
chosen.

## Phase 4K — Explicit sync-root registration

NubiSync can now explicitly register a durable sync root after both sides have
been validated.

Development command:

`nubisync sync roots add --mode receive_only`

The local directory and Drive folder ID are requested interactively so they are
not placed in shell history by the command itself.

Registration requirements:

- local path must already exist
- local path must be absolute
- local root cannot itself be a symbolic link
- remote root must pass Phase 4J Drive folder validation
- duplicate local roots are rejected
- duplicate remote root identifiers are rejected
- registration is insert-only; an existing root is never overwritten

Only `receive_only` is accepted in this phase. The domain model retains
`two_way` and `mirror_local_to_remote`, but the CLI fails closed for those modes
until transfer/write semantics exist.

Registration writes only NubiSync configuration metadata to SQLite. It does not
scan the tree, download content, mutate the selected local directory, or perform
a Drive write.

## Phase 4L — Registration dry-run and empty-root safety

Sync-root registration now supports a full validation dry-run:

`nubisync sync roots add --mode receive_only --dry-run`

The command uses the same interactive local-path and remote-ID prompts as real
registration and performs the same local and Drive validation, but it never
inserts a `sync_roots` row.

For the current receive-only alpha path, the selected local directory must also
be empty. This avoids mixing a newly established remote baseline with
pre-existing local content before conflict and transfer semantics are ready.

The following local roots are rejected:

- the filesystem root
- the user's home directory itself
- a symlink used as the selected root
- a non-directory
- a non-empty directory

The dry-run performs no local filesystem mutation, no inventory persistence, and
no Drive write.

## Phase 4M — Sync-root-scoped remote catalog storage

Selected-folder synchronization must not share one account-wide authoritative
catalog. NubiSync therefore adds a root-scoped catalog foundation in schema v6.

New durable tables:

- `sync_root_remote_items`
- `sync_root_remote_inventory_staging`
- `sync_root_remote_inventory_state`

Each row is keyed by `sync_root_id`, so identical provider remote IDs can be
represented independently in different roots without catalog collisions.

The earlier account-scoped `remote_items`, `remote_inventory_staging`, and
`remote_inventory_state` tables remain temporarily for Phase 4A–4F
compatibility. New selected-root bootstrap work must use the root-scoped tables.

`sync roots status` exposes only aggregate root-catalog counts and does not print
local paths, remote IDs, filenames, or provider metadata.

Phase 4M performs no Drive request and registers no sync root.

## Phase 4N — Bounded sync-root inventory integration

NubiSync can now run the recursive Drive traversal through a configured sync
root and exercise the root-scoped staging tables introduced in Phase 4M.

Development command:

`nubisync sync roots inventory --limit <1-10000>`

Safety behavior:

- zero configured roots: skip before keyring or network access
- more than one configured root: skip until an explicit selector exists
- exactly one root: validate that remote folder, traverse it breadth-first, and
  stage supported descendants under that `sync_root_id`
- bounded staging is always cleared before the command returns successfully
- no authoritative root snapshot or root inventory state is committed
- no provider cursor or remote event journal is modified
- the selected remote folder itself is the container and is not cataloged as a
  child item

If the bounded provider scan returns an error, NubiSync still attempts to clear
root-scoped staging before returning the scan error.

Phase 4N intentionally does not offer a full/persistent root bootstrap. Safe
subtree catch-up semantics for the account-wide Drive change stream must be
defined before a selected-root snapshot can become authoritative.

## Phase 4O — Transactional root-catalog mutation primitives

The root-scoped catalog can now support the structural mutations needed by a
future selected-root change-stream catch-up.

Storage primitives include:

- reading one root-scoped remote item
- transactional upsert of one non-trashed remote item
- treating a trashed upsert as subtree removal
- recursive subtree removal using `parent_remote_id`
- durable `item_count` refresh inside the same transaction

Subtree deletion is keyed by `sync_root_id`, so identical provider IDs in a
different sync root remain untouched.

The recursive deletion anchor does not need to exist as a stored catalog row.
This intentionally supports the selected remote root container itself, which is
not cataloged as a child item: deleting that virtual root ID clears all cataloged
descendants for that sync root.

Phase 4O performs no network access, no filesystem mutation, and does not advance
a provider cursor or mark catch-up complete. Provider-side subtree membership
resolution remains a later phase.
