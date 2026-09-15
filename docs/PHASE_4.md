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
