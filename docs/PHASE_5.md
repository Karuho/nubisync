# Phase 5 — Local receive-only materialization

## Phase 5A — Read-only local reconciliation plan

Phase 5A introduces a fail-closed, metadata-only planner between the authoritative
selected-root remote catalog and the real local sync directory.

CLI surface:

`nubisync sync roots reconcile-plan --approve`

The command requires exactly one `receive_only` root, completed authoritative
snapshot + initial catch-up, a durable selected-root change cursor, and no
pending durable change window.

It reads only the durable remote catalog and local filesystem metadata. It never
requests provider/network data, reads file content, mutates SQLite, or creates,
removes, renames, truncates, or overwrites local filesystem entries.

Remote names are rejected when unsafe as local path components (`/`, `.`, `..`,
NUL). Duplicate remote IDs, duplicate sibling names, disconnected catalogs and
path collisions fail closed. The local scan rejects changed/symlinked roots,
symlink entries, non-UTF-8 names, unsupported filesystem object types, and scans
larger than 1,000,000 entries.

Only aggregate counts are printed. Local paths/names, remote names/IDs, cursors
and tokens remain private.

`READY_FOR_DIRECTORY_PHASE=yes` means no local-only entries or type conflicts
block the next non-destructive directory-creation phase. Phase 5A itself is
strictly read-only.

## Phase 5B — Supervised directory materialization

Phase 5B is the first intentional local filesystem mutation in NubiSync.

CLI surface:

`nubisync sync roots materialize-directories --approve`

It reuses the Phase 5A fail-closed preconditions and requires
`READY_FOR_DIRECTORY_PHASE=yes` semantics: no local-only entries and no type
conflicts. It performs no provider/network request and no database mutation.

Only remote folders are materialized. Files are never created, opened, read,
truncated, renamed, deleted or downloaded in this phase.

Directory targets are derived deterministically from the validated authoritative
remote catalog. Parent folders are created before children. Before every create,
the existing parent must be a real directory inside the configured canonical
sync root; symlinks and path escapes fail closed. Existing target directories are
accepted idempotently, while files/symlinks at target paths are conflicts.

Directories created by the current invocation are tracked. If creation or the
final reconciliation postcondition fails, NubiSync removes only those newly
created directories in reverse order. A rollback failure is surfaced explicitly.

The final postcondition requires every remote directory to exist locally as a
directory while the Phase 5A safety conditions remain true. Output contains only
aggregate counts; local names/paths and remote metadata remain private.

## Phase 5C1 — Explicit Drive read-only OAuth upgrade

Phase 5C1 adds the supervised command:

`nubisync auth google upgrade-readonly --approve`

The normal `auth google login` command remains metadata-only. The upgrade requests
`drive.readonly`, verifies the exact granted scope, verifies that the returned
Google subject matches the already configured account, and only then replaces
the refresh token in the OS keyring.

If scope, account identity, or refresh-token issuance is invalid, the existing
refresh token is left intact. The upgrade performs no SQLite mutation, no local
filesystem mutation, no Drive write, and no Drive file-content request.

Phase 5C1 only grants the capability required by the later supervised file
materialization phase; it does not download files itself.

## Phase 5C2 — Supervised single-file materialization

Phase 5C2 adds `nubisync sync roots materialize-file --approve`.

The command is intentionally limited to exactly one missing ordinary file. It
requires a ready receive-only selected root, no pending durable change window,
all remote directories already present locally, no local-only/type conflicts,
and no pre-existing unverified remote files.

The durable remote size must be known and at most 16 MiB. Drive content is
streamed with `alt=media` into a `create_new` temporary file in the target
folder. The byte count must exactly match durable metadata and the temporary
file is fsynced before promotion.

Promotion uses an atomic hard-link creation of the final pathname, providing
no-overwrite semantics if a local entry appears concurrently. The temp name is
removed and the parent directory is fsynced before success. Failures clean the
temp file; failures after promotion roll back only the file created by that
invocation.

No SQLite mutation or Drive write occurs. Existing local entries are never
overwritten, deleted, or renamed. Output exposes only aggregate counts/bytes,
not local names/paths, remote IDs/metadata, or OAuth token values.

Descriptor-relative/openat-style filesystem race hardening remains required
before unattended/background materialization.

## Phase 5C3 — Durable file materialization receipts

Phase 5C3 introduces SQLite schema v9 and durable SHA-256 materialization
receipts. A receipt binds a selected-root remote file to its expected relative
path, durable byte size, SHA-256 digest, and materialization timestamp.

Future `materialize-file` runs hash bytes while streaming and persist the
receipt only after no-overwrite promotion and postconditions succeed. Selected-
root catalog upserts/deletes invalidate affected receipts transactionally; the
invalidation is subtree-aware so folder moves/renames also invalidate descendant
file receipts.

For the file downloaded in Phase 5C2, `sync roots verify-file --approve` hashes
the existing local file and streams the current remote blob into a SHA-256 sink.
It requires exact byte count and digest equality before recording the receipt.
The command does not modify the filesystem and performs no Drive write.

`reconcile-plan` remains metadata-only and therefore still reports existing
files as unverified by that planner while separately exposing the durable
materialization receipt count.

## Phase 5C4 — Offline local verification against durable receipts

Phase 5C4 adds:

`nubisync sync roots verify-local --approve`

The command performs no OAuth refresh and no provider request. It reads the
durable materialization receipts from SQLite and compares local regular files
against the recorded size and SHA-256 values.

Receipt paths are treated as untrusted durable data at the filesystem boundary.
Each path is walked component by component below the canonical configured root.
Missing components are reported as missing; symlinks and non-file type changes
are reported as type conflicts; root escapes are rejected.

The command is read-only with respect to both SQLite and the filesystem. It
prints only aggregate counters and never prints local paths, names, remote IDs,
or SHA-256 values.

A matching receipt means the local file still matches the content NubiSync
previously verified/materialized. It does not independently prove that the
provider has not changed since the receipt was recorded; provider-side catalog
changes invalidate receipts when they are durably applied.

## Phase 5C5 — Preserve stale materialization baselines

Phase 5C5 advances SQLite to schema v10 and changes receipt invalidation from
destructive deletion to a durable `current` -> `stale` transition.

A current receipt means the selected-root remote item has not been durably
mutated since that local content was verified/materialized. When an authoritative
selected-root upsert/delete affects the item or an ancestor subtree, the receipt
is marked stale in the same catalog transaction before the remote catalog is
updated.

Stale receipts deliberately keep the last verified local SHA-256, byte size and
relative path even if the current remote item changes or disappears. They no
longer have a foreign key to the current remote catalog, but remain bound to the
sync root. This preserves the baseline required to decide later whether the local
file was independently modified before applying a newer remote version.

Only current receipts participate in the existing `verify-local` command.
`reconcile-plan` now reports both current and stale receipt counts while
remaining metadata-only.

A new successful materialization of the same remote ID replaces the previous
baseline and returns the receipt to `current`.

Phase 5C5 still does not overwrite or delete an existing local file.

## Phase 5C6 — Safe remote replacement readiness plan

Phase 5C6 adds `nubisync sync roots replacement-plan --approve`.

This is a read-only decision phase. It performs no OAuth refresh, provider
request, SQLite mutation or filesystem mutation.

The plan requires exactly one stale materialization receipt, no current receipt,
one current ordinary remote file at the same remote ID and relative path, and a
clean receive-only topology.

NubiSync hashes the existing local file and compares it with the stale receipt,
which represents the last locally verified provider version. If the local file
still matches that baseline, the candidate is classified `SAFE_TO_REPLACE=1`.
If local bytes changed independently, it reports `LOCAL_CONFLICTS=1`.

The current remote blob is not downloaded in this phase. `READY_TO_REPLACE=yes`
only means a later supervised replacement operation may download the newer
provider version without having detected an independent local modification.

Renames, moves, missing local files and filesystem type conflicts fail closed or
produce a non-ready plan. This phase never overwrites or deletes anything.

## Phase 5C7 — Supervised safe replacement of one receive-only file

Phase 5C7 adds `nubisync sync roots replace-file --approve`.

The command remains supervised and is limited to exactly one ordinary remote
file with one stale materialization baseline. Before provider access, the 5C6
read-only replacement plan must report the candidate ready.

After refreshing the existing `drive.readonly` grant and verifying the configured
Google account, NubiSync fetches a provider-side SHA-256 fingerprint and size,
downloads the current remote blob to a same-parent temporary file, hashes SHA-256
while streaming, then fetches the provider fingerprint again. The pre/post
fingerprints must match each other and the downloaded SHA-256/byte count.

Immediately before the destructive step, NubiSync re-hashes the existing local
file against the stale receipt and checks Linux inode/device/size/mtime/ctime
state around that hash. Any detected local divergence or target race aborts.

Promotion uses same-directory `rename` for atomic replacement on Linux. The
downloaded temporary file is fsynced before promotion, the parent directory is
synced after promotion, and the promoted file is re-hashed before the durable
receipt is returned to `current`.

If persistence fails after filesystem promotion, the stale receipt remains
fail-closed rather than silently trusting the new local bytes. Ordinary pathname
APIs still leave a narrow TOCTOU window; descriptor-relative/openat2 hardening
remains required before unattended/adversarial operation.

Phase 5C7 performs no Drive writes.

## Phase 5C8 — Read-only remote deletion decision plan

Phase 5C8 adds `nubisync sync roots deletion-plan --approve`.

This phase does not delete anything. It is a read-only decision boundary for one
receive-only file whose authoritative remote identity has disappeared.

The plan requires:

- an authoritative selected-root catalog with no pending change window,
- no current materialization receipt,
- exactly one stale receipt,
- the stale remote ID to be absent from the current authoritative catalog,
- exactly one local-only entry,
- no missing remote targets or type conflicts,
- and the remaining local file to match the stale receipt SHA-256 and byte size.

The stale receipt therefore acts as proof of the last provider version that
NubiSync had materialized locally. If the local bytes still match it, the plan
reports `SAFE_TO_DELETE=1`. If local bytes diverged after the provider deletion,
the plan reports a local conflict and does not authorize deletion.

The command performs no provider request, SQLite mutation or filesystem mutation.
It does not print local names, remote IDs or hash values.

A later supervised deletion executor must revalidate the same baseline
immediately before unlinking and must address the pathname race boundary before
unattended/adversarial operation.

## Phase 5C9 — Supervised safe local deletion

Phase 5C9 adds `nubisync sync roots delete-file --approve`.

The command performs no provider request. It requires the 5C8 read-only deletion
plan to be ready and then revalidates the stale materialization baseline
immediately before any destructive filesystem action.

NubiSync hashes the remaining local file against the stale receipt and checks
Linux device/inode/size/mtime/ctime state around that hash. It then atomically
renames the exact candidate into an unpredictable same-parent quarantine name,
verifies that the quarantined inode is the one that was validated, hashes the
quarantined bytes again, and only then removes that quarantine file.

The parent directory is fsynced after deletion. The stale receipt is deleted from
SQLite only after the filesystem deletion succeeds. If receipt cleanup fails
after deletion, the stale receipt remains and later operations fail closed rather
than silently claiming a completed state.

The final postcondition requires no local-only entries, no missing remote targets,
no type conflicts, no remote files and zero current/stale materialization
receipts.

Ordinary pathname APIs still leave a narrow race between the final quarantine
verification and unlink. The quarantine+inode binding substantially narrows the
supervised mutation window but does not replace future descriptor-relative
openat2/dirfd hardening required for unattended/adversarial operation.

Phase 5C9 performs no Drive writes and no network request.

## Phase 5C10 — Durable directory materialization ownership receipts

Phase 5C10 introduces a separate durable ownership receipt for receive-only
directories and advances SQLite to schema v11.

A directory receipt binds a selected sync root, provider remote ID, safe relative
path and materialization timestamp. Current and stale states are separate. Catalog
upserts and subtree removals invalidate current directory receipts to stale in the
same transaction that already invalidates file receipts.

`materialize-directories --approve` now records directory ownership only for
directories created by that NubiSync run. Pre-existing matching directories are
not silently claimed.

For the existing prototype test directory created before directory receipts
existed, `sync roots adopt-directory --approve` is a deliberately narrow,
owner-supervised migration bridge. It requires exactly one authoritative remote
directory, exactly one matching local directory, no files, no local-only entries,
no conflicts, no file receipts, no existing directory receipts, and an empty
ordinary non-symlink directory. It performs no provider request and no filesystem
mutation; it only writes the durable ownership receipt after local metadata
validation.

This adoption path is not a general background ownership inference mechanism.
Future unattended operation must never adopt arbitrary pre-existing directories.

Phase 5C10 performs no Drive writes.

## Phase 5C11 — Read-only remote directory absence/removal plan

Phase 5C11 adds `nubisync sync roots directory-deletion-plan --approve`.

The planner is intentionally read-only. It performs no provider request, no
database mutation and no filesystem mutation.

For this first supervised directory-removal slice it accepts only the exact
single-directory state established by Phase 5C10 after that remote folder has
left the authoritative selected root:

- zero authoritative remote items
- exactly one local-only directory
- zero file receipts, current or stale
- zero current directory receipts
- exactly one stale directory receipt
- stale remote identity absent from the authoritative catalog
- stale receipt path resolving to one ordinary non-symlink directory
- the directory must be empty
- no missing remote items or type conflicts

The planner distinguishes an already-missing directory, a type/symlink conflict,
and a non-empty directory from a safe deletion candidate. It prints only
aggregate counters and booleans and never prints the root path, local names,
remote IDs or metadata.

Phase 5C11 does not add `rmdir` or any other destructive filesystem operation.
A later phase must separately implement and owner-validate supervised directory
deletion.

Phase 5C11 performs no Drive writes.
