# Phase 5H11 — Confirmed-write local settlement design

Status: **FROZEN DESIGN — no local settlement implementation**

Frozen against repository baseline:

`50117473662ec0e02f9baf09c401b8ae4ff36072`

Date: 2026-09-19.

## Purpose

Phase 5H11 defines how a confirmed local-to-remote folder create becomes part
of NubiSync's accepted local baseline **without losing unrelated local changes**.

This phase is documentation only.

It does not:

- mark any local event applied;
- advance the local baseline;
- change the local generation;
- create or alter ownership receipts;
- add schema;
- call Google Drive;
- execute any provider write;
- change the daemon out of TwoWay standby.

## Problem being solved

After Phase 5H10, a folder-create intent may be:

`confirmed`

while its source local event is still:

`pending`

and the durable local baseline still represents the state **before** that local
folder existed.

A naive settlement is unsafe.

### Why changing only the event status is wrong

`reconcile_sync_root_local_change_journal` is derived from the current durable
baseline.

For every observed local diff it performs an UPSERT whose conflict branch sets:

`status = 'pending'`

Therefore, if settlement merely changed the source event from `pending` to
`applied` while leaving the baseline unchanged, the next local journal
observation would see the same create again and reopen it as pending.

### Why replacing the whole baseline is wrong

The user's filesystem may contain unrelated local changes that appeared after
the original source event.

Promoting a fresh complete scan as the new baseline would silently accept those
unrelated changes before their own remote writes are confirmed.

That would violate the evidence boundary between:

- accepted/synchronized local state; and
- observed but unresolved local changes.

## Settlement scope

The first settlement implementation will support only:

- `RemoteWriteIntentOperation::CreateFolder`;
- intent status exactly `confirmed`;
- source local event kind exactly `created`;
- source local kind exactly `directory`;
- one settlement per explicit owner approval.

The following remain out of scope:

- file create settlement;
- update settlement;
- trash settlement;
- rename/move settlement;
- type-change settlement;
- multiple settlements in one approval;
- daemon settlement;
- automatic periodic settlement.

## Core rule

Settlement promotes **only the confirmed source folder** into the accepted local
baseline.

Every other currently observed local difference must remain unresolved and must
be re-expressed as a pending event in the next local generation.

The baseline must never be replaced wholesale from a fresh scan during folder
create settlement.

## Eligibility gates

A settlement candidate must satisfy all of the following before any durable
mutation:

1. exactly one selected Google account and one selected root;
2. root mode is exactly `two_way`;
3. exactly one selected candidate for the invocation;
4. remote-write intent exists;
5. intent operation is exactly `create_folder`;
6. intent status is exactly `confirmed`;
7. intent has a predetermined remote ID;
8. source local event still exists;
9. source event belongs to the same root;
10. source event status is exactly `pending`;
11. source event kind is exactly `created`;
12. source event current kind is exactly `directory`;
13. source event baseline generation equals the current durable local generation;
14. current local observation state is complete and valid;
15. no settlement record already exists for this intent;
16. authoritative remote catalog contains the predetermined remote ID;
17. remote catalog item is:
    - a folder;
    - not trashed;
    - exact expected name;
    - exact expected parent;
18. the current local path still exists as a real directory;
19. it is not a symlink;
20. local device/inode still identify the same directory originally bound to the intent.

Any mismatch fails closed.

## Directory identity after confirmation

For the create-folder settlement, `device_id + inode` are the durable object
identity.

The originally planned directory mtime is **not** required to remain unchanged
at settlement time.

Reason: adding a child inside a directory can legitimately change directory
mtime after remote confirmation. Treating that mtime change as a replacement
would block safe settlement of the parent and would turn unrelated child
creation into a false identity conflict.

The path, type, device and inode must still match.

A path disappearance, rename, replacement, symlink substitution or type change
blocks settlement.

## Fresh local observation

Immediately before settlement, NubiSync must perform the existing safe double
metadata scan.

The two scans must match exactly.

No file content is read.

The resulting snapshot is `current_observation`.

The currently durable accepted baseline is `old_baseline`.

NubiSync derives:

`old_diff = diff(old_baseline, current_observation)`

The exact source path must appear in `old_diff` as a directory create.

If it does not, settlement fails closed.

## Selective proposed baseline

NubiSync constructs a logical `proposed_baseline` from durable accepted state:

1. clone `old_baseline`;
2. insert exactly the source directory snapshot from `current_observation`;
3. do not copy any other new/modified/deleted entry from the current scan.

For the first folder-create settlement:

- old baseline must not already contain the source path;
- item count becomes old count + 1;
- source directory identity stored in the new baseline uses the fresh current
  directory metadata;
- no child or sibling change is implicitly accepted.

NubiSync then computes:

`residual_diff = diff(proposed_baseline, current_observation)`

The source path must no longer appear in `residual_diff`.

Every unrelated unresolved local change must still appear in `residual_diff`.

## Generation rebase

Settlement advances local authority from:

`generation N -> generation N+1`

because the accepted baseline changed.

The new generation does **not** mean all currently observed filesystem state was
accepted.

It means:

- generation N baseline plus the one confirmed source-folder promotion is now
  accepted;
- all other differences are re-journaled against generation N+1.

This rebase must happen atomically.

## Required atomic transaction

The implementation phase must provide a single SQLite transaction that performs
all settlement mutations together.

The transaction must compare-and-set the expected old generation and verify all
durable settlement preconditions again.

Within the same transaction it must:

1. verify root mode remains `two_way`;
2. verify local inventory state still matches expected:
   - generation;
   - item count;
   - snapshot-completed timestamp;
   - observation-valid flag;
3. verify confirmed intent identity/status/generation;
4. verify source event is still pending and generation-bound;
5. verify exact confirmed remote catalog item;
6. verify no prior settlement exists;
7. insert/update exactly the source directory in `sync_root_local_items`;
8. set local item count to old count + 1;
9. advance local generation exactly once;
10. keep `observation_valid = 1`;
11. update the baseline completion/update timestamp to the settlement timestamp;
12. mark the source event `applied`;
13. supersede every other still-pending event from generation N;
14. insert the full `residual_diff` as pending events for generation N+1;
15. create the synchronized directory ownership receipt for the predetermined
    remote ID and source relative path;
16. record durable settlement evidence for idempotency/audit;
17. commit.

No intermediate committed state may exist where:

- the source is applied but baseline not advanced;
- baseline advanced but residual events not re-journaled;
- ownership receipt exists but source event is still independently pending;
- generation advanced without settlement evidence.

## Durable settlement evidence

The implementation phase should use schema v18 and add explicit one-to-one
settlement evidence rather than overloading the remote confirmation status.

Recommended table:

`sync_root_remote_write_settlements`

Required fields:

- `intent_id` — primary/unique settlement identity;
- `sync_root_id`;
- `source_local_event_id`;
- `settled_from_generation`;
- `settled_to_generation`;
- `settled_at_unix_ms`.

The row may also contain the remote ID and relative path if useful for invariant
checking, but normal Debug/CLI output must redact them.

The remote-write intent remains `confirmed`.

`confirmed` means remote authority was proven through the Drive change stream.

The settlement row means local baseline authority incorporated that confirmed
write.

These are separate facts and must remain separately auditable.

A second settlement attempt for the same intent must be idempotently rejected or
reported as already settled; it must never advance generation twice.

## Ownership receipt semantics

Settlement must create a current directory ownership receipt using the existing:

`sync_root_directory_materialization_receipts`

for:

- selected root;
- predetermined remote ID;
- source relative path.

For TwoWay, this receipt is interpreted as a durable synchronized ownership /
identity binding.

It does **not** mean NubiSync necessarily created the local directory.

This binding is necessary because the remote-write planner currently resolves
future local modifications/deletions through file/directory receipts to recover
the corresponding remote ID.

The receipt may be inserted only after:

- remote confirmation is durable;
- exact remote catalog identity is verified;
- local source identity is still valid.

The existing unique current-path invariant must remain enforced.

## Concurrent child example

Assume generation N baseline has no `docs`.

The user creates:

`docs/`

NubiSync creates and confirms the remote `docs` folder.

Before settlement, the user also creates:

`docs/new.txt`

At settlement time:

`current_observation` contains:

- `docs/`
- `docs/new.txt`

The proposed accepted baseline contains only:

- `docs/`

The new generation N+1 residual diff contains:

- create `docs/new.txt`

Therefore:

- the confirmed parent folder is settled;
- the child remains a pending unsynchronized change;
- no local work is lost.

## Concurrent sibling example

If the user also creates:

`notes/`

before settlement, the new generation residual journal must retain:

- create `docs/new.txt`
- create `notes/`

Settlement of `docs/` must not accept either one.

## Existing pending events

The transaction must not simply carry old event rows forward verbatim.

Old events were derived relative to generation N.

After promoting the source into the baseline, event semantics may change.

Therefore the implementation must derive the complete `residual_diff` against
the proposed baseline and insert new generation N+1 events from that result.

All remaining pending events from generation N become `superseded`.

Only the exact source event becomes `applied`.

## Local snapshot timestamp semantics

The existing `snapshot_completed_at_unix_ms` field will act as the timestamp of
the latest accepted local-baseline update.

A selective settlement may update this timestamp even though NubiSync did not
replace the full baseline from the fresh filesystem scan.

The baseline contents remain the authoritative accepted state; the timestamp is
not proof that every observed current filesystem entry was accepted.

This interpretation must be documented in implementation tests.

## Remote authority after settlement

Settlement performs no Google Drive request.

It consumes the already confirmed durable catalog state.

The exact predetermined remote ID must remain present in the selected-root
catalog with expected:

- name;
- folder kind;
- parent;
- not-trashed state.

The root change cursor is not changed by local settlement.

Remote write-authority observation is not refreshed by settlement.

## Privacy contract

Normal output must not print:

- local root path;
- source relative path;
- local leaf name;
- predetermined remote ID;
- parent remote ID;
- Drive cursor;
- device ID;
- inode;
- hashes;
- token values;
- keyring values.

CLI output remains aggregate/status-only.

## Daemon policy

The first local-settlement executor remains an explicit supervised CLI command.

`nubisyncd` remains in TwoWay standby and must still:

- load no FullSync credential;
- perform no TwoWay network polling;
- perform no TwoWay filesystem scan;
- execute no remote-write intent;
- settle no confirmed write automatically.

## Proposed implementation sequence

### Phase 5H12 — supervised confirmed folder-create settlement

Expected work:

- schema v18 settlement evidence;
- one confirmed folder intent per approval;
- fresh double metadata scan;
- source identity/device/inode verification;
- proposed selective baseline;
- residual diff derivation;
- one atomic storage transaction for:
  - selective baseline promotion;
  - generation N -> N+1;
  - source event applied;
  - old unrelated pending events superseded;
  - residual events inserted pending at N+1;
  - current directory ownership receipt;
  - settlement evidence;
- no provider network;
- no provider write;
- daemon remains standby.

### Later slices

Only after folder-create settlement is proven should NubiSync generalize the
settlement machinery for:

- ordinary file create;
- file update;
- remote trash.

Each operation requires operation-specific baseline promotion semantics.

## Phase 5H11 closure rule

5H11 closes when this document is committed and pushed while runtime remains
unchanged:

- schema v17;
- no Rust source change;
- existing `files.create` surface unchanged;
- no provider method called;
- no local event applied;
- no baseline advancement;
- no ownership receipt created;
- no settlement row/table yet;
- daemon active in TwoWay standby.

The next implementation phase is 5H12.
