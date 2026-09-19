# Phase 5H13 — Real supervised folder-create end-to-end fixture validation design

Status: **FROZEN DESIGN — no fixture or Drive object mutation executed**

Frozen against repository baseline:

`90f4ea5ce1804218c7672d2ab382fe40c1be3e6e`

Date: 2026-09-19.

## Purpose

Phase 5H13 freezes the owner-runtime procedure for the first real end-to-end
local-to-Google-Drive synchronization of one ordinary empty folder.

This phase is documentation only. It does not create a local fixture, call
`files.generateIds`, call `files.create`, consume the Drive change stream, apply
a local event, advance the local baseline, or settle a remote-write intent.

The subsequent owner-runtime phase will validate the already implemented chain:

owner-created local folder
→ durable TwoWay local journal
→ `files.generateIds`
→ one durable `CreateFolder` intent
→ one supervised `files.create`
→ Drive change-stream confirmation
→ selective local settlement
→ generation N → N+1.

## Closed foundations entering 5H13

The design assumes the following are already closed and pushed:

- Phase 5G user-global cross-process execution ownership;
- schema v18;
- separate ReceiveOnly and FullSync credentials;
- root mode explicitly `two_way`;
- persistent daemon in TwoWay standby;
- daemon does not load the FullSync credential;
- cursor-bound remote write-authority snapshots;
- deterministic remote-write planning;
- supervised predetermined-ID allocation;
- durable folder-create execution states and recovery;
- one supervised folder-create submission per approval;
- change-stream confirmation from the persisted pre-submit fence;
- confirmed folder-create settlement with:
  - device+inode source identity;
  - selective baseline promotion;
  - residual diff rebase;
  - source event `pending -> applied`;
  - unrelated old events `pending -> superseded`;
  - generation N -> N+1;
  - current directory ownership receipt;
  - one-to-one durable settlement evidence;
- no automatic TwoWay daemon execution.

## Validation objective

The runtime validation succeeds only if one owner-created local directory is
carried through the complete authority chain without any hidden automatic step.

The successful path must prove all of the following facts independently:

1. the owner, not NubiSync, creates the local fixture;
2. NubiSync observes exactly one eligible local folder create;
3. `files.generateIds` allocates exactly one predetermined ID;
4. ID allocation does not mutate any Drive object;
5. exactly one durable `CreateFolder` intent is planned;
6. exactly one explicit submission approval can invoke `files.create`;
7. the intent becomes `awaiting_confirmation`, never directly `confirmed`;
8. the Drive change stream observes the exact predetermined ID and catalog
   postcondition;
9. only then does the intent become `confirmed`;
10. confirmation still leaves the source event pending and baseline unchanged;
11. one separate settlement approval promotes only the confirmed directory;
12. settlement performs no provider call;
13. the local generation advances exactly once;
14. the exact source event becomes applied;
15. durable ownership and settlement evidence exist;
16. a repeated settlement is a no-op and does not advance generation again;
17. no local or remote identifier, path, name, cursor, token, device ID or inode
   is printed by normal aggregate CLI output.

## Fixture contract

The first real fixture is deliberately narrow.

The owner must manually create exactly one new directory:

- directly under the selected local sync root;
- ordinary directory;
- empty;
- not a symlink;
- not a bind mount or special filesystem object;
- unique relative path not already represented in the durable baseline;
- no child file or child directory;
- no rename or replacement during the validation.

NubiSync must not create this local directory on behalf of the owner.

The validation harness must never accept a path argument that it then creates.
It may pause and instruct the owner to create the directory manually, but the
owner action remains outside the harness.

For the first validation, no other local filesystem change may be introduced
inside the selected root until the E2E path has settled.

The fixture name/path is private validation input and must not be echoed into
aggregate logs.

## Service ownership during the real validation

The persistent `nubisyncd` user service is normally active in TwoWay standby.

For the owner-runtime validation, the harness should:

1. record whether the service was active;
2. stop it before the owner creates the fixture;
3. verify it is stopped;
4. run every supervised CLI stage under the existing user-global cross-process
   execution lock;
5. restore the service to the prior active state only after success or controlled
   abort.

This removes avoidable process-lifecycle noise even though the current TwoWay
daemon remains write-inert.

The service must never receive or load the FullSync credential.

## Clean preflight before the owner creates the fixture

Before any local fixture exists, the runtime must require:

- repository/worktree matches the exact runtime-validation baseline;
- schema remains v18;
- exactly one configured Google account/root;
- root mode is `two_way`;
- FullSync credential exists for the explicit CLI lane;
- daemon FullSync loading remains disabled;
- local observation baseline is complete and valid;
- no local pending change event exists;
- no open selected-root change window exists;
- remote catalog is complete;
- durable change cursor exists;
- remote write-authority snapshot is current and cursor-bound;
- write gates are satisfied;
- folder-create recovery plan reports:
  - `PLANNED=0`
  - `SUBMITTED=0`
  - `AWAITING_CONFIRMATION=0`
  - `CONFLICT=0`
  - `FAILED=0`
  - `RECOVERY_REQUIRED=no`
- confirmed-but-unsettled folder-create count is zero.

If the provider cursor or authority snapshot is stale, the validation aborts.
5H13 does not authorize silently rebasing a stale TwoWay root as part of the
fixture test.

## Stage A — owner creates the local fixture

The harness pauses.

The owner manually creates exactly one empty top-level directory inside the
configured sync root.

The harness does not run `mkdir`, create a desktop file-manager action, synthesize
an inotify event, or otherwise manufacture the fixture.

After the owner confirms the directory exists, the validation resumes.

No command should print the directory name or path.

## Stage B — journal + predetermined ID allocation

The first NubiSync mutation after the manual owner action is:

`nubisync sync roots allocate-create-ids --approve`

This command is intentionally used instead of `local-journal`.

The existing `local-journal` command is ReceiveOnly-only. The TwoWay
`allocate-create-ids` path already performs the durable TwoWay local journal
before planning and ID allocation.

For this fixture, the invocation must report exactly:

- `PENDING_EVENTS=1`
- `ELIGIBLE_CREATE_CANDIDATES=1`
- `IDS_REQUESTED=1`
- `IDS_RETURNED=1`
- `INTENTS_PERSISTED=1`
- `GENERATE_IDS_CALL=performed`
- `REMOTE_CURSOR_STABLE=yes`
- `AUTHORITY_CURSOR_MATCH=yes`
- `LOCAL_EVENT_APPLIED=no`
- `BASELINE_ADVANCED=no`
- `REMOTE_OBJECT_MUTATION=no`
- `DRIVE_WRITE_ACCESS=generate_ids_only`

Any count greater than one invalidates the fixture and stops the run before
`files.create`.

An invalid fixture may leave extra planned IDs/intents durable. That is safer
than performing an unintended remote object mutation. The run must stop for
owner inspection rather than trying to auto-clean or auto-submit those intents.

## Stage C — planned-state gate

Immediately after allocation, run the read-only recovery plan:

`nubisync sync roots folder-create-recovery-plan --approve`

The expected first-fixture state is exactly:

- `FOLDER_CREATE_INTENTS=1`
- `PLANNED=1`
- `SUBMITTED=0`
- `AWAITING_CONFIRMATION=0`
- `CONFIRMED=0`
- `CONFLICT=0`
- `FAILED=0`
- `RECOVERY_REQUIRED=no`

If this exact state is not present, no submission approval is issued.

## Stage D — one supervised remote folder create

The only object-mutation step is:

`nubisync sync roots submit-folder-create --approve`

Exactly one owner approval is allowed.

Before POST, the existing executor must revalidate:

- local path still resolves to the same ordinary directory;
- planned identity including the submission-time device/inode/mtime fence;
- FullSync credential;
- Google account subject;
- durable parent write authority;
- fresh parent version/capability;
- parent topology;
- provider cursor equals the durable pre-submit fence.

The durable intent must become `submitted` before provider POST.

The normal successful result must report:

- `SELECTED_INTENTS=1`
- `SUBMISSION_ATTEMPTED=yes`
- `LOCAL_IDENTITY_VERIFIED=yes`
- `FULLSYNC_CREDENTIAL_VERIFIED=yes`
- `ACCOUNT_SUBJECT_MATCH=yes`
- `PARENT_AUTHORITY_VERIFIED=yes`
- `PARENT_TOPOLOGY_VERIFIED=yes`
- `PRE_SUBMIT_CURSOR_MATCH=yes`
- `DURABLE_SUBMITTED_BEFORE_POST=yes`
- `PROVIDER_POST=performed`
- `INTENT_STATUS_AFTER=awaiting_confirmation`
- `CONFIRMED=no`
- `CHANGE_STREAM_CONFIRMATION_REQUIRED=yes`
- `LOCAL_EVENT_APPLIED=no`
- `BASELINE_ADVANCED=no`
- `DRIVE_WRITE_ACCESS=folder_create_only`

A successful create may report `REMOTE_OBJECT_MUTATION=created`.

A provider `409 Conflict` is not automatically failure and is not automatically
success. The existing exact predetermined-ID recovery semantics remain
authoritative.

## Ambiguous submission branch

If submission leaves the intent in `submitted`, the validation must not call
`submit-folder-create` again.

It must first run:

`nubisync sync roots folder-create-recovery-plan --approve`

and then, under a new explicit owner approval:

`nubisync sync roots recover-folder-create-submission --approve`

Recovery is metadata-only and must not issue another provider POST.

Allowed outcomes:

- exact predetermined-ID match → `awaiting_confirmation`;
- exact predetermined-ID mismatch → `conflict`, stop;
- exact predetermined-ID missing → remain `submitted`, stop for owner analysis.

The first real E2E fixture validation does not authorize an automatic same-ID
retry loop.

## Stage E — supervised Drive change-stream confirmation

Once the intent is `awaiting_confirmation`, run:

`nubisync sync roots confirm-folder-create --approve`

Each invocation is separately owner-approved and bounded.

If the target change is not yet visible:

- staged change window is discarded;
- durable cursor does not advance;
- intent remains `awaiting_confirmation`;
- no local event is applied;
- no baseline advances;
- no provider write method is called.

The owner may explicitly invoke confirmation again later.

Automatic polling/retry loops are not part of this validation.

A successful confirmation must report:

- `SELECTED_INTENTS=1`
- `CHANGE_STREAM_SCAN=performed`
- `TARGET_CHANGE_OBSERVED=yes`
- `TARGET_LAST_CHANGE_EXACT=yes`
- `FINAL_CATALOG_ITEM_EXACT=yes`
- `CURSOR_ADVANCED=yes`
- `INTENT_STATUS_AFTER=confirmed`
- `CONFIRMED=yes`
- `PROVIDER_WRITE_METHOD_CALLED=no`
- `LOCAL_EVENT_APPLIED=no`
- `BASELINE_ADVANCED=no`
- `DRIVE_WRITE_ACCESS=readonly_confirmation_only`

Provider `files.get` evidence alone is never enough to produce `confirmed`.

## Stage F — supervised local settlement

Only after `confirmed=yes` may the owner run:

`nubisync sync roots settle-confirmed-folder-create --approve`

Expected successful settlement:

- `UNSETTLED_CONFIRMED_FOLDER_INTENTS=1`
- `SELECTED_INTENTS=1`
- `LOCAL_DOUBLE_SCAN=performed`
- `SOURCE_IDENTITY=device_inode`
- `SOURCE_MTIME_IDENTITY_REQUIRED=no`
- `SELECTIVE_BASELINE_PROMOTION=performed`
- `RESIDUAL_DIFF_REBASED=yes`
- `GENERATION_ADVANCED=yes`
- `SOURCE_EVENT_APPLIED=yes`
- `OWNERSHIP_RECEIPT_CREATED=yes`
- `SETTLEMENT_EVIDENCE_RECORDED=yes`
- `REMOTE_WRITE_INTENT_STATUS=confirmed`
- `SETTLEMENT_DATABASE_MUTATION=yes`
- `NETWORK_CHECK=not_performed`
- `PROVIDER_METHOD_CALLED=no`
- `REMOTE_OBJECT_MUTATION=no`
- `DRIVE_WRITE_ACCESS=no_remote_call`

The runtime must capture `GENERATION_FROM` and `GENERATION_TO` only as aggregate
numbers and require exactly `GENERATION_TO = GENERATION_FROM + 1`.

The fixture remains present locally and remotely after successful settlement.

No automatic cleanup is allowed in this phase because a proven remote-trash
workflow is not yet part of this validation.

## Stage G — live idempotency check

After successful settlement, invoke settlement once more:

`nubisync sync roots settle-confirmed-folder-create --approve`

The second call must be a no-op:

- `UNSETTLED_CONFIRMED_FOLDER_INTENTS=0`
- `SELECTED_INTENTS=0`
- `GENERATION_ADVANCED=no`
- `SOURCE_EVENT_APPLIED=no`
- `OWNERSHIP_RECEIPT_CREATED=no`
- `SETTLEMENT_EVIDENCE_RECORDED=no`
- `PROVIDER_METHOD_CALLED=no`
- `REMOTE_OBJECT_MUTATION=no`

This proves the one-to-one settlement evidence prevents a second generation
advance in the real database.

## Stage H — post-settlement authority refresh and clean plan

Change-stream confirmation advances the durable remote cursor.

The existing remote write-authority snapshot was bound to the earlier cursor, so
before asserting readiness for another future write, the owner must explicitly
refresh metadata authority:

`nubisync sync roots observe-write-authority --approve`

This is a read-only provider operation.

After the authority snapshot is rebound to the current cursor:

`nubisync sync roots remote-write-plan --approve`

must report a clean local state:

- `PENDING_EVENTS=0`
- `CREATE_FILE_NEEDS_ID=0`
- `CREATE_FOLDER_NEEDS_ID=0`
- `UPDATE_FILE_READY=0`
- `TRASH_ITEM_READY=0`
- `CONFLICTS=0`
- `BLOCKED_IDENTITY=0`
- `BLOCKED_AUTHORITY=0`
- `ROOT_WRITE_CAPABLE=yes`
- `FULLSYNC_CREDENTIAL_PRESENT=yes`
- `WRITE_GATES_SATISFIED=yes`
- `INTENTS_PERSISTED=0`
- no network request by the planner;
- no filesystem mutation.

The historical confirmed folder-create intent and its settlement evidence remain
durable audit facts; they are not deleted merely to make the planner clean.

## Final folder-create recovery state

The post-settlement recovery plan may still count the historical intent as
`CONFIRMED=1`.

That is expected.

The required recovery condition is:

- `SUBMITTED=0`
- `AWAITING_CONFIRMATION=0`
- `CONFLICT=0`
- `FAILED=0`
- `RECOVERY_REQUIRED=no`

`confirmed` records remote proof. Settlement evidence separately records local
baseline incorporation.

## Failure policy

Any unexpected count, state, cursor fence, local identity, account identity,
parent authority, provider result or catalog result stops the E2E run.

On failure:

- do not create a second local fixture;
- do not delete or rename the existing local fixture;
- do not manually delete the possible remote folder;
- do not issue a blind second `files.create`;
- do not edit SQLite by hand;
- do not discard or replace the predetermined remote ID;
- preserve the aggregate logs and durable database state;
- inspect `folder-create-recovery-plan` before choosing the next supervised
  recovery action.

If a remote folder might already exist, preserving state is safer than trying to
"clean up" before the predetermined-ID recovery path is understood.

## Evidence capture

The runtime validation harness should write privacy-safe aggregate command output
to a private local evidence directory, for example under:

`~/.local/state/nubisync/validation/`

The evidence may contain:

- phase/stage names;
- command exit status;
- aggregate counters;
- generation numbers;
- PASS/no-op flags;
- Git commit;
- schema version;
- service active/standby state.

It must not contain:

- sync-root path;
- fixture path or name;
- remote IDs;
- parent IDs;
- Drive versions;
- opaque cursors;
- OAuth tokens;
- refresh tokens;
- keyring contents;
- device IDs;
- inodes.

## Checkpoint transport rule

The canonical project checkpoint remains the local state file.

A byte-for-byte copy in the local GoogleDrive mount is only proof that the local
mirror file matches the canonical file.

It is not by itself proof that the cloud-side Google Drive object has already
uploaded and become visible through the provider API.

Future closure output should therefore distinguish:

- `CHECKPOINT_LOCAL_MIRROR_MATCH=yes`
- `REMOTE_CLOUD_CHECKPOINT_VERIFIED=yes|no`

and must not label a local `cmp` result as cloud verification.

## Explicit exclusions

5H13 and its first runtime validation do not authorize:

- automatic fixture creation;
- more than one `files.create` object mutation;
- file upload;
- file content reads;
- file update;
- remote trash;
- permanent delete;
- rename/move;
- type-change handling;
- automatic same-ID retry;
- automatic confirmation polling;
- daemon FullSync access;
- daemon remote-write execution;
- background settlement;
- automatic fixture cleanup;
- Shared Drives;
- multi-account execution.

## Phase 5H13 closure rule

5H13 closes when this design document is committed and pushed while runtime
behavior remains unchanged:

- schema remains v18;
- no Rust source changes;
- no local fixture created by the closure script;
- no `files.generateIds`;
- no `files.create`;
- no Drive change-stream consumption;
- no local event applied;
- no local generation advance;
- no new settlement row;
- no provider method called;
- no remote object mutation;
- daemon remains active in TwoWay standby.

The next phase is **5H14 — real supervised folder-create end-to-end owner-runtime
validation**.

5H14 must use the frozen procedure above and must not broaden the implementation
surface unless the real fixture exposes a concrete bug that requires a separately
audited fix.
