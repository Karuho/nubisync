# Phase 5H7 — First supervised remote-create boundary

Status: **FROZEN DESIGN — no remote object mutation implemented**

Frozen against repository baseline:

`bff35754e711d27ebfd5242aea1dcd0c3310e030`

Date: 2026-09-19.

## Purpose

Phase 5H7 freezes the first actual Google Drive mutation contract before any
`files.create` provider method is added.

The first mutation slice will be **folder create only**.

This phase is documentation only. It does not add a Drive POST/PATCH/DELETE
request, does not execute a durable remote-write intent, does not advance a
local baseline, and does not change `nubisyncd` out of TwoWay standby.

## Closed foundations entering 5H7

The design assumes the following are already closed:

- schema v16;
- a separate FullSync refresh credential exists in the OS keyring;
- the selected root is explicitly `two_way`;
- the persistent daemon is in `WriteCapableStandby`;
- the daemon does not load the FullSync credential;
- cross-process execution ownership is active;
- local baseline/journal authority is durable and generation-bound;
- remote catalog and remote write-authority snapshots are cursor-bound;
- deterministic remote-write planning exists;
- `files.generateIds` is the only FullSync Drive primitive currently present;
- predetermined IDs can be bound atomically to create intents;
- local events remain pending after create-ID allocation;
- no baseline advancement occurs during intent planning or ID allocation.

## First mutation choice: folder create

The first actual remote mutation is creation of an ordinary Drive folder.

Folder create is selected before ordinary file upload because it is
metadata-only and therefore does not introduce:

- file-content reads;
- content hashing during upload;
- multipart or resumable upload state;
- upload-session capability URLs;
- large byte-transfer limits;
- partial content-transfer recovery.

The first mutation must therefore support only:

`RemoteWriteIntentOperation::CreateFolder`

`CreateFile`, `UpdateFile`, `TrashItem`, type changes, rename/move and file upload
remain outside this slice.

## Google Drive request contract

The eventual provider method will issue exactly one metadata create request:

`POST https://www.googleapis.com/drive/v3/files`

The JSON body must contain exactly the NubiSync-authoritative create metadata:

- `id`: the predetermined Drive ID already persisted in the durable intent;
- `name`: the exact local leaf name derived from the validated relative path;
- `mimeType`: `application/vnd.google-apps.folder`;
- `parents`: an array containing exactly the single expected parent remote ID.

No implicit My Drive fallback is allowed. `parents` must be present.

No Google Workspace conversion is involved.

The request must ask Drive to return only the fields needed for immediate
postcondition validation:

- `id`
- `name`
- `mimeType`
- `parents`
- `trashed`
- `version`

Normal output must never print any of those sensitive object identity values.

## Preconditions immediately before mutation

Every execution attempt must run under the user-global cross-process execution
lock and must revalidate all of the following immediately before changing the
intent to `submitted`:

1. exactly one supported Google account is configured;
2. the selected root still exists and is still `two_way`;
3. the FullSync credential is still present;
4. refreshing the FullSync credential succeeds;
5. the refreshed Google subject exactly matches the configured account;
6. the intent still exists and is `planned`;
7. the intent operation is exactly `CreateFolder`;
8. the source local event still exists and is `pending`;
9. the event belongs to the same selected root;
10. the event baseline generation equals the current durable local generation;
11. the durable local observation baseline is complete and valid;
12. a fresh double local metadata scan still matches the source event;
13. the local target is still a real directory;
14. its device/inode/mtime identity still matches the intent;
15. the local target is not a symlink or unsupported filesystem type;
16. the predetermined remote ID is present and valid;
17. no other durable intent owns that predetermined remote ID;
18. the expected parent remote ID is present;
19. the remote catalog is fully caught up;
20. no root remote change window is open;
21. the durable root change cursor equals the cursor bound to the current
    remote write-authority snapshot;
22. the expected parent has current durable `canAddChildren=true`;
23. a fresh provider metadata read confirms the expected parent still:
    - exists;
    - is not trashed;
    - is a folder;
    - has the expected Drive version;
    - has `canAddChildren=true`;
    - remains inside the selected root topology;
24. a fresh provider change cursor equals the durable cursor immediately before
    submission.

Any mismatch stops before POST.

Local, remote and credential preconditions must never be weakened merely because
the intent was previously planned.

## Durable submission fence

A real Drive create must never be sent while the durable intent is only
`planned`.

Before POST, one SQLite transaction must atomically move the intent:

`planned -> submitted`

and persist enough recovery authority to make the attempt unambiguous.

The next implementation schema must record at least:

- `attempt_count`;
- `last_attempt_at_unix_ms`;
- `submitted_at_unix_ms`;
- the pre-submit root change cursor/fence;
- an execution generation/version allowing compare-and-set status transitions.

The opaque cursor must never be printed.

The transaction must verify that all durable source-event, baseline-generation,
root-mode and intent-status preconditions still match.

Only after this transaction commits may the provider POST occur.

This ordering is mandatory so a process crash after Drive accepts the create but
before NubiSync records the response is recoverable by the predetermined ID.

## Successful response validation

HTTP success is not enough.

The returned Drive resource must exactly match:

- predetermined `id`;
- expected leaf `name`;
- folder MIME type;
- exactly one parent equal to the expected parent;
- `trashed=false`;
- a valid Drive `version`.

A mismatch becomes `conflict` and must not be normalized or silently repaired.

A matching success transitions the intent to:

`awaiting_confirmation`

It does **not** transition directly to `confirmed`.

## HTTP 409 and retry semantics

Google documents that when a create using a pre-generated ID has already
succeeded, a subsequent retry can return HTTP `409 Conflict` and Drive does not
create a duplicate.

NubiSync therefore must not treat every 409 as either generic success or generic
failure.

After a 409, NubiSync must fetch the predetermined ID and validate the exact
folder postcondition.

If the object exists and exactly matches:

- predetermined ID;
- exact expected name;
- folder MIME type;
- exactly one expected parent;
- not trashed;

the intent may move to `awaiting_confirmation`.

If the ID exists but metadata differs, the intent becomes `conflict`.

If the exact-ID verification cannot complete because of transient network
failure, the intent remains `submitted`/ambiguous. The first implementation must
not blindly issue another create in the same recovery path.

A later supervised recovery invocation may inspect the same predetermined ID
again before deciding whether a same-ID retry is safe.

## Timeout and ambiguous provider outcomes

A connection drop, timeout, 5xx, or process crash after `submitted` is durable
must not return the intent to `planned`.

Recovery starts by reading the predetermined ID.

- exact object match -> `awaiting_confirmation`;
- mismatching object -> `conflict`;
- object absent -> a future bounded same-ID retry may be allowed only after all
  local/remote/parent preconditions are revalidated again;
- verification unavailable -> remain `submitted`.

The predetermined ID is never discarded and replaced merely because an attempt
was ambiguous.

## Change-stream confirmation contract

Provider response validation or exact-ID recovery establishes only that the
remote postcondition appears to exist.

NubiSync confirmation authority remains the root change stream and durable
catalog.

For an `awaiting_confirmation` folder intent:

1. start from the durable pre-submit change fence stored with the execution;
2. collect/commit Drive changes using the selected-root catalog rules;
3. require the catalog/change stream to observe the exact predetermined ID;
4. require exact expected name;
5. require folder kind;
6. require exactly the expected parent;
7. require `trashed=false`;
8. require the item to resolve inside the selected root;
9. advance the root change cursor only through the normal atomic catalog commit;
10. only then transition the intent:
   `awaiting_confirmation -> confirmed`.

A provider `files.get` result alone does not produce `confirmed`.

## Local settlement is deliberately separate

Even after a remote-write intent becomes `confirmed`:

- the source local event remains `pending`;
- the local baseline remains unchanged;
- no materialization receipt is fabricated;
- no local filesystem content or metadata is mutated.

A later explicit settlement phase must define how a confirmed local-to-remote
write updates local observation authority without losing unrelated concurrent
local changes.

This separation prevents an HTTP/provider confirmation from accidentally
rewriting local authority.

## Intent state transitions allowed by the folder-create slice

Allowed:

- `planned -> submitted`
- `submitted -> awaiting_confirmation`
- `submitted -> conflict`
- `submitted -> failed` only for deterministic non-ambiguous terminal provider
  rejection
- `awaiting_confirmation -> confirmed`
- `awaiting_confirmation -> conflict`

Not allowed:

- `planned -> confirmed`
- `submitted -> confirmed`
- `confirmed -> applied local event` in the same transaction/slice
- any transition that advances the local baseline
- any automatic replacement of the predetermined remote ID

Compare-and-set status transitions are required.

## Concurrency and daemon policy

The first folder-create executor remains an explicit supervised CLI operation.

`nubisyncd` must remain in TwoWay standby and must still:

- load no FullSync credential;
- perform no TwoWay network polling;
- perform no TwoWay filesystem scan;
- execute no remote-write intent.

The CLI execution owns the user-global cross-process lock for the full critical
section including:

- local revalidation;
- remote fence establishment;
- durable `submitted` transition;
- provider request/recovery check;
- immediate durable outcome transition.

Change-stream confirmation may be a separate supervised command but must also
take the same cross-process lock.

## Privacy contract

Normal output must not print:

- local root path;
- relative path or local name;
- predetermined remote ID;
- parent remote ID;
- provider object ID;
- Drive version;
- Drive cursor;
- checksum/hash;
- OAuth token;
- keyring value;
- raw provider error body if it can contain metadata.

CLI output is aggregate/status-only.

## Explicitly excluded from the first folder-create implementation

The following remain prohibited:

- ordinary file create/upload;
- multipart upload;
- resumable upload;
- file content read;
- file update;
- remote trash;
- permanent delete;
- rename/move;
- type-change automation;
- Google Workspace native file creation;
- Shared Drives;
- multi-account;
- daemon remote-write execution;
- automatic local settlement/baseline advancement.

## Proposed implementation sequence after this design

### Phase 5H8 — durable folder-create execution state foundation

No Drive object mutation yet.

Expected work:

- schema v17;
- durable submission-fence/attempt metadata;
- compare-and-set intent status APIs;
- exact `planned/submitted/awaiting_confirmation/confirmed/conflict/failed`
  transition validation;
- supervised recovery-plan inspection;
- no `files.create` provider method yet.

### Phase 5H9 — supervised folder-create submission

First actual Drive object mutation.

Expected work:

- fresh parent metadata authority read;
- exact folder `files.create` provider primitive;
- predetermined ID;
- successful-response validation;
- 409/exact-ID recovery handling;
- transition only as far as `awaiting_confirmation`;
- daemon still standby.

### Phase 5H10 — supervised change-stream confirmation

Expected work:

- confirmation catch-up from the persisted pre-submit fence;
- exact predetermined-ID catalog confirmation;
- `awaiting_confirmation -> confirmed`;
- source local event still pending;
- baseline still unchanged.

### Phase 5H11 — confirmed-write local settlement design

Only after confirmation is proven should NubiSync design applying the source
event and advancing local observation authority.

## Official Google Drive references reviewed

- https://developers.google.com/workspace/drive/api/guides/create-file
- https://developers.google.com/workspace/drive/api/guides/folder
- https://developers.google.com/workspace/drive/api/reference/rest/v3/files/create
- https://developers.google.com/workspace/drive/api/reference/rest/v3/files/get
- https://developers.google.com/workspace/drive/api/reference/rest/v3/files/generateIds
- https://developers.google.com/workspace/drive/api/guides/manage-uploads

## Phase 5H7 closure rule

5H7 closes when this document is committed and pushed while the runtime remains
unchanged:

- schema v16;
- selected root remains `two_way`;
- persistent daemon remains active in TwoWay standby;
- `files.generateIds` remains the only FullSync provider primitive;
- no `files.create` POST exists;
- no remote object mutation is performed;
- no intent state is changed by this design phase;
- no local event is applied;
- no baseline advances.

The next implementation phase is 5H8, durable folder-create execution state
foundation, still without a Drive object mutation.
