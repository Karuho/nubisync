# Phase 5H — Remote-write boundary design audit

Status: **FROZEN DESIGN — no Drive write capability enabled**

Frozen against repository baseline:

`d724704a6304528242a376632d9cab7d868a98a8`

Date: 2026-09-19.

## Purpose

Phase 5H defines the safety and authority boundary for future local-to-remote
synchronization before any Google Drive write method is implemented.

This phase is documentation only. It does not add a stronger OAuth grant,
provider write methods, TwoWay activation, file-content upload, remote mutation,
or a new daemon execution path.

The next implementation slice is Phase 5H1: durable remote-write intent
foundation with no network write.

## Current closed foundations

The design assumes these already-closed properties:

- ReceiveOnly convergence is functional and persistent.
- local metadata observation is durable and generation-bound.
- local change events use `pending`, `applied`, `superseded`, and `failed`.
- ReceiveOnly filesystem mutations invalidate the local observation baseline.
- local periodic observation runs only after ReceiveOnly convergence.
- cross-process execution ownership coordinates persistent `nubisyncd` and
  mutation-capable CLI flows.
- normal CLI output does not expose local paths/names, remote IDs, cursors,
  tokens, hashes, or the execution-lock path.
- Drive write access is still disabled.

## Repository gaps found by the audit

The current provider and durable models are intentionally insufficient for safe
remote writes:

1. `GoogleDriveApi` has read/probe/list/change/download primitives but no create,
   update, trash, or upload primitives.
2. `RemoteItem` and the selected-root remote catalog do not persist the Drive
   `version` field.
3. the remote catalog does not persist an MD5 content checksum or write
   capabilities.
4. a pending local change event is path-oriented and baseline-generation-bound,
   but is not bound to a remote object identity or remote version.
5. file materialization receipts already preserve useful remote-ID/path/SHA-256
   ownership evidence, but they are ReceiveOnly receipts, not remote-write
   authorization.
6. there is no durable remote-write intent state machine.
7. no FullSync refresh token is active in normal runtime.

A local change event therefore MUST NOT be sent directly to Drive.

## OAuth authority decision

NubiSync already models:

- `drive.metadata.readonly`
- `drive.readonly`
- `drive`

`GoogleDriveAccess::FullSync` maps to:

`https://www.googleapis.com/auth/drive`

For NubiSync's current product model—an arbitrary user-selected My Drive root
whose existing descendants are synchronized—the FullSync implementation profile
will use the `drive` scope.

`drive.file` MUST NOT be silently substituted for this profile. Google documents
`drive.file` as per-file access for files the user opens with or shares with the
app, commonly through Google Picker. NubiSync currently selects a root by Drive
identity and expects to synchronize arbitrary existing descendants; this is not
the same authority model.

A future Picker-based limited-access product profile MAY be designed separately,
but it must not weaken or ambiguously redefine the current selected-root
semantics.

The `drive` scope is a restricted scope. Public distribution therefore has
verification/compliance consequences that must be handled as a release/legal
readiness item.

## OAuth upgrade contract

Future write authorization must use an explicit supervised command, conceptually:

`nubisync auth google upgrade-full-sync --approve`

The exact command name is not implemented by this design phase.

Required behavior:

1. the existing ReceiveOnly/read-only credential remains intact while the
   upgrade is attempted.
2. the authorization flow explicitly requests `drive`.
3. returned scopes must include the exact `drive` scope.
4. OpenID Connect subject must exactly match the configured account.
5. a fresh refresh token is required before promotion.
6. the FullSync refresh token must be stored under a distinct OS-keyring purpose
   from the ReceiveOnly token.
7. failure must leave the ReceiveOnly credential untouched.
8. obtaining FullSync authority must NOT automatically change a root's sync mode.
9. changing a root from ReceiveOnly to a write-capable mode requires a separate
   explicit owner action.
10. access tokens remain memory-only.

This produces two independent gates:

- write credential present
- write-capable root mode explicitly enabled

Both are required before any remote mutation executor can run.

## Google Drive write surface

The first supported write surface is intentionally narrow.

### Create ordinary file

Use `files.generateIds` first and persist the pre-generated Drive ID in the
durable intent before upload.

Then use `files.create` with:

- the predetermined `id`
- exact name
- exactly one expected parent
- ordinary binary content
- no Google Workspace format conversion

Pre-generated IDs make retry identity stable. A completed prior create can be
recognized by the same ID instead of creating a duplicate.

### Create folder

Use a pre-generated Drive ID and `files.create` with:

`application/vnd.google-apps.folder`

The expected parent must be explicit.

### Update ordinary file content

Use `files.update` on the exact durable remote ID.

Content upload should use resumable upload. A fresh remote metadata read and
conflict check is mandatory before initiating the upload.

### Delete local item -> remote trash

Initial sync deletion MUST use reversible trash semantics:

`files.update` with `trashed=true`

Initial TwoWay support MUST NOT call `files.delete` for normal local deletion.

Permanent delete is outside the first write-capable boundary.

### Type change

`file <-> directory` type changes are a conflict/manual-intervention state in the
first write-capable boundary.

They MUST NOT be implemented as an implicit trash+create sequence.

### Rename and move

Rename coalescing remains disabled.

Until a dedicated rename/move phase exists, local rename remains represented by
the existing delete+create observation model. Automatic remote rename/move is
outside the first write-capable boundary.

## Remote metadata required before write implementation

The provider-neutral remote model must be extended before write execution.

At minimum, ordinary Drive catalog observations used for write planning need:

- remote ID
- parent remote ID
- name
- item kind
- size
- modified time
- trashed state
- Drive `version`
- optional `md5Checksum` for ordinary binary files
- ownership/capability information needed for the intended operation

Relevant capability checks include, as applicable:

- canEdit
- canDelete / ability to trash
- canAddChildren
- canMoveItemWithinDrive
- canRename

The exact minimal subset should be encoded per operation rather than storing
every Drive capability indiscriminately.

Drive `version` is a monotonically increasing server-side version number and is
the primary remote-change precondition signal in this design.

## Durable remote-write intent

Phase 5H1 must add a durable intent layer separate from
`sync_root_local_change_events`.

A write intent must bind, at minimum:

- selected sync root
- source local-change event ID
- local baseline generation
- operation kind
- relative path
- current local item kind
- exact local identity used for the plan
- target remote ID when one already exists
- predetermined remote ID for create
- expected parent remote ID
- expected remote kind
- expected remote Drive version for existing targets
- expected remote size/checksum when available and relevant
- observed/planned timestamp
- durable status

The intent's `Debug` representation and normal CLI surfaces must redact path,
remote ID, checksums, upload session information, and provider cursors.

### Initial intent statuses

The first durable state machine should distinguish:

- `planned`
- `submitted`
- `awaiting_confirmation`
- `confirmed`
- `conflict`
- `failed`
- `superseded`

A local event must not become `applied` merely because an HTTP write returned
success.

## Intent derivation preconditions

A local event may become a remote-write intent only if all of these hold inside
the cross-process execution ownership boundary:

1. the selected root is explicitly write-capable.
2. FullSync credential authority is present.
3. local observation baseline is complete and valid.
4. the event is still `pending`.
5. the event baseline generation matches current durable local authority.
6. a fresh local metadata observation still matches the event.
7. selected-root remote catalog is authoritative and fully caught up.
8. no pending remote change window exists.
9. the target's remote identity is unambiguous.
10. parent topology is complete and unambiguous.
11. the required provider capability permits the operation.
12. unsupported native Google Workspace items are not involved.

If any precondition fails, no write intent is executed.

## Conflict detection before every remote mutation

Immediately before a network write, NubiSync must fetch fresh metadata for every
existing target and relevant parent.

For existing targets, the fresh remote `version` must equal the version bound to
the durable intent. Kind, parent, trashed state and any operation-relevant size or
checksum must also remain compatible with the intent.

For create operations, the expected parent must still exist, remain the expected
kind, remain inside the selected root, and permit child creation.

A mismatch becomes `conflict`; it is not retried as a blind overwrite.

The current Drive v3 method references used by this design do not document a
Drive-specific optimistic `If-Match` contract for `files.update`/`files.delete`.
NubiSync therefore must not depend on an undocumented ETag precondition.
Read-before-write remote version/capability validation is mandatory.

## Local-content stability before upload

Before uploading an ordinary local file:

1. resolve the path beneath the canonical root.
2. reject symlinks or unsupported types.
3. capture device/inode/size/mtime/ctime identity.
4. stream the file while hashing SHA-256.
5. re-check identity after the read.
6. abort if the file changed while being read.
7. bind the resulting byte count and SHA-256 to the write execution record.

The first write implementation must remain bounded by an explicit local upload
size policy even though Drive supports much larger files.

## Upload strategy and recovery

Resumable upload is the default design for file create/update because it provides
explicit interruption recovery semantics and is suitable for bounded as well as
larger uploads.

NubiSync must not print or persist resumable session URIs as normal metadata.
They are capability-bearing provider URLs and are treated as sensitive runtime
state.

Initial recovery strategy:

- create: use predetermined Drive ID; after ambiguity/crash, query that ID and
  verify the remote postcondition before deciding whether to retry.
- update: after ambiguity/crash, fetch fresh remote metadata/version and verify
  content metadata before retrying.
- trash: after ambiguity/crash, fetch the target and verify `trashed`.
- never blindly replay an ambiguous mutation.

## Remote confirmation boundary

A successful HTTP response is not sufficient to advance local synchronization
authority.

After submitting a write:

1. durable intent becomes `awaiting_confirmation`.
2. the normal Drive change stream observes the resulting remote state.
3. selected-root catalog applies the authoritative change.
4. the observed remote ID/version/postcondition must match the intent.
5. only then may the intent become `confirmed`.
6. only confirmed intent may allow the source local event to become `applied`.
7. only a later explicit baseline-advance/rebaseline transaction may replace the
   local observation baseline.

This avoids silently advancing local authority before Drive's authoritative
change stream has confirmed the mutation.

## Feedback-loop prevention

A remote change produced by NubiSync itself will reappear through Drive changes.

The write-intent correlation layer must recognize its exact remote ID and
expected post-write state. A confirmed self-originated remote change must not be
interpreted as an unrelated remote edit that overwrites the local source bytes.

If the observed remote state differs from the intended postcondition, it is a
conflict, even if the remote ID matches.

## Ordering

Initial deterministic write ordering:

1. create parent directories before descendant creates.
2. create/update ordinary files after required parents exist.
3. trash files before trashing directories that contain them.
4. trash directories deepest-first.
5. never mix an unresolved conflict with later destructive operations in the
   same batch.

First implementation batches should be deliberately small.

## Non-goals for the first write-capable boundary

Not included:

- Shared Drives
- multiple accounts
- Google-native Docs/Sheets/Slides mutation
- permission/sharing changes
- permanent delete
- arbitrary rename/move coalescing
- type-change automation
- conflict auto-merge
- server-side NubiSync storage of refresh tokens
- unattended scope escalation
- bypass of cross-process execution ownership

## Compliance note

Google currently classifies `drive` and `drive.readonly` as restricted Drive
scopes. Google explicitly lists backup/sync applications among application types
that can qualify for restricted scopes.

Public release planning must include the applicable OAuth verification and
security-assessment obligations. This design decision is functional architecture,
not a claim that production verification is already complete.

## Official references reviewed

- https://developers.google.com/workspace/drive/api/guides/api-specific-auth
- https://developers.google.com/workspace/drive/api/reference/rest/v3/files
- https://developers.google.com/workspace/drive/api/reference/rest/v3/files/create
- https://developers.google.com/workspace/drive/api/reference/rest/v3/files/update
- https://developers.google.com/workspace/drive/api/reference/rest/v3/files/delete
- https://developers.google.com/workspace/drive/api/reference/rest/v3/files/generateIds
- https://developers.google.com/workspace/drive/api/guides/manage-uploads
- https://developers.google.com/workspace/drive/api/guides/delete
- https://developers.google.com/workspace/drive/api/guides/folder

## Phase 5H closure rule

Phase 5H is closed when this contract is committed and the runtime remains
unchanged with:

- schema v14
- ReceiveOnly service active
- Drive write access not added
- no provider write method added
- no stronger OAuth token requested

The next implementation step is Phase 5H1: durable remote-write intent
foundation only, with no Drive write request.
