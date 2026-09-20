# Phase 5H15 — Supervised ordinary-file create/upload boundary design

Status: **DESIGN FROZEN**

Baseline: `a5b2a209dfb804c2abbeb29a052a6a764c1bd075`

Date: 2026-09-19.

## Product direction

NubiSync is not intended to expose internal synchronization transactions as a sequence of user confirmations.

The owner approvals used in Phase 5H are development/validation instrumentation, not the target desktop UX.

The product contract is:

local change
→ internal durable journal
→ automatic policy evaluation
→ provider transfer
→ provider confirmation
→ local settlement
→ convergence

without an ordinary user prompt for each step.

For normal configured synchronization, the user should experience behavior closer to a native desktop sync client: changes become synchronized naturally in the background.

Prompts are reserved for exceptional states such as OAuth/re-authentication, unresolved semantic conflicts, large/destructive anomalies, unsupported provider-native semantics, or explicit settings changes.

Normal single-file create/update/delete behavior is not intended to require a per-operation confirmation once that mutation class has completed its owner validation milestones.

The internal safety gates, durable states, fences and postconditions remain even when the final product no longer exposes an `APPROVE ...` interaction.

## Performance direction

NubiSync should avoid work that scales with the full tree when the operating system or durable journal already identifies the changed object.

The intended steady-state architecture is:

- event-driven local change detection where the platform supports it;
- durable incremental local journal;
- Drive change-cursor driven remote observation;
- metadata cache instead of repeated full provider inventory;
- no pre-upload whole-file hashing pass;
- hash calculation while streaming the upload;
- bounded upload workers with later adaptive concurrency;
- no duplicate content copy merely to obtain a hash;
- no repeated full root scan as the normal synchronization mechanism;
- bounded verification/fallback scans only when required to recover confidence.

The Phase 5H15 implementation remains deliberately single-file and supervised while proving correctness. Performance concurrency and daemon automation are separate later gates.

## Provider contract verified for this design

Google Drive supports pre-generated file IDs through `files.generateIds`, resumable upload initiation with `uploadType=resumable`, session continuation by `PUT`, `308 Resume Incomplete`, accepted-range recovery through `Range`, and non-final chunk sizes aligned to 256 KiB.

Provider references used when freezing this design:

- https://developers.google.com/workspace/drive/api/guides/create-file
- https://developers.google.com/workspace/drive/api/guides/manage-uploads

The resumable session URI is capability-sensitive and must never appear in normal logs, stdout, evidence documents, crash diagnostics or durable public state.

## Scope

5H15 freezes the boundary for the first local-to-Drive ordinary-file create.

This phase is design-only.

No provider upload primitive is added here.

No file content is read here.

No schema migration is performed here.

The first implementation slice after this design must support exactly one ordinary binary file create intent.

Excluded:

- provider-native Google Workspace documents;
- shortcuts;
- Shared Drives;
- multi-account;
- file update;
- remote trash/delete;
- rename/move;
- recursive multi-file upload;
- automatic daemon write execution;
- cross-process durable resumable-session persistence.

## File-create authority chain

The file-create path inherits the proven folder-create architecture:

1. local metadata event exists durably;
2. write plan classifies exactly one ordinary file create;
3. predetermined Drive ID is allocated and persisted in the intent;
4. TwoWay remote metadata is current;
5. write-authority snapshot is current and bound to the same cursor;
6. expected parent is writable and topologically valid;
7. local file identity is revalidated;
8. intent enters durable `submitted` before the first provider upload request;
9. resumable upload session is initiated;
10. bytes are streamed from the selected local file;
11. provider completion is verified against the predetermined ID;
12. intent remains unconfirmed until the Drive change stream observes the exact object;
13. local settlement is separate from provider confirmation;
14. settlement creates ownership/content evidence and advances the local baseline exactly once;
15. any residual local mutation is journaled as a later update instead of being silently absorbed into the create.

## Local file eligibility

The first file-create implementation accepts only:

- regular file;
- inside the configured selected root;
- no symlink at the selected path;
- no provider-native conversion request;
- exactly one pending `created` file event;
- parent already mapped to a current remote directory receipt or the configured remote root;
- stable path resolution under the selected root.

## Stable file identity

Before the provider mutation begins, the executor binds the source to metadata including at minimum:

- relative path;
- ordinary-file kind;
- device identity;
- inode identity;
- size;
- modification time with nanosecond precision;
- change/status time with the strongest platform precision available.

The implementation phase should extend durable local metadata identity where necessary rather than weakening this rule.

The file is opened once for streaming and the open file descriptor is validated against the planned identity.

After streaming completes, the same open file descriptor is re-statted.

If identity/content-relevant metadata changed during the stream, the upload result is not treated as proof that the current local file equals the remote file.

The remote create may still be recoverable/confirmable by predetermined ID, but local settlement must leave a residual local modification for a later update path.

## Content hashing

There is no separate whole-file pre-hash pass.

SHA-256 is calculated incrementally over the same bytes being streamed to the provider.

This yields one local content read for the normal create path.

The hash value is sensitive evidence: it is never printed in normal output and may be stored only in the dedicated durable ownership/content receipt required for synchronization correctness.

## Resumable upload protocol

The first provider upload primitive uses Drive resumable upload for ordinary files regardless of whether the file is small enough for a simple upload.

Reason:

- one recovery model for small and large files;
- upload progress can be resumed inside the active execution;
- ambiguous network outcomes can be status-probed;
- later daemon automation can reuse the same state machine;
- no separate multipart/simple-upload duplicate correctness path.

The initial session request contains only the metadata required for exact ordinary-file creation:

- predetermined ID;
- leaf name;
- expected parent;
- ordinary non-Google MIME semantics;
- known content length.

No Google Workspace conversion is permitted.

## Chunking

Initial implementation policy:

- default chunk target: 8 MiB;
- every non-final chunk must be an exact multiple of 256 KiB;
- final chunk may be shorter;
- byte ranges come from the provider-confirmed accepted offset;
- the client never assumes a complete chunk was accepted merely because it was transmitted.

The 8 MiB value is an implementation starting point, not a permanent product limit.

Later performance work may adapt chunk size according to measured throughput, latency and retry rate without changing durable intent semantics.

## Resumable session secrecy

For the first implementation:

- session URI exists only in process memory;
- it is redacted from `Debug`;
- it is not printed;
- it is not persisted to SQLite;
- it is not included in evidence;
- it is not exposed to the daemon's ordinary logs.

A process crash can therefore lose the session and require controlled recovery.

Durable encrypted session-secret persistence is deferred to a separate threat-model/design phase.

## Ambiguous interruption and recovery

No blind duplicate create is allowed.

If a chunk request is interrupted or receives a retryable provider failure, the executor first queries resumable-session status.

Possible outcomes:

### Provider reports an accepted range

Continue from the first unaccepted byte.

### Provider reports upload complete

Inspect the predetermined ID and verify the exact ordinary-file postcondition.

Do not initiate a second create.

### Session expired/not found

Before opening a new resumable session:

1. inspect the predetermined ID;
2. if the exact expected file exists, move to confirmation;
3. if the ID exists with mismatching metadata, enter `conflict`;
4. if the ID is missing, a new session may be initiated only if the planned local source identity still matches.

Because the same predetermined ID is reused, duplicate Drive objects are not an accepted recovery outcome.

### Process crash

On restart:

1. use the durable intent and predetermined ID;
2. inspect that ID first;
3. exact object → continue toward change-stream confirmation;
4. mismatch → conflict;
5. missing → a new resumable session may be started only after local identity, parent authority and remote cursor fences are re-established.

## Provider completion postcondition

An upload HTTP success is not enough.

The completed resource must match:

- predetermined remote ID;
- expected leaf name;
- ordinary-file kind;
- expected parent;
- not trashed;
- expected byte size.

When a provider content checksum is available, it must be compared with the content streamed by NubiSync using the corresponding algorithm.

## Change-stream confirmation

The file-create intent does not become `confirmed` directly from upload completion.

It must reuse the folder-create confirmation discipline:

- confirmation uses the read-only credential;
- change window begins from the durable pre-submit cursor;
- exact predetermined ID must be observed;
- final selected-root catalog must contain the exact ordinary file;
- name/parent/kind/size must match;
- target unseen → staged window discarded and cursor not advanced;
- mismatch/delete → conflict;
- exact target → cursor advances and intent becomes `confirmed`.

No provider write occurs during confirmation.

## Local settlement

Settlement is local-only and separate from remote confirmation.

The file-create settlement must:

- identify the exact source local event;
- preserve the uploaded source snapshot as the promoted baseline identity;
- use the upload content receipt for remote ownership;
- create/update the current file ownership/content receipt;
- advance local generation exactly once;
- mark only the source event applied;
- rebuild the residual local diff against a fresh metadata scan;
- persist residual changes as the next-generation journal;
- preserve the durable confirmed intent and settlement evidence.

If the local file changed during/after upload, settlement must not erase that fact.

The create may settle ownership while a residual `modified` event remains for a future file-update implementation.

A second settlement invocation must be a no-op.

## Durable upload evidence

The implementation phase may require schema evolution beyond v18.

Any new durable execution state must distinguish at minimum:

- planned;
- submitted/session-start attempted;
- uploading;
- provider completion observed;
- awaiting change-stream confirmation;
- confirmed;
- conflict;
- failed/superseded where applicable.

Do not persist OAuth tokens, refresh tokens, resumable session URI in the first slice, full local absolute paths, or raw content bytes.

## Daemon / production UX boundary

5H15 does not enable the persistent daemon to execute remote writes.

The daemon remains TwoWay standby during owner-validation slices.

However, this is an engineering gate, not the final UX architecture.

After file-create, file-update and remote-trash classes have each completed required live validation and recovery tests, a later phase may enable an automatic write worker under policy.

That worker should:

- consume durable local events automatically;
- execute normal sync operations without asking the user per file;
- preserve the same internal fences and recovery state machines;
- throttle/constrain concurrency internally;
- surface progress/errors through UI/status rather than approval prompts;
- pause only on exceptional policy/conflict/safety conditions.

The final user experience must not require typing `APPROVE ...` for normal synchronization.

## Performance evolution after correctness proof

The intended optimization order is:

1. prove one ordinary file create;
2. prove safe create recovery;
3. prove file-create settlement;
4. prove real E2E file create;
5. add file update;
6. add remote trash with reversible/safety policy;
7. add OS event watcher ingestion so steady-state sync does not require repeated root scans;
8. add bounded parallel upload workers;
9. add adaptive chunk sizing;
10. add durable encrypted resumable-session recovery if justified;
11. enable automatic daemon write execution;
12. benchmark throughput/latency against representative rclone/native-client workloads and optimize measured bottlenecks.

Correctness invariants must not be traded away for throughput.

## Expected implementation slices

### 5H16 — provider resumable-upload primitive

Provider-only code and tests: initiate session, upload/probe chunks, exact completion metadata. No CLI orchestration and no daemon execution.

### 5H17 — durable file-create execution/recovery

Bind planned file intent, stable local file descriptor identity, predetermined ID, pre-submit cursor, resumable provider primitive and crash/timeout recovery.

### 5H18 — file change-stream confirmation

Add exact ordinary-file confirmation without local settlement.

### 5H19 — selective file-create settlement

Add ownership/content receipt and residual diff rebasing.

### 5H20 — real ordinary-file create E2E

Owner-created file fixture. Prove one logical create attempt, no duplicate remote file, streamed content identity, change-stream confirmation, one local generation advance, settlement idempotence, clean final plan and daemon standby.

Only after 5H20 should automatic daemon write execution be considered for already-proven mutation classes.

## Closure criteria

5H15 closes as design-only when:

- this document is the only repository change;
- schema remains v18;
- no runtime/source behavior changes;
- no provider call occurs;
- worktree is clean after commit;
- local and origin/main commit hashes match;
- the canonical checkpoint records the natural-product UX direction and 5H16 next action.
