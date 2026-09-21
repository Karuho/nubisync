# Phase 5H21C — Durable ordinary-file restart CAS

Status: **SOURCE IMPLEMENTATION**

Date: 2026-09-21.

Baseline: Phase 5H21B closed PASS.

## Scope

This slice adds only the durable authority transition required before a
replacement Google Drive resumable session may be initiated.

It does not wire restart into the CLI and performs no provider mutation.

## Storage transition

New storage method:

`restart_sync_root_file_create_submission`

The transition is deliberately:

`submitted -> submitted`

and requires exact expected:

- `execution_generation`;
- `attempt_count`;
- current catalog/write-authority cursor.

On success it atomically:

- increments `attempt_count` by exactly one;
- increments `execution_generation` by exactly one;
- refreshes `last_attempt_at_unix_ms`;
- refreshes `submitted_at_unix_ms`;
- replaces `pre_submit_change_cursor`;
- preserves the same intent;
- preserves source event;
- preserves predetermined remote ID;
- preserves expected parent;
- preserves any durable SHA-256 content evidence;
- leaves status `submitted`.

## Fail-closed preconditions

The transaction requires:

- root remains `two_way`;
- operation remains `create_file`;
- status remains exactly `submitted`;
- expected attempt and execution generation match;
- no completion/confirmation/terminal timestamps are present;
- local baseline snapshot is complete and observation-valid;
- local generation equals the intent baseline generation;
- source event remains the same pending `created/file` path;
- remote catalog snapshot is complete and caught up;
- durable catalog cursor equals the supplied fresh fence;
- no remote change window is open;
- write-authority state exists at the same cursor;
- authority row count and authority-state item count equal catalog descendants
  plus the selected root;
- expected parent still has durable `can_add_children`;
- no settlement exists for the intent;
- optional content evidence is either entirely absent or a coherent
  size-matching lowercase SHA-256 tuple;
- expected remote completion version is absent.

Any mismatch rolls back without changing attempt count or generation.

## Attempt semantics

This transition represents authorization for exactly one new provider mutation
attempt.

It is not used for:

- status probes;
- chunk retransmission;
- partial-prefix recovery;
- predetermined-ID inspection;
- Exact recovery without a new session.

Those operations therefore do not increment `attempt_count`.

## Tests

Focused storage tests verify:

- success increments attempt and execution generation once;
- durable content evidence and deterministic IDs are preserved;
- a stale second CAS cannot increment again;
- stale attempt count leaves state unchanged;
- open change window blocks restart;
- invalid local observation blocks restart;
- revoked parent authority blocks restart;
- cursor mismatch blocks restart;
- authority coverage mismatch blocks restart.

## Safety

Schema remains v18.

No CLI command invokes the restart CAS in this phase.

No provider API primitive is changed.

No network or remote-object mutation occurs during source closure.

Persistent daemon remote-write execution remains disabled.

## Next

Phase 5H21D wires supervised lost/expired-session recovery:

predetermined-ID inspection -> Exact/Mismatch/Missing -> fresh local/provider
fences -> restart CAS -> at most one replacement resumable session.
