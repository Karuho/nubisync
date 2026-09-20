# Phase 5H14 — Real supervised folder-create end-to-end owner runtime

Status: **CLOSED PASS**

Runtime baseline after 5H14A/5H14B recovery fixes:

`6bf395e49ec7dfcf91d8c1edf4da74e134ce0cb7`

Date: 2026-09-19.

## Scope

Phase 5H14 executed the first real supervised local-to-Google-Drive
folder-create chain frozen by Phase 5H13.

No Rust source was changed.

The local fixture was created manually by the owner. NubiSync did not create,
rename, populate or remove the local fixture.

## Proven end-to-end chain

The validated authority sequence was:

owner-created local empty folder
→ durable TwoWay local journal
→ supervised TwoWay metadata refresh using read-only credential
→ write-authority rebind to the current selected-root cursor
→ one predetermined Drive ID
→ one durable `CreateFolder` intent
→ one supervised Drive folder create
→ Drive change-stream confirmation
→ one confirmed intent
→ selective local settlement
→ generation 2 → 3
→ current directory ownership receipt
→ durable settlement evidence.

The run resumed the already-existing owner fixture after 5H14A/5H14B closed
the stale-cursor recovery gap. No new fixture was created by 5H14C.

The run may also resume from later durable states across an explicitly
controlled invocation boundary:

- execution resumed: `yes`

No resume path creates a second fixture or blindly repeats `files.create`.

## Remote mutation boundary proven

The folder-create executor remained bounded to one selected intent.

The mutation path preserved:

- durable `submitted` state before provider POST;
- predetermined remote identity;
- local identity revalidation;
- FullSync credential isolation to the supervised CLI;
- Google subject verification;
- fresh parent capability/version validation;
- parent topology validation;
- pre-submit cursor fence;
- no direct transition from submission to `confirmed`.

Any ambiguous submitted state is recoverable through exact predetermined-ID
inspection without a blind second POST.

## Change-stream confirmation proven

Remote confirmation remained separate from provider submission.

The exact target was required to appear in the Drive change stream and final
selected-root catalog before the durable intent became `confirmed`.

Confirmation performed no provider write and did not apply the local event or
advance the local baseline.

## Local settlement proven

Settlement was a separate local-only approval.

It proved:

- source identity at settlement uses directory device+inode;
- original directory mtime is not an identity requirement at settlement;
- fresh double metadata scan;
- selective baseline promotion only;
- residual diff rebase;
- zero residual pending events for the first isolated fixture;
- exact source event `pending -> applied`;
- local generation advanced exactly once: 2 -> 3;
- current directory ownership receipt exists;
- one durable `sync_root_remote_write_settlements` row exists;
- intent remains `confirmed`;
- no Google Drive request occurs during settlement.

A second settlement invocation was a live no-op and did not advance generation
again.

## Final durable state

- SQLite schema: v18
- root mode: `two_way`
- local observation baseline: complete and valid
- local current-generation pending events: 0
- open remote change window: 0
- folder-create intents: 1
- confirmed folder-create intents: 1
- submitted folder-create intents: 0
- awaiting-confirmation folder-create intents: 0
- conflict folder-create intents: 0
- failed folder-create intents: 0
- settlement rows: 1
- confirmed-but-unsettled folder intents: 0
- source local event status: `applied`
- synchronized directory ownership receipt: current
- intent-aware TwoWay metadata refresh preserved the historical confirmed intent
- write-authority metadata refreshed against the final selected-root cursor
- final remote-write plan clean
- FullSync credential remains present only for explicit CLI use

The historical confirmed intent and settlement evidence remain durable audit
facts.

## Daemon boundary

The persistent service was stopped for fixture/runtime ownership and restored
after validation.

A bounded current daemon run proved:

- `MODE=two_way`
- `WRITE_CAPABLE_STANDBY=yes`
- `FULLSYNC_CREDENTIAL_LOADED=no`
- `REMOTE_WRITE_EXECUTION_ENABLED=no`
- no network check
- no filesystem scan
- no Drive write access

The persistent user service was active again before Git closure.

## Privacy

Persisted validation evidence contains aggregate state only.

The runtime evidence does not intentionally persist or print:

- sync-root path;
- fixture path/name;
- remote IDs;
- parent IDs;
- Drive versions;
- Drive cursors;
- OAuth tokens;
- refresh tokens;
- keyring values;
- device IDs;
- inodes.

Raw CLI stderr is not persisted by the validation harness.

## Fixture retention

The successful test folder remains present locally and remotely.

Phase 5H14 performs no automatic cleanup because a proven TwoWay remote-trash
workflow is not yet closed.

## Closure

Phase 5H14 closes the first real supervised folder-create E2E path.

The next design slice is Phase 5H15: supervised ordinary-file create/upload
boundary design, preserving the same durable intent, predetermined-ID,
change-stream confirmation and separate settlement architecture while adding
bounded stable local-content reading and resumable upload recovery semantics.
