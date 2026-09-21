# Phase 5H20C — TwoWay rebaseline catch-up bridge

Status: **SOURCE IMPLEMENTATION**

Date: 2026-09-20.

## Trigger

Phase 5H20B correctly rebuilt the current remote snapshot under a fresh Drive
change fence and left the snapshot in the intentional bootstrap state:

- `snapshot_complete=true`
- `catchup_complete=false`
- `catchup_from_cursor=<fresh fence>`
- `change_cursor=NULL`

The existing `refresh-two-way-metadata` command, however, required
`ready_for_reconciliation()` before collecting changes. That rejected the exact
state produced by the rebaseline and left no TwoWay CLI route for completing
the initial catch-up.

`metadata-step` is not the solution because it intentionally supports only
ReceiveOnly roots.

## Fix

`refresh-two-way-metadata` now selects its base cursor according to durable
remote state:

- if initial catch-up is incomplete, use `catchup_from_cursor`;
- if initial catch-up is complete, use the durable incremental `change_cursor`;
- if the required cursor is missing, fail closed.

The existing durable change-window collector already supports both states. The
existing atomic change-window commit marks the first catch-up complete and
persists the resulting durable cursor.

The command still preserves the local baseline and pending-event count,
preserves the durable remote-write intent population, blocks submitted and
awaiting-confirmation intents, performs metadata-only Drive reads, performs no
provider write or remote-object mutation, and keeps persistent daemon
remote-write execution disabled.

The command now reports `INITIAL_CATCHUP_COMPLETED=yes` when that invocation
transitions a rebaseline/bootstrap snapshot to a fully caught-up catalog.

## Cleanup

`ChangePage` is used only by storage tests. Its import was moved from production
scope into the test module, removing the previous unused-import warning from
normal library and workspace builds.

## Next action

Run the retained Phase 5H20 runtime sequence:

`rebaseline-two-way-metadata -> refresh-two-way-metadata -> observe-write-authority -> remote-write-plan`

with bounded retries only for provider-boundary races after catch-up.

Do not replay upload, confirmation recovery, or settlement.
