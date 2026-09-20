# Phase 5H14B — Intent-aware TwoWay metadata refresh

Status: **IMPLEMENTED AND VALIDATED**

Baseline: `68f793024fac3872afc03fe926262727d5b2cc72`

Date: 2026-09-19.

## Trigger

Phase 5H14A added a safe TwoWay metadata refresh command but initially blocked
execution whenever any durable remote-write intent existed.

That rule was too broad.

Confirmed intents are intentionally retained as durable audit facts after local
settlement. Blocking on every historical intent would permanently disable
future TwoWay metadata refresh after the first successful remote write.

Planned intents also do not yet own a pre-submit change-stream fence.

## Correct eligibility rule

`refresh-two-way-metadata --approve` now blocks only when a remote-write intent
is in a state whose confirmation/recovery correctness depends on preserving its
pre-submit change boundary:

- `submitted`
- `awaiting_confirmation`

The rule is operation-neutral and applies to all remote-write intent kinds.

The following states do not by themselves block metadata refresh:

- `planned`
- `confirmed`
- `conflict`
- `failed`
- `superseded`

Other planners/executors remain responsible for deciding whether those states
permit a later write operation.

## Durable postconditions

A generic storage query now counts intents by `RemoteWriteIntentStatus`.

The TwoWay metadata refresh records the total durable intent count before the
provider read and requires the total count to remain exactly unchanged after the
selected-root change window is committed.

The command therefore proves:

- local baseline unchanged;
- local pending-event count unchanged;
- durable remote-write intent population unchanged;
- no submitted intent existed during refresh;
- no awaiting-confirmation intent existed during refresh;
- selected-root change window cleared;
- no provider write method called;
- no remote object mutation.

Schema remains v18.

## 5H14 fixture

The existing owner-created folder and its already-journaled pending local event
remain the active real E2E fixture.

No new fixture must be created.

The next runtime phase may now safely:

1. refresh TwoWay metadata while there is no intent;
2. rebind write authority;
3. allocate one predetermined ID;
4. if a later invocation starts with a `planned` intent, refresh/rebind again
   before submission if needed;
5. never run generic metadata refresh while the intent is `submitted` or
   `awaiting_confirmation`;
6. complete change-stream confirmation and local settlement;
7. retain the confirmed historical intent without disabling future metadata
   refresh.
