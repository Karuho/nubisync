# Phase 5H20B — Extreme backlog remote metadata rebaseline

Status: **SOURCE IMPLEMENTATION**

Date: 2026-09-20.

## Trigger

The first real supervised ordinary-file create reached a safe post-settlement
state, but the Google Drive account-wide My Drive change feed contained an
extreme historical backlog.

Runtime evidence exceeded 2,700 staged pages and 138,000 provider changes while
continuation tokens remained unique and pagination progress remained internally
consistent.

Continuing to consume that history is correct but operationally unreasonable
when the selected sync root can be rebuilt from current provider metadata.

## Supervised command

`nubisync sync roots rebaseline-two-way-metadata --approve`

The command is metadata-only and read-only against Google Drive.

It requires a `two_way` root, a complete valid local baseline, zero pending
events in the current local generation, no active/conflict write intent, every
confirmed intent already settled, and an incomplete durable change window with
at least 64 pages.

## Algorithm

1. Resolve and validate the configured remote root.
2. Capture a fresh Drive change fence before listing the current root tree.
3. Build a complete current metadata snapshot in non-authoritative staging.
4. Revalidate root identity after traversal.
5. Atomically replace the durable remote catalog, bind the fresh fence, mark
   catch-up incomplete, discard the historical window/events/tokens, invalidate
   stale write authority, and clear staging.
6. Preserve the local baseline and durable remote-write intents.
7. Perform normal read-only catch-up from the fresh fence.
8. Observe write authority only after the new catalog has caught up.

If snapshot construction or any compare-and-set precondition fails, the old
durable catalog and historical change window remain authoritative.

## Safety

The rebaseline never treats a fresh token as a snapshot by itself. The current
selected-root tree is rebuilt first, while the fence captured before traversal
ensures concurrent changes are recovered by the subsequent catch-up.

No file-content read, local filesystem mutation, Drive write, remote-object
mutation, upload replay, fixture cleanup, or schema change is introduced.

Persistent daemon remote-write execution remains disabled.

## Next action

Run one supervised runtime rebaseline on the retained Phase 5H20 state, perform
the short catch-up from the fresh fence, observe write authority, require a
clean final remote-write plan, and close Phase 5H20.

Phase 5H21 remains interrupted/expired ordinary-file upload recovery hardening.
