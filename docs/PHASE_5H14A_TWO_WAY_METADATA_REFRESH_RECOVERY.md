# Phase 5H14A — TwoWay pre-write metadata refresh recovery

Status: **IMPLEMENTED AND VALIDATED**

Baseline: `99d0b9782e336fcc6dbf12866cb89f57f8f4ed71`

Date: 2026-09-19.

## Trigger

The first real 5H14 fixture was correctly journaled, but create-ID allocation
stopped at the remote cursor fence before `files.generateIds`.

This exposed a missing recovery path: while a root is `two_way`, normal
ReceiveOnly metadata commands are intentionally unavailable and the daemon is
write-inert. A provider cursor change could therefore make the pre-write fence
stale without a safe TwoWay metadata-only refresh command.

## Repair

Adds:

`nubisync sync roots refresh-two-way-metadata --approve`

The command:

- is TwoWay-only;
- is blocked if any remote-write intent already exists;
- uses the existing read-only credential, never FullSync;
- runs under the user-global cross-process execution lock;
- consumes a bounded selected-root change window;
- updates only selected-root remote catalog/cursor metadata;
- preserves local baseline generation/item-count/validity;
- preserves current-generation pending local events;
- clears the staged change window on success;
- performs no filesystem operation or file-content read;
- performs no provider write method or remote object mutation;
- prints no paths, names, remote IDs, cursor values or tokens.

Schema remains v18.

## Recovery order

The interrupted fixture must remain unchanged.

After this phase closes:

1. run `refresh-two-way-metadata --approve`;
2. run `observe-write-authority --approve`;
3. verify the remote-write plan still contains exactly one folder create;
4. retry `allocate-create-ids --approve`;
5. continue the frozen 5H14 create → confirmation → settlement chain.

No switch to ReceiveOnly and no blind provider retry are permitted.
