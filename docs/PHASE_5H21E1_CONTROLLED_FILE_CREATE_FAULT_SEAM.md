# Phase 5H21E1 — Controlled ordinary-file proof fault seam

Status: **SOURCE IMPLEMENTATION**

Date: 2026-09-21.

Baseline: Phase 5H21D closed PASS.

## Goal

Provide deterministic proof-only controls for the final Phase 5H21 runtime
validation without changing the production daemon write boundary.

The seam exists in the development CLI only in effect. Release builds treat all
fault selectors as `None`.

## Fault selector

Environment variable:

`NUBISYNC_FILE_CREATE_PROOF_FAULT`

Accepted debug values:

- `partial_first_chunk`
- `abort_after_session_init`
- `none`

Any other debug value fails closed.

When `cfg!(debug_assertions)` is false, every value is ignored and resolves to
`None`.

The daemon never reads this variable.

## Partial-prefix proof

`partial_first_chunk` deliberately transmits exactly one Drive alignment unit
(256 KiB) from the first retained request while the upload buffer continues to
retain the full prepared request.

This is a real provider request, not a synthetic `Range` response.

Drive should acknowledge that transmitted prefix. The normal 5H21B logic must
then:

- accept the provider next offset;
- retain the unacknowledged suffix;
- continue from the provider prefix;
- avoid rereading or double-hashing retained bytes;
- complete through the ordinary provider protocol.

The upload buffer now exposes
`record_transmission_bytes(request, transmitted_bytes)` so proof accounting
records actual network bytes while preserving the full retained request.

The ordinary `record_transmission(request)` path remains unchanged in behavior.

## Lost-session proof

`abort_after_session_init` allows the development CLI to:

1. commit the durable submitted state;
2. initiate one real resumable session;
3. print only non-sensitive proof markers;
4. return a controlled error before the first content PUT.

The session URI remains process-memory only and is lost when the CLI process
ends.

A later normal recovery invocation must begin with predetermined-ID inspection.
If the ID is Missing, all 5H21D fences and the 5H21C restart CAS are required
before exactly one replacement session.

The same selector is also recognized after replacement-session initiation so a
future adversarial proof can demonstrate a second lost session without blind
same-invocation replay.

## Production boundary

- no provider primitive is changed;
- no session URI is exposed;
- release CLI builds ignore the proof selector;
- production daemon code does not reference the proof variable;
- persistent daemon remote-write execution remains disabled;
- schema remains v18.

## Runtime proof split

This source closure performs no provider request and no runtime DB mutation.

Phase 5H21E2 will create new dedicated fixtures and run:

1. a real partial-prefix upload;
2. a real lost-session upload followed by fenced recovery.

The closed Phase 5H20 fixture is not reused or deleted.
