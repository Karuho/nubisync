# Phase 5H17B — Supervised ordinary-file create upload and recovery

Status: **IMPLEMENTED AND VALIDATED IN SOURCE/TESTS**

Baseline: `3ac5d0d143218e7c56d5633c35015c5e647a5882`

Date: 2026-09-20.

## Scope

5H17B wires the first supervised ordinary-file create path to the 5H16
resumable provider primitive and the 5H17A durable execution state.

No live provider write is performed by the closure script itself.
Persistent daemon remote-write execution remains disabled.
Schema remains v18.

## New supervised CLI surfaces

- `nubisync sync roots submit-file-create --approve`
- `nubisync sync roots recover-file-create-submission --approve`

These approval gates are development/owner-validation instrumentation, not the
final product UX.

## Normal create path

The command selects exactly one planned `CreateFile` intent and:

1. opens the exact current local file after double metadata validation;
2. binds the open descriptor to size, mtime, device and inode;
3. verifies FullSync identity;
4. verifies durable/fresh parent authority and topology;
5. verifies provider cursor equals the durable cursor;
6. commits durable `submitted` before resumable initiation;
7. streams content in 8 MiB chunks;
8. computes SHA-256 over those same bytes, with no pre-hash pass;
9. persists size + SHA-256 before the final PUT;
10. accepts only provider-confirmed offsets;
11. status-probes an ambiguous chunk result instead of blind replay;
12. validates provider content SHA-256 or exact predetermined-ID metadata;
13. re-stats the same open file/path after streaming;
14. stores remote version and moves to `awaiting_confirmation`.

The local event remains pending and the baseline is not advanced.

## Crash/ambiguity recovery

Submitted recovery performs no provider write.

If the stream fingerprint is durable, it is reused. If not, recovery hashes the
current exact local source under stable identity and records that fingerprint
before the provider lookup.

Exact recovery requires ID, leaf name, parent, MIME, size, SHA-256 and positive
remote version.

Outcomes:

- exact → `awaiting_confirmation`;
- mismatch → `conflict`;
- missing → remain `submitted`, no blind replay.

Restarting an expired/missing resumable session is deliberately left for a
separate exceptional recovery slice.

## Durable content evidence

No schema migration is needed. Existing intent fields hold size, SHA-256,
remote kind and remote version.

SHA-256 is never printed.

## Local source race behavior

The open file descriptor remains the byte source for the upload. Size, mtime,
device, inode and Linux ctime are compared before/after streaming. A source
change is reported and later settlement must preserve a residual local
modification rather than silently erasing it.

## Non-goals

5H17B does not implement change-stream confirmation, local settlement, file
update, remote trash, automatic session restart or persistent daemon writes.

## Next action

Phase 5H18: implement ordinary-file create change-stream confirmation from the
durable pre-submit cursor and exact uploaded-content evidence. Phase 5H19 then
adds selective settlement.
