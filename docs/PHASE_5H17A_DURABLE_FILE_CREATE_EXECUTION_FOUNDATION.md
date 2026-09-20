# Phase 5H17A — Durable ordinary-file create execution foundation

Status: **IMPLEMENTED AND VALIDATED**

Baseline: `ddf1c6a010838255aaa41ba786d5bc179871cba6`

Date: 2026-09-20.

## Scope

5H17A adds file-specific durable execution authority and exact predetermined-ID recovery metadata. Schema remains v18. It does not wire CLI streaming, read local file content, perform a live Drive write, confirm via change stream, settle the local baseline, or enable daemon writes.

## Durable boundary

`RemoteWriteFileCreateCandidate` carries size, mtime, device+inode, predetermined ID, parent, status and execution generation while redacting path/IDs from Debug. `begin_sync_root_file_create_submission` requires TwoWay mode, a pending created/file source event, valid local baseline, complete remote catalog, no open change window, cursor-bound write authority and a writable parent. The transition to `submitted` atomically increments attempt count/execution generation and persists the pre-submit cursor before provider work.

File transitions are operation-specific and CAS guarded. Direct planned-to-confirmed is rejected.

## Exact recovery lookup

`inspect_expected_file` is provider read-only. Exact recovery requires predetermined ID, name, parent, ordinary MIME type, byte size, positive provider version and exact lowercase SHA-256. Missing SHA-256 is not accepted as exact recovery.

## Next action

5H17B wires one supervised CLI file-create upload and submitted recovery using stable open-file identity, 8 MiB resumable chunks, streaming SHA-256, pre-submit cursor fencing and no blind replay. The daemon remains write-inert.
