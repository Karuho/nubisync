# Phase 5H21A — Interrupted / expired ordinary-file upload recovery design

Status: **DESIGN FROZEN**

Date: 2026-09-21.

Baseline: Phase 5H20 closed PASS.

## Scope

This phase designs recovery for an ordinary-file `create_file` resumable upload
after either an ambiguous chunk outcome while the current process still owns
the resumable session URI, or loss/expiry of the resumable session.

The design does not enable persistent daemon writes and does not introduce any
automatic background provider mutation.

## Provider protocol contract

Google Drive resumable upload behavior is treated as follows:

- the resumable session URI is capability-sensitive and remains process-memory
  only;
- non-final upload requests use payload sizes that are multiples of 256 KiB;
- a final request may be shorter;
- `200` / `201` means upload completion;
- `308 Resume Incomplete` is authoritative only through the provider `Range`
  response;
- absence of `Range` on `308` means zero bytes accepted;
- the client never assumes that every transmitted byte was accepted;
- an empty status `PUT` uses `Content-Range: bytes */TOTAL`;
- `404` means the resumable session is expired and cannot be reused.

## In-process ambiguous chunk recovery

The uploader must retain the current pending upload buffer until Drive
acknowledges every byte in that buffer or returns completion.

The buffer has an absolute `start_offset`. Bytes enter the buffer only by
reading forward from the opened local source. Those source bytes are fed into
SHA-256 exactly once at first read.

Retransmission never feeds SHA-256 again.

After an ambiguous upload outcome:

1. query the resumable session status;
2. interpret `Range` as the authoritative accepted prefix;
3. require `next_offset` to be between the pending buffer start and the highest
   byte already read from the source;
4. discard only the acknowledged prefix from the retained buffer;
5. retain and resend only the unacknowledged suffix;
6. if more source bytes are needed to make a non-final request aligned to
   256 KiB, append newly read bytes to the retained suffix;
7. hash only those newly read bytes;
8. do not advance unrelated source data before the pending range is covered.

A provider offset behind the last acknowledged boundary, ahead of bytes
actually read, or otherwise inconsistent is a fail-closed resume-offset
mismatch.

This permits none, partial, full, or complete acceptance of the current
request.

## Streaming and SHA-256 invariant

The local source remains a single forward source stream for one provider
attempt.

`bytes_streamed` means unique local source bytes read, not network bytes sent.

Therefore retransmitted bytes are not rehashed, buffer top-up bytes are hashed
once when first read, final SHA-256 represents exactly the local file content
once, and network bytes may exceed local `bytes_streamed`.

The durable stream fingerprint must exist before a final provider request is
allowed to complete into `awaiting_confirmation`.

## Session expiration or process loss

The resumable session URI is never persisted to SQLite, logs, evidence files,
checkpoint files, command output, or Git.

When the session is unavailable or Drive returns `404`, the first recovery
action is always a metadata inspection of the predetermined remote ID.

### Exact

If ID, name, ordinary-file MIME type, parent, non-trashed state, size and
SHA-256 are exact, record the provider completion version and transition the
intent from `submitted` to `awaiting_confirmation`.

No new upload session is created.

### Mismatch

Transition the intent to `conflict`.

No upload replay occurs.

### Missing

A new resumable provider attempt is allowed only after every restart fence
below is revalidated.

Missing by itself never authorizes a blind restart.

## Restart fences

Before creating a replacement resumable session, recovery must revalidate:

1. root remains `two_way`;
2. intent remains exactly `submitted`;
3. source event is still the same pending `created/file` event;
4. local baseline generation and observation remain valid;
5. local source still matches relative path, kind, size, mtime, device and
   inode;
6. durable content evidence, when present, remains bound to the same candidate;
7. remote catalog is complete and caught up;
8. there is no open remote change window;
9. durable catalog cursor equals durable write-authority cursor;
10. parent remains an in-scope folder;
11. durable parent authority still allows adding children;
12. fresh parent authority matches the durable version and still allows adding
    children;
13. fresh parent topology still resolves inside the selected Drive root;
14. provider cursor immediately before restart equals the durable
    catalog/authority cursor;
15. predetermined target ID is still `Missing`.

Any failed fence stops recovery without provider mutation.

## Durable restart transition

A restart is a new provider mutation attempt and must be durable before a new
resumable session is requested.

Storage will gain a narrow compare-and-set transition named:

`restart_sync_root_file_create_submission`

Conceptual transition:

`submitted -> submitted`

with exact expected execution generation and attempt count, followed by:

- `attempt_count += 1`;
- `execution_generation += 1`;
- refreshed attempt/submission timestamps;
- replacement of `pre_submit_change_cursor` with the newly validated fence;
- preservation of source event, deterministic target ID, parent ID and durable
  content fingerprint;
- no new intent.

The provider session initiation occurs only after this transition commits.

## Attempt semantics

`attempt_count` counts provider mutation attempts, not HTTP chunk retries.

Therefore:

- status probe: no increment;
- retransmitting part or all of a chunk in the same resumable session:
  no increment;
- predetermined-ID inspection: no increment;
- Exact recovery without a new session: no increment;
- expired/lost session followed by a replacement resumable session:
  increment exactly once before initiation.

## Replacement-session failure semantics

After restart CAS, an ambiguous replacement attempt remains `submitted` at the
new execution generation. Recovery must inspect/query before any later restart.

It must never decrement attempt count, create a new predetermined ID, silently
reset to `planned`, or initiate multiple replacement sessions in one recovery
invocation.

## Process-local upload buffer

Implementation should introduce a process-local abstraction responsible for:

- absolute buffer start offset;
- retained bytes not yet provider-acknowledged;
- unique source bytes read;
- network bytes transmitted;
- refill toward the default 8 MiB target;
- 256 KiB alignment for non-final requests;
- suffix retention after partial acceptance;
- final-source detection;
- no session-secret persistence.

Durable authority remains in SQLite, not in this buffer.

## Provider primitive boundary

The provider parser already exposes `Incomplete { next_offset }`, `Complete`
and `Expired`.

The provider remains responsible for HTTP/session protocol, `Range` parsing and
exact completion postconditions. It does not decide whether restart is
authorized.

## CLI recovery behavior

`submit-file-create --approve` will gain safe partial-prefix handling inside
the current session.

`recover-file-create-submission --approve` will evolve from inspect-only
recovery to:

1. validate durable candidate/evidence;
2. inspect predetermined ID;
3. Exact -> awaiting confirmation;
4. Mismatch -> conflict;
5. Missing -> execute every restart fence;
6. commit restart CAS;
7. initiate one replacement resumable session;
8. stream with the same retained-buffer protocol;
9. finish in `awaiting_confirmation`, `conflict`, or durable `submitted`
   ambiguity.

One invocation may initiate at most one replacement session.

## Required tests

Provider/buffer tests must cover no Range, full acceptance, partial acceptance,
suffix retransmission, no double hashing, aligned refill, unaligned final
suffix, offset regression, offset beyond bytes read, Expired, and secret
redaction.

Storage tests must cover exact restart increment, stale generation/attempt CAS,
wrong status/operation/root, local authority mismatch, open change window,
catalog/authority cursor mismatch, missing/revoked parent authority, and
preservation of deterministic ID/content evidence.

CLI tests must cover partial recovery success, zero accepted bytes, full
current-chunk acceptance by status, Expired+Exact, Expired+Mismatch,
Missing+stale local identity, Missing+remote-fence mismatch,
Missing+valid-fences one restart, second ambiguity without blind replay, no
baseline advance/event application, and complete redaction.

## Runtime proof strategy

Do not reuse or corrupt the closed Phase 5H20 fixture.

The future 5H21 runtime proof uses a new dedicated create-file fixture and a
controlled test/proof-only fault-injection seam for:

1. partial acceptance followed by successful in-session recovery; and
2. expired/lost session followed by predetermined-ID inspection and exactly
   one fenced restart.

Fault injection must be unavailable to the production daemon path.

Remote cleanup remains out of scope until delete/trash semantics are proven.

## Non-goals

5H21 does not add automatic daemon remote writes, persistent session URIs,
arbitrary retry loops, new IDs after ambiguity, update/trash execution,
multi-account, Shared Drives, native Workspace uploads, background recovery, or
manual SQLite repair.

## Planned implementation slices

- **5H21B** — process-local resumable buffer and partial-prefix recovery.
- **5H21C** — durable submitted-to-submitted restart CAS and attempt semantics.
- **5H21D** — supervised restart fencing and CLI recovery wiring.
- **5H21E** — controlled fault-injection integration proof and final closure.
