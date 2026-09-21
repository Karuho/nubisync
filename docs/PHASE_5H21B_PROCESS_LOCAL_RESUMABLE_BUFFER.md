# Phase 5H21B — Process-local resumable upload buffer

Status: **SOURCE IMPLEMENTATION**

Date: 2026-09-21.

Baseline: Phase 5H21A design freeze.

## Scope

This slice implements recovery only while the current process still owns the
Google Drive resumable session URI.

It does not add durable lost-session restart, a submitted-to-submitted CAS,
background recovery, or persistent daemon writes.

## Retained upload buffer

`SelectedRootFileCreateUploadBuffer` owns the stable forward local source and
retains provider-unacknowledged bytes in memory.

Its invariant is:

`pending_start_offset + pending_len == unique_local_bytes_read`

New bytes enter SHA-256 exactly once when first read. Retransmission clones
retained bytes and does not reread or rehash them.

## Provider-prefix acknowledgement

`acknowledge_provider_offset(next_offset)` accepts only offsets inside the
currently retained range.

Partial acknowledgement discards only the accepted prefix. The suffix remains
buffered and is topped up with newly read source bytes for the next request.

Non-final requests remain aligned to the Drive 256 KiB requirement. A final
request may be shorter.

## Final fingerprint ordering

Preparing a final request reads the remaining unique source bytes first.

The CLI persists SHA-256 before issuing the first final provider request.
Retries of a partially accepted final request reuse retained bytes.

## Bounded no-progress behavior

A response that advances the provider prefix by zero bytes causes a retained
buffer retry. The supervised CLI permits at most three consecutive no-progress
results; a fourth stops safely with the intent remaining durable as submitted.

Chunk/status retries inside the same resumable session do not increment
`attempt_count`.

## Evidence

Successful ordinary-file submission reports request count, unique
`BYTES_STREAMED`, attempted network bytes, status-probe count,
partial-prefix recovery count, `RANGE_PREFIX_AUTHORITATIVE=yes`, and
`RETRANSMISSION_DOUBLE_HASH=no`.

No payload bytes, hash values, session URI, remote IDs, paths, cursors, or
tokens are printed.

## Tests

Focused daemon tests cover partial acceptance, retained suffix plus aligned
refill, short final requests, network bytes exceeding unique source bytes,
zero acceptance without reread, stable SHA across retransmission, provider
offset regression, and offsets beyond bytes already read.

## Safety

Schema remains v18. No storage restart transition is introduced.

Persistent daemon remote-write execution remains disabled.

No provider call is made by the source-closure script.

## Next

Phase 5H21C adds the durable `submitted -> submitted` replacement-session CAS
and exact attempt/execution-generation semantics. It does not yet perform a
real provider restart.
