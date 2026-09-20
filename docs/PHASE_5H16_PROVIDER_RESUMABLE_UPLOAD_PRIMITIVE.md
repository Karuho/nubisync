# Phase 5H16 — Google Drive provider resumable-upload primitive

Status: **IMPLEMENTED AND VALIDATED**

Baseline: `b93c5829adce3bb155159d9f2d24bf8fad6795f9`

Date: 2026-09-19.

## Scope

5H16 adds provider-only ordinary-file resumable-upload primitives to
`nubisync-drive`.

It does not add CLI orchestration, SQLite/schema changes, durable remote-write
execution, local file reading, settlement, change-stream confirmation, daemon
write execution, or live provider runtime validation.

Schema remains v18.

## Provider API

The provider now exposes an opaque resumable session plus operations to initiate
an ordinary-file create using a predetermined ID, upload one chunk, and query
session status.

Outcomes are parsed as `200/201` complete, `308` incomplete with the next offset
derived from Drive `Range`, and `404` expired session.

The provider never assumes a transmitted chunk was fully accepted.

## Safety and privacy

The session URI is capability-sensitive. It is private inside the opaque
session object, redacted from `Debug`, has no public accessor, and is not
persisted by this phase.

Session URIs are fail-closed to HTTPS `www.googleapis.com` upload paths.

Completion validation requires exact predetermined ID, leaf name, ordinary MIME
type, single expected parent, live state, total byte size and positive provider
version. Optional MD5/SHA-256 metadata are validated and redacted.

## Chunk policy

- default target: 8 MiB;
- non-final chunks: multiple of 256 KiB;
- final chunk may be shorter;
- checked `Content-Range`;
- provider `Range` controls the resume offset.

Zero-byte files are intentionally fail-closed in this first provider primitive
rather than guessing an undocumented terminal zero-byte resumable request shape.

## Validation

Unit tests cover constant alignment, Google-only session URI validation,
capability redaction, chunk ranges, alignment, `Range` parsing, exact completion
identity/size and checksum validation/redaction.

No real Google Drive object is created by 5H16.

## Next action

Phase 5H17 should bind this provider primitive to the durable file-create intent
state machine, stable local open-file identity, pre-submit cursor fence and
predetermined-ID recovery. The persistent daemon remains write-inert.
