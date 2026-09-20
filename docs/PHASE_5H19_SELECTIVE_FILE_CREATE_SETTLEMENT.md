# Phase 5H19 — Selective ordinary-file create settlement

Status: **IMPLEMENTED AND VALIDATED IN SOURCE/TESTS**

Baseline: `5a59b1016aa3a5f23017328bc42b4cbcd2e91d58`

Date: 2026-09-20.

## Scope

5H19 settles a confirmed ordinary-file `CreateFile` into the local baseline.

The phase is local-only:

- no provider API call;
- no network access;
- no remote mutation;
- persistent daemon remote writes remain disabled.

Schema remains v18.

## Core settlement rule

The baseline must represent the bytes that were actually uploaded, not blindly
whatever is present at the local pathname at settlement time.

The promoted file snapshot therefore comes from the durable create intent:

- relative path;
- file kind;
- uploaded byte size;
- upload-time mtime;
- upload-time device;
- upload-time inode.

The durable upload SHA-256 and remote version come from 5H17B evidence.

## Post-upload local changes

The current local tree is scanned twice.

A proposed next baseline is built by adding the uploaded snapshot to the old
baseline. The residual diff is then computed against the current local tree.

Therefore a post-upload:

- file modification -> residual `modified`;
- deletion -> residual `deleted`;
- replacement by directory -> residual `type_changed`;
- unrelated local change -> corresponding residual event.

The original create event is applied exactly once. Other old-generation pending
events are superseded and recreated against the next generation from the
residual diff.

## Content ambiguity fence

Metadata alone is not sufficient when the current pathname still appears
identical to the upload-time file identity.

When kind, size, mtime, device and inode all still match the uploaded snapshot,
NubiSync opens that exact source and recomputes SHA-256 locally.

Settlement proceeds only if:

- the descriptor/path remain stable through the recheck; and
- current SHA-256 equals the durable uploaded SHA-256.

If metadata appears identical but content differs, settlement fails closed.
This prevents a content-only local mutation from being silently erased by the
metadata-based journal.

When current metadata already differs, the residual journal records that
difference and no equality claim is made about current content.

## Atomic storage commit

The storage transaction requires:

- TwoWay mode;
- exact local baseline generation/count/timestamp;
- confirmed `create_file` intent and execution generation;
- exact upload-time local identity;
- exact durable file evidence: kind, remote version, size and SHA-256;
- source pending event is the original created/file event;
- exact remote catalog ID/name/parent/file-kind/size/non-trashed state;
- no pre-existing local baseline row for the path;
- no conflicting file materialization receipt.

The same transaction:

1. inserts the uploaded snapshot into the local baseline;
2. advances local generation exactly once;
3. marks the source event applied;
4. supersedes remaining old-generation pending events;
5. inserts residual events for the new generation;
6. creates a current file materialization/content ownership receipt containing
   remote ID, relative path, size and uploaded SHA-256;
7. inserts the remote-write settlement record.

A duplicate settlement is rejected.

## CLI surface

Development/owner-validation command:

`nubisync sync roots settle-confirmed-file-create --approve`

The approval gate is temporary engineering instrumentation, not final product
sync UX.

## Privacy

The command does not print:

- local names;
- remote IDs;
- SHA-256 values;
- tokens.

Receipt Debug behavior remains redacted.

## Non-goals

5H19 does not:

- perform the first real file-create E2E runtime proof;
- implement file update;
- implement remote trash;
- enable automatic daemon write execution;
- add zero-byte upload support.

## Next action

Phase 5H20 should perform the first tightly controlled end-to-end ordinary-file
create runtime proof:

local create -> journal -> plan -> predetermined ID -> supervised resumable
upload -> change-stream confirmation -> selective settlement -> final clean plan.

The proof fixture should be retained unless remote trash semantics have already
been separately proven. Persistent daemon writes remain disabled after the
proof.
