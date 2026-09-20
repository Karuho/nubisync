# Phase 5H18 — Ordinary-file create change-stream confirmation

Status: **IMPLEMENTED AND VALIDATED IN SOURCE/TESTS**

Baseline: `df03f6297ffd5523f87a07d04871b81af66cec05`

Date: 2026-09-20.

## Scope

5H18 adds read-only change-stream confirmation for the ordinary-file
`CreateFile` path established by 5H17B.

Schema remains v18.

The persistent daemon remains remote-write inert.

## CLI surface

Development/owner-validation command:

`nubisync sync roots confirm-file-create --approve`

The approval gate remains temporary development instrumentation, not final
product UX.

## Confirmation authority

Exactly one `awaiting_confirmation` file-create candidate is selected.

Confirmation requires all of the following:

1. durable execution state is still `awaiting_confirmation`;
2. execution generation matches the candidate;
3. durable pre-submit change cursor exists;
4. durable content evidence exists;
5. durable uploaded byte size matches the candidate;
6. durable uploaded remote version exists;
7. local baseline generation is unchanged and valid;
8. source local event remains pending;
9. staged window, if present, is bound to the pre-submit cursor;
10. otherwise the durable catalog cursor still equals the pre-submit cursor.

## Change-stream semantics

The ReceiveOnly credential is used.

The staged change window starts at the pre-submit cursor.

For the predetermined target ID:

- exact upsert requires leaf name, ordinary file kind, expected parent, exact
  uploaded byte size and non-trashed state;
- delete or mismatched upsert is non-exact;
- if the target is not observed at all, the staged window is discarded and the
  durable cursor does not advance.

Google documents the Drive changes collection as the efficient incremental
mechanism for observing file changes, with entries ordered oldest-first and a
new start page token becoming the next polling checkpoint after the final page.

## Content-evidence fence

A change-stream match alone is insufficient.

Before committing an observed target window, NubiSync performs a read-only exact
`files.get`-style inspection through the existing `inspect_expected_file`
provider primitive.

That inspection must match:

- predetermined ID;
- leaf name;
- expected parent;
- ordinary MIME type;
- uploaded byte size;
- durable SHA-256.

The current Drive `version` must be greater than or equal to the version
observed at upload completion. Equality is not required: Drive defines
`version` as monotonically increasing and it can advance for server-side changes
that are not visible to the user.

This prevents a same-ID metadata event from being accepted as proof of the
specific uploaded content while avoiding a false conflict caused only by a
legitimate server-side version increment.

## Commit and transition

When the target was observed:

1. provider metadata/content evidence is checked read-only;
2. the complete staged change window is committed;
3. the final durable catalog item is checked for name/kind/parent/size/live
   state;
4. the intent transitions:
   - exact change + exact final catalog + exact provider evidence -> confirmed;
   - otherwise -> conflict.

The local source event remains pending and the local baseline generation does
not advance.

## Privacy and mutation boundaries

5H18:

- performs no provider write;
- reads no local file content;
- mutates only local metadata state, staged change-window state, catalog cursor
  and intent confirmation state;
- prints no remote IDs, SHA-256 values, cursor values, provider metadata or
  token values.

## Non-goals

5H18 does not:

- settle the local baseline;
- create a content ownership receipt;
- implement file update;
- implement remote trash;
- enable persistent daemon writes.

## Next action

Phase 5H19 should implement selective ordinary-file create settlement.

Settlement must promote only the uploaded source snapshot, create a durable
content ownership receipt from the uploaded SHA-256, advance the local
generation exactly once, apply only the source event and preserve any
post-upload local modification as a residual event.
