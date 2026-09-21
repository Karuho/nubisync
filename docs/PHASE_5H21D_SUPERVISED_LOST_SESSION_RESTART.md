# Phase 5H21D — Supervised lost/expired-session restart wiring

Status: **SOURCE IMPLEMENTATION**

Date: 2026-09-21.

Baseline: Phase 5H21C closed PASS.

## Scope

This slice wires the existing supervised
`recover-file-create-submission --approve` command to the durable restart CAS.

It remains an explicit proof/control command. Persistent daemon remote writes
stay disabled.

## Recovery decision

Every invocation starts with metadata inspection of the deterministic target ID
using durable size and SHA-256 evidence.

- Exact: transition to `awaiting_confirmation`; no restart.
- Mismatch: transition to `conflict`; no restart.
- Missing: continue to restart fencing.

Before restart, the target ID is inspected a second time after fresh parent and
topology checks. Exact or Mismatch at that point also prevents restart.

Only two consecutive Missing results can reach the restart CAS.

## Restart fences

The command requires:

- one submitted ordinary-file create candidate;
- execution state remains submitted with non-zero attempt count;
- stable local identity can be opened;
- local size equals durable content evidence;
- durable remote catalog cursor exists;
- durable authority-state cursor equals catalog cursor;
- durable parent authority permits adding children;
- fresh parent remains a folder, permits children and has the same authority
  version;
- parent still resolves inside the selected Drive root;
- provider change cursor equals the durable cursor before the second ID
  inspection;
- target ID remains Missing;
- provider cursor is unchanged after that inspection.

Only then is the 5H21C `submitted -> submitted` restart CAS committed.

## One replacement session

After the CAS, the command initiates exactly one replacement resumable session.

There is no loop around session initiation.

If initiation fails, the intent remains durable `submitted` at the incremented
attempt/generation. A later invocation starts again from target-ID inspection.

If the replacement session expires again, the same rule applies: no second
replacement session is initiated in that invocation.

## Replacement stream

The replacement session uses the 5H21B retained-buffer protocol.

- provider next offset is authoritative;
- partial prefixes retain and resend only the suffix;
- retransmissions do not double-hash;
- no-progress responses remain bounded;
- before the first final request, stream SHA-256 must equal the durable
  evidence;
- the same evidence is reasserted at the new execution generation before the
  final PUT;
- provider completion SHA-256 must match when present;
- otherwise the deterministic target ID is inspected again;
- source stability is rechecked before transition to `awaiting_confirmation`.

## Attempt semantics

The 5H21C CAS increments `attempt_count` and `execution_generation` once before
the single replacement session.

Chunk retransmission, status probes and metadata inspections do not increment
attempt count.

## Safety

No new provider primitive is added.

No session URI is persisted or printed.

Remote IDs, local names, hashes, cursors and tokens remain absent from normal
proof output.

No local event is applied and no local baseline is advanced.

Schema remains v18.

The source-closure script itself performs no provider call or runtime DB
mutation.

## Tests

Focused CLI tests freeze the Exact/Mismatch/Missing decision mapping.

Full CLI, daemon, storage and workspace checks remain required before commit.

The controlled network fault-injection proof remains Phase 5H21E.

## Next

Phase 5H21E adds test/proof-only deterministic fault injection and executes the
two final supervised proofs:

1. partial-prefix in-session recovery;
2. lost/expired session -> Missing -> fenced single restart.

The already closed 5H20 fixture must not be reused or deleted.
