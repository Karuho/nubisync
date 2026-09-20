# Phase 5H20A — Provider version floor and false-conflict recovery

Status: **SOURCE CORRECTION / RUNTIME RECOVERY PRIMITIVE**

Baseline: `2021b7864c6d6bca7fa70a12e20e43337b69590d`

Date: 2026-09-20.

## Trigger

The first real 5H20 ordinary-file create upload succeeded:

- one predetermined ID;
- one resumable create;
- two chunks;
- expected byte count;
- durable stream SHA-256;
- exact target change in Drive change stream;
- exact final catalog item.

Confirmation nevertheless transitioned the intent to `conflict` because
5H18 treated the Drive `version` observed at upload completion as an immutable
identity value.

That assumption is incorrect.

Google Drive defines `File.version` as a monotonically increasing number that
reflects every server-side change, including changes not visible to the user.

Therefore:

- a current version lower than the upload-completion version is invalid;
- the same version is valid;
- a greater version is valid if all content and identity evidence still matches.

## Corrected confirmation semantics

Exact evidence remains mandatory for:

- predetermined remote ID;
- leaf name;
- parent;
- ordinary MIME type;
- byte size;
- non-trashed state;
- SHA-256.

The version fence is now:

`current_remote_version >= upload_completion_remote_version`

The current version value is not printed.

## Existing conflict recovery

A new supervised command is added:

`nubisync sync roots recover-file-create-confirmation-conflict --approve`

It is deliberately narrow.

It accepts exactly one unsettled `CreateFile` conflict and requires:

- TwoWay mode;
- one submission attempt;
- durable pre-submit cursor;
- complete durable file evidence;
- unchanged local baseline generation;
- original source event still pending;
- no open change window;
- exact durable remote catalog item;
- read-only provider inspection with exact ID/name/parent/MIME/size/SHA-256;
- current Drive version satisfying the monotonic version floor.

Only then can storage transition:

`conflict -> confirmed`

The recovery:

- increments execution generation by one;
- preserves attempt count;
- clears the stale terminal marker;
- performs no provider write;
- does not apply the local source event;
- does not advance the local baseline.

## Next action

After this source correction closes, resume the retained 5H20 runtime fixture.
Do not create a new fixture and do not replay the upload.

Persistent daemon remote-write execution remains disabled.
