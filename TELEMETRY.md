# Telemetry

Telemetry is optional and must never be required for synchronization.

## Levels

### Off

No NubiSync diagnostic telemetry leaves the device.

### Basic

Pseudonymous technical diagnostics may include:

- NubiSync version
- architecture
- Linux distribution
- desktop environment
- filesystem family
- installation type
- synchronization success/failure classes
- stable error codes
- latency/performance buckets
- conflict counts
- crash and recovery events
- provider rate-limit events

Basic telemetry must not contain direct user identity.

### Enhanced

The user may explicitly allow diagnostic events to be associated with a NubiSync account for support and troubleshooting.

Bulk event records still use a pseudonymous telemetry subject rather than embedding email or display name into every event.

## Explicitly prohibited telemetry fields

- file contents
- filenames
- directory names
- local or remote file paths
- Drive file IDs
- OAuth access tokens
- OAuth refresh tokens
- passwords
- secret keys

## Community insights

Optional demographic/product questions are separate from diagnostic telemetry.

Examples:

- age range
- country
- language
- primary use
- Linux experience level

These fields are voluntary and are intended for aggregated product decisions.

## Local buffering

Telemetry events should be buffered locally and uploaded in batches.

Loss or unavailability of the telemetry backend must not block synchronization.

Queued telemetry should have bounded size and retention.
