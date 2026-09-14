# Telemetry

Telemetry is optional and must never be required for synchronization.

NubiSync telemetry is designed as a separate system from cloud synchronization. The sync engine must remain functional if telemetry endpoints are unavailable, blocked or disabled.

## Levels

### Off

No NubiSync-operated diagnostic telemetry leaves the device.

Local logs and local diagnostic information may still exist when required for operation or troubleshooting, subject to local retention controls.

### Basic

Pseudonymous technical diagnostics may include:

- NubiSync version
- CPU architecture
- Linux distribution
- desktop environment
- filesystem family
- installation type
- synchronization success/failure classes
- stable error codes
- latency and performance buckets
- conflict counts
- crash and recovery events
- provider rate-limit events
- aggregate file-count or transfer-size buckets that do not identify individual files

Basic telemetry must not contain direct user identity.

### Enhanced

The user may explicitly allow diagnostic events to be associated with a NubiSync account for support and troubleshooting.

Bulk event records should still use a pseudonymous telemetry subject rather than embedding an email address or display name into every event.

Enhanced telemetry must not weaken the prohibited-field rules below.

## Explicitly prohibited telemetry fields

Diagnostic telemetry must not contain:

- file contents
- filenames
- directory names
- local or remote file paths
- Google Drive file IDs
- OAuth access tokens
- OAuth refresh tokens
- passwords
- private keys
- authentication secrets

## Identity separation

If official NubiSync services maintain account identity, identity records and bulk telemetry should be stored separately.

A diagnostic subject may be mapped to an account only where the selected telemetry mode permits it.

Basic telemetry identifiers should be resettable.

## Community insights

Optional demographic and product-research questions are separate from diagnostic telemetry.

Examples:

- age range
- country
- language
- primary use
- Linux experience level

These fields are voluntary and intended for aggregated product decisions.

NubiSync should not request an exact date of birth merely to derive an age range.

## Local buffering

Telemetry events should be buffered locally and uploaded in batches.

The queue must have bounded size and retention.

Loss or unavailability of the telemetry backend must not block synchronization.

## Analytics integrations

NubiSync may later forward selected non-identifying aggregate events to an analytics platform.

Direct personal identifiers such as email addresses, display names, OAuth credentials, filenames and paths must not be sent to Google Analytics or similar general analytics products.

The public NubiSync telemetry schema should remain independent from any specific analytics vendor.

## Transparency

The application should provide a way for users to inspect the categories of data collected.

The design target is to expose the next telemetry payload, or an equivalent human-readable preview, before transmission.

## Production gate

Before a production telemetry endpoint is enabled, the project must define and document:

- endpoint ownership
- processors/infrastructure providers
- event schema version
- retention periods
- deletion behavior
- abuse controls
- access controls
- incident response
- privacy contact method
