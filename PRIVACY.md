# NubiSync Privacy Policy and Design Commitments

**Status: pre-alpha.**

NubiSync is designed around data minimization, local processing and explicit user control.

This document describes the intended privacy model for official NubiSync builds and NubiSync-operated services. It must be reviewed and updated before any production telemetry backend or public Google OAuth production deployment is enabled.

## 1. Core synchronization data

To synchronize a user's cloud account, NubiSync must access the files and metadata that the user authorizes through the selected provider.

For the initial Google Drive provider, this can include file and folder metadata and file contents necessary to perform synchronization.

The architectural requirement is:

**cloud file contents move between the user's device and the cloud provider, not through NubiSync-operated telemetry infrastructure.**

NubiSync-operated telemetry must not collect:

- file contents
- filenames
- directory names
- local or remote file paths
- Google Drive file identifiers
- OAuth access tokens
- OAuth refresh tokens
- passwords
- private keys or other authentication secrets

## 2. Google account identity

Official builds may request basic Google identity information when it is needed for account selection, account display, consent management, support or security.

This may include:

- provider account subject identifier
- email address
- display name
- locale
- profile image for local user-interface display

Identity information must be separated from bulk diagnostic telemetry.

NubiSync does not need a user's exact date of birth for cloud synchronization and should not request a Google birthday scope merely for analytics.

## 3. OAuth credentials

OAuth access and refresh tokens are security credentials.

They must:

- remain on the user's device
- be stored using an operating-system credential facility where practical
- never be included in telemetry
- never be intentionally logged
- never be uploaded to NubiSync-operated analytics or support systems

## 4. Diagnostic telemetry

NubiSync supports three intended telemetry modes:

### Off

No NubiSync-operated diagnostic telemetry leaves the device.

### Basic

Pseudonymous technical diagnostics may be sent. Direct identity such as email and display name must not be embedded in ordinary Basic telemetry events.

### Enhanced

A user may explicitly choose to associate diagnostic events with a NubiSync account for support and troubleshooting. Bulk events should still use a pseudonymous diagnostic subject rather than repeating direct identity in each event.

See `TELEMETRY.md` for the telemetry data model.

## 5. Optional community insights

NubiSync may ask optional product-research questions separately from diagnostic telemetry.

Examples include:

- age range, not exact date of birth
- country
- preferred language
- primary NubiSync use
- Linux experience level

Participation must be voluntary. Optional questions should provide a skip or "Prefer not to say" choice where appropriate.

Community-insight data is intended for aggregate product decisions, such as accessibility, interface design, documentation priorities and platform support.

## 6. Google API data use

Google user data obtained through Google APIs must be used only for disclosed NubiSync functionality and in accordance with applicable Google API Services User Data requirements.

NubiSync must request only scopes necessary for implemented features and must not request additional Google permissions solely for speculative future use.

Before public production use of restricted Google scopes, the project must complete the applicable Google verification requirements and publish a privacy policy at the required public domain.

## 7. Advertising, sale and model training

NubiSync does not intend to:

- sell Google user data
- sell diagnostic telemetry
- provide user data to data brokers
- use Google Drive content for targeted advertising
- use Google Drive content to train general-purpose machine-learning or AI models

Any future material change to these commitments would require a clearly disclosed policy revision and any consent required by law or platform policy.

## 8. Analytics providers

NubiSync may later use an analytics service for aggregate product metrics.

If Google Analytics or a similar service is adopted, direct personal identifiers such as email addresses or display names must not be sent to that analytics service.

NubiSync's own event schema remains authoritative so analytics vendors can be replaced without changing core synchronization behavior.

## 9. Infrastructure separation

Identity data and bulk telemetry should be stored separately and linked, where necessary, through pseudonymous identifiers.

A failure or outage in telemetry, analytics, account-support or update infrastructure must not prevent core cloud synchronization.

## 10. Retention and deletion

Before production collection begins, NubiSync must publish concrete retention periods for each data class and implement deletion behavior consistent with those commitments.

The product design should provide, where applicable:

- telemetry controls
- visibility into telemetry payloads
- resettable telemetry identifiers
- export of account or diagnostic data
- deletion of stored diagnostic history
- withdrawal of optional community-insight data

## 11. Security

Reasonable technical and organizational controls must protect NubiSync-operated data.

Production secrets, signing keys and service credentials must not be stored in the public repository or the private operations repository.

## 12. Changes to this policy

Material privacy changes must be documented before the changed collection or use begins.

Git history may provide a public record of policy revisions, but production applications should also present material changes to users when appropriate.

## 13. Contact and production-readiness requirement

Before NubiSync is submitted for public Google OAuth verification or production telemetry is enabled, the project must publish a private contact method for privacy and data-rights requests.

Public GitHub issues should not be the only method for submitting privacy-sensitive requests.
