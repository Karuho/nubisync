# Privacy Principles

NubiSync is designed around data minimization.

## Core cloud data

NubiSync needs access to the cloud account selected by the user in order to perform synchronization.

Cloud file contents remain between the user's machine and the selected provider. NubiSync-operated telemetry infrastructure must not receive file contents.

The telemetry system must not collect:

- file contents
- filenames
- local paths
- Drive file identifiers
- OAuth access tokens
- OAuth refresh tokens
- authentication secrets

## Identity

Official builds may use basic identity information supplied during authentication when required for account UX, support or consent management, such as:

- provider account subject identifier
- email address
- display name
- locale

Identity data must be stored separately from bulk telemetry events.

## Optional community insights

Optional surveys may request non-essential information such as:

- age range
- country
- language
- primary use
- Linux experience level

Participation must be voluntary and include a "Prefer not to say" option where appropriate.

NubiSync does not need a user's exact date of birth for synchronization.

## User control

The application should provide:

- telemetry off/basic/enhanced controls
- telemetry payload visibility
- resettable telemetry identifiers
- data export where practical
- telemetry-history deletion where applicable
