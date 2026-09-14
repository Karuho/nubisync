# Phase 3A — Persistent Google OAuth Session

## Objective

Allow NubiSync to restart without forcing the user through browser authorization again.

This subphase deliberately does not implement Drive file synchronization yet.

## Credential model

NubiSync stores:

- Google user refresh token → OS credential store
- development Desktop OAuth client ID → OS credential store
- development Desktop OAuth client secret → OS credential store
- short-lived access token → memory only
- account metadata → SQLite
- Drive change cursor → SQLite

No OAuth credential is stored in telemetry.

## Commands

`nubisync auth google configure`

Stores the development Desktop OAuth client configuration in the OS credential store.

`nubisync auth google status`

Inspects local session state without a network request and without printing credentials.

`nubisync auth google refresh`

Uses the stored refresh token to obtain a new short-lived access token and verifies that OpenID Connect UserInfo returns the same stable Google `sub` stored in SQLite.

`nubisync auth google logout`

Removes the user refresh token while deliberately retaining local SQLite synchronization metadata and the Desktop client configuration.

## Testing-mode limitation

Google OAuth projects with an External consent screen in Testing status can issue refresh tokens that expire after seven days when scopes beyond basic profile identity are requested.

That is acceptable for NubiSync development, but production persistence must be validated again after the OAuth app moves through the appropriate production and verification process.

## Phase 3A exit criteria

- client configuration can be moved from environment variables into the OS credential store
- environment variables can then be unset
- `auth google status` reports a locally connected account without network access
- `auth google refresh` succeeds without browser interaction
- refreshed UserInfo `sub` matches the stored account
- access token remains memory-only
- full workspace tests and Clippy pass
