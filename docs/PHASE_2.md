# Phase 2 — Controlled Google Authentication and Read-Only Probe

## Objective

Connect a real Google account without giving NubiSync write access yet.

Phase 2 deliberately separates:

1. code and offline validation
2. user-controlled creation of a development OAuth client
3. one explicit live authorization
4. a metadata-only Drive probe

## Access level

The live Phase 2 flow requests:

- `openid`
- `email`
- `profile`
- `https://www.googleapis.com/auth/drive.metadata.readonly`

It does **not** request the full `drive` scope.

This allows NubiSync to validate identity and Drive metadata/change tracking while keeping the first live test non-destructive.

## Live operations performed

After explicit user authorization, the development CLI performs:

- OAuth authorization-code exchange using PKCE S256
- OpenID Connect UserInfo request
- Drive `about.get` with an explicit field mask
- Drive `changes.getStartPageToken`

It does not:

- call a Drive write endpoint
- list the user's files
- request file contents
- upload files
- delete files
- modify folders
- send OAuth tokens to NubiSync-operated infrastructure

## Token storage

The refresh token is stored through the operating-system credential store.

On Linux, NubiSync currently uses keyring-rs and the desktop Secret Service backend.

The local SQLite database stores account identity needed by the sync engine and the opaque Drive change cursor. It does not store the OAuth refresh token.

## Development OAuth client

A developer must create an OAuth client of type **Desktop app** in a Google Cloud project.

The client ID is an application identifier and can be supplied through:

`NUBISYNC_GOOGLE_CLIENT_ID`

No Google client secret is required by NubiSync's desktop PKCE implementation.

Never commit user tokens, authorization codes, service-account keys or unrelated Google Cloud credentials.

## Phase 2 closure

Phase 2 closes only after:

- the offline validation suite passes
- the Secret Service backend is available on the real Debian desktop
- the developer explicitly authorizes the test account
- UserInfo succeeds
- Drive `about.get` succeeds
- `changes.getStartPageToken` succeeds
- the refresh token is stored in the OS keyring
- the cursor is stored in SQLite
- no file listing or write operation occurs

## Loopback redirect URI

The Desktop OAuth flow uses `http://127.0.0.1:<random-port>` with the root path. The exact same redirect URI is used for authorization and code exchange.
