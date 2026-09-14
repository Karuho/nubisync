# Google OAuth

## Application type

NubiSync is a native desktop application.

Google OAuth for desktop applications uses the system browser and a loopback redirect URI.

NubiSync binds only to:

`127.0.0.1:<random-port>`

The loopback callback uses the URI root (`/`), so the redirect URI is exactly `http://127.0.0.1:<port>`.

## PKCE and state

Every authorization attempt generates:

- a fresh high-entropy OAuth `state`
- a fresh PKCE verifier
- an S256 PKCE challenge

The callback must match the exact loopback scheme, host, port and path created for that authorization attempt.

The returned `state` must match exactly before an authorization code is accepted.

OAuth authorization URLs are intentionally excluded from `Debug` output because they contain the state value.

## Access levels

NubiSync models Drive permissions explicitly:

### MetadataReadOnly

`https://www.googleapis.com/auth/drive.metadata.readonly`

Used for the first real Phase 2 connection.

### ReadOnly

`https://www.googleapis.com/auth/drive.readonly`

Reserved for a later phase when actual file downloads are implemented.

### FullSync

`https://www.googleapis.com/auth/drive`

Reserved for the phase that implements and validates remote mutations.

NubiSync must not request a stronger access level merely because future code may need it.

## Identity scopes

NubiSync requests:

- `openid`
- `email`
- `profile`

The OpenID Connect UserInfo response supplies the stable Google `sub` account identifier and may supply the display name and email address needed for account UX.

## Token exchange

Authorization codes are exchanged at:

`https://oauth2.googleapis.com/token`

using:

- client ID
- authorization code
- PKCE verifier
- `authorization_code` grant type
- exact loopback redirect URI

The desktop flow does not rely on a confidential client secret.

## Credential storage

Refresh tokens are security credentials.

They belong in the operating-system credential store and must never be:

- committed to Git
- stored in telemetry
- logged
- stored in NubiSync SQLite metadata tables
- sent to NubiSync-operated services

## Logging rule

Do not log:

- authorization URLs
- authorization codes
- access tokens
- refresh tokens
- PKCE verifiers
- OAuth state

Errors exposed to logs should use stable sanitized categories.

## Installed-app authorization behavior

NubiSync does not send `include_granted_scopes` in the Desktop installed-app authorization request. Google documents incremental authorization as unsupported for the Installed App flow. Each authorization therefore requests the explicit scope set required by the current NubiSync capability.

## Desktop client secret interoperability

Google Desktop OAuth clients include a `client_secret`. Installed desktop software cannot keep this value confidential, so it is not treated as a user credential. Current Google token-endpoint behavior may nevertheless require the value during authorization-code exchange even when PKCE is used.

Development runs provide it through `NUBISYNC_GOOGLE_CLIENT_SECRET`. NubiSync does not log it, write it to telemetry, or commit it to the public repository. User refresh tokens remain actual credentials and continue to live only in the OS credential store.

## Persistent session refresh

A successful offline authorization stores the Google refresh token in the OS credential store.

NubiSync later refreshes the short-lived access token by POSTing to Google's token endpoint with:

- `client_id`
- Desktop `client_secret`
- `refresh_token`
- `grant_type=refresh_token`

The resulting access token remains memory-only.

Before accepting a refreshed session, NubiSync calls OpenID Connect UserInfo and verifies that the returned stable `sub` is identical to the account subject stored in SQLite. A mismatch fails closed.

Development OAuth client configuration is also kept in the OS credential store so subsequent launches do not depend on shell environment variables.
