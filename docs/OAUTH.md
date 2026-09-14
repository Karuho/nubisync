# Google OAuth Foundation

## Phase 1 status

Phase 1 does **not** contact Google, exchange authorization codes or store real OAuth tokens.

It establishes and tests the authorization-request contract only.

## Intended production flow

NubiSync is a native desktop application.

The intended Google OAuth flow is:

1. generate a fresh high-entropy `state`
2. generate a fresh PKCE verifier and S256 challenge
3. bind an HTTP listener to `127.0.0.1` on an available local port
4. open the Google authorization URL in the user's browser
5. receive the authorization code on the loopback callback
6. require an exact `state` match
7. exchange the code with the original PKCE verifier
8. store the refresh token in the operating-system secret store
9. never place access or refresh tokens in SQLite telemetry tables, logs or NubiSync-operated services

Google documents loopback redirects for desktop applications and recommends PKCE for installed apps.

## Scopes

The current design requires:

- `openid`
- `email`
- `profile`
- `https://www.googleapis.com/auth/drive`

The full Drive scope is restricted. It is selected because NubiSync's primary function is full local synchronization of the user's existing Drive, not merely files created or individually selected by NubiSync.

Scope use must be re-reviewed before public OAuth verification.

## Client identity

A Google OAuth desktop client ID is an application identifier, not an end-user secret.

Development must not commit:

- user access tokens
- user refresh tokens
- authorization codes
- service-account private keys
- unrelated Google Cloud credentials

Official production OAuth configuration will be handled separately from user credentials.

## No speculative permissions

Future cloud providers or Google features must not cause additional Google scopes to be requested until the corresponding feature is implemented and justified.
