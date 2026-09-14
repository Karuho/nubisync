# Security Policy

## Security principles

NubiSync handles high-value user data and OAuth credentials.

The project follows these baseline rules:

1. OAuth access and refresh tokens are stored only on the user's device using an OS credential store where available.
2. OAuth tokens are never sent to NubiSync telemetry or operational infrastructure.
3. Cloud file contents, filenames and local filesystem paths are never telemetry fields.
4. Production secrets and private release-signing keys are never committed to Git.
5. Public builds contain only public verification material.
6. Telemetry and update services are non-authoritative dependencies: core synchronization must continue when they are unavailable.
7. Sensitive logs must be redacted before persistence.
8. Authentication, synchronization and storage code must fail closed when state integrity cannot be established.

## Secret handling

Production secrets belong in dedicated secret-management facilities such as:

- GitHub Actions secrets / OIDC
- Cloudflare secret bindings
- KMS/HSM or equivalent systems when applicable

The private operations repository may describe secret names and deployment wiring, but must not contain their values.
