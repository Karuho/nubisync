# NubiSync

**Native cloud sync for Linux.**

NubiSync is a Linux-first desktop synchronization client designed to keep a real local folder synchronized with cloud storage providers.

## Initial scope

The first supported provider is Google Drive.

NubiSync is intentionally **not** a FUSE mount. Files are stored locally and synchronized in the background.

Initial goals:

- Google Drive ↔ local Linux folder
- bidirectional synchronization
- local filesystem event monitoring
- incremental Google Drive change tracking
- resumable transfers
- transactional local metadata
- crash recovery
- safe conflict handling
- optional, privacy-conscious telemetry
- no dependency on NubiSync infrastructure for core synchronization

Future providers may include OneDrive, Dropbox and others, but they are explicitly out of scope for the first implementation.

## Privacy principle

Cloud file contents, file names and local paths must not be sent to NubiSync telemetry infrastructure.

NubiSync telemetry is optional and must never be required for synchronization.

See `PRIVACY.md`, `TELEMETRY.md`, `SECURITY.md` and `docs/ARCHITECTURE.md`.

## Licensing

NubiSync is **source-available software**, licensed under the **PolyForm Noncommercial License 1.0.0**. It is not presented as OSI-approved open-source software.

The repository license permits the uses granted by PolyForm Noncommercial 1.0.0. Commercial rights are not granted by that license and require a separate written license from the applicable rights holder.

See:

- `LICENSE` for the controlling software license
- `COMMERCIAL-LICENSING.md` for commercial licensing guidance
- `TRADEMARKS.md` for project-name and branding rules
- `CONTRIBUTING.md` for contribution requirements
- `docs/LEGAL-READINESS.md` for legal/compliance gates before public production launch

Third-party dependencies remain subject to their own licenses.

## Repository boundary

This public repository contains the complete end-user application and all code required to build and operate the synchronization client.

Private operational infrastructure, production telemetry backend configuration and release operations are maintained separately.

Production secrets and private signing keys are never stored in either repository.

## Status

Pre-alpha / Phase 0.

No production NubiSync telemetry service or public Google OAuth production integration should be considered active merely because source code or design documents exist in this repository.
