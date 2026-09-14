# Contributing to NubiSync

NubiSync welcomes bug reports, documentation improvements and code contributions that respect the project's security, licensing and privacy boundaries.

## Before implementing a large change

Open an issue describing:

- the problem
- proposed behavior
- security/privacy impact
- compatibility impact
- tests

The initial product scope is intentionally narrow: Google Drive synchronization on Linux.

New cloud providers should not be added before the initial Google Drive implementation is stable.

## Secrets and user data

Contributors must never commit:

- credentials
- OAuth access or refresh tokens
- API secrets
- private keys
- production configuration secrets
- real user cloud data

Fixtures must use synthetic data.

## Licensing of contributions

NubiSync uses a noncommercial source-available license and intends to preserve the option for separately authorized commercial licensing.

Those two goals require deliberate handling of third-party copyright.

During the pre-alpha period, maintainers may decline or postpone substantive external code contributions until an appropriate contributor agreement or other rights mechanism has been adopted.

Bug reports, design discussion and small documentation corrections may still be accepted subject to repository policy.

Submitting a pull request does not, by itself, grant the project broader relicensing rights than the rights actually provided by the contributor and applicable law.

Before NubiSync begins routinely accepting substantive third-party code, the project must publish a clear contributor-rights mechanism.

## Third-party code

Do not copy code from another project unless its license is compatible and the required attribution and notices are included.

Adding a dependency requires reviewing its license and security implications.

## Branding

A fork or modified distribution must follow `TRADEMARKS.md` and must not imply official NubiSync status without authorization.
