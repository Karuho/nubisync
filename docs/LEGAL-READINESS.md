# Legal and Compliance Readiness

This checklist records gates that should be completed before NubiSync is treated as a public production service.

It is intentionally stricter than the current pre-alpha requirements.

## Current baseline

- Product name: NubiSync
- Positioning: Native cloud sync for Linux
- Initial provider: Google Drive
- Public source license: PolyForm Noncommercial License 1.0.0
- Core synchronization must not depend on NubiSync-operated telemetry infrastructure
- Production secrets must remain outside Git

## Before accepting substantive external code routinely

- identify the legal person or entity that will administer commercial licensing
- publish a contributor-rights mechanism appropriate for dual/noncommercial + commercial licensing
- decide whether that mechanism is a CLA, copyright assignment, or another reviewed agreement
- preserve contributor attribution and third-party notices
- document how contributor data is processed

A DCO alone may establish provenance but should not be assumed to grant the relicensing rights needed for a commercial dual-licensing model.

## Before public Google OAuth production

- establish a production Google Cloud project separate from development/testing
- define the exact OAuth scopes used by implemented features
- request the narrowest scopes that satisfy full synchronization
- publish an application home page on a controlled public domain
- publish the production privacy policy on the same required domain
- publish a private support/privacy contact method
- configure accurate OAuth branding and support information
- verify required domains
- prepare scope justifications
- prepare the OAuth verification demonstration requested by Google
- complete restricted-scope verification when required
- complete any required security assessment before transmitting or storing restricted Google data on third-party servers
- confirm that Google Drive content does not transit NubiSync telemetry infrastructure
- document token storage and revocation behavior

## Before production telemetry

- publish the final telemetry event schema
- define Off, Basic and Enhanced behavior in code and documentation
- document all infrastructure processors
- define retention periods by data class
- implement deletion and identifier reset behavior
- ensure direct identity is separated from bulk telemetry
- prohibit filenames, paths, Drive IDs, contents and OAuth tokens at schema-validation level
- provide a private privacy contact
- implement access control and audit logging for telemetry administration
- implement abuse/rate limiting
- document incident response
- document backup and deletion behavior for telemetry stores
- verify that disabling or blocking telemetry never blocks sync

## Before optional community insights

- make participation voluntary
- use age ranges rather than exact birth dates unless a future feature creates a documented necessity
- include "Prefer not to say" or skip behavior where appropriate
- define retention and deletion
- keep community-insight data logically separate from Google Drive content
- do not infer sensitive traits from Drive content

## Before Google Analytics or another analytics vendor

- document the vendor in the privacy policy
- send no email address, display name or other direct personal identifier prohibited by that vendor
- send no OAuth credentials, filenames, paths, Drive IDs or file contents
- keep NubiSync's own event schema vendor-neutral
- verify consent requirements for target jurisdictions
- provide an opt-out consistent with the selected telemetry level

## Before the first signed binary release

- perform a dependency-license inventory
- generate third-party notices
- review binary redistribution obligations
- define the official signing authority
- keep private signing material outside Git
- publish only public verification material
- define update-signature verification and rollback behavior
- document security-reporting contact
- perform a release threat-model review

## Before branding is treated as commercially established

- perform a dedicated trademark/name search in relevant jurisdictions
- decide who legally owns or administers NubiSync branding
- document logo and brand-asset licensing
- avoid claiming registered trademark status unless registration actually exists

## Before a paid commercial license is offered

- identify the actual rights holder(s)
- ensure accepted contributions are legally relicensable
- define commercial support/warranty terms separately
- account for third-party components that cannot be relicensed
- use a separately reviewed written agreement
