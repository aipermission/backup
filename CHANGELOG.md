# Changelog

All notable changes to AIPermission Backup are documented in this file.

## [Unreleased]

### Added

- Added authenticated storage usage/quota reporting and optional upload quota
  enforcement.
- Added per-stream automatic retention policies with a side-effect-free
  preview and optional immediate application.

### Security

- Quota rejection leaves no backup metadata, automatic retention remains
  bounded to 1-1000 newest versions, and every stream retains its latest
  recovery version.

## [0.1.1] - 2026-08-01

### Added

- Added exact single-version and selected-version deletion for explicit backup
  cleanup without relying on keep-last-N ordering.
- Added durable restart-safe blob cleanup after selected backup records are
  removed.

### Security

- The final recovery version in a stream cannot be deleted, including through
  batch requests.
- Selected deletion remains authenticated, bounded, stream-scoped, and limited
  to immutable backup identifiers.

## [0.1.0] - 2026-07-31

### Added

- Added authenticated immutable upload, bounded listing, verified download,
  and keep-last-N pruning for encrypted AIPermission database streams.
- Added atomic blob storage, SHA-256 metadata, restart-safe cleanup, and a
  versioned protocol discovery endpoint.
- Added a non-root, read-only container with local-build and pinned GHCR
  deployment options.
- Added CI, race tests, vulnerability scanning, CodeQL, and automated GHCR
  publication for version tags.

### Security

- The service stores encrypted `.aipdb` blobs and never receives database
  passwords, decrypted content, connector credentials, or gateway vault keys.
- The recommended deployment keeps the raw service port private and uses HTTPS
  over a trusted LAN or VPN/private overlay network.
