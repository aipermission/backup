# Changelog

All notable changes to AIPermission Backup are documented in this file.

## [Unreleased]

### Changed

- Advanced the authenticated service contract to protocol v4 and pinned the
  release Compose example to the matching `0.4.0` service image.
- Expired upload operation identities now remain as durable tombstones, so a
  delayed retry cannot create a new backup after the original version is gone.

### Security

- Historical metadata migrations now preserve canonical timestamps and run
  under restrictive directory permissions before snapshots are created.

## [0.3.0] - 2026-09-14

### Changed

- Advanced the authenticated service contract to protocol v3 and require a
  client-generated operation ID for every immutable upload.
- Replaying the same upload operation now returns the original backup version
  without consuming the request body or creating a duplicate version.
- Reusing an operation ID with different stream or source metadata is rejected
  as an explicit conflict.
- AIPermission clients that require protocol v3 intentionally reject older
  backup services. Publish or deploy this service before upgrading the matching
  AIPermission client, then upgrade both sides as one coordinated change.

### Security

- Upload operation identities are stored atomically with backup metadata and
  protected by a unique database constraint, so a lost response cannot cause a
  second immutable backup during a retry.

## [0.2.0] - 2026-08-12

### Added

- Added authenticated storage usage/quota reporting and optional upload quota
  enforcement.
- Added per-stream automatic retention policies with a side-effect-free
  preview and optional immediate application.

### Changed

- Advanced the authenticated service contract to protocol v2 with explicit
  capability discovery and retention-aware storage metadata.
- AIPermission clients that require protocol v2 intentionally reject older
  backup services. Upgrade the backup service before the matching client.

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
