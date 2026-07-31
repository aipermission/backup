# Changelog

All notable changes to AIPermission Backup are documented in this file.

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
