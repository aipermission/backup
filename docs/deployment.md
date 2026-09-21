# Deployment

## Supported Shape

```text
AIPermission client -> trusted LAN or VPN -> HTTPS reverse proxy -> private backup container
```

The service is designed for one developer-operated installation and one strong
owner token. It is not a multi-user storage platform.

The sample Compose file exposes `127.0.0.1:8080` only. A reverse proxy on the
same host can terminate HTTPS and forward to that loopback endpoint. Do not
publish the raw HTTP port on `0.0.0.0` or rely on the bearer token as a
replacement for transport encryption.

The recommended use case is sharing one service between the owner's trusted
computers on a private local network. For access from another network, connect
through a VPN or private overlay network and keep HTTPS enabled. Direct public
internet exposure of the raw backup port is outside the recommended deployment
shape.

Use `docker-compose.release.yml` for normal installations. It pulls the pinned
`ghcr.io/aipermission/backup` release selected by
`AIPERMISSION_BACKUP_VERSION`; the default source Compose file remains the
development build path.

Back up the Docker volume independently if service availability matters. The
volume contains encrypted AIPermission snapshots and limited metadata, but its
disclosure still allows offline guessing against weak database passwords.

Set `AIPERMISSION_BACKUP_MAX_STORAGE_BYTES` when the service must enforce an
application-level storage ceiling. Leave it unset for unlimited service quota;
the host filesystem can still fill, so monitor both `/v1/storage` and the
volume itself. A stream retention policy allows the next upload to use space
that the same committed upload will reclaim from older versions; failed uploads
do not prune those versions. Retention policy is stream-specific and does not
replace an independent backup of the service volume.

Retention reduces committed storage after a successful upload; it does not
remove the temporary peak. Keep enough filesystem space for the current retained
set plus one maximum-size incoming snapshot. Enabling `apply_now` can reclaim
existing old versions immediately, but it is an explicit destructive operation
and should be previewed first.

`AIPERMISSION_BACKUP_MAX_UPLOAD_OPERATIONS` bounds the durable idempotency
ledger and defaults to 1,000,000 entries. Tombstones are deliberately never
aged out because reuse could duplicate an upload after a lost response. At the
limit, increase the configured bound after checking disk capacity; existing
operation identities remain replayable while new identities fail closed.

## Upgrade

1. Stop new uploads from AIPermission.
2. Back up the service volume.
3. Set `AIPERMISSION_BACKUP_VERSION` to the intended release and run
   `docker compose -f docker-compose.release.yml pull`.
4. Run `docker compose -f docker-compose.release.yml up -d` and wait for
   `/healthz`.
5. Verify `/v1/info` protocol compatibility before resuming uploads.

Protocol 4 is the minimum compatible protocol for current AIPermission
clients. It makes retained upload-operation tombstones part of the wire
contract; older daemons are rejected before a client prepares a new upload.

Before changing an existing metadata schema, the service creates a durable
`metadata.pre-migration-v<version>.db` snapshot beside `metadata.db`. Keep that
file until the upgraded service has been verified, but treat it as diagnostic
metadata rather than a complete rollback backup.

To roll back, stop the service and restore the **entire service volume** from
the snapshot taken in upgrade step 2 before starting the older image. Never
restore `metadata.db` alone: uploads or deletions performed after migration can
make old metadata disagree with the blob tree, and an older daemon may then
delete valid post-migration blobs as orphans or expose stale records whose blobs
no longer exist. If any upload, retention, prune, or delete operation occurred
after the upgrade, a matching full-volume snapshot is mandatory.

Never point two service versions at the same writable volume concurrently.
