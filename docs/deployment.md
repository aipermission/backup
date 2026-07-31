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

## Upgrade

1. Stop new uploads from AIPermission.
2. Back up the service volume.
3. Set `AIPERMISSION_BACKUP_VERSION` to the intended release and run
   `docker compose -f docker-compose.release.yml pull`.
4. Run `docker compose -f docker-compose.release.yml up -d` and wait for
   `/healthz`.
5. Verify `/v1/info` protocol compatibility before resuming uploads.

Never point two service versions at the same writable volume concurrently.
