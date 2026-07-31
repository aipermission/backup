# Deployment

## Supported Shape

```text
AIPermission client -> HTTPS reverse proxy -> private backup container
```

The service is designed for one developer-operated installation and one strong
owner token. It is not a multi-user storage platform.

The sample Compose file exposes `127.0.0.1:8080` only. A reverse proxy on the
same host can terminate HTTPS and forward to that loopback endpoint. Do not
publish the raw HTTP port on `0.0.0.0` or rely on the bearer token as a
replacement for transport encryption.

Back up the Docker volume independently if service availability matters. The
volume contains encrypted AIPermission snapshots and limited metadata, but its
disclosure still allows offline guessing against weak database passwords.

## Upgrade

1. Stop new uploads from AIPermission.
2. Back up the service volume.
3. Pull the pinned release image.
4. Start the container and wait for `/healthz`.
5. Verify `/v1/info` protocol compatibility before resuming uploads.

Never point two service versions at the same writable volume concurrently.

