# Backup Protocol v1

All `/v1` routes require:

```http
Authorization: Bearer <high-entropy-owner-token>
```

All routes except `/v1/info` additionally require:

```http
X-AIPermission-Protocol-Version: 1
```

JSON failures use this shape:

```json
{
  "error": {
    "code": "stable_machine_code",
    "message": "human-readable detail"
  }
}
```

## Health And Compatibility

```http
GET /healthz
GET /v1/info
```

`/healthz` is intentionally unauthenticated and returns only service health.
`/v1/info` returns the service version, protocol version, storage schema,
capabilities, and maximum upload size.

## Storage Usage And Quota

```http
GET /v1/storage
```

Returns the stored byte count, backup and stream counts, pending blob cleanup
count, and configured quota/remaining bytes when
`AIPERMISSION_BACKUP_MAX_STORAGE_BYTES` is enabled. Usage failures return an
error response; clients must not interpret a failed check as zero usage.

An upload that exceeds the configured quota returns
`507 storage_quota_exceeded` without creating stream or backup metadata.

## Upload An Immutable Version

```http
POST /v1/streams/{stream_id}/backups
Content-Type: application/octet-stream
X-AIPermission-Database-Name: My Project
X-AIPermission-Source-Installation-ID: install_d5dce07f
X-AIPermission-Protocol-Version: 1

<encrypted .aipdb bytes>
```

`stream_id` and the source installation ID must contain 1-128 ASCII letters,
digits, dots, underscores, or hyphens and must start with a letter or digit.
The display name is limited to 128 characters. Reusing a stream with a
different display name returns `409 stream_conflict`.

A successful response is `201 Created` with server-generated immutable backup
metadata and a `Location` header. Retrying the same body creates another
version; protocol v1 has no idempotency key. If automatic retention is enabled
for the stream, `retention_deleted_count` reports how many older versions were
pruned in the same metadata lifecycle.

## List Streams And Versions

```http
GET /v1/streams?limit=50&cursor=...
GET /v1/streams/{stream_id}/backups?limit=50&cursor=...
```

Limits are bounded to 1-100. Results use stable newest-first ordering and an
opaque `next_cursor` when another page exists. Stream records include
`retention_keep_latest` when automatic retention is enabled.

## Automatic Retention

Read the current policy:

```http
GET /v1/streams/{stream_id}/retention
```

Preview a policy without changing storage:

```http
POST /v1/streams/{stream_id}/retention/preview
Content-Type: application/json

{"keep_latest": 10}
```

The preview returns retained/deleted object and byte counts. Enable or disable
the policy with:

```http
PUT /v1/streams/{stream_id}/retention
Content-Type: application/json

{"enabled": true, "keep_latest": 10, "apply_now": true}
```

`keep_latest` is bounded to 1-1000. `apply_now` deletes the previewed older
versions as part of the policy update; when false, existing versions remain
until the next successful upload. Future uploads apply the enabled policy
automatically. The newest recovery version is always retained.

## Download One Version

```http
GET /v1/streams/{stream_id}/backups/{backup_id}
```

The service verifies the stored byte size and SHA-256 before returning
`application/octet-stream`. `X-AIPermission-SHA256` contains the lowercase hex
digest. Missing blobs or digest mismatches return `409 backup_corrupt` rather
than unverified content.

## Prune Old Versions

```http
POST /v1/streams/{stream_id}/prune
Content-Type: application/json

{"keep_latest": 10}
```

`keep_latest` must be an integer from 1 to 1000. The service preserves exactly
the newest requested number of immutable versions and permanently removes only
older versions from that stream. Metadata deletion is transactional; blob
paths are queued durably and removed after commit so interrupted cleanup can
resume on service startup. The endpoint never deletes the newest version.

## Delete Selected Versions

```http
DELETE /v1/streams/{stream_id}/backups/{backup_id}
```

For an explicitly reviewed batch:

```http
POST /v1/streams/{stream_id}/backups/delete
Content-Type: application/json

{"backup_ids":["bkp_first","bkp_second"]}
```

Batch deletion accepts 1-100 unique backup IDs. Every requested ID must exist
in the selected stream or the operation fails without deleting anything. At
least one recovery version must remain in each stream. Selected-version and
prune operations use the same transactional metadata deletion and durable blob
cleanup queue.

## Deliberately Absent From v1

- stream rename or overwrite;
- background jobs or scheduled uploads;
- server-side restore or SQLCipher access;
- account, OAuth, team, or sharing APIs;
- remote gateway and connector operations.
