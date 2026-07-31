# Contributing

Keep changes inside the passive encrypted-storage boundary described in the
README. Account systems, team RBAC, SaaS control planes, remote AIPermission
gateways, decryption, and connector execution are out of scope.

Before opening a pull request:

```bash
make check
docker compose build
```

Update protocol and deployment documentation when behavior changes. Never log
or return bearer tokens, request authorization headers, database passwords, or
encrypted backup bytes in JSON errors.

