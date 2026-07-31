# Security Policy

Please report vulnerabilities privately through GitHub Security Advisories.
Do not open a public issue for authentication bypass, encrypted backup
disclosure, path traversal, integrity failures, or denial-of-service findings.

Include the affected version, reproduction steps, impact, and whether encrypted
backup content or bearer tokens were exposed.

## Supported Boundary

- one self-hosted owner;
- one high-entropy bearer token;
- HTTPS for every non-loopback deployment;
- immutable encrypted `.aipdb` storage;
- no decryption, command execution, connectors, accounts, or hosted control
  plane.

The token file is preferred because environment variables may be visible to
container inspection tooling. Protect the persistent volume and reverse-proxy
configuration as sensitive infrastructure.

The service cannot protect a weak SQLCipher database password from offline
guessing after encrypted blob disclosure. Password eligibility is enforced by
the AIPermission client before remote backup activation; it is not delegated to
this service because the service must never receive that password.

