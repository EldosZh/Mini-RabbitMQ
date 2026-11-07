# Security model

This is a trusted local broker, not a multi-tenant service. The YAML default binds to `127.0.0.1`. All `/api/v1/*` and `/metrics` calls require the shared bearer token from `MINIRABBIT_TOKEN`, at least 16 characters. Health/readiness are unauthenticated and expose no payloads. Tokens are compared with `MessageDigest.isEqual`. The same token grants all management, publishing, and consumption rights. There are no users, roles, per-queue ACLs, or token rotation APIs.

Generate a random token and set it in the environment; do not paste it into source, command-line arguments, Git, or screenshots. `.env` is ignored. The dashboard holds a token only in component memory and sends it as an authorization header; reload/disconnect clears it. No browser local/session storage is used. Logs omit credentials and message bodies; demos print IDs/status, while the explicit CLI `consume` command prints message content as requested by its operator.

Validate resource names, keys, configuration limits, payload bytes, JSON structure, enum values, duplicate JSON keys, header count/length, retry ranges, and bounded delays. Resource names never become filesystem paths; all broker files use fixed names under the operator-configured storage directory. Only trusted administrators should edit that directory/configuration. The storage process lock rejects concurrent broker instances. Checksum failures stop startup.

HTTP bodies, connections, concurrency, request/response duration, actor work, queue messages, resident bytes, consumers, prefetch, and topology are bounded. At extreme listener/executor saturation, requests may receive a connection failure rather than JSON 429. The built-in JDK HTTP transport is intentionally simple and needs a hardened proxy for Internet exposure.

Docker publishes only on loopback, uses a non-root broker UID, and an unprivileged Nginx dashboard. Docker service-network traffic is HTTP. For remote deployment, terminate TLS at a trusted reverse proxy, restrict network access, enforce request limits, and forward authorization securely. Embedded TLS and secure multi-tenant isolation are not implemented. The dashboard Nginx response sets CSP and anti-sniffing headers. Do not expose the Vite development server publicly.

The app has no arbitrary URL forwarding, shell commands driven by user messages, or user-supplied storage paths in its REST API. Protect local disk access and backups: WAL payloads are unencrypted. At-rest encryption is future work.
