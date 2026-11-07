# Failure recovery runbook

1. Stop writers if `/ready` returns failure or logs report storage failure. Preserve the data directory before investigation.
2. Check disk space, ownership, and the configured storage path. One broker may own a directory; a lock is released by process exit.
3. Start the same configuration/token. Startup verifies WAL framing/checksums. Incomplete trailing frames are truncated to the last complete transaction; corrupt complete frames fail startup.
4. If corruption is reported, restore a known-good backup or inspect the WAL offline. Do not edit checksums or skip records to make startup succeed. No automatic salvage mode is provided.
5. Check `/ready`, `/api/v1/stats`, queue counts, scheduled entries, and consumers. Consumers must reconnect; old tokens are invalid.
6. Expect duplicate work for entries whose previous consumer did not commit an ACK. Use application-level idempotency.
7. Drain dead-letter destinations if maintenance cannot transfer expired/exhausted work. Capacity failures retain the source and back off.
8. Use `POST /api/v1/storage/compact` when disk space permits writing a replacement snapshot. Compaction never removes the old file before the new file is forced and verified.

Back up a stopped broker's data directory. Live file copies may end with a torn frame; no coordinated live backup endpoint exists. Keep storage outside a disposable container using the Compose named volume. `docker compose down` retains that volume; `down -v` destroys it and should only be used intentionally.

The forced-process test uses `Runtime.halt` in a separate JVM after durable ACK, active delivery, delayed enqueue, and compaction, then recovers in the parent. It proves process-crash recovery on this filesystem. It does not simulate controller write caches, physical power loss, or atomic-rename failure at every instruction.
