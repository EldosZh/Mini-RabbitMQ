# Persistence protocol

## Guarantees

| Queue | Message | After restart |
| --- | --- | --- |
| Durable | Persistent | Survives committed WAL replay |
| Durable | Transient | Definition survives, message disappears |
| Transient | Either | Definition and messages disappear |

Durable exchanges and bindings between durable endpoints survive. A persistent message in a durable queue survives even if it arrived through a transient exchange, but that exchange/binding does not. Consumers and tokens are never restored.

`always` calls `FileChannel.force(true)` before success on mutations, including enqueue, attempt increments, ACK, rejection, expiration, queue deletion, and metadata. `none` appends without forcing; only `ACCEPTED` confirms are emitted and a machine crash may lose recent changes, including ACKs. OS/hardware must honor flush requests. This project cannot promise survival of disk/controller failure.

## Binary WAL v1

Big-endian frame:

| Offset | Bytes | Field |
| --- | --- | --- |
| 0 | 4 | Magic `0x4D525131` (`MRQ1`) |
| 4 | 2 | Version `1` |
| 6 | 2 | Transaction type |
| 8 | 4 | JSON payload length, maximum 64 MiB |
| 12 | 4 | CRC32C of version byte, type byte, payload bytes |
| 16 | 4 | Header CRC32C of bytes 0–15 |
| 20 | length | UTF-8 durable `State` JSON |

Types: 1 topology/metadata, 2 enqueue, 3 delivery attempt, 4 ACK, 5 queue deletion, 6 rejection/retry/disconnect, 7 maintenance/expiration, 10 compacted snapshot. Types 8–9 are reserved and not emitted.

The payload contains durable exchange and queue definitions, durable bindings, a sequence counter, and live persistent envelopes. Every envelope includes a queue association through its enclosing queue map, envelope ID, message UUID, original exchange/key, binary payload encoded as base64, properties/headers, timestamp, sequence, UTC due/expiry timestamps, attempts, and dead-letter hop count. The length covers the complete transaction payload rather than an individual message. The format deliberately uses full-state transaction records instead of minimal enqueue/ACK deltas: routing and dead-letter transfers are atomic across queues with a simpler recovery protocol.

## Crash recovery

Acquire an exclusive process storage lock. Scan all frames with explicit size bounds; independently verify header CRC32C before trusting the payload length, then verify versions, JSON, and payload CRC32C. This prevents a damaged length field from masquerading as an incomplete append. Keep the latest complete state. A partial final header or a payload shorter than its verified declared length is truncated to the last complete frame and forced. A malformed header, checksum mismatch, or invalid JSON is a hard startup error; corruption is never skipped. Recovery cannot distinguish deliberate physical truncation of a final transaction from an interrupted append. Preserve a copy before repair if forensic analysis matters.

Ready and delayed envelopes are rebuilt from the latest snapshot. Attempts were persisted before delivery. Since runtime leases do not survive, in-flight envelopes become eligible again, and their next attempt is marked redelivered. Retry timestamps previously committed by NACK survive. A crash during a delivery can redeliver immediately after restart; it does not wait out the previous process's lease. Entries already at their retry limit are dead-lettered by maintenance before dispatch. ACKed envelopes are absent from the recovered state.

An append/fsync failure is an uncertain outcome: the client gets failure and the broker stops serving operations. A complete frame might still replay. Applications must deduplicate message IDs for uncertain publishes; publishes are never automatically retried by the SDK.

## Compaction

1. Serialize the actor's current durable state.
2. Write `broker.wal.tmp` with one complete checksummed snapshot.
3. Force the temporary file and replay it to verify it.
4. Close the old WAL handle and atomically replace it with the temporary file.
5. Reopen the WAL and force its parent directory where supported.

The old WAL remains authoritative until replacement. An abandoned temporary file is ignored on startup. Atomic replacement retries temporary access-denied errors up to five times with bounded delays (310 ms total), preserving the old WAL; other/persistent move failures are storage errors, with no unsafe non-atomic fallback. Initial WAL creation also forces its directory where supported. Windows does not expose portable directory fsync through this implementation; atomic rename and file flushing apply, but the strongest power-loss rename guarantees are filesystem/platform dependent. Compaction has been tested for live-state preservation and process termination after replacement, not hardware power cuts at every filesystem instruction.

The log grows between periodic compactions; monitor free disk space. Disk exhaustion fails the broker closed. No segmented WAL, incremental index, streaming replay, or disk quota enforcement is implemented.
