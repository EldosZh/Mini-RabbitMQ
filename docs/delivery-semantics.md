# Delivery and failure semantics

## Delivery lifecycle

Register a consumer for one queue with a prefetch budget. Polling creates a random UUID token bound to that consumer, queue, and queue envelope. The entry stays in queue state until acknowledged and is excluded from other valid deliveries. Attempt count is committed before returning a delivery. Global in-flight and per-consumer limits both apply.

ACK removes only the envelope identified by the active token, with the removal committed before success. NACK/reject with requeue applies retry backoff; without requeue it transfers to the configured dead-letter route or drops when no route exists. Missing, foreign, already completed, or expired tokens return 409. Tokens are random across restart; old tokens can never acknowledge a later delivery.

Leases use monotonic nanosecond deadlines inside the process. Responses expose a UTC epoch-millisecond informational expiration. Renew extends the monotonic lease and refreshes consumer liveness. Idle subscriptions expire after the configured timeout. Explicit disconnect releases their deliveries through the retry policy. Transport connections do not define subscriptions: reconnect with the same consumer ID/configuration to continue; otherwise wait for timeout or cancel explicitly.

Prefetch changes use `PATCH /api/v1/consumers/{id}`. Reducing below the current unacknowledged count returns 409. Subsequent consume requests must specify the updated prefetch. Worker concurrency creates distinct subscription IDs, one for each callback loop.

## At-least-once

Durable queues, persistent messages, fsync `always`, and explicit ACKs provide recovery of accepted work until ACK or a configured terminal policy (TTL, rejection, retry exhaustion, deletion). Worker failure can cause duplicate processing. A crash after processing but before durable ACK necessarily allows redelivery. Consumers must make effects idempotent, usually using message UUIDs and a transaction in their own datastore. There is no exactly-once guarantee, publish deduplication, transactional application processing, or AMQP compatibility.

## Retries and dead letters

Attempts include the initial delivery. Requeue, lease expiry, and consumer cancellation schedule `min(maxDelay, initialDelay × 2^(attempts-1))`; setting initial=max gives fixed delay. Optional jitter samples uniformly from approximately half to all of that delay. Due times are persisted in UTC. Retry counts are bounded at 1–100, with delays of at least 1 ms. Exhausted work is dead-lettered or dropped.

Dead letters preserve message UUID and binary bytes, add original exchange/key, failure reason, original attempt count, and dead-letter timestamp headers, reset the delivery attempts for the next queue, and increment a persistent hop counter. A source queue never dead-letters back into itself. Each source policy enforces a maximum hop count (1–16), bounding cross-queue cycles even if policies differ. Missing/unbound dead-letter exchanges drop terminal work. Full/oversized destinations roll back the terminal transition. Periodic maintenance backs off one second on capacity errors so the destination can still be consumed. A full destination can hold source work beyond its lease/TTL until capacity is restored; no new delivery is made for an expired entry.

Queue capacity overflow rejects publishing atomically with HTTP 429; overflow dead-lettering is intentionally unsupported. Queue deletion discards work explicitly.

## TTL and scheduling

The effective TTL is the shorter nonzero value of message TTL and queue TTL. Expiration starts at original publish time, including delay and retry time. Expired ready work is never delivered; maintenance transfers or removes it. An active delivery remains valid until its lease expires and may still ACK after TTL, allowing already-running work to finish. NACK or lease expiry after TTL terminates it rather than redelivering. Dead-letter copies use a fresh timestamp and clear the original per-message TTL; the destination's queue TTL still applies.

Scheduled delivery uses persisted UTC epoch milliseconds. Due messages do not become visible early. Clock changes can shift wall-clock scheduling/TTL behavior; synchronize host time. Active lease deadlines remain monotonic.

## Publisher confirmations

`PERSISTED`: persistent message, every target queue durable, and fsync `always` completed. `ACCEPTED`: target acceptance without that full durability guarantee, or non-mandatory zero-target drop. `UNROUTABLE`: mandatory zero-target publish. Errors reject the operation through a typed exception/HTTP error rather than returning a `REJECTED` confirmation body. A rejected frame that encountered I/O has an uncertain recovery outcome, never a successful persisted confirmation.
