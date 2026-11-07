# Fault-tolerance walkthrough

Run the broker and three [README demo roles](../README.md#demo-walkthrough). Start two order-consumer processes if you want to see more competing workers. The producer sends 100 topic-routed orders, including 10 intentionally failing notifications.

1. Observe `orders` topic bindings in the dashboard: `orders.*` → `order-processing`, `orders.#` → `notifications`.
2. Run `minirabbit stats` and `minirabbit queue list`. Published should increase by 100 per producer run. Both queues receive independent routed copies, while competing workers divide a queue's deliveries.
3. With workers active, successful ACKs remove messages. Failing notification IDs retry three total attempts with bounded backoff and then move to `dead-letters`. `minirabbit dlq list` lists its statistics.
4. The dashboard Dead Letters page can lease one failed entry, show reason/attempt headers, return it, or replay to its original route. Replaying a deliberately failing notification will fail again; this is expected.
5. For a controlled consumer crash, use REST consume with `consumerId=manual-1`, `prefetch=1`, `waitMs=0`, record the returned token, then do not ACK. Wait past the 30-second default lease plus retry delay. Consume again under another ID. Message UUID is preserved, token changes, and `redelivered=true`. ACKing the old token returns 409.
6. Publish with workers stopped, stop/restart the broker with the same data directory, and verify durable queue entries remain. Consumer registrations do not survive. Scheduled persistent work also survives; transient work does not.
7. Publish through REST using `delayMs=5000`. Consume before the due time returns 204 if no other work exists; consume after the due time returns the entry. Use `ttlMs=1000` with `delayMs=5000` to verify the entry expires instead of being delivered.
8. Declare a queue with `maxMessages=1` using REST/SDK, bind it to a dedicated route, and publish twice without consuming. The second publish returns 429. Other matching queues must not receive a partial second copy.
9. Request `/metrics` with bearer authentication and inspect actual delivery, redelivery, acknowledgement, dead-letter, storage, and queue values. Refresh the dashboard to see the same backend state.
10. Request compaction from the Storage page or `POST /api/v1/storage/compact`; queue state remains unchanged and WAL size typically decreases.

For a completely automated, isolated equivalent covering abrupt process recovery, routing 100 concurrent messages, cancellation/lease redelivery, stale ACK rejection, retries, DLQ, TTL, delays, overload, malformed HTTP requests, metrics, and SDK workers, run:

```bash
mvn -Dtest=BrokerTest,HttpIntegrationTest,CrashRecoveryTest test
```

`CrashRecoveryTest` terminates a child JVM with `Runtime.halt`, then checks unacked/scheduled recovery and durable ACK removal. It runs without an external server or broker. No physical power-cut or Docker fault test is claimed.
