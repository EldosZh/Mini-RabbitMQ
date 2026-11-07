# Routing and queues

An exchange selects a **set of queue names**. Overlapping bindings never produce duplicate copies in the same queue. Each target queue receives its own envelope with an independent acknowledgement lifecycle, while copies share the message UUID.

* **DIRECT:** binding key equals routing key.
* **FANOUT:** every bound queue; key ignored.
* **TOPIC:** dot-delimited segments; `*` matches exactly one segment, `#` zero or more. A dynamic-programming table gives O(pattern segments × routing-key segments) time and memory.

An empty key has zero segments; `#` matches it, `*` does not. Consecutive dots denote empty segments, and `*` can match one empty segment. Literal wildcard characters in a published key are ordinary data. Keys are limited to 255 characters; resource names match `[A-Za-z0-9][A-Za-z0-9_.-]{0,127}`.

Publishing validates every target and commits all queue copies in one WAL transaction. Capacity or message-size failure rejects the entire publish, including previously examined targets. Mandatory unroutable publishing returns `UNROUTABLE` with zero targets. Non-mandatory unroutable publishing returns `ACCEPTED` with zero targets and drops the message.

## Queue engine and ordering

Each queue is an actor-owned insertion-preserving map with monotonically increasing sequence numbers. Dispatch chooses the eligible entry with the lowest sequence; optional priorities choose highest priority first, then sequence. Dispatch scans the bounded queue rather than maintaining a more complex indexed scheduler.

Single consumer, equal priority, no delays, no redeliveries: FIFO delivery. Multiple consumers can complete in any order. Retries receive a new sequence and become eligible after backoff; they do not block ready work. Future scheduled messages do not block later ready messages. Priority range is 0–9; higher priorities can starve lower priorities. There is no strict global FIFO with priorities or retries.

Pull requests enter the actor in admission order. Any requesting consumer below its prefetch limit can take the next eligible message. Idle consumers do not reserve work. This is fair FIFO request admission, not strict round-robin assignment or equal distribution to differently paced workers.

Deleting a queue atomically removes its entries, bindings, subscriptions, and active tokens. Subsequent ACKs fail. Repeated equivalent topology declarations are safe; conflicting declarations return 409.
