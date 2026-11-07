package com.minirabbit.core;

import static com.minirabbit.core.Models.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

/** Bounded actor: only its worker touches state or WAL. Network work never runs here. */
public final class Broker implements AutoCloseable {
  public static final class Failure extends RuntimeException {
    public final int status;

    public Failure(int status, String message) {
      super(message);
      this.status = status;
    }
  }

  private record Session(String id, String queue, int prefetch, long touched) {}

  private record Flight(
      String token, String consumer, String queue, String entry, long deadline, long expires) {}

  private final BrokerConfiguration config;
  private final BrokerClock clock;
  private final Wal wal;
  private final ThreadPoolExecutor actor;
  private final ScheduledExecutorService timer;
  private State state;
  private Map<String, Session> sessions = new LinkedHashMap<>();
  private Map<String, Flight> flights = new LinkedHashMap<>();
  private volatile boolean closed, failed;
  private long lastCompaction, nextMaintenanceAttempt;
  private final long recoveryNanos;
  private long published,
      delivered,
      acked,
      nacked,
      redelivered,
      deadLettered,
      expired,
      publishNanos,
      deliveryLatencyNanos;

  public Broker(BrokerConfiguration config) throws IOException {
    this(config, BrokerClock.SYSTEM);
  }

  public Broker(BrokerConfiguration config, BrokerClock clock) throws IOException {
    this.config = config;
    this.clock = Objects.requireNonNull(clock);
    long start = clock.nanoTime();
    wal =
        new Wal(
            java.nio.file.Path.of(config.storageDirectory()),
            config.fsyncPolicy().equals("always"));
    try {
      state = wal.recover();
      validateRecovered();
    } catch (Exception e) {
      wal.close();
      throw e;
    }
    recoveryNanos = clock.nanoTime() - start;
    lastCompaction = clock.millis();
    actor =
        new ThreadPoolExecutor(
            1,
            1,
            0,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(config.maxPendingOperations()),
            Thread.ofPlatform().name("broker-state").factory(),
            new ThreadPoolExecutor.AbortPolicy());
    timer =
        Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().name("broker-clock").daemon().factory());
    timer.scheduleWithFixedDelay(
        () -> {
          if (closed || failed) return;
          try {
            call(
                () -> {
                  maintenance();
                  return null;
                });
          } catch (Exception e) {
            org.slf4j.LoggerFactory.getLogger(Broker.class)
                .warn("Maintenance failed: {}", e.getMessage());
          }
        },
        50,
        50,
        TimeUnit.MILLISECONDS);
  }

  private void validateRecovered() throws IOException {
    try {
      for (var q : state.queues.values()) {
        name(q.definition.name());
        if (!q.definition.durable()) throw new IllegalArgumentException();
        for (var e : q.entries.values()) {
          if (!e.message.properties().persistent()
              || e.id == null
              || e.message.payload().length > q.definition.maxMessageBytes())
            throw new IllegalArgumentException();
        }
      }
    } catch (RuntimeException e) {
      throw new IOException("Invalid recovered state", e);
    }
  }

  public BrokerConfiguration configuration() {
    return config;
  }

  private <T> T call(Supplier<T> action) {
    if (closed || failed) throw new Failure(503, "Broker unavailable");
    CompletableFuture<T> result = new CompletableFuture<>();
    try {
      actor.execute(
          () -> {
            try {
              if (failed) throw new Failure(503, "Storage unavailable");
              result.complete(action.get());
            } catch (Throwable e) {
              if (e instanceof Error) failed = true;
              result.completeExceptionally(e);
            }
          });
    } catch (RejectedExecutionException e) {
      throw new Failure(429, "Pending operation limit reached");
    }
    try {
      return result.join();
    } catch (CompletionException e) {
      if (e.getCause() instanceof RuntimeException r) throw r;
      throw new IllegalStateException(e.getCause());
    }
  }

  private <T> T transaction(int type, Supplier<T> action) {
    State previous = state;
    var oldSessions = sessions;
    var oldFlights = flights;
    long oldDeadLettered = deadLettered, oldExpired = expired;
    state = Json.read(Json.bytes(state), State.class);
    sessions = new LinkedHashMap<>(sessions);
    flights = new LinkedHashMap<>(flights);
    try {
      T result = action.get();
      validateCapacity();
      byte[] bytes = Json.bytes(durable());
      wal.append(type, bytes);
      return result;
    } catch (IOException e) {
      org.slf4j.LoggerFactory.getLogger(Broker.class).error("WAL commit failed", e);
      state = previous;
      sessions = oldSessions;
      flights = oldFlights;
      deadLettered = oldDeadLettered;
      expired = oldExpired;
      failed = true;
      throw new Failure(503, "Storage write failed; restart and inspect storage");
    } catch (RuntimeException e) {
      state = previous;
      sessions = oldSessions;
      flights = oldFlights;
      deadLettered = oldDeadLettered;
      expired = oldExpired;
      throw e;
    }
  }

  private void validateCapacity() {
    long bytes = 0, count = 0;
    for (var q : state.queues.values()) {
      count += q.entries.size();
      if (q.entries.size() > q.definition.maxMessages())
        throw new Failure(429, "Queue capacity reached");
      for (var e : q.entries.values())
        bytes +=
            e.message.payload().length + 512L + Json.bytes(e.message.properties().headers()).length;
    }
    if (count > config.maxTotalMessages() || bytes > config.maxResidentBytes())
      throw new Failure(429, "Broker resident capacity reached");
  }

  private State durable() {
    State d = new State();
    d.sequence = state.sequence;
    state.exchanges.forEach(
        (k, v) -> {
          if (v.durable()) d.exchanges.put(k, v);
        });
    state.queues.forEach(
        (k, v) -> {
          if (v.definition.durable()) {
            QueueState q = new QueueState();
            q.definition = v.definition;
            v.entries.forEach(
                (id, e) -> {
                  if (e.message.properties().persistent()) q.entries.put(id, e);
                });
            d.queues.put(k, q);
          }
        });
    for (var b : state.bindings)
      if (d.exchanges.containsKey(b.exchange()) && d.queues.containsKey(b.queue()))
        d.bindings.add(b);
    return d;
  }

  public static String name(String name) {
    if (name == null || !name.matches("[A-Za-z0-9][A-Za-z0-9_.-]{0,127}"))
      throw new Failure(400, "Invalid resource name");
    return name;
  }

  private static String key(String key) {
    if (key == null || key.length() > 255 || key.chars().anyMatch(Character::isISOControl))
      throw new Failure(400, "Invalid routing key");
    return key;
  }

  private QueueState queue(String name) {
    QueueState q = state.queues.get(name);
    if (q == null) throw new Failure(404, "Queue not found");
    return q;
  }

  public Exchange declareExchange(Exchange exchange) {
    name(exchange.name());
    if (exchange.type() == null) throw new Failure(400, "Exchange type required");
    return call(
        () ->
            transaction(
                1,
                () -> {
                  var old = state.exchanges.get(exchange.name());
                  if (old != null && !old.equals(exchange))
                    throw new Failure(409, "Exchange declaration conflict");
                  if (old == null && state.exchanges.size() >= 1024)
                    throw new Failure(429, "Exchange limit");
                  state.exchanges.put(exchange.name(), exchange);
                  return exchange;
                }));
  }

  public QueueDefinition declareQueue(QueueDefinition definition) {
    name(definition.name());
    if (definition.deadLetter() != null) {
      name(definition.deadLetter().exchange());
      key(definition.deadLetter().routingKey());
    }
    return call(
        () ->
            transaction(
                1,
                () -> {
                  var old = state.queues.get(definition.name());
                  if (old != null) {
                    if (!old.definition.equals(definition))
                      throw new Failure(409, "Queue declaration conflict");
                    return definition;
                  }
                  if (state.queues.size() >= 1024) throw new Failure(429, "Queue limit");
                  QueueState q = new QueueState();
                  q.definition = definition;
                  state.queues.put(definition.name(), q);
                  return definition;
                }));
  }

  public Binding bind(Binding b) {
    name(b.exchange());
    name(b.queue());
    key(b.key());
    return call(
        () ->
            transaction(
                1,
                () -> {
                  if (!state.exchanges.containsKey(b.exchange()))
                    throw new Failure(404, "Exchange not found");
                  queue(b.queue());
                  if (!state.bindings.contains(b)) {
                    if (state.bindings.size() >= 4096) throw new Failure(429, "Binding limit");
                    state.bindings.add(b);
                  }
                  return b;
                }));
  }

  public void deleteQueue(String name) {
    call(
        () ->
            transaction(
                5,
                () -> {
                  queue(name);
                  state.queues.remove(name);
                  state.bindings.removeIf(b -> b.queue().equals(name));
                  sessions.values().removeIf(s -> s.queue.equals(name));
                  flights.values().removeIf(f -> f.queue.equals(name));
                  return null;
                }));
  }

  public void deleteExchange(String name) {
    call(
        () ->
            transaction(
                1,
                () -> {
                  if (state.exchanges.remove(name) == null)
                    throw new Failure(404, "Exchange not found");
                  state.bindings.removeIf(b -> b.exchange().equals(name));
                  return null;
                }));
  }

  private Set<String> targets(String exchange, String routingKey) {
    var ex = state.exchanges.get(exchange);
    if (ex == null) throw new Failure(404, "Exchange not found");
    Set<String> result = new LinkedHashSet<>();
    for (var b : state.bindings)
      if (b.exchange().equals(exchange)
          && switch (ex.type()) {
            case DIRECT -> b.key().equals(routingKey);
            case FANOUT -> true;
            case TOPIC -> Routing.topic(b.key(), routingKey);
          }) result.add(b.queue());
    return result;
  }

  public PublisherConfirmation publish(Publish p) {
    name(p.exchange());
    key(p.routingKey());
    if (p.properties().headers().size() > 32)
      throw new Failure(400, "At most 32 external headers are allowed");
    if (p.payload().length > config.maxMessageBytes())
      throw new Failure(413, "Message exceeds broker size limit");
    return call(
        () -> {
          long start = clock.nanoTime();
          var target = targets(p.exchange(), p.routingKey());
          String id = UUID.randomUUID().toString();
          if (target.isEmpty())
            return new PublisherConfirmation(
                id, p.mandatory() ? ConfirmStatus.UNROUTABLE : ConfirmStatus.ACCEPTED, 0);
          var result =
              transaction(
                  2,
                  () -> {
                    Message m =
                        new Message(
                            id,
                            p.exchange(),
                            p.routingKey(),
                            p.payload(),
                            p.properties(),
                            clock.millis());
                    for (String q : target)
                      enqueue(queue(q), m, m.timestamp() + p.properties().delayMs(), 0, 0);
                    boolean persisted =
                        p.properties().persistent()
                            && config.fsyncPolicy().equals("always")
                            && target.stream().allMatch(q -> queue(q).definition.durable());
                    return new PublisherConfirmation(
                        id,
                        persisted ? ConfirmStatus.PERSISTED : ConfirmStatus.ACCEPTED,
                        target.size());
                  });
          published++;
          publishNanos += clock.nanoTime() - start;
          return result;
        });
  }

  private void enqueue(QueueState q, Message m, long available, int attempts, int hops) {
    if (q.entries.size() >= q.definition.maxMessages())
      throw new Failure(429, "Queue capacity reached: " + q.definition.name());
    if (m.payload().length > q.definition.maxMessageBytes())
      throw new Failure(413, "Message exceeds queue size limit");
    Entry e = new Entry();
    e.id = UUID.randomUUID().toString();
    e.message = m;
    e.availableAt = available;
    e.attempts = attempts;
    e.deadLetterHops = hops;
    e.sequence = ++state.sequence;
    long ttl = m.properties().ttlMs(), qt = q.definition.ttlMs();
    if (ttl == 0 || qt > 0 && qt < ttl) ttl = qt;
    e.expiresAt = ttl == 0 ? 0 : m.timestamp() + ttl;
    q.entries.put(e.id, e);
  }

  public Subscription subscribe(String queue, String consumerId, int prefetch) {
    name(consumerId);
    if (prefetch < 1 || prefetch > config.maxPrefetch()) throw new Failure(400, "Invalid prefetch");
    return call(
        () -> {
          queue(queue);
          var old = sessions.get(consumerId);
          if (old != null && (!old.queue.equals(queue) || old.prefetch != prefetch))
            throw new Failure(409, "Consumer already registered with different configuration");
          if (old == null && sessions.size() >= config.maxConsumers())
            throw new Failure(429, "Consumer limit reached");
          sessions.put(consumerId, new Session(consumerId, queue, prefetch, clock.nanoTime()));
          return new Subscription(consumerId, queue, prefetch);
        });
  }

  private Session session(String id) {
    var s = sessions.get(id);
    if (s == null) throw new Failure(404, "Consumer not registered");
    return s;
  }

  public void setPrefetch(String id, int prefetch) {
    if (prefetch < 1 || prefetch > config.maxPrefetch()) throw new Failure(400, "Invalid prefetch");
    call(
        () -> {
          var s = session(id);
          if (unacked(id) > prefetch)
            throw new Failure(409, "New prefetch is below current unacknowledged count");
          sessions.put(id, new Session(id, s.queue, prefetch, clock.nanoTime()));
          return null;
        });
  }

  private int unacked(String id) {
    return (int) flights.values().stream().filter(f -> f.consumer.equals(id)).count();
  }

  public Optional<Delivery> poll(String consumerId) {
    return call(
        () -> {
          maintenance();
          var s = session(consumerId);
          sessions.put(s.id, new Session(s.id, s.queue, s.prefetch, clock.nanoTime()));
          if (unacked(s.id) >= s.prefetch || flights.size() >= config.maxInFlight())
            return Optional.empty();
          var q = queue(s.queue);
          long now = clock.millis();
          Set<String> active = new HashSet<>();
          flights.values().forEach(f -> active.add(f.entry));
          Comparator<Entry> ordering = Comparator.comparingLong(e -> e.sequence);
          if (q.definition.priorities())
            ordering =
                Comparator.<Entry>comparingInt(e -> -e.message.properties().priority())
                    .thenComparingLong(e -> e.sequence);
          var candidate =
              q.entries.values().stream()
                  .filter(
                      e ->
                          !active.contains(e.id)
                              && e.attempts < q.definition.retry().maxAttempts()
                              && e.availableAt <= now
                              && (e.expiresAt == 0 || e.expiresAt > now))
                  .min(ordering);
          if (candidate.isEmpty()) return Optional.empty();
          String entryId = candidate.get().id;
          var result =
              transaction(
                  3,
                  () -> {
                    Entry e = queue(s.queue).entries.get(entryId);
                    e.attempts++;
                    String token = UUID.randomUUID().toString();
                    long expires = now + config.visibilityTimeoutMs();
                    flights.put(
                        token,
                        new Flight(
                            token,
                            s.id,
                            s.queue,
                            e.id,
                            clock.nanoTime()
                                + TimeUnit.MILLISECONDS.toNanos(config.visibilityTimeoutMs()),
                            expires));
                    return new Delivery(
                        token, s.id, s.queue, e.message, e.attempts, e.attempts > 1, expires);
                  });
          delivered++;
          if (result.redelivered()) redelivered++;
          deliveryLatencyNanos +=
              TimeUnit.MILLISECONDS.toNanos(Math.max(0, now - result.message().timestamp()));
          return Optional.of(result);
        });
  }

  private Flight flight(String token, String consumer) {
    var f = flights.get(token);
    if (f == null || !f.consumer.equals(consumer) || clock.nanoTime() >= f.deadline)
      throw new Failure(409, "Stale or foreign delivery token");
    return f;
  }

  public void ack(String token, String consumer) {
    call(
        () -> {
          flight(token, consumer);
          transaction(
              4,
              () -> {
                var f = flight(token, consumer);
                queue(f.queue).entries.remove(f.entry);
                flights.remove(token);
                return null;
              });
          acked++;
          return null;
        });
  }

  public void nack(String token, String consumer, boolean requeue) {
    call(
        () -> {
          flight(token, consumer);
          transaction(
              6,
              () -> {
                var f = flight(token, consumer);
                flights.remove(token);
                var q = queue(f.queue);
                var e = q.entries.get(f.entry);
                if (requeue) retry(q, e, "nack");
                else deadLetter(q, e, "rejected");
                return null;
              });
          nacked++;
          return null;
        });
  }

  public void renew(String token, String consumer) {
    call(
        () -> {
          var f = flight(token, consumer);
          long now = clock.millis();
          flights.put(
              token,
              new Flight(
                  token,
                  consumer,
                  f.queue,
                  f.entry,
                  clock.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.visibilityTimeoutMs()),
                  now + config.visibilityTimeoutMs()));
          var s = session(consumer);
          sessions.put(consumer, new Session(s.id, s.queue, s.prefetch, clock.nanoTime()));
          return null;
        });
  }

  public void disconnect(String consumer) {
    call(
        () ->
            transaction(
                6,
                () -> {
                  session(consumer);
                  release(consumer);
                  return null;
                }));
  }

  private void release(String id) {
    sessions.remove(id);
    var list = flights.values().stream().filter(f -> f.consumer.equals(id)).toList();
    for (var f : list) {
      flights.remove(f.token);
      var q = queue(f.queue);
      retry(q, q.entries.get(f.entry), "disconnect");
    }
  }

  private void retry(QueueState q, Entry e, String reason) {
    if (e.expiresAt > 0 && e.expiresAt <= clock.millis()) {
      deadLetter(q, e, "expired");
      return;
    }
    if (e.attempts >= q.definition.retry().maxAttempts()) {
      deadLetter(q, e, "attempts-exhausted");
      return;
    }
    e.availableAt = clock.millis() + q.definition.retry().delay(e.attempts);
    e.sequence = ++state.sequence;
  }

  private void deadLetter(QueueState q, Entry e, String reason) {
    var policy = q.definition.deadLetter();
    q.entries.remove(e.id);
    if (policy == null
        || e.deadLetterHops >= policy.maxHops()
        || !state.exchanges.containsKey(policy.exchange())) return;
    var target = targets(policy.exchange(), policy.routingKey());
    target.remove(q.definition.name());
    if (target.isEmpty()) return;
    // Keep source pending if DLQ cannot accept: the entire outer transaction rolls back.
    var headers = new HashMap<>(e.message.properties().headers());
    headers.putIfAbsent("x-original-exchange", e.message.exchange());
    headers.putIfAbsent("x-original-routing-key", e.message.routingKey());
    headers.put("x-failure-reason", reason);
    headers.put("x-attempts", Integer.toString(e.attempts));
    headers.put("x-dead-letter-at", Long.toString(clock.millis()));
    var props =
        new MessageProperties(
            e.message.properties().contentType(),
            e.message.properties().persistent(),
            0,
            0,
            e.message.properties().priority(),
            headers);
    Message m =
        new Message(
            e.message.id(),
            policy.exchange(),
            policy.routingKey(),
            e.message.payload(),
            props,
            clock.millis());
    for (var name : target) enqueue(queue(name), m, m.timestamp(), 0, e.deadLetterHops + 1);
    deadLettered++;
  }

  private void maintenance() {
    long nano = clock.nanoTime(), now = clock.millis();
    if (now < nextMaintenanceAttempt) return;
    var idle =
        sessions.values().stream()
            .filter(
                s ->
                    nano - s.touched
                        >= TimeUnit.MILLISECONDS.toNanos(config.consumerIdleTimeoutMs()))
            .map(Session::id)
            .toList();
    var timed =
        flights.values().stream().filter(f -> nano >= f.deadline).map(Flight::token).toList();
    Set<String> active = new HashSet<>();
    flights.values().forEach(f -> active.add(f.entry));
    boolean work =
        state.queues.values().stream()
            .flatMap(q -> q.entries.values().stream())
            .anyMatch(
                e ->
                    !active.contains(e.id)
                        && (e.expiresAt > 0 && e.expiresAt <= now || e.attempts >= 100));
    // Recovered deliveries retain attempts but no active lease. Enforce retry bounds before
    // dispatch.
    work |=
        state.queues.values().stream()
            .anyMatch(
                q ->
                    q.entries.values().stream()
                        .anyMatch(
                            e ->
                                !active.contains(e.id)
                                    && e.attempts >= q.definition.retry().maxAttempts()));
    if (!idle.isEmpty() || !timed.isEmpty() || work)
      try {
        transaction(
            7,
            () -> {
              for (String id : idle) release(id);
              for (String token : timed) {
                var f = flights.remove(token);
                if (f != null) {
                  var q = queue(f.queue);
                  retry(q, q.entries.get(f.entry), "lease-expired");
                }
              }
              Set<String> inFlight = new HashSet<>();
              flights.values().forEach(f -> inFlight.add(f.entry));
              for (var q : new ArrayList<>(state.queues.values()))
                for (var e : new ArrayList<>(q.entries.values()))
                  if (!inFlight.contains(e.id)) {
                    if (e.expiresAt > 0 && e.expiresAt <= now) {
                      deadLetter(q, e, "expired");
                      expired++;
                    } else if (e.attempts >= q.definition.retry().maxAttempts())
                      deadLetter(q, e, "attempts-exhausted");
                  }
              return null;
            });
      } catch (Failure e) {
        if (e.status == 429 || e.status == 413) {
          nextMaintenanceAttempt = now + 1000;
          return;
        }
        throw e;
      }
    if (now - lastCompaction >= config.compactionIntervalMs()) {
      compactInternal();
      lastCompaction = now;
    }
  }

  private void compactInternal() {
    try {
      wal.compact(Json.bytes(durable()));
    } catch (IOException e) {
      org.slf4j.LoggerFactory.getLogger(Broker.class).error("WAL compaction failed", e);
      failed = true;
      throw new Failure(503, "Compaction failed; restart required");
    }
  }

  public void compact() {
    call(
        () -> {
          compactInternal();
          return null;
        });
  }

  public List<Exchange> exchanges() {
    return call(() -> List.copyOf(state.exchanges.values()));
  }

  public List<Binding> bindings() {
    return call(() -> List.copyOf(state.bindings));
  }

  private QueueStatistics statistics(QueueState q) {
    int unacked =
        (int) flights.values().stream().filter(f -> f.queue.equals(q.definition.name())).count();
    Set<String> active = new HashSet<>();
    flights.values().forEach(f -> active.add(f.entry));
    long now = clock.millis();
    int scheduled =
        (int)
            q.entries.values().stream()
                .filter(e -> !active.contains(e.id) && e.availableAt > now)
                .count();
    return new QueueStatistics(
        q.definition,
        q.entries.size(),
        q.entries.size() - unacked - scheduled,
        scheduled,
        unacked,
        (int) sessions.values().stream().filter(s -> s.queue.equals(q.definition.name())).count());
  }

  public List<QueueStatistics> queues() {
    return call(
        () -> {
          maintenance();
          return state.queues.values().stream().map(this::statistics).toList();
        });
  }

  public QueueStatistics queueStatistics(String name) {
    return call(
        () -> {
          maintenance();
          return statistics(queue(name));
        });
  }

  public List<Consumer> consumers() {
    return call(
        () ->
            sessions.values().stream()
                .map(s -> new Consumer(s.id, s.queue, s.prefetch, unacked(s.id), "ACTIVE"))
                .toList());
  }

  public Map<String, Object> stats() {
    return call(
        () -> {
          Map<String, Object> m = new LinkedHashMap<>();
          m.put("published", published);
          m.put("delivered", delivered);
          m.put("acked", acked);
          m.put("nacked", nacked);
          m.put("redelivered", redelivered);
          m.put("deadLettered", deadLettered);
          m.put("expired", expired);
          m.put("queueDepth", state.queues.values().stream().mapToInt(q -> q.entries.size()).sum());
          m.put("unacked", flights.size());
          m.put("activeConsumers", sessions.size());
          m.put(
              "persistentMessages",
              durable().queues.values().stream().mapToInt(q -> q.entries.size()).sum());
          try {
            m.put("storageBytes", wal.size());
          } catch (IOException e) {
            throw new Failure(503, "Storage inaccessible");
          }
          m.put("compactions", wal.compactions());
          m.put("recoverySeconds", recoveryNanos / 1e9);
          m.put("publishSeconds", publishNanos / 1e9);
          m.put("deliveryLatencySeconds", deliveryLatencyNanos / 1e9);
          m.put("fsyncPolicy", config.fsyncPolicy());
          m.put("ready", !failed && !closed);
          return m;
        });
  }

  public String metrics() {
    var m = stats();
    StringBuilder b = new StringBuilder();
    Map<String, String> metrics =
        Map.ofEntries(
            Map.entry("published", "messages_published_total"),
            Map.entry("delivered", "messages_delivered_total"),
            Map.entry("acked", "messages_acked_total"),
            Map.entry("nacked", "messages_nacked_total"),
            Map.entry("redelivered", "messages_redelivered_total"),
            Map.entry("deadLettered", "messages_dead_lettered_total"),
            Map.entry("queueDepth", "queue_depth"),
            Map.entry("unacked", "unacked_messages"),
            Map.entry("activeConsumers", "active_consumers"),
            Map.entry("storageBytes", "storage_bytes"),
            Map.entry("recoverySeconds", "wal_recovery_duration_seconds"),
            Map.entry("publishSeconds", "publish_duration_seconds_sum"),
            Map.entry("deliveryLatencySeconds", "delivery_latency_seconds_sum"));
    metrics.forEach(
        (k, v) -> b.append("minirabbit_").append(v).append(' ').append(m.get(k)).append('\n'));
    return b.toString();
  }

  @Override
  public void close() throws IOException {
    if (closed) return;
    closed = true;
    timer.shutdownNow();
    actor.shutdown();
    boolean interrupted = false;
    while (true)
      try {
        if (actor.awaitTermination(1, TimeUnit.SECONDS)) break;
      } catch (InterruptedException e) {
        interrupted = true;
      }
    try {
      wal.close();
    } finally {
      if (interrupted) Thread.currentThread().interrupt();
    }
  }
}
