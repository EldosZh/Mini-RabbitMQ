package com.minirabbit.core;

import java.util.*;

/** Wire and domain types. Times are UTC epoch milliseconds; payload is JSON base64. */
public final class Models {
  private Models() {}

  public enum ExchangeType {
    DIRECT,
    FANOUT,
    TOPIC
  }

  public record Exchange(String name, ExchangeType type, boolean durable) {}

  public record Binding(String exchange, String queue, String key) {}

  public record RetryPolicy(int maxAttempts, long initialDelayMs, long maxDelayMs, boolean jitter) {
    public RetryPolicy {
      if (maxAttempts < 1
          || maxAttempts > 100
          || initialDelayMs < 1
          || maxDelayMs < initialDelayMs
          || maxDelayMs > 86_400_000) throw new IllegalArgumentException("Invalid retry policy");
    }

    public static RetryPolicy defaults() {
      return new RetryPolicy(5, 1000, 60000, false);
    }

    public long delay(int attempts) {
      long d =
          Math.min(maxDelayMs, initialDelayMs * (1L << Math.min(30, Math.max(0, attempts - 1))));
      return jitter
          ? java.util.concurrent.ThreadLocalRandom.current().nextLong(Math.max(1, d / 2), d + 1)
          : d;
    }
  }

  public record DeadLetterPolicy(String exchange, String routingKey, int maxHops) {
    public DeadLetterPolicy {
      if (exchange == null || routingKey == null || maxHops < 1 || maxHops > 16)
        throw new IllegalArgumentException("Invalid dead-letter policy");
    }
  }

  public record QueueDefinition(
      String name,
      boolean durable,
      int maxMessages,
      int maxMessageBytes,
      long ttlMs,
      boolean priorities,
      RetryPolicy retry,
      DeadLetterPolicy deadLetter) {
    public QueueDefinition {
      if (maxMessages < 1
          || maxMessages > 100000
          || maxMessageBytes < 1
          || maxMessageBytes > 8 * 1024 * 1024
          || ttlMs < 0
          || ttlMs > 604800000L) throw new IllegalArgumentException("Invalid queue limits");
      if (retry == null) retry = RetryPolicy.defaults();
    }

    public static QueueDefinition defaults(String name, boolean durable) {
      return new QueueDefinition(
          name, durable, 10000, 1024 * 1024, 0, false, RetryPolicy.defaults(), null);
    }
  }

  public record MessageProperties(
      String contentType,
      boolean persistent,
      long ttlMs,
      long delayMs,
      int priority,
      Map<String, String> headers) {
    public MessageProperties {
      if (ttlMs < 0
          || delayMs < 0
          || delayMs > 604_800_000L
          || ttlMs > 604_800_000L
          || priority < 0
          || priority > 9) throw new IllegalArgumentException("Invalid message properties");
      headers = headers == null ? Map.of() : Map.copyOf(headers);
      if (headers.size() > 40
          || headers.entrySet().stream()
              .anyMatch(
                  e ->
                      e.getKey().isBlank()
                          || e.getKey().length() > 128
                          || e.getValue().length() > 1024
                          || e.getKey().chars().anyMatch(Character::isISOControl)
                          || e.getValue().chars().anyMatch(Character::isISOControl)))
        throw new IllegalArgumentException("Headers exceed limits");
      if (contentType == null) contentType = "application/octet-stream";
      if (contentType.length() > 128) throw new IllegalArgumentException("Content type too long");
    }

    public static MessageProperties defaults() {
      return new MessageProperties("application/octet-stream", true, 0, 0, 0, Map.of());
    }
  }

  public record Message(
      String id,
      String exchange,
      String routingKey,
      byte[] payload,
      MessageProperties properties,
      long timestamp) {
    public Message {
      payload = payload.clone();
    }

    @Override
    public byte[] payload() {
      return payload.clone();
    }
  }

  public record Publish(
      String exchange,
      String routingKey,
      byte[] payload,
      MessageProperties properties,
      boolean mandatory) {
    public Publish {
      if (payload == null) throw new IllegalArgumentException("Payload required");
      payload = payload.clone();
      if (properties == null) properties = MessageProperties.defaults();
    }

    @Override
    public byte[] payload() {
      return payload.clone();
    }
  }

  public enum ConfirmStatus {
    ACCEPTED,
    PERSISTED,
    REJECTED,
    UNROUTABLE
  }

  public record PublisherConfirmation(String messageId, ConfirmStatus status, int queues) {}

  public record Consumer(String id, String queue, int prefetch, int unacked, String status) {}

  public record Subscription(String consumerId, String queue, int prefetch) {}

  public record Delivery(
      String token,
      String consumerId,
      String queue,
      Message message,
      int attempts,
      boolean redelivered,
      long leaseExpiresAt) {}

  public record Acknowledgement(String consumerId, boolean requeue) {}

  public record ConsumeRequest(String consumerId, int prefetch, long waitMs) {}

  public record PrefetchRequest(int prefetch) {}

  public record QueueStatistics(
      QueueDefinition definition,
      int messages,
      int ready,
      int scheduled,
      int unacked,
      int consumers) {}

  /** Mutable state objects are private to the broker actor; never returned to callers. */
  public static final class Entry {
    public String id;
    public Message message;
    public long sequence;
    public long availableAt;
    public long expiresAt;
    public int attempts;
    public int deadLetterHops;

    public Entry() {}
  }

  public static final class QueueState {
    public QueueDefinition definition;
    public LinkedHashMap<String, Entry> entries = new LinkedHashMap<>();

    public QueueState() {}
  }

  public static final class State {
    public LinkedHashMap<String, Exchange> exchanges = new LinkedHashMap<>();
    public LinkedHashMap<String, QueueState> queues = new LinkedHashMap<>();
    public ArrayList<Binding> bindings = new ArrayList<>();
    public long sequence;

    public State() {}
  }
}
