package com.minirabbit;

import static com.minirabbit.core.Models.*;
import static org.junit.jupiter.api.Assertions.*;

import com.minirabbit.core.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class BrokerTest {
  final ManualClock clock = new ManualClock();
  @TempDir Path dir;

  BrokerConfiguration config() {
    return new BrokerConfiguration(
        "127.0.0.1",
        0,
        dir.toString(),
        "always",
        128,
        16,
        32,
        10,
        32,
        1000,
        1024,
        1048576,
        100,
        1000,
        3600000,
        true);
  }

  QueueDefinition definition(
      String name,
      boolean durable,
      int capacity,
      boolean priorities,
      long ttl,
      DeadLetterPolicy dl) {
    return new QueueDefinition(
        name, durable, capacity, 1024, ttl, priorities, new RetryPolicy(3, 20, 100, false), dl);
  }

  void topology(Broker b, ExchangeType type) {
    b.declareExchange(new Exchange("e", type, true));
    b.declareQueue(definition("q", true, 1000, false, 0, null));
    b.bind(new Binding("e", "q", "a.*"));
  }

  Publish message(String key, String body) {
    return new Publish(
        "e",
        key,
        body.getBytes(java.nio.charset.StandardCharsets.UTF_8),
        MessageProperties.defaults(),
        true);
  }

  void await(BooleanSupplier check) throws Exception {
    long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!check.getAsBoolean()) {
      if (System.nanoTime() > end) fail("Condition timed out");
      clock.advance(10);
    }
  }

  @ParameterizedTest
  @CsvSource({
    "orders.*,orders.created,true",
    "orders.*,orders.eu.created,false",
    "orders.#,orders,true",
    "orders.#,orders.eu.created,true",
    "#.created,created,true",
    "#.created,x.y.created,true",
    "*.created,created,false",
    "#.#,a.b,true",
    "a.#.b,a.b,true",
    "a.#.b,a.x.y.b,true",
    "a.*.b,a.b,false",
    "#,a,true"
  })
  void topic(String pattern, String key, boolean result) {
    assertEquals(result, Routing.topic(pattern, key));
  }

  @Test
  void topicEmptySegments() {
    assertTrue(Routing.topic("#", ""));
    assertFalse(Routing.topic("*", ""));
    assertTrue(Routing.topic("a.*", "a."));
    assertTrue(Routing.topic("", ""));
  }

  @Test
  void allExchangeTypesAndDeduplication() throws Exception {
    for (var type : ExchangeType.values()) {
      Path sub = Files.createDirectory(dir.resolve(type.name()));
      var d = config();
      var c =
          new BrokerConfiguration(
              d.host(),
              d.port(),
              sub.toString(),
              d.fsyncPolicy(),
              128,
              16,
              32,
              10,
              32,
              1000,
              1024,
              1048576,
              100,
              1000,
              3600000,
              true);
      try (var b = new Broker(c, clock)) {
        b.declareExchange(new Exchange("e", type, true));
        b.declareQueue(definition("q", true, 100, false, 0, null));
        b.bind(new Binding("e", "q", type == ExchangeType.DIRECT ? "a.b" : "a.*"));
        b.bind(new Binding("e", "q", "#"));
        assertEquals(1, b.publish(message("a.b", "x")).queues());
        assertEquals(1, b.queueStatistics("q").messages());
        if (type == ExchangeType.DIRECT)
          assertEquals(ConfirmStatus.UNROUTABLE, b.publish(message("unmatched", "x")).status());
      }
    }
  }

  @Test
  void directAndUnroutable() throws Exception {
    try (var b = new Broker(config(), clock)) {
      b.declareExchange(new Exchange("e", ExchangeType.DIRECT, true));
      b.declareQueue(definition("q", true, 100, false, 0, null));
      b.bind(new Binding("e", "q", "a"));
      assertEquals(0, b.publish(message("b", "x")).queues());
      assertEquals(ConfirmStatus.UNROUTABLE, b.publish(message("b", "x")).status());
      assertEquals(
          ConfirmStatus.ACCEPTED,
          b.publish(new Publish("e", "b", new byte[0], null, false)).status());
      assertEquals(1, b.publish(message("a", "x")).queues());
    }
  }

  @Test
  void fifoPrefetchCompetingAndStaleAck() throws Exception {
    try (var b = new Broker(config(), clock)) {
      topology(b, ExchangeType.TOPIC);
      for (int i = 0; i < 4; i++) b.publish(message("a.b", "" + i));
      b.subscribe("q", "c1", 1);
      b.subscribe("q", "c2", 1);
      var one = b.poll("c1").orElseThrow();
      assertEquals("0", new String(one.message().payload()));
      assertTrue(b.poll("c1").isEmpty());
      var two = b.poll("c2").orElseThrow();
      assertNotEquals(one.message().id(), two.message().id());
      assertThrows(Broker.Failure.class, () -> b.ack(one.token(), "c2"));
      b.ack(one.token(), "c1");
      assertThrows(Broker.Failure.class, () -> b.ack(one.token(), "c1"));
      b.nack(two.token(), "c2", true);
      b.disconnect("c1");
      await(() -> b.queueStatistics("q").ready() > 0);
      var three = b.poll("c2").orElseThrow();
      assertEquals("2", new String(three.message().payload()));
      b.ack(three.token(), "c2");
    }
  }

  @Test
  void leaseRedeliveryRejectsOldToken() throws Exception {
    try (var b = new Broker(config(), clock)) {
      topology(b, ExchangeType.TOPIC);
      b.publish(message("a.b", "x"));
      b.subscribe("q", "c", 1);
      var d = b.poll("c").orElseThrow();
      clock.advance(150);
      await(() -> b.queueStatistics("q").unacked() == 0);
      b.subscribe("q", "new", 1);
      var second = new java.util.concurrent.atomic.AtomicReference<Delivery>();
      await(
          () -> {
            b.poll("new").ifPresent(second::set);
            return second.get() != null;
          });
      assertEquals(d.message().id(), second.get().message().id());
      assertEquals(2, second.get().attempts());
      assertTrue(second.get().redelivered());
      assertThrows(Broker.Failure.class, () -> b.ack(d.token(), "c"));
      b.ack(second.get().token(), "new");
    }
  }

  @Test
  void disconnectAndRetryLimitDeadLetters() throws Exception {
    try (var b = new Broker(config(), clock)) {
      topology(b, ExchangeType.TOPIC);
      b.declareExchange(new Exchange("dead", ExchangeType.DIRECT, true));
      b.declareQueue(definition("dlq", true, 10, false, 0, null));
      b.bind(new Binding("dead", "dlq", "failed"));
      b.declareQueue(
          definition("failing", true, 10, false, 0, new DeadLetterPolicy("dead", "failed", 2)));
      b.bind(new Binding("e", "failing", "fail"));
      b.publish(message("fail", "x"));
      b.subscribe("failing", "c", 1);
      for (int i = 1; i <= 3; i++) {
        var d = new java.util.concurrent.atomic.AtomicReference<Delivery>();
        await(
            () -> {
              b.poll("c").ifPresent(d::set);
              return d.get() != null;
            });
        assertEquals(i, d.get().attempts());
        b.nack(d.get().token(), "c", true);
      }
      assertEquals(0, b.queueStatistics("failing").messages());
      b.subscribe("dlq", "d", 1);
      var dead = b.poll("d").orElseThrow();
      assertEquals(
          "attempts-exhausted", dead.message().properties().headers().get("x-failure-reason"));
      assertEquals("3", dead.message().properties().headers().get("x-attempts"));
    }
  }

  @Test
  void priorityDelayTtlAndBinary() throws Exception {
    try (var b = new Broker(config(), clock)) {
      b.declareExchange(new Exchange("e", ExchangeType.FANOUT, true));
      b.declareQueue(definition("q", true, 100, true, 0, null));
      b.bind(new Binding("e", "q", ""));
      byte[] bytes = {(byte) 255, 0, 1};
      b.publish(
          new Publish("e", "", bytes, new MessageProperties(null, true, 0, 0, 1, Map.of()), true));
      b.publish(
          new Publish(
              "e", "", new byte[] {9}, new MessageProperties(null, true, 0, 0, 9, Map.of()), true));
      b.subscribe("q", "c", 1);
      var first = b.poll("c").orElseThrow();
      assertEquals(9, first.message().payload()[0]);
      b.ack(first.token(), "c");
      var second = b.poll("c").orElseThrow();
      assertArrayEquals(bytes, second.message().payload());
      b.ack(second.token(), "c");
      b.publish(
          new Publish(
              "e", "", bytes, new MessageProperties(null, true, 30, 200, 0, Map.of()), true));
      assertTrue(b.poll("c").isEmpty());
      await(() -> b.queueStatistics("q").messages() == 0);
    }
  }

  @Test
  void persistenceCombinationsAckAndCompaction() throws Exception {
    String persistentId;
    try (var b = new Broker(config(), clock)) {
      topology(b, ExchangeType.TOPIC);
      b.declareQueue(definition("transient", false, 100, false, 0, null));
      b.bind(new Binding("e", "transient", "a.*"));
      b.publish(message("a.b", "acked"));
      b.subscribe("q", "c", 1);
      var ack = b.poll("c").orElseThrow();
      b.ack(ack.token(), "c");
      persistentId = b.publish(message("a.b", "live")).messageId();
      b.publish(
          new Publish(
              "e",
              "a.b",
              new byte[] {1},
              new MessageProperties(null, false, 0, 0, 0, Map.of()),
              true));
      b.compact();
    }
    try (var b = new Broker(config(), clock)) {
      assertEquals(1, b.queues().size());
      assertEquals(1, b.queueStatistics("q").messages());
      b.subscribe("q", "c", 1);
      assertEquals(persistentId, b.poll("c").orElseThrow().message().id());
    }
  }

  @Test
  void scheduledMessageAndUnackedSurviveRestart() throws Exception {
    String id;
    try (var b = new Broker(config(), clock)) {
      topology(b, ExchangeType.TOPIC);
      b.publish(message("a.b", "inflight"));
      b.subscribe("q", "c", 1);
      id = b.poll("c").orElseThrow().message().id();
      b.publish(
          new Publish(
              "e",
              "a.b",
              new byte[] {2},
              new MessageProperties(null, true, 0, 300, 0, Map.of()),
              true));
    }
    try (var b = new Broker(config(), clock)) {
      b.subscribe("q", "c", 1);
      var d = b.poll("c").orElseThrow();
      assertEquals(id, d.message().id());
      assertTrue(d.redelivered());
      b.ack(d.token(), "c");
      assertTrue(b.poll("c").isEmpty());
      await(() -> b.queueStatistics("q").scheduled() == 0);
      assertTrue(b.poll("c").isPresent());
    }
  }

  @Test
  void truncatedWalAndCorruption() throws Exception {
    try (var b = new Broker(config(), clock)) {
      topology(b, ExchangeType.TOPIC);
      b.publish(message("a.b", "survive"));
    }
    Path wal = dir.resolve("broker.wal");
    long good = Files.size(wal);
    Files.write(wal, new byte[] {1, 2, 3}, StandardOpenOption.APPEND);
    try (var b = new Broker(config(), clock)) {
      assertEquals(1, b.queueStatistics("q").messages());
      assertEquals(good, Files.size(wal));
    }
    try (var ch = FileChannel.open(wal, StandardOpenOption.WRITE)) {
      ch.write(ByteBuffer.wrap(new byte[] {(byte) 255}), 20);
    }
    assertThrows(java.io.IOException.class, () -> new Broker(config(), clock));
  }

  @Test
  void incompletePayloadTruncated() throws Exception {
    try (var b = new Broker(config(), clock)) {
      topology(b, ExchangeType.TOPIC);
    }
    long good = Files.size(dir.resolve("broker.wal"));
    try (var b = new Broker(config(), clock)) {
      b.publish(message("a.b", "x"));
    }
    try (var c = FileChannel.open(dir.resolve("broker.wal"), StandardOpenOption.WRITE)) {
      c.truncate(c.size() - 5);
    }
    try (var b = new Broker(config(), clock)) {
      assertEquals(0, b.queueStatistics("q").messages());
      assertEquals(good, Files.size(dir.resolve("broker.wal")));
    }
  }

  @Test
  void atomicMultiQueueCapacityRollback() throws Exception {
    try (var b = new Broker(config(), clock)) {
      topology(b, ExchangeType.TOPIC);
      b.declareQueue(definition("full", true, 1, false, 0, null));
      b.bind(new Binding("e", "full", "a.*"));
      b.publish(message("a.b", "first"));
      var error = assertThrows(Broker.Failure.class, () -> b.publish(message("a.b", "second")));
      assertEquals(429, error.status);
      assertEquals(1, b.queueStatistics("q").messages());
    }
    try (var b = new Broker(config(), clock)) {
      assertEquals(1, b.queueStatistics("q").messages());
      assertEquals(1, b.queueStatistics("full").messages());
    }
  }

  @Test
  void concurrentPublishAndAckAndDelete() throws Exception {
    try (var b = new Broker(config(), clock);
        var pool = Executors.newFixedThreadPool(8)) {
      topology(b, ExchangeType.TOPIC);
      List<Future<?>> tasks = new ArrayList<>();
      for (int i = 0; i < 100; i++) tasks.add(pool.submit(() -> b.publish(message("a.b", "x"))));
      for (var t : tasks) t.get();
      assertEquals(100, b.queueStatistics("q").messages());
      Set<String> ids = ConcurrentHashMap.newKeySet();
      tasks.clear();
      for (int i = 0; i < 4; i++) {
        String c = "c" + i;
        b.subscribe("q", c, 10);
        tasks.add(
            pool.submit(
                () -> {
                  for (; ; ) {
                    var d = b.poll(c);
                    if (d.isEmpty()) return;
                    assertTrue(ids.add(d.get().message().id()));
                    b.ack(d.get().token(), c);
                  }
                }));
      }
      for (var t : tasks) t.get();
      assertEquals(100, ids.size());
      assertEquals(0, b.queueStatistics("q").messages());
      b.publish(message("a.b", "x"));
      var d = b.poll("c0").orElseThrow();
      b.deleteQueue("q");
      assertThrows(Broker.Failure.class, () -> b.ack(d.token(), "c0"));
    }
  }

  @Test
  void validatesInputsAndStorageLock() throws Exception {
    try (var b = new Broker(config(), clock)) {
      assertThrows(
          Broker.Failure.class,
          () -> b.declareQueue(definition("../evil", true, 10, false, 0, null)));
      assertThrows(Exception.class, () -> new Broker(config(), clock));
      topology(b, ExchangeType.TOPIC);
      assertThrows(
          Broker.Failure.class,
          () -> b.publish(new Publish("e", "a.b", new byte[1025], null, true)));
      assertThrows(Broker.Failure.class, () -> b.subscribe("q", "c", 11));
      assertThrows(IllegalArgumentException.class, () -> new RetryPolicy(0, 1, 10, false));
    }
  }

  @Test
  void confirmsNeverOverclaimDurability() throws Exception {
    try (var b = new Broker(config(), clock)) {
      topology(b, ExchangeType.TOPIC);
      assertEquals(ConfirmStatus.PERSISTED, b.publish(message("a.b", "x")).status());
      assertEquals(
          ConfirmStatus.ACCEPTED,
          b.publish(
                  new Publish(
                      "e",
                      "a.b",
                      new byte[1],
                      new MessageProperties(null, false, 0, 0, 0, Map.of()),
                      true))
              .status());
      b.declareQueue(definition("t", false, 10, false, 0, null));
      b.bind(new Binding("e", "t", "a.*"));
      assertEquals(ConfirmStatus.ACCEPTED, b.publish(message("a.b", "x")).status());
    }
  }

  @Test
  void renewKeepsDeliveryActive() throws Exception {
    try (var b = new Broker(config(), clock)) {
      topology(b, ExchangeType.TOPIC);
      b.publish(message("a.b", "x"));
      b.subscribe("q", "c", 1);
      var d = b.poll("c").orElseThrow();
      clock.advance(60);
      b.renew(d.token(), "c");
      clock.advance(60);
      b.ack(d.token(), "c");
      assertEquals(0, b.queueStatistics("q").messages());
    }
  }
}
