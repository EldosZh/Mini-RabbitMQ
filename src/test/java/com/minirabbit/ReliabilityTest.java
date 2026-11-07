package com.minirabbit;

import static com.minirabbit.core.Models.*;
import static org.junit.jupiter.api.Assertions.*;

import com.minirabbit.core.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

class ReliabilityTest {
  final ManualClock clock = new ManualClock();
  @TempDir Path dir;

  BrokerConfiguration config() {
    return new BrokerConfiguration(
        "127.0.0.1",
        0,
        dir.toString(),
        "always",
        128,
        8,
        4,
        10,
        10,
        100,
        1024,
        1048576,
        500,
        1000,
        3600000,
        true);
  }

  static QueueDefinition q(String name, long ttl, DeadLetterPolicy dl) {
    return new QueueDefinition(
        name, true, 100, 1024, ttl, false, new RetryPolicy(2, 10, 100, false), dl);
  }

  static void setup(Broker b, QueueDefinition q) {
    b.declareExchange(new Exchange("e", ExchangeType.DIRECT, true));
    b.declareQueue(q);
    b.bind(new Binding("e", q.name(), "key"));
  }

  static void publish(Broker b) {
    b.publish(new Publish("e", "key", new byte[] {1}, null, true));
  }

  @Test
  void rejectWithoutRequeueDropsWhenNoDeadLetterRoute() throws Exception {
    try (var b = new Broker(config(), clock)) {
      setup(b, q("q", 0, null));
      publish(b);
      b.subscribe("q", "c", 1);
      b.nack(b.poll("c").orElseThrow().token(), "c", false);
      assertEquals(0, b.queueStatistics("q").messages());
    }
    try (var b = new Broker(config(), clock)) {
      assertEquals(0, b.queueStatistics("q").messages());
    }
  }

  @Test
  void queueTtlExpiresReadyButDoesNotInvalidateActiveAck() throws Exception {
    try (var b = new Broker(config(), clock)) {
      setup(b, q("q", 100, null));
      publish(b);
      b.subscribe("q", "c", 1);
      var d = b.poll("c").orElseThrow();
      clock.advance(150);
      b.ack(d.token(), "c");
      publish(b);
      clock.advance(150);
      assertTrue(b.poll("c").isEmpty());
      assertEquals(0, b.queueStatistics("q").messages());
    }
  }

  @Test
  void delayedMessageCannotAppearEarly() throws Exception {
    try (var b = new Broker(config(), clock)) {
      setup(b, q("q", 0, null));
      long start = clock.millis();
      b.publish(
          new Publish(
              "e",
              "key",
              new byte[] {1},
              new MessageProperties(null, true, 0, 300, 0, Map.of()),
              true));
      b.subscribe("q", "c", 1);
      assertTrue(b.poll("c").isEmpty());
      clock.advance(350);
      var d = b.poll("c").orElseThrow();
      assertTrue(clock.millis() >= start + 300);
      b.ack(d.token(), "c");
    }
  }

  @Test
  void cancellationRedeliversWithFreshToken() throws Exception {
    try (var b = new Broker(config(), clock)) {
      setup(b, q("q", 0, null));
      publish(b);
      b.subscribe("q", "c", 1);
      var d = b.poll("c").orElseThrow();
      b.disconnect("c");
      clock.advance(30);
      b.subscribe("q", "new", 1);
      var next = b.poll("new").orElseThrow();
      assertEquals(d.message().id(), next.message().id());
      assertNotEquals(d.token(), next.token());
      assertEquals(2, next.attempts());
      assertThrows(Broker.Failure.class, () -> b.ack(d.token(), "c"));
      b.ack(next.token(), "new");
    }
  }

  @Test
  void competingAckAndNackOnlyOneCanWin() throws Exception {
    try (var b = new Broker(config(), clock);
        var workers = Executors.newFixedThreadPool(2)) {
      setup(b, q("q", 0, null));
      publish(b);
      b.subscribe("q", "c", 1);
      var d = b.poll("c").orElseThrow();
      var gate = new CountDownLatch(1);
      var wins = new java.util.concurrent.atomic.AtomicInteger();
      var losses = new java.util.concurrent.atomic.AtomicInteger();
      List<Future<?>> fs = new ArrayList<>();
      for (boolean ack : List.of(true, false))
        fs.add(
            workers.submit(
                () -> {
                  try {
                    gate.await();
                    if (ack) b.ack(d.token(), "c");
                    else b.nack(d.token(), "c", false);
                    wins.incrementAndGet();
                  } catch (Broker.Failure e) {
                    assertEquals(409, e.status);
                    losses.incrementAndGet();
                  } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                  }
                }));
      gate.countDown();
      for (var f : fs) f.get();
      assertEquals(1, wins.get());
      assertEquals(1, losses.get());
      assertEquals(0, b.queueStatistics("q").messages());
    }
  }

  @Test
  void deadLetterCycleBoundAndFullHeaderMap() throws Exception {
    try (var b = new Broker(config(), clock)) {
      b.declareExchange(new Exchange("e", ExchangeType.DIRECT, true));
      b.declareQueue(q("a", 0, new DeadLetterPolicy("e", "b", 2)));
      b.declareQueue(q("b", 0, new DeadLetterPolicy("e", "a", 2)));
      b.bind(new Binding("e", "a", "a"));
      b.bind(new Binding("e", "b", "b"));
      Map<String, String> headers = new HashMap<>();
      for (int i = 0; i < 32; i++) headers.put("h" + i, "v");
      b.publish(
          new Publish(
              "e", "a", new byte[0], new MessageProperties(null, true, 0, 0, 0, headers), true));
      b.subscribe("a", "ca", 1);
      b.subscribe("b", "cb", 1);
      for (int i = 0; i < 3; i++) {
        String id = i % 2 == 0 ? "ca" : "cb";
        var d = b.poll(id).orElseThrow();
        b.nack(d.token(), id, false);
      }
      assertEquals(0, b.queueStatistics("a").messages());
      assertEquals(0, b.queueStatistics("b").messages());
      assertEquals(2, ((Number) b.stats().get("deadLettered")).intValue());
    }
  }

  @Test
  void prefetchUpdateAndConsumerLimit() throws Exception {
    try (var b = new Broker(config(), clock)) {
      setup(b, q("q", 0, null));
      publish(b);
      publish(b);
      b.subscribe("q", "c", 2);
      var one = b.poll("c").orElseThrow();
      var two = b.poll("c").orElseThrow();
      assertThrows(Broker.Failure.class, () -> b.setPrefetch("c", 1));
      b.ack(one.token(), "c");
      b.setPrefetch("c", 1);
      assertTrue(b.poll("c").isEmpty());
      b.ack(two.token(), "c");
      for (int i = 0; i < 3; i++) b.subscribe("q", "c" + i, 1);
      assertThrows(Broker.Failure.class, () -> b.subscribe("q", "excess", 1));
    }
  }

  @Test
  void retryBackoffAndJitterAreBounded() {
    var fixed = new RetryPolicy(10, 20, 20, false);
    var exp = new RetryPolicy(10, 10, 100, false);
    assertEquals(20, fixed.delay(9));
    assertEquals(10, exp.delay(1));
    assertEquals(20, exp.delay(2));
    assertEquals(100, exp.delay(100));
    for (int i = 0; i < 100; i++) {
      long delay = new RetryPolicy(10, 10, 100, true).delay(10);
      assertTrue(delay >= 50 && delay <= 100);
    }
  }

  @Test
  void maliciousHeadersAndOversizedTtlRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new MessageProperties(null, true, 0, 0, 0, Map.of("bad\nkey", "value")));
    assertThrows(IllegalArgumentException.class, () -> q("q", Long.MAX_VALUE, null));
  }
}
