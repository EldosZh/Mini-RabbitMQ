package com.minirabbit;

import static com.minirabbit.core.Models.*;
import static org.junit.jupiter.api.Assertions.*;

import com.minirabbit.core.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

class CrashRecoveryTest {
  @TempDir Path dir;

  static BrokerConfiguration config(Path dir) {
    return new BrokerConfiguration(
        "127.0.0.1",
        0,
        dir.toString(),
        "always",
        128,
        8,
        32,
        10,
        32,
        1000,
        1024,
        1048576,
        10000,
        20000,
        3600000,
        true);
  }

  static Publish publish(String text) {
    return new Publish("e", "key", text.getBytes(), MessageProperties.defaults(), true);
  }

  static void setup(Broker b) {
    b.declareExchange(new Exchange("e", ExchangeType.DIRECT, true));
    b.declareQueue(QueueDefinition.defaults("q", true));
    b.bind(new Binding("e", "q", "key"));
  }

  /** Runs in a separate JVM. halt() bypasses broker.close and every shutdown hook. */
  public static class CrashingProcess {
    public static void main(String[] args) throws Exception {
      try {
        Broker b = new Broker(config(Path.of(args[0])));
        setup(b);
        b.publish(publish("acked"));
        b.subscribe("q", "c", 1);
        b.ack(b.poll("c").orElseThrow().token(), "c");
        b.publish(publish("inflight"));
        b.poll("c").orElseThrow();
        b.publish(
            new Publish(
                "e",
                "key",
                new byte[] {1},
                new MessageProperties(null, true, 0, 60000, 0, Map.of()),
                true));
        b.compact();
        Runtime.getRuntime().halt(0);
      } catch (Throwable e) {
        e.printStackTrace();
        Runtime.getRuntime().halt(1);
      }
    }
  }

  @Test
  void forcedProcessTerminationRecoversLiveStateAndAck() throws Exception {
    String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    String cp = System.getProperty("java.class.path");
    Process child =
        new ProcessBuilder(java, "-cp", cp, CrashingProcess.class.getName(), dir.toString())
            .redirectErrorStream(true)
            .redirectOutput(dir.resolve("child-output.txt").toFile())
            .start();
    try {
      assertTrue(child.waitFor(20, TimeUnit.SECONDS));
    } finally {
      if (child.isAlive()) {
        child.destroyForcibly();
        child.waitFor(5, TimeUnit.SECONDS);
      }
    }
    assertEquals(0, child.exitValue(), Files.readString(dir.resolve("child-output.txt")));
    try (var b = new Broker(config(dir))) {
      assertEquals(2, b.queueStatistics("q").messages());
      assertEquals(1, b.queueStatistics("q").scheduled());
      b.subscribe("q", "new", 1);
      var d = b.poll("new").orElseThrow();
      assertEquals("inflight", new String(d.message().payload()));
      assertTrue(d.redelivered());
      assertEquals(2, d.attempts());
      b.ack(d.token(), "new");
      assertTrue(b.poll("new").isEmpty());
    }
  }

  @Test
  void queueCapacityIsAtomicUnderConcurrentProducers() throws Exception {
    try (var b = new Broker(config(dir));
        var workers = Executors.newFixedThreadPool(8)) {
      b.declareExchange(new Exchange("e", ExchangeType.DIRECT, true));
      b.declareQueue(new QueueDefinition("q", true, 5, 1024, 0, false, null, null));
      b.bind(new Binding("e", "q", "key"));
      List<Future<Boolean>> futures = new ArrayList<>();
      for (int i = 0; i < 50; i++)
        futures.add(
            workers.submit(
                () -> {
                  try {
                    b.publish(publish("x"));
                    return true;
                  } catch (Broker.Failure e) {
                    assertEquals(429, e.status);
                    return false;
                  }
                }));
      int accepted = 0;
      for (var f : futures) if (f.get()) accepted++;
      assertEquals(5, accepted);
      assertEquals(5, b.queueStatistics("q").messages());
    }
    try (var b = new Broker(config(dir))) {
      assertEquals(5, b.queueStatistics("q").messages());
    }
  }

  @Test
  void shutdownDuringPublishPreservesEveryConfirmedMessage() throws Exception {
    Broker b = new Broker(config(dir));
    setup(b);
    var accepted = new java.util.concurrent.atomic.AtomicInteger();
    try (var workers = Executors.newFixedThreadPool(8)) {
      List<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < 100; i++)
        futures.add(
            workers.submit(
                () -> {
                  try {
                    b.publish(publish("x"));
                    accepted.incrementAndGet();
                  } catch (Broker.Failure e) {
                    assertTrue(e.status == 503 || e.status == 429);
                  }
                }));
      b.close();
      for (var f : futures) f.get();
    }
    try (var recovered = new Broker(config(dir))) {
      assertEquals(accepted.get(), recovered.queueStatistics("q").messages());
    }
  }

  @Test
  void delayedRetrySurvivesRestart() throws Exception {
    try (var b = new Broker(config(dir))) {
      b.declareExchange(new Exchange("e", ExchangeType.DIRECT, true));
      b.declareQueue(
          new QueueDefinition(
              "q", true, 10, 1024, 0, false, new RetryPolicy(3, 60000, 60000, false), null));
      b.bind(new Binding("e", "q", "key"));
      b.publish(publish("retry"));
      b.subscribe("q", "c", 1);
      b.nack(b.poll("c").orElseThrow().token(), "c", true);
    }
    try (var b = new Broker(config(dir))) {
      assertEquals(1, b.queueStatistics("q").scheduled());
      b.subscribe("q", "c", 1);
      assertTrue(b.poll("c").isEmpty());
    }
  }

  @Test
  void fullDeadLetterQueueDoesNotBlockItsOwnDrain() throws Exception {
    try (var b = new Broker(config(dir))) {
      b.declareExchange(new Exchange("e", ExchangeType.DIRECT, true));
      b.declareExchange(new Exchange("dead", ExchangeType.DIRECT, true));
      b.declareQueue(
          new QueueDefinition(
              "source",
              true,
              10,
              1024,
              20,
              false,
              null,
              new DeadLetterPolicy("dead", "failed", 2)));
      b.declareQueue(new QueueDefinition("dlq", true, 1, 1024, 0, false, null, null));
      b.bind(new Binding("e", "source", "key"));
      b.bind(new Binding("dead", "dlq", "failed"));
      b.publish(new Publish("dead", "failed", new byte[1], null, true));
      b.publish(publish("expire"));
      Thread.sleep(100);
      b.subscribe("dlq", "drain", 1);
      var d = b.poll("drain").orElseThrow();
      b.ack(d.token(), "drain");
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
      while (b.queueStatistics("source").messages() != 0) {
        if (System.nanoTime() > deadline) fail("DLQ remained blocked");
        Thread.sleep(20);
      }
      assertEquals(1, b.queueStatistics("dlq").messages());
    }
  }
}
