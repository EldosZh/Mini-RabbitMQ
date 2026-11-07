package com.minirabbit.bench;

import static com.minirabbit.core.Models.*;

import com.minirabbit.core.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

/** End-to-end microbenchmark; output is measured, never a throughput claim for production. */
public final class Benchmark {
  private static void report(
      String name,
      long[] times,
      long elapsed,
      int bytes,
      int producers,
      int consumers,
      String fsync,
      int errors) {
    Arrays.sort(times);
    int n = times.length;
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("scenario", name);
    m.put("messages", n);
    m.put("messagesPerSecond", n * 1e9 / elapsed);
    m.put("mibPerSecond", n * (double) bytes * 1e9 / elapsed / 1048576);
    m.put("p50Micros", times[(n - 1) / 2] / 1000.0);
    m.put("p95Micros", times[(int) ((n - 1) * .95)] / 1000.0);
    m.put("p99Micros", times[(int) ((n - 1) * .99)] / 1000.0);
    m.put("errorRate", errors / (double) n);
    m.put("messageBytes", bytes);
    m.put("producers", producers);
    m.put("consumers", consumers);
    m.put("fsyncPolicy", fsync);
    System.out.println(new String(Json.bytes(m), java.nio.charset.StandardCharsets.UTF_8));
  }

  public static void main(String[] args) throws Exception {
    int n = args.length > 0 ? Integer.parseInt(args[0]) : 200;
    if (n < 10 || n > 5000) throw new IllegalArgumentException("Count must be 10..5000");
    System.out.println(
        "# Java "
            + System.getProperty("java.version")
            + "; "
            + System.getProperty("os.name")
            + " "
            + System.getProperty("os.arch")
            + "; CPUs "
            + Runtime.getRuntime().availableProcessors()
            + "; maxHeap "
            + Runtime.getRuntime().maxMemory());
    for (String fsync : List.of("none", "always"))
      for (boolean persistent : List.of(false, true)) {
        Path dir = Files.createTempDirectory("minirabbit-bench-");
        var d = BrokerConfiguration.defaults();
        var config =
            new BrokerConfiguration(
                "127.0.0.1",
                0,
                dir.toString(),
                fsync,
                128,
                32,
                256,
                256,
                2048,
                10000,
                1048576,
                16777216,
                30000,
                60000,
                3600000,
                true);
        try (Broker b = new Broker(config)) {
          b.declareExchange(new Exchange("e", ExchangeType.TOPIC, true));
          b.declareQueue(QueueDefinition.defaults("q", true));
          b.bind(new Binding("e", "q", "orders.#"));
          byte[] body = new byte[256];
          var props =
              new MessageProperties("application/octet-stream", persistent, 0, 0, 0, Map.of());
          long[] latency = new long[n];
          long start = System.nanoTime();
          for (int i = 0; i < n; i++) {
            long t = System.nanoTime();
            b.publish(new Publish("e", "orders.created", body, props, true));
            latency[i] = System.nanoTime() - t;
          }
          report(
              persistent ? "durable-publish" : "memory-publish",
              latency,
              System.nanoTime() - start,
              256,
              1,
              0,
              fsync,
              0);
          b.subscribe("q", "c", 256);
          List<Delivery> pending = new ArrayList<>();
          start = System.nanoTime();
          for (int i = 0; i < n; i++) {
            long t = System.nanoTime();
            var delivery = b.poll("c").orElseThrow();
            latency[i] = System.nanoTime() - t;
            pending.add(delivery);
            if (pending.size() == 200 || i == n - 1) {
              for (var item : pending) b.ack(item.token(), "c");
              pending.clear();
            }
          }
          report(
              (persistent ? "persistent-" : "transient-") + "consume-including-ack",
              latency,
              System.nanoTime() - start,
              256,
              1,
              1,
              fsync,
              0);
          for (int i = 0; i < n; i++)
            b.publish(new Publish("e", "orders.created", body, props, true));
          for (int i = 0; i < n; i++) {
            var delivery = b.poll("c").orElseThrow();
            long t = System.nanoTime();
            b.ack(delivery.token(), "c");
            latency[i] = System.nanoTime() - t;
          }
          report(
              (persistent ? "persistent-" : "transient-") + "ack-only",
              latency,
              Arrays.stream(latency).sum(),
              256,
              1,
              1,
              fsync,
              0);
          for (int i = 0; i < n / 10; i++)
            b.publish(new Publish("e", "orders.created", body, props, true));
          b.compact();
        }
        long t = System.nanoTime();
        try (Broker recovered = new Broker(config)) {
          int expected = persistent ? n / 10 : 0;
          if (recovered.queueStatistics("q").messages() != expected)
            throw new AssertionError("Recovery count");
          System.out.println(
              "{\"scenario\":\"recovery\",\"persistent\":"
                  + persistent
                  + ",\"liveMessages\":"
                  + expected
                  + ",\"fsyncPolicy\":\""
                  + fsync
                  + "\",\"elapsedMicros\":"
                  + (System.nanoTime() - t) / 1000.0
                  + "}");
        }
      }
    long[] routing = new long[n * 100];
    long start = System.nanoTime();
    for (int i = 0; i < routing.length; i++) {
      long t = System.nanoTime();
      if (!Routing.topic("orders.#.*", "orders.eu.created")) throw new AssertionError();
      routing[i] = System.nanoTime() - t;
    }
    report("topic-routing", routing, System.nanoTime() - start, 0, 1, 0, "n/a", 0);
    for (String fsync : List.of("none", "always")) {
      Path dir = Files.createTempDirectory("minirabbit-wal-bench-");
      try (Wal wal = new Wal(dir, fsync.equals("always"))) {
        byte[] bytes = Json.bytes(new State());
        long[] times = new long[n];
        start = System.nanoTime();
        for (int i = 0; i < n; i++) {
          long t = System.nanoTime();
          wal.append(1, bytes);
          times[i] = System.nanoTime() - t;
        }
        report(
            "wal-empty-state-append",
            times,
            System.nanoTime() - start,
            bytes.length,
            1,
            0,
            fsync,
            0);
      }
    }
    for (int workers : List.of(1, 4)) scaling(n, workers);
  }

  private static void scaling(int n, int workers) throws Exception {
    Path dir = Files.createTempDirectory("minirabbit-scaling-");
    var config =
        new BrokerConfiguration(
            "127.0.0.1",
            0,
            dir.toString(),
            "none",
            128,
            32,
            256,
            256,
            2048,
            10000,
            1048576,
            16777216,
            30000,
            60000,
            3600000,
            true);
    try (Broker b = new Broker(config);
        var executor = Executors.newFixedThreadPool(workers)) {
      b.declareExchange(new Exchange("e", ExchangeType.FANOUT, false));
      b.declareQueue(QueueDefinition.defaults("q", false));
      b.bind(new Binding("e", "q", ""));
      byte[] body = new byte[256];
      var p =
          new Publish("e", "", body, new MessageProperties(null, false, 0, 0, 0, Map.of()), true);
      long[] latency = new long[n];
      var index = new java.util.concurrent.atomic.AtomicInteger();
      List<Future<?>> tasks = new ArrayList<>();
      long start = System.nanoTime();
      for (int w = 0; w < workers; w++)
        tasks.add(
            executor.submit(
                () -> {
                  for (int i; (i = index.getAndIncrement()) < n; ) {
                    long t = System.nanoTime();
                    b.publish(p);
                    latency[i] = System.nanoTime() - t;
                  }
                }));
      for (var task : tasks) task.get();
      report(
          "concurrent-producer-scaling",
          latency,
          System.nanoTime() - start,
          256,
          workers,
          0,
          "none",
          0);
      tasks.clear();
      index.set(0);
      for (int w = 0; w < workers; w++) b.subscribe("q", "c" + w, 1);
      start = System.nanoTime();
      for (int w = 0; w < workers; w++) {
        String id = "c" + w;
        tasks.add(
            executor.submit(
                () -> {
                  for (; ; ) {
                    long t = System.nanoTime();
                    var d = b.poll(id);
                    if (d.isEmpty()) return;
                    b.ack(d.get().token(), id);
                    latency[index.getAndIncrement()] = System.nanoTime() - t;
                  }
                }));
      }
      for (var task : tasks) task.get();
      if (index.get() != n) throw new AssertionError("Consume count");
      report(
          "competing-consumer-scaling",
          latency,
          System.nanoTime() - start,
          256,
          0,
          workers,
          "none",
          0);
    }
  }
}
