package com.minirabbit.cli;

import static com.minirabbit.core.Models.*;

import com.minirabbit.client.MiniRabbitClient;
import com.minirabbit.core.*;
import com.minirabbit.server.BrokerServer;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import picocli.CommandLine;
import picocli.CommandLine.*;

@Command(
    name = "minirabbit",
    mixinStandardHelpOptions = true,
    version = "Mini RabbitMQ 1.0.0",
    subcommands = {
      Main.Server.class,
      Main.ExchangeCommands.class,
      Main.QueueCommands.class,
      Main.Bind.class,
      Main.PublishCommand.class,
      Main.Consume.class,
      Main.Stats.class,
      Main.Dlq.class,
      Main.Version.class
    })
public final class Main implements Runnable {
  @Option(
      names = "--url",
      defaultValue = "${env:MINIRABBIT_URL:-http://localhost:8080}",
      scope = ScopeType.INHERIT)
  String url;

  public static void main(String[] args) {
    System.exit(new CommandLine(new Main()).execute(args));
  }

  public void run() {
    CommandLine.usage(this, System.out);
  }

  static MiniRabbitClient client(Main root) {
    return MiniRabbitClient.connect(root.url);
  }

  static void print(Object value) {
    System.out.println(new String(Json.bytes(value), java.nio.charset.StandardCharsets.UTF_8));
  }

  @Command(name = "server", subcommands = Start.class)
  static class Server {}

  @Command(name = "start", mixinStandardHelpOptions = true)
  static class Start implements Runnable {
    @Option(names = "--config", defaultValue = "configs/broker.yaml")
    Path config;

    public void run() {
      try {
        Broker broker = new Broker(BrokerConfiguration.load(config));
        BrokerServer server;
        try {
          server = new BrokerServer(broker, System.getenv("MINIRABBIT_TOKEN"));
        } catch (Exception e) {
          broker.close();
          throw e;
        }
        Runtime.getRuntime()
            .addShutdownHook(
                new Thread(
                    () -> {
                      server.close();
                      try {
                        broker.close();
                      } catch (Exception e) {
                        System.err.println("Storage shutdown failed");
                      }
                    }));
        server.start();
        new CountDownLatch(1).await();
      } catch (Exception e) {
        throw new IllegalStateException(e.getMessage(), e);
      }
    }
  }

  @Command(
      name = "exchange",
      mixinStandardHelpOptions = true,
      subcommands = {ExchangeCreate.class, ExchangeList.class})
  static class ExchangeCommands {
    @ParentCommand Main root;
  }

  @Command(name = "create")
  static class ExchangeCreate implements Runnable {
    @ParentCommand ExchangeCommands parent;

    @Parameters(index = "0")
    String name;

    @Option(names = "--type", defaultValue = "DIRECT")
    String type;

    @Option(names = "--transient")
    boolean transientExchange;

    public void run() {
      try (var c = client(parent.root)) {
        print(
            c.declareExchange(name, ExchangeType.valueOf(type.toUpperCase()), !transientExchange));
      }
    }
  }

  @Command(name = "list")
  static class ExchangeList implements Runnable {
    @ParentCommand ExchangeCommands parent;

    public void run() {
      try (var c = client(parent.root)) {
        System.out.println(c.request("GET", "/api/v1/exchanges", null));
      }
    }
  }

  @Command(
      name = "queue",
      subcommands = {QueueCreate.class, QueueList.class, QueueStats.class})
  static class QueueCommands {
    @ParentCommand Main root;
  }

  @Command(name = "create")
  static class QueueCreate implements Runnable {
    @ParentCommand QueueCommands parent;

    @Parameters(index = "0")
    String name;

    @Option(names = "--durable")
    boolean durable;

    public void run() {
      try (var c = client(parent.root)) {
        print(c.declareQueue(name, durable));
      }
    }
  }

  @Command(name = "list")
  static class QueueList implements Runnable {
    @ParentCommand QueueCommands parent;

    public void run() {
      try (var c = client(parent.root)) {
        System.out.println(c.request("GET", "/api/v1/queues", null));
      }
    }
  }

  @Command(name = "stats")
  static class QueueStats implements Runnable {
    @ParentCommand QueueCommands parent;

    @Parameters(index = "0")
    String name;

    public void run() {
      try (var c = client(parent.root)) {
        System.out.println(c.request("GET", "/api/v1/queues/" + Broker.name(name), null));
      }
    }
  }

  @Command(name = "bind")
  static class Bind implements Runnable {
    @ParentCommand Main root;

    @Parameters(index = "0")
    String exchange;

    @Parameters(index = "1")
    String queue;

    @Parameters(index = "2")
    String key;

    public void run() {
      try (var c = client(root)) {
        c.bind(exchange, queue, key);
        System.out.println("Bound");
      }
    }
  }

  @Command(name = "publish")
  static class PublishCommand implements Runnable {
    @ParentCommand Main root;

    @Parameters(index = "0")
    String exchange;

    @Parameters(index = "1")
    String key;

    @Parameters(index = "2")
    String body;

    public void run() {
      try (var c = client(root)) {
        print(c.publish(exchange, key, body));
      }
    }
  }

  @Command(name = "consume")
  static class Consume implements Runnable {
    @ParentCommand Main root;

    @Parameters(index = "0")
    String queue;

    public void run() {
      try (var c = client(root)) {
        String id = "cli-" + java.util.UUID.randomUUID();
        Runtime.getRuntime()
            .addShutdownHook(
                new Thread(
                    () -> {
                      try {
                        c.request("DELETE", "/api/v1/consumers/" + id, null);
                      } catch (Exception ignored) {
                      }
                    }));
        while (!Thread.currentThread().isInterrupted()) {
          var d = c.poll(queue, id, 1, 1000);
          if (d.isPresent()) {
            System.out.println(d.get().bodyAsString());
            d.get().ack();
          }
        }
      }
    }
  }

  @Command(name = "stats")
  static class Stats implements Runnable {
    @ParentCommand Main root;

    public void run() {
      try (var c = client(root)) {
        System.out.println(c.request("GET", "/api/v1/stats", null));
      }
    }
  }

  @Command(name = "dlq", subcommands = DlqList.class)
  static class Dlq {
    @ParentCommand Main root;
  }

  @Command(name = "list")
  static class DlqList implements Runnable {
    @ParentCommand Dlq parent;

    public void run() {
      try (var c = client(parent.root)) {
        var queues = Json.MAPPER.readTree(c.request("GET", "/api/v1/queues", null));
        var bindings = Json.MAPPER.readTree(c.request("GET", "/api/v1/bindings", null));
        java.util.Set<String> destinations = new java.util.HashSet<>();
        for (var q : queues) {
          var dl = q.path("definition").path("deadLetter");
          if (!dl.isNull() && !dl.isMissingNode())
            for (var b : bindings)
              if (b.path("exchange").asText().equals(dl.path("exchange").asText()))
                destinations.add(b.path("queue").asText());
        }
        for (var q : queues)
          if (destinations.contains(q.path("definition").path("name").asText()))
            System.out.println(q);
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
  }

  @Command(name = "version")
  static class Version implements Runnable {
    public void run() {
      System.out.println("Mini RabbitMQ 1.0.0");
    }
  }
}
