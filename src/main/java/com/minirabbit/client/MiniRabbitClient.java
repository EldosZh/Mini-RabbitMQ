package com.minirabbit.client;

import static com.minirabbit.core.Models.*;

import com.minirabbit.core.*;
import java.net.*;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public final class MiniRabbitClient implements AutoCloseable {
  private final String base, token;
  private final HttpClient http;
  private final Set<Worker> workers = ConcurrentHashMap.newKeySet();
  private volatile boolean closed;

  public MiniRabbitClient(String base, String token) {
    this.base = base.replaceAll("/+$", "");
    this.token = Objects.requireNonNull(token, "Token required");
    URI uri = URI.create(base);
    if (!Set.of("http", "https").contains(uri.getScheme()) || uri.getHost() == null)
      throw new IllegalArgumentException("Invalid broker URL");
    http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
  }

  public static MiniRabbitClient connect(String base) {
    return new MiniRabbitClient(base, System.getenv("MINIRABBIT_TOKEN"));
  }

  public String request(String method, String path, Object body) {
    if (closed) throw new IllegalStateException("Client closed");
    int attempts = method.equals("GET") ? 3 : 1;
    for (int i = 0; ; i++)
      try {
        var builder =
            HttpRequest.newBuilder(URI.create(base + path))
                .timeout(Duration.ofSeconds(15))
                .header("Authorization", "Bearer " + token)
                .header("Content-Type", "application/json");
        builder.method(
            method,
            body == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofByteArray(Json.bytes(body)));
        var response = http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 400)
          throw new Broker.Failure(response.statusCode(), response.body());
        return response.body();
      } catch (java.io.IOException e) {
        if (i + 1 >= attempts) throw new IllegalStateException("Broker request failed", e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted", e);
      }
  }

  public Exchange declareExchange(String name, ExchangeType type, boolean durable) {
    return decode(
        request("POST", "/api/v1/exchanges", new Exchange(name, type, durable)), Exchange.class);
  }

  public QueueDefinition declareQueue(String name, boolean durable) {
    return declareQueue(QueueDefinition.defaults(name, durable));
  }

  public QueueDefinition declareQueue(QueueDefinition q) {
    return decode(request("POST", "/api/v1/queues", q), QueueDefinition.class);
  }

  public void bind(String exchange, String queue, String key) {
    request("POST", "/api/v1/bindings", new Binding(exchange, queue, key));
  }

  public void setPrefetch(String consumerId, int prefetch) {
    request("PATCH", "/api/v1/consumers/" + Broker.name(consumerId), new PrefetchRequest(prefetch));
  }

  public PublisherConfirmation publish(String exchange, String key, String body) {
    return publish(
        new Publish(
            exchange,
            key,
            body.getBytes(StandardCharsets.UTF_8),
            MessageProperties.defaults(),
            true));
  }

  public PublisherConfirmation publish(Publish p) {
    return decode(request("POST", "/api/v1/messages/publish", p), PublisherConfirmation.class);
  }

  public PublisherConfirmation publishDelayed(
      String exchange, String key, byte[] payload, Duration delay) {
    return publish(
        new Publish(
            exchange,
            key,
            payload,
            new MessageProperties(
                "application/octet-stream", true, 0, delay.toMillis(), 0, Map.of()),
            true));
  }

  public Optional<ClientDelivery> poll(String queue, String id, int prefetch, long waitMs) {
    String json =
        request(
            "POST",
            "/api/v1/queues/" + Broker.name(queue) + "/consume",
            new ConsumeRequest(id, prefetch, waitMs));
    return json.isBlank()
        ? Optional.empty()
        : Optional.of(new ClientDelivery(decode(json, Delivery.class)));
  }

  public final class ClientDelivery {
    private final Delivery delivery;

    ClientDelivery(Delivery d) {
      delivery = d;
    }

    public Delivery value() {
      return delivery;
    }

    public byte[] body() {
      return delivery.message().payload();
    }

    public String bodyAsString() {
      return new String(body(), StandardCharsets.UTF_8);
    }

    private void act(String action, boolean requeue) {
      request(
          "POST",
          "/api/v1/deliveries/" + delivery.token() + "/" + action,
          new Acknowledgement(delivery.consumerId(), requeue));
    }

    public void ack() {
      act("ack", false);
    }

    public void nack(boolean requeue) {
      act("nack", requeue);
    }

    public void reject(boolean requeue) {
      act("reject", requeue);
    }

    public void renew() {
      act("renew", false);
    }
  }

  public Worker consume(String queue, java.util.function.Consumer<ClientDelivery> handler) {
    return consume(queue, 1, 1, handler);
  }

  public Worker consume(
      String queue,
      int prefetch,
      int concurrency,
      java.util.function.Consumer<ClientDelivery> handler) {
    if (concurrency < 1 || concurrency > 64)
      throw new IllegalArgumentException("concurrency must be 1..64");
    Worker w = new Worker(queue, prefetch, concurrency, handler);
    workers.add(w);
    return w;
  }

  public final class Worker implements AutoCloseable {
    private final ExecutorService executor;
    private final List<String> ids = new ArrayList<>();
    private volatile boolean stopped;

    private Worker(
        String queue,
        int prefetch,
        int concurrency,
        java.util.function.Consumer<ClientDelivery> handler) {
      executor = Executors.newFixedThreadPool(concurrency, Thread.ofVirtual().factory());
      for (int i = 0; i < concurrency; i++) {
        String id = "sdk-" + UUID.randomUUID();
        ids.add(id);
        executor.submit(
            () -> {
              while (!stopped && !closed) {
                try {
                  var d = poll(queue, id, prefetch, 1000);
                  if (d.isPresent()) {
                    try {
                      handler.accept(d.get());
                    } catch (Exception e) {
                      try {
                        d.get().nack(true);
                      } catch (Exception ignored) {
                      }
                    }
                  }
                } catch (Exception e) {
                  if (stopped || closed) break;
                  try {
                    Thread.sleep(250);
                  } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    break;
                  }
                }
              }
            });
      }
    }

    @Override
    public void close() {
      if (stopped) return;
      stopped = true;
      executor.shutdownNow();
      try {
        executor.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      for (var id : ids)
        try {
          request("DELETE", "/api/v1/consumers/" + id, null);
        } catch (Exception ignored) {
        }
      workers.remove(this);
    }
  }

  private static <T> T decode(String text, Class<T> type) {
    return Json.read(text.getBytes(StandardCharsets.UTF_8), type);
  }

  @Override
  public void close() {
    if (closed) return;
    for (var w : List.copyOf(workers)) w.close();
    closed = true;
    http.close();
  }
}
