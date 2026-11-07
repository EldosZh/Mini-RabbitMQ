package com.minirabbit.server;

import static com.minirabbit.core.Models.*;

import com.minirabbit.core.*;
import com.sun.net.httpserver.*;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;

/** HTTP/JSON transport. All /api and /metrics requests require a bearer token. */
public final class BrokerServer implements AutoCloseable {
  private final Broker broker;
  private final HttpServer server;
  private final ExecutorService network;
  private final Semaphore slots;
  private final byte[] token;

  public BrokerServer(Broker broker, String token) throws IOException {
    if (token == null || token.length() < 16)
      throw new IllegalArgumentException("MINIRABBIT_TOKEN must contain at least 16 characters");
    this.broker = broker;
    this.token = token.getBytes(StandardCharsets.UTF_8);
    System.setProperty("sun.net.httpserver.maxReqTime", "15");
    System.setProperty("sun.net.httpserver.maxRspTime", "20");
    System.setProperty("sun.net.httpserver.idleInterval", "15");
    System.setProperty("sun.net.httpserver.maxConnections", "256");
    System.setProperty("sun.net.httpserver.maxReqHeaders", "64");
    var c = broker.configuration();
    server = HttpServer.create(new InetSocketAddress(c.host(), c.port()), c.maxNetworkRequests());
    slots = new Semaphore(c.maxNetworkRequests(), true);
    network =
        new ThreadPoolExecutor(
            c.maxNetworkRequests() + 1,
            c.maxNetworkRequests() + 1,
            0,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(c.maxNetworkRequests()),
            Thread.ofVirtual().name("http-", 0).factory(),
            new ThreadPoolExecutor.AbortPolicy());
    server.setExecutor(network);
    server.createContext("/", this::handle);
  }

  public void start() {
    server.start();
    org.slf4j.LoggerFactory.getLogger(BrokerServer.class)
        .info("Broker listening at {}", server.getAddress());
  }

  public int port() {
    return server.getAddress().getPort();
  }

  private void handle(HttpExchange x) {
    boolean acquired = slots.tryAcquire();
    try {
      if (!acquired) throw new Broker.Failure(429, "Concurrent request limit reached");
      String path = x.getRequestURI().getPath(), method = x.getRequestMethod();
      if ((path.equals("/health") || path.equals("/ready")) && method.equals("GET")) {
        boolean ready = path.equals("/health") || Boolean.TRUE.equals(broker.stats().get("ready"));
        send(x, ready ? 200 : 503, Map.of("status", ready ? "ok" : "unavailable"));
        return;
      }
      String header = x.getRequestHeaders().getFirst("Authorization");
      byte[] actual =
          (header != null && header.startsWith("Bearer ") ? header.substring(7) : "")
              .getBytes(StandardCharsets.UTF_8);
      if (!MessageDigest.isEqual(token, actual)) {
        send(
            x,
            401,
            Map.of("error", Map.of("code", 401, "message", "Bearer authentication required")));
        return;
      }
      if (path.equals("/metrics") && method.equals("GET")) {
        if (!broker.configuration().metricsEnabled())
          throw new Broker.Failure(404, "Metrics disabled");
        sendText(x, 200, broker.metrics(), "text/plain; version=0.0.4; charset=utf-8");
        return;
      }
      if (!path.startsWith("/api/v1/")) throw new Broker.Failure(404, "Endpoint not found");
      String[] p = path.substring(8).split("/");
      Object response = null;
      int status = 200;
      if (p.length == 1)
        switch (p[0]) {
          case "exchanges" -> {
            if (method.equals("GET")) response = broker.exchanges();
            else if (method.equals("POST"))
              response = broker.declareExchange(body(x, Exchange.class));
            else throw method();
          }
          case "queues" -> {
            if (method.equals("GET")) response = broker.queues();
            else if (method.equals("POST"))
              response = broker.declareQueue(body(x, QueueDefinition.class));
            else throw method();
          }
          case "bindings" -> {
            if (method.equals("GET")) response = broker.bindings();
            else if (method.equals("POST")) response = broker.bind(body(x, Binding.class));
            else throw method();
          }
          case "consumers" -> {
            if (method.equals("GET")) response = broker.consumers();
            else throw method();
          }
          case "stats" -> {
            if (method.equals("GET")) response = broker.stats();
            else throw method();
          }
          default -> throw new Broker.Failure(404, "Endpoint not found");
        }
      else if (p.length == 2 && p[0].equals("exchanges") && method.equals("DELETE")) {
        broker.deleteExchange(Broker.name(p[1]));
        response = Map.of("deleted", true);
      } else if (p.length == 2 && p[0].equals("queues")) {
        Broker.name(p[1]);
        if (method.equals("GET")) response = broker.queueStatistics(p[1]);
        else if (method.equals("DELETE")) {
          broker.deleteQueue(p[1]);
          response = Map.of("deleted", true);
        } else throw method();
      } else if (p.length == 2
          && p[0].equals("messages")
          && p[1].equals("publish")
          && method.equals("POST")) response = broker.publish(body(x, Publish.class));
      else if (p.length == 3
          && p[0].equals("queues")
          && p[2].equals("consume")
          && method.equals("POST")) {
        ConsumeRequest request = body(x, ConsumeRequest.class);
        if (request.waitMs() < 0 || request.waitMs() > 10000)
          throw new Broker.Failure(400, "waitMs must be 0..10000");
        broker.subscribe(Broker.name(p[1]), request.consumerId(), request.prefetch());
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(request.waitMs());
        Optional<Delivery> delivery;
        do {
          delivery = broker.poll(request.consumerId());
          if (delivery.isPresent() || System.nanoTime() >= deadline) break;
          Thread.sleep(25);
        } while (true);
        if (delivery.isEmpty()) {
          status = 204;
        } else response = delivery.get();
      } else if (p.length == 3 && p[0].equals("deliveries") && method.equals("POST")) {
        var request = body(x, Acknowledgement.class);
        if (request.consumerId() == null) throw new Broker.Failure(400, "consumerId required");
        switch (p[2]) {
          case "ack" -> broker.ack(p[1], request.consumerId());
          case "nack", "reject" -> broker.nack(p[1], request.consumerId(), request.requeue());
          case "renew" -> broker.renew(p[1], request.consumerId());
          default -> throw new Broker.Failure(404, "Endpoint not found");
        }
        response = Map.of("ok", true);
      } else if (p.length == 2 && p[0].equals("consumers") && method.equals("DELETE")) {
        broker.disconnect(Broker.name(p[1]));
        response = Map.of("disconnected", true);
      } else if (p.length == 2 && p[0].equals("consumers") && method.equals("PATCH")) {
        broker.setPrefetch(Broker.name(p[1]), body(x, PrefetchRequest.class).prefetch());
        response = Map.of("updated", true);
      } else if (p.length == 2
          && p[0].equals("storage")
          && p[1].equals("compact")
          && method.equals("POST")) {
        broker.compact();
        response = Map.of("compacted", true);
      } else throw new Broker.Failure(404, "Endpoint not found");
      send(x, status, response);
    } catch (Broker.Failure e) {
      error(x, e.status, e.getMessage());
    } catch (IllegalArgumentException e) {
      error(x, 400, e.getMessage());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      error(x, 503, "Request interrupted");
    } catch (Exception e) {
      org.slf4j.LoggerFactory.getLogger(BrokerServer.class).error("Request failed", e);
      error(x, 500, "Internal server error");
    } finally {
      if (acquired) slots.release();
      x.close();
    }
  }

  private Broker.Failure method() {
    return new Broker.Failure(405, "Method not allowed");
  }

  private <T> T body(HttpExchange x, Class<T> type) throws IOException {
    String contentType = x.getRequestHeaders().getFirst("Content-Type");
    if (contentType == null || !contentType.toLowerCase(Locale.ROOT).startsWith("application/json"))
      throw new Broker.Failure(415, "application/json required");
    int limit = broker.configuration().maxMessageBytes() * 4 / 3 + 65536;
    byte[] bytes = x.getRequestBody().readNBytes(limit + 1);
    if (bytes.length > limit) throw new Broker.Failure(413, "Request body exceeds limit");
    return Json.read(bytes, type);
  }

  private void error(HttpExchange x, int status, String message) {
    try {
      send(
          x,
          status,
          Map.of(
              "error",
              Map.of("code", status, "message", message == null ? "Invalid request" : message)));
    } catch (IOException ignored) {
    }
  }

  private void send(HttpExchange x, int status, Object value) throws IOException {
    if (status == 204) {
      x.sendResponseHeaders(204, -1);
      return;
    }
    sendBytes(x, status, Json.bytes(value), "application/json; charset=utf-8");
  }

  private void sendText(HttpExchange x, int status, String value, String type) throws IOException {
    sendBytes(x, status, value.getBytes(StandardCharsets.UTF_8), type);
  }

  private void sendBytes(HttpExchange x, int status, byte[] bytes, String type) throws IOException {
    x.getResponseHeaders().set("Content-Type", type);
    x.getResponseHeaders().set("Cache-Control", "no-store");
    x.getResponseHeaders().set("X-Content-Type-Options", "nosniff");
    x.sendResponseHeaders(status, bytes.length);
    x.getResponseBody().write(bytes);
  }

  @Override
  public void close() {
    server.stop(1);
    network.shutdownNow();
  }
}
