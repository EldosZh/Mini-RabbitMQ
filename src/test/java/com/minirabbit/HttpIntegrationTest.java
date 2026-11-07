package com.minirabbit;

import static com.minirabbit.core.Models.*;
import static org.junit.jupiter.api.Assertions.*;

import com.minirabbit.client.MiniRabbitClient;
import com.minirabbit.core.*;
import com.minirabbit.server.BrokerServer;
import java.net.*;
import java.net.http.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

class HttpIntegrationTest {
  @TempDir Path dir;

  @Test
  void realNetworkSdkSecurityAndMetrics() throws Exception {
    var config =
        new BrokerConfiguration(
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
            1000,
            10000,
            3600000,
            true);
    try (var b = new Broker(config);
        var s = new BrokerServer(b, "integration-token-12345")) {
      s.start();
      String base = "http://127.0.0.1:" + s.port();
      try (var c = new MiniRabbitClient(base, "integration-token-12345");
          var http = HttpClient.newHttpClient()) {
        c.declareExchange("e", ExchangeType.TOPIC, true);
        c.declareQueue("q", true);
        c.bind("e", "q", "orders.#");
        assertEquals(ConfirmStatus.PERSISTED, c.publish("e", "orders.created", "hello").status());
        var d = c.poll("q", "c", 1, 0).orElseThrow();
        assertEquals("hello", d.bodyAsString());
        d.nack(true);
        assertThrows(Broker.Failure.class, d::ack);
        Thread.sleep(1050);
        var again = c.poll("q", "c", 1, 2000).orElseThrow();
        assertTrue(again.value().redelivered());
        again.ack();
        assertEquals(0, b.queueStatistics("q").messages());
        assertEquals(
            401,
            http.send(
                    HttpRequest.newBuilder(URI.create(base + "/api/v1/queues/q")).DELETE().build(),
                    HttpResponse.BodyHandlers.ofString())
                .statusCode());
        var malformed =
            HttpRequest.newBuilder(URI.create(base + "/api/v1/messages/publish"))
                .header("Authorization", "Bearer integration-token-12345")
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{bad"))
                .build();
        assertEquals(400, http.send(malformed, HttpResponse.BodyHandlers.ofString()).statusCode());
        var metrics =
            http.send(
                HttpRequest.newBuilder(URI.create(base + "/metrics"))
                    .header("Authorization", "Bearer integration-token-12345")
                    .build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, metrics.statusCode());
        assertTrue(metrics.body().contains("minirabbit_messages_acked_total 1"));
        assertEquals(
            200,
            http.send(
                    HttpRequest.newBuilder(URI.create(base + "/health")).build(),
                    HttpResponse.BodyHandlers.ofString())
                .statusCode());
        assertThrows(
            Broker.Failure.class, () -> c.request("DELETE", "/api/v1/queues/%2e%2e", null));
        assertThrows(
            Broker.Failure.class,
            () ->
                c.request(
                    "POST",
                    "/api/v1/messages/publish",
                    Map.of(
                        "exchange",
                        "e",
                        "routingKey",
                        "orders.created",
                        "payload",
                        "not base64 !",
                        "mandatory",
                        true)));
        c.publish("e", "orders.paid", "worker");
        CountDownLatch latch = new CountDownLatch(1);
        try (var worker =
            c.consume(
                "q",
                d2 -> {
                  d2.ack();
                  latch.countDown();
                })) {
          assertTrue(latch.await(5, TimeUnit.SECONDS));
        }
        c.request("POST", "/api/v1/storage/compact", Map.of());
        assertEquals(1, ((Number) b.stats().get("compactions")).intValue());
      }
    }
  }

  @Test
  void cliCommandsAreRegistered() {
    var command = new picocli.CommandLine(new com.minirabbit.cli.Main());
    var bytes = new java.io.StringWriter();
    command.setOut(new java.io.PrintWriter(bytes));
    assertEquals(0, command.execute("--version"));
    assertTrue(bytes.toString().contains("Mini RabbitMQ"));
    assertTrue(command.getSubcommands().containsKey("dlq"));
    assertEquals(0, command.execute("exchange", "--help"));
  }
}
