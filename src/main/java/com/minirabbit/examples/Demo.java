package com.minirabbit.examples;

import static com.minirabbit.core.Models.*;

import com.minirabbit.client.MiniRabbitClient;
import java.util.concurrent.CountDownLatch;

/** Three runnable demo roles share idempotent topology setup. */
public final class Demo {
  public static void main(String[] args) throws Exception {
    String role = args.length == 0 ? "producer" : args[0];
    String url = System.getenv().getOrDefault("MINIRABBIT_URL", "http://localhost:8080");
    try (var c = MiniRabbitClient.connect(url)) {
      c.declareExchange("orders", ExchangeType.TOPIC, true);
      c.declareExchange("dead", ExchangeType.DIRECT, true);
      c.declareQueue("dead-letters", true);
      c.bind("dead", "dead-letters", "failed");
      c.declareQueue("order-processing", true);
      c.declareQueue(
          new QueueDefinition(
              "notifications",
              true,
              10000,
              1048576,
              0,
              false,
              new RetryPolicy(3, 100, 1000, false),
              new DeadLetterPolicy("dead", "failed", 3)));
      c.bind("orders", "order-processing", "orders.*");
      c.bind("orders", "notifications", "orders.#");
      switch (role) {
        case "producer" -> {
          String[] keys = {"orders.created", "orders.paid", "orders.cancelled"};
          for (int i = 0; i < 100; i++)
            c.publish("orders", keys[i % 3], "{\"id\":" + i + ",\"fail\":" + (i % 10 == 0) + "}");
          System.out.println("Published 100 orders");
        }
        case "order-consumer" -> {
          try (var worker =
              c.consume(
                  "order-processing",
                  1,
                  2,
                  d -> {
                    System.out.println("Processed order " + d.value().message().id());
                    d.ack();
                  })) {
            new CountDownLatch(1).await();
          }
        }
        case "notification-worker" -> {
          try (var worker =
              c.consume(
                  "notifications",
                  d -> {
                    if (d.bodyAsString().contains("\"fail\":true")) {
                      System.out.println("Retrying notification attempt " + d.value().attempts());
                      d.nack(true);
                    } else {
                      System.out.println("Notification completed");
                      d.ack();
                    }
                  })) {
            new CountDownLatch(1).await();
          }
        }
        default ->
            throw new IllegalArgumentException(
                "Expected producer, order-consumer, or notification-worker");
      }
    }
  }
}
