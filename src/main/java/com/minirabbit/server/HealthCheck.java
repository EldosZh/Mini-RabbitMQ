package com.minirabbit.server;

import java.net.URI;
import java.net.http.*;
import java.time.Duration;

/** Compiled probe that also works in a JRE-only container. */
public final class HealthCheck {
  public static void main(String[] args) throws Exception {
    try (var client = HttpClient.newHttpClient()) {
      var request =
          HttpRequest.newBuilder(URI.create("http://127.0.0.1:8080/ready"))
              .timeout(Duration.ofSeconds(3))
              .build();
      if (client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode() != 200)
        System.exit(1);
    }
  }
}
