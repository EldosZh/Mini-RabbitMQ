package com.minirabbit.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.nio.file.*;

public record BrokerConfiguration(
    String host,
    int port,
    String storageDirectory,
    String fsyncPolicy,
    int maxPendingOperations,
    int maxNetworkRequests,
    int maxConsumers,
    int maxPrefetch,
    int maxInFlight,
    int maxTotalMessages,
    int maxMessageBytes,
    long maxResidentBytes,
    long visibilityTimeoutMs,
    long consumerIdleTimeoutMs,
    long compactionIntervalMs,
    boolean metricsEnabled) {
  public BrokerConfiguration {
    if (host == null
        || host.isBlank()
        || port < 0
        || port > 65535
        || storageDirectory == null
        || storageDirectory.isBlank()
        || fsyncPolicy == null
        || !java.util.Set.of("always", "none").contains(fsyncPolicy)
        || maxPendingOperations < 1
        || maxPendingOperations > 4096
        || maxNetworkRequests < 1
        || maxNetworkRequests > 256
        || maxConsumers < 1
        || maxConsumers > 4096
        || maxPrefetch < 1
        || maxPrefetch > 1024
        || maxInFlight < 1
        || maxInFlight > 65536
        || maxTotalMessages < 1
        || maxTotalMessages > 100000
        || maxMessageBytes < 1
        || maxMessageBytes > 8 * 1024 * 1024
        || maxResidentBytes < 1
        || maxResidentBytes > 24 * 1024 * 1024
        || visibilityTimeoutMs < 10
        || visibilityTimeoutMs > 86400000
        || consumerIdleTimeoutMs < visibilityTimeoutMs
        || consumerIdleTimeoutMs > 604800000
        || compactionIntervalMs < 100
        || compactionIntervalMs > 604800000)
      throw new IllegalArgumentException("Invalid broker configuration");
  }

  public static BrokerConfiguration defaults() {
    return new BrokerConfiguration(
        "127.0.0.1",
        8080,
        "data",
        "always",
        128,
        32,
        256,
        256,
        2048,
        10000,
        1024 * 1024,
        16 * 1024 * 1024,
        30000,
        60000,
        60000,
        true);
  }

  public static BrokerConfiguration load(Path path) throws java.io.IOException {
    return new ObjectMapper(new YAMLFactory())
        .readValue(Files.readAllBytes(path), BrokerConfiguration.class);
  }
}
