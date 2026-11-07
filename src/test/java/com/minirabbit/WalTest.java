package com.minirabbit;

import static com.minirabbit.core.Models.*;
import static org.junit.jupiter.api.Assertions.*;

import com.minirabbit.core.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

class WalTest {
  @TempDir Path dir;

  @Test
  void appendReplayAndCompaction() throws Exception {
    State s = new State();
    s.exchanges.put("e", new Exchange("e", ExchangeType.TOPIC, true));
    try (var wal = new Wal(dir, true)) {
      wal.append(1, Json.bytes(s));
      s.sequence = 100;
      wal.append(2, Json.bytes(s));
      assertEquals(100, wal.recover().sequence);
      long before = wal.size();
      wal.compact(Json.bytes(s));
      assertTrue(wal.size() < before);
      assertEquals(100, wal.recover().sequence);
    }
    try (var wal = new Wal(dir, true)) {
      assertEquals(ExchangeType.TOPIC, wal.recover().exchanges.get("e").type());
    }
  }

  @Test
  void abandonedTemporaryFileDoesNotReplaceWal() throws Exception {
    State s = new State();
    s.sequence = 9;
    try (var wal = new Wal(dir, true)) {
      wal.append(1, Json.bytes(s));
    }
    Files.writeString(dir.resolve("broker.wal.tmp"), "torn temporary snapshot");
    try (var wal = new Wal(dir, true)) {
      assertEquals(9, wal.recover().sequence);
    }
  }

  @Test
  void failedCompactionKeepsOriginalValidCopy() throws Exception {
    State s = new State();
    s.sequence = 9;
    try (var wal = new Wal(dir, true)) {
      wal.append(1, Json.bytes(s));
      Files.createDirectory(dir.resolve("broker.wal.tmp"));
      assertThrows(java.io.IOException.class, () -> wal.compact(Json.bytes(s)));
      assertEquals(9, wal.recover().sequence);
    }
    try (var wal = new Wal(dir, true)) {
      assertEquals(9, wal.recover().sequence);
    }
  }

  @Test
  void invalidLengthAndVersionFailRecovery() throws Exception {
    try (var wal = new Wal(dir, true)) {
      wal.append(1, Json.bytes(new State()));
    }
    try (var c = FileChannel.open(dir.resolve("broker.wal"), StandardOpenOption.WRITE)) {
      c.write(ByteBuffer.allocate(4).putInt(Integer.MAX_VALUE).flip(), 8);
    }
    try (var wal = new Wal(dir, true)) {
      assertThrows(java.io.IOException.class, wal::recover);
    }
  }

  @Test
  void closedStorageCannotAcknowledgeAppend() throws Exception {
    Wal wal = new Wal(dir, true);
    wal.close();
    assertThrows(java.io.IOException.class, () -> wal.append(1, Json.bytes(new State())));
  }

  @Test
  void damagedLengthCannotMasqueradeAsIncompleteTail() throws Exception {
    try (var wal = new Wal(dir, true)) {
      wal.append(1, Json.bytes(new State()));
    }
    Path path = dir.resolve("broker.wal");
    byte[] bytes = Files.readAllBytes(path);
    int originalLength = ByteBuffer.wrap(bytes).getInt(8);
    try (var channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
      channel.write(ByteBuffer.allocate(4).putInt(originalLength + 1).flip(), 8);
    }
    try (var wal = new Wal(dir, true)) {
      assertThrows(java.io.IOException.class, wal::recover);
    }
    assertEquals(bytes.length, Files.size(path));
  }

  @Test
  void fsyncNoneDoesNotClaimPersistence() throws Exception {
    var config =
        new BrokerConfiguration(
            "127.0.0.1",
            0,
            dir.toString(),
            "none",
            32,
            8,
            16,
            10,
            32,
            100,
            1024,
            1048576,
            1000,
            10000,
            60000,
            true);
    try (var b = new Broker(config)) {
      b.declareExchange(new Exchange("e", ExchangeType.DIRECT, true));
      b.declareQueue(QueueDefinition.defaults("q", true));
      b.bind(new Binding("e", "q", "k"));
      assertEquals(
          ConfirmStatus.ACCEPTED,
          b.publish(new Publish("e", "k", new byte[0], null, true)).status());
    }
  }
}
