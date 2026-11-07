package com.minirabbit.core;

import static java.nio.file.StandardOpenOption.*;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.zip.CRC32C;

/** Each checksummed frame commits an entire durable projection as one atomic transaction. */
public final class Wal implements AutoCloseable {
  private static final int MAGIC = 0x4D525131, MAX = 64 * 1024 * 1024, HEADER = 20;
  private final Path path;
  private final boolean sync;
  private FileChannel channel, lockChannel;
  private FileLock lock;
  private long compactions;

  public Wal(Path directory, boolean sync) throws IOException {
    Files.createDirectories(directory);
    this.path = directory.resolve("broker.wal");
    this.sync = sync;
    lockChannel = FileChannel.open(directory.resolve("broker.lock"), CREATE, WRITE);
    try {
      lock = lockChannel.tryLock();
      if (lock == null) throw new IOException("Storage is already in use");
      channel = FileChannel.open(path, CREATE, READ, WRITE);
      if (sync) syncDirectory(directory);
    } catch (Exception e) {
      if (channel != null) channel.close();
      lockChannel.close();
      throw e;
    }
  }

  public Models.State recover() throws IOException {
    return read(channel, true);
  }

  private static Models.State read(FileChannel c, boolean repair) throws IOException {
    long offset = 0, size = c.size();
    Models.State latest = new Models.State();
    while (offset < size) {
      if (size - offset < HEADER) {
        if (repair) {
          c.truncate(offset);
          c.force(true);
          break;
        }
        throw new IOException("Truncated header");
      }
      ByteBuffer h = ByteBuffer.allocate(HEADER);
      readFully(c, h, offset);
      h.flip();
      int magic = h.getInt(),
          version = h.getShort(),
          type = h.getShort(),
          length = h.getInt(),
          checksum = h.getInt(),
          headerChecksum = h.getInt();
      if (headerCrc(h.array()) != headerChecksum)
        throw new IOException("WAL header checksum mismatch at " + offset);
      if (magic != MAGIC || version != 1 || type < 1 || type > 10 || length < 2 || length > MAX)
        throw new IOException("Corrupt WAL header at " + offset);
      if (size - offset - HEADER < length) {
        if (repair) {
          c.truncate(offset);
          c.force(true);
          break;
        }
        throw new IOException("Truncated payload");
      }
      ByteBuffer b = ByteBuffer.allocate(length);
      readFully(c, b, offset + HEADER);
      byte[] data = b.array();
      if (crc(type, data) != checksum) throw new IOException("WAL checksum mismatch at " + offset);
      try {
        latest = Json.read(data, Models.State.class);
      } catch (RuntimeException e) {
        throw new IOException("Invalid WAL state at " + offset, e);
      }
      offset += HEADER + length;
    }
    c.position(c.size());
    return latest;
  }

  private static void readFully(FileChannel c, ByteBuffer b, long offset) throws IOException {
    while (b.hasRemaining()) {
      int n = c.read(b, offset + b.position());
      if (n < 0) throw new EOFException();
    }
  }

  private static int crc(int type, byte[] data) {
    CRC32C crc = new CRC32C();
    crc.update(1);
    crc.update(type);
    crc.update(data);
    return (int) crc.getValue();
  }

  private static int headerCrc(byte[] bytes) {
    CRC32C crc = new CRC32C();
    crc.update(bytes, 0, 16);
    return (int) crc.getValue();
  }

  private static ByteBuffer frame(int type, byte[] data) throws IOException {
    if (type < 1 || type > 10) throw new IOException("Invalid WAL transaction type");
    if (data.length > MAX) throw new IOException("Durable state exceeds WAL frame limit");
    ByteBuffer frame =
        ByteBuffer.allocate(HEADER + data.length)
            .putInt(MAGIC)
            .putShort((short) 1)
            .putShort((short) type)
            .putInt(data.length)
            .putInt(crc(type, data));
    return frame.putInt(headerCrc(frame.array())).put(data).flip();
  }

  public void append(int type, byte[] data) throws IOException {
    ByteBuffer b = frame(type, data);
    channel.position(channel.size());
    while (b.hasRemaining()) channel.write(b);
    if (sync) channel.force(true);
  }

  public void compact(byte[] data) throws IOException {
    Path tmp = path.resolveSibling("broker.wal.tmp");
    try (FileChannel c = FileChannel.open(tmp, CREATE, TRUNCATE_EXISTING, READ, WRITE)) {
      ByteBuffer b = frame(10, data);
      while (b.hasRemaining()) c.write(b);
      c.force(true);
      read(c, false);
    }
    channel.close();
    try {
      atomicReplace(tmp);
      compactions++;
    } finally {
      channel = FileChannel.open(path, CREATE, READ, WRITE);
      channel.position(channel.size());
    }
    // Directory fsync is supported on Unix. Windows relies on atomic replacement semantics.
    syncDirectory(path.getParent());
  }

  private void atomicReplace(Path tmp) throws IOException {
    for (int attempt = 0; ; attempt++)
      try {
        Files.move(tmp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        return;
      } catch (AccessDeniedException e) {
        // Windows scanners can briefly hold a file after close. Preserve atomic semantics.
        if (attempt == 5) throw e;
        try {
          Thread.sleep(10L << attempt);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new IOException("Compaction interrupted", interrupted);
        }
      }
  }

  private static void syncDirectory(Path path) throws IOException {
    try (FileChannel directory = FileChannel.open(path, READ)) {
      directory.force(true);
    } catch (AccessDeniedException | UnsupportedOperationException ignored) {
    }
  }

  public long size() throws IOException {
    return channel.size();
  }

  public long compactions() {
    return compactions;
  }

  @Override
  public void close() throws IOException {
    try {
      if (channel != null) channel.close();
    } finally {
      try {
        if (lock != null) lock.release();
      } finally {
        lockChannel.close();
      }
    }
  }
}
