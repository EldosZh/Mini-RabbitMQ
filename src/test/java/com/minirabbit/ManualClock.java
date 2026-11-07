package com.minirabbit;

import com.minirabbit.core.BrokerClock;
import java.util.concurrent.atomic.AtomicLong;

final class ManualClock implements BrokerClock {
  private final AtomicLong elapsed = new AtomicLong();

  public long millis() {
    return 1_800_000_000_000L + elapsed.get();
  }

  public long nanoTime() {
    return elapsed.get() * 1_000_000L;
  }

  void advance(long millis) {
    elapsed.addAndGet(millis);
  }
}
