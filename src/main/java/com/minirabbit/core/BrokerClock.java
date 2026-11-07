package com.minirabbit.core;

/** Separate persisted wall-clock timestamps from process-local monotonic deadlines. */
public interface BrokerClock {
  long millis();

  long nanoTime();

  BrokerClock SYSTEM =
      new BrokerClock() {
        public long millis() {
          return System.currentTimeMillis();
        }

        public long nanoTime() {
          return System.nanoTime();
        }
      };
}
