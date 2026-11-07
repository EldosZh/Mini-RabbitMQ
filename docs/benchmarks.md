# Benchmark methodology and measured results

The executable is `com.minirabbit.bench.Benchmark`. The full, actual output from the final run is retained in [benchmark-results.txt](benchmark-results.txt); the first line records Java/OS/CPU-count/heap information and subsequent lines are JSON results. No rates are estimated or invented.

## Hardware and runtime

* Windows 11 amd64.
* Intel Core i5-1155G7 @ 2.50 GHz, 4 cores / 8 logical processors.
* 16,952,647,680 bytes physical RAM (approximately 15.8 GiB).
* Physical disk model `511BS0512HB`, reported capacity 512,105,932,800 bytes. The OS reports fixed media; no measured disk bandwidth or controller cache properties are assumed.
* Temurin Java 25.0.5+8 LTS, default JVM settings, maximum heap 4,238,344,192 bytes.
* Temporary benchmark storage was on D:; source/JAR were in the OneDrive-backed project directory on C:. Other machine services were not disabled.

## Procedure

```powershell
& 'D:\mini-rabbitmq-build-tools\jdk\bin\java.exe' '-Djava.io.tmpdir=D:\mini-rabbitmq-build-tools' -cp target/mini-rabbitmq-1.0.0.jar com.minirabbit.bench.Benchmark 100
```

This is one short, in-process diagnostic pass, run after the smoke-test processes had stopped. It is not JMH, a network benchmark, or a steady-state production load test. There is no controlled warmup, repeated fork distribution, CPU pinning, or background-service isolation. Early cases include JIT/classloading effects; do not infer that persistence is faster than memory from different cases in this short run.

Each messaging scenario uses 100 messages of 256 payload bytes, one target queue, and topic routing. Both `always` and `none` fsync policies are exercised with persistent and transient messages. The queue fills for publish, then drains through consumption/ACK. Full-state WAL cost changes with depth. `memory-publish` means a transient message in a durable queue: the implementation still commits the durable projection, so this is not a disk-free baseline. `durable-publish` with `none` means persistent envelopes appended without fsync and does not imply a `PERSISTED` confirm.

Publish percentiles time each complete in-process publish. Consume throughput includes ACKs; its percentiles measure poll/delivery latency only. ACK-only throughput uses the sum of measured ACK durations, excluding preparatory polls. Recovery reopens a compacted WAL with 10 live persistent messages, or zero for the transient case. Routing performs 10,000 matcher calls. Raw WAL cases append an empty-state JSON record (55 payload bytes, plus a 20-byte frame header), isolating storage framing/force overhead. MiB/s counts logical payload/JSON bytes, excluding framing, serialization amplification, and fanout copies.

Scaling uses transient topology/messages and fsync `none`, comparing one versus four concurrent producers, followed by one versus four competing consumers. It does not measure simultaneous streaming producers and consumers. The executable aborts on operation failure or incorrect recovered/consumed counts; completed scenarios below had zero errors.

## Selected final results

Latencies are microseconds. Rows are rounded for readability; raw output retains full precision.

| Scenario | Fsync | msg/s | MiB/s | p50 µs | p95 µs | p99 µs |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Transient publish | none | 168.49 | 0.0411 | 3,117.7 | 18,227.5 | 33,353.0 |
| Persistent publish | none | 1,104.35 | 0.2696 | 818.9 | 1,762.5 | 2,369.1 |
| Transient publish | always | 347.01 | 0.0847 | 2,635.0 | 4,510.7 | 4,938.3 |
| Persistent publish | always | 252.68 | 0.0617 | 3,820.4 | 5,987.3 | 6,499.1 |
| Persistent consume + ACK | always | 123.52 | 0.0302 | 5,105.6 | 6,064.3 | 6,768.6 |
| Persistent ACK only | always | 305.87 | 0.0747 | 2,665.4 | 5,024.8 | 17,703.2 |
| Topic matcher | n/a | 174,289.21 | 0 | 5.5 | 6.6 | 14.2 |
| Empty-state WAL append | none | 2,800.47 | 0.1469 | 15.2 | 37.0 | 207.8 |
| Empty-state WAL append | always | 824.26 | 0.0432 | 887.1 | 3,093.2 | 3,910.8 |
| One concurrent producer | none | 493.72 | 0.1205 | 1,595.2 | 3,289.7 | 9,773.9 |
| Four concurrent producers | none | 703.21 | 0.1717 | 4,846.4 | 11,080.6 | 11,934.7 |
| One competing consumer | none | 307.22 | 0.0750 | 3,087.0 | 5,783.9 | 6,171.0 |
| Four competing consumers | none | 325.23 | 0.0794 | 12,153.1 | 22,242.1 | 24,170.5 |

Recovery of the compacted 10-message persistent state took **3,705.9 µs** under `none` and **3,867.8 µs** under `always` in this run. Zero-message transient recovery took 5,859.5 µs and 4,756.6 µs respectively. These are single observations, not latency percentile estimates.

## Interpretation and next measurements

Full-state copying, serialization, queue scans, and per-transition fsync dominate messaging cost; the standalone matcher is much cheaper. More producers can overlap submission, but all mutations still pass through one actor. Additional consumers do not remove that bottleneck and increase individual latency.

Before any capacity planning, run longer warmup/measurement forks, vary payload sizes, depths and fanout, measure HTTP throughput and disk bytes, inspect GC/heap, run hour-long producer/consumer mixes, test overload, and measure recovery from a large uncompacted log. None of those unperformed experiments are claimed here.
