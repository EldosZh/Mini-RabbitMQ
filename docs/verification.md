# Verification report

## Environment

Development took place on Windows 11 amd64 with Node 24.12.0, Temurin OpenJDK **25.0.5+8 LTS**, and Maven **3.9.11**. The original Java PATH entry was unusable and Maven was absent. Build tools were downloaded into ignored/project-specific caches; the JDK and Maven dependency repository reside on D: because C: had insufficient free space. No global Java/Git settings were changed.

The workspace now has its own Git repository on branch `main`. No commits, remotes, or pushes were created. Runtime state, node_modules, target binaries, and local tools are ignored.

## Java verification

Clean Maven build and test verification passed:

| Suite | Cases | Failures | Errors | Skips |
| --- | ---: | ---: | ---: | ---: |
| BrokerTest | 28 | 0 | 0 | 0 |
| ReliabilityTest | 9 | 0 | 0 | 0 |
| WalTest | 7 | 0 | 0 | 0 |
| CrashRecoveryTest | 5 | 0 | 0 | 0 |
| HttpIntegrationTest | 2 | 0 | 0 | 0 |
| **Total** | **51** | **0** | **0** | **0** |

Unit tests verify direct/fanout/topic routing and wildcard edges; per-queue deduplication; mandatory routing; FIFO and priorities; binary payloads; atomic all-target rollback; queue limits under concurrent producers; competing consumers; prefetch updates and consumer limits; ACK/NACK/reject; concurrent ACK-versus-NACK; stale/foreign tokens; lease renewal, expiry, and cancellation; TTL and delayed visibility; retry/backoff/jitter bounds; bounded dead-letter cycles and full-header handling; persistent/transient combinations; durable metadata, messages, schedules and ACKs; WAL append/replay, partial header/payload recovery and payload/header corruption (including a length field that could otherwise resemble a torn append); abandoned temporary snapshots; failed compaction preserving the old WAL; storage locks and invalid inputs.

Core timer tests use an injected clock. HTTP tests use an actual authenticated loopback server and SDK, verify malformed JSON/base64 and unauthorized deletion, exercise callback workers, check metrics and health, and request real compaction. Process tests halt a child JVM without shutdown hooks and verify recovery of in-flight/delayed work and absence of acknowledged work. Other process tests cover shutdown during publishing, retry schedules across restart, concurrent capacity, and full-DLQ draining.

Initial runs found a CLI help flag omission, a short real-time test lease, insufficient child cleanup after failure, and one Windows compaction failure that did not reproduce in the isolated rerun. Changes added CLI help, controllable clocks, forced child cleanup, storage exception diagnostics, and bounded retries for `AccessDeniedException` during atomic replacement. The original compaction failure's precise filesystem cause was not captured. Atomicity is preserved and persistent storage failure remains fatal. Final suite results above are from the corrected clean build.

## Executable smoke verification

`node scripts/smoke.mjs` starts isolated processes and checks:

* Executable CLI `server start`, `exchange create/list`, `queue create/list/stats`, `bind`, `publish`, `consume`, `stats`, `dlq list`, and `version`.
* All three Java demo roles: 100 orders, competing workers, successful processing, retry exhaustion, and exactly 10 dead letters from intentionally failing notifications.
* Authenticated metrics matching the demo state.
* Vite serving the dashboard HTML and proxying the authenticated stats API with the expected live dead-letter counter.

The script generates its token in memory, uses a unique temporary storage directory, terminates only processes it launches, and removes only its own temporary directory. The first smoke run completed all functional checks but exposed a cleanup bug for processes terminated by signal; cleanup was corrected and the script rerun.

## Dashboard and deployment

TypeScript compilation and Vite production build passed. `npm audit` after the Vite update reported **zero known vulnerabilities** for the installed dependency graph. This is an npm audit result, not a claim of a comprehensive security audit.

No interactive browser-control tool was available. Visual rendering, button interactions, accessibility with assistive technologies, and responsive layout were not browser-verified. Type checking, asset generation, actual HTML serving, and live API proxying were verified separately.

Docker CLI 29.4.3 is present, but the Docker daemon was not running. `docker compose config --quiet` passed using a validation-only environment token. Image builds, container health checks, volume ownership, and full Compose operation were not executed. The compiled Java health probe uses JRE APIs; source-file launching is not used.

## Exact commands used in this environment

```powershell
$env:JAVA_HOME = 'D:\mini-rabbitmq-build-tools\jdk'
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
& '.tools/maven/apache-maven-3.9.11/bin/mvn.cmd' '-Dmaven.repo.local=D:\mini-rabbitmq-build-tools\m2' -q clean verify

# Offline test verification after dependency resolution:
& '.tools/maven/apache-maven-3.9.11/bin/mvn.cmd' '-Dmaven.repo.local=D:\mini-rabbitmq-build-tools\m2' -o -q test

# From dashboard/:
npm run build
npm audit

# Back at repository root:
$env:MINIRABBIT_TEST_TMP = 'D:\mini-rabbitmq-build-tools'
node scripts/smoke.mjs
& "$env:JAVA_HOME\bin\java.exe" '-Djava.io.tmpdir=D:\mini-rabbitmq-build-tools' -cp target/mini-rabbitmq-1.0.0.jar com.minirabbit.bench.Benchmark 100
```

These cache paths are specific to this development machine. A normal checkout with Java 25/Maven installed uses `mvn clean verify`, `npm ci`, and the portable commands in the README.

To start the built project on this machine:

```powershell
$env:JAVA_HOME = 'D:\mini-rabbitmq-build-tools\jdk'
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
$env:MINIRABBIT_TOKEN = [Convert]::ToHexString([System.Security.Cryptography.RandomNumberGenerator]::GetBytes(24))
java -jar target/mini-rabbitmq-1.0.0.jar server start
```

Open another terminal with the same token and run demo roles or the CLI. Start the local dashboard with `cd dashboard; npm run dev` and enter the token in its connection form. No permanent server is left running after verification.

## Benchmark scope and remaining limits

Actual measured output and methodology are in [benchmarks](benchmarks.md) and [raw results](benchmark-results.txt). All benchmark scenarios completed with no operation errors. The short local run does not establish production capacity, long-term reliability, physical power-loss durability, or security against hostile Internet traffic. No results for Docker or browser rendering are fabricated.
