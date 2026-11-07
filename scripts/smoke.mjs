// Process-level smoke test. Requires a built jar and installed dashboard dependencies.
import { spawn } from "node:child_process";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { randomBytes } from "node:crypto";
const java = process.env.JAVA_HOME
  ? join(
      process.env.JAVA_HOME,
      "bin",
      process.platform === "win32" ? "java.exe" : "java",
    )
  : "java";
const jar = resolve("target/mini-rabbitmq-1.0.0.jar"),
  token = randomBytes(24).toString("hex");
const directory = await mkdtemp(
  join(process.env.MINIRABBIT_TEST_TMP || tmpdir(), "minirabbit-smoke-"),
);
const config = join(directory, "config.yaml");
const port = Number(process.env.MINIRABBIT_TEST_PORT || 18080),
  dashboardPort = Number(process.env.MINIRABBIT_DASHBOARD_PORT || 15173);
const children = [];
const env = {
  ...process.env,
  MINIRABBIT_TOKEN: token,
  MINIRABBIT_URL: `http://127.0.0.1:${port}`,
  MINIRABBIT_PROXY_TARGET: `http://127.0.0.1:${port}`,
};
function launch(command, args, cwd = process.cwd()) {
  const child = spawn(command, args, {
    env,
    cwd,
    windowsHide: true,
    stdio: ["ignore", "pipe", "pipe"],
  });
  children.push(child);
  let output = "";
  child.stdout.on("data", (b) => (output += b));
  child.stderr.on("data", (b) => (output += b));
  const done = new Promise((res, rej) => {
    child.on("error", rej);
    child.on("exit", (code) =>
      code === 0
        ? res(output)
        : rej(Error(`Process failed (${code}): ${output}`)),
    );
  });
  done.catch(() => {});
  return { child, done, output: () => output };
}
async function until(check, timeout = 45000) {
  const end = Date.now() + timeout;
  let error;
  while (Date.now() < end) {
    try {
      if (await check()) return;
    } catch (e) {
      error = e;
    }
    await new Promise((r) => setTimeout(r, 100));
  }
  throw Error("Smoke check timed out" + (error ? ": " + error.message : ""));
}
async function api(path) {
  const r = await fetch(env.MINIRABBIT_URL + "/api/v1/" + path, {
    headers: { Authorization: "Bearer " + token },
    signal: AbortSignal.timeout(3000),
  });
  if (!r.ok) throw Error("API " + r.status);
  return r.json();
}
async function cli(...args) {
  return launch(java, ["-jar", jar, ...args]).done;
}
try {
  await writeFile(
    config,
    `host: 127.0.0.1\nport: ${port}\nstorageDirectory: ${JSON.stringify(join(directory, "data"))}\nfsyncPolicy: always\nmaxPendingOperations: 128\nmaxNetworkRequests: 32\nmaxConsumers: 256\nmaxPrefetch: 256\nmaxInFlight: 2048\nmaxTotalMessages: 10000\nmaxMessageBytes: 1048576\nmaxResidentBytes: 16777216\nvisibilityTimeoutMs: 30000\nconsumerIdleTimeoutMs: 60000\ncompactionIntervalMs: 60000\nmetricsEnabled: true\n`,
  );
  launch(java, ["-jar", jar, "server", "start", "--config", config]);
  await until(async () => {
    const r = await fetch(env.MINIRABBIT_URL + "/ready");
    return r.ok;
  });
  for (const args of [
    ["version"],
    ["exchange", "create", "cli.orders", "--type", "topic"],
    ["exchange", "list"],
    ["queue", "create", "cli.jobs", "--durable"],
    ["queue", "list"],
    ["bind", "cli.orders", "cli.jobs", "orders.*"],
    ["publish", "cli.orders", "orders.created", '{"id":123}'],
    ["queue", "stats", "cli.jobs"],
    ["stats"],
    ["dlq", "list"],
  ])
    await cli(...args);
  const consumer = launch(java, ["-jar", jar, "consume", "cli.jobs"]);
  await until(
    async () =>
      consumer.output().includes("123") &&
      (await api("queues/cli.jobs")).messages === 0,
  );
  consumer.child.kill();
  console.log(
    "PASS: executable CLI server, exchange, queue, bind, publish, consume, stats, dlq, version",
  );
  for (const role of ["order-consumer", "notification-worker"])
    launch(java, ["-cp", jar, "com.minirabbit.examples.Demo", role]);
  await launch(java, ["-cp", jar, "com.minirabbit.examples.Demo", "producer"])
    .done;
  await until(async () => {
    const qs = await api("queues");
    return (
      qs.find((q) => q.definition.name === "order-processing")?.messages ===
        0 &&
      qs.find((q) => q.definition.name === "notifications")?.messages === 0 &&
      qs.find((q) => q.definition.name === "dead-letters")?.messages === 10
    );
  });
  const stats = await api("stats");
  if (stats.deadLettered !== 10 || stats.published !== 101)
    throw Error("Unexpected demo counters");
  console.log(
    "PASS: all three demo applications, 100 orders, competing workers, retries, 10 dead letters",
  );
  const metrics = await fetch(env.MINIRABBIT_URL + "/metrics", {
    headers: { Authorization: "Bearer " + token },
  });
  if (
    !(await metrics.text()).includes(
      "minirabbit_messages_dead_lettered_total 10",
    )
  )
    throw Error("Metrics mismatch");
  // Vite's proxy has a fixed 8080 default; override through an explicit test environment variable.
  launch(
    process.execPath,
    [
      resolve("dashboard/node_modules/vite/bin/vite.js"),
      "--host",
      "127.0.0.1",
      "--port",
      String(dashboardPort),
      "--strictPort",
    ],
    resolve("dashboard"),
  );
  await until(async () => {
    const r = await fetch(`http://127.0.0.1:${dashboardPort}/`);
    return r.ok && (await r.text()).includes("Mini RabbitMQ");
  });
  const viaProxy = await fetch(
    `http://127.0.0.1:${dashboardPort}/api/v1/stats`,
    { headers: { Authorization: "Bearer " + token } },
  );
  if (!viaProxy.ok || (await viaProxy.json()).deadLettered !== 10)
    throw Error("Dashboard API proxy mismatch");
  console.log(
    "PASS: dashboard HTML and live authenticated API proxy; interactive browser rendering not tested",
  );
  console.log("PASS: smoke test complete");
} finally {
  for (const child of children)
    if (child.exitCode === null && child.signalCode === null) child.kill();
  await Promise.all(
    children.map((c) =>
      c.exitCode !== null || c.signalCode !== null
        ? null
        : new Promise((r) => c.once("exit", r)),
    ),
  );
  // Only remove the unique directory created by this script.
  if (
    !directory.startsWith(resolve(process.env.MINIRABBIT_TEST_TMP || tmpdir()))
  )
    throw Error("Unsafe cleanup path");
  await rm(directory, { recursive: true, force: true });
}
