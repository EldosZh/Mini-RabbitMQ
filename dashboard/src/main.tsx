import React, { useEffect, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import "./style.css";

type Stats = {
  published: number;
  delivered: number;
  acked: number;
  nacked: number;
  redelivered: number;
  deadLettered: number;
  queueDepth: number;
  unacked: number;
  activeConsumers: number;
  storageBytes: number;
  persistentMessages: number;
  compactions: number;
  recoverySeconds: number;
  fsyncPolicy: string;
  ready: boolean;
};
type Queue = {
  definition: {
    name: string;
    durable: boolean;
    priorities: boolean;
    ttlMs: number;
    retry: { maxAttempts: number };
    deadLetter: { exchange: string; routingKey: string } | null;
  };
  messages: number;
  ready: number;
  scheduled: number;
  unacked: number;
  consumers: number;
};
type Exchange = { name: string; type: string; durable: boolean };
type Binding = { exchange: string; queue: string; key: string };
type Consumer = {
  id: string;
  queue: string;
  prefetch: number;
  unacked: number;
  status: string;
};
type Delivery = {
  token: string;
  consumerId: string;
  queue: string;
  attempts: number;
  message: {
    id: string;
    exchange: string;
    routingKey: string;
    payload: string;
    properties: { headers: Record<string, string> };
  };
};
type Snapshot = {
  stats: Stats;
  queues: Queue[];
  exchanges: Exchange[];
  bindings: Binding[];
  consumers: Consumer[];
};
const pages = [
  "Overview",
  "Exchanges",
  "Queues",
  "Consumers",
  "Dead Letters",
  "Storage",
] as const;
type Page = (typeof pages)[number];
const fmt = (n: number) => n.toLocaleString();
function App() {
  const [token, setToken] = useState(""),
    [connected, setConnected] = useState(false),
    [page, setPage] = useState<Page>("Overview"),
    [data, setData] = useState<Snapshot | null>(null),
    [error, setError] = useState(""),
    [busy, setBusy] = useState(false),
    [rate, setRate] = useState<number | null>(null),
    [history, setHistory] = useState<number[]>([]),
    [last, setLast] = useState(""),
    [inspected, setInspected] = useState<Delivery | null>(null),
    [dlq, setDlq] = useState("");
  const prev = useRef<{ n: number; t: number } | null>(null);
  const loading = useRef(false);
  const generation = useRef(0);
  const consumer = useRef("dashboard-" + crypto.randomUUID());
  function disconnect() {
    void api("consumers/" + consumer.current, "DELETE").catch(() => {});
    generation.current++;
    loading.current = false;
    prev.current = null;
    consumer.current = "dashboard-" + crypto.randomUUID();
    setConnected(false);
    setToken("");
    setData(null);
    setInspected(null);
    setError("");
    setHistory([]);
    setLast("");
  }
  async function api<T>(
    path: string,
    method = "GET",
    body?: unknown,
  ): Promise<T> {
    const r = await fetch("/api/v1/" + path, {
      method,
      headers: {
        Authorization: "Bearer " + token,
        "Content-Type": "application/json",
      },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: AbortSignal.timeout(15000),
    });
    if (!r.ok) {
      let text = "Request failed (" + r.status + ")";
      try {
        text = (await r.json()).error?.message || text;
      } catch {}
      throw Error(text);
    }
    return r.status === 204 ? (null as T) : await r.json();
  }
  async function refresh() {
    if (loading.current) return;
    const version = generation.current;
    loading.current = true;
    try {
      const [stats, queues, exchanges, bindings, consumers] = await Promise.all(
        [
          api<Stats>("stats"),
          api<Queue[]>("queues"),
          api<Exchange[]>("exchanges"),
          api<Binding[]>("bindings"),
          api<Consumer[]>("consumers"),
        ],
      );
      if (version !== generation.current) return;
      setData({ stats, queues, exchanges, bindings, consumers });
      setError("");
      setLast(new Date().toLocaleTimeString());
      const now = performance.now();
      if (prev.current && stats.published >= prev.current.n) {
        const r =
          (stats.published - prev.current.n) / ((now - prev.current.t) / 1000);
        setRate(r);
        setHistory((h) => [...h, r].slice(-30));
      } else {
        setRate(null);
        setHistory([]);
      }
      prev.current = { n: stats.published, t: now };
    } catch (e) {
      if (version === generation.current) setError((e as Error).message);
    } finally {
      if (version === generation.current) loading.current = false;
    }
  }
  useEffect(() => {
    if (!connected) return;
    void refresh();
    const timer = setInterval(() => void refresh(), 2500);
    return () => clearInterval(timer);
  }, [connected, token]);
  async function action(fn: () => Promise<void>) {
    setBusy(true);
    try {
      await fn();
      setError("");
      await refresh();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }
  const dlqNames = data
    ? [
        ...new Set(
          data.queues.flatMap((q) =>
            q.definition.deadLetter
              ? data.bindings
                  .filter(
                    (b) => b.exchange === q.definition.deadLetter!.exchange,
                  )
                  .map((b) => b.queue)
              : [],
          ),
        ),
      ]
    : [];
  const max = Math.max(1, ...history);
  const path = history
    .map(
      (v, i) =>
        `${i === 0 ? "M" : "L"} ${history.length <= 1 ? 0 : (i / (history.length - 1)) * 900} ${160 - (v / max) * 135}`,
    )
    .join(" ");
  return (
    <div className="shell">
      <aside>
        <a
          className="brand"
          href="#"
          onClick={(e) => {
            e.preventDefault();
            setPage("Overview");
          }}
        >
          <span className="brandmark">
            m<span>q</span>
          </span>
          <span>
            mini rabbit<span className="brand-sub">BROKER CONSOLE</span>
          </span>
        </a>
        <div className="workspace">
          <span className="dot" /> Local broker{" "}
          <span className="version">v1.0</span>
        </div>
        <div className="nav-label">WORKSPACE</div>
        <nav aria-label="Main navigation">
          {pages.map((p, i) => (
            <button
              key={p}
              onClick={() => setPage(p)}
              className={page === p ? "active" : ""}
            >
              <span className="nav-icon">
                {["◫", "⇄", "▤", "♧", "↳", "▱"][i]}
              </span>
              {p}
              {p === "Queues" && data && (
                <span className="count">{data.queues.length}</span>
              )}
            </button>
          ))}
        </nav>
        <div className="aside-foot">
          <span className="dot" /> Single broker<div>Java · HTTP / JSON</div>
          <small>At-least-once delivery</small>
        </div>
      </aside>
      <main>
        <header>
          <div className="breadcrumb">
            Workspace <span>/</span> <b>{page}</b>
          </div>
          <div className="header-right">
            <span
              className={
                "status " + (data?.stats.ready && !error ? "healthy" : "")
              }
            >
              {data?.stats.ready && !error ? "● Connected" : "○ Disconnected"}
            </span>
            <a
              href="https://github.com/rabbitmq/rabbitmq-server"
              target="_blank"
              rel="noreferrer"
              className="hidden-link"
            >
              RabbitMQ inspiration ↗
            </a>
          </div>
        </header>
        <div className="content">
          <div className="page-heading">
            <div>
              <div className="eyebrow">MINI RABBITMQ</div>
              <h1>{page === "Overview" ? "Broker overview" : page}</h1>
              <p>
                {
                  {
                    Overview:
                      "A live view of your messages, queues, and workers.",
                    Exchanges:
                      "Follow messages from routing keys to their destinations.",
                    Queues:
                      "Monitor ready, scheduled, and unacknowledged work.",
                    Consumers:
                      "Keep an eye on your competing workers and prefetch limits.",
                    "Dead Letters":
                      "Inspect failed work and choose when to replay it.",
                    Storage:
                      "Durable state, recovery, and write-ahead log maintenance.",
                  }[page]
                }
              </p>
            </div>
            {connected && (
              <button className="button" onClick={() => void refresh()}>
                ↻ Refresh
              </button>
            )}
          </div>
          {!connected && (
            <form
              className="connect panel"
              onSubmit={(e) => {
                e.preventDefault();
                setConnected(true);
              }}
            >
              <div>
                <h2>Connect to your broker</h2>
                <p>
                  Enter MINIRABBIT_TOKEN. The token stays in this page’s memory.
                </p>
              </div>
              <label className="sr-only" htmlFor="token">
                Broker token
              </label>
              <input
                id="token"
                type="password"
                autoComplete="off"
                minLength={16}
                required
                value={token}
                onChange={(e) => setToken(e.target.value)}
                placeholder="Management bearer token"
              />
              <button className="button primary">Connect →</button>
            </form>
          )}
          {error && (
            <div role="alert" className="error">
              {error} <button onClick={disconnect}>Reconnect</button>
            </div>
          )}
          {connected && !data && !error && (
            <div className="empty panel">Connecting to broker…</div>
          )}
          {!connected && !data && (
            <div className="welcome panel">
              <span className="welcome-icon">⇄</span>
              <h2>Your infrastructure, in view.</h2>
              <p>
                Connect to see live routing, delivery, and storage activity.
                <br />
                Operational data appears when the broker responds.
              </p>
              <div className="feature-row">
                <span>01 &nbsp; Publish</span>
                <span>02 &nbsp; Route</span>
                <span>03 &nbsp; Deliver</span>
                <span>04 &nbsp; Acknowledge</span>
              </div>
            </div>
          )}
          {data && (
            <>
              {page === "Overview" && (
                <>
                  <div className="metric-grid">
                    {[
                      [
                        "Messages published",
                        fmt(data.stats.published),
                        "Accepted this process",
                      ],
                      [
                        "Publish rate",
                        rate === null ? "—" : rate.toFixed(1) + "/s",
                        "Published over last interval",
                      ],
                      [
                        "Queue depth",
                        fmt(data.stats.queueDepth),
                        data.stats.unacked + " in flight",
                      ],
                      [
                        "Active consumers",
                        fmt(data.stats.activeConsumers),
                        "Registered workers",
                      ],
                    ].map(([title, value, sub], i) => (
                      <article className="metric panel" key={title}>
                        <div className="metric-title">
                          {title}
                          <span>{["↗", "⌁", "▤", "♧"][i]}</span>
                        </div>
                        <strong>{value}</strong>
                        <small>{sub}</small>
                      </article>
                    ))}
                  </div>
                  <section className="panel chart-panel">
                    <div className="section-heading">
                      <div>
                        <h2>Message activity</h2>
                        <p>Publish rate · messages per second</p>
                      </div>
                      <span className="live">
                        <span className="dot" /> LIVE · 2.5s
                      </span>
                    </div>
                    <div
                      className="chart"
                      role="img"
                      aria-label="Publish rate history"
                    >
                      <div className="chart-scale">
                        <span>{max.toFixed(1)}</span>
                        <span>{(max / 2).toFixed(1)}</span>
                        <span>0</span>
                      </div>
                      <svg viewBox="0 0 900 180" preserveAspectRatio="none">
                        <defs>
                          <linearGradient id="fill" x1="0" y1="0" x2="0" y2="1">
                            <stop
                              offset="0"
                              stopColor="#d4f582"
                              stopOpacity=".25"
                            />
                            <stop
                              offset="1"
                              stopColor="#d4f582"
                              stopOpacity="0"
                            />
                          </linearGradient>
                        </defs>
                        {[25, 92, 160].map((y) => (
                          <line
                            key={y}
                            x1="0"
                            y1={y}
                            x2="900"
                            y2={y}
                            stroke="#e9ece9"
                            strokeDasharray="4 5"
                          />
                        ))}
                        {history.length > 1 && (
                          <>
                            <path
                              d={path + " L 900 180 L 0 180 Z"}
                              fill="url(#fill)"
                            />
                            <path
                              d={path}
                              fill="none"
                              stroke="#49753b"
                              strokeWidth="2.5"
                            />
                          </>
                        )}
                      </svg>
                    </div>
                    <div className="chart-foot">
                      <span>Last {history.length} observations</span>
                      <span>
                        <i /> Published
                      </span>
                      <span>Now</span>
                    </div>
                  </section>
                  <div className="bottom-grid">
                    <QueueTable queues={data.queues} compact />
                    <section className="panel health">
                      <div className="section-heading">
                        <h2>Broker health</h2>
                        <span className="pill">
                          {error ? "Unavailable" : "Online"}
                        </span>
                      </div>
                      {[
                        ["Acknowledged", fmt(data.stats.acked)],
                        ["Redelivered", fmt(data.stats.redelivered)],
                        ["Dead-lettered", fmt(data.stats.deadLettered)],
                        [
                          "Persistence",
                          data.stats.fsyncPolicy === "always"
                            ? "Fsync always"
                            : "No fsync",
                        ],
                      ].map(([k, v]) => (
                        <div className="health-row" key={k}>
                          <span>{k}</span>
                          <b>{v}</b>
                        </div>
                      ))}
                      <p className="health-note">
                        One broker. No replication or automatic failover.
                      </p>
                    </section>
                  </div>
                </>
              )}
              {page === "Queues" && <QueueTable queues={data.queues} />}
              {page === "Exchanges" && (
                <section className="panel">
                  <div className="section-heading">
                    <h2>Exchange registry</h2>
                    <span>{data.exchanges.length} exchanges</span>
                  </div>
                  <div className="table-wrap">
                    <table>
                      <thead>
                        <tr>
                          <th>Name</th>
                          <th>Type</th>
                          <th>Durability</th>
                          <th>Bindings / routing</th>
                        </tr>
                      </thead>
                      <tbody>
                        {data.exchanges.map((e) => (
                          <tr key={e.name}>
                            <td className="mono">{e.name}</td>
                            <td>
                              <span className="pill neutral">{e.type}</span>
                            </td>
                            <td>{e.durable ? "Durable" : "Transient"}</td>
                            <td>
                              {data.bindings
                                .filter((b) => b.exchange === e.name)
                                .map((b) => (
                                  <div key={b.queue + b.key}>
                                    <code>{b.key || "(empty)"}</code> →{" "}
                                    {b.queue}
                                  </div>
                                ))}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                  {!data.exchanges.length && (
                    <Empty text="Declare an exchange with the CLI or SDK." />
                  )}
                </section>
              )}
              {page === "Consumers" && (
                <section className="panel">
                  <div className="section-heading">
                    <h2>Registered consumers</h2>
                    <span>{data.consumers.length} active</span>
                  </div>
                  <div className="table-wrap">
                    <table>
                      <thead>
                        <tr>
                          <th>Consumer</th>
                          <th>Queue</th>
                          <th>Prefetch</th>
                          <th>Unacked</th>
                          <th>Status</th>
                        </tr>
                      </thead>
                      <tbody>
                        {data.consumers.map((c) => (
                          <tr key={c.id}>
                            <td className="mono">{c.id}</td>
                            <td>{c.queue}</td>
                            <td>{c.prefetch}</td>
                            <td>{c.unacked}</td>
                            <td>
                              <span className="pill">{c.status}</span>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                  {!data.consumers.length && (
                    <Empty text="No consumers registered. Start a worker to receive messages." />
                  )}
                </section>
              )}
              {page === "Storage" && (
                <>
                  <div className="metric-grid">
                    {[
                      [
                        "WAL size",
                        (data.stats.storageBytes / 1048576).toFixed(2) + " MiB",
                      ],
                      [
                        "Persistent messages",
                        fmt(data.stats.persistentMessages),
                      ],
                      ["Compactions", fmt(data.stats.compactions)],
                      [
                        "Recovery duration",
                        data.stats.recoverySeconds.toFixed(3) + " s",
                      ],
                    ].map(([k, v]) => (
                      <article className="panel metric" key={k}>
                        <div className="metric-title">{k}</div>
                        <strong>{v}</strong>
                      </article>
                    ))}
                  </div>
                  <section className="panel storage">
                    <h2>Write-ahead log</h2>
                    <p>
                      Each checksummed transaction records the complete durable
                      state. Compaction writes and verifies a replacement before
                      atomic publication. Storage operations briefly pause the
                      broker engine.
                    </p>
                    <div className="health-row">
                      <span>Fsync policy</span>
                      <b>{data.stats.fsyncPolicy}</b>
                    </div>
                    <button
                      disabled={busy}
                      className="button primary"
                      onClick={() =>
                        void action(async () => {
                          await api("storage/compact", "POST", {});
                        })
                      }
                    >
                      {busy ? "Compacting…" : "Compact WAL"}
                    </button>
                  </section>
                </>
              )}
              {page === "Dead Letters" && (
                <section className="panel storage">
                  <h2>Inspect failed messages</h2>
                  <p>
                    Destinations are derived from configured dead-letter
                    exchanges and bindings. Inspection leases one message for 30
                    seconds by default. An expired lease safely returns it to
                    the queue.
                  </p>
                  <div className="inline-form">
                    <label htmlFor="dlq">Queue</label>
                    <select
                      id="dlq"
                      value={dlq}
                      disabled={!!inspected}
                      onChange={(e) => setDlq(e.target.value)}
                    >
                      <option value="">Select dead-letter destination</option>
                      {dlqNames.map((n) => (
                        <option key={n}>{n}</option>
                      ))}
                    </select>
                    <button
                      className="button"
                      disabled={busy || !dlq || !!inspected}
                      onClick={() =>
                        void action(async () => {
                          const d = await api<Delivery | null>(
                            "queues/" + encodeURIComponent(dlq) + "/consume",
                            "POST",
                            {
                              consumerId: consumer.current,
                              prefetch: 1,
                              waitMs: 0,
                            },
                          );
                          setInspected(d);
                          if (!d)
                            throw Error("No ready dead letters in this queue");
                        })
                      }
                    >
                      Inspect next
                    </button>
                  </div>
                  {inspected && (
                    <div className="dead-detail">
                      <dl>
                        <dt>Message</dt>
                        <dd className="mono">{inspected.message.id}</dd>
                        <dt>Failure reason</dt>
                        <dd>
                          {inspected.message.properties.headers[
                            "x-failure-reason"
                          ] || "No reason header"}
                        </dd>
                        <dt>Original routing key</dt>
                        <dd>
                          {
                            inspected.message.properties.headers[
                              "x-original-routing-key"
                            ]
                          }
                        </dd>
                        <dt>Attempts before dead lettering</dt>
                        <dd>
                          {inspected.message.properties.headers["x-attempts"]}
                        </dd>
                      </dl>
                      <p>
                        Replay publishes to the original exchange, then
                        acknowledges this dead letter. A failure between those
                        steps can create a duplicate.
                      </p>
                      <div className="inline-form">
                        <button
                          className="button primary"
                          disabled={busy}
                          onClick={() =>
                            void action(async () => {
                              const d = inspected,
                                h = d.message.properties.headers;
                              await api(
                                "deliveries/" + d.token + "/renew",
                                "POST",
                                { consumerId: d.consumerId, requeue: false },
                              );
                              const props = {
                                contentType: "application/octet-stream",
                                persistent: true,
                                ttlMs: 0,
                                delayMs: 0,
                                priority: 0,
                                headers: {},
                              };
                              const confirm = await api<{ queues: number }>(
                                "messages/publish",
                                "POST",
                                {
                                  exchange: h["x-original-exchange"],
                                  routingKey: h["x-original-routing-key"],
                                  payload: d.message.payload,
                                  properties: props,
                                  mandatory: true,
                                },
                              );
                              if (!confirm.queues)
                                throw Error(
                                  "Original route is unroutable; dead letter retained",
                                );
                              await api(
                                "deliveries/" + d.token + "/ack",
                                "POST",
                                { consumerId: d.consumerId, requeue: false },
                              );
                              setInspected(null);
                              void api(
                                "consumers/" + d.consumerId,
                                "DELETE",
                              ).catch(() => {});
                              consumer.current =
                                "dashboard-" + crypto.randomUUID();
                            })
                          }
                        >
                          Replay message
                        </button>
                        <button
                          className="button"
                          disabled={busy}
                          onClick={() =>
                            void action(async () => {
                              try {
                                await api(
                                  "consumers/" + consumer.current,
                                  "DELETE",
                                );
                              } finally {
                                consumer.current =
                                  "dashboard-" + crypto.randomUUID();
                                setInspected(null);
                              }
                            })
                          }
                        >
                          Return to queue
                        </button>
                      </div>
                    </div>
                  )}
                  {!dlqNames.length && (
                    <Empty text="No dead-letter destinations configured." />
                  )}
                </section>
              )}
            </>
          )}
          <footer>
            <span>
              <span className="dot" />{" "}
              {data ? "Live broker data" : "Awaiting connection"}
            </span>
            <span>
              {last
                ? "Last updated " + last
                : "Mini RabbitMQ · Built from scratch in Java"}
            </span>
            {connected && <button onClick={disconnect}>Disconnect</button>}
          </footer>
        </div>
      </main>
    </div>
  );
}
function Empty({ text }: { text: string }) {
  return <div className="empty">{text}</div>;
}
function QueueTable({
  queues,
  compact = false,
}: {
  queues: Queue[];
  compact?: boolean;
}) {
  return (
    <section className="panel">
      <div className="section-heading">
        <div>
          <h2>{compact ? "Queue activity" : "Queue registry"}</h2>
          <p>{queues.length} queues in this broker</p>
        </div>
        <span className="small-label">READY / IN FLIGHT</span>
      </div>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Queue name</th>
              <th>Ready</th>
              <th>Unacked</th>
              <th>Workers</th>
              {!compact && (
                <>
                  <th>Scheduled</th>
                  <th>Configuration</th>
                </>
              )}
            </tr>
          </thead>
          <tbody>
            {queues.map((q) => (
              <tr key={q.definition.name}>
                <td>
                  <span className="queue-icon">▤</span>
                  <span className="mono">{q.definition.name}</span>
                </td>
                <td>{fmt(q.ready)}</td>
                <td>{fmt(q.unacked)}</td>
                <td>{q.consumers}</td>
                {!compact && (
                  <>
                    <td>{q.scheduled}</td>
                    <td>
                      {q.definition.durable ? "Durable" : "Transient"} ·{" "}
                      {q.definition.priorities ? "Priority" : "FIFO"} ·{" "}
                      {q.definition.retry.maxAttempts} attempts
                      {q.definition.deadLetter && (
                        <div>DLX: {q.definition.deadLetter.exchange}</div>
                      )}
                    </td>
                  </>
                )}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {!queues.length && (
        <Empty text="No queues yet. Declare one with the CLI or SDK." />
      )}
    </section>
  );
}
createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
