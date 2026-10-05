import { PerformanceObserver, constants as perfConstants, monitorEventLoopDelay, performance } from "node:perf_hooks";
import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from "prom-client";

export interface ConnectionStats {
  active: number;
  lagging: number;
  subscriptions: number;
  channels: number;
  pending: number;
}

const gcKinds: Record<number, string> = {
  [perfConstants.NODE_PERFORMANCE_GC_MAJOR]: "major",
  [perfConstants.NODE_PERFORMANCE_GC_MINOR]: "minor",
  [perfConstants.NODE_PERFORMANCE_GC_INCREMENTAL]: "incremental",
  [perfConstants.NODE_PERFORMANCE_GC_WEAKCB]: "weakcb",
};

export class Metrics {
  readonly registry = new Registry();
  readonly connections: Gauge<"state">;
  readonly subscriptions: Gauge;
  readonly channelsLocal: Gauge;
  readonly pendingMessages: Gauge;
  readonly messagesIn: Counter<"t">;
  readonly messagesOut: Counter<"kind">;
  readonly fanoutDuration: Histogram;
  readonly wsBufferedBytes: Histogram;
  readonly slowConsumerDisconnects: Counter;
  readonly ephemeralDropped: Counter;
  readonly resumeMessages: Histogram;
  readonly redisCommandSeconds: Histogram<"cmd">;
  readonly eventLoopUtilization: Gauge;
  readonly eventLoopLag: Gauge<"quantile">;
  private readonly loopDelay: ReturnType<typeof monitorEventLoopDelay>;
  readonly gcPause: Histogram<"kind">;
  readonly upgrades: Counter<"result">;
  readonly heartbeatTerminations: Counter;
  readonly rateLimited: Counter<"bucket">;
  readonly resets: Counter<"reason">;
  readonly closes: Counter<"code">;
  readonly publishes: Counter<"result">;

  readonly outDurable;
  readonly outEphemeral;
  readonly outPresence;
  readonly outControl;
  readonly outHistory;

  private gcObserver: PerformanceObserver | null = null;

  constructor(stats: () => ConnectionStats, labels: Record<string, string>) {
    const registry = this.registry;
    registry.setDefaultLabels(labels);
    collectDefaultMetrics({ register: registry, eventLoopMonitoringPrecision: 10 });

    this.connections = new Gauge({
      name: "rt_connections",
      help: "Open WebSocket connections by state",
      labelNames: ["state"],
      registers: [registry],
      collect() {
        const s = stats();
        this.set({ state: "active" }, s.active);
        this.set({ state: "lagging" }, s.lagging);
      },
    });
    this.subscriptions = new Gauge({
      name: "rt_subscriptions",
      help: "Channel subscriptions held by local connections",
      registers: [registry],
      collect() {
        this.set(stats().subscriptions);
      },
    });
    this.channelsLocal = new Gauge({
      name: "rt_channels_local",
      help: "Channels with at least one local subscriber",
      registers: [registry],
      collect() {
        this.set(stats().channels);
      },
    });
    this.pendingMessages = new Gauge({
      name: "rt_pending_messages",
      help: "Durable messages queued in per-connection pending queues",
      registers: [registry],
      collect() {
        this.set(stats().pending);
      },
    });
    this.messagesIn = new Counter({
      name: "rt_messages_in_total",
      help: "Frames received from clients by type",
      labelNames: ["t"],
      registers: [registry],
    });
    this.messagesOut = new Counter({
      name: "rt_messages_out_total",
      help: "Frames sent to clients by kind",
      labelNames: ["kind"],
      registers: [registry],
    });
    this.outDurable = this.messagesOut.labels("durable");
    this.outEphemeral = this.messagesOut.labels("ephemeral");
    this.outPresence = this.messagesOut.labels("presence");
    this.outControl = this.messagesOut.labels("control");
    this.outHistory = this.messagesOut.labels("history");
    this.fanoutDuration = new Histogram({
      name: "rt_fanout_duration_seconds",
      help: "Time from receiving a message from Redis to handing it to the last local subscriber",
      buckets: [0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1],
      registers: [registry],
    });
    this.wsBufferedBytes = new Histogram({
      name: "rt_ws_buffered_bytes",
      help: "Sampled ws.bufferedAmount per connection",
      buckets: [0, 1024, 16384, 65536, 262144, 1048576, 4194304],
      registers: [registry],
    });
    this.slowConsumerDisconnects = new Counter({
      name: "rt_slow_consumer_disconnects_total",
      help: "Connections closed with 4008 because of backpressure",
      registers: [registry],
    });
    this.ephemeralDropped = new Counter({
      name: "rt_ephemeral_dropped_total",
      help: "Ephemeral frames dropped because of backpressure",
      registers: [registry],
    });
    this.resumeMessages = new Histogram({
      name: "rt_resume_messages",
      help: "Messages replayed from history on subscribe with from",
      buckets: [0, 1, 5, 10, 50, 100, 500, 1000, 5000],
      registers: [registry],
    });
    this.redisCommandSeconds = new Histogram({
      name: "rt_redis_command_seconds",
      help: "Redis command latency",
      labelNames: ["cmd"],
      buckets: [0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5],
      registers: [registry],
    });
    const elu = { last: performance.eventLoopUtilization(), value: 0 };
    this.eventLoopUtilization = new Gauge({
      name: "rt_event_loop_utilization",
      help: "Event loop utilization since the previous scrape (0..1)",
      registers: [registry],
      collect() {
        const now = performance.eventLoopUtilization();
        const delta = performance.eventLoopUtilization(now, elu.last);
        if (delta.idle + delta.active >= 250) {
          elu.last = now;
          elu.value = delta.utilization;
        }
        this.set(elu.value);
      },
    });
    const resolutionMs = 10;
    const loopDelay = monitorEventLoopDelay({ resolution: resolutionMs });
    loopDelay.enable();
    this.loopDelay = loopDelay;
    this.eventLoopLag = new Gauge({
      name: "rt_event_loop_lag_seconds",
      help: "Event loop delay since the previous scrape with the 10 ms sampling interval subtracted",
      labelNames: ["quantile"],
      registers: [registry],
      collect() {
        const corrected = (ns: number) => Math.max(0, ns / 1e9 - resolutionMs / 1000);
        this.set({ quantile: "0.5" }, corrected(loopDelay.percentile(50)));
        this.set({ quantile: "0.99" }, corrected(loopDelay.percentile(99)));
        this.set({ quantile: "1" }, corrected(loopDelay.max));
        loopDelay.reset();
      },
    });
    this.gcPause = new Histogram({
      name: "rt_gc_pause_seconds",
      help: "Garbage collection pauses by kind",
      labelNames: ["kind"],
      buckets: [0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25],
      registers: [registry],
    });
    this.upgrades = new Counter({
      name: "rt_upgrades_total",
      help: "WebSocket upgrade attempts by result",
      labelNames: ["result"],
      registers: [registry],
    });
    this.heartbeatTerminations = new Counter({
      name: "rt_heartbeat_terminations_total",
      help: "Connections terminated because pong did not arrive in time",
      registers: [registry],
    });
    this.rateLimited = new Counter({
      name: "rt_rate_limited_total",
      help: "Frames rejected by token buckets",
      labelNames: ["bucket"],
      registers: [registry],
    });
    this.resets = new Counter({
      name: "rt_resets_total",
      help: "reset frames sent on subscribe",
      labelNames: ["reason"],
      registers: [registry],
    });
    this.closes = new Counter({
      name: "rt_closes_total",
      help: "Connection closes by code",
      labelNames: ["code"],
      registers: [registry],
    });
    this.publishes = new Counter({
      name: "rt_publishes_total",
      help: "Durable publishes by result",
      labelNames: ["result"],
      registers: [registry],
    });
    for (const result of ["ok", "unauthorized", "user_limit", "shed", "draining", "bad_origin", "not_found", "aborted"]) {
      this.upgrades.inc({ result }, 0);
    }
    for (const t of ["sub", "unsub", "pub", "eph", "pres", "ping"]) this.messagesIn.inc({ t }, 0);
    for (const kind of ["durable", "ephemeral", "presence", "control", "history"]) this.messagesOut.inc({ kind }, 0);
    for (const result of ["ok", "duplicate"]) this.publishes.inc({ result }, 0);
    for (const bucket of ["pub", "eph", "ctl"]) this.rateLimited.inc({ bucket }, 0);
  }

  startGcObserver(): void {
    if (this.gcObserver !== null) return;
    const histogram = this.gcPause;
    this.gcObserver = new PerformanceObserver((list) => {
      for (const entry of list.getEntries()) {
        const detail = (entry as unknown as { detail?: { kind?: number } }).detail;
        const kind = gcKinds[detail?.kind ?? -1] ?? "other";
        histogram.observe({ kind }, entry.duration / 1000);
      }
    });
    this.gcObserver.observe({ entryTypes: ["gc"] });
  }

  stop(): void {
    this.loopDelay.disable();
    this.gcObserver?.disconnect();
    this.gcObserver = null;
  }

  async timeRedis<T>(cmd: string, run: () => Promise<T>): Promise<T> {
    const end = this.redisCommandSeconds.startTimer({ cmd });
    try {
      return await run();
    } finally {
      end();
    }
  }
}
