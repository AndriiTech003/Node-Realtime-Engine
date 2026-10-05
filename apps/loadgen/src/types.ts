import type { HistogramData } from "./histogram.js";

export interface Target {
  ws: string;
  http: string;
}

export interface ClientSpec {
  id: number;
  uid: string;
  rooms: string[];
  target: number;
  presence: boolean;
  publishRoom?: string;
  publishIntervalMs?: number;
  publishBytes?: number;
  cursorHz?: number;
  slow?: boolean;
  chaos?: { minOnMs: number; maxOnMs: number; minOffMs: number; maxOffMs: number };
  group?: string;
}

export interface WorkerConfig {
  workerId: number;
  targets: Target[];
  codec: "json" | "msgpack";
  deflate: boolean;
  jitter: boolean;
  backoffBaseMs: number;
  backoffMaxMs: number;
  jwtSecret: string;
  connectRatePerSec: number;
  ticketConcurrency: number;
  t0: number;
  bucketMs: number;
}

export type MainToWorker =
  | { cmd: "init"; config: WorkerConfig; clients: ClientSpec[] }
  | { cmd: "connect" }
  | { cmd: "start" }
  | { cmd: "stop" }
  | { cmd: "report"; reset: boolean }
  | { cmd: "close" };

export interface TimelineBucket {
  attempts: number;
  opened: number;
  failed: number;
  ticketErrors: number;
}

export interface WorkerReport {
  workerId: number;
  connected: number;
  everConnected: number;
  connecting: number;
  latency: Record<string, HistogramData>;
  ticketLatency: HistogramData;
  connectLatency: HistogramData;
  received: number;
  ephReceived: number;
  published: number;
  publishErrors: number;
  gaps: number;
  duplicates: number;
  resets: number;
  resumed: number;
  reconnects: number;
  connectErrors: number;
  ticketErrors: number;
  closeCodes: Record<string, number>;
  timeline: Record<string, TimelineBucket>;
  memory: { rss: number; heapUsed: number };
}

export type WorkerToMain = { evt: "report"; report: WorkerReport } | { evt: "ready" } | { evt: "closed" } | { evt: "error"; message: string };
