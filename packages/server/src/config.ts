import { hostname } from "node:os";
import {
  CMID_TTL_SECONDS,
  HISTORY_MAX_LEN,
  HISTORY_RETENTION_MS,
  MAX_CONNECTIONS_PER_USER,
  MAX_PAYLOAD_BYTES,
  MAX_SUBSCRIPTIONS,
  RESUME_LIMIT,
  TICKET_TTL_SECONDS,
  EPHEMERAL_COALESCE_MS,
} from "@ashamrai/realtime-protocol";

export type HeartbeatMode = "shared" | "per-connection";

export interface BackpressureConfig {
  lowBytes: number;
  highBytes: number;
  hardBytes: number;
  maxPending: number;
  lagTimeoutMs: number;
  drainTickMs: number;
}

export interface RateConfig {
  pubPerSec: number;
  pubBurst: number;
  ephPerSec: number;
  ephBurst: number;
  ctlPerSec: number;
  ctlBurst: number;
  violationsToClose: number;
  violationWindowMs: number;
}

export interface ServerConfig {
  nodeId: string;
  host: string;
  port: number;
  redisUrl: string;
  redisPrefix: string;
  jwtSecret: string;
  serverApiKey: string;
  allowedOrigins: string[];
  allowNoOrigin: boolean;
  maxConnections: number;
  maxConnectionsPerUser: number;
  maxSubscriptions: number;
  maxPayload: number;
  perMessageDeflate: boolean;
  heartbeatMode: HeartbeatMode;
  heartbeatIntervalMs: number;
  heartbeatTimeoutMs: number;
  presenceTtlMs: number;
  presenceRefreshMs: number;
  presenceSweepMs: number;
  presenceListLimit: number;
  nodeAliveTtlMs: number;
  nodeAliveRefreshMs: number;
  backpressure: BackpressureConfig;
  rate: RateConfig;
  fanoutSerializeOnce: boolean;
  fanoutChunkThreshold: number;
  fanoutChunkSize: number;
  historyMaxLen: number;
  historyRetentionMs: number;
  resumeLimit: number;
  historyPageSize: number;
  cmidTtlSec: number;
  ticketTtlSec: number;
  drainMaxDelayMs: number;
  drainCloseAfterMs: number;
  drainNotifyDelayMs: number;
  ephemeralCoalesceMs: number;
  bufferedSampleMs: number;
  logLevel: string;
}

export const defaultConfig: ServerConfig = {
  nodeId: `node-${hostname()}-${process.pid}`,
  host: "0.0.0.0",
  port: 4301,
  redisUrl: "redis://127.0.0.1:6379/3",
  redisPrefix: "rt:",
  jwtSecret: "dev-jwt-secret-change-me",
  serverApiKey: "dev-server-key-change-me",
  allowedOrigins: [
    "http://localhost:4320",
    "http://127.0.0.1:4320",
    "http://localhost:4321",
    "http://127.0.0.1:4321",
  ],
  allowNoOrigin: true,
  maxConnections: 20000,
  maxConnectionsPerUser: MAX_CONNECTIONS_PER_USER,
  maxSubscriptions: MAX_SUBSCRIPTIONS,
  maxPayload: MAX_PAYLOAD_BYTES,
  perMessageDeflate: false,
  heartbeatMode: "shared",
  heartbeatIntervalMs: 25000,
  heartbeatTimeoutMs: 10000,
  presenceTtlMs: 45000,
  presenceRefreshMs: 15000,
  presenceSweepMs: 10000,
  presenceListLimit: 1000,
  nodeAliveTtlMs: 15000,
  nodeAliveRefreshMs: 5000,
  backpressure: {
    lowBytes: 256 * 1024,
    highBytes: 1024 * 1024,
    hardBytes: 4 * 1024 * 1024,
    maxPending: 2000,
    lagTimeoutMs: 30000,
    drainTickMs: 50,
  },
  rate: {
    pubPerSec: 20,
    pubBurst: 40,
    ephPerSec: 60,
    ephBurst: 60,
    ctlPerSec: 50,
    ctlBurst: 200,
    violationsToClose: 50,
    violationWindowMs: 10000,
  },
  fanoutSerializeOnce: true,
  fanoutChunkThreshold: 1000,
  fanoutChunkSize: 500,
  historyMaxLen: HISTORY_MAX_LEN,
  historyRetentionMs: HISTORY_RETENTION_MS,
  resumeLimit: RESUME_LIMIT,
  historyPageSize: 1000,
  cmidTtlSec: CMID_TTL_SECONDS,
  ticketTtlSec: TICKET_TTL_SECONDS,
  drainMaxDelayMs: 10000,
  drainCloseAfterMs: 15000,
  drainNotifyDelayMs: 0,
  ephemeralCoalesceMs: EPHEMERAL_COALESCE_MS,
  bufferedSampleMs: 5000,
  logLevel: "info",
};

export type ConfigOverrides = Partial<Omit<ServerConfig, "backpressure" | "rate">> & {
  backpressure?: Partial<BackpressureConfig>;
  rate?: Partial<RateConfig>;
};

export function resolveConfig(overrides: ConfigOverrides = {}): ServerConfig {
  return {
    ...defaultConfig,
    ...overrides,
    backpressure: { ...defaultConfig.backpressure, ...overrides.backpressure },
    rate: { ...defaultConfig.rate, ...overrides.rate },
  };
}

type Env = Record<string, string | undefined>;

function envInt(env: Env, key: string): number | undefined {
  const raw = env[key];
  if (raw === undefined || raw === "") return undefined;
  const value = Number(raw);
  if (!Number.isFinite(value)) throw new Error(`${key} must be a number`);
  return value;
}

function envBool(env: Env, key: string): boolean | undefined {
  const raw = env[key];
  if (raw === undefined || raw === "") return undefined;
  return raw === "1" || raw.toLowerCase() === "true";
}

function envStr(env: Env, key: string): string | undefined {
  const raw = env[key];
  return raw === undefined || raw === "" ? undefined : raw;
}

function compact<T extends object>(value: T): Partial<T> {
  const out: Partial<T> = {};
  for (const [k, v] of Object.entries(value)) {
    if (v !== undefined) (out as Record<string, unknown>)[k] = v;
  }
  return out;
}

export function configFromEnv(env: Env = process.env): ServerConfig {
  const origins = envStr(env, "ALLOWED_ORIGINS");
  const mode = envStr(env, "HEARTBEAT_MODE");
  const overrides: ConfigOverrides = compact({
    nodeId: envStr(env, "NODE_ID"),
    host: envStr(env, "HOST"),
    port: envInt(env, "PORT"),
    redisUrl: envStr(env, "REDIS_URL"),
    redisPrefix: envStr(env, "REDIS_PREFIX"),
    jwtSecret: envStr(env, "JWT_SECRET"),
    serverApiKey: envStr(env, "SERVER_API_KEY"),
    allowedOrigins: origins === undefined ? undefined : origins.split(",").map((o) => o.trim()).filter(Boolean),
    allowNoOrigin: envBool(env, "ALLOW_NO_ORIGIN"),
    maxConnections: envInt(env, "MAX_CONNECTIONS"),
    maxConnectionsPerUser: envInt(env, "MAX_CONNECTIONS_PER_USER"),
    perMessageDeflate: envBool(env, "PERMESSAGE_DEFLATE"),
    heartbeatMode: mode === "per-connection" ? "per-connection" : mode === "shared" ? "shared" : undefined,
    heartbeatIntervalMs: envInt(env, "HEARTBEAT_INTERVAL_MS"),
    heartbeatTimeoutMs: envInt(env, "HEARTBEAT_TIMEOUT_MS"),
    presenceTtlMs: envInt(env, "PRESENCE_TTL_MS"),
    presenceRefreshMs: envInt(env, "PRESENCE_REFRESH_MS"),
    presenceSweepMs: envInt(env, "PRESENCE_SWEEP_MS"),
    nodeAliveTtlMs: envInt(env, "NODE_ALIVE_TTL_MS"),
    nodeAliveRefreshMs: envInt(env, "NODE_ALIVE_REFRESH_MS"),
    fanoutSerializeOnce: envBool(env, "FANOUT_SERIALIZE_ONCE"),
    fanoutChunkThreshold: envInt(env, "FANOUT_CHUNK_THRESHOLD"),
    fanoutChunkSize: envInt(env, "FANOUT_CHUNK_SIZE"),
    historyMaxLen: envInt(env, "HISTORY_MAX_LEN"),
    resumeLimit: envInt(env, "RESUME_LIMIT"),
    drainMaxDelayMs: envInt(env, "DRAIN_MAX_DELAY_MS"),
    drainCloseAfterMs: envInt(env, "DRAIN_CLOSE_AFTER_MS"),
    drainNotifyDelayMs: envInt(env, "DRAIN_NOTIFY_DELAY_MS"),
    logLevel: envStr(env, "LOG_LEVEL"),
  });
  const rate = compact({
    pubPerSec: envInt(env, "RATE_PUB_PER_SEC"),
    pubBurst: envInt(env, "RATE_PUB_BURST"),
    ephPerSec: envInt(env, "RATE_EPH_PER_SEC"),
    ephBurst: envInt(env, "RATE_EPH_BURST"),
  });
  const backpressure = compact({
    lowBytes: envInt(env, "BP_LOW_BYTES"),
    highBytes: envInt(env, "BP_HIGH_BYTES"),
    hardBytes: envInt(env, "BP_HARD_BYTES"),
    maxPending: envInt(env, "BP_MAX_PENDING"),
    lagTimeoutMs: envInt(env, "BP_LAG_TIMEOUT_MS"),
  });
  return resolveConfig({ ...overrides, rate, backpressure });
}
