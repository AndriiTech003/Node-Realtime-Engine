import { createHash, randomBytes } from "node:crypto";
import { Redis } from "ioredis";
import type { Keys } from "./keys.js";
import type { Metrics } from "./metrics.js";
import {
  PRESENCE_JOIN_SCRIPT,
  PRESENCE_LEAVE_SCRIPT,
  PRESENCE_LIST_SCRIPT,
  PRESENCE_UPDATE_SCRIPT,
  PUBLISH_SCRIPT,
  RELEASE_LOCK_SCRIPT,
  USER_ACQUIRE_SCRIPT,
  USER_RELEASE_SCRIPT,
} from "./lua.js";

type RedisValue = string | number;

class LuaScript {
  readonly sha: string;
  constructor(readonly source: string) {
    this.sha = createHash("sha1").update(source).digest("hex");
  }

  async run(redis: Redis, keys: string[], args: RedisValue[]): Promise<unknown> {
    try {
      return await redis.evalsha(this.sha, keys.length, ...keys, ...args);
    } catch (error) {
      if (error instanceof Error && error.message.includes("NOSCRIPT")) {
        return redis.eval(this.source, keys.length, ...keys, ...args);
      }
      throw error;
    }
  }
}

const scripts = {
  publish: new LuaScript(PUBLISH_SCRIPT),
  presenceJoin: new LuaScript(PRESENCE_JOIN_SCRIPT),
  presenceLeave: new LuaScript(PRESENCE_LEAVE_SCRIPT),
  presenceUpdate: new LuaScript(PRESENCE_UPDATE_SCRIPT),
  presenceList: new LuaScript(PRESENCE_LIST_SCRIPT),
  releaseLock: new LuaScript(RELEASE_LOCK_SCRIPT),
  userAcquire: new LuaScript(USER_ACQUIRE_SCRIPT),
  userRelease: new LuaScript(USER_RELEASE_SCRIPT),
};

export interface PublishResult {
  seq: number;
  dup: boolean;
  mid: string;
}

export interface HistoryEntry {
  seq: number;
  ts: number;
  payload: string;
}

export interface PresenceEntry {
  uid: string;
  meta?: unknown;
  node: string;
  at: number;
}

export interface PublishOptions {
  ch: string;
  payload: string;
  ts: number;
  cmid: string;
  mid: string;
  maxLen: number;
  retentionCutoff: number;
  cmidTtlSec: number;
}

export type ShardMessageHandler = (channel: string, message: string) => void;

export class RedisStore {
  readonly cmd: Redis;
  readonly sub: Redis;
  private handler: ShardMessageHandler | null = null;

  constructor(
    url: string,
    readonly keys: Keys,
    private readonly metrics: Metrics,
  ) {
    const options = { maxRetriesPerRequest: 3, enableAutoPipelining: true, lazyConnect: true };
    this.cmd = new Redis(url, { ...options, connectionName: "rt-cmd" });
    this.sub = new Redis(url, { lazyConnect: true, connectionName: "rt-sub" });
    this.sub.on("smessage", (channel: string, message: string) => {
      this.handler?.(channel, message);
    });
  }

  async connect(): Promise<void> {
    await Promise.all([this.cmd.connect(), this.sub.connect()]);
    await Promise.all(Object.values(scripts).map((script) => this.cmd.script("LOAD", script.source)));
  }

  onShardMessage(handler: ShardMessageHandler): void {
    this.handler = handler;
  }

  async quit(): Promise<void> {
    await Promise.allSettled([this.cmd.quit(), this.sub.quit()]);
  }

  disconnectNow(): void {
    this.cmd.disconnect();
    this.sub.disconnect();
  }

  async ssubscribe(fan: string): Promise<void> {
    await this.metrics.timeRedis("ssubscribe", () => this.sub.ssubscribe(fan));
  }

  async sunsubscribe(fan: string): Promise<void> {
    await this.metrics.timeRedis("sunsubscribe", () => this.sub.sunsubscribe(fan));
  }

  async publish(options: PublishOptions): Promise<PublishResult> {
    const { ch } = options;
    const result = (await this.metrics.timeRedis("publish", () =>
      scripts.publish.run(
        this.cmd,
        [this.keys.seq(ch), this.keys.log(ch), this.keys.cmid(ch, options.cmid)],
        [
          options.payload,
          options.ts,
          this.keys.fan(ch),
          options.maxLen,
          options.retentionCutoff,
          options.cmidTtlSec,
          options.mid,
        ],
      ),
    )) as [number, number, string];
    return { seq: Number(result[0]), dup: Number(result[1]) === 1, mid: String(result[2]) };
  }

  async headSeq(ch: string): Promise<number> {
    const raw = await this.metrics.timeRedis("get", () => this.cmd.get(this.keys.seq(ch)));
    return raw === null ? 0 : Number(raw);
  }

  async readRange(ch: string, fromSeq: number, toSeq: number, count: number): Promise<HistoryEntry[]> {
    const raw = (await this.metrics.timeRedis("xrange", () =>
      this.cmd.xrange(this.keys.log(ch), `0-${fromSeq}`, `0-${toSeq}`, "COUNT", count),
    )) as [string, string[]][];
    const out: HistoryEntry[] = [];
    for (const [id, fields] of raw) {
      let payload = "";
      let ts = 0;
      for (let i = 0; i + 1 < fields.length; i += 2) {
        if (fields[i] === "p") payload = fields[i + 1] as string;
        else if (fields[i] === "ts") ts = Number(fields[i + 1]);
      }
      out.push({ seq: Number(id.slice(2)), ts, payload });
    }
    return out;
  }

  async firstSeq(ch: string): Promise<number | null> {
    const raw = (await this.cmd.xrange(this.keys.log(ch), "-", "+", "COUNT", 1)) as [string, string[]][];
    const first = raw[0];
    return first === undefined ? null : Number(first[0].slice(2));
  }

  async presenceJoin(
    ch: string,
    connId: string,
    uid: string,
    entry: string,
    expiresAt: number,
    joinFrame: string,
    nodeId: string,
  ): Promise<number> {
    const result = await this.metrics.timeRedis("presence_join", () =>
      scripts.presenceJoin.run(
        this.cmd,
        [this.keys.pres(ch), this.keys.presExp(ch), this.keys.presUsers(ch)],
        [connId, uid, entry, expiresAt, this.keys.fan(ch), joinFrame],
      ),
    );
    await this.cmd.sadd(this.keys.presIndex(), ch);
    await this.cmd.sadd(this.keys.nodePresence(nodeId), `${ch}\n${connId}`);
    return Number(result);
  }

  async presenceLeave(ch: string, connId: string, nodeId: string): Promise<number> {
    const result = await this.metrics.timeRedis("presence_leave", () =>
      scripts.presenceLeave.run(
        this.cmd,
        [this.keys.pres(ch), this.keys.presExp(ch), this.keys.presUsers(ch)],
        [connId, this.keys.fan(ch), ch],
      ),
    );
    await this.cmd.srem(this.keys.nodePresence(nodeId), `${ch}\n${connId}`);
    return Number(result);
  }

  async presenceUpdate(ch: string, connId: string, entry: string, frame: string): Promise<boolean> {
    const result = await this.metrics.timeRedis("presence_update", () =>
      scripts.presenceUpdate.run(this.cmd, [this.keys.pres(ch)], [connId, entry, this.keys.fan(ch), frame]),
    );
    return Number(result) === 1;
  }

  async presenceList(ch: string, limit: number): Promise<{ total: number; entries: PresenceEntry[] }> {
    const result = (await this.metrics.timeRedis("presence_list", () =>
      scripts.presenceList.run(this.cmd, [this.keys.pres(ch), this.keys.presUsers(ch)], [limit]),
    )) as [number, string[]];
    return { total: Number(result[0]), entries: result[1].map((raw) => JSON.parse(raw) as PresenceEntry) };
  }

  async refreshPresence(byChannel: Map<string, string[]>, expiresAt: number): Promise<void> {
    if (byChannel.size === 0) return;
    const pipeline = this.cmd.pipeline();
    for (const [ch, connIds] of byChannel) {
      const args: RedisValue[] = [];
      for (const id of connIds) args.push(expiresAt, id);
      pipeline.zadd(this.keys.presExp(ch), ...args);
    }
    await this.metrics.timeRedis("presence_refresh", () => pipeline.exec());
  }

  async spublishMany(messages: [string, string][]): Promise<void> {
    if (messages.length === 0) return;
    const pipeline = this.cmd.pipeline();
    for (const [fan, message] of messages) pipeline.spublish(fan, message);
    await this.metrics.timeRedis("spublish", () => pipeline.exec());
  }

  async createTicket(value: string, ttlSec: number): Promise<string> {
    const ticket = randomBytes(32).toString("base64url");
    await this.metrics.timeRedis("ticket_set", () => this.cmd.set(this.keys.ticket(ticket), value, "EX", ttlSec));
    return ticket;
  }

  async consumeTicket(ticket: string): Promise<string | null> {
    return this.metrics.timeRedis("ticket_getdel", () => this.cmd.getdel(this.keys.ticket(ticket)));
  }

  async markAlive(nodeId: string, ttlMs: number): Promise<boolean> {
    return this.metrics.timeRedis("node_alive", async () => {
      await this.cmd.set(this.keys.nodeAlive(nodeId), String(Date.now()), "PX", ttlMs);
      return (await this.cmd.sadd(this.keys.nodes(), nodeId)) === 1;
    });
  }

  async removeNode(nodeId: string): Promise<void> {
    await this.clearNodeUsers(nodeId);
    await this.cmd.del(this.keys.nodeAlive(nodeId), this.keys.nodePresence(nodeId));
    await this.cmd.srem(this.keys.nodes(), nodeId);
  }

  trackNodeUser(nodeId: string, uid: string): void {
    this.cmd.sadd(this.keys.nodeUsers(nodeId), uid).catch(() => undefined);
  }

  untrackNodeUser(nodeId: string, uid: string): void {
    this.cmd.srem(this.keys.nodeUsers(nodeId), uid).catch(() => undefined);
  }

  async userAcquire(uid: string, nodeId: string, limit: number): Promise<{ ok: boolean; total: number }> {
    const result = (await this.metrics.timeRedis("user_acquire", () =>
      scripts.userAcquire.run(this.cmd, [this.keys.userConns(uid)], [nodeId, limit]),
    )) as [number, number];
    return { ok: Number(result[0]) === 1, total: Number(result[1]) };
  }

  async userRelease(uid: string, nodeId: string): Promise<number> {
    const result = await this.metrics.timeRedis("user_release", () =>
      scripts.userRelease.run(this.cmd, [this.keys.userConns(uid)], [nodeId]),
    );
    return Number(result);
  }

  async userConnections(uid: string): Promise<Record<string, number>> {
    const raw = await this.cmd.hgetall(this.keys.userConns(uid));
    const out: Record<string, number> = {};
    for (const [node, count] of Object.entries(raw)) out[node] = Number(count);
    return out;
  }

  async nodeUserIds(nodeId: string): Promise<string[]> {
    return this.cmd.smembers(this.keys.nodeUsers(nodeId));
  }

  async clearNodeUsers(nodeId: string): Promise<number> {
    const setKey = this.keys.nodeUsers(nodeId);
    let cleared = 0;
    let cursor = "0";
    do {
      const [next, uids] = await this.cmd.sscan(setKey, cursor, "COUNT", 500);
      cursor = next;
      if (uids.length === 0) continue;
      const pipeline = this.cmd.pipeline();
      for (const uid of uids) pipeline.hdel(this.keys.userConns(uid), nodeId);
      const results = await pipeline.exec();
      for (const [, removed] of results ?? []) cleared += Number(removed ?? 0);
    } while (cursor !== "0");
    await this.cmd.del(setKey);
    return cleared;
  }

  async resyncNodeUsers(nodeId: string, snapshot: () => Map<string, number> | null): Promise<boolean> {
    const members = await this.cmd.smembers(this.keys.nodeUsers(nodeId));
    const counts = snapshot();
    if (counts === null) return false;
    const pipeline = this.cmd.pipeline();
    for (const uid of members) {
      if (counts.has(uid)) continue;
      pipeline.hdel(this.keys.userConns(uid), nodeId);
      pipeline.srem(this.keys.nodeUsers(nodeId), uid);
    }
    for (const [uid, count] of counts) {
      pipeline.hset(this.keys.userConns(uid), nodeId, count);
      pipeline.sadd(this.keys.nodeUsers(nodeId), uid);
    }
    await this.metrics.timeRedis("user_resync", () => pipeline.exec());
    return true;
  }

  async acquireLock(key: string, token: string, ttlMs: number): Promise<boolean> {
    const result = await this.cmd.set(key, token, "PX", ttlMs, "NX");
    return result === "OK";
  }

  async releaseLock(key: string, token: string): Promise<void> {
    await scripts.releaseLock.run(this.cmd, [key], [token]);
  }

  async reapNode(node: string): Promise<number> {
    let removed = 0;
    await this.clearNodeUsers(node);
    const members = await this.cmd.smembers(this.keys.nodePresence(node));
    for (const member of members) {
      const sep = member.indexOf("\n");
      if (sep < 0) continue;
      const result = await this.presenceLeave(member.slice(0, sep), member.slice(sep + 1), node);
      if (result > 0) removed++;
    }
    await this.removeNode(node);
    return removed;
  }

  async sweepPresence(nowMs: number, selfNodeId: string, nodeId: string): Promise<{ removed: number; deadNodes: string[] }> {
    let removed = 0;
    const deadNodes: string[] = [];
    const nodes = await this.cmd.smembers(this.keys.nodes());
    for (const node of nodes) {
      if (node === selfNodeId) continue;
      const alive = await this.cmd.exists(this.keys.nodeAlive(node));
      if (alive === 1) continue;
      deadNodes.push(node);
      removed += await this.reapNode(node);
    }
    const channels = await this.cmd.smembers(this.keys.presIndex());
    for (const ch of channels) {
      const expired = await this.cmd.zrangebyscore(this.keys.presExp(ch), "-inf", nowMs, "LIMIT", 0, 500);
      for (const connId of expired) {
        const raw = await this.cmd.hget(this.keys.pres(ch), connId);
        const owner = raw === null ? nodeId : (JSON.parse(raw) as PresenceEntry).node;
        const result = await this.presenceLeave(ch, connId, owner);
        if (result > 0) removed++;
      }
      const size = await this.cmd.hlen(this.keys.pres(ch));
      if (size === 0) await this.cmd.srem(this.keys.presIndex(), ch);
    }
    return { removed, deadNodes };
  }
}
