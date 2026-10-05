import type { Connection } from "./connection.js";
import type { Keys } from "./keys.js";
import type { Metrics } from "./metrics.js";
import { parseFanMessage, type FanItem } from "./outbound.js";
import type { RedisStore } from "./redis.js";

export interface FanoutOptions {
  serializeOnce: boolean;
  chunkThreshold: number;
  chunkSize: number;
}

interface LocalChannel {
  name: string;
  fan: string;
  conns: Set<Connection>;
  ready: Promise<void>;
  queue: FanItem[];
  delivering: boolean;
}

export class ChannelRegistry {
  private readonly channels = new Map<string, LocalChannel>();
  private readonly byFan = new Map<string, LocalChannel>();

  constructor(
    private readonly store: RedisStore,
    private readonly keys: Keys,
    private readonly metrics: Metrics,
    private readonly options: FanoutOptions,
  ) {
    store.onShardMessage((fan, message) => this.onShardMessage(fan, message));
  }

  get size(): number {
    return this.channels.size;
  }

  subscribers(ch: string): number {
    return this.channels.get(ch)?.conns.size ?? 0;
  }

  names(): IterableIterator<string> {
    return this.channels.keys();
  }

  add(ch: string, conn: Connection): Promise<void> {
    let channel = this.channels.get(ch);
    if (channel === undefined) {
      const fan = this.keys.fan(ch);
      const created: LocalChannel = {
        name: ch,
        fan,
        conns: new Set(),
        ready: this.store.ssubscribe(fan),
        queue: [],
        delivering: false,
      };
      created.ready.catch(() => {
        if (this.channels.get(ch) === created && created.conns.size === 0) this.drop(created);
      });
      this.channels.set(ch, created);
      this.byFan.set(fan, created);
      channel = created;
    }
    channel.conns.add(conn);
    return channel.ready;
  }

  remove(ch: string, conn: Connection): void {
    const channel = this.channels.get(ch);
    if (channel === undefined) return;
    channel.conns.delete(conn);
    if (channel.conns.size === 0) this.drop(channel);
  }

  private drop(channel: LocalChannel): void {
    if (this.channels.get(channel.name) !== channel) return;
    this.channels.delete(channel.name);
    this.byFan.delete(channel.fan);
    channel.queue = [];
    this.store.sunsubscribe(channel.fan).catch(() => undefined);
  }

  onShardMessage(fan: string, message: string): void {
    const channel = this.byFan.get(fan);
    if (channel === undefined) return;
    const item = parseFanMessage(channel.name, message, this.options.serializeOnce, performance.now());
    if (item === null) return;
    channel.queue.push(item);
    if (!channel.delivering) this.pump(channel);
  }

  private pump(channel: LocalChannel): void {
    while (channel.queue.length > 0) {
      const item = channel.queue[0] as FanItem;
      const size = channel.conns.size;
      if (this.options.chunkThreshold > 0 && size > this.options.chunkThreshold) {
        channel.delivering = true;
        this.deliverChunked(channel, item, Array.from(channel.conns), 0);
        return;
      }
      channel.queue.shift();
      for (const conn of channel.conns) deliver(conn, item);
      this.metrics.fanoutDuration.observe((performance.now() - item.receivedAt) / 1000);
    }
    channel.delivering = false;
  }

  private deliverChunked(channel: LocalChannel, item: FanItem, conns: Connection[], start: number): void {
    const end = Math.min(conns.length, start + this.options.chunkSize);
    for (let i = start; i < end; i++) deliver(conns[i] as Connection, item);
    if (end < conns.length) {
      setImmediate(() => this.deliverChunked(channel, item, conns, end));
      return;
    }
    this.metrics.fanoutDuration.observe((performance.now() - item.receivedAt) / 1000);
    if (channel.queue[0] === item) channel.queue.shift();
    channel.delivering = false;
    if (channel.queue.length > 0) setImmediate(() => this.pump(channel));
  }
}

export function deliver(conn: Connection, item: FanItem): void {
  const sub = conn.subs.get(item.ch);
  if (sub === undefined) return;
  switch (item.kind) {
    case "durable":
      if (sub.state === "syncing") {
        sub.buffer.push(item);
        return;
      }
      if (item.seq <= sub.lastSeq) return;
      sub.lastSeq = item.seq;
      conn.sendFrame(item.frame, "durable", item.ch);
      return;
    case "presence":
      if (!sub.presence) return;
      if (sub.state === "syncing") {
        sub.buffer.push(item);
        return;
      }
      conn.sendFrame(item.frame, "presence", item.ch);
      return;
    case "ephemeral":
      if (sub.state === "syncing" || item.origin === conn.id) return;
      conn.sendFrame(item.frame, "ephemeral", item.ch);
      return;
  }
}
