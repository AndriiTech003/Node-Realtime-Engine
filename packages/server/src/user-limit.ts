import type { Logger } from "pino";
import type { RedisStore } from "./redis.js";

export class UserConnectionLimiter {
  private readonly held = new Map<string, number>();
  private inFlight = 0;
  private dirty = false;

  constructor(
    private readonly store: RedisStore,
    private readonly nodeId: string,
    private readonly limit: number,
    private readonly log: Logger,
  ) {}

  get enabled(): boolean {
    return this.limit > 0;
  }

  get needsResync(): boolean {
    return this.dirty;
  }

  localCount(uid: string): number {
    return this.held.get(uid) ?? 0;
  }

  async acquire(uid: string): Promise<boolean> {
    if (!this.enabled) return true;
    this.increment(uid);
    this.inFlight++;
    try {
      const result = await this.store.userAcquire(uid, this.nodeId, this.limit);
      if (!result.ok) this.decrement(uid);
      return result.ok;
    } catch (error) {
      this.decrement(uid);
      this.dirty = true;
      throw error;
    } finally {
      this.inFlight--;
    }
  }

  release(uid: string): void {
    if (!this.enabled) return;
    this.store.userRelease(uid, this.nodeId).catch((error: unknown) => {
      this.dirty = true;
      this.log.warn({ err: error, uid }, "user connection release failed");
    });
    this.decrement(uid);
  }

  async resync(): Promise<boolean> {
    if (!this.enabled) return true;
    this.dirty = true;
    const done = await this.store.resyncNodeUsers(this.nodeId, () => (this.inFlight > 0 ? null : new Map(this.held)));
    if (done) this.dirty = false;
    return done;
  }

  reset(): void {
    this.held.clear();
  }

  private increment(uid: string): void {
    const next = (this.held.get(uid) ?? 0) + 1;
    this.held.set(uid, next);
    if (next === 1) this.store.trackNodeUser(this.nodeId, uid);
  }

  private decrement(uid: string): void {
    const next = (this.held.get(uid) ?? 0) - 1;
    if (next > 0) {
      this.held.set(uid, next);
      return;
    }
    this.held.delete(uid);
    this.store.untrackNodeUser(this.nodeId, uid);
  }
}
