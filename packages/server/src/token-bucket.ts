export class TokenBucket {
  private tokens: number;
  private updatedAt: number;

  constructor(
    readonly ratePerSec: number,
    readonly burst: number,
    now: number = performance.now(),
  ) {
    this.tokens = burst;
    this.updatedAt = now;
  }

  take(cost = 1, now: number = performance.now()): boolean {
    const elapsed = now - this.updatedAt;
    if (elapsed > 0) {
      this.tokens = Math.min(this.burst, this.tokens + (elapsed * this.ratePerSec) / 1000);
      this.updatedAt = now;
    }
    if (this.tokens >= cost) {
      this.tokens -= cost;
      return true;
    }
    return false;
  }

  available(now: number = performance.now()): number {
    const elapsed = Math.max(0, now - this.updatedAt);
    return Math.min(this.burst, this.tokens + (elapsed * this.ratePerSec) / 1000);
  }
}

export class ViolationCounter {
  private count = 0;
  private windowStart: number;

  constructor(
    readonly limit: number,
    readonly windowMs: number,
    now: number = performance.now(),
  ) {
    this.windowStart = now;
  }

  hit(now: number = performance.now()): boolean {
    if (now - this.windowStart > this.windowMs) {
      this.windowStart = now;
      this.count = 0;
    }
    this.count++;
    return this.count >= this.limit;
  }
}
