const GROWTH = 1.02;
const MIN_VALUE = 0.01;
const BUCKETS = 1200;
const LOG_GROWTH = Math.log(GROWTH);

export interface HistogramData {
  counts: number[];
  count: number;
  sum: number;
  min: number;
  max: number;
}

export class Histogram {
  counts = new Array<number>(BUCKETS).fill(0);
  count = 0;
  sum = 0;
  min = Number.POSITIVE_INFINITY;
  max = 0;

  static index(value: number): number {
    if (value <= MIN_VALUE) return 0;
    const i = Math.floor(Math.log(value / MIN_VALUE) / LOG_GROWTH) + 1;
    return Math.min(BUCKETS - 1, i);
  }

  static upper(index: number): number {
    return index === 0 ? MIN_VALUE : MIN_VALUE * GROWTH ** index;
  }

  record(value: number): void {
    const v = value < 0 ? 0 : value;
    this.counts[Histogram.index(v)] = (this.counts[Histogram.index(v)] ?? 0) + 1;
    this.count++;
    this.sum += v;
    if (v < this.min) this.min = v;
    if (v > this.max) this.max = v;
  }

  merge(other: HistogramData): void {
    for (let i = 0; i < BUCKETS; i++) this.counts[i] = (this.counts[i] ?? 0) + (other.counts[i] ?? 0);
    this.count += other.count;
    this.sum += other.sum;
    if (other.count > 0) {
      this.min = Math.min(this.min, other.min);
      this.max = Math.max(this.max, other.max);
    }
  }

  percentile(p: number): number {
    if (this.count === 0) return 0;
    const target = Math.ceil((p / 100) * this.count);
    let seen = 0;
    for (let i = 0; i < BUCKETS; i++) {
      seen += this.counts[i] ?? 0;
      if (seen >= target) return Math.min(this.max, Histogram.upper(i));
    }
    return this.max;
  }

  mean(): number {
    return this.count === 0 ? 0 : this.sum / this.count;
  }

  toJSON(): HistogramData {
    return { counts: this.counts, count: this.count, sum: this.sum, min: this.count === 0 ? 0 : this.min, max: this.max };
  }

  summary(): { count: number; p50: number; p95: number; p99: number; max: number; mean: number } {
    const round = (v: number) => Math.round(v * 100) / 100;
    return {
      count: this.count,
      p50: round(this.percentile(50)),
      p95: round(this.percentile(95)),
      p99: round(this.percentile(99)),
      max: round(this.max),
      mean: round(this.mean()),
    };
  }

  static from(data: HistogramData): Histogram {
    const h = new Histogram();
    h.merge(data);
    return h;
  }
}
