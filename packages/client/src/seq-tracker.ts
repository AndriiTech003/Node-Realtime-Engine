export type SeqVerdict = "accept" | "duplicate" | "gap";

export class SeqTracker {
  last: number | null;

  constructor(initial: number | null = null) {
    this.last = initial;
  }

  check(seq: number): SeqVerdict {
    if (this.last === null) return "accept";
    if (seq <= this.last) return "duplicate";
    if (seq === this.last + 1) return "accept";
    return "gap";
  }

  commit(seq: number): void {
    this.last = seq;
  }

  reset(seq: number): void {
    this.last = seq;
  }

  baseline(seq: number): void {
    if (this.last === null || seq > this.last) this.last = seq;
  }
}
