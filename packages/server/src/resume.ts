export type ResumePlan =
  | { kind: "live"; seq: number }
  | { kind: "replay"; from: number; to: number; strict: boolean }
  | { kind: "reset"; seq: number; reason: "ahead" | "gap" };

export function planResume(
  from: number | undefined,
  history: number | undefined,
  head: number,
  limit: number,
): ResumePlan {
  if (from === undefined) {
    if (history !== undefined && history > 0 && head > 0) {
      return { kind: "replay", from: Math.max(0, head - Math.min(history, limit)), to: head, strict: false };
    }
    return { kind: "live", seq: head };
  }
  if (from > head) return { kind: "reset", seq: head, reason: "ahead" };
  if (from === head) return { kind: "live", seq: head };
  if (head - from > limit) return { kind: "reset", seq: head, reason: "gap" };
  return { kind: "replay", from, to: head, strict: true };
}

export interface Sequenced {
  seq: number;
}

export type PageCheck = { ok: true } | { ok: false; firstSeq: number | null };

export function checkPage(expectedNext: number, page: readonly Sequenced[], to: number): PageCheck {
  if (page.length === 0) return expectedNext > to ? { ok: true } : { ok: false, firstSeq: null };
  let expected = expectedNext;
  for (const entry of page) {
    if (entry.seq !== expected) return { ok: false, firstSeq: page[0]?.seq ?? null };
    expected++;
  }
  return { ok: true };
}

export function mergeLive<T extends { seq?: number | undefined }>(
  lastSent: number,
  buffered: readonly T[],
): { items: T[]; lastSeq: number } {
  const items: T[] = [];
  let last = lastSent;
  for (const item of buffered) {
    if (item.seq === undefined) {
      items.push(item);
      continue;
    }
    if (item.seq <= last) continue;
    items.push(item);
    last = item.seq;
  }
  return { items, lastSeq: last };
}
