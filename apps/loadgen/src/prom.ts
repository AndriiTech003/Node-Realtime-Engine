export interface Sample {
  name: string;
  labels: Record<string, string>;
  value: number;
}

export function parsePrometheus(text: string): Sample[] {
  const out: Sample[] = [];
  for (const line of text.split("\n")) {
    if (line.length === 0 || line.startsWith("#")) continue;
    const match = /^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{([^}]*)\})?\s+(\S+)/.exec(line);
    if (match === null) continue;
    const labels: Record<string, string> = {};
    if (match[3] !== undefined) {
      for (const pair of match[3].matchAll(/([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"/g)) {
        labels[pair[1] as string] = pair[2] as string;
      }
    }
    out.push({ name: match[1] as string, labels, value: Number(match[4]) });
  }
  return out;
}

export function pick(samples: Sample[], name: string, labels: Record<string, string> = {}): number {
  let total = 0;
  let found = false;
  for (const s of samples) {
    if (s.name !== name) continue;
    if (Object.entries(labels).every(([k, v]) => s.labels[k] === v)) {
      total += s.value;
      found = true;
    }
  }
  return found ? total : Number.NaN;
}

export function histogramBuckets(samples: Sample[], name: string): Map<number, number> {
  const out = new Map<number, number>();
  for (const s of samples) {
    if (s.name !== `${name}_bucket`) continue;
    const le = s.labels["le"];
    if (le === undefined) continue;
    const bound = le === "+Inf" ? Number.POSITIVE_INFINITY : Number(le);
    out.set(bound, (out.get(bound) ?? 0) + s.value);
  }
  return out;
}

export function quantileFromBuckets(before: Map<number, number>, after: Map<number, number>, q: number): number {
  const bounds = Array.from(after.keys()).sort((a, b) => a - b);
  const deltas = bounds.map((b) => (after.get(b) ?? 0) - (before.get(b) ?? 0));
  const total = deltas[deltas.length - 1] ?? 0;
  if (total <= 0) return 0;
  const target = q * total;
  for (let i = 0; i < bounds.length; i++) {
    if ((deltas[i] ?? 0) >= target) {
      const upper = bounds[i] as number;
      const lower = i === 0 ? 0 : (bounds[i - 1] as number);
      if (!Number.isFinite(upper)) return lower;
      const prevCount = i === 0 ? 0 : (deltas[i - 1] ?? 0);
      const inBucket = (deltas[i] ?? 0) - prevCount;
      if (inBucket <= 0) return upper;
      return lower + ((target - prevCount) / inBucket) * (upper - lower);
    }
  }
  return bounds[bounds.length - 1] ?? 0;
}

export async function scrape(url: string): Promise<Sample[]> {
  const res = await fetch(url);
  return parsePrometheus(await res.text());
}
