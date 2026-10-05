export type JitterMode = "full" | "none";

export interface BackoffOptions {
  baseMs: number;
  maxMs: number;
  jitter: JitterMode;
}

export function backoffDelay(attempt: number, options: BackoffOptions, random: () => number = Math.random): number {
  const exp = Math.min(options.maxMs, options.baseMs * 2 ** Math.max(0, attempt));
  if (options.jitter === "none") return exp;
  return Math.floor(random() * exp);
}
