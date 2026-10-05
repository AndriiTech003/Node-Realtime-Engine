export interface DemoUser {
  id: string;
  name: string;
  color: string;
}

export interface Session {
  token: string;
  user: DemoUser;
}

const STORAGE_KEY = "pulse-rooms-session";

export function loadSession(): Session | null {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    return raw === null ? null : (JSON.parse(raw) as Session);
  } catch {
    return null;
  }
}

export function saveSession(session: Session | null): void {
  try {
    if (session === null) localStorage.removeItem(STORAGE_KEY);
    else localStorage.setItem(STORAGE_KEY, JSON.stringify(session));
  } catch {
    return;
  }
}

async function json<T>(res: Response): Promise<T> {
  if (!res.ok) {
    const body = (await res.json().catch(() => ({}))) as { error?: string };
    throw new Error(body.error ?? `HTTP ${res.status}`);
  }
  return (await res.json()) as T;
}

export async function createSession(name: string): Promise<Session> {
  const res = await fetch("/api/session", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ name }),
  });
  return json<Session>(res);
}

export async function fetchConfig(): Promise<{ wsUrl: string }> {
  return json<{ wsUrl: string }>(await fetch("/api/config"));
}

export async function fetchTicket(token: string): Promise<string> {
  const res = await fetch("/api/ticket", { method: "POST", headers: { authorization: `Bearer ${token}` } });
  const body = await json<{ ticket: string }>(res);
  return body.ticket;
}

export interface HistoryMessage {
  seq: number;
  mid: string;
  d: unknown;
  ts: number;
  from: string;
}

export async function fetchHistory(token: string, ch: string, limit = 50): Promise<{ seq: number; messages: HistoryMessage[] }> {
  const res = await fetch(`/api/history?ch=${encodeURIComponent(ch)}&limit=${limit}`, {
    headers: { authorization: `Bearer ${token}` },
  });
  return json(res);
}

export async function simulateSlow(token: string, cid: string, ms: number): Promise<boolean> {
  const res = await fetch("/api/simulate-slow", {
    method: "POST",
    headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
    body: JSON.stringify({ cid, ms }),
  });
  return res.ok;
}
