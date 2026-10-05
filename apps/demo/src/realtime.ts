import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { RealtimeClient, type Channel, type ClientState, type ClientStats, type JsonValue } from "@ashamrai/realtime-client";
import { fetchHistory, fetchTicket, type Session } from "./api";

export interface ChatMessage {
  seq: number;
  mid: string;
  text: string;
  from: string;
  name: string;
  color: string;
  ts: number;
  resumed: boolean;
}

export interface Member {
  uid: string;
  name: string;
  color: string;
  status?: string;
}

export interface Cursor {
  uid: string;
  x: number;
  y: number;
  at: number;
}

export interface FeedItem {
  seq: number;
  resumed: boolean;
  own: boolean;
  at: number;
}

export interface LabEvent {
  id: number;
  at: number;
  text: string;
  tone: "info" | "warn" | "good";
}

let eventCounter = 0;

export function useClient(session: Session, wsUrl: string) {
  const [state, setState] = useState<ClientState>("idle");
  const [node, setNode] = useState<string | null>(null);
  const [cid, setCid] = useState<string | null>(null);
  const [stats, setStats] = useState<ClientStats>({ reconnects: 0, gaps: 0, duplicates: 0, resumedMessages: 0, resets: 0, queued: 0 });
  const [events, setEvents] = useState<LabEvent[]>([]);

  const client = useMemo(
    () =>
      new RealtimeClient({
        url: wsUrl,
        getTicket: () => fetchTicket(session.token),
        autoConnect: false,
        reconnect: { baseMs: 500, maxMs: 10000, jitter: "full" },
      }),
    [session.token, wsUrl],
  );

  const log = useCallback(
    (text: string, tone: LabEvent["tone"] = "info") =>
      setEvents((prev) => [{ id: ++eventCounter, at: Date.now(), text, tone }, ...prev].slice(0, 40)),
    [],
  );

  useEffect(() => {
    const offs = [
      client.on("state", (s) => {
        setState(s.state);
        log(`state ${s.previous} → ${s.state}`, s.state === "open" ? "good" : s.state === "closed" ? "warn" : "info");
      }),
      client.on("open", (o) => {
        setNode(o.node);
        setCid(o.cid);
        log(`connected to ${o.node}`, "good");
      }),
      client.on("close", (c) => log(`socket closed (${c.code}${c.reason ? `: ${c.reason}` : ""})`, "warn")),
      client.on("drain", (d) => log(`server draining, reconnect in ${d.after} ms`, "warn")),
      client.on("reconnecting", (r) => log(`reconnect #${r.attempt + 1} in ${r.delay} ms (${r.reason})`)),
      client.on("error", (e) => log(`error: ${e.message}`, "warn")),
    ];
    const timer = setInterval(() => setStats({ ...client.stats }), 250);
    client.connect();
    return () => {
      for (const off of offs) off();
      clearInterval(timer);
      client.close();
    };
  }, [client, log]);

  return { client, state, node, cid, stats, events, log };
}

interface ChatPayload {
  text: string;
  name: string;
  color: string;
}

function asChat(d: unknown): ChatPayload | null {
  if (typeof d !== "object" || d === null) return null;
  const r = d as Record<string, unknown>;
  if (typeof r["text"] !== "string") return null;
  return {
    text: r["text"],
    name: typeof r["name"] === "string" ? r["name"] : "?",
    color: typeof r["color"] === "string" ? r["color"] : "#888",
  };
}

function toMember(uid: string, meta: JsonValue | undefined): Member {
  const m = (typeof meta === "object" && meta !== null && !Array.isArray(meta) ? meta : {}) as Record<string, JsonValue>;
  return {
    uid,
    name: typeof m["name"] === "string" ? m["name"] : uid,
    color: typeof m["color"] === "string" ? m["color"] : "#888",
    ...(typeof m["status"] === "string" ? { status: m["status"] } : {}),
  };
}

export function useRoom(client: RealtimeClient, session: Session, roomName: string, log: (text: string, tone?: LabEvent["tone"]) => void) {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [members, setMembers] = useState<Member[]>([]);
  const [typing, setTyping] = useState<Record<string, number>>({});
  const [cursors, setCursors] = useState<Record<string, Cursor>>({});
  const [feed, setFeed] = useState<FeedItem[]>([]);
  const channelRef = useRef<Channel | null>(null);

  useEffect(() => {
    const channel = client.subscribe(roomName, { history: 50 });
    channelRef.current = channel;
    setMessages([]);
    setMembers([]);
    setFeed([]);
    setCursors({});
    setTyping({});
    const offs = [
      channel.on("message", (m) => {
        const chat = asChat(m.d);
        setFeed((prev) => [{ seq: m.seq, resumed: m.resumed, own: m.from === session.user.id, at: Date.now() }, ...prev].slice(0, 60));
        if (chat === null) return;
        setMessages((prev) => {
          if (prev.length > 0 && (prev[prev.length - 1]?.seq ?? 0) >= m.seq) return prev;
          return [...prev, { seq: m.seq, mid: m.mid, text: chat.text, from: m.from, name: chat.name, color: chat.color, ts: m.ts, resumed: m.resumed }].slice(-300);
        });
        setTyping((prev) => {
          if (!(m.from in prev)) return prev;
          const next = { ...prev };
          delete next[m.from];
          return next;
        });
      }),
      channel.on("presence", (list) => setMembers(list.map((p) => toMember(p.uid, p.meta)))),
      channel.on("join", (p) => log(`${toMember(p.uid, p.meta).name} joined ${roomName}`, "good")),
      channel.on("leave", (p) => {
        log(`${p.uid} left ${roomName}`);
        setCursors((prev) => {
          const next = { ...prev };
          delete next[p.uid];
          return next;
        });
      }),
      channel.on("ephemeral", (e) => {
        const d = e.d as Record<string, unknown> | null;
        if (d === null || typeof d !== "object") return;
        if (d["k"] === "cursor" && typeof d["x"] === "number" && typeof d["y"] === "number") {
          const cursor: Cursor = { uid: e.from, x: d["x"], y: d["y"], at: Date.now() };
          setCursors((prev) => ({ ...prev, [e.from]: cursor }));
        } else if (d["k"] === "typing") {
          setTyping((prev) => {
            const next = { ...prev };
            if (d["on"] === true) next[e.from] = Date.now();
            else delete next[e.from];
            return next;
          });
        }
      }),
      channel.on("subscribed", (s) => {
        if (s.resumed > 0) log(`resumed ${s.resumed} missed message(s) in ${roomName}`, "good");
      }),
      channel.on("gap", (g) => log(`gap detected (expected ${g.expected}, got ${g.got}); resubscribing`, "warn")),
      channel.on("lag", () => log("server is dropping ephemeral updates (backpressure)", "warn")),
      channel.on("reset", (r) => {
        log(`history trimmed, reloading ${roomName} from HTTP (seq ${r.seq})`, "warn");
        void fetchHistory(session.token, roomName, 50).then((h) => {
          setMessages(
            h.messages.flatMap((m) => {
              const chat = asChat(m.d);
              return chat === null ? [] : [{ seq: m.seq, mid: m.mid, text: chat.text, from: m.from, name: chat.name, color: chat.color, ts: m.ts, resumed: true }];
            }),
          );
        });
      }),
    ];
    const sweep = setInterval(() => {
      const now = Date.now();
      setTyping((prev) => {
        const entries = Object.entries(prev).filter(([, at]) => now - at < 4000);
        return entries.length === Object.keys(prev).length ? prev : Object.fromEntries(entries);
      });
      setCursors((prev) => {
        const entries = Object.entries(prev).filter(([, c]) => now - c.at < 10000);
        return entries.length === Object.keys(prev).length ? prev : Object.fromEntries(entries);
      });
    }, 1000);
    return () => {
      for (const off of offs) off();
      clearInterval(sweep);
      channel.unsubscribe();
      channelRef.current = null;
    };
  }, [client, roomName, session.token, session.user.id, log]);

  const send = (text: string) => {
    const channel = channelRef.current;
    if (channel === null) return Promise.resolve();
    return channel.publish({ text, name: session.user.name, color: session.user.color }).then(() => undefined);
  };
  const moveCursor = (x: number, y: number) => channelRef.current?.sendEphemeral({ k: "cursor", x, y });
  const setTypingState = (on: boolean) => channelRef.current?.sendEphemeral({ k: "typing", on });
  const setStatus = (status: string) =>
    channelRef.current?.setPresence({ name: session.user.name, color: session.user.color, status }).catch(() => undefined);

  return { messages, members, typing, cursors, feed, send, moveCursor, setTypingState, setStatus };
}
