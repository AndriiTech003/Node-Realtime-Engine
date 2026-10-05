import { useEffect, useRef, useState, type FormEvent, type MouseEvent } from "react";
import { createSession, fetchConfig, loadSession, saveSession, simulateSlow, type Session } from "./api";
import { useClient, useRoom, type ChatMessage, type FeedItem, type LabEvent, type Member } from "./realtime";

const ROOMS = ["room:lobby", "room:design", "room:random"];

export function App() {
  const [session, setSession] = useState<Session | null>(() => loadSession());
  const [wsUrl, setWsUrl] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchConfig()
      .then((c) => setWsUrl(c.wsUrl))
      .catch((e: unknown) => setError(e instanceof Error ? e.message : String(e)));
  }, []);

  if (error !== null) return <div className="fatal">Cannot reach the auth service: {error}</div>;
  if (session === null) {
    return (
      <Login
        onLogin={(s) => {
          saveSession(s);
          setSession(s);
        }}
      />
    );
  }
  if (wsUrl === null) return <div className="fatal">Loading…</div>;
  return (
    <Workspace
      key={session.token}
      session={session}
      wsUrl={wsUrl}
      onLogout={() => {
        saveSession(null);
        setSession(null);
      }}
    />
  );
}

function Login({ onLogin }: { onLogin: (session: Session) => void }) {
  const [name, setName] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const submit = (e: FormEvent) => {
    e.preventDefault();
    if (name.trim().length === 0) return;
    setBusy(true);
    createSession(name.trim())
      .then(onLogin)
      .catch((err: unknown) => setError(err instanceof Error ? err.message : String(err)))
      .finally(() => setBusy(false));
  };
  return (
    <div className="login">
      <form onSubmit={submit} className="login-card">
        <h1>
          <span className="logo-dot" /> Pulse Rooms
        </h1>
        <p>Chat, presence and live cursors over a plain Node.js WebSocket engine. Pick a display name to join.</p>
        <input data-testid="login-name" autoFocus placeholder="Your name" value={name} maxLength={32} onChange={(e) => setName(e.target.value)} />
        <button data-testid="login-submit" disabled={busy || name.trim().length === 0}>
          Join
        </button>
        {error !== null && <div className="error">{error}</div>}
      </form>
    </div>
  );
}

function Workspace({ session, wsUrl, onLogout }: { session: Session; wsUrl: string; onLogout: () => void }) {
  const { client, state, node, cid, stats, events, log } = useClient(session, wsUrl);
  const [room, setRoom] = useState(ROOMS[0] as string);
  const [custom, setCustom] = useState("");
  const roomState = useRoom(client, session, room, log);

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <span className="logo-dot" /> Pulse Rooms
        </div>
        <div className={`conn conn-${state}`} data-testid="conn-state" data-state={state}>
          <span className="conn-dot" />
          {state}
          {node !== null && state === "open" && <span className="conn-node" data-testid="conn-node">{node}</span>}
        </div>
        <div className="me">
          <Avatar member={{ uid: session.user.id, name: session.user.name, color: session.user.color }} />
          <span>{session.user.name}</span>
          <button className="ghost" onClick={onLogout}>
            Leave
          </button>
        </div>
      </header>
      <aside className="sidebar">
        <h3>Rooms</h3>
        <ul className="rooms">
          {ROOMS.map((r) => (
            <li key={r}>
              <button data-testid={`room-${r}`} className={r === room ? "active" : ""} onClick={() => setRoom(r)}>
                # {r.slice(5)}
              </button>
            </li>
          ))}
        </ul>
        <form
          className="custom-room"
          onSubmit={(e) => {
            e.preventDefault();
            const id = custom.trim().replace(/[^A-Za-z0-9_.-]/g, "-").slice(0, 40);
            if (id.length > 0) setRoom(`room:${id}`);
            setCustom("");
          }}
        >
          <input placeholder="other room…" value={custom} onChange={(e) => setCustom(e.target.value)} />
        </form>
        <h3>
          Here now <span className="count">{roomState.members.length}</span>
        </h3>
        <ul className="members" data-testid="presence-list">
          {roomState.members.map((m) => (
            <li key={m.uid} data-testid="presence-member" data-uid={m.uid} data-name={m.name}>
              <Avatar member={m} />
              <span className="member-name">
                {m.name}
                {m.uid === session.user.id && <em> (you)</em>}
              </span>
              {m.status !== undefined && <span className="status">{m.status}</span>}
            </li>
          ))}
        </ul>
        <div className="status-picker">
          {["online", "busy", "away"].map((s) => (
            <button key={s} className="ghost small" onClick={() => roomState.setStatus(s)}>
              {s}
            </button>
          ))}
        </div>
      </aside>
      <ChatPanel session={session} room={room} roomState={roomState} />
      <NetworkLab
        token={session.token}
        cid={cid}
        state={state}
        stats={stats}
        events={events}
        feed={roomState.feed}
        onOffline={(ms) => {
          client.disconnect();
          log(`offline for ${ms / 1000} s (publishes are queued)`, "warn");
          setTimeout(() => {
            log("back online, resuming", "good");
            client.connect();
          }, ms);
        }}
        onKill={() => {
          log("killing the socket without a close handshake from the app", "warn");
          client.kill();
        }}
        log={log}
      />
    </div>
  );
}

type RoomState = ReturnType<typeof useRoom>;

function ChatPanel({ session, room, roomState }: { session: Session; room: string; roomState: RoomState }) {
  const [text, setText] = useState("");
  const listRef = useRef<HTMLDivElement | null>(null);
  const typingSent = useRef(false);
  const typingTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    const el = listRef.current;
    if (el !== null) el.scrollTop = el.scrollHeight;
  }, [roomState.messages.length]);

  const stopTyping = () => {
    if (typingTimer.current !== null) clearTimeout(typingTimer.current);
    typingTimer.current = null;
    if (typingSent.current) roomState.setTypingState(false);
    typingSent.current = false;
  };

  const onInput = (value: string) => {
    setText(value);
    if (!typingSent.current && value.length > 0) {
      roomState.setTypingState(true);
      typingSent.current = true;
    }
    if (typingTimer.current !== null) clearTimeout(typingTimer.current);
    typingTimer.current = setTimeout(stopTyping, 2500);
  };

  const submit = (e: FormEvent) => {
    e.preventDefault();
    const value = text.trim();
    if (value.length === 0) return;
    setText("");
    stopTyping();
    void roomState.send(value);
  };

  const onMove = (e: MouseEvent<HTMLDivElement>) => {
    const rect = e.currentTarget.getBoundingClientRect();
    roomState.moveCursor(Number(((e.clientX - rect.left) / rect.width).toFixed(4)), Number(((e.clientY - rect.top) / rect.height).toFixed(4)));
  };

  const typingNames = Object.keys(roomState.typing)
    .filter((uid) => uid !== session.user.id)
    .map((uid) => roomState.members.find((m) => m.uid === uid)?.name ?? uid);

  return (
    <main className="chat">
      <div className="chat-header">
        <h2># {room.slice(5)}</h2>
        <span className="hint">move your mouse here — others see your cursor</span>
      </div>
      <div className="chat-body" onMouseMove={onMove}>
        <div className="messages" ref={listRef} data-testid="message-list">
          {roomState.messages.length === 0 && <div className="empty">No messages yet. Say hi!</div>}
          {roomState.messages.map((m) => (
            <Message key={m.mid} message={m} own={m.from === session.user.id} />
          ))}
        </div>
        {Object.values(roomState.cursors)
          .filter((c) => c.uid !== session.user.id)
          .map((c) => {
            const member = roomState.members.find((m) => m.uid === c.uid);
            return (
              <div key={c.uid} className="cursor" data-testid="remote-cursor" data-uid={c.uid} style={{ left: `${c.x * 100}%`, top: `${c.y * 100}%`, color: member?.color ?? "#888" }}>
                <svg width="14" height="18" viewBox="0 0 14 18">
                  <path d="M0 0 L14 10 L7 11 L4 18 Z" fill="currentColor" />
                </svg>
                <span style={{ background: member?.color ?? "#888" }}>{member?.name ?? c.uid}</span>
              </div>
            );
          })}
      </div>
      <div className="typing" data-testid="typing-indicator">
        {typingNames.length > 0 && `${typingNames.join(", ")} ${typingNames.length === 1 ? "is" : "are"} typing…`}
      </div>
      <form className="composer" onSubmit={submit}>
        <input data-testid="composer-input" placeholder={`Message #${room.slice(5)}`} value={text} onChange={(e) => onInput(e.target.value)} maxLength={2000} />
        <button data-testid="composer-send" disabled={text.trim().length === 0}>
          Send
        </button>
      </form>
    </main>
  );
}

function Message({ message, own }: { message: ChatMessage; own: boolean }) {
  return (
    <div className={`message${own ? " own" : ""}${message.resumed ? " resumed" : ""}`} data-testid="message-item" data-seq={message.seq} data-resumed={message.resumed ? "true" : "false"}>
      <span className="seq">#{message.seq}</span>
      <span className="author" style={{ color: message.color }}>
        {message.name}
      </span>
      <span className="text">{message.text}</span>
      <time>{new Date(message.ts).toLocaleTimeString()}</time>
    </div>
  );
}

function Avatar({ member }: { member: Member }) {
  const initials = member.name
    .split(/\s+/)
    .map((p) => p[0] ?? "")
    .join("")
    .slice(0, 2)
    .toUpperCase();
  return (
    <span className="avatar" style={{ background: member.color }} title={member.name}>
      {initials}
    </span>
  );
}

interface LabProps {
  token: string;
  cid: string | null;
  state: string;
  stats: { reconnects: number; gaps: number; duplicates: number; resumedMessages: number; resets: number; queued: number };
  events: LabEvent[];
  feed: FeedItem[];
  onOffline: (ms: number) => void;
  onKill: () => void;
  log: (text: string, tone?: LabEvent["tone"]) => void;
}

function NetworkLab({ token, cid, state, stats, events, feed, onOffline, onKill, log }: LabProps) {
  const [offlineUntil, setOfflineUntil] = useState<number | null>(null);
  const [now, setNow] = useState(Date.now());

  useEffect(() => {
    if (offlineUntil === null) return;
    const timer = setInterval(() => {
      setNow(Date.now());
      if (Date.now() >= offlineUntil) setOfflineUntil(null);
    }, 200);
    return () => clearInterval(timer);
  }, [offlineUntil]);

  const remaining = offlineUntil === null ? 0 : Math.max(0, Math.ceil((offlineUntil - now) / 1000));

  return (
    <aside className="lab" data-testid="network-lab">
      <h3>Network lab</h3>
      <div className="lab-buttons">
        <button
          data-testid="lab-offline"
          disabled={offlineUntil !== null}
          onClick={() => {
            setOfflineUntil(Date.now() + 10000);
            setNow(Date.now());
            onOffline(10000);
          }}
        >
          {offlineUntil === null ? "Go offline 10 s" : `Offline… ${remaining}s`}
        </button>
        <button data-testid="lab-kill" disabled={state !== "open"} onClick={onKill}>
          Kill connection
        </button>
        <button
          data-testid="lab-slow"
          disabled={state !== "open" || cid === null}
          onClick={() => {
            if (cid === null) return;
            log("server corks this socket for 5 s (simulated slow network)", "warn");
            void simulateSlow(token, cid, 5000).then((ok) => {
              if (!ok) log("slow-client simulation is not available on this node", "warn");
            });
          }}
        >
          Simulate slow client
        </button>
      </div>
      <dl className="lab-stats" data-testid="lab-stats">
        <div>
          <dt>reconnects</dt>
          <dd data-testid="stat-reconnects">{stats.reconnects}</dd>
        </div>
        <div>
          <dt>resumed</dt>
          <dd data-testid="stat-resumed">{stats.resumedMessages}</dd>
        </div>
        <div>
          <dt>gaps</dt>
          <dd>{stats.gaps}</dd>
        </div>
        <div>
          <dt>dupes</dt>
          <dd>{stats.duplicates}</dd>
        </div>
        <div>
          <dt>resets</dt>
          <dd>{stats.resets}</dd>
        </div>
        <div>
          <dt>queued</dt>
          <dd data-testid="stat-queued">{stats.queued}</dd>
        </div>
      </dl>
      <h4>seq feed</h4>
      <div className="feed" data-testid="seq-feed">
        {feed.map((f) => (
          <span key={`${f.seq}-${f.at}`} data-testid="seq-feed-item" data-seq={f.seq} data-resumed={f.resumed ? "true" : "false"} className={`chip${f.resumed ? " resumed" : ""}${f.own ? " own" : ""}`}>
            {f.seq}
          </span>
        ))}
      </div>
      <div className="legend">
        <span className="chip">live</span>
        <span className="chip resumed">resumed after reconnect</span>
        <span className="chip own">yours</span>
      </div>
      <h4>events</h4>
      <ol className="events">
        {events.map((e) => (
          <li key={e.id} className={`ev-${e.tone}`}>
            <time>{new Date(e.at).toLocaleTimeString()}</time> {e.text}
          </li>
        ))}
      </ol>
    </aside>
  );
}
