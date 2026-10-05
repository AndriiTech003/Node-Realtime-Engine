import type { ErrFrame, JsonValue, MsgFrame, PresenceMember, SubFrame } from "@ashamrai/realtime-protocol";
import { Emitter } from "./emitter.js";
import { SeqTracker } from "./seq-tracker.js";

export interface ChannelMessage {
  ch: string;
  seq: number;
  mid: string;
  d: JsonValue;
  ts: number;
  from: string;
  resumed: boolean;
}

export interface ChannelEvents {
  message: ChannelMessage;
  reset: { seq: number };
  presence: PresenceMember[];
  join: PresenceMember;
  leave: { uid: string };
  update: PresenceMember;
  ephemeral: { d: JsonValue; from: string };
  lag: { ch: string };
  subscribed: { seq: number; resumed: number; members: number | undefined };
  gap: { expected: number; got: number };
  error: ErrFrame;
}

export interface SubscribeOptions {
  history?: number;
  presence?: boolean;
  from?: number;
}

export interface PublishAck {
  seq: number;
  mid: string;
  dup: boolean;
}

export interface ChannelHost {
  sendSub(channel: Channel): void;
  sendUnsub(channel: Channel): void;
  publish(ch: string, d: JsonValue): Promise<PublishAck>;
  sendEphemeral(ch: string, d: JsonValue): void;
  setPresence(ch: string, meta: JsonValue): Promise<void>;
  countDuplicate(): void;
  countGap(): void;
  countResumed(n: number): void;
  countReset(): void;
}

export type ChannelState = "pending" | "subscribing" | "subscribed" | "closed";

export class Channel extends Emitter<ChannelEvents> {
  state: ChannelState = "pending";
  readonly members = new Map<string, JsonValue | undefined>();
  private readonly tracker: SeqTracker;
  private resuming = false;
  private everSubscribed = false;
  private resumedCount = 0;
  requestId = 0;

  constructor(
    readonly name: string,
    readonly options: SubscribeOptions,
    private readonly host: ChannelHost,
  ) {
    super();
    this.tracker = new SeqTracker(options.from ?? null);
  }

  get lastSeq(): number | null {
    return this.tracker.last;
  }

  get isResuming(): boolean {
    return this.resuming;
  }

  buildSubFrame(id: number): SubFrame {
    this.requestId = id;
    this.state = "subscribing";
    this.resumedCount = 0;
    const frame: SubFrame = { t: "sub", id, ch: this.name };
    if (this.tracker.last !== null) {
      frame.from = this.tracker.last;
      this.resuming = true;
    } else {
      this.resuming = false;
      if (this.options.history !== undefined && !this.everSubscribed) frame.history = this.options.history;
    }
    if (this.options.presence === false) frame.presence = false;
    return frame;
  }

  publish(d: JsonValue): Promise<PublishAck> {
    return this.host.publish(this.name, d);
  }

  sendEphemeral(d: JsonValue): void {
    this.host.sendEphemeral(this.name, d);
  }

  setPresence(meta: JsonValue): Promise<void> {
    return this.host.setPresence(this.name, meta);
  }

  unsubscribe(): void {
    if (this.state === "closed") return;
    this.host.sendUnsub(this);
    this.state = "closed";
    this.members.clear();
  }

  memberList(): PresenceMember[] {
    return Array.from(this.members, ([uid, meta]) => (meta === undefined ? { uid } : { uid, meta }));
  }

  handleMsg(frame: MsgFrame): void {
    const verdict = this.tracker.check(frame.seq);
    if (verdict === "duplicate") {
      this.host.countDuplicate();
      return;
    }
    if (verdict === "gap") {
      if (this.state === "subscribing") return;
      const expected = (this.tracker.last ?? 0) + 1;
      this.host.countGap();
      this.emit("gap", { expected, got: frame.seq });
      this.host.sendSub(this);
      return;
    }
    this.tracker.commit(frame.seq);
    const resumed = this.state === "subscribing" && this.resuming;
    if (resumed) this.resumedCount++;
    this.emit("message", {
      ch: frame.ch,
      seq: frame.seq,
      mid: frame.mid,
      d: frame.d,
      ts: frame.ts,
      from: frame.from,
      resumed,
    });
  }

  handleOk(seq: number | undefined, presence: PresenceMember[] | undefined, pn: number | undefined): void {
    if (seq !== undefined) this.tracker.baseline(seq);
    this.state = "subscribed";
    this.everSubscribed = true;
    if (this.resumedCount > 0) this.host.countResumed(this.resumedCount);
    if (presence !== undefined) {
      this.members.clear();
      for (const m of presence) this.members.set(m.uid, m.meta);
      this.emit("presence", this.memberList());
    }
    this.emit("subscribed", { seq: this.tracker.last ?? 0, resumed: this.resumedCount, members: pn });
    this.resuming = false;
  }

  handleReset(seq: number): void {
    this.tracker.reset(seq);
    this.host.countReset();
    this.emit("reset", { seq });
  }

  handleJoin(uid: string, meta: JsonValue | undefined): void {
    this.members.set(uid, meta);
    this.emit("join", meta === undefined ? { uid } : { uid, meta });
    this.emit("presence", this.memberList());
  }

  handleLeave(uid: string): void {
    if (!this.members.delete(uid)) return;
    this.emit("leave", { uid });
    this.emit("presence", this.memberList());
  }

  handleUpdate(uid: string, meta: JsonValue | undefined): void {
    this.members.set(uid, meta);
    this.emit("update", meta === undefined ? { uid } : { uid, meta });
    this.emit("presence", this.memberList());
  }

  handleEphemeral(d: JsonValue, from: string): void {
    this.emit("ephemeral", { d, from });
  }

  handleLag(): void {
    this.emit("lag", { ch: this.name });
  }

  handleError(frame: ErrFrame): void {
    if (frame.id === this.requestId && this.state === "subscribing") this.state = "closed";
    this.emit("error", frame);
  }

  markDisconnected(): void {
    if (this.state !== "closed") this.state = "pending";
  }
}
