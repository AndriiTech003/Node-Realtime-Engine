import type { WebSocketLike } from "../../src/client.js";

export class FakeSocket implements WebSocketLike {
  static instances: FakeSocket[] = [];
  readyState = 0;
  protocol = "pulse.v1.json";
  binaryType = "blob";
  onopen: ((event: unknown) => void) | null = null;
  onmessage: ((event: { data: unknown }) => void) | null = null;
  onclose: ((event: { code: number; reason: string }) => void) | null = null;
  onerror: ((event: unknown) => void) | null = null;
  sent: Record<string, unknown>[] = [];
  closedWith: { code: number | undefined; reason: string | undefined } | null = null;

  constructor(
    readonly url: string,
    readonly protocols?: string | string[],
  ) {
    FakeSocket.instances.push(this);
  }

  static last(): FakeSocket {
    const socket = FakeSocket.instances[FakeSocket.instances.length - 1];
    if (socket === undefined) throw new Error("no socket");
    return socket;
  }

  static reset(): void {
    FakeSocket.instances = [];
  }

  send(data: string | ArrayBufferLike | ArrayBufferView): void {
    this.sent.push(JSON.parse(String(data)) as Record<string, unknown>);
  }

  close(code?: number, reason?: string): void {
    this.closedWith = { code, reason };
    this.readyState = 3;
  }

  open(cid = "c1"): void {
    this.readyState = 1;
    this.onopen?.({});
    this.receive({ t: "hello", cid, node: "n1", hb: 25000, v: 1 });
  }

  receive(frame: Record<string, unknown>): void {
    this.onmessage?.({ data: JSON.stringify(frame) });
  }

  serverClose(code: number, reason = ""): void {
    this.readyState = 3;
    this.onclose?.({ code, reason });
  }

  sentOf(t: string): Record<string, unknown>[] {
    return this.sent.filter((f) => f["t"] === t);
  }
}
