export class Keys {
  constructor(readonly prefix: string) {
    if (prefix.includes("{") || prefix.includes("}")) throw new Error("redis prefix must not contain hash tag braces");
  }

  seq(ch: string): string {
    return `${this.prefix}ch:{${ch}}:seq`;
  }

  log(ch: string): string {
    return `${this.prefix}ch:{${ch}}:log`;
  }

  cmid(ch: string, cmid: string): string {
    return `${this.prefix}ch:{${ch}}:cmid:${cmid}`;
  }

  fan(ch: string): string {
    return `${this.prefix}fan:{${ch}}`;
  }

  channelFromFan(fan: string): string | null {
    const start = this.prefix.length + 5;
    if (!fan.startsWith(`${this.prefix}fan:{`) || !fan.endsWith("}")) return null;
    return fan.slice(start, -1);
  }

  pres(ch: string): string {
    return `${this.prefix}pres:{${ch}}`;
  }

  presExp(ch: string): string {
    return `${this.prefix}pres:{${ch}}:exp`;
  }

  presUsers(ch: string): string {
    return `${this.prefix}pres:{${ch}}:users`;
  }

  presIndex(): string {
    return `${this.prefix}pres:index`;
  }

  ticket(ticket: string): string {
    return `${this.prefix}ticket:${ticket}`;
  }

  nodeAlive(nodeId: string): string {
    return `${this.prefix}node:${nodeId}:alive`;
  }

  nodePresence(nodeId: string): string {
    return `${this.prefix}node:${nodeId}:pres`;
  }

  userConns(uid: string): string {
    return `${this.prefix}user:{${uid}}:conns`;
  }

  nodeUsers(nodeId: string): string {
    return `${this.prefix}node:${nodeId}:users`;
  }

  nodes(): string {
    return `${this.prefix}nodes`;
  }

  sweepLock(): string {
    return `${this.prefix}lock:presence-sweep`;
  }
}
