import { CHANNEL_NAME_MAX, CHANNEL_NAME_PATTERN } from "./constants.js";

export type ChannelType = "room" | "user" | "broadcast";

export interface ChannelInfo {
  name: string;
  type: ChannelType;
  id: string;
  durable: boolean;
  presence: boolean;
  ephemeral: boolean;
  clientPublish: boolean;
}

export type ChannelParseResult = { ok: true; channel: ChannelInfo } | { ok: false; reason: "invalid" | "unknown_type" };

export function isValidChannelName(name: string): boolean {
  return name.length > 0 && name.length <= CHANNEL_NAME_MAX && CHANNEL_NAME_PATTERN.test(name);
}

export function parseChannel(name: string): ChannelParseResult {
  if (!isValidChannelName(name)) return { ok: false, reason: "invalid" };
  const colon = name.indexOf(":");
  if (colon <= 0 || colon === name.length - 1) return { ok: false, reason: "invalid" };
  const prefix = name.slice(0, colon);
  const id = name.slice(colon + 1);
  switch (prefix) {
    case "room":
      return {
        ok: true,
        channel: { name, type: "room", id, durable: true, presence: true, ephemeral: true, clientPublish: true },
      };
    case "user":
      return {
        ok: true,
        channel: { name, type: "user", id, durable: true, presence: false, ephemeral: false, clientPublish: false },
      };
    case "broadcast":
      return {
        ok: true,
        channel: { name, type: "broadcast", id, durable: true, presence: false, ephemeral: false, clientPublish: false },
      };
    default:
      return { ok: false, reason: "unknown_type" };
  }
}
