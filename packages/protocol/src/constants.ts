export const PROTOCOL_VERSION = 1;

export const SUBPROTOCOL_JSON = "pulse.v1.json";
export const SUBPROTOCOL_MSGPACK = "pulse.v1.msgpack";
export const SUBPROTOCOLS = [SUBPROTOCOL_JSON, SUBPROTOCOL_MSGPACK] as const;
export type Subprotocol = (typeof SUBPROTOCOLS)[number];

export const MAX_PAYLOAD_BYTES = 64 * 1024;
export const MAX_SUBSCRIPTIONS = 100;
export const MAX_CONNECTIONS_PER_USER = 10;
export const RESUME_LIMIT = 5000;
export const HISTORY_MAX_LEN = 10000;
export const HISTORY_RETENTION_MS = 24 * 60 * 60 * 1000;
export const CMID_TTL_SECONDS = 300;
export const TICKET_TTL_SECONDS = 30;
export const EPHEMERAL_COALESCE_MS = 50;
export const CHANNEL_NAME_MAX = 128;
export const CHANNEL_NAME_PATTERN = /^[A-Za-z0-9_.:-]+$/;

export const CloseCode = {
  Normal: 1000,
  GoingAway: 1001,
  MessageTooBig: 1009,
  InvalidTicket: 4001,
  Forbidden: 4003,
  SlowConsumer: 4008,
  RateLimited: 4029,
  ProtocolViolation: 4400,
} as const;
export type CloseCode = (typeof CloseCode)[keyof typeof CloseCode];

export const ErrorCode = {
  BadRequest: "BAD_REQUEST",
  UnknownChannelType: "UNKNOWN_CHANNEL_TYPE",
  Forbidden: "FORBIDDEN",
  NotSubscribed: "NOT_SUBSCRIBED",
  RateLimited: "RATE_LIMITED",
  PayloadTooLarge: "PAYLOAD_TOO_LARGE",
  TooManySubscriptions: "TOO_MANY_SUBSCRIPTIONS",
  Internal: "INTERNAL",
} as const;
export type ErrorCode = (typeof ErrorCode)[keyof typeof ErrorCode];

export type ReconnectPolicy = "none" | "backoff" | "immediate" | "new-ticket" | "drain";

export function closeCodePolicy(code: number): ReconnectPolicy {
  switch (code) {
    case CloseCode.Normal:
    case CloseCode.Forbidden:
    case CloseCode.MessageTooBig:
    case CloseCode.ProtocolViolation:
      return "none";
    case CloseCode.GoingAway:
      return "drain";
    case CloseCode.InvalidTicket:
      return "new-ticket";
    case CloseCode.SlowConsumer:
      return "immediate";
    case CloseCode.RateLimited:
      return "backoff";
    default:
      return "backoff";
  }
}
