export const PUBLISH_SCRIPT = `
local existing = redis.call('GET', KEYS[3])
if existing then
  local sep = string.find(existing, '|', 1, true)
  return {tonumber(string.sub(existing, 1, sep - 1)), 1, string.sub(existing, sep + 1)}
end
local seq = redis.call('INCR', KEYS[1])
redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[4], '0-' .. seq, 'p', ARGV[1], 'ts', ARGV[2])
redis.call('SET', KEYS[3], seq .. '|' .. ARGV[7], 'EX', ARGV[6])
redis.call('SPUBLISH', ARGV[3], seq .. '|' .. ARGV[2] .. '|' .. ARGV[1])
local cutoff = tonumber(ARGV[5])
if cutoff > 0 then
  local oldest = redis.call('XRANGE', KEYS[2], '-', '+', 'COUNT', 32)
  local keep = nil
  for i = 1, #oldest do
    if tonumber(oldest[i][2][4]) >= cutoff then
      keep = oldest[i][1]
      break
    end
  end
  if keep == nil and #oldest > 0 then
    local last = oldest[#oldest][1]
    local lastSeq = tonumber(string.sub(last, 3))
    keep = '0-' .. (lastSeq + 1)
  end
  if keep ~= nil and #oldest > 0 and keep ~= oldest[1][1] then
    redis.call('XTRIM', KEYS[2], 'MINID', keep)
  end
end
return {seq, 0, ARGV[7]}
`;

export const PRESENCE_JOIN_SCRIPT = `
local added = redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
redis.call('ZADD', KEYS[2], ARGV[4], ARGV[1])
if added == 0 then
  return 0
end
local n = redis.call('HINCRBY', KEYS[3], ARGV[2], 1)
if n == 1 then
  redis.call('SPUBLISH', ARGV[5], 'p|' .. ARGV[6])
  return 2
end
return 1
`;

export const PRESENCE_LEAVE_SCRIPT = `
local raw = redis.call('HGET', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
if not raw then
  return 0
end
redis.call('HDEL', KEYS[1], ARGV[1])
local entry = cjson.decode(raw)
local n = redis.call('HINCRBY', KEYS[3], entry.uid, -1)
if n <= 0 then
  redis.call('HDEL', KEYS[3], entry.uid)
  local frame = '{"t":"pl","ch":' .. cjson.encode(ARGV[3]) .. ',"uid":' .. cjson.encode(entry.uid) .. '}'
  redis.call('SPUBLISH', ARGV[2], 'p|' .. frame)
  return 2
end
return 1
`;

export const PRESENCE_UPDATE_SCRIPT = `
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 0 then
  return 0
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('SPUBLISH', ARGV[3], 'p|' .. ARGV[4])
return 1
`;

export const PRESENCE_LIST_SCRIPT = `
local limit = tonumber(ARGV[1])
local total = redis.call('HLEN', KEYS[2])
local values = redis.call('HVALS', KEYS[1])
local best = {}
local order = {}
for i = 1, #values do
  local entry = cjson.decode(values[i])
  local current = best[entry.uid]
  if current == nil then
    order[#order + 1] = entry.uid
    best[entry.uid] = {values[i], entry.at or 0}
  elseif (entry.at or 0) > current[2] then
    best[entry.uid] = {values[i], entry.at or 0}
  end
end
local out = {}
for i = 1, math.min(#order, limit) do
  out[#out + 1] = best[order[i]][1]
end
return {total, out}
`;

export const RELEASE_LOCK_SCRIPT = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`;

export const USER_ACQUIRE_SCRIPT = `
local limit = tonumber(ARGV[2])
local counts = redis.call('HVALS', KEYS[1])
local total = 0
for i = 1, #counts do
  total = total + tonumber(counts[i])
end
if total >= limit then
  return {0, total}
end
redis.call('HINCRBY', KEYS[1], ARGV[1], 1)
return {1, total + 1}
`;

export const USER_RELEASE_SCRIPT = `
local n = redis.call('HINCRBY', KEYS[1], ARGV[1], -1)
if n <= 0 then
  redis.call('HDEL', KEYS[1], ARGV[1])
  return 0
end
return n
`;
