local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return -1 end
local record = cjson.decode(raw)
if record.state == 'COMPLETED' then return 1 end
if record.state ~= 'PROCESSING' then return -1 end
local due_type = redis.call('TYPE', KEYS[2]).ok
if due_type ~= 'none' and due_type ~= 'zset' then return redis.error_reply('Invalid reservation deadline key type') end
record.state = 'COMPLETED'
record.consumeStatus = 'SUCCEEDED'
redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(record))
redis.call('ZREM', KEYS[2], ARGV[1])
return 1
