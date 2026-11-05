local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return -1 end
local record = cjson.decode(raw)
if record.state == 'PROCESSING' then
    record.consumeStatus = 'FAILED_RETRYABLE'
    local now = redis.call('TIME')
    record.lastFailureMillis = now[1] * 1000 + math.floor(now[2] / 1000)
    redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(record))
end
return 1
