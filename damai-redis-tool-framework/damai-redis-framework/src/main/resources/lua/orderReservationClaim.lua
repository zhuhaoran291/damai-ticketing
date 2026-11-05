local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return -1 end
local record = cjson.decode(raw)
if record.state == 'CANCELLED' or record.state == 'COMPLETED' then return 0 end
if record.state == 'PENDING' then
    local now = redis.call('TIME')
    if now[1] * 1000 + math.floor(now[2] / 1000) >= record.deadlineMillis then return 0 end
    record.state = 'PROCESSING'
    record.sendStatus = 'DELIVERED'
    record.consumeStatus = 'PROCESSING'
    redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(record))
    return 1
end
if record.state == 'PROCESSING' then return 1 end
return -1
