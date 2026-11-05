local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return -1 end
local record = cjson.decode(raw)
if record.state == 'PENDING' or record.state == 'PROCESSING' then
    if record.sendStatus ~= 'ACKNOWLEDGED' and record.sendStatus ~= 'DELIVERED' then record.sendStatus = ARGV[2] end
    redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(record))
end
return 1
