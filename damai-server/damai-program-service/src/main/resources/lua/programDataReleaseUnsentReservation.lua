-- Only release seats still owned by this order. Validate every key before writing.
local order_number = ARGV[1]
local record_json = redis.call('HGET', KEYS[1], order_number)
if not record_json then return -1 end
local record = cjson.decode(record_json)
local due_type = redis.call('TYPE', KEYS[2]).ok
if due_type ~= 'none' and due_type ~= 'zset' then
    return redis.error_reply('Invalid reservation deadline key type')
end
if record.state == 'CANCELLED' then return 0 end
if record.state ~= 'PENDING' then return -1 end
if ARGV[3] == 'EXPIRED' then
    local now = redis.call('TIME')
    local now_ms = now[1] * 1000 + math.floor(now[2] / 1000)
    if now_ms < record.deadlineMillis then return -1 end
end
local categories = cjson.decode(ARGV[2])
local updates = {}
local seen = {}
local total = 0
local expected = 0
for _, category in ipairs(categories) do
    local remain = redis.call('HGET', category.remainHashKey, category.ticketCategoryId)
    if not remain or not string.match(remain, '^%d+$') then
        return redis.error_reply('Invalid or missing ticket inventory')
    end
    local category_updates = {}
    for _, seat_id in ipairs(category.seatIds) do
        expected = expected + 1
        local unique_key = category.seatLockHashKey .. '\0' .. seat_id
        if seen[unique_key] then
            return redis.error_reply('Duplicate reserved seat')
        end
        seen[unique_key] = true
        local seat_json = redis.call('HGET', category.seatLockHashKey, seat_id)
        if seat_json then
            local seat = cjson.decode(seat_json)
            if seat.reservationOrderNumber == order_number and tonumber(seat.sellStatus) == 2 then
                if redis.call('HEXISTS', category.seatNoSoldHashKey, seat_id) == 1 then
                    return redis.error_reply('Reserved seat already exists in the available set')
                end
                seat.reservationOrderNumber = nil
                seat.sellStatus = 1
                table.insert(category_updates, {id = seat_id, data = cjson.encode(seat)})
            end
        end
    end
    if tonumber(remain) + #category_updates > 9007199254740991 then
        return redis.error_reply('Ticket inventory exceeds safe integer range')
    end
    total = total + #category_updates
    table.insert(updates, {category = category, seats = category_updates})
end
if expected == 0 or expected ~= #record.seats or total ~= expected then
    return redis.error_reply('Reservation ownership does not match')
end
for _, update in ipairs(updates) do
    if #update.seats > 0 then
        redis.call('HINCRBY', update.category.remainHashKey, update.category.ticketCategoryId, #update.seats)
        for _, seat in ipairs(update.seats) do
            redis.call('HDEL', update.category.seatLockHashKey, seat.id)
            redis.call('HSET', update.category.seatNoSoldHashKey, seat.id, seat.data)
        end
    end
end
record.state = 'CANCELLED'
record.consumeStatus = 'NOT_CONSUMED'
record.closeReason = ARGV[3]
redis.call('HSET', KEYS[1], order_number, cjson.encode(record))
redis.call('ZREM', KEYS[2], order_number)
return total
