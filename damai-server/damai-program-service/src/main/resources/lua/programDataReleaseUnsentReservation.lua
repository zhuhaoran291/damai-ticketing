-- Only release seats still owned by this order. Validate every key before writing.
local order_number = ARGV[1]
local categories = cjson.decode(ARGV[2])
local updates = {}
local seen = {}
local total = 0
for _, category in ipairs(categories) do
    local remain = redis.call('HGET', category.remainHashKey, category.ticketCategoryId)
    if not remain or not string.match(remain, '^%d+$') then
        return redis.error_reply('Invalid or missing ticket inventory')
    end
    local category_updates = {}
    for _, seat_id in ipairs(category.seatIds) do
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
    table.insert(updates, {category = category, seats = category_updates})
end
for _, update in ipairs(updates) do
    if #update.seats > 0 then
        redis.call('HINCRBY', update.category.remainHashKey, update.category.ticketCategoryId, #update.seats)
        for _, seat in ipairs(update.seats) do
            redis.call('HDEL', update.category.seatLockHashKey, seat.id)
            redis.call('HSET', update.category.seatNoSoldHashKey, seat.id, seat.data)
            total = total + 1
        end
    end
end
return total
