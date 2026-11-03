local type = tonumber(KEYS[1])
local placeholder_seat_no_sold_hash_key = KEYS[2]
local placeholder_seat_lock_hash_key = KEYS[3]
local program_id = KEYS[4]
local ticket_count_list = cjson.decode(ARGV[1])
local purchase_seat_list = {}
local total_seat_dto_price = 0
local total_seat_vo_price = 0
if KEYS[5] and KEYS[6] then
    for i = 5, 6 do
        local pending_type = redis.call('type', KEYS[i]).ok
        if pending_type ~= 'none' and pending_type ~= 'hash' then
            return redis.error_reply('pending order key must be a hash')
        end
    end
    if KEYS[7] then
        local request_type = redis.call('type', KEYS[7]).ok
        if request_type ~= 'none' and request_type ~= 'string' then
            return redis.error_reply('order request key must be a string')
        end
        local previous = redis.call('get', KEYS[7])
        if previous then
            return '{"code":0,"pendingOrderNumber":"' .. previous .. '"}'
        end
    end
    local existing = redis.call('hget', KEYS[6], ARGV[5])
    if existing then
        if KEYS[7] then
            redis.call('set', KEYS[7], existing, 'EX', 86400)
        end
        return '{"code":0,"pendingOrderNumber":"' .. existing .. '"}'
    end
end
local function find_adjacent_seats(all_seats, seat_count)
    local adjacent_seats = {}
    
    table.sort(all_seats, function(s1, s2)
        if s1.rowCode == s2.rowCode then
            return s1.colCode < s2.colCode
        else
            return s1.rowCode < s2.rowCode
        end
    end)
    
    for i = 1, #all_seats - seat_count + 1 do
        local seats_found = true
        for j = 0, seat_count - 2 do
            local current = all_seats[i + j]
            local next = all_seats[i + j + 1]

            if not (current.rowCode == next.rowCode and next.colCode - current.colCode == 1) then
                seats_found = false
                break
            end
        end
        if seats_found then
            for k = 0, seat_count - 1 do
                table.insert(adjacent_seats, all_seats[i + k])
            end
            return adjacent_seats
        end
    end
    return adjacent_seats
end

if (type == 1) then
    for index,ticket_count in ipairs(ticket_count_list) do
        local ticket_remain_number_hash_key = ticket_count.programTicketRemainNumberHashKey
        local ticket_category_id = ticket_count.ticketCategoryId
        local count = ticket_count.ticketCount
        local remain_number_str = redis.call('hget', ticket_remain_number_hash_key, tostring(ticket_category_id))
        if not remain_number_str then
            return string.format('{"%s": %d}', 'code', 40010)
        end
        local remain_number = tonumber(remain_number_str)
        if (count > remain_number) then
            return string.format('{"%s": %d}', 'code', 40011)
        end
    end
    local seat_data_list= cjson.decode(ARGV[2])
    for index, seatData in pairs(seat_data_list) do
        local seat_no_sold_hash_key = seatData.seatNoSoldHashKey;
        local seat_dto_list = cjson.decode(seatData.seatDataList)
        for index2,seat_dto in ipairs(seat_dto_list) do
            local id = seat_dto.id
            local seat_dto_price = seat_dto.price
            local seat_vo_str = redis.call('hget', seat_no_sold_hash_key, tostring(id))
            if not seat_vo_str then
                return string.format('{"%s": %d}', 'code', 40001)
            end
            local seat_vo = cjson.decode(seat_vo_str)
            if (seat_vo.sellStatus == 2) then
                return string.format('{"%s": %d}', 'code', 40002)
            end
            if (seat_vo.sellStatus == 3) then
                return string.format('{"%s": %d}', 'code', 40003)
            end
            table.insert(purchase_seat_list,seat_vo)
            total_seat_dto_price = total_seat_dto_price + seat_dto_price
            total_seat_vo_price = total_seat_vo_price + seat_vo.price
            if (total_seat_dto_price > total_seat_vo_price) then
                return string.format('{"%s": %d}', 'code', 40008)
            end
        end
    end
end
if (type == 2) then
    for index,ticket_count in ipairs(ticket_count_list) do
        local ticket_remain_number_hash_key = ticket_count.programTicketRemainNumberHashKey
        local ticket_category_id = ticket_count.ticketCategoryId
        local count = ticket_count.ticketCount
        local remain_number_str = redis.call('hget', ticket_remain_number_hash_key, tostring(ticket_category_id))
        if not remain_number_str then
            return string.format('{"%s": %d}', 'code', 40010)
        end
        local remain_number = tonumber(remain_number_str)
        if (count > remain_number) then
            return string.format('{"%s": %d}', 'code', 40011)
        end
        local seat_no_sold_hash_key = ticket_count.seatNoSoldHashKey
        local seat_vo_no_sold_str_list = redis.call('hvals',seat_no_sold_hash_key)
        local filter_seat_vo_no_sold_list = {}
        for index,seat_vo_no_sold_str in ipairs(seat_vo_no_sold_str_list) do
            local seat_vo_no_sold = cjson.decode(seat_vo_no_sold_str)
            table.insert(filter_seat_vo_no_sold_list,seat_vo_no_sold)
        end
        purchase_seat_list = find_adjacent_seats(filter_seat_vo_no_sold_list,count)
        if (#purchase_seat_list < count) then
            return string.format('{"%s": %d}', 'code', 40004)
        end
    end
end
local seat_id_list = {}
local seat_data_list = {}
for index,seat in ipairs(purchase_seat_list) do
    local seat_id = seat.id
    local ticket_category_id = seat.ticketCategoryId
    if not seat_id_list[ticket_category_id] then
        seat_id_list[ticket_category_id] = {}
    end
    table.insert(seat_id_list[ticket_category_id], tostring(seat_id))

    if not seat_data_list[ticket_category_id] then
        seat_data_list[ticket_category_id] = {}
    end
    table.insert(seat_data_list[ticket_category_id], tostring(seat_id))
    seat.sellStatus = 2
    table.insert(seat_data_list[ticket_category_id], cjson.encode(seat))
end
for index,ticket_count in ipairs(ticket_count_list) do
    local ticket_remain_number_hash_key = ticket_count.programTicketRemainNumberHashKey
    local ticket_category_id = ticket_count.ticketCategoryId
    local count = ticket_count.ticketCount
    redis.call('hincrby',ticket_remain_number_hash_key,ticket_category_id,"-" .. count)
end
for ticket_category_id, seat_id_array in pairs(seat_id_list) do
    redis.call('hdel',string.format(placeholder_seat_no_sold_hash_key,program_id,tostring(ticket_category_id)),unpack(seat_id_array))
end
for ticket_category_id, seat_data_array in pairs(seat_data_list) do
    redis.call('hmset',string.format(placeholder_seat_lock_hash_key,program_id,tostring(ticket_category_id)),unpack(seat_data_array))    
end
-- V4 records the request in the same script as the seat reservation so that a
-- restarted producer can retry it without reserving the seats a second time.
if KEYS[5] and KEYS[6] and ARGV[3] and ARGV[4] and ARGV[5] then
    local pending = '{"request":' .. ARGV[3] .. ',"seats":' .. cjson.encode(purchase_seat_list) .. '}'
    redis.call('hset', KEYS[5], ARGV[4], pending)
    redis.call('hset', KEYS[6], ARGV[5], ARGV[4])
    if KEYS[7] then
        redis.call('set', KEYS[7], ARGV[4], 'EX', 86400)
    end
end
return string.format('{"%s": %d, "%s": %s}', 'code', 0, 'purchaseSeatList', cjson.encode(purchase_seat_list))
