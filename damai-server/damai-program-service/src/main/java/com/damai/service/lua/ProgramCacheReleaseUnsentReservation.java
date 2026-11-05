package com.damai.service.lua;

import com.alibaba.fastjson.JSON;
import com.damai.core.RedisKeyManage;
import com.damai.redis.RedisCache;
import com.damai.redis.RedisKeyBuild;
import com.damai.vo.SeatVo;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Release only the seats owned by an order whose Kafka send has not been attempted. */
@Component
public class ProgramCacheReleaseUnsentReservation {
    @Autowired
    private RedisCache redisCache;

    private DefaultRedisScript<Long> script;

    @PostConstruct
    public void init() {
        script = new DefaultRedisScript<>();
        script.setScriptSource(new ResourceScriptSource(new ClassPathResource("lua/programDataReleaseUnsentReservation.lua")));
        script.setResultType(Long.class);
    }

    public long release(Long programId, Long orderNumber, List<SeatVo> seats) {
        return release(programId, orderNumber, seats, "PRE_SEND_FAILURE");
    }

    public long releaseExpired(Long programId, Long orderNumber, List<SeatVo> seats) {
        return release(programId, orderNumber, seats, "EXPIRED");
    }

    private long release(Long programId, Long orderNumber, List<SeatVo> seats, String reason) {
        Map<Long, Map<String, Object>> categories = new LinkedHashMap<>();
        for (SeatVo seat : seats) {
            Long categoryId = seat.getTicketCategoryId();
            Map<String, Object> category = categories.computeIfAbsent(categoryId, id -> {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("ticketCategoryId", String.valueOf(id));
                item.put("remainHashKey", RedisKeyBuild.createRedisKey(
                        RedisKeyManage.PROGRAM_TICKET_REMAIN_NUMBER_HASH_RESOLUTION, programId, id).getRelKey());
                item.put("seatLockHashKey", RedisKeyBuild.createRedisKey(
                        RedisKeyManage.PROGRAM_SEAT_LOCK_RESOLUTION_HASH, programId, id).getRelKey());
                item.put("seatNoSoldHashKey", RedisKeyBuild.createRedisKey(
                        RedisKeyManage.PROGRAM_SEAT_NO_SOLD_RESOLUTION_HASH, programId, id).getRelKey());
                item.put("seatIds", new ArrayList<String>());
                return item;
            });
            @SuppressWarnings("unchecked")
            List<String> seatIds = (List<String>) category.get("seatIds");
            seatIds.add(String.valueOf(seat.getId()));
        }
        Long released = (Long) redisCache.getInstance().execute(script, List.of(
                RedisKeyBuild.getRedisKey(RedisKeyManage.PROGRAM_ORDER_RESERVATION_LEDGER),
                RedisKeyBuild.getRedisKey(RedisKeyManage.PROGRAM_ORDER_RESERVATION_DUE)),
                String.valueOf(orderNumber), JSON.toJSONString(categories.values()), reason);
        if (released == null) {
            throw new IllegalStateException("Redis returned no result for reservation release");
        }
        return released;
    }
}
