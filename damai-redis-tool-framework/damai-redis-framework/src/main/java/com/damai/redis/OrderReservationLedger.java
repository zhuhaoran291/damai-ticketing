package com.damai.redis;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.damai.core.RedisKeyManage;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Component;

import java.util.List;

/** Atomic transitions shared by the producer, consumer and recovery worker. */
@Component
public class OrderReservationLedger {
    private DefaultRedisScript<Long> claim;
    private DefaultRedisScript<Long> complete;
    private DefaultRedisScript<Long> sendStatus;
    private DefaultRedisScript<Long> consumeFailure;

    @Autowired
    private RedisCache redisCache;

    @PostConstruct
    public void init() {
        claim = script("lua/orderReservationClaim.lua");
        complete = script("lua/orderReservationComplete.lua");
        sendStatus = script("lua/orderReservationSendStatus.lua");
        consumeFailure = script("lua/orderReservationConsumeFailure.lua");
    }

    private DefaultRedisScript<Long> script(String path) {
        DefaultRedisScript<Long> result = new DefaultRedisScript<>();
        result.setScriptSource(new ResourceScriptSource(new ClassPathResource(path)));
        result.setResultType(Long.class);
        return result;
    }

    public Long claim(Long orderNumber) {
        return (Long) redisCache.getInstance().execute(claim, List.of(ledgerKey()), String.valueOf(orderNumber));
    }

    public void complete(Long orderNumber) {
        Long result = (Long) redisCache.getInstance().execute(complete, List.of(ledgerKey(), dueKey()), String.valueOf(orderNumber));
        if (result == null || result != 1L) {
            throw new IllegalStateException("Reservation cannot be completed: " + orderNumber);
        }
    }

    public void updateSendStatus(Long orderNumber, String status) {
        redisCache.getInstance().execute(sendStatus, List.of(ledgerKey()), String.valueOf(orderNumber), status);
    }

    public void markConsumeFailure(Long orderNumber) {
        redisCache.getInstance().execute(consumeFailure, List.of(ledgerKey()), String.valueOf(orderNumber));
    }

    public JSONObject find(String orderNumber) {
        Object raw = redisCache.getInstance().opsForHash().get(ledgerKey(), orderNumber);
        return raw == null ? null : JSON.parseObject(String.valueOf(raw));
    }

    public String ledgerKey() {
        return RedisKeyBuild.getRedisKey(RedisKeyManage.PROGRAM_ORDER_RESERVATION_LEDGER);
    }

    public String dueKey() {
        return RedisKeyBuild.getRedisKey(RedisKeyManage.PROGRAM_ORDER_RESERVATION_DUE);
    }
}
