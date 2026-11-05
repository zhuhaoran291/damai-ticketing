package com.damai.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.damai.core.SpringUtil;
import com.damai.redis.OrderReservationLedger;
import com.damai.redis.RedisCache;
import com.damai.service.lua.ProgramCacheReleaseUnsentReservation;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.ZSetOperations;

import java.lang.reflect.Field;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@SuppressWarnings({"rawtypes", "unchecked"})
class ReservationRecoveryServiceTest {
    private void set(Object service, String name, Object value) throws Exception {
        Field field = ReservationRecoveryService.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(service, value);
    }

    private JSONObject record(String state) {
        JSONObject record = new JSONObject();
        record.put("state", state);
        record.put("request", "{\"programId\":4,\"userId\":10000}");
        record.put("seats", JSON.parseArray("[{\"id\":81,\"ticketCategoryId\":6}]"));
        return record;
    }

    @Test
    void expiredPendingIsReleasedByOwnershipCheckedScript() throws Exception {
        ReservationRecoveryService service = new ReservationRecoveryService();
        RedisCache cache = mock(RedisCache.class);
        RedisTemplate template = mock(RedisTemplate.class);
        ZSetOperations zset = mock(ZSetOperations.class);
        OrderReservationLedger ledger = mock(OrderReservationLedger.class);
        ProgramCacheReleaseUnsentReservation release = mock(ProgramCacheReleaseUnsentReservation.class);
        ProgramOrderService orders = mock(ProgramOrderService.class);
        set(service, "redisCache", cache); set(service, "ledger", ledger);
        set(service, "release", release); set(service, "orderService", orders);
        when(cache.getInstance()).thenReturn(template);
        when(template.opsForZSet()).thenReturn(zset);
        when(ledger.dueKey()).thenReturn("due");
        when(zset.rangeByScore(eq("due"), eq(0.0), anyDouble(), eq(0L), eq(100L))).thenReturn(Set.of("123"));
        when(ledger.find("123")).thenReturn(record("PENDING"));
        when(release.releaseExpired(eq(4L), eq(123L), anyList())).thenReturn(1L);
        service.reconcile();
        verify(release).releaseExpired(eq(4L), eq(123L), anyList());
        verifyNoInteractions(orders);
    }

    @Test
    void processingOrderIsRetriedAndNeverReleasedByWorker() throws Exception {
        ReservationRecoveryService service = new ReservationRecoveryService();
        RedisCache cache = mock(RedisCache.class);
        RedisTemplate template = mock(RedisTemplate.class);
        ZSetOperations zset = mock(ZSetOperations.class);
        ValueOperations values = mock(ValueOperations.class);
        OrderReservationLedger ledger = mock(OrderReservationLedger.class);
        ProgramCacheReleaseUnsentReservation release = mock(ProgramCacheReleaseUnsentReservation.class);
        ProgramOrderService orders = mock(ProgramOrderService.class);
        set(service, "redisCache", cache); set(service, "ledger", ledger);
        set(service, "release", release); set(service, "orderService", orders);
        when(cache.getInstance()).thenReturn(template);
        when(template.opsForZSet()).thenReturn(zset);
        when(template.opsForValue()).thenReturn(values);
        when(ledger.dueKey()).thenReturn("due");
        when(zset.rangeByScore(eq("due"), eq(0.0), anyDouble(), eq(0L), eq(100L))).thenReturn(Set.of("123"));
        JSONObject record = record("PROCESSING");
        when(ledger.find("123")).thenReturn(record);
        when(values.setIfAbsent(anyString(), eq("1"), eq(10L), eq(TimeUnit.SECONDS))).thenReturn(true);
        try (MockedStatic<SpringUtil> spring = mockStatic(SpringUtil.class)) {
            spring.when(SpringUtil::getPrefixDistinctionName).thenReturn("test");
            service.reconcile();
        }
        verify(orders).retryProcessingReservation(record);
        verifyNoInteractions(release);
    }
}
