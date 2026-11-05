package com.damai.service;

import com.damai.core.SpringUtil;
import com.damai.dto.OrderCreateDto;
import com.damai.entity.Order;
import com.damai.mapper.OrderMapper;
import com.damai.redis.RedisCache;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class TrackedOrderIdempotencyTest {
    @Test
    void duplicateTrackedMessageReturnsExistingOrderWithoutCreatingAgain() throws Exception {
        OrderService service = spy(new OrderService());
        OrderMapper mapper = mock(OrderMapper.class);
        RedisCache redis = mock(RedisCache.class);
        Field mapperField = OrderService.class.getDeclaredField("orderMapper");
        mapperField.setAccessible(true);
        mapperField.set(service, mapper);
        Field redisField = OrderService.class.getDeclaredField("redisCache");
        redisField.setAccessible(true);
        redisField.set(service, redis);
        Order existing = new Order();
        existing.setOrderNumber(123L);
        when(mapper.selectOne(any())).thenReturn(existing);
        OrderCreateDto dto = new OrderCreateDto();
        dto.setOrderNumber(123L);
        dto.setReservationTracked(true);
        try (MockedStatic<SpringUtil> spring = mockStatic(SpringUtil.class)) {
            spring.when(SpringUtil::getPrefixDistinctionName).thenReturn("test");
            assertEquals("123", service.createMq(dto));
        }
        verify(service, never()).create(any());
        verify(redis).set(any(), eq("123"), eq(1L), eq(TimeUnit.MINUTES));
    }
}
