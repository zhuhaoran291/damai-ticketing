package com.damai.service.kafka;

import com.damai.context.DelayQueueContext;
import com.damai.core.SpringUtil;
import com.damai.dto.OrderCreateDto;
import com.damai.dto.OrderGetDto;
import com.damai.entity.Order;
import com.damai.mapper.OrderMapper;
import com.damai.redis.RedisCache;
import com.damai.service.OrderService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class CreateOrderConsumerTest {
    private static final String MESSAGE = "{\"orderNumber\":123,\"programId\":456,\"userId\":789}";

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void clearsPendingOnlyAfterOrderCreationAndCancellationScheduling() {
        OrderService orders = mock(OrderService.class);
        RedisCache cache = mock(RedisCache.class);
        RedisTemplate template = mock(RedisTemplate.class);
        DelayQueueContext delay = mock(DelayQueueContext.class);
        when(cache.getInstance()).thenReturn(template);
        when(orders.createMq(any())).thenReturn("123");
        CreateOrderConsumer consumer = new CreateOrderConsumer(orders, cache, delay);
        try (MockedStatic<SpringUtil> spring = mockStatic(SpringUtil.class)) {
            spring.when(SpringUtil::getPrefixDistinctionName).thenReturn("test");
            consumer.consumerOrderMessage(new ConsumerRecord<>("orders", 0, 0, null, MESSAGE));
        }
        InOrder order = inOrder(orders, cache, delay, template);
        order.verify(orders).createMq(any());
        order.verify(cache).set(any(), eq("123"), eq(1L), eq(TimeUnit.MINUTES));
        order.verify(delay).sendMessage(anyString(), anyString(), eq(10L), eq(TimeUnit.MINUTES));
        order.verify(template).execute(any(DefaultRedisScript.class), anyList(), eq("123"), eq("789:456"));
    }

    @Test
    void leavesPendingWhenOrderCreationFails() {
        OrderService orders = mock(OrderService.class);
        RedisCache cache = mock(RedisCache.class);
        DelayQueueContext delay = mock(DelayQueueContext.class);
        when(orders.createMq(any())).thenThrow(new IllegalStateException("database unavailable"));
        new CreateOrderConsumer(orders, cache, delay)
                .consumerOrderMessage(new ConsumerRecord<>("orders", 0, 0, null, MESSAGE));
        verifyNoInteractions(cache, delay);
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void leavesPendingWhenCancellationCannotBeScheduled() {
        OrderService orders = mock(OrderService.class);
        RedisCache cache = mock(RedisCache.class);
        RedisTemplate template = mock(RedisTemplate.class);
        DelayQueueContext delay = mock(DelayQueueContext.class);
        when(cache.getInstance()).thenReturn(template);
        when(orders.createMq(any())).thenReturn("123");
        doThrow(new IllegalStateException("delay queue unavailable"))
                .when(delay).sendMessage(anyString(), anyString(), eq(10L), eq(TimeUnit.MINUTES));
        CreateOrderConsumer consumer = new CreateOrderConsumer(orders, cache, delay);
        try (MockedStatic<SpringUtil> spring = mockStatic(SpringUtil.class)) {
            spring.when(SpringUtil::getPrefixDistinctionName).thenReturn("test");
            consumer.consumerOrderMessage(new ConsumerRecord<>("orders", 0, 0, null, MESSAGE));
        }
        verifyNoInteractions(template);
    }

    @Test
    void alreadyCreatedOrderIsNotCreatedAgain() throws Exception {
        OrderService orders = spy(new OrderService());
        OrderMapper mapper = mock(OrderMapper.class);
        Order existing = new Order();
        existing.setOrderNumber(123L);
        when(mapper.selectOne(any())).thenReturn(existing);
        Field field = OrderService.class.getDeclaredField("orderMapper");
        field.setAccessible(true);
        field.set(orders, mapper);
        OrderCreateDto dto = new OrderCreateDto();
        dto.setOrderNumber(123L);

        assertEquals("123", orders.createMq(dto));
        verify(orders, never()).create(any());
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void anExistingOrderCanBeFoundAfterItsShortLivedCacheExpires() throws Exception {
        OrderService orders = new OrderService();
        OrderMapper mapper = mock(OrderMapper.class);
        RedisCache cache = mock(RedisCache.class);
        RedisTemplate template = mock(RedisTemplate.class);
        HashOperations hash = mock(HashOperations.class);
        when(cache.getInstance()).thenReturn(template);
        when(template.opsForHash()).thenReturn(hash);
        when(hash.hasKey(anyString(), eq("123"))).thenReturn(false);
        Order existing = new Order();
        existing.setOrderNumber(123L);
        when(mapper.selectOne(any())).thenReturn(existing);
        Field mapperField = OrderService.class.getDeclaredField("orderMapper");
        mapperField.setAccessible(true);
        mapperField.set(orders, mapper);
        Field cacheField = OrderService.class.getDeclaredField("redisCache");
        cacheField.setAccessible(true);
        cacheField.set(orders, cache);
        OrderGetDto query = new OrderGetDto();
        query.setOrderNumber(123L);
        try (MockedStatic<SpringUtil> spring = mockStatic(SpringUtil.class)) {
            spring.when(SpringUtil::getPrefixDistinctionName).thenReturn("test");
            assertEquals("123", orders.getCache(query));
        }
    }
}
