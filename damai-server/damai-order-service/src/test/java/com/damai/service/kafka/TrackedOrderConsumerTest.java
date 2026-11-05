package com.damai.service.kafka;

import com.alibaba.fastjson.JSON;
import com.damai.context.DelayQueueContext;
import com.damai.core.SpringUtil;
import com.damai.dto.OrderCreateDto;
import com.damai.redis.OrderReservationLedger;
import com.damai.service.OrderService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class TrackedOrderConsumerTest {
    private final OrderService orders = mock(OrderService.class);
    private final OrderReservationLedger ledger = mock(OrderReservationLedger.class);
    private final DelayQueueContext delay = mock(DelayQueueContext.class);
    private final CreateOrderConsumer consumer = new CreateOrderConsumer(orders, ledger, delay);

    private ConsumerRecord<String, String> message() {
        OrderCreateDto dto = new OrderCreateDto();
        dto.setOrderNumber(123L);
        dto.setCreateOrderTime(new Date());
        dto.setReservationTracked(true);
        return new ConsumerRecord<>("orders", 0, 0, null, JSON.toJSONString(dto));
    }

    @Test
    void reservationTopicRejectsUntrackedMessages() {
        ConsumerRecord<String, String> untracked = new ConsumerRecord<>("reservation", 0, 0, null,
                "{\"orderNumber\":123}");
        assertThrows(IllegalArgumentException.class, () -> consumer.consumerReservationMessage(untracked));
        verifyNoInteractions(orders, ledger, delay);
    }

    @Test
    void closedOrExpiredReservationDoesNotCreateAnOrder() {
        when(ledger.claim(123L)).thenReturn(0L);
        consumer.consumerReservationMessage(message());
        verifyNoInteractions(orders, delay);
        verify(ledger, never()).complete(anyLong());
    }

    @Test
    void missingReservationFailsClosed() {
        when(ledger.claim(123L)).thenReturn(-1L);
        assertThrows(IllegalStateException.class, () -> consumer.consumerReservationMessage(message()));
        verifyNoInteractions(orders, delay);
        verify(ledger, never()).complete(anyLong());
    }

    @Test
    void completesOnlyAfterOrderAndDelayedCancellationAreRecorded() {
        when(ledger.claim(123L)).thenReturn(1L);
        when(orders.createMq(any())).thenReturn("123");
        try (MockedStatic<SpringUtil> spring = mockStatic(SpringUtil.class)) {
            spring.when(SpringUtil::getPrefixDistinctionName).thenReturn("test");
            consumer.consumerReservationMessage(message());
        }
        var calls = inOrder(orders, delay, ledger);
        calls.verify(orders).createMq(any());
        calls.verify(delay).sendMessage(anyString(), anyString(), anyLong(), eq(TimeUnit.MILLISECONDS));
        calls.verify(ledger).complete(123L);
    }

    @Test
    void delayQueueFailureKeepsTheOrderForIdempotentRetry() {
        when(ledger.claim(123L)).thenReturn(1L);
        when(orders.createMq(any())).thenReturn("123");
        doThrow(new IllegalStateException("delay queue unavailable"))
                .when(delay).sendMessage(anyString(), anyString(), anyLong(), eq(TimeUnit.MILLISECONDS));
        try (MockedStatic<SpringUtil> spring = mockStatic(SpringUtil.class)) {
            spring.when(SpringUtil::getPrefixDistinctionName).thenReturn("test");
            assertThrows(IllegalStateException.class, () -> consumer.consumerReservationMessage(message()));
        }
        verify(ledger, never()).complete(anyLong());
        verify(ledger).markConsumeFailure(123L);
    }

    @Test
    void failureDoesNotMarkReservationCompleted() {
        when(ledger.claim(123L)).thenReturn(1L);
        when(orders.createMq(any())).thenThrow(new IllegalStateException("db unavailable"));
        assertThrows(IllegalStateException.class, () -> consumer.consumerReservationMessage(message()));
        verify(ledger, never()).complete(anyLong());
        verifyNoInteractions(delay);
    }
}
