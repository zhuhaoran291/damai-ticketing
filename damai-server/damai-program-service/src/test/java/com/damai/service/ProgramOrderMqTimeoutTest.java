package com.damai.service;

import com.damai.dto.OrderCreateDto;
import com.damai.dto.ProgramOrderCreateDto;
import com.damai.core.SpringUtil;
import com.damai.exception.DaMaiFrameException;
import com.damai.exception.OrderPendingException;
import com.damai.mq.callback.SuccessCallback;
import com.damai.service.kafka.CreateOrderSend;
import com.damai.service.kafka.KafkaTopic;
import com.damai.service.lua.ProgramCacheResolutionOperate;
import com.damai.redis.RedisCache;
import com.damai.vo.ProgramVo;
import com.damai.vo.SeatVo;
import com.alibaba.fastjson.JSON;
import org.junit.jupiter.api.Test;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.mockito.MockedStatic;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.math.BigDecimal;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class ProgramOrderMqTimeoutTest {
    @Test
    void mismatchedTicketUsersAreRejectedBeforeAnySeatsAreReserved() {
        ProgramOrderCreateDto request = new ProgramOrderCreateDto();
        request.setTicketCount(2);
        request.setTicketUserIdList(List.of(11L));
        DaMaiFrameException failure = assertThrows(DaMaiFrameException.class,
                () -> new ProgramOrderService().createNewAsync(request));
        assertEquals(com.damai.enums.BaseCode.TICKET_USER_COUNT_UNEQUAL_SEAT_COUNT.getCode(), failure.getCode());
    }

    @Test
    void missingKafkaCallbackMustNotBlockTheRequestIndefinitely() throws Exception {
        ProgramOrderService service = new ProgramOrderService();
        Field sender = ProgramOrderService.class.getDeclaredField("createOrderSend");
        sender.setAccessible(true);
        // The mock accepts the send but never invokes either callback.
        sender.set(service, mock(CreateOrderSend.class));

        Method createOrder = ProgramOrderService.class.getDeclaredMethod("createOrderByMq", OrderCreateDto.class);
        createOrder.setAccessible(true);
        OrderCreateDto order = new OrderCreateDto();
        order.setOrderNumber(123L);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> request = executor.submit(() -> {
                try {
                    return createOrder.invoke(service, order);
                } catch (InvocationTargetException e) {
                    throw new RuntimeException(e.getCause());
                } catch (ReflectiveOperationException e) {
                    throw new RuntimeException(e);
                }
            });
            try {
                request.get(4, TimeUnit.SECONDS);
                fail("An unknown send result must be reported as pending");
            } catch (ExecutionException e) {
                OrderPendingException pending = assertInstanceOf(OrderPendingException.class, e.getCause().getCause());
                assertEquals(515, pending.getCode());
                assertEquals("123", pending.getOrderNumber());
            } catch (TimeoutException e) {
                fail("The V4 request is still blocked after the Kafka callback failed to arrive");
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void lateSuccessDoesNotReleaseTheReservedSeats() throws Exception {
        ProgramOrderService service = new ProgramOrderService();
        CreateOrderSend sender = mock(CreateOrderSend.class);
        ProgramCacheResolutionOperate cache = mock(ProgramCacheResolutionOperate.class);
        AtomicReference<SuccessCallback<SendResult<String, String>>> success = new AtomicReference<>();
        doAnswer(invocation -> {
            success.set(invocation.getArgument(1));
            return null;
        }).when(sender).sendMessage(anyString(), any(), any());
        Field senderField = ProgramOrderService.class.getDeclaredField("createOrderSend");
        senderField.setAccessible(true);
        senderField.set(service, sender);
        Field cacheField = ProgramOrderService.class.getDeclaredField("programCacheResolutionOperate");
        cacheField.setAccessible(true);
        cacheField.set(service, cache);
        Method createOrder = ProgramOrderService.class.getDeclaredMethod("createOrderByMq", OrderCreateDto.class);
        createOrder.setAccessible(true);
        OrderCreateDto order = new OrderCreateDto();
        order.setOrderNumber(123L);

        try {
            createOrder.invoke(service, order);
            fail("The request should have timed out before the callback");
        } catch (InvocationTargetException e) {
            assertInstanceOf(DaMaiFrameException.class, e.getCause());
        }
        @SuppressWarnings("unchecked")
        SendResult<String, String> result = mock(SendResult.class);
        when(result.getRecordMetadata()).thenReturn(mock(RecordMetadata.class));
        success.get().onSuccess(result);
        verifyNoInteractions(cache);
    }

    @Test
    void failedSendWithUnknownOutcomeKeepsTheReservationPending() throws Exception {
        ProgramOrderService service = new ProgramOrderService();
        CreateOrderSend sender = mock(CreateOrderSend.class);
        ProgramCacheResolutionOperate cache = mock(ProgramCacheResolutionOperate.class);
        doAnswer(invocation -> {
            com.damai.mq.callback.FailureCallback failure = invocation.getArgument(2);
            failure.onFailure(new org.apache.kafka.common.errors.TimeoutException("acknowledgement lost"));
            return null;
        }).when(sender).sendMessage(anyString(), any(), any());
        for (Map.Entry<String, Object> field : Map.of(
                "createOrderSend", sender, "programCacheResolutionOperate", cache).entrySet()) {
            Field target = ProgramOrderService.class.getDeclaredField(field.getKey());
            target.setAccessible(true);
            target.set(service, field.getValue());
        }
        Method createOrder = ProgramOrderService.class.getDeclaredMethod("createOrderByMq", OrderCreateDto.class);
        createOrder.setAccessible(true);
        OrderCreateDto order = new OrderCreateDto();
        order.setOrderNumber(123L);
        try {
            createOrder.invoke(service, order);
            fail("An unknown send result must remain pending");
        } catch (InvocationTargetException e) {
            assertInstanceOf(OrderPendingException.class, e.getCause());
        }
        verifyNoInteractions(cache);
    }

    @Test
    void lateFailureDoesNotReturnSeatsThatMayAlreadyBeOrdered() throws Exception {
        ProgramOrderService service = new ProgramOrderService();
        CreateOrderSend sender = mock(CreateOrderSend.class);
        ProgramCacheResolutionOperate cache = mock(ProgramCacheResolutionOperate.class);
        AtomicReference<com.damai.mq.callback.FailureCallback> failure = new AtomicReference<>();
        doAnswer(invocation -> {
            failure.set(invocation.getArgument(2));
            return null;
        }).when(sender).sendMessage(anyString(), any(), any());
        for (Map.Entry<String, Object> field : Map.of(
                "createOrderSend", sender, "programCacheResolutionOperate", cache).entrySet()) {
            Field target = ProgramOrderService.class.getDeclaredField(field.getKey());
            target.setAccessible(true);
            target.set(service, field.getValue());
        }
        Method createOrder = ProgramOrderService.class.getDeclaredMethod("createOrderByMq", OrderCreateDto.class);
        createOrder.setAccessible(true);
        OrderCreateDto order = new OrderCreateDto();
        order.setOrderNumber(123L);
        try {
            createOrder.invoke(service, order);
            fail("The request should have timed out before the callback");
        } catch (InvocationTargetException e) {
            assertInstanceOf(OrderPendingException.class, e.getCause());
        }
        failure.get().onFailure(new org.apache.kafka.common.errors.TimeoutException("acknowledgement lost"));
        verifyNoInteractions(cache);
    }

    @Test
    void unavailableKafkaDoesNotKeepTheRequestThreadBlocked() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:1");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000);
        DefaultKafkaProducerFactory<String, String> factory = new DefaultKafkaProducerFactory<>(config);
        KafkaTopic topic = new KafkaTopic();
        topic.setTopic("unavailable-broker-test");
        ProgramOrderService service = new ProgramOrderService();
        Field sender = ProgramOrderService.class.getDeclaredField("createOrderSend");
        sender.setAccessible(true);
        sender.set(service, new CreateOrderSend(new KafkaTemplate<>(factory), topic));
        Field cache = ProgramOrderService.class.getDeclaredField("programCacheResolutionOperate");
        cache.setAccessible(true);
        ProgramCacheResolutionOperate cacheOperation = mock(ProgramCacheResolutionOperate.class);
        cache.set(service, cacheOperation);
        Method createOrder = ProgramOrderService.class.getDeclaredMethod("createOrderByMq", OrderCreateDto.class);
        createOrder.setAccessible(true);
        OrderCreateDto order = new OrderCreateDto();
        order.setOrderNumber(123L);
        order.setProgramId(456L);
        try (MockedStatic<SpringUtil> spring = mockStatic(SpringUtil.class)) {
            spring.when(SpringUtil::getPrefixDistinctionName).thenReturn("test");
            long started = System.nanoTime();
            try {
                createOrder.invoke(service, order);
                fail("An unavailable Kafka broker must not produce a successful response");
            } catch (InvocationTargetException expected) {
                OrderPendingException pending = assertInstanceOf(OrderPendingException.class, expected.getCause());
                assertEquals("123", pending.getOrderNumber());
                assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) < 4000,
                        "Kafka send and callback wait must finish promptly");
            }
            verifyNoInteractions(cacheOperation);
        } finally {
            factory.destroy();
        }
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void recoveryResendsTheSameOrderNumberAndReservedSeats() throws Exception {
        ProgramOrderService service = new ProgramOrderService();
        RedisCache cache = mock(RedisCache.class);
        RedisTemplate template = mock(RedisTemplate.class);
        HashOperations hash = mock(HashOperations.class);
        Cursor cursor = mock(Cursor.class);
        ProgramService programs = mock(ProgramService.class);
        CreateOrderSend sender = mock(CreateOrderSend.class);
        when(cache.getInstance()).thenReturn(template);
        when(template.opsForHash()).thenReturn(hash);
        ProgramOrderCreateDto request = new ProgramOrderCreateDto();
        request.setProgramId(456L);
        request.setUserId(789L);
        request.setTicketUserIdList(List.of(11L));
        SeatVo seat = new SeatVo();
        seat.setId(42L);
        seat.setTicketCategoryId(7L);
        seat.setPrice(BigDecimal.TEN);
        String pending = JSON.toJSONString(Map.of("request", request, "seats", List.of(seat)));
        when(hash.scan(anyString(), any(ScanOptions.class))).thenReturn(cursor);
        when(cursor.hasNext()).thenReturn(true, false);
        when(cursor.next()).thenReturn(new AbstractMap.SimpleImmutableEntry<>("123", pending));
        when(programs.simpleGetProgramAndShowMultipleCache(456L)).thenReturn(new ProgramVo());
        for (Map.Entry<String, Object> field : Map.of(
                "redisCache", cache, "programService", programs, "createOrderSend", sender).entrySet()) {
            Field target = ProgramOrderService.class.getDeclaredField(field.getKey());
            target.setAccessible(true);
            target.set(service, field.getValue());
        }
        try (MockedStatic<SpringUtil> spring = mockStatic(SpringUtil.class)) {
            spring.when(SpringUtil::getPrefixDistinctionName).thenReturn("test");
            service.retryPendingOrders();
        }
        org.mockito.ArgumentCaptor<String> message = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(sender).sendMessage(message.capture(), any(), any());
        OrderCreateDto resent = JSON.parseObject(message.getValue(), OrderCreateDto.class);
        assertEquals(123L, resent.getOrderNumber());
        assertEquals(42L, resent.getOrderTicketUserCreateDtoList().get(0).getSeatId());
    }
}
