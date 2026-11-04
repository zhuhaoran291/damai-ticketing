package com.damai.service;

import com.damai.dto.OrderCreateDto;
import com.damai.core.SpringUtil;
import com.damai.service.kafka.KafkaTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.mockito.MockedStatic;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import com.damai.exception.DaMaiFrameException;
import com.damai.enums.BaseCode;
import com.damai.mq.callback.FailureCallback;
import com.damai.mq.callback.SuccessCallback;
import com.damai.service.kafka.CreateOrderSend;
import com.damai.service.lua.ProgramCacheResolutionOperate;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.SendResult;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import java.time.Duration;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ProgramOrderMqWaitTest {
    private ProgramOrderService service;
    private CreateOrderSend sender;
    private ProgramCacheResolutionOperate cache;
    private OrderCreateDto order;

    @BeforeEach
    void setUp() throws Exception {
        service = new ProgramOrderService();
        sender = mock(CreateOrderSend.class);
        cache = mock(ProgramCacheResolutionOperate.class);
        setField("createOrderSend", sender);
        setField("programCacheResolutionOperate", cache);
        setField("mqWaitMillis", 100L);
        order = new OrderCreateDto();
        order.setOrderNumber(123L);
        order.setProgramId(9L);
    }

    private void setField(String name, Object value) throws Exception {
        Field field = ProgramOrderService.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(service, value);
    }

    private String createOrder() {
        try {
            Method method = ProgramOrderService.class.getDeclaredMethod("createOrderByMq", OrderCreateDto.class, List.class);
            method.setAccessible(true);
            return (String) method.invoke(service, order, Collections.emptyList());
        } catch (InvocationTargetException ex) {
            throw (RuntimeException) ex.getCause();
        } catch (ReflectiveOperationException ex) {
            throw new AssertionError(ex);
        }
    }

    @Test
    void noCallbackTimesOutWithoutRestoringSeats() {
        DaMaiFrameException exception = assertTimeoutPreemptively(Duration.ofSeconds(2),
                () -> assertThrows(DaMaiFrameException.class, this::createOrder));
        assertEquals(BaseCode.ORDER_CREATE_RESULT_UNKNOWN.getCode(), exception.getCode());
        assertTrue(exception.getMessage().contains("订单结果待确认"));
        assertTrue(exception.getMessage().contains("123"));
        verifyNoInteractions(cache);
    }

    @Test
    void successfulCallbackReturnsOrderNumber() {
        @SuppressWarnings("unchecked") SendResult<String, String> sendResult = mock(SendResult.class);
        RecordMetadata metadata = mock(RecordMetadata.class);
        when(sendResult.getRecordMetadata()).thenReturn(metadata);
        when(metadata.topic()).thenReturn("create_order");
        doAnswer(invocation -> {
            SuccessCallback<SendResult<String, String>> success = invocation.getArgument(1);
            success.onSuccess(sendResult);
            return null;
        }).when(sender).sendMessage(anyString(), any(), any());
        assertEquals("123", createOrder());
    }

    @Test
    void recoveryExceptionStillReleasesTheWaiter() {
        // The empty seat list still reaches the mocked cache operation.
        doThrow(new IllegalStateException("redis unavailable"))
                .when(cache).programCacheOperate(anyList(), any(String[].class));
        doAnswer(invocation -> {
            FailureCallback failure = invocation.getArgument(2);
            failure.onFailure(new RuntimeException("send failed"));
            return null;
        }).when(sender).sendMessage(anyString(), any(), any());
        DaMaiFrameException exception = assertTimeoutPreemptively(Duration.ofSeconds(2),
                () -> assertThrows(DaMaiFrameException.class, this::createOrder));
        assertEquals(BaseCode.ORDER_CREATE_RESULT_UNKNOWN.getCode(), exception.getCode());
        assertTrue(exception.getMessage().contains("订单结果待确认"));
    }

    @Test
    void completedFutureDoesNotRunBlockingCallbackOnRequestThread() throws Exception {
        @SuppressWarnings("unchecked") KafkaTemplate<String, String> template = mock(KafkaTemplate.class);
        when(template.send(anyString(), anyString())).thenReturn(
                CompletableFuture.failedFuture(new IllegalStateException("send failed")));
        KafkaTopic topic = new KafkaTopic();
        topic.setTopic("orders");
        CreateOrderSend realSender = new CreateOrderSend(template, topic);
        CountDownLatch callbackStarted = new CountDownLatch(1);
        CountDownLatch releaseCallback = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> request = executor.submit(() -> {
                try (MockedStatic<SpringUtil> spring = mockStatic(SpringUtil.class)) {
                    spring.when(SpringUtil::getPrefixDistinctionName).thenReturn("test");
                    realSender.sendMessage("{}", ignored -> fail("unexpected success"), ex -> {
                        callbackStarted.countDown();
                        try {
                            releaseCallback.await();
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                        }
                    });
                }
            });
            request.get(3, TimeUnit.SECONDS);
            assertTrue(callbackStarted.await(3, TimeUnit.SECONDS));
        } finally {
            releaseCallback.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void unavailableKafkaDoesNotBlockForever() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:1");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000);
        DefaultKafkaProducerFactory<String, String> factory = new DefaultKafkaProducerFactory<>(config);
        KafkaTopic topic = new KafkaTopic();
        topic.setTopic("unavailable-broker-test");
        setField("createOrderSend", new CreateOrderSend(new KafkaTemplate<>(factory), topic));
        try (MockedStatic<SpringUtil> spring = mockStatic(SpringUtil.class)) {
            spring.when(SpringUtil::getPrefixDistinctionName).thenReturn("test");
            long start = System.nanoTime();
            DaMaiFrameException exception = assertThrows(DaMaiFrameException.class, this::createOrder);
            assertNotNull(exception.getCode());
            assertTrue(Duration.ofNanos(System.nanoTime() - start).toMillis() < 5000);
        } finally {
            factory.destroy();
        }
    }

    @Test
    void synchronousSendExceptionIsReportedAsUnknown() {
        doThrow(new IllegalStateException("send unavailable")).when(sender).sendMessage(anyString(), any(), any());
        DaMaiFrameException exception = assertThrows(DaMaiFrameException.class, this::createOrder);
        assertTrue(exception.getMessage().contains("订单结果待确认"));
        verifyNoInteractions(cache);
    }
}
