package com.damai.controller;

import com.damai.common.ApiResponse;
import com.damai.dto.ProgramOrderCreateDto;
import com.damai.enums.ProgramOrderVersion;
import com.damai.exception.OrderPendingException;
import com.damai.service.strategy.ProgramOrderContext;
import com.damai.service.strategy.ProgramOrderStrategy;
import com.damai.service.ProgramOrderService;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verifyNoInteractions;

class ProgramOrderPendingResponseTest {
    @Test
    void v4ReturnsTheOrderNumberWhenItsSendResultIsPending() throws Exception {
        ProgramOrderController controller = new ProgramOrderController();
        ProgramOrderContext context = mock(ProgramOrderContext.class);
        ProgramOrderStrategy strategy = mock(ProgramOrderStrategy.class);
        when(context.get(ProgramOrderVersion.V4_VERSION.getVersion())).thenReturn(strategy);
        when(strategy.createOrder(any())).thenThrow(new OrderPendingException(123L));
        Field field = ProgramOrderController.class.getDeclaredField("programOrderContext");
        field.setAccessible(true);
        field.set(controller, context);

        ApiResponse<String> response = controller.createV4(new ProgramOrderCreateDto());

        assertEquals(515, response.getCode());
        assertEquals("123", response.getData());
        assertEquals("订单结果确认中，请勿重复提交", response.getMessage());
    }

    @Test
    void retryOfAnExistingRequestReturnsTheOriginalOrderWithoutStartingAnother() throws Exception {
        ProgramOrderController controller = new ProgramOrderController();
        ProgramOrderContext context = mock(ProgramOrderContext.class);
        ProgramOrderService orders = mock(ProgramOrderService.class);
        ProgramOrderCreateDto request = new ProgramOrderCreateDto();
        request.setRequestId("retry-1");
        when(orders.getExistingRequestOrder(request)).thenReturn("123");
        Field contextField = ProgramOrderController.class.getDeclaredField("programOrderContext");
        contextField.setAccessible(true);
        contextField.set(controller, context);
        Field ordersField = ProgramOrderController.class.getDeclaredField("programOrderService");
        ordersField.setAccessible(true);
        ordersField.set(controller, orders);

        ApiResponse<String> response = controller.createV4(request);

        assertEquals(515, response.getCode());
        assertEquals("123", response.getData());
        verifyNoInteractions(context);
    }
}
