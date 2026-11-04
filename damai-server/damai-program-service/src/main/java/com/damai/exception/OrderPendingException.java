package com.damai.exception;

import com.damai.enums.BaseCode;

/** The send result is unknown; the order number is needed to check it later. */
public class OrderPendingException extends DaMaiFrameException {
    private final String orderNumber;

    public OrderPendingException(Long orderNumber) {
        super(BaseCode.EXECUTE_TIME_OUT.getCode(), "订单结果确认中，请勿重复提交");
        this.orderNumber = String.valueOf(orderNumber);
    }

    public String getOrderNumber() {
        return orderNumber;
    }
}
