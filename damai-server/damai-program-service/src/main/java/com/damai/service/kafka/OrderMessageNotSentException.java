package com.damai.service.kafka;

/** Metadata lookup failed before KafkaTemplate.send was invoked. */
public class OrderMessageNotSentException extends RuntimeException {
    public OrderMessageNotSentException(Throwable cause) {
        super("Kafka metadata lookup failed before sending the order message", cause);
    }
}
