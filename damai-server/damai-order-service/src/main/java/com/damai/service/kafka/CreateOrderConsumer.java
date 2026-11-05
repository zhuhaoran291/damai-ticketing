package com.damai.service.kafka;

import com.alibaba.fastjson.JSON;
import com.damai.dto.OrderCreateDto;
import com.damai.dto.DelayOrderCancelDto;
import com.damai.context.DelayQueueContext;
import com.damai.core.SpringUtil;
import com.damai.redis.OrderReservationLedger;
import com.damai.dto.OrderTicketUserCreateDto;
import com.damai.enums.OrderStatus;
import com.damai.service.OrderService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.damai.constant.Constant.SPRING_INJECT_PREFIX_DISTINCTION_NAME;
import static com.damai.service.constant.OrderConstant.DELAY_ORDER_CANCEL_TOPIC;

/**
 * @program: 极度真实还原大麦网高并发实战项目。 添加 阿星不是程序员 微信，添加时备注 大麦 来获取项目的完整资料 
 * @description: kafka 创建订单 消费
 * @author: 阿星不是程序员
 **/
@Slf4j
@AllArgsConstructor
@Component
public class CreateOrderConsumer {
    
    @Autowired
    private OrderService orderService;

    @Autowired
    private OrderReservationLedger reservationLedger;

    @Autowired
    private DelayQueueContext delayQueueContext;
    
    public static Long MESSAGE_DELAY_TIME = 5000L;
    
    /**
     * ⚠️注意！在升级的大麦pro版本中进行了优化，加入了消息延迟的废单统计、补偿机制、消息对账、实时监控等功能
     * ✅大麦pro升级功能的详细介绍，请看 <a href="https://javaup.chat/damai/damai-pro/release-intro">...</a>
     * ✨如何获取大麦pro项目？请看 <a href="https://articles.zsxq.com/id_m4d7ni4zwkbq.html">...</a>
     **/
    @KafkaListener(topics = {SPRING_INJECT_PREFIX_DISTINCTION_NAME+"-"+"${spring.kafka.topic:create_order}"})
    public void consumerOrderMessage(ConsumerRecord<String,String> consumerRecord){
        try {
            Optional.ofNullable(consumerRecord.value()).map(String::valueOf).ifPresent(value -> {
                
                OrderCreateDto orderCreateDto = JSON.parseObject(value, OrderCreateDto.class);
                if (Boolean.TRUE.equals(orderCreateDto.getReservationTracked())) {
                    try {
                        consumeTrackedOrder(orderCreateDto);
                    } catch (RuntimeException ex) {
                        try {
                            reservationLedger.markConsumeFailure(orderCreateDto.getOrderNumber());
                        } catch (RuntimeException recordException) {
                            log.error("记录订单消费异常状态失败 orderNumber: {}", orderCreateDto.getOrderNumber(), recordException);
                        }
                        throw ex;
                    }
                    return;
                }
                
                long createOrderTimeTimestamp = orderCreateDto.getCreateOrderTime().getTime();
                
                long currentTimeTimestamp = System.currentTimeMillis();
                
                long delayTime = currentTimeTimestamp - createOrderTimeTimestamp;
                
                log.info("消费到kafka的创建订单消息 消息体: {} 延迟时间 : {} 毫秒",value,delayTime);
                
                if (currentTimeTimestamp - createOrderTimeTimestamp > MESSAGE_DELAY_TIME) {
                    log.info("消费到kafka的创建订单消息延迟时间大于了 {} 毫秒 此订单消息被丢弃 订单号 : {}",
                            delayTime,orderCreateDto.getOrderNumber());
                    Map<Long, List<OrderTicketUserCreateDto>> orderTicketUserSeatList =
                            orderCreateDto.getOrderTicketUserCreateDtoList().stream().collect(Collectors.groupingBy(OrderTicketUserCreateDto::getTicketCategoryId));
                    Map<Long,List<Long>> seatMap = new HashMap<>(orderTicketUserSeatList.size());
                    orderTicketUserSeatList.forEach((k,v) -> {
                        seatMap.put(k,v.stream().map(OrderTicketUserCreateDto::getSeatId).collect(Collectors.toList()));
                    });
                    orderService.updateProgramRelatedDataMq(orderCreateDto.getProgramId(),seatMap, OrderStatus.CANCEL);
                }else {
                    String orderNumber = orderService.createMq(orderCreateDto);
                    log.info("消费到kafka的创建订单消息 创建订单成功 订单号 : {}",orderNumber);
                }
            });
        }catch (Exception e) {
            log.error("处理消费到kafka的创建订单消息失败 error",e);
            throw new IllegalStateException("创建订单消息处理失败", e);
        }
    }

    @KafkaListener(topics = {SPRING_INJECT_PREFIX_DISTINCTION_NAME+"-"+"${spring.kafka.reservation-topic:create_order_reservation}"},
            groupId = "${spring.kafka.consumer.group-id:create_order_data}-reservation",
            properties = {"auto.offset.reset=earliest"})
    public void consumerReservationMessage(ConsumerRecord<String,String> consumerRecord) {
        if (consumerRecord.value() == null) {
            throw new IllegalArgumentException("预留订单消息为空");
        }
        OrderCreateDto order = JSON.parseObject(consumerRecord.value(), OrderCreateDto.class);
        if (!Boolean.TRUE.equals(order.getReservationTracked())) {
            throw new IllegalArgumentException("预留订单消息缺少状态标记");
        }
        consumerOrderMessage(consumerRecord);
    }

    private void consumeTrackedOrder(OrderCreateDto order) {
        Long claim = reservationLedger.claim(order.getOrderNumber());
        if (claim == null || claim < 0) {
            throw new IllegalStateException("订单预留记录缺失或状态无效: " + order.getOrderNumber());
        }
        if (claim == 0) {
            log.info("订单预留已关闭、已完成或已过期，跳过创建 orderNumber: {}", order.getOrderNumber());
            return;
        }
        String orderNumber = orderService.createMq(order);
        DelayOrderCancelDto cancel = new DelayOrderCancelDto();
        cancel.setOrderNumber(order.getOrderNumber());
        long delayMillis = Math.max(0, order.getCreateOrderTime().getTime() + TimeUnit.MINUTES.toMillis(10)
                - System.currentTimeMillis());
        delayQueueContext.sendMessage(SpringUtil.getPrefixDistinctionName() + "-" + DELAY_ORDER_CANCEL_TOPIC,
                JSON.toJSONString(cancel), delayMillis, TimeUnit.MILLISECONDS);
        reservationLedger.complete(order.getOrderNumber());
        log.info("订单创建完成并更新预留状态 orderNumber: {}", orderNumber);
    }
}
