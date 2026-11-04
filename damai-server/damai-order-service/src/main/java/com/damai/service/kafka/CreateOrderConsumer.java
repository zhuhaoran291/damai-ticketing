package com.damai.service.kafka;

import com.alibaba.fastjson.JSON;
import com.damai.context.DelayQueueContext;
import com.damai.core.RedisKeyManage;
import com.damai.core.SpringUtil;
import com.damai.dto.DelayOrderCancelDto;
import com.damai.dto.OrderCreateDto;
import com.damai.redis.RedisCache;
import com.damai.redis.RedisKeyBuild;
import com.damai.service.OrderService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;

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
    private static final DefaultRedisScript<Long> COMPLETE_PENDING = new DefaultRedisScript<>(
            "if redis.call('hget', KEYS[2], ARGV[2]) == ARGV[1] then " +
                    "redis.call('hdel', KEYS[2], ARGV[2]) end " +
                    "return redis.call('hdel', KEYS[1], ARGV[1])", Long.class);
    
    @Autowired
    private OrderService orderService;
    @Autowired
    private RedisCache redisCache;
    @Autowired
    private DelayQueueContext delayQueueContext;
    
    /**
     * ⚠️注意！在升级的大麦pro版本中进行了优化，加入了消息延迟的废单统计、补偿机制、消息对账、实时监控等功能
     * ✅大麦pro升级功能的详细介绍，请看 <a href="https://javaup.chat/damai/damai-pro/release-intro">...</a>
     * ✨如何获取大麦pro项目？请看 <a href="https://articles.zsxq.com/id_m4d7ni4zwkbq.html">...</a>
     **/
    @KafkaListener(topics = {SPRING_INJECT_PREFIX_DISTINCTION_NAME+"-"+"${spring.kafka.topic:create_order}"})
    public void consumerOrderMessage(ConsumerRecord<String,String> consumerRecord){
        if (consumerRecord.value() == null) {
            return;
        }
        try {
            OrderCreateDto order = JSON.parseObject(consumerRecord.value(), OrderCreateDto.class);
            // The DB transaction has committed when createMq returns.
            String orderNumber = orderService.createMq(order);
            redisCache.set(RedisKeyBuild.createRedisKey(RedisKeyManage.ORDER_MQ, orderNumber),
                    orderNumber, 1, TimeUnit.MINUTES);
            DelayOrderCancelDto cancel = new DelayOrderCancelDto();
            cancel.setOrderNumber(order.getOrderNumber());
            delayQueueContext.sendMessage(SpringUtil.getPrefixDistinctionName() + "-" + DELAY_ORDER_CANCEL_TOPIC,
                    JSON.toJSONString(cancel), 10L, TimeUnit.MINUTES);
            redisCache.getInstance().execute(COMPLETE_PENDING, List.of(
                    RedisKeyBuild.getRedisKey(RedisKeyManage.PROGRAM_ORDER_PENDING),
                    RedisKeyBuild.getRedisKey(RedisKeyManage.PROGRAM_ORDER_PENDING_BY_USER)),
                    orderNumber, order.getUserId() + ":" + order.getProgramId());
            log.info("订单创建完成 orderNumber: {}", orderNumber);
        }catch (Exception e) {
            // Keep the pending record so the producer can retry.
            log.error("处理创建订单消息失败，等待重试",e);
        }
    }
}
