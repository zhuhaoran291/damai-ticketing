package com.damai.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.damai.core.RedisKeyManage;
import com.damai.dto.ProgramOrderCreateDto;
import com.damai.redis.OrderReservationLedger;
import com.damai.redis.RedisCache;
import com.damai.redis.RedisKeyBuild;
import com.damai.service.lua.ProgramCacheReleaseUnsentReservation;
import com.damai.vo.SeatVo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Reconcile expired reservations without releasing seats once consumption has begun. */
@Slf4j
@Service
public class ReservationRecoveryService {
    @Autowired private RedisCache redisCache;
    @Autowired private OrderReservationLedger ledger;
    @Autowired private ProgramCacheReleaseUnsentReservation release;
    @Autowired private ProgramOrderService orderService;

    @Scheduled(fixedDelayString = "${damai.order.recovery-interval-millis:5000}", scheduler = "reservationRecoveryScheduler")
    @SuppressWarnings("unchecked")
    public void reconcile() {
        try {
            Set<Object> due = redisCache.getInstance().opsForZSet().rangeByScore(
                    ledger.dueKey(), 0, System.currentTimeMillis(), 0, 100);
            if (due == null) return;
            for (Object value : due) {
                String orderNumber = String.valueOf(value);
                try {
                    JSONObject record = ledger.find(orderNumber);
                    if (record == null) {
                        log.error("预留记录缺失，不能自动释放座位 orderNumber: {}", orderNumber);
                        postpone(orderNumber);
                        continue;
                    }
                    String state = record.getString("state");
                    if ("PENDING".equals(state)) {
                        ProgramOrderCreateDto request = JSON.parseObject(record.getString("request"), ProgramOrderCreateDto.class);
                        List<SeatVo> seats = record.getJSONArray("seats").toJavaList(SeatVo.class);
                        long released = release.releaseExpired(request.getProgramId(), Long.valueOf(orderNumber), seats);
                        if (released > 0) log.info("过期待处理订单已关闭并释放座位 orderNumber: {} count: {}", orderNumber, released);
                    } else if ("PROCESSING".equals(state)) {
                        String lockKey = RedisKeyBuild.createRedisKey(RedisKeyManage.PROGRAM_ORDER_RECOVERY_LOCK, orderNumber).getRelKey();
                        Boolean acquired = redisCache.getInstance().opsForValue().setIfAbsent(lockKey, "1", 10, TimeUnit.SECONDS);
                        if (Boolean.TRUE.equals(acquired)) {
                            redisCache.getInstance().opsForZSet().add(ledger.dueKey(), orderNumber, System.currentTimeMillis() + 10000);
                            orderService.retryProcessingReservation(record);
                        }
                    } else if ("CANCELLED".equals(state) || "COMPLETED".equals(state)) {
                        redisCache.getInstance().opsForZSet().remove(ledger.dueKey(), orderNumber);
                    } else {
                        log.error("预留记录状态未知，不能自动释放座位 orderNumber: {} state: {}", orderNumber, state);
                        postpone(orderNumber);
                    }
                } catch (Exception ex) {
                    log.error("预留订单对账失败，稍后重试 orderNumber: {}", orderNumber, ex);
                    try { postpone(orderNumber); } catch (Exception ignored) { log.warn("对账任务延后失败 orderNumber: {}", orderNumber); }
                }
            }
        } catch (Exception ex) {
            log.error("读取待对账订单失败", ex);
        }
    }

    private void postpone(String orderNumber) {
        redisCache.getInstance().opsForZSet().add(ledger.dueKey(), orderNumber, System.currentTimeMillis() + 10000);
    }
}
