package com.damai.service.resilience;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.function.LongSupplier;

/**
 * @program: 极度真实还原大麦网高并发实战项目。 添加 阿星不是程序员 微信，添加时备注 大麦 来获取项目的完整资料 
 * @description: ES 检索熔断器（CLOSED / OPEN / HALF_OPEN）
 * @author: 阿星不是程序员
 **/
@Slf4j
@Component
public class EsCircuitBreaker {

    public enum State {
        /** 正常放行 */
        CLOSED,
        /** 跳闸：直接拒绝，不再调用 ES */
        OPEN,
        /** 半开：放少量探针试探 */
        HALF_OPEN
    }

    private final int windowSize;
    private final int minCalls;
    private final double failureRateThreshold;
    private final long openNanos;
    private final int halfOpenProbes;
    private final LongSupplier nanoClock;

    /** 最近 windowSize 次调用的结果，true 表示失败（滑动窗口） */
    private final boolean[] outcomes;
    private int cursor;
    private int filled;
    private int failureCount;

    private State state = State.CLOSED;
    private long openUntil;
    private int probesUsed;

    @Autowired
    public EsCircuitBreaker(
            @Value("${search.circuit-breaker.window-size:20}") int windowSize,
            @Value("${search.circuit-breaker.min-calls:10}") int minCalls,
            @Value("${search.circuit-breaker.failure-rate-threshold:0.5}") double failureRateThreshold,
            @Value("${search.circuit-breaker.open-seconds:10}") long openSeconds,
            @Value("${search.circuit-breaker.half-open-probes:1}") int halfOpenProbes) {
        this(windowSize, minCalls, failureRateThreshold, openSeconds, halfOpenProbes, System::nanoTime);
    }

    /** 供测试注入假时钟 */
    public EsCircuitBreaker(int windowSize, int minCalls, double failureRateThreshold,
                            long openSeconds, int halfOpenProbes, LongSupplier nanoClock) {
        this.windowSize = Math.max(1, windowSize);
        this.minCalls = Math.max(1, Math.min(minCalls, this.windowSize));
        this.failureRateThreshold = failureRateThreshold;
        this.openNanos = openSeconds * 1_000_000_000L;
        this.halfOpenProbes = Math.max(1, halfOpenProbes);
        this.nanoClock = nanoClock;
        this.outcomes = new boolean[this.windowSize];
    }

    /**
     * 是否允许调用 ES。
     * OPEN 期间返回 false（0 耗时拒绝，不再产生 2 秒的无效等待）；冷却结束自动转 HALF_OPEN 放探针。
     * */
    public synchronized boolean allowRequest() {
        if (state == State.OPEN) {
            if (nanoClock.getAsLong() < openUntil) {
                return false;
            }
            state = State.HALF_OPEN;
            probesUsed = 0;
            log.warn("ES熔断器 OPEN -> HALF_OPEN，放 {} 个探针试探", halfOpenProbes);
        }
        if (state == State.HALF_OPEN) {
            if (probesUsed >= halfOpenProbes) {
                return false;
            }
            probesUsed++;
        }
        return true;
    }

    public synchronized void onSuccess() {
        if (state == State.HALF_OPEN) {
            state = State.CLOSED;
            resetWindow();
            log.warn("ES熔断器 HALF_OPEN -> CLOSED，ES 已恢复");
            return;
        }
        record(false);
    }

    public synchronized void onFailure() {
        if (state == State.HALF_OPEN) {
            reopen("探针失败");
            return;
        }
        record(true);
        if (filled >= minCalls && (double) failureCount / filled >= failureRateThreshold) {
            reopen("最近 " + filled + " 次调用中失败 " + failureCount + " 次");
        }
    }

    public synchronized State getState() {
        return state;
    }

    private void record(boolean failure) {
        if (filled == windowSize && outcomes[cursor]) {
            failureCount--;
        }
        outcomes[cursor] = failure;
        if (failure) {
            failureCount++;
        }
        cursor = (cursor + 1) % windowSize;
        filled = Math.min(filled + 1, windowSize);
    }

    private void reopen(String reason) {
        state = State.OPEN;
        openUntil = nanoClock.getAsLong() + openNanos;
        probesUsed = 0;
        resetWindow();
        log.error("ES熔断器 CLOSED -> OPEN（{}），{} 秒内不再调用 ES", reason, openNanos / 1_000_000_000L);
    }

    private void resetWindow() {
        cursor = 0;
        filled = 0;
        failureCount = 0;
        java.util.Arrays.fill(outcomes, false);
    }
}
