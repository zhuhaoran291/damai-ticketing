package com.damai.service.resilience;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 搜索降级的令牌桶限流（Issue #11）。
 *
 * <p>ES 挂了以后所有流量都会转到兜底 SQL（`like '%x%'` 全表扫描），
 * 没有限流就等于"用数据库给检索层陪葬"。
 */
class FallbackRateLimiterTest {

    private long now = 0;

    /** 每秒补 10 个，桶容量 5 */
    private FallbackRateLimiter limiter() {
        return new FallbackRateLimiter(10, 5, () -> now);
    }

    @Test
    @DisplayName("初始桶内令牌可以一次突发打完，超出立即拒绝")
    void allowsBurstUpToCapacity() {
        FallbackRateLimiter l = limiter();
        for (int i = 0; i < 5; i++) {
            assertTrue(l.tryAcquire(), "桶容量内的前 5 个应该放行");
        }
        assertFalse(l.tryAcquire(), "令牌用完必须立刻拒绝（0 耗时）");
    }

    @Test
    @DisplayName("时间推移后按速率补令牌，但不超过桶容量")
    void refillsOverTime() {
        FallbackRateLimiter l = limiter();
        for (int i = 0; i < 5; i++) {
            l.tryAcquire();
        }
        assertFalse(l.tryAcquire());

        now += 1_000_000_000L;          // 过 1 秒，补 10 个，但上限是 5
        for (int i = 0; i < 5; i++) {
            assertTrue(l.tryAcquire());
        }
        assertFalse(l.tryAcquire(), "补充数量被桶容量封顶");
    }

    @Test
    @DisplayName("拒绝后不会扣令牌，下一轮补充仍然可用")
    void rejectionDoesNotConsumeTokens() {
        FallbackRateLimiter l = limiter();
        for (int i = 0; i < 6; i++) {
            l.tryAcquire();
        }
        now += 500_000_000L;            // 过 0.5 秒，补 5 个
        assertTrue(l.tryAcquire());
        assertTrue(l.tryAcquire());
    }
}
