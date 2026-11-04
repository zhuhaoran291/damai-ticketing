package com.damai.service.resilience;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ES 熔断器（Issue #11）。
 *
 * <p>熔断不是为了"让请求成功"，而是为了在下游持续失败时**停止无效调用**：
 * 打开期间 0 耗时拒绝，不再让每个请求白等 2 秒超时；冷却后放探针试探，恢复了自动关闭。
 */
class EsCircuitBreakerTest {

    /** 假时钟（纳秒），避免测试里真的 sleep */
    private long now = 0;

    /** 窗口 20、最少 10 次、失败率 50%、跳闸 10 秒、半开放 1 个探针 */
    private EsCircuitBreaker breaker() {
        return new EsCircuitBreaker(20, 10, 0.5, 10, 1, () -> now);
    }

    @Test
    @DisplayName("失败率不到阈值，保持 CLOSED")
    void staysClosedWhenFailureRateBelowThreshold() {
        EsCircuitBreaker b = breaker();
        for (int i = 0; i < 9; i++) {
            b.onSuccess();
        }
        b.onFailure();

        assertEquals(EsCircuitBreaker.State.CLOSED, b.getState());
        assertTrue(b.allowRequest(), "没跳闸就应该继续放行");
    }

    @Test
    @DisplayName("达到最少调用数且失败率 ≥ 阈值 → OPEN，并立即拒绝请求")
    void opensWhenFailureRateReached() {
        EsCircuitBreaker b = breaker();
        for (int i = 0; i < 9; i++) {
            b.onFailure();
        }
        assertEquals(EsCircuitBreaker.State.CLOSED, b.getState(), "不足最少调用数不跳闸");

        b.onFailure();
        assertEquals(EsCircuitBreaker.State.OPEN, b.getState());
        assertFalse(b.allowRequest(), "OPEN 期间必须直接拒绝，不能再去调 ES");
    }

    @Test
    @DisplayName("冷却结束后进入 HALF_OPEN，且只放指定数量的探针")
    void halfOpenAllowsLimitedProbes() {
        EsCircuitBreaker b = breaker();
        for (int i = 0; i < 10; i++) {
            b.onFailure();
        }
        now += 10_000_000_000L;

        assertTrue(b.allowRequest(), "冷却结束后放第一个探针");
        assertEquals(EsCircuitBreaker.State.HALF_OPEN, b.getState());
        assertFalse(b.allowRequest(), "探针数量用完后继续拒绝");
    }

    @Test
    @DisplayName("探针失败 → 重新 OPEN，再等一个冷却周期")
    void probeFailureReopens() {
        EsCircuitBreaker b = breaker();
        for (int i = 0; i < 10; i++) {
            b.onFailure();
        }
        now += 10_000_000_000L;
        assertTrue(b.allowRequest());
        b.onFailure();

        assertEquals(EsCircuitBreaker.State.OPEN, b.getState());
        assertFalse(b.allowRequest());
    }

    @Test
    @DisplayName("探针成功 → 回到 CLOSED，恢复正常放行")
    void probeSuccessCloses() {
        EsCircuitBreaker b = breaker();
        for (int i = 0; i < 10; i++) {
            b.onFailure();
        }
        now += 10_000_000_000L;
        assertTrue(b.allowRequest());
        b.onSuccess();

        assertEquals(EsCircuitBreaker.State.CLOSED, b.getState());
        assertTrue(b.allowRequest());
    }

    @Test
    @DisplayName("探针成功关闭后窗口清零，需要重新累积失败才会再次跳闸")
    void windowResetAfterClose() {
        EsCircuitBreaker b = breaker();
        for (int i = 0; i < 10; i++) {
            b.onFailure();
        }
        now += 10_000_000_000L;
        assertTrue(b.allowRequest());
        b.onSuccess();
        assertEquals(EsCircuitBreaker.State.CLOSED, b.getState());

        for (int i = 0; i < 9; i++) {
            b.onFailure();
        }
        assertEquals(EsCircuitBreaker.State.CLOSED, b.getState(), "关掉之后窗口重新计数，9 次还没到最少调用数");

        b.onFailure();
        assertEquals(EsCircuitBreaker.State.OPEN, b.getState());
    }
}
