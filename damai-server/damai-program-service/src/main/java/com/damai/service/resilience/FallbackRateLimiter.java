package com.damai.service.resilience;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.function.LongSupplier;

/**
 * @program: 极度真实还原大麦网高并发实战项目。 添加 阿星不是程序员 微信，添加时备注 大麦 来获取项目的完整资料 
 * @description: 搜索降级（查库）的令牌桶限流器
 * @author: 阿星不是程序员
 **/
@Slf4j
@Component
public class FallbackRateLimiter {

    private final double permitsPerSecond;
    private final double capacity;
    private final LongSupplier nanoClock;

    private double tokens;
    private long lastNanos;

    @Autowired
    public FallbackRateLimiter(
            @Value("${search.fallback-rate-limit.permits-per-second:50}") double permitsPerSecond,
            @Value("${search.fallback-rate-limit.bucket-capacity:100}") double capacity) {
        this(permitsPerSecond, capacity, System::nanoTime);
    }

    /** 供测试注入假时钟 */
    public FallbackRateLimiter(double permitsPerSecond, double capacity, LongSupplier nanoClock) {
        this.permitsPerSecond = permitsPerSecond;
        this.capacity = capacity;
        this.nanoClock = nanoClock;
        this.tokens = capacity;
        this.lastNanos = nanoClock.getAsLong();
    }

    /**
     * 尝试取一个令牌。
     * 拿不到就拒绝：宁可让用户看到"搜索暂不可用"，也不能让 like 全表扫把数据库拖死。
     * */
    public synchronized boolean tryAcquire() {
        long now = nanoClock.getAsLong();
        double add = (now - lastNanos) / 1_000_000_000.0 * permitsPerSecond;
        if (add > 0) {
            tokens = Math.min(capacity, tokens + add);
            lastNanos = now;
        }
        if (tokens >= 1) {
            tokens -= 1;
            return true;
        }
        return false;
    }

    /** 当前可用令牌数（仅用于日志/测试） */
    public synchronized double availableTokens() {
        return tokens;
    }
}
