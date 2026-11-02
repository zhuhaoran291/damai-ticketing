package com.damai.service.strategy;

import com.damai.dto.ProgramOrderCreateDto;
import com.damai.enums.ProgramOrderVersion;
import com.damai.repeatexecutelimit.annotion.RepeatExecuteLimit;
import com.damai.service.strategy.impl.ProgramOrderV1Strategy;
import com.damai.service.strategy.impl.ProgramOrderV2Strategy;
import com.damai.service.strategy.impl.ProgramOrderV3Strategy;
import com.damai.service.strategy.impl.ProgramOrderV4Strategy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 下单防重复提交（Issue #7）。
 *
 * <p>重复提交由 {@code RepeatExecuteLimitAspect} 拦截，但它只有在
 * {@code durationTime > 0} 时才会往 Redis 写防重标记；写成默认值 0 就等于
 * 「切面跑了一遍，什么都没留下」，前后两次请求各拿各的锁、各下一单。
 * 这是配置层面的坑，接口测试要等整套环境起来才能发现，所以这里用反射把它钉死。
 *
 * <p>不使用 Spring 上下文，不连 MySQL / Redis / Nacos。
 */
class ProgramOrderRepeatLimitTest {

    private static final List<Class<?>> STRATEGIES = List.of(
            ProgramOrderV1Strategy.class,
            ProgramOrderV2Strategy.class,
            ProgramOrderV3Strategy.class,
            ProgramOrderV4Strategy.class);

    @Test
    @DisplayName("四个版本的下单接口都必须配置防重窗口，否则防重完全不生效")
    void allStrategiesMustDeclareRepeatWindow() {
        for (Class<?> strategy : STRATEGIES) {
            long durationTime = annotationOf(strategy).durationTime();
            assertTrue(durationTime > 0,
                    strategy.getSimpleName() + " 的 durationTime=" + durationTime
                            + "，切面只在 > 0 时写防重标记，等于没开防重");
        }
    }

    @Test
    @DisplayName("四个版本用同一个窗口，避免 v1 挡得住而 v4 挡不住")
    void allStrategiesShareTheSameWindow() {
        List<Long> windows = STRATEGIES.stream()
                .map(strategy -> annotationOf(strategy).durationTime())
                .distinct()
                .collect(Collectors.toList());
        assertEquals(1, windows.size(), "各版本防重窗口不一致：" + windows);
    }

    @Test
    @DisplayName("被拦下时要有可展示的提示，不能是空 message")
    void repeatMessageMustNotBeBlank() {
        for (Class<?> strategy : STRATEGIES) {
            String message = annotationOf(strategy).message();
            assertTrue(message != null && !message.isBlank(),
                    strategy.getSimpleName() + " 的防重提示为空，前端会弹空白 toast");
        }
    }

    @Test
    @DisplayName("策略清单不能过期：ProgramOrderVersion 加了版本，这里也要一起加")
    void strategyListMustCoverEveryVersion() {
        assertEquals(ProgramOrderVersion.values().length, STRATEGIES.size(),
                "新增了下单版本但没有纳入本用例：请把新的 Strategy 加进 STRATEGIES");
    }

    private RepeatExecuteLimit annotationOf(Class<?> strategy) {
        Method createOrder;
        try {
            createOrder = strategy.getDeclaredMethod("createOrder", ProgramOrderCreateDto.class);
        } catch (NoSuchMethodException e) {
            throw new AssertionError(strategy.getSimpleName() + " 没有声明 createOrder(ProgramOrderCreateDto)", e);
        }
        RepeatExecuteLimit annotation = createOrder.getAnnotation(RepeatExecuteLimit.class);
        assertNotNull(annotation, strategy.getSimpleName() + ".createOrder 上缺少 @RepeatExecuteLimit");
        return annotation;
    }
}
