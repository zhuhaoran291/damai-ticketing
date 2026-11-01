package com.damai.service.composite.impl;

import com.damai.client.OrderClient;
import com.damai.client.UserClient;
import com.damai.core.SpringUtil;
import com.damai.dto.ProgramGetDto;
import com.damai.dto.ProgramOrderCreateDto;
import com.damai.enums.BaseCode;
import com.damai.exception.DaMaiFrameException;
import com.damai.redis.RedisCache;
import com.damai.redis.RedisKeyBuild;
import com.damai.service.ProgramService;
import com.damai.service.tool.TokenExpireManager;
import com.damai.vo.ProgramVo;
import com.damai.vo.TicketUserVo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/**
 * 下单校验：每账号限购（Issue #8）。
 *
 * <p>这里不使用 Spring 上下文，也不连接 MySQL / Redis / Nacos：
 * Redis、用户服务、订单服务、节目服务全部 mock 掉，只验证「规则本身」。
 *
 * <p>覆盖的失效形态是「判断分支被删掉/注释掉/比较方向写反」——一旦 execute() 不再抛异常，
 * 下面的用例就会红，而不用等到有人手工跑一遍才发现。
 */
@ExtendWith(MockitoExtension.class)
class ProgramUserExistCheckHandlerTest {

    private static final long USER_ID = 1001L;
    private static final long PROGRAM_ID = 8L;
    private static final long TICKET_USER_ID = 5001L;
    private static final int LIMIT = 6;

    @Mock
    private UserClient userClient;

    @Mock
    private RedisCache redisCache;

    @Mock
    private OrderClient orderClient;

    @Mock
    private ProgramService programService;

    @Mock
    private TokenExpireManager tokenExpireManager;

    @InjectMocks
    private ProgramUserExistCheckHandler handler;

    private MockedStatic<SpringUtil> springUtil;

    @BeforeEach
    void setUp() {
        // RedisKeyBuild.createRedisKey 会读 SpringUtil 的前缀，单测没有 Spring 上下文，直接替换掉
        springUtil = Mockito.mockStatic(SpringUtil.class);
        springUtil.when(SpringUtil::getPrefixDistinctionName).thenReturn("damai");
    }

    @AfterEach
    void tearDown() {
        springUtil.close();
    }

    @Test
    @DisplayName("已购 6 张后再买 1 张必须被拒（Issue #8 的核心场景）")
    void shouldRejectWhenPurchasedEqualsLimit() {
        givenTicketUserOfCurrentUser();
        givenLimit(LIMIT);
        givenPurchased(LIMIT);

        DaMaiFrameException ex = assertThrows(DaMaiFrameException.class, () -> handler.execute(order(1)));

        assertEquals(BaseCode.PER_ACCOUNT_PURCHASE_COUNT_OVER_LIMIT.getCode(), ex.getCode());
    }

    @Test
    @DisplayName("已购 5 张再买 1 张，刚好到限购数，允许")
    void shouldAllowWhenPurchasedPlusCurrentEqualsLimit() {
        givenTicketUserOfCurrentUser();
        givenLimit(LIMIT);
        givenPurchased(LIMIT - 1);

        assertDoesNotThrow(() -> handler.execute(order(1)));
    }

    @Test
    @DisplayName("单笔一次买超过限购数，拒绝")
    void shouldRejectSingleOrderOverLimit() {
        givenTicketUserOfCurrentUser();
        givenLimit(LIMIT);
        givenPurchased(0);

        DaMaiFrameException ex = assertThrows(DaMaiFrameException.class, () -> handler.execute(order(LIMIT + 1)));

        assertEquals(BaseCode.PER_ACCOUNT_PURCHASE_COUNT_OVER_LIMIT.getCode(), ex.getCode());
    }

    @Test
    @DisplayName("单笔一次买满限购数，允许")
    void shouldAllowSingleOrderEqualsLimit() {
        givenTicketUserOfCurrentUser();
        givenLimit(LIMIT);
        givenPurchased(0);

        assertDoesNotThrow(() -> handler.execute(order(LIMIT)));
    }

    @Test
    @DisplayName("节目未配置限购数时不拦截，避免恢复判断后引入 NPE")
    void shouldNotBlockWhenLimitIsNull() {
        givenTicketUserOfCurrentUser();
        givenLimit(null);
        givenPurchased(100);

        assertDoesNotThrow(() -> handler.execute(order(1)));
    }

    private void givenTicketUserOfCurrentUser() {
        TicketUserVo ticketUser = new TicketUserVo();
        ticketUser.setId(TICKET_USER_ID);
        when(redisCache.getValueIsList(any(RedisKeyBuild.class), eq(TicketUserVo.class)))
                .thenReturn(List.of(ticketUser));
    }

    private void givenLimit(Integer limit) {
        ProgramVo programVo = new ProgramVo();
        programVo.setPerAccountLimitPurchaseCount(limit);
        when(programService.detailV2(any(ProgramGetDto.class))).thenReturn(programVo);
    }

    private void givenPurchased(int count) {
        when(redisCache.hasKey(any(RedisKeyBuild.class))).thenReturn(true);
        when(redisCache.get(any(RedisKeyBuild.class), eq(Integer.class))).thenReturn(count);
    }

    private ProgramOrderCreateDto order(int ticketCount) {
        ProgramOrderCreateDto dto = new ProgramOrderCreateDto();
        dto.setUserId(USER_ID);
        dto.setProgramId(PROGRAM_ID);
        dto.setTicketCategoryId(10L);
        dto.setTicketCount(ticketCount);
        dto.setTicketUserIdList(List.of(TICKET_USER_ID));
        return dto;
    }
}
