package com.damai.service;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.damai.client.BaseDataClient;
import com.damai.common.ApiResponse;
import com.damai.dto.ProgramSearchDto;
import com.damai.entity.ProgramJoinShowTime;
import com.damai.enums.BaseCode;
import com.damai.exception.DaMaiFrameException;
import com.damai.mapper.ProgramCategoryMapper;
import com.damai.mapper.ProgramMapper;
import com.damai.mapper.TicketCategoryMapper;
import com.damai.page.PageVo;
import com.damai.service.es.EsSearchException;
import com.damai.service.es.ProgramEs;
import com.damai.service.resilience.FallbackRateLimiter;
import com.damai.vo.AreaVo;
import com.damai.vo.ProgramListVo;
import com.damai.vo.ProgramSearchPageVo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 搜索降级（Issue #11）。
 *
 * <p>原来 ES 层把异常吞掉、返回一个空分页，上游分不清"确实没有"和"检索挂了"，
 * 于是接口照常返回成功码 + 0 条，用户以为平台没票、监控全绿。
 *
 * <p>现在的约定：
 * <ul>
 *   <li>ES 抛 {@link EsSearchException} → 降级查库，结果打 degraded=true；</li>
 *   <li>ES 正常但结果为空 → 就是"真的没有"，不降级；</li>
 *   <li>ES 和数据库都失败 → 抛业务异常（错误码 40033），让调用方明确知道搜索不可用。</li>
 * </ul>
 *
 * <p>不启动 Spring 上下文，不连 MySQL / Redis / ES。
 */
@ExtendWith(MockitoExtension.class)
class ProgramSearchFallbackTest {

    @Mock
    private ProgramEs programEs;

    @Mock
    private ProgramMapper programMapper;

    @Mock
    private ProgramCategoryMapper programCategoryMapper;

    @Mock
    private TicketCategoryMapper ticketCategoryMapper;

    @Mock
    private BaseDataClient baseDataClient;

    @Mock
    private FallbackRateLimiter fallbackRateLimiter;

    @InjectMocks
    private ProgramService programService;

    private ProgramSearchDto request() {
        ProgramSearchDto dto = new ProgramSearchDto();
        dto.setContent("演唱会");
        dto.setAreaId(2L);
        dto.setTimeType(0);
        dto.setType(3);
        dto.setPageNumber(1);
        dto.setPageSize(10);
        return dto;
    }

    @Test
    @DisplayName("ES 不可用 → 降级查库，结果标记 degraded=true")
    void esDown_thenFallbackToDb() {
        when(programEs.search(any())).thenThrow(new EsSearchException("Connection refused"));
        when(fallbackRateLimiter.tryAcquire()).thenReturn(true);
        when(programMapper.selectSearch(any(), any())).thenReturn(dbPage(1L, "周杰伦嘉年华演唱会"));
        when(baseDataClient.selectByIdList(any())).thenReturn(ApiResponse.ok(new ArrayList<AreaVo>()));

        ProgramSearchPageVo result = programService.search(request());

        assertTrue(result.isDegraded(), "降级返回必须带标记，前端/监控才能看出来");
        assertEquals(1L, result.getTotalSize());
        assertEquals("周杰伦嘉年华演唱会", result.getList().get(0).getTitle());
        verify(programMapper).selectSearch(any(), any());
    }

    @Test
    @DisplayName("降级也被限流拒绝 → 抛 40033，且一次都不查库")
    void esDown_andFallbackRateLimited_thenThrow() {
        when(programEs.search(any())).thenThrow(new EsSearchException("Connection refused"));
        when(fallbackRateLimiter.tryAcquire()).thenReturn(false);

        DaMaiFrameException ex = assertThrows(DaMaiFrameException.class,
                () -> programService.search(request()));

        assertEquals(BaseCode.PROGRAM_SEARCH_NOT_AVAILABLE.getCode(), ex.getCode());
        verify(programMapper, never()).selectSearch(any(), any());
    }

    @Test
    @DisplayName("ES 和数据库都失败 → 抛 40033，不再返回成功 + 空列表")
    void esDown_andDbDown_thenThrowBusinessCode() {
        when(programEs.search(any())).thenThrow(new EsSearchException("Connection refused"));
        when(fallbackRateLimiter.tryAcquire()).thenReturn(true);
        when(programMapper.selectSearch(any(), any())).thenThrow(new RuntimeException("db timeout"));

        DaMaiFrameException ex = assertThrows(DaMaiFrameException.class,
                () -> programService.search(request()));

        assertEquals(BaseCode.PROGRAM_SEARCH_NOT_AVAILABLE.getCode(), ex.getCode());
    }

    @Test
    @DisplayName("ES 正常但没结果 → 属于「真的没有」，不降级、不查库")
    void esEmptyResult_isNotFailure() {
        when(programEs.search(any())).thenReturn(new PageVo<>(1L, 10L, 0L, new ArrayList<>()));

        ProgramSearchPageVo result = programService.search(request());

        assertFalse(result.isDegraded());
        assertEquals(0L, result.getTotalSize());
        verify(programMapper, never()).selectSearch(any(), any());
    }

    @Test
    @DisplayName("ES 正常有结果 → 正常返回，degraded=false")
    void esOk_thenNoDegrade() {
        ProgramListVo vo = new ProgramListVo();
        vo.setId(1L);
        vo.setTitle("五月天演唱会");
        when(programEs.search(any())).thenReturn(new PageVo<>(1L, 10L, 1L, List.of(vo)));

        ProgramSearchPageVo result = programService.search(request());

        assertFalse(result.isDegraded());
        assertEquals(1L, result.getTotalSize());
        verify(programMapper, never()).selectSearch(any(), any());
    }

    private Page<ProgramJoinShowTime> dbPage(Long id, String title) {
        ProgramJoinShowTime record = new ProgramJoinShowTime();
        record.setId(id);
        record.setTitle(title);
        record.setAreaId(2L);
        record.setProgramCategoryId(11L);
        record.setParentProgramCategoryId(1L);

        Page<ProgramJoinShowTime> page = new Page<>(1, 10);
        page.setRecords(List.of(record));
        page.setTotal(1);
        return page;
    }
}
