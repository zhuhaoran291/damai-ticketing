package com.damai.vo;

import com.damai.page.PageVo;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serial;

/**
 * @program: 极度真实还原大麦网高并发实战项目。 添加 阿星不是程序员 微信，添加时备注 大麦 来获取项目的完整资料 
 * @description: 节目搜索结果（带降级标记）
 * @author: 阿星不是程序员
 **/
@Data
@EqualsAndHashCode(callSuper = true)
@Schema(title="ProgramSearchPageVo", description ="节目搜索结果")
public class ProgramSearchPageVo extends PageVo<ProgramListVo> {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 是否降级返回：true 表示 ES 不可用，本次结果由数据库兜底查询得到。
     * 前端据此提示"当前为降级结果"，监控据此计数。
     * */
    @Schema(name ="degraded", type ="Boolean", description ="是否降级返回（ES不可用时查库兜底）")
    private boolean degraded;

    public static ProgramSearchPageVo of(PageVo<ProgramListVo> pageVo, boolean degraded) {
        ProgramSearchPageVo result = new ProgramSearchPageVo();
        result.setPageNum(pageVo.getPageNum());
        result.setPageSize(pageVo.getPageSize());
        result.setTotalSize(pageVo.getTotalSize());
        result.setList(pageVo.getList());
        result.setDegraded(degraded);
        return result;
    }
}
