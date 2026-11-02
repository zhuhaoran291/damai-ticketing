package com.damai.core;

/**
 * @program: 极度真实还原大麦网高并发实战项目。 添加 阿星不是程序员 微信，添加时备注 大麦 来获取项目的完整资料 
 * @description: 防重复幂等业务名管理
 * @author: 阿星不是程序员
 **/
public class RepeatExecuteLimitConstants {
    
    public static final String CONSUMER_API_DATA_MESSAGE = "consumer_api_data_message";
    
    public static final String CREATE_PROGRAM_ORDER = "create_program_order";

    /**
     * 下单防重窗口（秒）：同一账号 + 同一节目在该窗口内只允许成功提交一次。
     * <p>
     * 注解的 durationTime 默认是 0，切面里只有 durationTime > 0 才会写防重标记，
     * 也就是「不配就等于完全不生效」，所以下单场景必须显式指定。
     */
    public static final long CREATE_PROGRAM_ORDER_REPEAT_DURATION_SECONDS = 5L;
   
    public final static String CANCEL_PROGRAM_ORDER = "cancel_program_order";
    
    public static final String CREATE_PROGRAM_ORDER_MQ = "create_program_order_mq";
    
    public static final String PROGRAM_CACHE_REVERSE_MQ = "program_cache_reverse_mq";
}
