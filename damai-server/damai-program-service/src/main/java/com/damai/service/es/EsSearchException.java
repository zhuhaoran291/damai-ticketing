package com.damai.service.es;

/**
 * @program: 极度真实还原大麦网高并发实战项目。 添加 阿星不是程序员 微信，添加时备注 大麦 来获取项目的完整资料 
 * @description: ES 检索不可用异常
 * @author: 阿星不是程序员
 **/
public class EsSearchException extends RuntimeException {

    public EsSearchException(String message) {
        super(message);
    }

    public EsSearchException(String message, Throwable cause) {
        super(message, cause);
    }
}
