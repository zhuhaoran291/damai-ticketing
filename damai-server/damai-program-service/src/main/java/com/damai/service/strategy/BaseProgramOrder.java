package com.damai.service.strategy;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.StrUtil;
import com.damai.dto.ProgramOrderCreateDto;
import com.damai.dto.SeatDto;
import com.damai.enums.BaseCode;
import com.damai.exception.DaMaiFrameException;
import com.damai.locallock.LocalLockCache;
import com.damai.lock.LockTask;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * @program: 极度真实还原大麦网高并发实战项目。 添加 阿星不是程序员 微信，添加时备注 大麦 来获取项目的完整资料 
 * @description: 节目订单
 * @author: 阿星不是程序员
 **/
@Slf4j
@Component
public class BaseProgramOrder {
    
    @Autowired
    private LocalLockCache localLockCache;
    
    public String localLockCreateOrder(String lockKeyPrefix, ProgramOrderCreateDto programOrderCreateDto, 
                                          LockTask<String> lockTask){
        return localLockCreateOrder(lockKeyPrefix, programOrderCreateDto, lockTask, -1);
    }

    /** 使用一个总时限获取所有票档锁；获取失败时释放已经取得的锁。 */
    public String localLockCreateOrder(String lockKeyPrefix, ProgramOrderCreateDto programOrderCreateDto,
                                       LockTask<String> lockTask, long lockWaitMillis) {
        List<SeatDto> seatDtoList = programOrderCreateDto.getSeatDtoList();
        List<Long> ticketCategoryIdList = new ArrayList<>();
        if (CollectionUtil.isNotEmpty(seatDtoList)) {
            ticketCategoryIdList =
                    seatDtoList.stream().map(SeatDto::getTicketCategoryId).distinct().sorted().collect(Collectors.toList());
        }else {
            ticketCategoryIdList.add(programOrderCreateDto.getTicketCategoryId());
        }
        List<ReentrantLock> localLockList = new ArrayList<>(ticketCategoryIdList.size());
        List<ReentrantLock> localLockSuccessList = new ArrayList<>(ticketCategoryIdList.size());
        for (Long ticketCategoryId : ticketCategoryIdList) {
            String lockKey = StrUtil.join("-",lockKeyPrefix,
                    programOrderCreateDto.getProgramId(),ticketCategoryId);
            ReentrantLock localLock = localLockCache.getLock(lockKey,false);
            localLockList.add(localLock);
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(0, lockWaitMillis));
        try {
            for (ReentrantLock reentrantLock : localLockList) {
                if (lockWaitMillis < 0) {
                    reentrantLock.lock();
                } else {
                    long remainingNanos = Math.max(0, deadline - System.nanoTime());
                    if (!reentrantLock.tryLock(remainingNanos, TimeUnit.NANOSECONDS)) {
                        throw new DaMaiFrameException(BaseCode.OPERATION_IS_TOO_FREQUENT_PLEASE_TRY_AGAIN_LATER);
                    }
                }
                localLockSuccessList.add(reentrantLock);
            }
            return lockTask.execute();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new DaMaiFrameException(BaseCode.OPERATION_IS_TOO_FREQUENT_PLEASE_TRY_AGAIN_LATER.getCode(),
                    "等待下单锁时被中断，请稍后重试", ex);
        } finally {
            for (int i = localLockSuccessList.size() - 1; i >= 0; i--) {
                try {
                    localLockSuccessList.get(i).unlock();
                } catch (Throwable t) {
                    log.error("local lock unlock error", t);
                }
            }
        }
    }
}
