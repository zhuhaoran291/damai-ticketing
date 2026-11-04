package com.damai.service.strategy;

import com.damai.dto.ProgramOrderCreateDto;
import com.damai.dto.SeatDto;
import com.damai.exception.DaMaiFrameException;
import com.damai.locallock.LocalLockCache;
import org.junit.jupiter.api.Test;
import java.lang.reflect.Field;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ProgramOrderLockWaitTest {
    @Test
    void timeoutReleasesPreviouslyAcquiredTicketCategoryLock() throws Exception {
        BaseProgramOrder service = new BaseProgramOrder();
        LocalLockCache cache = mock(LocalLockCache.class);
        Field cacheField = BaseProgramOrder.class.getDeclaredField("localLockCache");
        cacheField.setAccessible(true);
        cacheField.set(service, cache);
        ReentrantLock first = new ReentrantLock();
        ReentrantLock second = new ReentrantLock();
        when(cache.getLock(contains("-1-1"), eq(false))).thenReturn(first);
        when(cache.getLock(contains("-1-2"), eq(false))).thenReturn(second);
        ProgramOrderCreateDto request = new ProgramOrderCreateDto();
        request.setProgramId(1L);
        SeatDto seat1 = new SeatDto();
        seat1.setTicketCategoryId(1L);
        SeatDto seat2 = new SeatDto();
        seat2.setTicketCategoryId(2L);
        request.setSeatDtoList(List.of(seat1, seat2));
        AtomicBoolean taskRan = new AtomicBoolean();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        second.lock();
        try {
            Future<DaMaiFrameException> result = executor.submit(() -> assertThrows(DaMaiFrameException.class,
                    () -> service.localLockCreateOrder("v4", request, () -> {
                        taskRan.set(true);
                        return "ok";
                    }, 100)));
            assertNotNull(result.get(2, TimeUnit.SECONDS));
            assertFalse(taskRan.get());
            assertTrue(first.tryLock(), "first lock must be released after the second lock times out");
            first.unlock();
        } finally {
            second.unlock();
            executor.shutdownNow();
        }
    }
}
