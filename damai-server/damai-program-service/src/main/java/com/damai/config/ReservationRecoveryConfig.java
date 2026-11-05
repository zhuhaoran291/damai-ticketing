package com.damai.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Configuration
public class ReservationRecoveryConfig {
    @Bean("reservationRecoveryScheduler")
    public ThreadPoolTaskScheduler reservationRecoveryScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(2);
        scheduler.setThreadNamePrefix("reservation-recovery-");
        return scheduler;
    }
}
