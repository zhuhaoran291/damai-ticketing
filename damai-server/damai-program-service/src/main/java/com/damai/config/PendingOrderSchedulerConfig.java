package com.damai.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Configuration
public class PendingOrderSchedulerConfig {
    @Bean(name = "taskScheduler")
    public ThreadPoolTaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setThreadNamePrefix("program-scheduled-");
        scheduler.setPoolSize(1);
        return scheduler;
    }

    @Bean(name = "pendingOrderRecoveryScheduler")
    public ThreadPoolTaskScheduler pendingOrderRecoveryScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setThreadNamePrefix("pending-order-recovery-");
        scheduler.setPoolSize(1);
        return scheduler;
    }
}
