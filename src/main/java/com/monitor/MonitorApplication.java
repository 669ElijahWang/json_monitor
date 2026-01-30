package com.monitor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 应用启动类：
 * - @SpringBootApplication：启用 Spring Boot 自动配置与组件扫描
 * - @EnableScheduling：启用 @Scheduled 定时任务（例如定时生产 Demo 消息、定时刷新 backlog 指标）
 */
@SpringBootApplication
@EnableScheduling
public class MonitorApplication {
    public static void main(String[] args) {
        SpringApplication.run(MonitorApplication.class, args);
    }
}
