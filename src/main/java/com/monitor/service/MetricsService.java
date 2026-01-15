package com.monitor.service;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.springframework.stereotype.Service;

import java.time.Duration;

/**
 * 指标服务：封装 Micrometer 指标写入。
 * 指标列表：
 * - msg_produced_total(counter): 观察到消息（按租户/系统/事件类型）
 * - msg_consumed_total(counter): 观察到消费结果（按租户/系统/结果/节点）
 * - msg_e2e_latency_seconds(timer): 端到端耗时（按租户/系统/节点）
 * - msg_timeout_total(counter): 超时计数（按租户/系统/节点/阈值）
 */
@Service
public class MetricsService {
    private final MeterRegistry registry;

    public MetricsService(MeterRegistry registry) {
        this.registry = registry;
    }

    /** 增加“消息被观察到”的计数 */
    public void incProduced(String tenant, String systemNo, String adviseKey) {
        registry.counter("msg_produced_total", Tags.of(
                "tenant", safeTag(tenant),
                "system", safeTag(systemNo),
                "eventType", safeTag(adviseKey)
        )).increment();
    }

    /** 增加“消息消费结果被观察到”的计数 */
    public void incConsumed(String tenant, String systemNo, String result, String nodeName) {
        registry.counter("msg_consumed_total", Tags.of(
                "tenant", safeTag(tenant),
                "system", safeTag(systemNo),
                "result", safeTag(result),
                "nodeName", safeTag(nodeName)
        )).increment();
    }

    /** 记录端到端耗时（Timer） */
    public void recordLatency(String tenant, String systemNo, String nodeName, Duration duration) {
        registry.timer("msg_e2e_latency_seconds", Tags.of(
                "tenant", safeTag(tenant),
                "system", safeTag(systemNo),
                "nodeName", safeTag(nodeName)
        )).record(duration);
    }

    /** 超时计数（达到或超过阈值） */
    public void incTimeout(String tenant, String systemNo, String nodeName, String threshold) {
        registry.counter("msg_timeout_total", Tags.of(
                "tenant", safeTag(tenant),
                "system", safeTag(systemNo),
                "nodeName", safeTag(nodeName),
                "threshold", safeTag(threshold)
        )).increment();
    }

    private static String safeTag(String value) {
        return (value == null || value.isBlank()) ? "unknown" : value;
    }
}
