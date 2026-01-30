package com.monitor.service;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 指标服务：封装 Micrometer 指标写入。
 * 指标列表：
 * - msg_produced_total(counter): 观察到消息（按租户/系统/事件类型）
 * - msg_consumed_total(counter): 观察到消费结果（按租户/系统/结果/节点）
 * - msg_e2e_latency_seconds(timer): 端到端耗时（按租户/系统/节点）
 * - msg_timeout_total(counter): 超时计数（按租户/系统/节点/阈值）
 * - msg_task_events_total(counter): taskId 维度事件（高基数，谨慎开启）
 * - msg_task_last_seen_seconds(gauge): taskId 维度最近一次观察时间（高基数，谨慎开启）
 */
@Service
public class MetricsService {
    private final MeterRegistry registry;
    private final boolean taskMetricsEnabled;
    private final int taskMetricsMaxSeries;
    private final ConcurrentMap<String, AtomicLong> taskLastSeen = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, AtomicLong> partitionOldestAgeSeconds = new ConcurrentHashMap<>();
    private final long bigMessageThresholdBytes;
    private final int bigMessageTraceMax;
    private final Deque<Map<String, Object>> bigMessageTrace = new ConcurrentLinkedDeque<>();

    public MetricsService(MeterRegistry registry,
            @Value("${monitor.prometheus.task-metrics-enabled:true}") boolean taskMetricsEnabled,
            @Value("${monitor.prometheus.task-metrics-max-series:5000}") int taskMetricsMaxSeries,
            @Value("${monitor.message.big-threshold-bytes:1048576}") long bigMessageThresholdBytes,
            @Value("${monitor.message.big-trace-max:200}") int bigMessageTraceMax) {
        this.registry = registry;
        this.taskMetricsEnabled = taskMetricsEnabled;
        this.taskMetricsMaxSeries = taskMetricsMaxSeries;
        this.bigMessageThresholdBytes = Math.max(0, bigMessageThresholdBytes);
        this.bigMessageTraceMax = Math.max(1, bigMessageTraceMax);
    }

    /** 增加“消息被观察到”的计数 */
    public void incProduced(String tenant, String systemNo, String adviseKey) {
        registry.counter("msg_produced_total", Tags.of(
                "tenant", safeTag(tenant),
                "system", safeTag(systemNo),
                "eventType", safeTag(adviseKey))).increment();
    }

    /** 增加“消息消费结果被观察到”的计数 */
    public void incConsumed(String tenant, String systemNo, String result, String nodeName) {
        registry.counter("msg_consumed_total", Tags.of(
                "tenant", safeTag(tenant),
                "system", safeTag(systemNo),
                "result", safeTag(result),
                "nodeName", safeTag(nodeName))).increment();
    }

    /** 按消息类型统计 */
    public void incByMessageType(String messageType, String tenant, String result) {
        registry.counter("msg_by_type_total", Tags.of(
                "messageType", safeTag(messageType),
                "tenant", safeTag(tenant),
                "result", safeTag(result))).increment();
    }

    /** 按watchState统计（STATE类型消息） */
    public void incByWatchState(String tenant, String watchState) {
        registry.counter("msg_watch_state_total", Tags.of(
                "tenant", safeTag(tenant),
                "watchState", safeTag(watchState))).increment();
    }

    /** 按systemState统计（AGENT类型消息） */
    public void incBySystemState(String tenant, String systemNo, String systemState) {
        registry.counter("msg_system_state_total", Tags.of(
                "tenant", safeTag(tenant),
                "system", safeTag(systemNo),
                "systemState", safeTag(systemState))).increment();
    }

    /** 按workitemState统计（AGENT类型消息） */
    public void incByWorkitemState(String tenant, String systemNo, String workitemState) {
        registry.counter("msg_workitem_state_total", Tags.of(
                "tenant", safeTag(tenant),
                "system", safeTag(systemNo),
                "workitemState", safeTag(workitemState))).increment();
    }

    /** 记录端到端耗时（Timer） */
    public void recordE2eLatency(String tenant, String systemNo, String nodeName, String topic, Duration duration) {
        registry.timer("msg_e2e_latency_seconds", Tags.of(
                "tenant", safeTag(tenant),
                "system", safeTag(systemNo),
                "nodeName", safeTag(nodeName),
                "topic", safeTag(topic))).record(duration);
    }

    public void recordLagLatency(String tenant, String systemNo, String nodeName, String topic, Duration duration) {
        registry.timer("msg_lag_latency_seconds", Tags.of(
                "tenant", safeTag(tenant),
                "system", safeTag(systemNo),
                "nodeName", safeTag(nodeName),
                "topic", safeTag(topic))).record(duration);
    }

    public void recordInternalLatency(String tenant, String systemNo, String nodeName, String topic,
            Duration duration) {
        registry.timer("msg_internal_latency_seconds", Tags.of(
                "tenant", safeTag(tenant),
                "system", safeTag(systemNo),
                "nodeName", safeTag(nodeName),
                "topic", safeTag(topic))).record(duration);
    }

    /** 超时计数（达到或超过阈值） */
    public void incTimeout(String tenant, String systemNo, String nodeName, String threshold) {
        registry.counter("msg_timeout_total", Tags.of(
                "tenant", safeTag(tenant),
                "system", safeTag(systemNo),
                "nodeName", safeTag(nodeName),
                "threshold", safeTag(threshold))).increment();
    }

    public void observeTaskEvent(ParsedMessage parsed, long observedTimeMs) {
        if (!taskMetricsEnabled) {
            return;
        }
        if (parsed == null) {
            return;
        }

        Tags tags = Tags.of(
                "tenant", safeTag(parsed.getTenant()),
                "system", safeTag(parsed.getSystemNo()),
                "taskId", safeTag(parsed.getTaskId()),
                "nodeName", safeTag(parsed.getNodeName()),
                "result", safeTag(parsed.getResult()),
                "busId", safeTag(parsed.getBusId()),
                "eventType", safeTag(parsed.getAdviseKey()));

        registry.counter("msg_task_events_total", tags).increment();

        String key = tags.toString();
        AtomicLong gauge = taskLastSeen.get(key);
        if (gauge == null) {
            if (taskLastSeen.size() >= taskMetricsMaxSeries) {
                return;
            }
            AtomicLong created = new AtomicLong(0);
            AtomicLong existing = taskLastSeen.putIfAbsent(key, created);
            gauge = existing == null ? created : existing;
            if (existing == null) {
                Gauge.builder("msg_task_last_seen_seconds", gauge, AtomicLong::get)
                        .tags(tags)
                        .register(registry);
            }
        }
        gauge.set(Math.max(0, observedTimeMs / 1000));
    }

    public void setPartitionOldestMessageAgeSeconds(String topic, String groupId, int partition, long ageSeconds) {
        Tags tags = Tags.of(
                "topic", safeTag(topic),
                "groupId", safeTag(groupId),
                "partition", String.valueOf(partition));
        String key = tags.toString();
        AtomicLong gauge = partitionOldestAgeSeconds.get(key);
        if (gauge == null) {
            AtomicLong created = new AtomicLong(0);
            AtomicLong existing = partitionOldestAgeSeconds.putIfAbsent(key, created);
            gauge = existing == null ? created : existing;
            if (existing == null) {
                Gauge.builder("kafka_partition_oldest_message_age_seconds", gauge, AtomicLong::get)
                        .tags(tags)
                        .register(registry);
            }
        }
        gauge.set(Math.max(0, ageSeconds));
    }

    public void recordMessageSize(String topic, long sizeBytes) {
        DistributionSummary summary = DistributionSummary.builder("msg_size_bytes")
                .tags(Tags.of(
                        "topic", safeTag(topic)))
                .register(registry);
        summary.record(Math.max(0, sizeBytes));
    }

    public void observeBigMessage(ParsedMessage parsed,
            String topic,
            int partition,
            long offset,
            String key,
            long sizeBytes,
            long observedTimeMs) {
        if (parsed == null) {
            return;
        }
        long bytes = Math.max(0, sizeBytes);
        if (bigMessageThresholdBytes > 0 && bytes < bigMessageThresholdBytes) {
            return;
        }

        registry.counter("msg_big_message_total", Tags.of("topic", safeTag(topic))).increment();

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("observedAt", Instant.ofEpochMilli(Math.max(0, observedTimeMs)).toString());
        row.put("sizeBytes", bytes);
        row.put("topic", topic);
        row.put("partition", partition);
        row.put("offset", offset);
        row.put("key", key);
        row.put("taskId", parsed.getTaskId());
        row.put("tenant", parsed.getTenant());
        row.put("systemNo", parsed.getSystemNo());
        row.put("nodeName", parsed.getNodeName());
        row.put("result", parsed.getResult());
        row.put("adviseKey", parsed.getAdviseKey());
        row.put("busId", parsed.getBusId());

        bigMessageTrace.addFirst(row);
        while (bigMessageTrace.size() > bigMessageTraceMax) {
            bigMessageTrace.pollLast();
        }
    }

    public long getBigMessageThresholdBytes() {
        return bigMessageThresholdBytes;
    }

    public List<Map<String, Object>> listBigMessages(int limit) {
        int safeLimit = Math.max(1, Math.min(500, limit));
        List<Map<String, Object>> out = new ArrayList<>();
        int i = 0;
        for (Map<String, Object> m : bigMessageTrace) {
            out.add(m);
            i++;
            if (i >= safeLimit) {
                break;
            }
        }
        return out;
    }

    public int bigMessageCount() {
        return bigMessageTrace.size();
    }

    private static String safeTag(String value) {
        return (value == null || value.isBlank()) ? "unknown" : value;
    }
}
