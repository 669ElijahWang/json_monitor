package com.monitor.service;

import com.monitor.entity.StoredMessage;
import com.monitor.repository.StoredMessageRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 统计服务：对存储消息做窗口统计与聚合。
 * 提供：
 * - overview：窗口内总数、成功/失败
 * - timeseries：按分钟的吞吐/成功/失败/超时
 * - breakdown：按租户/系统拆分计数 Top
 * - latency：窗口内延迟的 p50/p95/p99/平均
 */
@Service
public class StatsService {
    private final StoredMessageRepository repository;
    private final long timeoutSeconds;

    public StatsService(StoredMessageRepository repository,
                        @Value("${monitor.latency.timeout-seconds:5}") long timeoutSeconds) {
        this.repository = repository;
        this.timeoutSeconds = timeoutSeconds;
    }

    public Map<String, Object> overview(int minutes) {
        Instant to = Instant.now();
        Instant from = to.minus(minutes, ChronoUnit.MINUTES);

        long total = repository.countByCreatedAtBetween(from, to);
        long success = repository.countByResultAndCreatedAtBetween("SUCCESS", from, to);
        long fail = repository.countByResultAndCreatedAtBetween("FAIL", from, to);

        Map<String, Object> result = new HashMap<>();
        result.put("window", minutes + "m");
        result.put("from", from.toString());
        result.put("to", to.toString());
        result.put("messages", total);
        result.put("success", success);
        result.put("fail", fail);
        return result;
    }

    public List<Map<String, Object>> timeseries(int minutes) {
        Instant to = Instant.now();
        Instant from = to.minus(minutes, ChronoUnit.MINUTES);
        List<StoredMessage> messages = repository.findByCreatedAtBetween(from, to);

        Map<Instant, Bucket> buckets = new HashMap<>();
        for (StoredMessage m : messages) {
            Instant t = truncateToMinute(m.getCreatedAt());
            Bucket b = buckets.computeIfAbsent(t, ignored -> new Bucket());
            b.total++;
            if ("SUCCESS".equalsIgnoreCase(m.getResult())) {
                b.success++;
            } else if ("FAIL".equalsIgnoreCase(m.getResult())) {
                b.fail++;
            } else {
                b.other++;
            }

            Long produce = m.getProduceTimeMs();
            Long observed = m.getObservedTimeMs();
            if (produce != null && observed != null) {
                long latencyMs = Math.max(0, observed - produce);
                if (latencyMs >= timeoutSeconds * 1000) {
                    b.timeout++;
                }
            }
        }

        List<Instant> timeline = buildMinuteTimeline(from, to);
        List<Map<String, Object>> out = new ArrayList<>(timeline.size());
        for (Instant ts : timeline) {
            Bucket b = buckets.getOrDefault(truncateToMinute(ts), new Bucket());
            Map<String, Object> row = new HashMap<>();
            row.put("ts", ts.toString());
            row.put("total", b.total);
            row.put("success", b.success);
            row.put("fail", b.fail);
            row.put("timeout", b.timeout);
            row.put("other", b.other);
            out.add(row);
        }
        return out;
    }

    public List<Map<String, Object>> breakdown(int minutes, String by) {
        Instant to = Instant.now();
        Instant from = to.minus(minutes, ChronoUnit.MINUTES);
        List<StoredMessage> messages = repository.findByCreatedAtBetween(from, to);

        Map<String, Long> grouped = messages.stream()
                .collect(Collectors.groupingBy(m -> {
                    if ("system".equalsIgnoreCase(by) || "systemNo".equalsIgnoreCase(by)) {
                        return safe(m.getSystemNo());
                    }
                    if ("tenant".equalsIgnoreCase(by)) {
                        return safe(m.getTenant());
                    }
                    return "unknown";
                }, Collectors.counting()));

        return grouped.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(20)
                .map(e -> {
                    Map<String, Object> row = new HashMap<>();
                    row.put("name", e.getKey());
                    row.put("count", e.getValue());
                    return row;
                })
                .collect(Collectors.toList());
    }

    public Map<String, Object> latency(int minutes) {
        Instant to = Instant.now();
        Instant from = to.minus(minutes, ChronoUnit.MINUTES);
        List<StoredMessage> messages = repository.findByCreatedAtBetween(from, to);

        List<Long> latenciesMs = messages.stream()
                .map(m -> {
                    Long produce = m.getProduceTimeMs();
                    Long observed = m.getObservedTimeMs();
                    if (produce == null || observed == null) {
                        return null;
                    }
                    return Math.max(0, observed - produce);
                })
                .filter(Objects::nonNull)
                .sorted()
                .collect(Collectors.toList());

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("window", minutes + "m");
        out.put("count", latenciesMs.size());
        out.put("p50Ms", percentile(latenciesMs, 0.50));
        out.put("p95Ms", percentile(latenciesMs, 0.95));
        out.put("p99Ms", percentile(latenciesMs, 0.99));
        out.put("avgMs", average(latenciesMs));
        return out;
    }

    private static String safe(String s) {
        return (s == null || s.isBlank()) ? "unknown" : s;
    }

    private static Instant truncateToMinute(Instant instant) {
        ZonedDateTime zdt = instant.atZone(ZoneId.systemDefault()).truncatedTo(ChronoUnit.MINUTES);
        return zdt.toInstant();
    }

    private static List<Instant> buildMinuteTimeline(Instant from, Instant to) {
        Instant start = truncateToMinute(from);
        Instant end = truncateToMinute(to);
        List<Instant> list = new ArrayList<>();
        Instant cursor = start;
        while (!cursor.isAfter(end)) {
            list.add(cursor);
            cursor = cursor.plus(1, ChronoUnit.MINUTES);
        }
        return list;
    }

    /** 百分位计算：输入需为排序后的数组 */
    private static Long percentile(List<Long> sorted, double p) {
        if (sorted == null || sorted.isEmpty()) {
            return null;
        }
        double idx = p * (sorted.size() - 1);
        int lo = (int) Math.floor(idx);
        int hi = (int) Math.ceil(idx);
        if (lo == hi) {
            return sorted.get(lo);
        }
        long a = sorted.get(lo);
        long b = sorted.get(hi);
        double w = idx - lo;
        return Math.round(a + (b - a) * w);
    }

    /** 平均值（空集合返回 null） */
    private static Long average(List<Long> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        long sum = 0;
        for (Long v : values) {
            sum += v;
        }
        return Math.round(sum * 1.0 / values.size());
    }

    private static class Bucket {
        long total;
        long success;
        long fail;
        long timeout;
        long other;
    }
}
