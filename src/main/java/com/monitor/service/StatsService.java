package com.monitor.service;

import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
    private final PrometheusQueryService prometheus;
    private final MetricsService metrics;

    public StatsService(PrometheusQueryService prometheus, MetricsService metrics) {
        this.prometheus = prometheus;
        this.metrics = metrics;
    }

    public Map<String, Object> overview(int minutes) {
        Instant to = Instant.now();
        Instant from = to.minus(minutes, ChronoUnit.MINUTES);

        String window = minutes + "m";
        Double total = prometheus.queryScalar("sum(increase(msg_consumed_total[" + window + "]))");
        Double success = prometheus
                .queryScalar("sum(increase(msg_consumed_total{result=\"SUCCESS\"}[" + window + "]))");
        Double fail = prometheus.queryScalar("sum(increase(msg_consumed_total{result=\"FAIL\"}[" + window + "]))");

        Map<String, Object> result = new HashMap<>();
        result.put("window", window);
        result.put("from", from.toString());
        result.put("to", to.toString());
        result.put("messages", toLongOrZero(total));
        result.put("success", toLongOrZero(success));
        result.put("fail", toLongOrZero(fail));
        return result;
    }

    public List<Map<String, Object>> timeseries(int minutes) {
        Instant to = Instant.now();
        Instant from = to.minus(minutes, ChronoUnit.MINUTES);
        List<Instant> timeline = buildMinuteTimeline(from, to);
        long startEpoch = truncateToMinute(from).getEpochSecond();
        long endEpoch = truncateToMinute(to).getEpochSecond();
        String step = "60";

        String totalQ = "sum(increase(msg_consumed_total[1m]))";
        String successQ = "sum(increase(msg_consumed_total{result=\"SUCCESS\"}[1m]))";
        String failQ = "sum(increase(msg_consumed_total{result=\"FAIL\"}[1m]))";
        String timeoutQ = "sum(increase(msg_timeout_total[1m]))";

        Map<Long, Double> total = prometheus.queryRangeSingleSeries(totalQ, startEpoch, endEpoch, step);
        Map<Long, Double> success = prometheus.queryRangeSingleSeries(successQ, startEpoch, endEpoch, step);
        Map<Long, Double> fail = prometheus.queryRangeSingleSeries(failQ, startEpoch, endEpoch, step);
        Map<Long, Double> timeout = prometheus.queryRangeSingleSeries(timeoutQ, startEpoch, endEpoch, step);

        List<Map<String, Object>> out = new ArrayList<>(timeline.size());
        for (Instant ts : timeline) {
            long key = truncateToMinute(ts).getEpochSecond();
            Map<String, Object> row = new HashMap<>();
            row.put("ts", ts.toString());
            row.put("total", toLongOrZero(total.get(key)));
            row.put("success", toLongOrZero(success.get(key)));
            row.put("fail", toLongOrZero(fail.get(key)));
            row.put("timeout", toLongOrZero(timeout.get(key)));
            row.put("other", 0);
            out.add(row);
        }
        return out;
    }

    public List<Map<String, Object>> breakdown(int minutes, String by) {
        String window = minutes + "m";
        String label = "tenant";
        if ("system".equalsIgnoreCase(by) || "systemNo".equalsIgnoreCase(by)) {
            label = "system";
        }

        String q = "topk(20, sum(increase(msg_consumed_total[" + window + "])) by (" + label + "))";
        List<PrometheusQueryService.SeriesPoint> series = prometheus.queryVector(q);

        List<Map<String, Object>> out = new ArrayList<>(series.size());
        for (PrometheusQueryService.SeriesPoint sp : series) {
            String name = sp.getMetric().getOrDefault(label, "unknown");
            Map<String, Object> row = new HashMap<>();
            row.put("name", name);
            row.put("count", toLongOrZero(sp.getValue()));
            out.add(row);
        }
        return out;
    }

    public Map<String, Object> latency(int minutes) {
        String window = minutes + "m";

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("window", window);
        out.put("lag", latencyOf("msg_lag_latency_seconds", window));
        out.put("internal", latencyOf("msg_internal_latency_seconds", window));
        out.put("e2e", latencyOf("msg_e2e_latency_seconds", window));
        return out;
    }

    public Map<String, Object> messageSizeDistribution(int minutes) {
        int safeMinutes = Math.max(1, Math.min(60 * 24 * 15, minutes));
        String window = safeMinutes + "m";

        String q = "sum(increase(msg_size_bytes_bucket[" + window + "])) by (le)";
        List<PrometheusQueryService.SeriesPoint> series = prometheus.queryVector(q);

        List<Map<String, Object>> buckets = new ArrayList<>();
        for (PrometheusQueryService.SeriesPoint sp : series) {
            String le = sp.getMetric().getOrDefault("le", "");
            Map<String, Object> row = new HashMap<>();
            row.put("le", le);
            row.put("cumulative", toLongOrZero(sp.getValue()));
            buckets.add(row);
        }

        buckets.sort((a, b) -> compareLe(String.valueOf(a.get("le")), String.valueOf(b.get("le"))));

        long prev = 0L;
        for (Map<String, Object> b : buckets) {
            long cum = (long) b.getOrDefault("cumulative", 0L);
            long cnt = Math.max(0L, cum - prev);
            b.put("count", cnt);
            prev = cum;
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("window", window);
        out.put("serverTimeMs", System.currentTimeMillis());
        out.put("buckets", buckets);
        return out;
    }

    public Map<String, Object> bigMessages(int limit) {
        int safeLimit = Math.max(1, Math.min(200, limit));
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("thresholdBytes", metrics.getBigMessageThresholdBytes());
        out.put("serverTimeMs", System.currentTimeMillis());
        out.put("content", metrics.listBigMessages(safeLimit));
        out.put("totalElements", metrics.bigMessageCount());
        return out;
    }

    /**
     * 状态统计：返回消息类型分布、watchState分布、systemState分布、workitemState分布
     */
    public Map<String, Object> stateStats(int minutes) {
        String window = minutes + "m";
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("window", window);
        out.put("serverTimeMs", System.currentTimeMillis());

        // 消息类型分布
        List<Map<String, Object>> messageTypes = queryLabelDistribution("msg_by_type_total", "messageType", window);
        out.put("messageTypes", messageTypes);

        // watchState分布（STATE类型消息的Kafka接收状态）
        List<Map<String, Object>> watchStates = queryLabelDistribution("msg_watch_state_total", "watchState", window);
        // 添加状态描述
        for (Map<String, Object> ws : watchStates) {
            String state = String.valueOf(ws.get("name"));
            ws.put("desc", getWatchStateDesc(state));
        }
        out.put("watchStates", watchStates);

        // systemState分布（AGENT类型消息的系统状态）
        List<Map<String, Object>> systemStates = queryLabelDistribution("msg_system_state_total", "systemState",
                window);
        out.put("systemStates", systemStates);

        // workitemState分布（AGENT类型消息的工作项状态）
        List<Map<String, Object>> workitemStates = queryLabelDistribution("msg_workitem_state_total", "workitemState",
                window);
        // 添加状态描述
        for (Map<String, Object> ws : workitemStates) {
            String state = String.valueOf(ws.get("name"));
            ws.put("desc", getWorkitemStateDesc(state));
        }
        out.put("workitemStates", workitemStates);

        return out;
    }

    private List<Map<String, Object>> queryLabelDistribution(String metric, String label, String window) {
        String q = "sum(increase(" + metric + "[" + window + "])) by (" + label + ")";
        List<PrometheusQueryService.SeriesPoint> series = prometheus.queryVector(q);

        List<Map<String, Object>> out = new ArrayList<>(series.size());
        for (PrometheusQueryService.SeriesPoint sp : series) {
            String name = sp.getMetric().getOrDefault(label, "unknown");
            Map<String, Object> row = new HashMap<>();
            row.put("name", name);
            row.put("count", toLongOrZero(sp.getValue()));
            out.add(row);
        }
        // 按count降序排序
        out.sort((a, b) -> Long.compare((Long) b.get("count"), (Long) a.get("count")));
        return out;
    }

    private static String getWatchStateDesc(String state) {
        if (state == null)
            return "";
        switch (state) {
            case "0":
                return "待处理";
            case "1":
                return "获取";
            case "2":
                return "处理中";
            case "4":
                return "处理失败";
            case "5":
                return "处理完成";
            default:
                return state;
        }
    }

    private static String getWorkitemStateDesc(String state) {
        if (state == null)
            return "";
        switch (state) {
            case "1":
                return "初始化";
            case "2":
                return "待处理";
            case "4":
                return "处理中";
            case "5":
                return "挂起";
            case "6":
                return "完成";
            case "7":
                return "已终止";
            default:
                return state;
        }
    }

    private Map<String, Object> latencyOf(String metric, String window) {
        Double p50 = prometheus
                .queryScalar("histogram_quantile(0.5, sum(rate(" + metric + "_bucket[" + window + "])) by (le))");
        Double p95 = prometheus
                .queryScalar("histogram_quantile(0.95, sum(rate(" + metric + "_bucket[" + window + "])) by (le))");
        Double p99 = prometheus
                .queryScalar("histogram_quantile(0.99, sum(rate(" + metric + "_bucket[" + window + "])) by (le))");
        Double avg = prometheus.queryScalar(
                "sum(rate(" + metric + "_sum[" + window + "])) / sum(rate(" + metric + "_count[" + window + "]))");
        Double count = prometheus.queryScalar("sum(increase(" + metric + "_count[" + window + "]))");

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("count", toLongOrZero(count));
        out.put("p50S", secondsToSecOrNull(p50));
        out.put("p95S", secondsToSecOrNull(p95));
        out.put("p99S", secondsToSecOrNull(p99));
        out.put("avgS", secondsToSecOrNull(avg));
        return out;
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

    private static long toLongOrZero(Double v) {
        if (v == null || Double.isNaN(v) || Double.isInfinite(v)) {
            return 0;
        }
        return Math.max(0, Math.round(v));
    }

    private static Double secondsToSecOrNull(Double seconds) {
        if (seconds == null || Double.isNaN(seconds) || Double.isInfinite(seconds)) {
            return null;
        }
        double v = Math.max(0, seconds);
        return Math.round(v * 1000.0) / 1000.0;
    }

    private static int compareLe(String a, String b) {
        if ("+Inf".equals(a) && "+Inf".equals(b))
            return 0;
        if ("+Inf".equals(a))
            return 1;
        if ("+Inf".equals(b))
            return -1;
        try {
            double da = Double.parseDouble(a);
            double db = Double.parseDouble(b);
            return Double.compare(da, db);
        } catch (Exception ignored) {
            return a.compareTo(b);
        }
    }
}
