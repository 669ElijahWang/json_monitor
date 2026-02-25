package com.monitor.service;

import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class PrometheusMessageService {
    private final PrometheusQueryService prometheus;
    private final MessageRawStoreService rawStore;

    public PrometheusMessageService(PrometheusQueryService prometheus, MessageRawStoreService rawStore) {
        this.prometheus = prometheus;
        this.rawStore = rawStore;
    }

    public Map<String, Object> search(String q,
            String category,
            String nodeName,
            String taskId,
            int minutes,
            int page,
            int size) {
        int safePage = Math.max(0, page);
        int safeSize = clamp(size, 1, 100000);
        int start = safePage * safeSize;

        String window = clamp(minutes, 1, 60 * 24 * 30) + "m";
        String selector = buildSelector(q, category, nodeName, taskId);
        long totalElements = toLongOrZero(prometheus
                .queryScalar("count(max_over_time(msg_task_last_seen_seconds" + selector + "[" + window + "]))"));

        List<Map<String, Object>> content = searchDataRange(q, category, nodeName, taskId, minutes, start, safeSize);
        return pageOf(content, totalElements);
    }

    public List<Map<String, Object>> searchDataRange(String q,
            String category,
            String nodeName,
            String taskId,
            int minutes,
            int start,
            int size) {
        int safeMinutes = clamp(minutes, 1, 60 * 24 * 30);
        int safeStart = Math.max(0, start);
        int safeSize = clamp(size, 1, 100000);

        String selector = buildSelector(q, category, nodeName, taskId);
        String window = safeMinutes + "m";
        String lastSeenWindow = "max_over_time(msg_task_last_seen_seconds" + selector + "[" + window + "])";

        int limit = safeStart + safeSize;
        String lastSeenQ = "topk(" + limit + ", " + lastSeenWindow + ")";
        List<PrometheusQueryService.SeriesPoint> lastSeen = prometheus.queryVector(lastSeenQ);

        lastSeen.sort(Comparator.comparing(PrometheusQueryService.SeriesPoint::getValue,
                Comparator.nullsLast(Comparator.naturalOrder())).reversed());

        if (safeStart >= lastSeen.size()) {
            return Collections.emptyList();
        }
        int to = Math.min(safeStart + safeSize, lastSeen.size());

        List<Map<String, Object>> rows = new ArrayList<>(to - safeStart);
        for (PrometheusQueryService.SeriesPoint sp : lastSeen.subList(safeStart, to)) {
            Map<String, String> m = sp.getMetric();
            String task = m.getOrDefault("taskId", "unknown");
            String node = m.getOrDefault("nodeName", "unknown");
            String result = m.getOrDefault("result", "unknown");
            String system = m.getOrDefault("system", "unknown");
            String ten = m.getOrDefault("tenant", "unknown");

            Instant createdAt = toInstant(sp.getValue());
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", task + "-" + node + "-" + result + "-" + m.getOrDefault("category", "unknown") + "-"
                    + m.getOrDefault("system", "unknown") + "-" + m.getOrDefault("workitemId", "") + "-"
                    + m.getOrDefault("transNo", "") + "-" + m.getOrDefault("eventType", ""));
            row.put("createdAt", createdAt != null ? createdAt.toString() : null);
            row.put("taskId", task);
            row.put("category", m.getOrDefault("category", "unknown"));
            row.put("tenant", ten);
            row.put("systemNo", system);
            row.put("nodeName", node);
            row.put("result", result);
            row.put("busId", m.getOrDefault("busId", ""));
            row.put("adviseKey", m.getOrDefault("eventType", ""));
            row.put("transNo", m.getOrDefault("transNo", ""));
            row.put("workitemId", m.getOrDefault("workitemId", ""));
            row.put("messageType", m.getOrDefault("messageType", ""));
            row.put("labels", m);

            String rawJson = rawStore.get(rawStore.buildLookupKey(m));
            row.put("rawJson", rawJson);
            rows.add(row);
        }
        return rows;
    }

    public List<Map<String, Object>> trace(String taskId) {
        if (taskId == null || taskId.isBlank()) {
            return Collections.emptyList();
        }
        String selector = "{taskId=\"" + escapeLabelValue(taskId.trim()) + "\"}";
        String q = "max_over_time(msg_task_last_seen_seconds" + selector + "[30d])";
        List<PrometheusQueryService.SeriesPoint> lastSeen = prometheus.queryVector(q);

        lastSeen.sort(Comparator.comparing(PrometheusQueryService.SeriesPoint::getValue,
                Comparator.nullsLast(Comparator.naturalOrder())));

        List<Map<String, Object>> out = new ArrayList<>(lastSeen.size());
        for (PrometheusQueryService.SeriesPoint sp : lastSeen) {
            Map<String, String> m = sp.getMetric();
            String node = m.getOrDefault("nodeName", "unknown");
            String result = m.getOrDefault("result", "unknown");
            String system = m.getOrDefault("system", "unknown");
            String ten = m.getOrDefault("tenant", "unknown");

            Instant createdAt = toInstant(sp.getValue());
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", taskId + "-" + node + "-" + result + "-" + m.getOrDefault("category", "unknown") + "-"
                    + m.getOrDefault("system", "unknown") + "-" + m.getOrDefault("workitemId", "") + "-"
                    + m.getOrDefault("transNo", "") + "-" + m.getOrDefault("eventType", ""));
            row.put("createdAt", createdAt != null ? createdAt.toString() : null);
            row.put("taskId", taskId);
            row.put("tenant", ten);
            row.put("systemNo", system);
            row.put("category", m.getOrDefault("category", "unknown"));
            row.put("transNo", m.getOrDefault("transNo", ""));
            row.put("nodeName", node);
            row.put("result", result);
            row.put("workitemId", m.getOrDefault("workitemId", ""));
            row.put("messageType", m.getOrDefault("messageType", ""));
            row.put("adviseKey", m.getOrDefault("eventType", ""));
            row.put("busId", m.getOrDefault("busId", ""));
            row.put("labels", m);

            // 尝试获取原始 JSON
            String rawJson = rawStore.get(rawStore.buildLookupKey(m));
            row.put("rawJson", rawJson);

            out.add(row);
        }
        return out;
    }

    public Map<String, Object> pendingTasks(String q,
            String category,
            String nodeName,
            int minutes,
            int expectedSeconds,
            int size) {
        int safeMinutes = clamp(minutes, 1, 60 * 24 * 15);
        int safeExpected = clamp(expectedSeconds, 1, 60 * 60 * 24);
        int safeSize = clamp(size, 1, 500);

        long nowSec = Math.max(0, System.currentTimeMillis() / 1000);
        String selector = buildSelector(q, category, nodeName, null);
        String window = safeMinutes + "m";

        String startSelector = mergeSelector(selector, "result=\"PENDING\"");
        String doneSelector = mergeSelector(selector, "result!=\"PENDING\"");

        String startQ = "max by (taskId,tenant,system,busId,eventType,nodeName) (max_over_time(msg_task_last_seen_seconds"
                + startSelector + "[" + window + "]))";
        String doneQ = "max by (taskId) (max_over_time(msg_task_last_seen_seconds" + doneSelector + "[" + window
                + "]))";

        List<PrometheusQueryService.SeriesPoint> starts = prometheus.queryVector(startQ);
        List<PrometheusQueryService.SeriesPoint> dones = prometheus.queryVector(doneQ);

        Map<String, Long> doneByTaskId = new LinkedHashMap<>();
        for (PrometheusQueryService.SeriesPoint sp : dones) {
            String taskId = sp.getMetric().getOrDefault("taskId", "");
            if (taskId.isEmpty()) {
                continue;
            }
            doneByTaskId.put(taskId, toLongOrZero(sp.getValue()));
        }

        List<Map<String, Object>> rows = new ArrayList<>();
        for (PrometheusQueryService.SeriesPoint sp : starts) {
            Map<String, String> m = sp.getMetric();
            String taskId = m.getOrDefault("taskId", "");
            if (taskId.isEmpty()) {
                continue;
            }

            long startSeenSec = toLongOrZero(sp.getValue());
            if (startSeenSec <= 0) {
                continue;
            }
            if (doneByTaskId.containsKey(taskId)) {
                continue;
            }

            long waitSec = Math.max(0, nowSec - startSeenSec);
            if (waitSec < safeExpected) {
                continue;
            }

            Map<String, Object> row = new LinkedHashMap<>();
            row.put("taskId", taskId);
            row.put("tenant", m.getOrDefault("tenant", "unknown"));
            row.put("systemNo", m.getOrDefault("system", "unknown"));
            row.put("busId", m.getOrDefault("busId", ""));
            row.put("adviseKey", m.getOrDefault("eventType", ""));
            row.put("startNodeName", m.getOrDefault("nodeName", "unknown"));
            row.put("startSeenAt", Instant.ofEpochSecond(startSeenSec).toString());
            row.put("waitSeconds", waitSec);
            row.put("labels", m);
            rows.add(row);
        }

        rows.sort((a, b) -> Long.compare(toLongOrZero(b.get("waitSeconds")), toLongOrZero(a.get("waitSeconds"))));
        if (rows.size() > safeSize) {
            rows = rows.subList(0, safeSize);
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("window", window);
        out.put("expectedSeconds", safeExpected);
        out.put("serverTimeMs", System.currentTimeMillis());
        out.put("content", rows);
        out.put("totalElements", rows.size());
        return out;
    }

    private static Map<String, Object> pageOf(List<Map<String, Object>> content, long totalElements) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("content", content);
        out.put("totalElements", totalElements);
        return out;
    }

    private static String buildSelector(String q, String category, String nodeName, String taskId) {
        List<String> parts = new ArrayList<>();
        if (category != null && !category.isBlank()) {
            if ("__ALL_TENANTS__".equals(category)) {
                parts.add("category!~\"state|competence|STATE|COMPETENCE\"");
            } else {
                parts.add("category=\"" + escapeLabelValue(category.trim()) + "\"");
            }
        }
        if (nodeName != null && !nodeName.isBlank()) {
            parts.add("nodeName=\"" + escapeLabelValue(nodeName.trim()) + "\"");
        }
        if (taskId != null && !taskId.isBlank()) {
            parts.add("taskId=\"" + escapeLabelValue(taskId.trim()) + "\"");
        } else if (q != null && !q.isBlank()) {
            parts.add("taskId=~\".*" + escapeRegexLiteral(q.trim()) + ".*\"");
        }

        if (parts.isEmpty()) {
            return "";
        }
        return "{" + String.join(",", parts) + "}";
    }

    private static String mergeSelector(String selector, String extra) {
        if (selector == null || selector.isBlank()) {
            return "{" + extra + "}";
        }
        String s = selector.trim();
        if (s.endsWith("}")) {
            return s.substring(0, s.length() - 1) + "," + extra + "}";
        }
        return "{" + extra + "}";
    }

    private static Instant toInstant(Double v) {
        if (v == null || Double.isNaN(v) || Double.isInfinite(v) || v <= 0) {
            return null;
        }
        return Instant.ofEpochMilli(Math.round(v * 1000.0));
    }

    private static long toLongOrZero(Double v) {
        if (v == null || Double.isNaN(v) || Double.isInfinite(v)) {
            return 0;
        }
        return Math.max(0, Math.round(v));
    }

    private static long toLongOrZero(Object v) {
        if (v == null) {
            return 0;
        }
        if (v instanceof Number) {
            return Math.max(0, ((Number) v).longValue());
        }
        try {
            return Math.max(0, Long.parseLong(String.valueOf(v)));
        } catch (Exception ignored) {
            return 0;
        }
    }

    private static int clamp(int v, int min, int max) {
        return Math.max(min, Math.min(max, v));
    }

    private static String escapeLabelValue(String v) {
        return v.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String escapeRegexLiteral(String v) {
        if (v == null || v.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder(v.length() * 2);
        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);
            if (c == '\\' || c == '.' || c == '*' || c == '+' || c == '?' || c == '^' || c == '$'
                    || c == '{' || c == '}' || c == '(' || c == ')' || c == '|' || c == '[' || c == ']') {
                sb.append('\\');
            }
            sb.append(c);
        }
        return escapeLabelValue(sb.toString());
    }
}
