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

    public PrometheusMessageService(PrometheusQueryService prometheus) {
        this.prometheus = prometheus;
    }

    public Map<String, Object> search(String q,
            String tenant,
            String systemNo,
            String busId,
            String taskId,
            int minutes,
            int page,
            int size) {
        int safeMinutes = clamp(minutes, 1, 60 * 24 * 15);
        int safePage = Math.max(0, page);
        int safeSize = clamp(size, 1, 200);

        String selector = buildSelector(q, tenant, systemNo, busId, taskId);
        String window = safeMinutes + "m";
        String lastSeenWindow = "max_over_time(msg_task_last_seen_seconds" + selector + "[" + window + "])";
        long totalElements = toLongOrZero(prometheus.queryScalar("count(" + lastSeenWindow + ")"));

        long rawLimit = (long) safeSize * (safePage + 1L);
        int limit = rawLimit > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) rawLimit;
        String lastSeenQ = "topk(" + limit + ", " + lastSeenWindow + ")";
        String countQ = "increase(msg_task_events_total" + selector + "[" + safeMinutes + "m])";

        List<PrometheusQueryService.SeriesPoint> lastSeen = prometheus.queryVector(lastSeenQ);
        List<PrometheusQueryService.SeriesPoint> counts = prometheus.queryVector(countQ);

        Map<String, Long> countByKey = new LinkedHashMap<>();
        for (PrometheusQueryService.SeriesPoint sp : counts) {
            countByKey.put(keyOf(sp.getMetric()), toLongOrZero(sp.getValue()));
        }

        lastSeen.sort(Comparator.comparing(PrometheusQueryService.SeriesPoint::getValue,
                Comparator.nullsLast(Comparator.naturalOrder())).reversed());

        int from = safePage * safeSize;
        if (from >= lastSeen.size()) {
            return pageOf(Collections.emptyList(), totalElements);
        }
        int to = Math.min(from + safeSize, lastSeen.size());

        List<Map<String, Object>> rows = new ArrayList<>(to - from);
        for (PrometheusQueryService.SeriesPoint sp : lastSeen.subList(from, to)) {
            Map<String, String> m = sp.getMetric();
            String task = m.getOrDefault("taskId", "unknown");
            String node = m.getOrDefault("nodeName", "unknown");
            String result = m.getOrDefault("result", "unknown");
            String system = m.getOrDefault("system", "unknown");
            String ten = m.getOrDefault("tenant", "unknown");

            long lastSeenSec = toLongOrZero(sp.getValue());
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", task + "-" + node + "-" + result);
            row.put("createdAt", lastSeenSec == 0 ? null : Instant.ofEpochSecond(lastSeenSec).toString());
            row.put("taskId", task);
            row.put("category", m.getOrDefault("category", "unknown"));
            row.put("tenant", ten);
            row.put("systemNo", system);
            row.put("nodeName", node);
            row.put("result", result);
            row.put("busId", m.getOrDefault("busId", ""));
            row.put("adviseKey", m.getOrDefault("eventType", ""));
            row.put("count", countByKey.getOrDefault(keyOf(m), 0L));
            row.put("labels", m);
            rows.add(row);
        }

        return pageOf(rows, totalElements);
    }

    public List<Map<String, Object>> trace(String taskId) {
        if (taskId == null || taskId.isBlank()) {
            return Collections.emptyList();
        }
        String selector = "{taskId=\"" + escapeLabelValue(taskId.trim()) + "\"}";
        String q = "max_over_time(msg_task_last_seen_seconds" + selector + "[15d])";
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

            long lastSeenSec = toLongOrZero(sp.getValue());
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", taskId + "-" + node + "-" + result);
            row.put("createdAt", lastSeenSec == 0 ? null : Instant.ofEpochSecond(lastSeenSec).toString());
            row.put("taskId", taskId);
            row.put("tenant", ten);
            row.put("systemNo", system);
            row.put("nodeName", node);
            row.put("result", result);
            row.put("labels", m);
            out.add(row);
        }
        return out;
    }

    public Map<String, Object> pendingTasks(String q,
            String tenant,
            String systemNo,
            String busId,
            int minutes,
            int expectedSeconds,
            int size) {
        int safeMinutes = clamp(minutes, 1, 60 * 24 * 15);
        int safeExpected = clamp(expectedSeconds, 1, 60 * 60 * 24);
        int safeSize = clamp(size, 1, 500);

        long nowSec = Math.max(0, System.currentTimeMillis() / 1000);
        String selector = buildSelector(q, tenant, systemNo, busId, null);
        String window = safeMinutes + "m";

        String startSelector = mergeSelector(selector, "result=\"NEW\"");
        String doneSelector = mergeSelector(selector, "result!=\"NEW\"");

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

    private static String buildSelector(String q, String tenant, String systemNo, String busId, String taskId) {
        List<String> parts = new ArrayList<>();
        if (tenant != null && !tenant.isBlank()) {
            parts.add("tenant=\"" + escapeLabelValue(tenant.trim()) + "\"");
        }
        if (systemNo != null && !systemNo.isBlank()) {
            parts.add("system=\"" + escapeLabelValue(systemNo.trim()) + "\"");
        }
        if (busId != null && !busId.isBlank()) {
            parts.add("busId=\"" + escapeLabelValue(busId.trim()) + "\"");
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

    private static String keyOf(Map<String, String> m) {
        return safe(m.get("tenant"))
                + "|" + safe(m.get("system"))
                + "|" + safe(m.get("taskId"))
                + "|" + safe(m.get("nodeName"))
                + "|" + safe(m.get("result"))
                + "|" + safe(m.get("busId"))
                + "|" + safe(m.get("eventType"));
    }

    private static String safe(String v) {
        return v == null ? "" : v;
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

    private static String escapeRegexValue(String v) {
        return escapeLabelValue(v).replace(".", "\\.").replace("*", "\\*").replace("+", "\\+")
                .replace("?", "\\?").replace("^", "\\^").replace("$", "\\$")
                .replace("{", "\\{").replace("}", "\\}").replace("(", "\\(").replace(")", "\\)")
                .replace("|", "\\|").replace("[", "\\[").replace("]", "\\]");
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
