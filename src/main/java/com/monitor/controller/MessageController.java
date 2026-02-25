package com.monitor.controller;

import com.monitor.service.PrometheusMessageService;
import com.monitor.service.RealtimeStatsService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;

/**
 * 消息查询接口：提供基于 Prometheus 的历史查询和基于内存的实时查询。
 */
@RestController
@RequestMapping("/api")
public class MessageController {
    private final PrometheusMessageService messages;
    private final RealtimeStatsService realtimeStats;

    public MessageController(PrometheusMessageService messages, RealtimeStatsService realtimeStats) {
        this.messages = messages;
        this.realtimeStats = realtimeStats;
    }

    @GetMapping("/messages")
    public Map<String, Object> list(@RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(defaultValue = "60") int minutes) {
        return search(null, null, null, null, minutes, page, size);
    }

    @GetMapping("/messages/{taskId}")
    public Object byTaskId(@PathVariable String taskId) {
        // 1. 获取 Prometheus 历史数据
        List<Map<String, Object>> prometheusTrace = messages.trace(taskId);

        // 2. 获取内存中的实时数据
        List<Map<String, Object>> realtimeTrace = realtimeStats.searchMessages(null, null, null, taskId);

        // 3. 合并并去重
        Map<String, Map<String, Object>> merged = new LinkedHashMap<>();

        // 先处理 Prometheus 数据（历史轨迹）
        for (Map<String, Object> m : prometheusTrace) {
            String key = buildDedupeKey(m);
            merged.put(key, m);
        }

        // 处理实时数据（可能包含更全的 rawJson），如果 Key 冲突，则保留较新或较全的那一个
        // 这里采用：如果已存在，则只在原数据缺失 rawJson 时补充
        for (Map<String, Object> rm : realtimeTrace) {
            String key = buildDedupeKey(rm);
            if (merged.containsKey(key)) {
                Map<String, Object> existing = merged.get(key);
                if ((existing.get("rawJson") == null || String.valueOf(existing.get("rawJson")).isEmpty())
                        && (rm.get("rawJson") != null)) {
                    existing.put("rawJson", rm.get("rawJson"));
                }
            } else {
                merged.put(key, rm);
            }
        }

        List<Map<String, Object>> combined = new ArrayList<>(merged.values());

        // 4. 按 createdAt 排序
        combined.sort((a, b) -> {
            String ta = (String) a.getOrDefault("createdAt", "");
            String tb = (String) b.getOrDefault("createdAt", "");
            return ta.compareTo(tb);
        });

        return combined;
    }

    @GetMapping("/messages/search")
    public Map<String, Object> search(@RequestParam(required = false) String q,
            @RequestParam(required = false) String category,
            @RequestParam(required = false) String nodeName,
            @RequestParam(required = false) String taskId,
            @RequestParam(defaultValue = "60") int minutes,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {

        // 1. 获取 Prometheus 数据 (扩大范围以进行全局去重)
        Map<String, Object> pResult = messages.search(q, category, nodeName, taskId, minutes, 0, 5000);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> pContent = (List<Map<String, Object>>) pResult.get("content");

        // 2. 获取内存中的实时数据
        List<Map<String, Object>> realtimeAll = realtimeStats.searchMessages(q, category, nodeName, taskId);

        // 3. 全局合并并去重
        Map<String, Map<String, Object>> merged = new LinkedHashMap<>();

        // 先放 Prometheus 数据
        for (Map<String, Object> m : pContent) {
            merged.put(buildAggressiveDedupeKey(m), m);
        }

        // 再放内存数据，补充缺失内容
        for (Map<String, Object> rm : realtimeAll) {
            String key = buildAggressiveDedupeKey(rm);
            if (merged.containsKey(key)) {
                Map<String, Object> existing = merged.get(key);
                if (isEmpty(existing.get("rawJson")) && !isEmpty(rm.get("rawJson"))) {
                    existing.put("rawJson", rm.get("rawJson"));
                }
            } else {
                merged.put(key, rm);
            }
        }

        List<Map<String, Object>> allItems = new ArrayList<>(merged.values());
        // 按时间倒序
        allItems.sort((a, b) -> {
            String ta = safeStr(a.get("createdAt"), "");
            String tb = safeStr(b.get("createdAt"), "");
            return tb.compareTo(ta);
        });

        int totalElements = allItems.size();
        int startIndex = page * size;
        List<Map<String, Object>> pageContent = new ArrayList<>();
        if (startIndex < totalElements) {
            int toIndex = Math.min(startIndex + size, totalElements);
            pageContent = allItems.subList(startIndex, toIndex);
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("content", pageContent);
        out.put("totalElements", (long) totalElements);
        return out;
    }

    private String buildDedupeKey(Map<String, Object> m) {
        return buildAggressiveDedupeKey(m);
    }

    private String buildAggressiveDedupeKey(Map<String, Object> m) {
        // 更加激进的去重标识：
        // 1. 如果 transNo 或 workitemId 为空/unknown，则不参与唯一性判断，避免同一个事件的补全过程产生多条记录
        // 2. 全部转小写并去空格

        String tid = safeStr(m.get("taskId"), "unknown").toLowerCase().trim();
        String node = safeStr(m.get("nodeName"), "unknown").toLowerCase().trim();
        String res = safeStr(m.get("result"), "unknown").toLowerCase().trim();
        String cat = safeStr(m.get("category"), "unknown").toLowerCase().trim();
        String sys = safeStr(m.get("systemNo"), "unknown").toLowerCase().trim();

        // 只有当有实际业务编号时才加入 Key，否则视为同一事件的不同阶段
        String trn = safeStr(m.get("transNo"), "");
        String wid = safeStr(m.get("workitemId"), "");
        String adv = safeStr(m.get("adviseKey"), "");

        return tid + "|" + node + "|" + res + "|" + cat + "|" + sys + "|" + trn + "|" + wid + "|" + adv;
    }

    private boolean isEmpty(Object val) {
        if (val == null)
            return true;
        return val.toString().isBlank();
    }

    private String safeStr(Object val, String def) {
        if (val == null)
            return def;
        String s = val.toString();
        // 关键修复：Prometheus 指标中空值为 "unknown"，而内存中为 null。
        // 将 "unknown" 视为无值，回退到 def (def 可能就是 "unknown" 也可能是 "")
        if (s.isBlank() || "unknown".equalsIgnoreCase(s))
            return def;
        return s.trim();
    }

    @GetMapping("/tasks/pending")
    public Map<String, Object> pendingTasks(@RequestParam(required = false) String q,
            @RequestParam(required = false) String category,
            @RequestParam(required = false) String nodeName,
            @RequestParam(defaultValue = "60") int minutes,
            @RequestParam(defaultValue = "30") int expectedSeconds,
            @RequestParam(defaultValue = "200") int size) {
        return messages.pendingTasks(q, category, nodeName, minutes, expectedSeconds, size);
    }
}
