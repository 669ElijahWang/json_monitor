package com.monitor.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

/**
 * 消息解析器：从原始 JSON 中抽取监控所需核心字段。
 * 解析策略：
 * - 优先从顶层读取 result；为空时回退到 transResponse.result；仍为空则置为 UNKNOWN
 * - 长整型支持数字与字符串两种表示
 * - 解析异常时返回 PARSE_ERROR，并以 UNKNOWN 兜底 taskId
 */
@Component
public class MessageParser {
    private final ObjectMapper objectMapper;

    public MessageParser(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * 解析原始 JSON 文本为 ParsedMessage。
     */
    public ParsedMessage parse(String rawJson) {
        try {
            JsonNode root = objectMapper.readTree(rawJson);
            String tenant = text(root, "priTenant");
            String taskId = text(root, "taskId");
            String adviseKey = text(root, "adviseKey");
            Long produceTimeMs = longValue(root, "produceTime");

            JsonNode transRequest = root.path("transRequest");
            String systemNo = text(transRequest, "systemNo");
            String nodeName = text(transRequest.path("operDetail"), "nodeName");
            JsonNode businessProcess = transRequest.path("businessProcess");
            String busId = text(businessProcess, "busId");
            String busVer = text(businessProcess, "busVer");

            String result = text(root, "result");
            if (result == null || result.isBlank()) {
                result = text(root.path("transResponse"), "result");
            }
            if (result == null || result.isBlank()) {
                result = "UNKNOWN";
            }

            return new ParsedMessage(
                    safe(taskId, "UNKNOWN"),
                    tenant,
                    systemNo,
                    adviseKey,
                    nodeName,
                    busId,
                    busVer,
                    result,
                    produceTimeMs
            );
        } catch (Exception e) {
            return new ParsedMessage("UNKNOWN", null, null, null, null, null, null, "PARSE_ERROR", null);
        }
    }

    /**
     * 读取文本字段；缺失或空白返回 null。
     */
    private static String text(JsonNode node, String field) {
        JsonNode v = node.path(field);
        if (v.isMissingNode() || v.isNull()) {
            return null;
        }
        String s = v.asText();
        return (s == null || s.isBlank()) ? null : s;
    }

    /**
     * 读取 Long 值；支持数值与字符串，异常时返回 null。
     */
    private static Long longValue(JsonNode node, String field) {
        JsonNode v = node.path(field);
        if (v.isMissingNode() || v.isNull()) {
            return null;
        }
        if (v.isNumber()) {
            return v.asLong();
        }
        String s = v.asText();
        if (s == null || s.isBlank()) {
            return null;
        }
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static String safe(String value, String fallback) {
        return (value == null || value.isBlank()) ? fallback : value;
    }
}
