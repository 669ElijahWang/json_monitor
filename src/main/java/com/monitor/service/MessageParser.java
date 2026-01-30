package com.monitor.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

/**
 * 消息解析器：从原始 JSON 中抽取监控所需核心字段。
 * 支持两种消息类型：
 * 1. STATE 类型（key以state/开头）- Kafka消息接收状态
 * - watchState: 0=待处理, 1=获取, 2=处理中, 4=处理失败, 5=处理完成
 * 2. AGENT 类型（key以AGENT/开头）- 业务流程节点状态
 * - systemState: WaitForCheckOut, WaitForApply, Running, Suspend, Complete,
 * Terminate, Revoke
 * - workitemState: 1=初始化, 2=待处理, 4=处理中, 5=挂起, 6=完成, 7=已终止
 */
@Component
public class MessageParser {
    private final ObjectMapper objectMapper;

    public static final String MSG_TYPE_STATE = "STATE";
    public static final String MSG_TYPE_AGENT = "AGENT";
    public static final String MSG_TYPE_UNKNOWN = "UNKNOWN";

    public MessageParser(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * 根据消息key判断消息类型
     */
    public String detectMessageType(String key) {
        if (key == null || key.isBlank()) {
            return MSG_TYPE_UNKNOWN;
        }
        if (key.startsWith("state/")) {
            return MSG_TYPE_STATE;
        }
        if (key.startsWith("AGENT/")) {
            return MSG_TYPE_AGENT;
        }
        return MSG_TYPE_UNKNOWN;
    }

    /**
     * 解析原始 JSON 文本为 ParsedMessage，带 key 用于判断消息类型。
     */
    public ParsedMessage parse(String key, String rawJson) {
        String messageType = detectMessageType(key);

        try {
            JsonNode root = objectMapper.readTree(rawJson);

            if (MSG_TYPE_STATE.equals(messageType)) {
                return parseStateMessage(root);
            } else if (MSG_TYPE_AGENT.equals(messageType)) {
                return parseAgentMessage(root);
            } else {
                // 兼容旧格式
                return parseLegacyMessage(root);
            }
        } catch (Exception e) {
            return createErrorMessage(messageType);
        }
    }

    /**
     * 解析 STATE 类型消息（Kafka消息接收状态）
     * key格式: state/{taskId}/{uuid}
     * 示例:
     * state/SUNYARDBP26011515382155320260129180226778644436/59bcec04-b73a-443f-a186-a20297e705e5
     */
    private ParsedMessage parseStateMessage(JsonNode root) {
        String taskId = text(root, "taskId");
        String tenant = text(root, "tenant");
        String transNo = text(root, "transNo");
        String watchState = text(root, "watchState");
        String serverIp = text(root, "serverIp");
        String nodeName = text(root, "nodeName");
        String workitemId = text(root, "workitemId");
        String processId = text(root, "processId");

        // 根据watchState决定result
        String result = mapWatchStateToResult(watchState);

        return new ParsedMessage(
                MSG_TYPE_STATE,
                safe(taskId, "UNKNOWN"),
                tenant,
                null, // systemNo
                null, // adviseKey
                nodeName,
                null, // busId
                null, // busVer
                result,
                null, // produceTimeMs
                null, // processedTimeMs
                null, // internalSeconds
                watchState,
                serverIp,
                processId,
                workitemId,
                transNo,
                null, // systemState
                null, // workitemState
                null, // userNo
                null, // startTime
                null, // checkOutTime
                null // checkInTime
        );
    }

    /**
     * 解析 AGENT 类型消息（业务流程节点状态）
     * key格式: AGENT/{taskId}/{transNo}/{partition}/{workitemId}/{uuid}
     * 示例:
     * AGENT/SUNYARDBP26011515382155320260129175254051282744/KAFKA4000/0/5230032/9a11d
     */
    private ParsedMessage parseAgentMessage(JsonNode root) {
        String tenant = text(root, "priTenant");
        String taskId = text(root, "taskId");
        String adviseKey = text(root, "adviseKey");

        JsonNode transRequest = root.path("transRequest");
        String systemNo = text(transRequest, "systemNo");
        String userNo = text(transRequest, "userNo");

        JsonNode operDetail = transRequest.path("operDetail");
        String nodeName = text(operDetail, "nodeName");
        String workitemId = text(operDetail, "workitemId");
        String startTime = text(operDetail, "startTime");
        String checkOutTime = text(operDetail, "checkOutTime");
        String checkInTime = text(operDetail, "checkInTime");
        String workitemState = text(operDetail, "workitemState");

        JsonNode businessProcess = transRequest.path("businessProcess");
        String busTaskId = text(businessProcess, "taskId");
        if (taskId == null && busTaskId != null) {
            taskId = busTaskId;
        }
        String busId = text(businessProcess, "busId");
        String busVer = text(businessProcess, "busVer");

        JsonNode systemInfo = root.path("systemInfo");
        String systemState = text(systemInfo, "state");
        String processId = text(systemInfo, "processId");
        if (workitemId == null) {
            // 从systemInfo取workitemId（可能是数字类型）
            JsonNode wid = systemInfo.path("workitemId");
            if (!wid.isMissingNode() && !wid.isNull()) {
                workitemId = wid.isNumber() ? String.valueOf(wid.asLong()) : wid.asText();
            }
        }

        // 根据systemState和workitemState决定result
        String result = mapAgentStateToResult(systemState, workitemState);

        return new ParsedMessage(
                MSG_TYPE_AGENT,
                safe(taskId, "UNKNOWN"),
                tenant,
                systemNo,
                adviseKey,
                nodeName,
                busId,
                busVer,
                result,
                null, // produceTimeMs
                null, // processedTimeMs
                null, // internalSeconds
                null, // watchState
                null, // serverIp
                processId,
                workitemId,
                null, // transNo
                systemState,
                workitemState,
                userNo,
                startTime,
                checkOutTime,
                checkInTime);
    }

    /**
     * 解析旧格式消息（兼容）
     */
    private ParsedMessage parseLegacyMessage(JsonNode root) {
        String tenant = text(root, "priTenant");
        String taskId = text(root, "taskId");
        String adviseKey = text(root, "adviseKey");
        Long produceTimeMs = longValue(root, "produceTime");
        Long processedTimeMs = longValue(root, "processedTime");
        Double internalSeconds = doubleValue(root, "internalSeconds");

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
                MSG_TYPE_UNKNOWN,
                safe(taskId, "UNKNOWN"),
                tenant,
                systemNo,
                adviseKey,
                nodeName,
                busId,
                busVer,
                result,
                produceTimeMs,
                processedTimeMs,
                internalSeconds,
                null, null, null, null, null,
                null, null, null, null, null, null);
    }

    /**
     * 将watchState映射为result
     */
    private String mapWatchStateToResult(String watchState) {
        if (watchState == null)
            return "UNKNOWN";
        switch (watchState) {
            case "0":
                return "PENDING"; // 待处理
            case "1":
                return "ACQUIRED"; // 获取
            case "2":
                return "PROCESSING"; // 处理中
            case "4":
                return "FAIL"; // 处理失败
            case "5":
                return "SUCCESS"; // 处理完成
            default:
                return "UNKNOWN";
        }
    }

    /**
     * 将AGENT状态映射为result
     */
    private String mapAgentStateToResult(String systemState, String workitemState) {
        // 优先使用systemState判断
        if (systemState != null) {
            switch (systemState) {
                case "Complete":
                    return "SUCCESS";
                case "Terminate":
                    return "TERMINATED";
                case "Revoke":
                    return "REVOKED";
                case "Suspend":
                    return "SUSPENDED";
                case "Running":
                    return "PROCESSING";
                case "WaitForCheckOut":
                    return "WAIT_CHECKOUT";
                case "WaitForApply":
                    return "WAIT_APPLY";
            }
        }
        // 其次使用workitemState判断
        if (workitemState != null) {
            switch (workitemState) {
                case "1":
                    return "INIT"; // 初始化
                case "2":
                    return "PENDING"; // 待处理
                case "4":
                    return "PROCESSING"; // 处理中
                case "5":
                    return "SUSPENDED"; // 挂起
                case "6":
                    return "SUCCESS"; // 完成
                case "7":
                    return "TERMINATED"; // 已终止
            }
        }
        return "UNKNOWN";
    }

    private ParsedMessage createErrorMessage(String messageType) {
        return new ParsedMessage(
                messageType,
                "UNKNOWN", null, null, null, null, null, null, "PARSE_ERROR",
                null, null, null,
                null, null, null, null, null,
                null, null, null, null, null, null);
    }

    /**
     * 解析原始 JSON 文本为 ParsedMessage（无key版本，兼容旧调用）。
     */
    public ParsedMessage parse(String rawJson) {
        return parse(null, rawJson);
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

    private static Double doubleValue(JsonNode node, String field) {
        JsonNode v = node.path(field);
        if (v.isMissingNode() || v.isNull()) {
            return null;
        }
        if (v.isNumber()) {
            return v.asDouble();
        }
        String s = v.asText();
        if (s == null || s.isBlank()) {
            return null;
        }
        try {
            return Double.parseDouble(s);
        } catch (Exception ignored) {
            return null;
        }
    }

    private static String safe(String value, String fallback) {
        return (value == null || value.isBlank()) ? fallback : value;
    }
}
