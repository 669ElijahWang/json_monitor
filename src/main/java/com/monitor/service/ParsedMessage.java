package com.monitor.service;

/**
 * 解析后的消息对象，支持多种消息类型：
 * 1. STATE 类型（state/开头）- Kafka消息接收状态
 * - watchState: 0=初始节点, 1=获取, 2=处理中, 4=处理失败, 5=处理完成
 * 2. COMPETENCE 类型（competence/开头）- 异常消息
 * - errorCode, errorType, errorLevel, errorInfo, errorInterface
 * 3. TENANT_MESSAGE 类型（SUNYARD/AGENT/其他租户开头）- 租户业务消息
 * - systemState: WaitForCheckOut, WaitForApply, Running, Suspend, Complete,
 * Terminate, Revoke
 * - workitemState: 1=初始化, 2=初始节点, 4=处理中, 5=挂起, 6=完成, 7=已终止
 */
public class ParsedMessage {
    /** 消息类型：STATE/COMPETENCE/TENANT_MESSAGE/UNKNOWN */
    private final String messageType;
    /** 消息种类：从key前缀提取，如 state/competence/SUNYARD/AGENT 等 */
    private final String category;
    private final String taskId;
    private final String tenant;
    private final String systemNo;
    private final String adviseKey;
    private final String nodeName;
    private final String result;

    // STATE消息特有字段
    /** Kafka消息接收状态：0=初始节点, 1=获取, 2=处理中, 4=处理失败, 5=处理完成 */
    private final String watchState;
    private final String serverIp;
    private final String processId;
    private final String workitemId;
    private final String transNo;

    // COMPETENCE消息特有字段（异常）
    private final String errorCode;
    private final String errorType;
    private final String errorLevel;
    private final String errorInfo;
    private final String errorInterface;

    // TENANT_MESSAGE消息特有字段（租户消息）
    /**
     * SystemInfo状态：WaitForCheckOut, WaitForApply, Running, Suspend, Complete,
     * Terminate, Revoke
     */
    private final String systemState;
    /** operDetail.workitemstate：1=初始化, 2=初始节点, 4=处理中, 5=挂起, 6=完成, 7=已终止 */
    private final String workitemState;
    private final String userNo;
    private final String startTime;
    private final String checkOutTime;
    private final String checkInTime;

    public ParsedMessage(String messageType,
            String category,
            String taskId,
            String tenant,
            String systemNo,
            String adviseKey,
            String nodeName,
            String result,
            String watchState,
            String serverIp,
            String processId,
            String workitemId,
            String transNo,
            String errorCode,
            String errorType,
            String errorLevel,
            String errorInfo,
            String errorInterface,
            String systemState,
            String workitemState,
            String userNo,
            String startTime,
            String checkOutTime,
            String checkInTime) {
        this.messageType = messageType;
        this.category = category;
        this.taskId = taskId;
        this.tenant = tenant;
        this.systemNo = systemNo;
        this.adviseKey = adviseKey;
        this.nodeName = nodeName;
        this.result = result;
        this.watchState = watchState;
        this.serverIp = serverIp;
        this.processId = processId;
        this.workitemId = workitemId;
        this.transNo = transNo;
        this.errorCode = errorCode;
        this.errorType = errorType;
        this.errorLevel = errorLevel;
        this.errorInfo = errorInfo;
        this.errorInterface = errorInterface;
        this.systemState = systemState;
        this.workitemState = workitemState;
        this.userNo = userNo;
        this.startTime = startTime;
        this.checkOutTime = checkOutTime;
        this.checkInTime = checkInTime;
    }

    public String getMessageType() {
        return messageType;
    }

    public String getCategory() {
        return category;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getTenant() {
        return tenant;
    }

    public String getSystemNo() {
        return systemNo;
    }

    public String getAdviseKey() {
        return adviseKey;
    }

    public String getNodeName() {
        return nodeName;
    }

    public String getResult() {
        return result;
    }

    public String getWatchState() {
        return watchState;
    }

    public String getServerIp() {
        return serverIp;
    }

    public String getProcessId() {
        return processId;
    }

    public String getWorkitemId() {
        return workitemId;
    }

    public String getTransNo() {
        return transNo;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public String getErrorType() {
        return errorType;
    }

    public String getErrorLevel() {
        return errorLevel;
    }

    public String getErrorInfo() {
        return errorInfo;
    }

    public String getErrorInterface() {
        return errorInterface;
    }

    public String getSystemState() {
        return systemState;
    }

    public String getWorkitemState() {
        return workitemState;
    }

    public String getUserNo() {
        return userNo;
    }

    public String getStartTime() {
        return startTime;
    }

    public String getCheckOutTime() {
        return checkOutTime;
    }

    public String getCheckInTime() {
        return checkInTime;
    }

    /**
     * 获取watchState的中文描述
     */
    public String getWatchStateDesc() {
        if (watchState == null)
            return null;
        switch (watchState) {
            case "0":
                return "初始节点";
            case "1":
                return "获取";
            case "2":
                return "处理中";
            case "4":
                return "处理失败";
            case "5":
                return "处理完成";
            default:
                return watchState;
        }
    }

    /**
     * 获取workitemState的中文描述
     */
    public String getWorkitemStateDesc() {
        if (workitemState == null)
            return null;
        switch (workitemState) {
            case "1":
                return "初始化";
            case "2":
                return "初始节点";
            case "4":
                return "处理中";
            case "5":
                return "挂起";
            case "6":
                return "完成";
            case "7":
                return "已终止";
            default:
                return workitemState;
        }
    }

    /**
     * 将时间字符串(yyyyMMddHHmmss格式)转换为毫秒时间戳
     */
    private static Long parseTimeString(String timeStr) {
        if (timeStr == null || timeStr.length() != 14) {
            return null;
        }
        try {
            int year = Integer.parseInt(timeStr.substring(0, 4));
            int month = Integer.parseInt(timeStr.substring(4, 6));
            int day = Integer.parseInt(timeStr.substring(6, 8));
            int hour = Integer.parseInt(timeStr.substring(8, 10));
            int minute = Integer.parseInt(timeStr.substring(10, 12));
            int second = Integer.parseInt(timeStr.substring(12, 14));

            java.util.Calendar cal = java.util.Calendar.getInstance();
            cal.set(year, month - 1, day, hour, minute, second);
            cal.set(java.util.Calendar.MILLISECOND, 0);
            return cal.getTimeInMillis();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 获取startTime的毫秒时间戳
     */
    public Long getStartTimeMs() {
        return parseTimeString(startTime);
    }

    /**
     * 获取checkOutTime的毫秒时间戳
     */
    public Long getCheckOutTimeMs() {
        return parseTimeString(checkOutTime);
    }

    /**
     * 获取checkInTime的毫秒时间戳
     */
    public Long getCheckInTimeMs() {
        return parseTimeString(checkInTime);
    }
}
