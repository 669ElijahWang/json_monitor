package com.monitor.service;

public class ParsedMessage {
    private final String taskId;
    private final String tenant;
    private final String systemNo;
    private final String adviseKey;
    private final String nodeName;
    private final String busId;
    private final String busVer;
    private final String result;
    private final Long produceTimeMs;

    public ParsedMessage(String taskId,
                         String tenant,
                         String systemNo,
                         String adviseKey,
                         String nodeName,
                         String busId,
                         String busVer,
                         String result,
                         Long produceTimeMs) {
        this.taskId = taskId;
        this.tenant = tenant;
        this.systemNo = systemNo;
        this.adviseKey = adviseKey;
        this.nodeName = nodeName;
        this.busId = busId;
        this.busVer = busVer;
        this.result = result;
        this.produceTimeMs = produceTimeMs;
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

    public String getBusId() {
        return busId;
    }

    public String getBusVer() {
        return busVer;
    }

    public String getResult() {
        return result;
    }

    public Long getProduceTimeMs() {
        return produceTimeMs;
    }
}

