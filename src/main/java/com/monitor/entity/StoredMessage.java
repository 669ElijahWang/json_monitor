package com.monitor.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Lob;
import javax.persistence.Table;
import java.time.Instant;

/**
 * 监控落库消息实体：用于前端检索、链路追踪、窗口统计。
 * rawJson 保存原始消息体，便于排查与回放。
 */
@Entity
@Table(name = "stored_message", indexes = {
        @Index(name = "idx_task_id", columnList = "taskId"),
        @Index(name = "idx_created_at", columnList = "createdAt"),
        @Index(name = "idx_tenant_system", columnList = "tenant,systemNo")
})
public class StoredMessage {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String taskId;

    private String tenant;
    private String systemNo;
    private String adviseKey;
    private String nodeName;
    private String busId;
    private String busVer;
    private String result;

    private Long produceTimeMs;
    private Long observedTimeMs;

    @Column(nullable = false)
    private Instant createdAt;

    @Lob
    @Column(nullable = false)
    private String rawJson;

    public StoredMessage() {
    }

    public Long getId() {
        return id;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public String getSystemNo() {
        return systemNo;
    }

    public void setSystemNo(String systemNo) {
        this.systemNo = systemNo;
    }

    public String getAdviseKey() {
        return adviseKey;
    }

    public void setAdviseKey(String adviseKey) {
        this.adviseKey = adviseKey;
    }

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public String getBusId() {
        return busId;
    }

    public void setBusId(String busId) {
        this.busId = busId;
    }

    public String getBusVer() {
        return busVer;
    }

    public void setBusVer(String busVer) {
        this.busVer = busVer;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public Long getProduceTimeMs() {
        return produceTimeMs;
    }

    public void setProduceTimeMs(Long produceTimeMs) {
        this.produceTimeMs = produceTimeMs;
    }

    public Long getObservedTimeMs() {
        return observedTimeMs;
    }

    public void setObservedTimeMs(Long observedTimeMs) {
        this.observedTimeMs = observedTimeMs;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public String getRawJson() {
        return rawJson;
    }

    public void setRawJson(String rawJson) {
        this.rawJson = rawJson;
    }
}
