package com.monitor.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Demo 消息生产者：
 * 生成两种格式的消息：
 * 1. STATE 类型（key以state/开头）- 模拟Kafka消息接收状态
 * - watchState: 0=待处理, 1=获取, 2=处理中, 4=处理失败, 5=处理完成
 * 2. AGENT 类型（key以AGENT/开头）- 模拟业务流程节点状态
 * - systemState: WaitForCheckOut, WaitForApply, Running, Suspend, Complete,
 * Terminate, Revoke
 * - workitemState: 1=初始化, 2=待处理, 4=处理中, 5=挂起, 6=完成, 7=已终止
 */
@Component
@ConditionalOnProperty(prefix = "monitor.demo.producer", name = "enabled", havingValue = "true")
public class DemoJsonProducer {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;
    private final AtomicLong workitemIdSeq = new AtomicLong(5230000);
    private final AtomicLong processIdSeq = new AtomicLong(260115170000000L);

    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    // watchState 可选值
    private static final String[] WATCH_STATES = { "0", "1", "2", "4", "5" };
    // systemState 可选值
    private static final String[] SYSTEM_STATES = { "WaitForCheckOut", "WaitForApply", "Running", "Suspend", "Complete",
            "Terminate", "Revoke" };
    // workitemState 可选值
    private static final String[] WORKITEM_STATES = { "1", "2", "4", "5", "6", "7" };
    // 租户
    private static final String[] TENANTS = { "SUNYARD", "BIOM", "DEMO" };
    // 节点
    private static final String[] NODES = { "AT1", "AT2", "AT3", "AT4", "AT5", "deptA", "deptB", "deptC" };
    // transNo
    private static final String[] TRANS_NOS = { "KAFKA2030", "KAFKA4000", "KAFKA5000" };

    public DemoJsonProducer(KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            @Value("${monitor.demo.producer.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    @Scheduled(fixedDelayString = "${monitor.demo.producer.rate-ms:1000}")
    public void produce() {
        // 随机选择生成哪种类型的消息
        if (ThreadLocalRandom.current().nextBoolean()) {
            produceStateMessage();
        } else {
            produceAgentMessage();
        }
    }

    /**
     * 生成 STATE 类型消息（Kafka消息接收状态）
     * key格式: state/{taskId}/{uuid}
     */
    private void produceStateMessage() {
        String tenant = pick(TENANTS);
        String taskId = tenant + "BP" + formatTimestamp() + randomDigits(15);
        String uuid = UUID.randomUUID().toString();
        String key = "state/" + taskId + "/" + uuid;

        String watchState = pickWeighted(WATCH_STATES, new int[] { 5, 10, 15, 5, 65 }); // 5:处理完成概率最高
        String nodeName = pick(NODES);
        String workitemId = String.valueOf(workitemIdSeq.incrementAndGet());
        String processId = "BC" + processIdSeq.incrementAndGet();
        String transNo = pick(TRANS_NOS);
        String serverIp = generateRandomIpv6();

        ObjectNode root = objectMapper.createObjectNode();
        root.put("taskId", taskId);
        root.put("tenant", tenant);
        root.put("transNo", transNo);
        root.put("watchState", watchState);
        root.put("serverIp", serverIp);
        root.put("nodeName", nodeName);
        root.put("workitemId", workitemId);
        root.put("processId", processId);

        kafkaTemplate.send(topic, key, root.toString());
    }

    /**
     * 生成 AGENT 类型消息（业务流程节点状态）
     * key格式: AGENT/{taskId}/{transNo}/{partition}/{workitemId}/{uuid}
     */
    private void produceAgentMessage() {
        String tenant = pick(TENANTS);
        String taskId = tenant + "BP" + formatTimestamp() + randomDigits(15);
        String transNo = pick(TRANS_NOS);
        int partition = ThreadLocalRandom.current().nextInt(0, 3);
        String workitemId = String.valueOf(workitemIdSeq.incrementAndGet());
        String shortUuid = UUID.randomUUID().toString().substring(0, 5);
        String key = "AGENT/" + taskId + "/" + transNo + "/" + partition + "/" + workitemId + "/" + shortUuid;

        String systemState = pickWeighted(SYSTEM_STATES, new int[] { 15, 10, 20, 5, 40, 5, 5 }); // Complete概率最高
        String workitemState = pickWeighted(WORKITEM_STATES, new int[] { 5, 10, 15, 5, 60, 5 }); // 6(完成)概率最高
        String nodeName = pick(NODES);
        String systemNo = "AGENT";
        String userNo = nodeName;
        String processId = "BC" + processIdSeq.incrementAndGet();
        String startTime = formatTimestamp();
        String adviseKey = "BP" + formatTimestamp().substring(0, 12) + "-2-" + processId + "-" + nodeName;

        ObjectNode root = objectMapper.createObjectNode();
        root.put("priTenant", tenant);
        root.put("taskId", taskId);
        root.put("adviseKey", adviseKey);

        // transRequest
        ObjectNode transRequest = root.putObject("transRequest");
        transRequest.putNull("transNo");
        transRequest.putNull("bankNo");
        transRequest.put("systemNo", systemNo);
        transRequest.put("userNo", userNo);
        transRequest.putNull("deleGatUserNo");
        transRequest.putNull("organNo");

        // transRequest.transInfo
        ObjectNode transInfo = transRequest.putObject("transInfo");
        transInfo.putNull("transId");
        ObjectNode businessInfo = transInfo.putObject("businessInfo");
        businessInfo.put("flag", "1");
        ObjectNode workitemInfos = businessInfo.putObject("WORKITEM_INFOS");
        workitemInfos.put("CHECKIN_STATE", systemState.equals("Complete") ? "WKI_FINISH" : "WKI_PROCESSING");
        workitemInfos.put("FLOW_STATE", Integer.parseInt(workitemState));
        workitemInfos.put("IS_COUNTERSIGN", false);

        // transRequest.operDetail
        ObjectNode operDetail = transRequest.putObject("operDetail");
        operDetail.put("workitemId", workitemId);
        operDetail.put("startTime", startTime);

        // 根据状态决定是否生成checkOutTime和checkInTime
        // 对于完成/终止状态，生成完整的时间信息
        if ("Complete".equals(systemState) || "Terminate".equals(systemState) || "6".equals(workitemState)
                || "7".equals(workitemState)) {
            // 已完成状态：startTime在1-5秒前，处理耗时0.1-2秒
            long startMs = System.currentTimeMillis() - ThreadLocalRandom.current().nextInt(1000, 5000);
            long checkOutMs = startMs + ThreadLocalRandom.current().nextInt(100, 500); // 0.1-0.5秒
            long checkInMs = checkOutMs + ThreadLocalRandom.current().nextInt(100, 2000); // 0.1-2秒
            operDetail.put("startTime", formatTimestamp(startMs));
            operDetail.put("checkOutTime", formatTimestamp(checkOutMs));
            operDetail.put("checkInTime", formatTimestamp(checkInMs));
        } else if ("Running".equals(systemState) || "WaitForCheckOut".equals(systemState)
                || "4".equals(workitemState)) {
            // 处理中状态：startTime在1-10秒前
            long startMs = System.currentTimeMillis() - ThreadLocalRandom.current().nextInt(1000, 10000);
            long checkOutMs = startMs + ThreadLocalRandom.current().nextInt(100, 500);
            operDetail.put("startTime", formatTimestamp(startMs));
            operDetail.put("checkOutTime", formatTimestamp(checkOutMs));
            operDetail.putNull("checkInTime");
        } else {
            // 初始化/待处理状态，没有checkOutTime和checkInTime
            operDetail.putNull("checkOutTime");
            operDetail.putNull("checkInTime");
        }

        operDetail.put("nodeName", nodeName);
        operDetail.put("workitemState", workitemState);

        // transRequest.process
        ObjectNode process = transRequest.putObject("process");
        process.put("processId", processId);
        process.put("preFlowId", "0");
        process.put("preProcessId", "0");

        // transRequest.businessProcess
        ObjectNode businessProcess = transRequest.putObject("businessProcess");
        businessProcess.put("taskId", taskId);

        // systemInfo
        ObjectNode systemInfo = root.putObject("systemInfo");
        systemInfo.put("processId", processId);
        systemInfo.put("workitemId", Long.parseLong(workitemId));
        systemInfo.put("name", nodeName);
        systemInfo.put("user", userNo);
        systemInfo.put("state", systemState);
        systemInfo.put("priority", 0.0);
        systemInfo.put("priId", 0);
        systemInfo.put("groupId", 0);
        systemInfo.put("systemNo", systemNo);

        kafkaTemplate.send(topic, key, root.toString());
    }

    private String formatTimestamp() {
        return LocalDateTime.now().format(TIME_FMT);
    }

    private String formatTimestamp(long timeMs) {
        return LocalDateTime.ofInstant(
                java.time.Instant.ofEpochMilli(timeMs),
                java.time.ZoneId.systemDefault()).format(TIME_FMT);
    }

    private String randomDigits(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(ThreadLocalRandom.current().nextInt(10));
        }
        return sb.toString();
    }

    private String generateRandomIpv6() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 8; i++) {
            if (i > 0)
                sb.append(":");
            sb.append(String.format("%x", ThreadLocalRandom.current().nextInt(0, 65536)));
        }
        return sb.toString();
    }

    private static String pick(String... values) {
        if (values == null || values.length == 0) {
            return "unknown";
        }
        return values[ThreadLocalRandom.current().nextInt(values.length)];
    }

    /**
     * 带权重的随机选择
     * 
     * @param values  可选值数组
     * @param weights 对应权重数组
     */
    private static String pickWeighted(String[] values, int[] weights) {
        int totalWeight = 0;
        for (int w : weights) {
            totalWeight += w;
        }
        int random = ThreadLocalRandom.current().nextInt(totalWeight);
        int cumulative = 0;
        for (int i = 0; i < values.length; i++) {
            cumulative += weights[i];
            if (random < cumulative) {
                return values[i];
            }
        }
        return values[values.length - 1];
    }
}
