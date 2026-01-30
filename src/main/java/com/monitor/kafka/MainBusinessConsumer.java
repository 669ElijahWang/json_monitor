package com.monitor.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 主业务消费者（模拟），用于演示从主 Topic 消费并“处理”后写入已处理 Topic。
 * 受配置 monitor.kafka.main.enabled 控制是否启用。
 * 处理逻辑包括：
 * - 随机睡眠以模拟业务耗时
 * - 写入 result、processedTime
 * - 补齐 transRequest.operDetail.nodeName
 * - 通过 KafkaTemplate 发送到 processed-topic
 *
 * 说明：
 * - @Component：注册为 Spring Bean
 * - @ConditionalOnProperty：使用配置开关启停主消费者（便于演示/排障）
 * - @KafkaListener：从主 Topic 消费，并把“处理后的消息”投递到 processed-topic
 */
@Component
@ConditionalOnProperty(prefix = "monitor.kafka.main", name = "enabled", havingValue = "true")
public class MainBusinessConsumer {
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String processedTopic;

    public MainBusinessConsumer(ObjectMapper objectMapper,
                                KafkaTemplate<String, String> kafkaTemplate,
                                @Value("${monitor.kafka.main.processed-topic}") String processedTopic) {
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
        this.processedTopic = processedTopic;
    }

    /**
     * 消费主业务 Topic，做简单模拟处理并投递到 processedTopic。
     */
    @KafkaListener(topics = "${monitor.kafka.main.topic}", groupId = "${monitor.kafka.main.group-id}")
    public void consume(ConsumerRecord<String, String> record) {
        String raw = record.value();
        if (raw == null || raw.isBlank()) {
            // 空消息直接忽略
            return;
        }

        String taskId = record.key();
        try {
            JsonNode root = objectMapper.readTree(raw);
            if (taskId == null || taskId.isBlank()) {
                // 尝试从消息体中补齐 taskId
                taskId = root.path("taskId").asText();
            }
            long internalMs = ThreadLocalRandom.current().nextLong(100, 1001);
            Thread.sleep(internalMs);

            long processedMs = Instant.now().toEpochMilli();

            // 构造输出 JSON，写入处理结果与时间戳
            ObjectNode out = (root.isObject() ? (ObjectNode) root : objectMapper.createObjectNode());
            out.put("result", ThreadLocalRandom.current().nextInt(100) < 90 ? "SUCCESS" : "FAIL");
            out.put("processedTime", processedMs);
            out.put("internalSeconds", internalMs / 1000.0);

            // 补齐链路中的 nodeName（模拟节点变更）
            JsonNode transRequest = out.path("transRequest");
            ObjectNode transRequestObj = transRequest.isObject() ? (ObjectNode) transRequest : out.putObject("transRequest");
            JsonNode operDetail = transRequestObj.path("operDetail");
            ObjectNode operDetailObj = operDetail.isObject() ? (ObjectNode) operDetail : transRequestObj.putObject("operDetail");
            operDetailObj.put("nodeName", "deptC");

            // 发送到已处理 Topic，使用 key=taskId 便于链路追踪
            kafkaTemplate.send(processedTopic, taskId, out.toString());
        } catch (Exception ignored) {
            // 演示场景下忽略异常，避免影响示例运行
        }
    }
}
