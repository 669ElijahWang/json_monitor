package com.monitor.kafka;

import com.monitor.service.MessageParser;
import com.monitor.service.MetricsService;
import com.monitor.service.ParsedMessage;
import com.monitor.service.RealtimeStatsService;
import com.monitor.service.MessageRawStoreService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

/**
 * 观察者消费者：监听一个或多个 Topic，把消息解析后写入监控指标。
 * 默认监听 monitor.kafka.observer.topics（逗号分隔），常见为主 Topic + processed Topic。
 *
 * 说明：
 * - @Component：注册为 Spring Bean
 * - @KafkaListener：声明 Kafka 消费监听；topics 支持 SpEL 表达式，可把逗号分隔配置拆成数组
 */
@Component
public class MessageObserverConsumer {
    private final MessageParser parser;
    private final MetricsService metrics;
    private final RealtimeStatsService realtimeStats;
    private final MessageRawStoreService rawStore;
    private final double timeoutSeconds;

    public MessageObserverConsumer(MessageParser parser,
            MetricsService metrics,
            RealtimeStatsService realtimeStats,
            MessageRawStoreService rawStore,
            @Value("${monitor.latency.timeout-seconds:0.8}") double timeoutSeconds) {
        this.parser = parser;
        this.metrics = metrics;
        this.realtimeStats = realtimeStats;
        this.rawStore = rawStore;
        this.timeoutSeconds = timeoutSeconds;
    }

    /**
     * 旁路观测消费：
     * - groupId 独立于主业务消费组，避免影响主链路 offset
     * - topics 由配置 monitor.kafka.observer.topics 提供，支持监听多个 Topic
     */
    @KafkaListener(topics = "#{'${monitor.kafka.observer.topics}'.split(',')}", groupId = "${monitor.kafka.observer.group-id}")
    public void observe(ConsumerRecord<String, String> record) {
        // rawJson 可能为 null；解析器会做兜底并返回 PARSE_ERROR
        String rawJson = record.value();
        String key = record.key();

        // 打印监控到的json放入根目录下的json文件夹
        if (rawJson != null) {
            saveJsonToFile(record.topic(), key, rawJson);
        }
        ParsedMessage parsed = parser.parse(key, rawJson);

        String messageType = parsed.getMessageType();
        String category = parsed.getCategory();

        // 只统计已知类型消息，忽略 UNKNOWN 类型
        if ("UNKNOWN".equals(messageType)) {
            // UNKNOWN 类型消息不统计（不是我们监控的格式）
            return;
        }

        // 按消息类型、种类和状态统计
        metrics.incByMessageType(messageType, parsed.getTenant(), parsed.getResult());
        metrics.incByCategory(category, parsed.getTenant(), parsed.getResult());
        metrics.incProduced(parsed.getTenant(), parsed.getSystemNo(), parsed.getAdviseKey());
        metrics.incConsumed(parsed.getTenant(), parsed.getSystemNo(), parsed.getResult(), parsed.getNodeName());

        long nowMs = System.currentTimeMillis();
        String topic = record.topic();

        long sizeBytes = 0;
        if (rawJson != null) {
            try {
                sizeBytes = rawJson.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            } catch (Exception ignored) {
                sizeBytes = rawJson.length();
            }
        }
        metrics.recordMessageSize(topic, sizeBytes);
        metrics.observeBigMessage(parsed, topic, record.partition(), record.offset(), record.key(), sizeBytes, nowMs);

        long recordTs = record.timestamp();
        long lagMs = Math.max(0, nowMs - recordTs);
        metrics.recordLagLatency(parsed.getTenant(), parsed.getSystemNo(), parsed.getNodeName(), topic,
                Duration.ofMillis(lagMs));

        Long e2eMs = null;
        Long internalMs = null;

        // 只对 TENANT_MESSAGE 类型计算 E2E 和处理延迟（只有此类型有 startTime/checkOutTime/checkInTime）
        if ("TENANT_MESSAGE".equals(messageType)) {
            Long startTimeMs = parsed.getStartTimeMs();

            // 使用startTime计算E2E延迟
            if (startTimeMs != null) {
                e2eMs = Math.max(0, nowMs - startTimeMs);
                metrics.recordE2eLatency(parsed.getTenant(), parsed.getSystemNo(), parsed.getNodeName(), topic,
                        Duration.ofMillis(e2eMs));
            }

            // 使用checkOutTime-startTime或checkInTime-startTime计算处理延迟
            if (startTimeMs != null) {
                Long checkOutTimeMs = parsed.getCheckOutTimeMs();
                Long checkInTimeMs = parsed.getCheckInTimeMs();
                Long endTimeMs = checkInTimeMs != null ? checkInTimeMs
                        : (checkOutTimeMs != null ? checkOutTimeMs : null);
                if (endTimeMs != null) {
                    internalMs = Math.max(0, endTimeMs - startTimeMs);
                    metrics.recordInternalLatency(parsed.getTenant(), parsed.getSystemNo(), parsed.getNodeName(), topic,
                            Duration.ofMillis(internalMs));
                }
            }

            // 使用startTime到当前时间作为超时检测基准
            if (startTimeMs != null) {
                long timeoutBasisMs = Math.max(0, nowMs - startTimeMs);
                if (timeoutBasisMs > Math.round(timeoutSeconds * 1000.0)) {
                    metrics.incTimeout(parsed.getTenant(), parsed.getSystemNo(), parsed.getNodeName(),
                            thresholdLabel(timeoutSeconds));
                }
            }
        }

        // 以下是传统的 Prometheus 计数器，用于告警和长期存储
        // STATE类型消息按watchState统计（Kafka消息接收状态）
        if ("STATE".equals(messageType) && parsed.getWatchState() != null) {
            metrics.incByWatchState(parsed.getTenant(), parsed.getWatchState());
        }

        // COMPETENCE类型消息按errorType和errorLevel统计（异常消息）
        if ("COMPETENCE".equals(messageType)) {
            if (parsed.getErrorType() != null) {
                metrics.incByErrorType(parsed.getTenant(), parsed.getErrorType());
            }
            if (parsed.getErrorLevel() != null) {
                metrics.incByErrorLevel(parsed.getTenant(), parsed.getErrorLevel());
            }
        }

        // TENANT_MESSAGE类型消息按systemState和workitemState统计（租户业务消息）
        if ("TENANT_MESSAGE".equals(messageType)) {
            if (parsed.getSystemState() != null) {
                metrics.incBySystemState(parsed.getTenant(), parsed.getSystemNo(), parsed.getSystemState());
            }
            if (parsed.getWorkitemState() != null) {
                metrics.incByWorkitemState(parsed.getTenant(), parsed.getSystemNo(), parsed.getWorkitemState());
            }
        }

        metrics.observeTaskEvent(parsed, nowMs);

        // 记录原始消息到缓存（包含 Kafka Key 和原始 JSON）
        String fullContent = rawJson;
        if (rawJson != null) {
            String lookupKey = buildLookupKey(parsed);

            // 采用带标记的格式，方便前端解析展示
            fullContent = "KAFKA_KEY[" + (key != null ? key : "null") + "]KAFKA_BODY[" + rawJson + "]";
            rawStore.put(lookupKey, fullContent);
        }

        // 实时统计与搜索缓存（传入带Key的完整内容，方便前端展示Key）
        realtimeStats.recordMessage(parsed, lagMs, e2eMs, internalMs, fullContent);
    }

    private String buildLookupKey(ParsedMessage parsed) {
        return safe(parsed.getTenant())
                + "|" + safe(parsed.getSystemNo())
                + "|" + safe(parsed.getTaskId())
                + "|" + safe(parsed.getNodeName())
                + "|" + safe(parsed.getResult())
                + "|" + "" // busId is not in ParsedMessage but in labels, we don't use it for raw store
                           // key if not present
                + "|" + safe(parsed.getAdviseKey())
                + "|" + safe(parsed.getTransNo());
    }

    private static String safe(String v) {
        return (v == null || v.isBlank()) ? "unknown" : v;
    }

    private static String thresholdLabel(double seconds) {
        double v = Math.max(0, seconds);
        long ms = Math.round(v * 1000.0);
        if (ms % 1000 == 0) {
            return (ms / 1000) + "s";
        }
        return (ms / 1000.0) + "s";
    }

    // 打印json
    private void saveJsonToFile(String topic, String key, String jsonContent) {
        try {
            // Define the directory path
            String directoryName = "json";
            Path directoryPath = Paths.get(directoryName);

            // Create the directory if it doesn't exist
            if (!Files.exists(directoryPath)) {
                Files.createDirectories(directoryPath);
            }

            // Generate a unique filename
            String safeKey = (key != null) ? key.replaceAll("[^a-zA-Z0-9._-]", "_") : "null_key";
            String timestamp = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSS").format(LocalDateTime.now());
            String uuid = UUID.randomUUID().toString().substring(0, 6);
            String filename = String.format("%s_%s_%s_%s.json", topic, timestamp, safeKey, uuid);

            // Create the file and write the content
            File file = new File(directoryPath.toFile(), filename);
            try (FileWriter writer = new FileWriter(file)) {
                writer.write(jsonContent);
                // System.out.println("Saved JSON to: " + file.getAbsolutePath());
            }
        } catch (IOException e) {
            System.err.println("Failed to save JSON to file: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
