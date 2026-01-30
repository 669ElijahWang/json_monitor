package com.monitor.kafka;

import com.monitor.service.MessageParser;
import com.monitor.service.MetricsService;
import com.monitor.service.ParsedMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Duration;

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
    private final double timeoutSeconds;

    public MessageObserverConsumer(MessageParser parser,
            MetricsService metrics,
            @Value("${monitor.latency.timeout-seconds:0.8}") double timeoutSeconds) {
        this.parser = parser;
        this.metrics = metrics;
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
        ParsedMessage parsed = parser.parse(key, rawJson);

        String messageType = parsed.getMessageType();

        // 只统计 STATE 和 AGENT 类型消息，忽略 UNKNOWN 类型
        if ("UNKNOWN".equals(messageType)) {
            // UNKNOWN 类型消息不统计（不是我们监控的格式）
            return;
        }

        // 按消息类型和状态统计
        metrics.incByMessageType(messageType, parsed.getTenant(), parsed.getResult());
        metrics.incProduced(parsed.getTenant(), parsed.getSystemNo(), parsed.getAdviseKey());
        metrics.incConsumed(parsed.getTenant(), parsed.getSystemNo(), parsed.getResult(), parsed.getNodeName());

        // STATE类型消息按watchState统计（Kafka消息接收状态）
        if ("STATE".equals(messageType) && parsed.getWatchState() != null) {
            metrics.incByWatchState(parsed.getTenant(), parsed.getWatchState());
        }

        // AGENT类型消息按systemState和workitemState统计（业务流程状态）
        if ("AGENT".equals(messageType)) {
            if (parsed.getSystemState() != null) {
                metrics.incBySystemState(parsed.getTenant(), parsed.getSystemNo(), parsed.getSystemState());
            }
            if (parsed.getWorkitemState() != null) {
                metrics.incByWorkitemState(parsed.getTenant(), parsed.getSystemNo(), parsed.getWorkitemState());
            }
        }

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
        Duration lag = Duration.ofMillis(Math.max(0, nowMs - recordTs));
        metrics.recordLagLatency(parsed.getTenant(), parsed.getSystemNo(), parsed.getNodeName(), topic, lag);

        Long produceMs = parsed.getProduceTimeMs();
        Long processedMs = parsed.getProcessedTimeMs();
        if (produceMs != null && processedMs != null) {
            Duration e2e = Duration.ofMillis(Math.max(0, processedMs - produceMs));
            metrics.recordE2eLatency(parsed.getTenant(), parsed.getSystemNo(), parsed.getNodeName(), topic, e2e);
        }

        // 对于AGENT消息，使用startTime计算E2E延迟
        Long startTimeMs = parsed.getStartTimeMs();
        if (startTimeMs != null && produceMs == null) {
            Duration e2e = Duration.ofMillis(Math.max(0, nowMs - startTimeMs));
            metrics.recordE2eLatency(parsed.getTenant(), parsed.getSystemNo(), parsed.getNodeName(), topic, e2e);
        }

        Double internalSeconds = parsed.getInternalSeconds();
        if (internalSeconds != null) {
            Duration internal = Duration.ofMillis(Math.max(0L, Math.round(internalSeconds * 1000.0)));
            metrics.recordInternalLatency(parsed.getTenant(), parsed.getSystemNo(), parsed.getNodeName(), topic,
                    internal);
        }

        // 对于AGENT消息，使用checkOutTime-startTime或checkInTime-startTime计算处理延迟
        if (internalSeconds == null && startTimeMs != null) {
            Long checkOutTimeMs = parsed.getCheckOutTimeMs();
            Long checkInTimeMs = parsed.getCheckInTimeMs();
            Long endTimeMs = checkInTimeMs != null ? checkInTimeMs : (checkOutTimeMs != null ? checkOutTimeMs : null);
            if (endTimeMs != null) {
                Duration internal = Duration.ofMillis(Math.max(0, endTimeMs - startTimeMs));
                metrics.recordInternalLatency(parsed.getTenant(), parsed.getSystemNo(), parsed.getNodeName(), topic,
                        internal);
            }
        }

        Duration timeoutBasis = null;
        if (internalSeconds != null) {
            timeoutBasis = Duration.ofMillis(Math.max(0L, Math.round(internalSeconds * 1000.0)));
        } else if (produceMs != null && processedMs != null) {
            timeoutBasis = Duration.ofMillis(Math.max(0, processedMs - produceMs));
        } else if (startTimeMs != null) {
            // 使用startTime到当前时间作为超时检测基准
            timeoutBasis = Duration.ofMillis(Math.max(0, nowMs - startTimeMs));
        }
        if (timeoutBasis != null && timeoutBasis.toMillis() > Math.round(timeoutSeconds * 1000.0)) {
            metrics.incTimeout(parsed.getTenant(), parsed.getSystemNo(), parsed.getNodeName(),
                    thresholdLabel(timeoutSeconds));
        }

        metrics.observeTaskEvent(parsed, nowMs);
    }

    private static String thresholdLabel(double seconds) {
        double v = Math.max(0, seconds);
        long ms = Math.round(v * 1000.0);
        if (ms % 1000 == 0) {
            return (ms / 1000) + "s";
        }
        return (ms / 1000.0) + "s";
    }
}
