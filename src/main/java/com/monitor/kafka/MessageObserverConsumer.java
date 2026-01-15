package com.monitor.kafka;

import com.monitor.entity.StoredMessage;
import com.monitor.repository.StoredMessageRepository;
import com.monitor.service.MessageParser;
import com.monitor.service.MetricsService;
import com.monitor.service.ParsedMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

/**
 * 观察者消费者：监听一个或多个 Topic，把消息解析后写入监控指标与本地存储。
 * 默认监听 monitor.kafka.observer.topics（逗号分隔），常见为主 Topic + processed Topic。
 */
@Component
public class MessageObserverConsumer {
    private final MessageParser parser;
    private final MetricsService metrics;
    private final StoredMessageRepository repository;
    private final long timeoutSeconds;

    public MessageObserverConsumer(MessageParser parser,
                                  MetricsService metrics,
                                  StoredMessageRepository repository,
                                  @Value("${monitor.latency.timeout-seconds:5}") long timeoutSeconds) {
        this.parser = parser;
        this.metrics = metrics;
        this.repository = repository;
        this.timeoutSeconds = timeoutSeconds;
    }

    @KafkaListener(
            topics = "#{'${monitor.kafka.observer.topics}'.split(',')}",
            groupId = "${monitor.kafka.observer.group-id}"
    )
    public void observe(ConsumerRecord<String, String> record) {
        // rawJson 可能为 null；解析器会做兜底并返回 PARSE_ERROR
        String rawJson = record.value();
        ParsedMessage parsed = parser.parse(rawJson);

        // produced/consumed 指标分别表示：观察到消息、观察到消费结果（基于解析字段）
        metrics.incProduced(parsed.getTenant(), parsed.getSystemNo(), parsed.getAdviseKey());
        metrics.incConsumed(parsed.getTenant(), parsed.getSystemNo(), parsed.getResult(), parsed.getNodeName());

        long observedMs = record.timestamp();
        Long produceMs = parsed.getProduceTimeMs();
        if (produceMs != null) {
            Duration latency = Duration.ofMillis(Math.max(0, observedMs - produceMs));
            metrics.recordLatency(parsed.getTenant(), parsed.getSystemNo(), parsed.getNodeName(), latency);
            if (latency.getSeconds() >= timeoutSeconds) {
                metrics.incTimeout(parsed.getTenant(), parsed.getSystemNo(), parsed.getNodeName(), timeoutSeconds + "s");
            }
        }

        // 落库用于前端搜索/链路追踪/统计
        StoredMessage entity = new StoredMessage();
        entity.setTaskId(parsed.getTaskId());
        entity.setTenant(parsed.getTenant());
        entity.setSystemNo(parsed.getSystemNo());
        entity.setAdviseKey(parsed.getAdviseKey());
        entity.setNodeName(parsed.getNodeName());
        entity.setBusId(parsed.getBusId());
        entity.setBusVer(parsed.getBusVer());
        entity.setResult(parsed.getResult());
        entity.setProduceTimeMs(produceMs);
        entity.setObservedTimeMs(observedMs);
        entity.setCreatedAt(Instant.now());
        entity.setRawJson(rawJson == null ? "" : rawJson);
        repository.save(entity);
    }
}
