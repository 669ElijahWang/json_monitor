package com.monitor.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Kafka 堆积（Backlog/Lag）查询服务：
 * - 使用 AdminClient 获取消费组已提交 offset
 * - 使用 KafkaConsumer 获取 topic 分区的 endOffsets/beginningOffsets
 * - 计算 lag = endOffset - committedOffset，并抽样读取部分消息用于页面展示
 *
 * 定时指标：
 * - 通过 @Scheduled 周期性刷新“最老未消费消息年龄”到 Micrometer，便于 Prometheus/Grafana 展示
 */
@Service
public class KafkaBacklogService {
    private final String bootstrapServers;
    private final String defaultTopic;
    private final String defaultGroupId;
    private final MetricsService metrics;
    private final boolean backlogMetricsEnabled;

    public KafkaBacklogService(@Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers,
                               @Value("${monitor.kafka.main.topic:monitor-topic}") String defaultTopic,
                               @Value("${monitor.kafka.main.group-id:monitor-main-group}") String defaultGroupId,
                               MetricsService metrics,
                               @Value("${monitor.backlog.metrics-enabled:true}") boolean backlogMetricsEnabled) {
        this.bootstrapServers = bootstrapServers;
        this.defaultTopic = defaultTopic;
        this.defaultGroupId = defaultGroupId;
        this.metrics = metrics;
        this.backlogMetricsEnabled = backlogMetricsEnabled;
    }

    public Map<String, Object> backlog(String topic, String groupId, int limit) {
        String safeTopic = (topic == null || topic.isBlank()) ? defaultTopic : topic.trim();
        String safeGroupId = (groupId == null || groupId.isBlank()) ? defaultGroupId : groupId.trim();
        int safeLimit = clamp(limit, 1, 200);

        long nowMs = System.currentTimeMillis();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("topic", safeTopic);
        out.put("groupId", safeGroupId);
        out.put("limit", safeLimit);
        out.put("serverTimeMs", nowMs);

        try (AdminClient admin = AdminClient.create(adminProps());
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps())) {

            List<PartitionInfo> partitionInfos = consumer.partitionsFor(safeTopic, Duration.ofSeconds(3));
            if (partitionInfos == null || partitionInfos.isEmpty()) {
                out.put("partitions", Collections.emptyList());
                out.put("totalLag", 0L);
                out.put("records", Collections.emptyList());
                return out;
            }

            List<TopicPartition> partitions = new ArrayList<>(partitionInfos.size());
            for (PartitionInfo pi : partitionInfos) {
                partitions.add(new TopicPartition(safeTopic, pi.partition()));
            }
            partitions.sort(Comparator.comparingInt(TopicPartition::partition));

            Map<TopicPartition, OffsetAndMetadata> committedOffsets = admin
                    .listConsumerGroupOffsets(safeGroupId)
                    .partitionsToOffsetAndMetadata()
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get(5, TimeUnit.SECONDS);

            consumer.assign(partitions);
            consumer.poll(Duration.ofMillis(0));
            Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
            try {
                beginningOffsets = consumer.beginningOffsets(partitions, Duration.ofSeconds(5));
            } catch (Exception ignored) {
            }
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions, Duration.ofSeconds(5));

            List<Map<String, Object>> partitionRows = new ArrayList<>(partitions.size());
            long totalLag = 0;
            Map<Integer, Long> effectiveStartByPartition = new HashMap<>();
            for (TopicPartition tp : partitions) {
                long end = endOffsets.getOrDefault(tp, 0L);
                OffsetAndMetadata om = committedOffsets.get(tp);
                long committed = om == null ? 0L : Math.max(0L, om.offset());
                long lag = Math.max(0L, end - committed);
                totalLag += lag;

                long beginning = beginningOffsets.getOrDefault(tp, 0L);
                long effectiveStart = Math.max(beginning, committed);
                if (effectiveStart < end) {
                    effectiveStartByPartition.put(tp.partition(), effectiveStart);
                }

                Map<String, Object> row = new LinkedHashMap<>();
                row.put("partition", tp.partition());
                row.put("committedOffset", committed);
                row.put("beginningOffset", beginning);
                row.put("endOffset", end);
                row.put("lag", lag);
                partitionRows.add(row);
            }

            for (TopicPartition tp : partitions) {
                OffsetAndMetadata om = committedOffsets.get(tp);
                long committed = om == null ? 0L : Math.max(0L, om.offset());
                long beginning = beginningOffsets.getOrDefault(tp, 0L);
                consumer.seek(tp, Math.max(beginning, committed));
            }

            List<Map<String, Object>> records = new ArrayList<>(safeLimit);
            Map<Integer, Long> oldestTsByPartition = new HashMap<>();
            Map<Integer, Long> oldestOffsetByPartition = new HashMap<>();
            int polls = 0;
            int expectedOldestCount = effectiveStartByPartition.size();
            while ((records.size() < safeLimit || oldestTsByPartition.size() < expectedOldestCount) && polls < 10) {
                polls++;
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(300))) {
                    Map<String, Object> row = new HashMap<>();
                    row.put("topic", r.topic());
                    row.put("partition", r.partition());
                    row.put("offset", r.offset());
                    row.put("timestamp", r.timestamp());
                    row.put("key", r.key());
                    row.put("value", truncate(r.value(), 2000));
                    if (records.size() < safeLimit) {
                        records.add(row);
                    }

                    long desired = effectiveStartByPartition.getOrDefault(r.partition(), Long.MIN_VALUE);
                    if (desired != Long.MIN_VALUE && !oldestTsByPartition.containsKey(r.partition()) && r.offset() >= desired) {
                        oldestTsByPartition.put(r.partition(), r.timestamp());
                        oldestOffsetByPartition.put(r.partition(), r.offset());
                    }

                    if (records.size() >= safeLimit && oldestTsByPartition.size() >= expectedOldestCount) {
                        break;
                    }
                }
            }

            records.sort(Comparator
                    .comparing((Map<String, Object> r) -> (Integer) r.getOrDefault("partition", 0))
                    .thenComparing(r -> (Long) r.getOrDefault("offset", 0L)));

            for (Map<String, Object> pr : partitionRows) {
                int p = (Integer) pr.getOrDefault("partition", 0);
                Long ts = oldestTsByPartition.get(p);
                if (ts != null && ts > 0) {
                    pr.put("oldestOffset", oldestOffsetByPartition.get(p));
                    pr.put("oldestTimestamp", ts);
                    pr.put("oldestAgeMs", Math.max(0, nowMs - ts));
                } else {
                    pr.put("oldestOffset", null);
                    pr.put("oldestTimestamp", null);
                    pr.put("oldestAgeMs", null);
                }
            }

            out.put("partitions", partitionRows);
            out.put("totalLag", totalLag);
            out.put("records", records);
            return out;
        } catch (Exception e) {
            out.put("error", e.getClass().getSimpleName() + ": " + safeMsg(e.getMessage()));
            out.put("partitions", Collections.emptyList());
            out.put("totalLag", 0L);
            out.put("records", Collections.emptyList());
            return out;
        }
    }

    /**
     * 定时刷新 backlog 衍生指标（默认 15s 一次）：
     * - 从默认 topic/group 快照中提取每个分区“最老消息年龄”
     * - 写入 Micrometer（Gauge），让 Prometheus 抓取并用于告警/看板
     */
    @Scheduled(fixedDelayString = "${monitor.backlog.metrics-rate-ms:15000}")
    public void updateOldestAgeMetrics() {
        if (!backlogMetricsEnabled) {
            return;
        }
        try {
            Map<String, Object> snap = backlog(defaultTopic, defaultGroupId, 1);
            Object partsObj = snap.get("partitions");
            if (!(partsObj instanceof List)) {
                return;
            }
            List parts = (List) partsObj;
            for (Object o : parts) {
                if (!(o instanceof Map)) {
                    continue;
                }
                Map m = (Map) o;
                Object pObj = m.get("partition");
                Object ageMsObj = m.get("oldestAgeMs");
                int p = pObj == null ? 0 : Integer.parseInt(String.valueOf(pObj));
                long ageSeconds = 0;
                if (ageMsObj != null) {
                    long ageMs = Long.parseLong(String.valueOf(ageMsObj));
                    ageSeconds = Math.max(0, ageMs / 1000);
                }
                metrics.setPartitionOldestMessageAgeSeconds(defaultTopic, defaultGroupId, p, ageSeconds);
            }
        } catch (Exception ignored) {
        }
    }

    public Map<String, Object> backlogRecords(String topic,
                                              String groupId,
                                              int partition,
                                              Long startOffset,
                                              int limit,
                                              int maxValueLen) {
        String safeTopic = (topic == null || topic.isBlank()) ? defaultTopic : topic.trim();
        String safeGroupId = (groupId == null || groupId.isBlank()) ? defaultGroupId : groupId.trim();
        int safeLimit = clamp(limit, 1, 1000);
        int safeMaxValueLen = clamp(maxValueLen, 0, 20000);

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("topic", safeTopic);
        out.put("groupId", safeGroupId);
        out.put("partition", partition);
        out.put("limit", safeLimit);
        out.put("maxValueLen", safeMaxValueLen);
        out.put("serverTimeMs", System.currentTimeMillis());

        TopicPartition tp = new TopicPartition(safeTopic, partition);

        try (AdminClient admin = AdminClient.create(adminProps());
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps())) {

            List<PartitionInfo> partitionInfos = consumer.partitionsFor(safeTopic, Duration.ofSeconds(3));
            boolean partitionExists = false;
            if (partitionInfos != null) {
                for (PartitionInfo pi : partitionInfos) {
                    if (pi.partition() == partition) {
                        partitionExists = true;
                        break;
                    }
                }
            }
            if (!partitionExists) {
                out.put("error", "PartitionNotFound: " + safeTopic + "-" + partition);
                out.put("records", Collections.emptyList());
                out.put("hasMore", false);
                return out;
            }

            Map<TopicPartition, OffsetAndMetadata> committedOffsets = admin
                    .listConsumerGroupOffsets(safeGroupId)
                    .partitionsToOffsetAndMetadata()
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get(5, TimeUnit.SECONDS);

            consumer.assign(Collections.singletonList(tp));
            consumer.poll(Duration.ofMillis(0));

            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(Collections.singletonList(tp), Duration.ofSeconds(5));
            long endOffset = endOffsets.getOrDefault(tp, 0L);
            OffsetAndMetadata om = committedOffsets.get(tp);
            long committed = om == null ? 0L : Math.max(0L, om.offset());

            long beginningOffset = 0L;
            try {
                Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(Collections.singletonList(tp));
                beginningOffset = beginningOffsets.getOrDefault(tp, 0L);
            } catch (Exception ignored) {
            }

            long desiredStart = startOffset == null ? committed : Math.max(0L, startOffset);
            long effectiveStart = Math.max(beginningOffset, Math.max(committed, desiredStart));

            out.put("committedOffset", committed);
            out.put("beginningOffset", beginningOffset);
            out.put("endOffset", endOffset);
            out.put("startOffset", effectiveStart);

            if (effectiveStart >= endOffset) {
                out.put("records", Collections.emptyList());
                out.put("nextOffset", effectiveStart);
                out.put("hasMore", false);
                return out;
            }

            consumer.seek(tp, effectiveStart);

            List<Map<String, Object>> records = new ArrayList<>(Math.min(safeLimit, 200));
            long nextOffset = effectiveStart;
            int polls = 0;
            while (records.size() < safeLimit && polls < 20) {
                polls++;
                int addedThisPoll = 0;
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(300))) {
                    if (r.offset() >= endOffset) {
                        break;
                    }
                    Map<String, Object> row = new HashMap<>();
                    row.put("topic", r.topic());
                    row.put("partition", r.partition());
                    row.put("offset", r.offset());
                    row.put("timestamp", r.timestamp());
                    row.put("key", r.key());
                    row.put("value", safeMaxValueLen == 0 ? null : truncate(r.value(), safeMaxValueLen));
                    records.add(row);
                    nextOffset = r.offset() + 1;
                    addedThisPoll++;
                    if (records.size() >= safeLimit) {
                        break;
                    }
                }
                if (addedThisPoll == 0) {
                    break;
                }
            }

            out.put("records", records);
            out.put("nextOffset", nextOffset);
            out.put("hasMore", nextOffset < endOffset);
            return out;
        } catch (Exception e) {
            out.put("error", e.getClass().getSimpleName() + ": " + safeMsg(e.getMessage()));
            out.put("records", Collections.emptyList());
            out.put("hasMore", false);
            return out;
        }
    }

    private Properties adminProps() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "monitor-admin-" + UUID.randomUUID());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        return props;
    }

    private Properties consumerProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", "monitor-backlog-viewer-" + UUID.randomUUID());
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("max.poll.records", "500");
        props.put("request.timeout.ms", "5000");
        props.put("default.api.timeout.ms", "5000");
        return props;
    }

    private static int clamp(int v, int min, int max) {
        if (v < min) return min;
        if (v > max) return max;
        return v;
    }

    private static String truncate(String s, int maxLen) {
        if (s == null) {
            return null;
        }
        if (s.length() <= maxLen) {
            return s;
        }
        return s.substring(0, maxLen);
    }

    private static String safeMsg(String s) {
        if (s == null) {
            return "";
        }
        String v = s.trim();
        return v.length() > 500 ? v.substring(0, 500) : v;
    }
}
