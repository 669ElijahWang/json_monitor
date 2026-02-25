package com.monitor.service;

import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class BaseHealthService {
    private final PrometheusQueryService prometheus;

    public BaseHealthService(PrometheusQueryService prometheus) {
        this.prometheus = prometheus;
    }

    public Map<String, Object> health() {
        Double produceLatencyMs = prometheus.queryScalar(
                "max(avg_over_time(kafka_network_requestmetrics_totallatency_ms{request=\"Produce\"}[5m]))");
        Double diskIoBusyRatio = prometheus.queryScalar("max(rate(node_disk_io_time_seconds_total[5m]))");
        Double networkProcessorIdle = prometheus
                .queryScalar("min(avg_over_time(kafka_network_processoridletime_avg[5m]))");
        Double underReplicated = prometheus.queryScalar("max(kafka_server_replicamanager_underreplicatedpartitions)");
        Double controllerSwitches1h = prometheus
                .queryScalar("max(changes(kafka_controller_kafkacontroller_activecontrollercount[1h]))");
        Double isrShrinks1h = prometheus
                .queryScalar("sum(increase(kafka_server_replicamanager_isrshrinks_total[1h]))");

        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("produceLatencyMsAvg5m", produceLatencyMs);
        metrics.put("diskIoBusyRatio", diskIoBusyRatio);
        metrics.put("networkProcessorIdleRatio", networkProcessorIdle);
        metrics.put("underReplicatedPartitions", underReplicated);
        metrics.put("controllerSwitches1h", controllerSwitches1h);
        metrics.put("isrShrinks1h", isrShrinks1h);

        List<String> missing = new ArrayList<>();
        if (produceLatencyMs == null)
            missing.add("Produce 延迟");
        if (diskIoBusyRatio == null)
            missing.add("磁盘 I/O");
        if (networkProcessorIdle == null)
            missing.add("网络线程空闲");
        if (underReplicated == null)
            missing.add("副本状态(URP)");
        if (controllerSwitches1h == null)
            missing.add("控制器切换");
        if (isrShrinks1h == null)
            missing.add("ISR 变动");

        List<String> suggestions = new ArrayList<>();
        int level = 0;

        if (underReplicated != null && underReplicated > 0) {
            suggestions.add("Under-Replicated Partitions > 0：存在副本不同步，优先检查 Broker/网络/磁盘负载");
            level = Math.max(level, 2);
        }

        if (produceLatencyMs != null) {
            if (produceLatencyMs >= 500) {
                suggestions.add("Produce 请求延迟 >= 500ms：SDK 写入明显变慢，建议检查磁盘/网络与大报文");
                level = Math.max(level, 2);
            } else if (produceLatencyMs >= 200) {
                suggestions.add("Produce 请求延迟 >= 200ms：写入变慢，建议关注批量参数与 Broker 资源");
                level = Math.max(level, 1);
            }
        }

        if (diskIoBusyRatio != null) {
            if (diskIoBusyRatio >= 0.80) {
                suggestions.add("磁盘 I/O 等待 >= 80%：磁盘繁忙可能引起积压与延迟升高");
                level = Math.max(level, 2);
            } else if (diskIoBusyRatio >= 0.60) {
                suggestions.add("磁盘 I/O 等待 >= 60%：磁盘压力偏高，建议关注大报文与磁盘带宽");
                level = Math.max(level, 1);
            }
        }

        if (networkProcessorIdle != null) {
            if (networkProcessorIdle <= 0.15) {
                suggestions.add("Network Processor 空闲率 <= 15%：网络线程繁忙，建议扩容 Broker 或优化 Batch/压缩");
                level = Math.max(level, 2);
            } else if (networkProcessorIdle <= 0.30) {
                suggestions.add("Network Processor 空闲率 <= 30%：Broker 网络处理趋紧，建议关注连接数与批量发送");
                level = Math.max(level, 1);
            }
        }

        if (controllerSwitches1h != null) {
            if (controllerSwitches1h >= 3) {
                suggestions.add("控制器切换频繁(>=3/1h)：集群控制面不稳定，检查 ZooKeeper/KRaft 节点压力与网络");
                level = Math.max(level, 2);
            } else if (controllerSwitches1h > 0) {
                suggestions.add("存在控制器切换：建议检查控制面节点负载与网络抖动");
                level = Math.max(level, 1);
            }
        }

        if (isrShrinks1h != null) {
            if (isrShrinks1h >= 10) {
                suggestions.add("ISR 缩小次数偏高(>=10/1h)：可能存在网络闪断或 Broker 压力导致副本跟不上");
                level = Math.max(level, 2);
            } else if (isrShrinks1h > 0) {
                suggestions.add("存在 ISR 缩小：建议排查网络质量与 Broker 资源水位");
                level = Math.max(level, 1);
            }
        }

        String status;
        String brokerStatus = (controllerSwitches1h != null || underReplicated != null) ? "ONLINE" : "OFFLINE";

        if (level >= 2) {
            status = "CRITICAL";
        } else if (level == 1) {
            status = "WARN";
        } else if (brokerStatus.equals("OFFLINE")) {
            status = "UNKNOWN";
            suggestions.add(
                    "无法获取 Kafka 核心指标，请检查：1. Kafka 是否开启 JMX (9999)；2. kafka-jmx-exporter 是否正常运行；3. Prometheus 是否配置了抓取任务");
        } else {
            status = "OK";
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("sampledAt", Instant.now().toString());
        out.put("status", status);
        out.put("brokerStatus", brokerStatus);
        out.put("metrics", metrics);
        out.put("suggestions", suggestions);
        out.put("missing", missing);
        return out;
    }
}
