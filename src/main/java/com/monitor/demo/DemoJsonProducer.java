package com.monitor.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Component
@ConditionalOnProperty(prefix = "monitor.demo.producer", name = "enabled", havingValue = "true")
public class DemoJsonProducer {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;

    public DemoJsonProducer(KafkaTemplate<String, String> kafkaTemplate,
                            ObjectMapper objectMapper,
                            @Value("${monitor.demo.producer.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    @Scheduled(fixedDelayString = "${monitor.demo.producer.rate-ms:1000}")
    public void produce() {
        String taskId = "task-" + UUID.randomUUID();
        long produceTime = Instant.now().toEpochMilli();

        ObjectNode root = objectMapper.createObjectNode();
        root.put("priTenant", pick("BIOM", "DEMO", "TENANT_A"));
        root.put("taskId", taskId);
        root.put("adviseKey", pick("ORDER_CREATE", "ORDER_PAY", "SHIP_NOTIFY"));
        root.put("produceTime", produceTime);

        ObjectNode transRequest = root.putObject("transRequest");
        transRequest.put("systemNo", pick("SYS-A", "SYS-B"));
        ObjectNode operDetail = transRequest.putObject("operDetail");
        operDetail.put("nodeName", pick("deptA", "deptB"));
        ObjectNode businessProcess = transRequest.putObject("businessProcess");
        businessProcess.put("busId", "BUS-" + ThreadLocalRandom.current().nextInt(1, 6));
        businessProcess.put("busVer", pick("v1", "v2"));

        root.put("result", "NEW");

        kafkaTemplate.send(topic, taskId, root.toString());
    }

    private static String pick(String... values) {
        if (values == null || values.length == 0) {
            return "unknown";
        }
        return values[ThreadLocalRandom.current().nextInt(values.length)];
    }
}

