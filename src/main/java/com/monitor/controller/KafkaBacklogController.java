package com.monitor.controller;

import com.monitor.service.KafkaBacklogService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/kafka")
public class KafkaBacklogController {
    private final KafkaBacklogService service;

    public KafkaBacklogController(KafkaBacklogService service) {
        this.service = service;
    }

    @GetMapping("/backlog")
    public Map<String, Object> backlog(@RequestParam(required = false) String topic,
                                       @RequestParam(required = false) String groupId,
                                       @RequestParam(defaultValue = "50") int limit) {
        return service.backlog(topic, groupId, limit);
    }

    @GetMapping("/backlog/records")
    public Map<String, Object> backlogRecords(@RequestParam(required = false) String topic,
                                              @RequestParam(required = false) String groupId,
                                              @RequestParam int partition,
                                              @RequestParam(required = false) Long startOffset,
                                              @RequestParam(defaultValue = "200") int limit,
                                              @RequestParam(defaultValue = "2000") int maxValueLen) {
        return service.backlogRecords(topic, groupId, partition, startOffset, limit, maxValueLen);
    }
}
