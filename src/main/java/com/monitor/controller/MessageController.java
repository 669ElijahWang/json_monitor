package com.monitor.controller;

import com.monitor.service.PrometheusMessageService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * 消息查询接口：
 * - 列表分页
 * - 按 taskId 查询链路消息
 * - 多条件检索（包含 rawJson 的模糊匹配）
 */
@RestController
@RequestMapping("/api")
public class MessageController {
    private final PrometheusMessageService messages;

    public MessageController(PrometheusMessageService messages) {
        this.messages = messages;
    }

    @GetMapping("/messages")
    public Map<String, Object> list(@RequestParam(defaultValue = "0") int page,
                                    @RequestParam(defaultValue = "20") int size,
                                    @RequestParam(defaultValue = "60") int minutes) {
        return messages.search(null, null, null, null, null, minutes, page, size);
    }

    @GetMapping("/messages/{taskId}")
    public Object byTaskId(@PathVariable String taskId) {
        return messages.trace(taskId);
    }

    @GetMapping("/messages/search")
    public Map<String, Object> search(@RequestParam(required = false) String q,
                                      @RequestParam(required = false) String tenant,
                                      @RequestParam(required = false) String systemNo,
                                      @RequestParam(required = false) String busId,
                                      @RequestParam(required = false) String taskId,
                                      @RequestParam(defaultValue = "60") int minutes,
                                      @RequestParam(defaultValue = "0") int page,
                                      @RequestParam(defaultValue = "20") int size) {
        return messages.search(q, tenant, systemNo, busId, taskId, minutes, page, size);
    }

    @GetMapping("/tasks/pending")
    public Map<String, Object> pendingTasks(@RequestParam(required = false) String q,
                                            @RequestParam(required = false) String tenant,
                                            @RequestParam(required = false) String systemNo,
                                            @RequestParam(required = false) String busId,
                                            @RequestParam(defaultValue = "60") int minutes,
                                            @RequestParam(defaultValue = "30") int expectedSeconds,
                                            @RequestParam(defaultValue = "200") int size) {
        return messages.pendingTasks(q, tenant, systemNo, busId, minutes, expectedSeconds, size);
    }
}
