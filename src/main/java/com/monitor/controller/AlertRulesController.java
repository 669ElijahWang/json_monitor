package com.monitor.controller;

import com.monitor.service.AlertRulesService;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * Prometheus 告警规则管理接口：
 * - GET 返回当前 rules 文件内容（纯文本）
 * - POST 写入 rules 文件，并尝试触发 Prometheus 热加载（/-/reload）
 */
@RestController
@RequestMapping("/api/alerts")
public class AlertRulesController {
    private final AlertRulesService service;

    public AlertRulesController(AlertRulesService service) {
        this.service = service;
    }

    @GetMapping(value = "/rules", produces = MediaType.TEXT_PLAIN_VALUE)
    public String getRules() {
        return service.readRules();
    }

    @PostMapping(value = "/rules", consumes = MediaType.TEXT_PLAIN_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public Map<String, Object> setRules(@RequestBody String newRules) {
        service.writeRules(newRules);
        boolean reloaded = service.reloadPrometheus();
        Map<String, Object> result = new HashMap<>();
        result.put("saved", true);
        result.put("reloaded", reloaded);
        return result;
    }
}
