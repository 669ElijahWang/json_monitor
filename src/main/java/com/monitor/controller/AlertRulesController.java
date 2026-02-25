package com.monitor.controller;

import com.monitor.service.LatencyAlertConfig;
import com.monitor.service.LatencyAlertService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * 告警配置管理接口：
 * - GET 获取延时告警配置
 * - POST 保存延时告警配置
 */
@RestController
@RequestMapping("/api/alerts")
public class AlertRulesController {
    private final LatencyAlertService latencyAlertService;

    public AlertRulesController(LatencyAlertService latencyAlertService) {
        this.latencyAlertService = latencyAlertService;
    }

    @GetMapping("/latency-config")
    public LatencyAlertConfig getLatencyConfig() {
        return latencyAlertService.getConfig();
    }

    @PostMapping("/latency-config")
    public Map<String, Object> setLatencyConfig(@RequestBody LatencyAlertConfig config) {
        latencyAlertService.saveConfig(config);
        Map<String, Object> result = new HashMap<>();
        result.put("success", true);
        return result;
    }

    @PostMapping("/test-email")
    public Map<String, Object> testEmail() {
        Map<String, Object> result = new HashMap<>();
        try {
            latencyAlertService.sendTestEmail();
            result.put("success", true);
        } catch (Exception e) {
            result.put("success", false);
            result.put("message", e.getMessage());
        }
        return result;
    }
}
