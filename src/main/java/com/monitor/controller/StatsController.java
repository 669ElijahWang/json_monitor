package com.monitor.controller;

import com.monitor.service.StatsService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 统计接口：给前端仪表盘提供聚合数据。
 * minutes 参数用于定义统计窗口（默认 60 分钟）。
 */
@RestController
@RequestMapping("/api/stats")
public class StatsController {
    private final StatsService stats;

    public StatsController(StatsService stats) {
        this.stats = stats;
    }

    @GetMapping("/overview")
    public Map<String, Object> overview(@RequestParam(defaultValue = "60") int minutes) {
        return stats.overview(minutes);
    }

    @GetMapping("/timeseries")
    public List<Map<String, Object>> timeseries(@RequestParam(defaultValue = "60") int minutes) {
        return stats.timeseries(minutes);
    }

    @GetMapping("/breakdown")
    public List<Map<String, Object>> breakdown(@RequestParam(defaultValue = "60") int minutes,
            @RequestParam(defaultValue = "tenant") String by) {
        return stats.breakdown(minutes, by);
    }

    @GetMapping("/latency")
    public Map<String, Object> latency(@RequestParam(defaultValue = "60") int minutes) {
        return stats.latency(minutes);
    }

    @GetMapping("/message-size")
    public Map<String, Object> messageSize(@RequestParam(defaultValue = "60") int minutes) {
        return stats.messageSizeDistribution(minutes);
    }

    @GetMapping("/big-messages")
    public Map<String, Object> bigMessages(@RequestParam(defaultValue = "50") int limit) {
        return stats.bigMessages(limit);
    }

    @GetMapping("/state-stats")
    public Map<String, Object> stateStats(@RequestParam(defaultValue = "60") int minutes) {
        return stats.stateStats(minutes);
    }
}
