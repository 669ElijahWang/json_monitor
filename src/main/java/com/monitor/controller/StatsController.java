package com.monitor.controller;

import com.monitor.service.StatsService;
import com.monitor.service.RealtimeStatsService;
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
    private final RealtimeStatsService realtimeStats;

    public StatsController(StatsService stats, RealtimeStatsService realtimeStats) {
        this.stats = stats;
        this.realtimeStats = realtimeStats;
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

    // ===================== 实时统计接口 =====================

    /**
     * 实时概览统计（内存实时数据，无 Prometheus 延迟）
     * 返回指定分钟窗口内的 State、Competence、Tenant 消息总数
     */
    @GetMapping("/realtime/overview")
    public Map<String, Object> realtimeOverview(@RequestParam(defaultValue = "60") int minutes) {
        return realtimeStats.getOverview(minutes);
    }

    /**
     * 实时吞吐趋势（内存实时数据，按分钟统计）
     * 返回每分钟的 State、Competence、Tenant 消息数量
     */
    @GetMapping("/realtime/timeseries")
    public List<Map<String, Object>> realtimeTimeseries(@RequestParam(defaultValue = "60") int minutes) {
        return realtimeStats.getTimeseries(minutes);
    }

    /**
     * 实时 watchState 分布统计（累积数据，只增不减）
     */
    @GetMapping("/realtime/watch-states")
    public List<Map<String, Object>> realtimeWatchStates() {
        return realtimeStats.getWatchStateStats();
    }

    /**
     * 实时 messageType 分布统计（累积数据，只增不减）
     */
    @GetMapping("/realtime/message-types")
    public List<Map<String, Object>> realtimeMessageTypes() {
        return realtimeStats.getMessageTypeStats();
    }

    /**
     * 实时租户(Tenant)统计（取代 Prometheus 的 breakdown）
     */
    @GetMapping("/realtime/tenants")
    public List<Map<String, Object>> realtimeTenants() {
        return realtimeStats.getTenantStats();
    }

    /**
     * 实时展示延迟百分位数（来自内存最近记录）
     */
    @GetMapping("/realtime/latency")
    public Map<String, Object> realtimeLatency(@RequestParam(defaultValue = "60") int minutes) {
        return realtimeStats.getLatencyStats(minutes);
    }
}
