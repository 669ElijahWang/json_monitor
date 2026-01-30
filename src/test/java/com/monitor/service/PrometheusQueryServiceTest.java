package com.monitor.service;

import org.junit.jupiter.api.Test;
import org.springframework.boot.web.client.RestTemplateBuilder;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class PrometheusQueryServiceTest {
    @Test
    void queryVector_shouldEncodePromqlAndNotThrow() {
        PrometheusQueryService svc = new PrometheusQueryService(new RestTemplateBuilder(), "http://127.0.0.1:1");
        List<PrometheusQueryService.SeriesPoint> out = assertDoesNotThrow(
                () -> svc.queryVector("count((msg_task_last_seen_seconds{result=\"SUCCESS\"}[60m] > time() - 3600))")
        );
        assertNotNull(out);
    }
}
