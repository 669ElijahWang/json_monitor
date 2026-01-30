package com.monitor.controller;

import com.monitor.service.BaseHealthService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/base")
public class BaseHealthController {
    private final BaseHealthService service;

    public BaseHealthController(BaseHealthService service) {
        this.service = service;
    }

    @GetMapping("/health")
    public Map<String, Object> health() {
        return service.health();
    }
}

