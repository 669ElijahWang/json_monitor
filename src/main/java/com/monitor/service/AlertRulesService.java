package com.monitor.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;

/**
 * 告警规则服务：读写 Prometheus rules 文件，并可触发 Prometheus 热加载。
 * rulesPath 支持相对/绝对路径，内部统一解析为绝对路径。
 */
@Service
public class AlertRulesService {
    private final Path rulesFile;
    private final String reloadUrl;
    private final boolean reloadEnabled;
    private final RestTemplate restTemplate;

    public AlertRulesService(@Value("${monitor.alert.rules-path:prometheus/alert-rules.yml}") String rulesPath,
                             @Value("${monitor.alert.prometheus-reload-url:http://localhost:9090/-/reload}") String reloadUrl,
                             @Value("${monitor.alert.reload-enabled:true}") boolean reloadEnabled,
                             RestTemplateBuilder restTemplateBuilder) {
        this.rulesFile = resolvePath(rulesPath);
        this.reloadUrl = reloadUrl;
        this.reloadEnabled = reloadEnabled;
        this.restTemplate = restTemplateBuilder
                .setConnectTimeout(Duration.ofSeconds(3))
                .setReadTimeout(Duration.ofSeconds(5))
                .build();
    }

    public String readRules() {
        try {
            if (!Files.exists(rulesFile)) {
                return "";
            }
            return Files.readString(rulesFile, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("读取规则文件失败: " + rulesFile, e);
        }
    }

    public void writeRules(String content) {
        try {
            Files.createDirectories(rulesFile.getParent());
            Files.writeString(rulesFile, content == null ? "" : content, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("写入规则文件失败: " + rulesFile, e);
        }
    }

    public boolean reloadPrometheus() {
        if (!reloadEnabled) {
            return false;
        }
        try {
            ResponseEntity<String> resp = restTemplate.exchange(reloadUrl, HttpMethod.POST, HttpEntity.EMPTY, String.class);
            return resp.getStatusCode().is2xxSuccessful();
        } catch (Exception e) {
            return false;
        }
    }

    private static Path resolvePath(String p) {
        Path path = Paths.get(p);
        if (path.isAbsolute()) {
            return path.normalize();
        }
        return Paths.get("").toAbsolutePath().resolve(path).normalize();
    }
}
