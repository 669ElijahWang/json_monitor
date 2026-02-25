package com.monitor.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Service
public class PrometheusQueryService {
    private final RestTemplate restTemplate;
    private final String baseUrl;

    public PrometheusQueryService(RestTemplateBuilder restTemplateBuilder,
            @Value("${monitor.prometheus.base-url:http://localhost:9090}") String baseUrl) {
        this.restTemplate = restTemplateBuilder
                .setConnectTimeout(Duration.ofSeconds(3))
                .setReadTimeout(Duration.ofSeconds(8))
                .build();
        this.baseUrl = baseUrl;
    }

    public Double queryScalar(String promql) {
        List<SeriesPoint> vector = queryVector(promql);
        if (vector.isEmpty()) {
            return null;
        }
        return vector.get(0).value;
    }

    public List<SeriesPoint> queryVector(String promql) {
        Map<String, Object> body;
        try {
            URI uri = URI.create(trimTrailingSlash(baseUrl) + "/api/v1/query?query=" + encode(promql));
            ResponseEntity<Map> resp = restTemplate.getForEntity(uri, Map.class);
            body = resp.getBody();
        } catch (Exception e) {
            return Collections.emptyList();
        }
        if (body == null) {
            return Collections.emptyList();
        }
        Object status = body.get("status");
        if (!Objects.equals("success", status)) {
            return Collections.emptyList();
        }

        Map<String, Object> data = castMap(body.get("data"));
        if (data == null) {
            return Collections.emptyList();
        }
        Object resultObj = data.get("result");
        if (!(resultObj instanceof List)) {
            return Collections.emptyList();
        }

        List<SeriesPoint> out = new ArrayList<>();
        for (Object item : (List<?>) resultObj) {
            Map<String, Object> m = castMap(item);
            if (m == null) {
                continue;
            }
            Map<String, String> metric = castStringMap(m.get("metric"));
            Double value = parseValuePair(m.get("value"));
            if (value == null) {
                continue;
            }
            out.add(new SeriesPoint(metric == null ? Collections.emptyMap() : metric, value));
        }
        return out;
    }

    public Map<Long, Double> queryRangeSingleSeries(String promql, long startEpochSeconds, long endEpochSeconds,
            String stepSeconds) {
        Map<String, Object> body;
        try {
            String url = trimTrailingSlash(baseUrl)
                    + "/api/v1/query_range?query=" + encode(promql)
                    + "&start=" + startEpochSeconds
                    + "&end=" + endEpochSeconds
                    + "&step=" + encode(stepSeconds);
            URI uri = URI.create(url);
            ResponseEntity<Map> resp = restTemplate.getForEntity(uri, Map.class);
            body = resp.getBody();
        } catch (Exception e) {
            return Collections.emptyMap();
        }
        if (body == null) {
            return Collections.emptyMap();
        }
        Object status = body.get("status");
        if (!Objects.equals("success", status)) {
            return Collections.emptyMap();
        }

        Map<String, Object> data = castMap(body.get("data"));
        if (data == null) {
            return Collections.emptyMap();
        }
        Object resultObj = data.get("result");
        if (!(resultObj instanceof List)) {
            return Collections.emptyMap();
        }
        List<?> results = (List<?>) resultObj;
        if (results.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, Object> first = castMap(results.get(0));
        if (first == null) {
            return Collections.emptyMap();
        }
        Object valuesObj = first.get("values");
        if (!(valuesObj instanceof List)) {
            return Collections.emptyMap();
        }

        Map<Long, Double> out = new LinkedHashMap<>();
        for (Object pair : (List<?>) valuesObj) {
            Long ts = parseTimestamp(pair);
            Double val = parseValuePairSecond(pair);
            if (ts == null || val == null) {
                continue;
            }
            out.put(ts, val);
        }
        return out;
    }

    private static String encode(String s) {
        return URLEncoder.encode(s == null ? "" : s, StandardCharsets.UTF_8);
    }

    private static String trimTrailingSlash(String s) {
        if (s == null || s.isBlank()) {
            return "";
        }
        if (s.endsWith("/")) {
            return s.substring(0, s.length() - 1);
        }
        return s;
    }

    private static Map<String, Object> castMap(Object v) {
        if (v instanceof Map) {
            return (Map<String, Object>) v;
        }
        return null;
    }

    private static Map<String, String> castStringMap(Object v) {
        if (!(v instanceof Map)) {
            return null;
        }
        Map<?, ?> src = (Map<?, ?>) v;
        Map<String, String> out = new LinkedHashMap<>();
        for (Map.Entry<?, ?> e : src.entrySet()) {
            if (e.getKey() == null || e.getValue() == null) {
                continue;
            }
            out.put(String.valueOf(e.getKey()), String.valueOf(e.getValue()));
        }
        return out;
    }

    private static Double parseValuePair(Object valueObj) {
        if (!(valueObj instanceof List)) {
            return null;
        }
        List<?> pair = (List<?>) valueObj;
        if (pair.size() < 2) {
            return null;
        }
        Object v = pair.get(1);
        if (v == null) {
            return null;
        }
        try {
            return Double.parseDouble(String.valueOf(v));
        } catch (Exception ignored) {
            return null;
        }
    }

    private static Long parseTimestamp(Object valueObj) {
        if (!(valueObj instanceof List)) {
            return null;
        }
        List<?> pair = (List<?>) valueObj;
        if (pair.isEmpty()) {
            return null;
        }
        Object ts = pair.get(0);
        if (ts == null) {
            return null;
        }
        try {
            double d = Double.parseDouble(String.valueOf(ts));
            return (long) d;
        } catch (Exception ignored) {
            return null;
        }
    }

    private static Double parseValuePairSecond(Object valueObj) {
        return parseValuePair(valueObj);
    }

    public List<SeriesData> queryRangeMultiSeries(String promql, long startEpochSeconds, long endEpochSeconds,
            String stepSeconds) {
        Map<String, Object> body;
        try {
            String url = trimTrailingSlash(baseUrl)
                    + "/api/v1/query_range?query=" + encode(promql)
                    + "&start=" + startEpochSeconds
                    + "&end=" + endEpochSeconds
                    + "&step=" + encode(stepSeconds);
            URI uri = URI.create(url);
            ResponseEntity<Map> resp = restTemplate.getForEntity(uri, Map.class);
            body = resp.getBody();
        } catch (Exception e) {
            return Collections.emptyList();
        }
        if (body == null) {
            return Collections.emptyList();
        }
        Object status = body.get("status");
        if (!Objects.equals("success", status)) {
            return Collections.emptyList();
        }

        Map<String, Object> data = castMap(body.get("data"));
        if (data == null) {
            return Collections.emptyList();
        }
        Object resultObj = data.get("result");
        if (!(resultObj instanceof List)) {
            return Collections.emptyList();
        }
        List<?> results = (List<?>) resultObj;

        List<SeriesData> out = new ArrayList<>();
        for (Object item : results) {
            Map<String, Object> m = castMap(item);
            if (m == null)
                continue;

            Map<String, String> metric = castStringMap(m.get("metric"));
            Object valuesObj = m.get("values");

            Map<Long, Double> values = new LinkedHashMap<>();
            if (valuesObj instanceof List) {
                for (Object pair : (List<?>) valuesObj) {
                    Long ts = parseTimestamp(pair);
                    Double val = parseValuePairSecond(pair);
                    if (ts != null && val != null) {
                        values.put(ts, val);
                    }
                }
            }
            out.add(new SeriesData(metric == null ? Collections.emptyMap() : metric, values));
        }
        return out;
    }

    public static class SeriesPoint {
        private final Map<String, String> metric;
        private final Double value;

        public SeriesPoint(Map<String, String> metric, Double value) {
            this.metric = metric;
            this.value = value;
        }

        public Map<String, String> getMetric() {
            return metric;
        }

        public Double getValue() {
            return value;
        }
    }

    public static class SeriesData {
        private final Map<String, String> metric;
        private final Map<Long, Double> values;

        public SeriesData(Map<String, String> metric, Map<Long, Double> values) {
            this.metric = metric;
            this.values = values;
        }

        public Map<String, String> getMetric() {
            return metric;
        }

        public Map<Long, Double> getValues() {
            return values;
        }
    }
}
