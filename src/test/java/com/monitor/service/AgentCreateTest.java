package com.monitor.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Agent Create 接口压测类
 * 用于多次调用 /agent/create 接口，生成Kafka消息供监控统计
 */
public class AgentCreateTest {

    private static final String BASE_URL = "http://localhost:8086";
    private static final String CREATE_ENDPOINT = "/agent/create";
    
    private RestTemplate restTemplate;
    private ObjectMapper objectMapper;

    @BeforeEach
    public void setup() {
        restTemplate = new RestTemplate();
        objectMapper = new ObjectMapper();
    }

    /**
     * 单线程顺序调用测试
     * 适合调试和验证接口功能
     */
    @Test
    public void testCreateAgentSequential() {
        int callCount = 1; // 调用次数
        
        System.out.println("========== 开始顺序调用测试 ==========");
        System.out.println("调用次数: " + callCount);
        System.out.println("目标地址: " + BASE_URL + CREATE_ENDPOINT);
        System.out.println("=====================================\n");
        
        for (int i = 1; i <= callCount; i++) {
            try {
                Map<String, Object> request = buildCreateRequest(i);
                ResponseEntity<String> response = sendCreateRequest(request);
                
                System.out.printf("[%d/%d] 调用成功 - SceneNo: %s, 响应: %s%n", 
                    i, callCount, request.get("sceneNo"), response.getBody());
                
                // 间隔时间，避免过快调用
                Thread.sleep(100);
            } catch (Exception e) {
                System.err.printf("[%d/%d] 调用失败: %s%n", i, callCount, e.getMessage());
            }
        }
        
        System.out.println("\n========== 顺序调用测试完成 ==========");
    }

    /**
     * 多线程并发调用测试
     * 适合压力测试和大量数据生成
     */
    @Test
    public void testCreateAgentConcurrent() throws InterruptedException {
        int totalCalls = 100;      // 总调用次数
        int threadCount = 10;      // 并发线程数
        
        System.out.println("========== 开始并发调用测试 ==========");
        System.out.println("总调用次数: " + totalCalls);
        System.out.println("并发线程数: " + threadCount);
        System.out.println("目标地址: " + BASE_URL + CREATE_ENDPOINT);
        System.out.println("=====================================\n");
        
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(totalCalls);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);
        
        long startTime = System.currentTimeMillis();
        
        for (int i = 1; i <= totalCalls; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    Map<String, Object> request = buildCreateRequest(index);
                    ResponseEntity<String> response = sendCreateRequest(request);
                    
                    successCount.incrementAndGet();
                    System.out.printf("[%d/%d] 成功 - SceneNo: %s%n", 
                        index, totalCalls, request.get("sceneNo"));
                } catch (Exception e) {
                    failCount.incrementAndGet();
                    System.err.printf("[%d/%d] 失败: %s%n", index, totalCalls, e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        
        // 等待所有任务完成
        latch.await();
        executor.shutdown();
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        System.out.println("\n========== 并发调用测试完成 ==========");
        System.out.println("总耗时: " + duration + " ms");
        System.out.println("成功次数: " + successCount.get());
        System.out.println("失败次数: " + failCount.get());
        System.out.println("平均耗时: " + (duration / totalCalls) + " ms/次");
        System.out.println("QPS: " + (totalCalls * 1000.0 / duration));
        System.out.println("=====================================");
    }

    /**
     * 持续调用测试
     * 按照指定的速率持续发送请求
     */
    @Test
    public void testCreateAgentContinuous() throws InterruptedException {
        int durationSeconds = 60;  // 持续时间（秒）
        int qps = 5;               // 每秒请求数
        
        System.out.println("========== 开始持续调用测试 ==========");
        System.out.println("持续时间: " + durationSeconds + " 秒");
        System.out.println("目标QPS: " + qps);
        System.out.println("目标地址: " + BASE_URL + CREATE_ENDPOINT);
        System.out.println("=====================================\n");
        
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(qps);
        AtomicInteger counter = new AtomicInteger(0);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);
        
        long intervalMillis = 1000 / qps;
        
        ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(() -> {
            int index = counter.incrementAndGet();
            try {
                Map<String, Object> request = buildCreateRequest(index);
                ResponseEntity<String> response = sendCreateRequest(request);
                
                successCount.incrementAndGet();
                System.out.printf("[%d] 成功 - SceneNo: %s%n", index, request.get("sceneNo"));
            } catch (Exception e) {
                failCount.incrementAndGet();
                System.err.printf("[%d] 失败: %s%n", index, e.getMessage());
            }
        }, 0, intervalMillis, TimeUnit.MILLISECONDS);
        
        // 等待指定时间
        Thread.sleep(durationSeconds * 1000L);
        
        future.cancel(false);
        scheduler.shutdown();
        scheduler.awaitTermination(5, TimeUnit.SECONDS);
        
        System.out.println("\n========== 持续调用测试完成 ==========");
        System.out.println("总调用次数: " + counter.get());
        System.out.println("成功次数: " + successCount.get());
        System.out.println("失败次数: " + failCount.get());
        System.out.println("实际QPS: " + (counter.get() / (double) durationSeconds));
        System.out.println("=====================================");
    }

    /**
     * 不同场景号的混合测试
     * 模拟多种业务场景
     */
    @Test
    public void testCreateAgentMultipleScenes() {
        int callsPerScene = 20;  // 每个场景调用次数
        String[] sceneNos = {"SCENE_001", "SCENE_002", "SCENE_003", "SCENE_004", "SCENE_005"};
        
        System.out.println("========== 开始多场景调用测试 ==========");
        System.out.println("场景数量: " + sceneNos.length);
        System.out.println("每场景调用: " + callsPerScene + " 次");
        System.out.println("总调用次数: " + (sceneNos.length * callsPerScene));
        System.out.println("======================================\n");
        
        int totalIndex = 0;
        for (String sceneNo : sceneNos) {
            System.out.println("--- 场景: " + sceneNo + " ---");
            for (int i = 1; i <= callsPerScene; i++) {
                totalIndex++;
                try {
                    Map<String, Object> request = buildCreateRequestWithScene(totalIndex, sceneNo);
                    ResponseEntity<String> response = sendCreateRequest(request);
                    
                    System.out.printf("[%s-%d] 成功 - SceneNo: %s%n", 
                        sceneNo, i, request.get("sceneNo"));
                    
                    Thread.sleep(50);
                } catch (Exception e) {
                    System.err.printf("[%s-%d] 失败: %s%n", sceneNo, i, e.getMessage());
                }
            }
            System.out.println();
        }
        
        System.out.println("========== 多场景调用测试完成 ==========");
    }

    /**
     * 构建创建请求参数
     */
    private Map<String, Object> buildCreateRequest(int index) {
        return buildCreateRequestWithScene(index, "sc111111");
    }

    /**
     * 构建指定场景的创建请求参数
     * 按照指定格式：
     * {
     *     "sceneNo": "sc111111",
     *     "businessInfo": {
     *         "flag": "1"
     *     }
     * }
     */
    private Map<String, Object> buildCreateRequestWithScene(int index, String sceneNo) {
        Map<String, Object> request = new HashMap<>();
        
        // 场景号
        request.put("sceneNo", sceneNo);
        
        // 业务信息 - 只包含 flag 字段
        Map<String, Object> businessInfo = new HashMap<>();
        businessInfo.put("flag", "1");
        request.put("businessInfo", businessInfo);
        
        return request;
    }

    /**
     * 发送创建请求
     */
    private ResponseEntity<String> sendCreateRequest(Map<String, Object> request) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        
        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(request, headers);
        
        return restTemplate.exchange(
            BASE_URL + CREATE_ENDPOINT,
            HttpMethod.POST,
            entity,
            String.class
        );
    }

    /**
     * 定时批量调用测试 - 每30秒批量发送50次请求
     * 适合长期运行和监控
     */
    @Test
    public void testCreateAgentScheduled() throws InterruptedException {
        int batchSize = 50;  // 每批次调用次数
        int intervalSeconds = 30;  // 调用间隔（秒）
        
        System.out.println("========== 开始定时批量调用测试 ==========");
        System.out.println("调用间隔: " + intervalSeconds + " 秒");
        System.out.println("每批次调用: " + batchSize + " 次");
        System.out.println("目标地址: " + BASE_URL + CREATE_ENDPOINT);
        System.out.println("提示: 按 Ctrl+C 停止测试");
        System.out.println("========================================\n");
        
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        AtomicInteger batchCounter = new AtomicInteger(0);
        AtomicInteger totalSuccessCount = new AtomicInteger(0);
        AtomicInteger totalFailCount = new AtomicInteger(0);
        
        // 每30秒执行一次批量调用
        scheduler.scheduleAtFixedRate(() -> {
            int batchNum = batchCounter.incrementAndGet();
            String timestamp = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
            
            System.out.printf("========== [%s] 第 %d 批次开始 ==========\n", timestamp, batchNum);
            
            int batchSuccess = 0;
            int batchFail = 0;
            long batchStartTime = System.currentTimeMillis();
            
            // 批量调用50次
            for (int i = 1; i <= batchSize; i++) {
                try {
                    Map<String, Object> request = buildCreateRequest((batchNum - 1) * batchSize + i);
                    ResponseEntity<String> response = sendCreateRequest(request);
                    
                    batchSuccess++;
                    totalSuccessCount.incrementAndGet();
                    System.out.printf("  [%d/%d] 成功 - SceneNo: %s%n", i, batchSize, request.get("sceneNo"));
                } catch (Exception e) {
                    batchFail++;
                    totalFailCount.incrementAndGet();
                    System.err.printf("  [%d/%d] 失败: %s%n", i, batchSize, e.getMessage());
                }
            }
            
            long batchEndTime = System.currentTimeMillis();
            long batchDuration = batchEndTime - batchStartTime;
            
            System.out.printf("========== 第 %d 批次完成 ==========\n", batchNum);
            System.out.printf("批次耗时: %d ms\n", batchDuration);
            System.out.printf("批次成功: %d 次, 批次失败: %d 次\n", batchSuccess, batchFail);
            System.out.printf("累计成功: %d 次, 累计失败: %d 次\n", totalSuccessCount.get(), totalFailCount.get());
            System.out.printf("下次执行时间: %s\n", 
                new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(
                    new Date(System.currentTimeMillis() + intervalSeconds * 1000)));
            System.out.println("=====================================\n");
            
        }, 0, intervalSeconds, TimeUnit.SECONDS);
        
        // 运行5分钟后自动停止（可以根据需要调整或注释掉这行让它一直运行）
        Thread.sleep(5 * 60 * 1000);
        
        scheduler.shutdown();
        scheduler.awaitTermination(5, TimeUnit.SECONDS);
        
        System.out.println("\n========== 定时批量调用测试完成 ==========");
        System.out.println("总批次数: " + batchCounter.get());
        System.out.println("总调用次数: " + (batchCounter.get() * batchSize));
        System.out.println("总成功次数: " + totalSuccessCount.get());
        System.out.println("总失败次数: " + totalFailCount.get());
        System.out.println("========================================");
    }

    /**
     * 主方法 - 可以直接运行
     */
    public static void main(String[] args) throws Exception {
        AgentCreateTest test = new AgentCreateTest();
        test.setup();
        
        System.out.println("请选择测试模式:");
        System.out.println("1. 顺序调用测试");
        System.out.println("2. 并发调用测试");
        System.out.println("3. 持续调用测试");
        System.out.println("4. 多场景调用测试");
        System.out.println("5. 定时调用测试 (每30秒)");
        System.out.println("6. 全部执行");
        
        Scanner scanner = new Scanner(System.in);
        System.out.print("\n请输入选项 (1-6): ");
        int choice = scanner.nextInt();
        
        switch (choice) {
            case 1:
                test.testCreateAgentSequential();
                break;
            case 2:
                test.testCreateAgentConcurrent();
                break;
            case 3:
                test.testCreateAgentContinuous();
                break;
            case 4:
                test.testCreateAgentMultipleScenes();
                break;
            case 5:
                test.testCreateAgentScheduled();
                break;
            case 6:
                System.out.println("\n执行全部测试...\n");
                test.testCreateAgentSequential();
                Thread.sleep(2000);
                test.testCreateAgentMultipleScenes();
                Thread.sleep(2000);
                test.testCreateAgentConcurrent();
                break;
            default:
                System.out.println("无效选项");
        }
        
        scanner.close();
    }
}
