# Kafka 流程监控系统开发任务

## 项目概述

设计并实现一个前后端分离的 Kafka 消息监控系统，用于监控流程部门间的 JSON 数据传递。

------

## 任务列表

### 1. 项目规划与设计

- 分析 JSON 数据结构，设计监控指标
-  设计系统架构（Spring Boot + Vue + Prometheus + Grafana）
-  创建实施计划文档

### 2. 后端开发 (Spring Boot)

-  初始化 Spring Boot 项目结构

-  配置 Kafka Consumer（主消费者 + 旁路观测消费者）

-  集成 Micrometer + Prometheus 指标埋点

-  

  实现消息解析与指标采集

  -  生产频率指标 (msg_produced_total)
  -  消费结果状态指标 (msg_consumed_total)
  -  端到端延迟指标 (msg_e2e_latency_seconds)
  -  超时计数指标 (msg_timeout_total)

-  

  实现 REST API

  -  消息查询搜索接口
  -  统计汇总接口
  -  告警规则配置接口

-  配置告警规则（AlertManager 集成）

### 3. 前端开发 (Vue 3)

-  初始化 Vue 3 + Vite 项目

-  

  设计并实现 UI 组件

  -  仪表盘首页
  -  Grafana iframe 嵌入组件
  -  消息搜索查询界面
  -  告警配置管理界面
  -  链路追踪视图

-  集成 ECharts 自定义图表（可选）

### 4. 基础设施配置

-  

  Docker Compose 编排文件

  -  Kafka + Zookeeper
  -  Prometheus
  -  Grafana
  -  AlertManager

-  Grafana Dashboard JSON 模板

-  Prometheus 告警规则配置

### 5. 测试与验证

-  模拟 Kafka 消息生产
-  验证指标采集正确性
-  验证 Grafana 可视化效果
-  验证告警触发功能