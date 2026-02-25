# 手工流水线测试用例文档

> **项目**: Kafka 监控工程 (json_monitor)
> **版本**: 1.0
> **编写日期**: 2026-02-11
> **文档类型**: 业务流水线手工测试
> **测试环境**: localhost:5173 (前端) + localhost:8080 (后端) + Docker Compose 基础设施

---

## 文档说明

### 与功能测试的区别

| 维度 | 功能测试 (`TEST_CASES.md`) | 流水线测试（本文档） |
|:---|:---|:---|
| **关注点** | 单个页面、单个功能是否正常 | 端到端业务流程是否贯通 |
| **范围** | 独立的 UI 操作 | 跨组件、跨服务的完整链路 |
| **数据流** | 页面内数据展示 | Kafka → 后端消费 → 指标记录 → Prometheus → 前端展示 |
| **验证方式** | 前端 UI 状态检查 | 前端 + 后端日志 + Prometheus 指标 + Kafka 状态 多维度交叉验证 |

### 流水线测试覆盖的业务场景

本文档定义 **5 条核心业务流水线**，共计 **30** 个测试步骤：

| 流水线编号 | 名称 | 验证目标 |
|:---|:---|:---|
| PL-01 | 消息全链路流转（STATE 类型） | 验证 STATE 消息从 Kafka 产生到前端展示的完整链路 |
| PL-02 | 租户消息链路追踪（TENANT 类型） | 验证 TENANT 消息流转及链路追踪功能的完整性 |
| PL-03 | 积压监控与消息预览 | 验证 Kafka 积压检测和消息预览的端到端能力 |
| PL-04 | 延迟告警触发与恢复（告警生命周期） | 验证延迟超阈值触发告警邮件、恢复后发送恢复邮件的完整流程 |
| PL-05 | 系统异常与恢复 | 验证 Kafka / Prometheus 宕机场景下系统的降级与恢复能力 |

---

### 前置条件（所有流水线通用）

| # | 条件 | 验证方法 |
|:---|:---|:---|
| 1 | Docker Compose 基础设施全部运行 | `docker-compose ps` 显示所有服务 Up |
| 2 | 后端服务运行在 `localhost:8080` | 访问 `http://localhost:8080/actuator/health` 返回 `{"status":"UP"}` |
| 3 | 前端服务运行在 `localhost:5173` | 浏览器访问 `http://localhost:5173/dashboard` 正常加载 |
| 4 | Prometheus 正常采集 | 访问 `http://localhost:9090/targets` 显示所有 target UP |
| 5 | Kafka Broker 正常运行 | `docker exec kafka kafka-topics --list --bootstrap-server localhost:9092` 返回 Topic 列表 |

### 测试结果标记

| 标记 | 含义 |
|:---|:---|
| ✅ PASS | 测试通过 |
| ❌ FAIL | 测试失败 |
| ⚠️ SKIP | 跳过（环境不满足） |
| 🔄 BLOCK | 阻塞（依赖其他用例） |

---

## PL-01: 消息全链路流转（STATE 类型）

> **验证目标**: 验证 STATE 类型消息从 Kafka 产生 → 后端旁路消费 → 消息解析 → 指标计数 → Prometheus 存储 → 前端仪表盘/消息搜索展示的完整链路。

```
Kafka Producer
     │
     ▼
 Kafka Topic (state)
     │
     ▼
 MessageObserverConsumer.observe()
     │
     ├──→ MessageParser.parseStateMessage()    ──→ ParsedMessage
     │
     ├──→ MetricsService
     │      ├── incByMessageType("STATE")
     │      ├── incByCategory("state")
     │      ├── incByWatchState()
     │      ├── incProduced() / incConsumed()
     │      ├── recordMessageSize()
     │      └── recordLagLatency()
     │
     ├──→ RealtimeStatsService.recordMessage()  ──→ 内存统计
     │
     ├──→ MessageRawStoreService.put()          ──→ 原始JSON缓存
     │
     └──→ saveJsonToFile()                      ──→ json/state/ 目录
              │
              ▼
         Prometheus scrape (/actuator/prometheus)
              │
              ▼
         前端 Dashboard / MessageSearch 展示
```

### PL-01-STEP-01: 发送 STATE 消息到 Kafka

| 项目 | 内容 |
|:---|:---|
| **步骤编号** | PL-01-STEP-01 |
| **步骤名称** | 向 state Topic 发送一条测试 STATE 消息 |
| **操作方式** | 使用 Kafka Console Producer 或应用内发送 |
| **操作步骤** | 1. 运行 Kafka Console Producer：<br>`docker exec -it kafka kafka-console-producer --bootstrap-server localhost:9092 --topic state --property "parse.key=true" --property "key.separator=\|"`<br>2. 输入消息（Key\|Value 格式）：<br>Key: `state/TEST_TASK_PL01_001/uuid-pl01-001`<br>Value: `{"taskId":"TEST_TASK_PL01_001","watchState":"5","nodeName":"AT1","systemNo":"SYS001","adviseKey":"checkOut"}` |
| **预期结果** | 1. 消息成功发送，无报错<br>2. 后端控制台可观察到消费日志（如无日志不影响后续步骤） |
| **测试结果** | ✅|
| **备注** | watchState=5 映射为 SUCCESS |

### PL-01-STEP-02: 验证后端消费与 JSON 文件生成

| 项目 | 内容 |
|:---|:---|
| **步骤编号** | PL-01-STEP-02 |
| **步骤名称** | 确认后端旁路消费者正确消费消息 |
| **操作步骤** | 1. 检查 `json/` 目录下是否生成新的 JSON 文件：<br>`ls json/ \| sort -r \| head -5`<br>2. 打开最新的 JSON 文件，查看内容 |
| **预期结果** | 1. `json/` 目录下新增一个 `state_*_TEST_TASK_PL01_001_*.json` 文件<br>2. 文件内容为发送的原始 JSON |
| **测试结果** | ✅|
| **备注** | 文件名格式：`{topic}_{timestamp}_{safeKey}_{uuid}.json` |

### PL-01-STEP-03: 验证 Prometheus 指标写入

| 项目 | 内容 |
|:---|:---|
| **步骤编号** | PL-01-STEP-03 |
| **步骤名称** | 通过 Prometheus 验证指标计数器递增 |
| **操作步骤** | 1. 访问 `http://localhost:8080/actuator/prometheus`<br>2. 搜索以下指标：<br>  - `msg_by_type_total{messageType="STATE"}`<br>  - `msg_by_category_total{category="state"}`<br>  - `msg_by_watch_state_total{watchState="5"}`<br>3. 访问 Prometheus UI `http://localhost:9090`<br>4. 执行 PromQL：`msg_by_type_total{messageType="STATE"}` |
| **预期结果** | 1. Actuator 端点中上述指标值递增（+1）<br>2. Prometheus 中查询结果显示最新计数<br>3. `msg_by_watch_state_total` 中 watchState="5" 的计数递增 |
| **测试结果** | ✅|
| **备注** | Prometheus 采集间隔 15 秒，可能需等待 |

### PL-01-STEP-04: 验证前端仪表盘数据更新

| 项目 | 内容 |
|:---|:---|
| **步骤编号** | PL-01-STEP-04 |
| **步骤名称** | 前端仪表盘显示新增消息 |
| **操作步骤** | 1. 打开 `http://localhost:5173/dashboard`<br>2. 等待自动刷新（约 5 秒）或手动刷新页面<br>3. 观察以下区域：<br>  - 消息数统计卡片（State 数量）<br>  - 吞吐趋势图（节点状态折线）<br>  - watchState 饼图（"处理完成"扇区） |
| **预期结果** | 1. State 消息总数递增<br>2. 吞吐趋势图中"节点状态"折线在最近时间点有数据<br>3. watchState 饼图中"处理完成"(watchState=5)计数+1 |
| **测试结果** | ✅|
| **备注** | |

### PL-01-STEP-05: 验证前端消息搜索可检索

| 项目 | 内容                                                                                                   |
|:---|:-----------------------------------------------------------------------------------------------------|
| **步骤编号** | PL-01-STEP-05                                                                                        |
| **步骤名称** | 在消息搜索中检索到新消息                                                                                         |
| **操作步骤** | 1. 导航到 `http://localhost:5173/messages`<br>2. 在"任务ID"中输入 `TESTID`<br>3. 在"种类"下拉选择"节点状态"<br>4. 点击"查询" |
| **预期结果** | 1. 搜索结果中出现包含 `TESTID` 的记录<br>2. 种类列显示蓝色"节点状态"标签<br>3. 结果列显示"SUCCESS"<br>4. 节点列显示"AT1"                |
| **测试结果** |    ✅                                                                                                  |
| **备注** |                                                                                                      |

### PL-01-STEP-06: 验证消息详情中 rawJson 完整性

| 项目 | 内容 |
|:---|:---|
| **步骤编号** | PL-01-STEP-06 |
| **步骤名称** | 点击详情查看原始 JSON |
| **操作步骤** | 1. 在上一步搜索结果中，点击对应记录的"详情"按钮<br>2. 观察弹窗内容 |
| **预期结果** | 1. 弹窗显示"原始 JSON 数据"<br>2. Kafka Key 部分显示 `state/TEST_TASK_PL01_001/uuid-pl01-001`<br>3. Body 部分显示完整的 JSON 内容，包含 taskId、watchState、nodeName 等字段<br>4. JSON 格式化正确，可读性好 |
| **测试结果** | ✅|
| **备注** | 如果消息已超出 LRU 缓存（2000条），则显示"标签数据" |

---

## PL-02: 租户消息链路追踪（TENANT 类型）

> **验证目标**: 验证 TENANT 消息全链路流转，并通过链路追踪功能查看同一任务的多条消息按时间排序展示。

```
  SUNYARD Topic          AGENT Topic          state Topic
       │                      │                    │
       ▼                      ▼                    ▼
  MessageObserverConsumer (旁路消费，统一处理)
       │                      │                    │
       ├──→ parseTenantMessage()  parseTenantMessage()  parseStateMessage()
       │
       ├──→ MetricsService (内部延迟 + 超时检测)
       │
       ├──→ RealtimeStatsService (按 taskId 聚合)
       │
       └──→ rawStore (LRU缓存)
              │
              ▼
         消息搜索 → 输入 taskId → 链路追踪完整展示
```

### PL-02-STEP-01: 发送同一任务的多条消息（模拟完整交易）

| 项目 | 内容                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|:---|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **步骤编号** | PL-02-STEP-01                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| **步骤名称** | 向多个 Topic 发送同一 taskId 的消息，模拟完整交易流程                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| **操作步骤** | **消息 1 - SUNYARD/AGENT/xxx (租户消息)**：<br>Topic(例如): `SUNYARD`, Key: `SUNYARD/TRACE_TASK_001/TXN001/0/5001/uuid-002`<br>Value: `{"taskId":"TRACE_TASK_001","systemNo":"SYS001","nodeName":"AT1","systemState":"Running","workitemState":"4","startTime":"1739260510000","checkOutTime":"1739260511000"}`<br><br>**消息 2 - STATE (完成)**：<br>Topic: `state`, Key: `state/TRACE_TASK_001/uuid-003`<br>Value: `{"taskId":"TRACE_TASK_001","watchState":"5","nodeName":"AT1","systemNo":"SYS001"}`<br><br> |
| **预期结果** | 1. 消息依次发送成功<br>2. 后端消费无异常                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| **测试结果** |   ✅                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| **备注** | 注意消息间间隔 1-2 秒以确保时间排序可区分                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |

### PL-02-STEP-02: 验证延迟指标计算（仅 TENANT 类型）

| 项目 | 内容                                                                                                                                                                                         |
|:---|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **步骤编号** | PL-02-STEP-02                                                                                                                                                                              |
| **步骤名称** | 确认 TENANT、STATE 消息的延迟被正确计算                                                                                                                                                                 |
| **操作步骤** | 1. 访问 `http://localhost:8080/actuator/prometheus`<br>2. 搜索 `msg_e2e_latency_seconds`<br>3. 搜索 `msg_internal_latency_seconds`                                                               |
| **预期结果** | 1. `msg_e2e_latency_seconds` 计数递增（SUNYARD 和 AGENT 消息各+1）<br>2. `msg_internal_latency_seconds` 计数递增（仅有 startTime 且有 checkOutTime/checkInTime 的消息）<br>3. STATE 类型消息**不产生** E2E/Internal 延迟指标 |
| **测试结果** |     ✅                                                                                                                                                                                       |
| **备注** | 延迟计算逻辑： now - startTime, Internal = checkOutTime/checkInTime - startTime                                                                                                              |

### PL-02-STEP-03: 链路追踪查询完整交易

| 项目 | 内容                                                                                                                                                                            |
|:---|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **步骤编号** | PL-02-STEP-03                                                                                                                                                                 |
| **步骤名称** | 通过链路追踪查看完整交易过程                                                                                                                                                                |
| **操作步骤** | 1. 导航到 `http://localhost:5173/trace`<br>2. 在"任务ID"输入框输入 `TASKID`<br>3. 点击"查询链路"                                                                                               |
| **预期结果** | 1. 时间线中展示消息节点<br>2. 按时间从上到下排序<br>3. 各节点显示正确的种类标签：<br> 4. 流水号字段正确显示                                                                            |
| **测试结果** |   ✅                                                                                                                                                                            |
| **备注** |                                                                                                                                                                               |

### PL-02-STEP-04: 链路追踪详情一致性

| 项目 | 内容                                                                                                                              |
|:---|:--------------------------------------------------------------------------------------------------------------------------------|
| **步骤编号** | PL-02-STEP-04                                                                                                                   |
| **步骤名称** | 验证链路中每个节点的详情与消息搜索一致                                                                                                             |
| **操作步骤** | 1. 在链路追踪中点击消息节点的"详情"<br>2. 记录 rawJson 内容<br>3. 导航到消息搜索，搜索 `TASKID` <br>4. 点击对应记录的"详情"<br>5. 对比两端的 rawJson                       |
| **预期结果** | 1. 链路追踪中展示的 rawJson 与消息搜索中的**完全一致**<br>2. Kafka Key 和 Body 均正确显示<br>3. 种类标签颜色一致                                                 |
| **测试结果** |     ✅                                                                                                                            |
| **备注** |                                                                                                                                 |

### PL-02-STEP-05: 仪表盘租户统计反映

| 项目 | 内容 |
|:---|:---|
| **步骤编号** | PL-02-STEP-05 |
| **步骤名称** | 验证仪表盘租户 Top 柱状图更新 |
| **操作步骤** | 1. 导航回仪表盘 `/dashboard`<br>2. 观察"租户 Top"柱状图 |
| **预期结果** | 1. SUNYARD 和 AGENT 的计数均递增<br>2. 消息类型分布图中"租户数据"数值递增 |
| **测试结果** | ✅|
| **备注** | |

---

## PL-03: 积压监控与消息预览

> **验证目标**: 验证 Kafka 积压检测的端到端能力——通过制造消费延迟/停止消费者，使积压产生，验证积压监控页面正确展示分区积压、支持查看积压中的消息内容。

```
  Kafka Producer (持续发送消息)
       │
       ▼
  Kafka Topic (SUNYARD)
       │                     ← 消费者正常：lag=0
       │                     ← 停止消费者/消费速度 < 生产速度：lag > 0
       ▼
  KafkaBacklogService
       ├── AdminClient.listConsumerGroupOffsets()  → committed offset
       ├── KafkaConsumer.endOffsets()               → end offset
       └── lag = end - committed
              │
              ▼
         前端 BacklogView 展示
```

### PL-03-STEP-01: 记录当前基线积压

| 项目 | 内容 |
|:---|:---|
| **步骤编号** | PL-03-STEP-01 |
| **步骤名称** | 记录当前各 Topic 的基线积压值 |
| **操作步骤** | 1. 打开 `http://localhost:5173/backlog`<br>2. 分别查看 SUNYARD、AGENT、state 三个 Topic 的积压<br>3. 记录每个 Topic 的：<br>  - 总积压数（Total Lag）<br>  - 各分区的 committed / end / lag |
| **预期结果** | 1. 页面正常显示各 Topic 积压信息<br>2. 记录下基线数据供后续对比 |
| **测试结果** | ✅|
| **备注** |  |

### PL-03-STEP-02: 批量发送消息制造积压

| 项目 | 内容 |
|:---|:---|
| **步骤编号** | PL-03-STEP-02 |
| **步骤名称** | 快速发送多条消息到 SUNYARD Topic |
| **操作步骤** | 1. 使用 Kafka Console Producer 快速发送 10+ 条消息到 SUNYARD Topic：<br>`docker exec kafka kafka-console-producer --bootstrap-server localhost:9092 --topic SUNYARD --property "parse.key=true" --property "key.separator=\|"`<br>2. 连续输入多条消息 |
| **预期结果** | 1. 消息发送成功<br>2. 短时间内积压数可能暂时上升 |
| **测试结果** | ✅|
| **备注** | 如果消费速度快于发送速度，可能需要更大量的发送 |

### PL-03-STEP-03: 验证积压数据变化

| 项目 | 内容 |
|:---|:---|
| **步骤编号** | PL-03-STEP-03 |
| **步骤名称** | 观察积压监控页面数据变化 |
| **操作步骤** | 1. 刷新积压监控页面<br>2. 对比步骤 01 的基线数据<br>3. 观察 end offset 是否增长<br>4. 等待消费者追上，观察 lag 是否归零 |
| **预期结果** | 1. end offset 明显增长（反映新消息到达）<br>2. committed offset 逐步追上 end（消费者正在处理）<br>3. 最终 lag 归零或接近零<br>4. 总积压数值正确更新 |
| **测试结果** | ✅|
| **备注** | |

### PL-03-STEP-04: 积压消息预览

| 项目 | 内容 |
|:---|:---|
| **步骤编号** | PL-03-STEP-04 |
| **步骤名称** | 查看积压分区中的消息内容 |
| **操作步骤** | 1. 在积压监控中选择有数据的分区<br>2. 在"全部积压浏览"区域选择分区号<br>3. 点击"从committed开始"<br>4. 点击"查询"<br>5. 浏览显示的消息列表 |
| **预期结果** | 1. 消息列表正确显示 offset、key、value、timestamp<br>2. value 内容与发送的消息一致<br>3. 分页功能正常（如有多条消息），上一页/下一页切换正确<br>4. 底部显示 committed/end/nextOffset 信息 |
| **测试结果** | ✅|
| **备注** | |

---

## PL-04: 延迟告警触发与恢复（告警生命周期）

> **验证目标**: 验证当消息延迟超过配置阈值时触发告警邮件，延迟恢复正常后发送恢复邮件的完整生命周期。

```
  Messages with high latency
       │
       ▼
  MetricsService.recordLagLatency()  → msg_lag_latency_seconds
       │
       ▼
  Prometheus scrape                  → 存储直方图数据
       │
       ▼
  LatencyAlertService.checkLatency()  ← @Scheduled(60s)
       │
       ├── PromQL: histogram_quantile(pValue, sum(rate(msg_lag_latency_seconds_bucket[1m])) by (le))
       │
       ├── 超阈值 && !isAlerting → sendAlertEmail()    → 告警邮件
       │
       └── 恢复 && isAlerting    → sendRecoveryEmail() → 恢复邮件
```

### PL-04-STEP-01: 配置低阈值告警参数

| 项目 | 内容                                                                                                                                                    |
|:---|:------------------------------------------------------------------------------------------------------------------------------------------------------|
| **步骤编号** | PL-04-STEP-01                                                                                                                                         |
| **步骤名称** | 在前端设置一个极低的延迟阈值以便触发告警                                                                                                                                  |
| **操作步骤** | 1. 导航到 `http://localhost:5173/alerts`<br>2. 开启"启用告警"开关<br>3. 设置分位数为 P99<br>4. 设置延迟阈值为 **0.01 秒**（极低，几乎必触发）<br>5. 配置 SMTP、发送邮箱、授权码、接收邮箱<br>6. 点击"保存配置" |
| **预期结果** | 1. 保存成功提示<br>2. 刷新页面后配置保持                                                                                                                             |
| **测试结果** |  ✅                                                                                                                                                     |
| **备注** | 阈值设为 0.001s 是为了测试目的，确保触发告警                                                                                                                            |

### PL-04-STEP-02: 验证告警配置持久化

| 项目 | 内容 |
|:---|:---|
| **步骤编号** | PL-04-STEP-02 |
| **步骤名称** | 确认配置写入 `latency-alert-config.json` |
| **操作步骤** | 1. 打开项目根目录的 `latency-alert-config.json` 文件<br>2. 查看文件内容 |
| **预期结果** | 1. `enabled` 为 `true`<br>2. `pValue` 为 `0.99`<br>3. `thresholdSeconds` 为 `0.001`<br>4. SMTP 和邮箱配置正确写入 |
| **测试结果** | ✅|
| **备注** | |

### PL-04-STEP-03: 发送测试邮件验证邮箱通道

| 项目 | 内容 |
|:---|:---|
| **步骤编号** | PL-04-STEP-03 |
| **步骤名称** | 发送测试邮件确保邮箱配置正确 |
| **操作步骤** | 1. 在告警配置页面点击"发送测试邮件"<br>2. 检查收件箱 |
| **预期结果** | 1. 页面提示"测试邮件已发送"<br>2. 收件箱收到主题为 **"Kafka Monitor Test Email"** 的邮件<br>3. 邮件内容为 "Congratulations! Your Kafka Monitor alert email configuration is correct." |
| **测试结果** | ✅|
| **备注** | 如果此步失败，后续告警邮件也将无法发送 |

### PL-04-STEP-04: 触发延迟告警

| 项目 | 内容 |
|:---|:---|
| **步骤编号** | PL-04-STEP-04 |
| **步骤名称** | 等待告警服务检测到延迟超阈值 |
| **操作步骤** | 1. 发送几条消息到 Kafka（确保有延迟数据）<br>2. 等待 1-2 分钟（LatencyAlertService 每 60 秒检查一次）<br>3. 查看后端日志中是否出现 `"Lag alert triggered!"` |
| **预期结果** | 1. 后端日志出现：`Lag alert triggered! Current P99 = X.XXX s > threshold 0.001 s`<br>2. 后端日志出现：`Alert email sent to {receiver}`<br>3. 收件箱收到告警邮件，主题为 **"Backlog Lag Alert: P99"**<br>4. 邮件内容包含当前延迟值和阈值 |
| **测试结果** | ✅|
| **备注** | 如果 Prometheus 无延迟数据，告警不会触发 |

### PL-04-STEP-05: 恢复正常阈值并验证恢复邮件

| 项目 | 内容 |
|:---|:---|
| **步骤编号** | PL-04-STEP-05 |
| **步骤名称** | 提高阈值使延迟恢复"正常"，验证恢复邮件发送 |
| **操作步骤** | 1. 回到告警配置页面<br>2. 将延迟阈值修改为 **60 秒**（极高，确保不再告警）<br>3. 保存配置<br>4. 等待 1-2 分钟 |
| **预期结果** | 1. 后端日志出现：`Lag alert resolved. Current value = X.XXX s`<br>2. 后端日志出现：`Recovery email sent to {receiver}`<br>3. 收件箱收到恢复邮件，主题为 **"Backlog Lag Resolved: P99"**<br>4. 邮件内容表明延迟已恢复到阈值以下 |
| **测试结果** | ✅|
| **备注** | |

### PL-04-STEP-06: 清理——恢复正常告警配置

| 项目 | 内容 |
|:---|:---|
| **步骤编号** | PL-04-STEP-06 |
| **步骤名称** | 恢复合理的告警阈值 |
| **操作步骤** | 1. 将阈值恢复为 **0.25 秒**<br>2. 保存配置 |
| **预期结果** | 1. 配置保存成功<br>2. `latency-alert-config.json` 更新为合理值 |
| **测试结果** | ✅|
| **备注** | 每次流水线测试执行后都需清理环境 |

---

## PL-05: 系统异常与恢复

> **验证目标**: 验证当关键依赖（Kafka、Prometheus）不可用时，系统的降级表现和恢复后的自愈能力。

### PL-05-STEP-01: Prometheus 宕机降级

| 项目 | 内容 |
|:---|:---|
| **步骤编号** | PL-05-STEP-01 |
| **步骤名称** | 停止 Prometheus 服务，验证系统降级表现 |
| **操作步骤** | 1. 停止 Prometheus：`docker-compose stop prometheus`<br>2. 刷新仪表盘 `/dashboard`<br>3. 刷新消息搜索 `/messages` 并搜索<br>4. 查看 Kafka 健康 `/pending` |
| **预期结果** | 1. 仪表盘：<br>  - 消息数保持旧数据<br>  - 吞吐趋势图无新数据<br>  - 但**内存实时统计部分仍可工作**（如实时消息数仍递增）<br>2. 消息搜索：<br>  - 从 Prometheus 查询的历史数据不可用<br>  - 但内存实时数据仍可搜索到最近的消息<br>3. Kafka 健康：<br>  - 状态显示"未知"<br>  - 提示"无法获取 Kafka 核心指标" |
| **测试结果** | ✅|
| **备注** | 验证系统不会因 Prometheus 不可用而崩溃 |

### PL-05-STEP-02: Prometheus 恢复

| 项目 | 内容 |
|:---|:---|
| **步骤编号** | PL-05-STEP-02 |
| **步骤名称** | 重新启动 Prometheus，观察系统自动恢复 |
| **操作步骤** | 1. 启动 Prometheus：`docker-compose start prometheus`<br>2. 等待 15-30 秒（Prometheus scrape_interval）<br>3. 刷新仪表盘<br>4. 刷新 Kafka 健康页面 |
| **预期结果** | 1. 仪表盘数据恢复正常<br>2. Kafka 健康页面显示具体指标和状态标签<br>3. 消息搜索可从 Prometheus 获取历史数据<br>4. 无需重启后端服务 |
| **测试结果** | ✅|
| **备注** | |

### PL-05-STEP-03: 后端服务重启恢复

| 项目 | 内容 |
|:---|:---|
| **步骤编号** | PL-05-STEP-03 |
| **步骤名称** | 重启后端服务，验证数据恢复 |
| **操作步骤** | 1. 停止后端服务（Ctrl+C 或关闭 IDE）<br>2. 等待 10 秒后重新启动后端服务<br>3. 刷新前端各页面 |
| **预期结果** | 1. 后端重启后自动连接 Kafka 并恢复消费<br>2. 告警配置正确加载（从 `latency-alert-config.json`）<br>3. 前端页面正常加载，无持续报错<br>4. 内存实时数据清空（重启后无历史内存数据——预期行为）<br>5. Prometheus 历史数据仍可查询 |
| **测试结果** | ✅|
| **备注** | 内存数据丢失是预期行为，Prometheus 数据不受影响 |

### PL-05-STEP-04: AlertManager 宕机降级

| 项目 | 内容 |
|:---|:---|
| **步骤编号** | PL-05-STEP-04 |
| **步骤名称** | 停止 AlertManager，验证基础设施告警降级 |
| **操作步骤** | 1. 停止 AlertManager：`docker-compose stop alertmanager`<br>2. 访问 Prometheus `/alerts` 页面<br>3. 验证应用内延迟告警是否不受影响 |
| **预期结果** | 1. Prometheus 告警规则仍存在但无法推送到 AlertManager<br>2. **应用内延迟告警不受影响**（LatencyAlertService 直接发送邮件，不依赖 AlertManager）<br>3. 前端告警配置页面正常操作 |
| **测试结果** | ✅|
| **备注** | AlertManager 仅影响基础设施级别告警（如 KafkaBrokerDown），不影响应用内告警 |

---


