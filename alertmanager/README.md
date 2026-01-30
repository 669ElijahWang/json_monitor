# Alertmanager 邮件通知配置指南

## 📧 SMTP 配置说明

在使用邮件通知前，需要修改 `alertmanager/alertmanager.yml` 文件中的 SMTP 配置。

### 1️⃣ 常见邮件服务商配置

#### Gmail
```yaml
smtp_smarthost: 'smtp.gmail.com:587'
smtp_from: 'your-email@gmail.com'
smtp_auth_username: 'your-email@gmail.com'
smtp_auth_password: 'your-app-password'  # 需要使用应用专用密码
smtp_require_tls: true
```

**注意**: Gmail 需要开启"两步验证"并生成"应用专用密码"
- 设置地址: https://myaccount.google.com/apppasswords

#### QQ 邮箱
```yaml
smtp_smarthost: 'smtp.qq.com:587'
smtp_from: 'your-email@qq.com'
smtp_auth_username: 'your-email@qq.com'
smtp_auth_password: 'your-authorization-code'  # 使用授权码，不是登录密码
smtp_require_tls: true
```

**获取授权码**: QQ 邮箱设置 → 账户 → POP3/IMAP/SMTP/Exchange/CardDAV/CalDAV服务 → 生成授权码

#### 163 邮箱
```yaml
smtp_smarthost: 'smtp.163.com:465'
smtp_from: 'your-email@163.com'
smtp_auth_username: 'your-email@163.com'
smtp_auth_password: 'your-authorization-code'  # 使用授权码
smtp_require_tls: true
```

#### 企业邮箱 (腾讯企业邮箱)
```yaml
smtp_smarthost: 'smtp.exmail.qq.com:587'
smtp_from: 'your-email@yourcompany.com'
smtp_auth_username: 'your-email@yourcompany.com'
smtp_auth_password: 'your-password'
smtp_require_tls: true
```

#### Outlook / Office 365
```yaml
smtp_smarthost: 'smtp.office365.com:587'
smtp_from: 'your-email@outlook.com'
smtp_auth_username: 'your-email@outlook.com'
smtp_auth_password: 'your-password'
smtp_require_tls: true
```

### 2️⃣ 修改收件人邮箱

在 `alertmanager.yml` 中找到以下配置并修改：

```yaml
receivers:
  - name: 'email-notifications'
    email_configs:
      - to: 'team@example.com'  # ← 修改为实际接收告警的邮箱

  - name: 'email-critical'
    email_configs:
      - to: 'oncall@example.com'  # ← 修改为紧急联系人邮箱
```

### 3️⃣ 配置步骤

1. **编辑配置文件**
   ```bash
   # 打开 alertmanager/alertmanager.yml
   # 修改以下字段：
   # - smtp_smarthost: SMTP 服务器地址
   # - smtp_from: 发件人邮箱
   # - smtp_auth_username: SMTP 用户名
   # - smtp_auth_password: SMTP 密码/授权码
   # - to: 收件人邮箱
   ```

2. **重启服务**
   ```bash
   docker-compose restart alertmanager prometheus
   ```

3. **验证配置**
   - 访问 Alertmanager UI: http://localhost:9093
   - 访问 Prometheus Alerts: http://localhost:9090/alerts

### 4️⃣ 测试告警

你可以手动触发一个测试告警来验证邮件是否正常发送：

```bash
# 向 Alertmanager 发送测试告警
curl -H "Content-Type: application/json" -d '[
  {
    "labels": {
      "alertname": "TestAlert",
      "severity": "warning"
    },
    "annotations": {
      "summary": "这是一个测试告警"
    }
  }
]' http://localhost:9093/api/v1/alerts
```

### 5️⃣ 常见问题

#### Q: 邮件发送失败，显示认证错误
A: 
- 检查是否使用了"授权码"而不是登录密码（QQ、163 等需要）
- Gmail 需要开启两步验证并生成应用专用密码
- 确认 SMTP 用户名和发件人邮箱一致

#### Q: 端口连接失败
A:
- 尝试使用 465 端口（SSL）或 587 端口（TLS）
- 检查防火墙是否阻止了 SMTP 端口

#### Q: 收不到邮件
A:
- 检查垃圾邮件文件夹
- 查看 Alertmanager 日志: `docker-compose logs alertmanager`
- 确认收件人邮箱地址正确

### 6️⃣ 查看日志

```bash
# 查看 Alertmanager 日志
docker-compose logs -f alertmanager

# 查看 Prometheus 日志
docker-compose logs -f prometheus
```

### 7️⃣ 告警规则说明

当前配置的告警规则（在 `prometheus/alert-rules.yml` 中）：

1. **HighFailureRate**: 消息失败率超过 10%，持续 2 分钟
2. **HighLatencyP95**: P95 延迟超过 5 秒，持续 3 分钟
3. **KafkaPartitionOldestMessageStuck**: 分区最老消息停留超过 5 分钟

这些告警会根据严重程度发送到不同的邮箱：
- `warning` 级别 → `email-notifications` 接收者
- `critical` 级别 → `email-critical` 接收者（更快发送，1 小时重复一次）

---

## 🚀 快速开始

1. 修改 `alertmanager/alertmanager.yml` 中的 SMTP 配置
2. 修改收件人邮箱地址
3. 运行 `docker-compose up -d` 启动所有服务
4. 访问 http://localhost:9093 查看 Alertmanager 状态
5. 发送测试告警验证配置

配置完成后，当 Kafka 监控系统检测到异常时，会自动发送邮件通知！
