<template>
  <div style="padding: 16px">
    <el-card>
      <template #header>积压延时告警 (P-value)</template>
      <el-form :model="latencyConfig" label-width="120px" style="max-width: 600px">
        <el-form-item label="启用告警">
          <el-switch v-model="latencyConfig.enabled" />
        </el-form-item>
        <el-form-item label="分位数 (P)">
          <el-select v-model="latencyConfig.pValue" placeholder="选择分位数">
            <el-option label="P50" :value="0.5" />
            <el-option label="P90" :value="0.9" />
            <el-option label="P95" :value="0.95" />
            <el-option label="P99" :value="0.99" />
          </el-select>
        </el-form-item>
        <el-form-item label="延迟阈值 (秒)">
          <el-input-number v-model="latencyConfig.thresholdSeconds" :precision="2" :step="0.1" :min="0" />
        </el-form-item>
        <el-divider content-position="left">邮件通知设置</el-divider>
        <el-form-item label="SMTP 服务器">
          <el-select
            v-model="latencyConfig.smtpHost"
            filterable
            allow-create
            default-first-option
            placeholder="请选择或输入 SMTP 服务器"
            style="width: 100%"
            @change="handleSmtpChange"
          >
            <el-option
              v-for="item in smtpPresets"
              :key="item.host"
              :label="item.label"
              :value="item.host"
            />
          </el-select>
        </el-form-item>
        <el-form-item label="SMTP 端口">
          <el-input-number v-model="latencyConfig.smtpPort" :min="1" :max="65535" />
        </el-form-item>
        <el-form-item label="发送邮箱">
          <el-input v-model="latencyConfig.senderEmail" placeholder="发送账号" />
        </el-form-item>
        <el-form-item label="授权码/密码">
          <el-input v-model="latencyConfig.emailPassword" type="password" show-password placeholder="邮箱授权码" />
        </el-form-item>
        <el-form-item label="接收邮箱">
          <el-input v-model="latencyConfig.receiverEmail" placeholder="接收账号" />
        </el-form-item>
        <el-form-item>
          <el-button type="primary" @click="saveLatency">保存配置</el-button>
          <el-button @click="testEmail" :loading="testing">发送测试邮件</el-button>
        </el-form-item>
      </el-form>
    </el-card>
  </div>
</template>

<script setup>
import { ref, reactive } from "vue";
import { api } from "../api/client";
import { ElMessage } from "element-plus";

const latencyConfig = reactive({
  enabled: false,
  pValue: 0.95,
  thresholdSeconds: 1.0,
  smtpHost: "smtp.qq.com",
  smtpPort: 465,
  senderEmail: "",
  emailPassword: "",
  receiverEmail: ""
});

const smtpPresets = [
  { label: "QQ 邮箱 (smtp.qq.com)", host: "smtp.qq.com", port: 465 },
  { label: "网易 163 邮箱 (smtp.163.com)", host: "smtp.163.com", port: 465 },
  { label: "网易 126 邮箱 (smtp.126.com)", host: "smtp.126.com", port: 465 },
  { label: "Gmail (smtp.gmail.com)", host: "smtp.gmail.com", port: 587 },
  { label: "Outlook (smtp-mail.outlook.com)", host: "smtp-mail.outlook.com", port: 587 },
  { label: "腾讯企业邮 (smtp.exmail.qq.com)", host: "smtp.exmail.qq.com", port: 465 }
];

function handleSmtpChange(val) {
  const preset = smtpPresets.find(p => p.host === val);
  if (preset) {
    latencyConfig.smtpPort = preset.port;
  }
}

const testing = ref(false);

async function load() {
  const configRes = await api.get("/alerts/latency-config");
  if (configRes.data) {
    Object.assign(latencyConfig, configRes.data);
  }
}

async function saveLatency() {
  const res = await api.post("/alerts/latency-config", latencyConfig);
  if (res.data.success) {
    ElMessage.success("保存延时告警配置成功");
  } else {
    ElMessage.error("保存延时告警配置失败");
  }
}

async function testEmail() {
  testing.value = true;
  try {
    const res = await api.post("/alerts/test-email");
    if (res.data.success) {
      ElMessage.success("测试邮件已发送，请检查收件箱");
    } else {
      ElMessage.error("发送失败: " + res.data.message);
    }
  } catch (e) {
    ElMessage.error("发送出错: " + (e.response?.data?.message || e.message));
  } finally {
    testing.value = false;
  }
}

load();
</script>
