<template>
  <div style="padding: 16px">
    <el-card>
      <template #header>告警规则</template>
      <el-input v-model="rules" type="textarea" :rows="18" placeholder="粘贴 Prometheus rules 内容" />
      <div style="margin-top: 12px; display: flex; gap: 8px">
        <el-button @click="load">刷新</el-button>
        <el-button type="primary" @click="save">保存</el-button>
      </div>
    </el-card>
  </div>
</template>

<script setup>
import { ref } from "vue";
import { api } from "../api/client";
import { ElMessage } from "element-plus";

const rules = ref("");

async function load() {
  const res = await api.get("/alerts/rules", { responseType: "text" });
  rules.value = res.data;
}

async function save() {
  const res = await api.post("/alerts/rules", rules.value, { headers: { "Content-Type": "text/plain" } });
  if (res?.data?.reloaded) {
    ElMessage.success("保存成功，Prometheus 已热加载");
  } else {
    ElMessage.warning("保存成功，但 Prometheus 热加载失败（请重启 prometheus 容器）");
  }
}

load();
</script>
