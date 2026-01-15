<template>
  <div style="padding: 16px">
    <el-card>
      <el-form :inline="true" @submit.prevent>
        <el-form-item label="Grafana URL">
          <el-input v-model="url" placeholder="http://localhost:3000" style="width: 360px" />
        </el-form-item>
        <el-form-item>
          <el-button @click="reload">重新加载</el-button>
          <el-button type="primary" @click="openNewTab">新窗口打开</el-button>
        </el-form-item>
      </el-form>
    </el-card>

    <el-card style="margin-top: 16px; padding: 0">
      <el-alert
        v-if="error"
        type="error"
        show-icon
        :closable="false"
        title="Grafana 无法嵌入"
        :description="error"
        style="margin: 16px"
      />
      <iframe
        :key="iframeKey"
        :src="url"
        style="width: 100%; height: calc(100vh - 220px); border: 0"
        @load="onLoad"
        @error="onError"
      />
    </el-card>
  </div>
</template>

<script setup>
import { ref } from "vue";

const url = ref("http://localhost:3000");
const iframeKey = ref(0);
const error = ref("");

function reload() {
  error.value = "";
  iframeKey.value += 1;
}

function openNewTab() {
  window.open(url.value, "_blank", "noopener,noreferrer");
}

function onLoad() {
  error.value = "";
}

function onError() {
  error.value =
    "通常是 Grafana 返回了 X-Frame-Options/CSP 导致 iframe 被浏览器拦截，或 Grafana 未启动。请先确认 http://localhost:3000 可访问，然后重建 grafana 容器后刷新。";
}
</script>
