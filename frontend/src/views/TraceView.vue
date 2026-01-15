<template>
  <div style="padding: 16px">
    <el-card>
      <el-form :inline="true" @submit.prevent>
        <el-form-item label="taskId">
          <el-input v-model="taskId" placeholder="输入 taskId" style="width: 320px" />
        </el-form-item>
        <el-form-item>
          <el-button type="primary" @click="load">查询链路</el-button>
        </el-form-item>
      </el-form>
    </el-card>

    <el-card style="margin-top: 16px">
      <template #header>按时间排序的节点路径</template>
      <el-timeline>
        <el-timeline-item v-for="m in messages" :key="m.id" :timestamp="m.createdAt">
          <div style="display: flex; gap: 12px; flex-wrap: wrap">
            <el-tag>{{ m.systemNo || "unknown" }}</el-tag>
            <el-tag type="info">{{ m.nodeName || "unknown" }}</el-tag>
            <el-tag :type="m.result === 'SUCCESS' ? 'success' : (m.result === 'FAIL' ? 'danger' : 'warning')">
              {{ m.result }}
            </el-tag>
          </div>
        </el-timeline-item>
      </el-timeline>
    </el-card>
  </div>
</template>

<script setup>
import { ref } from "vue";
import { api } from "../api/client";

const taskId = ref("");
const messages = ref([]);

async function load() {
  if (!taskId.value) {
    messages.value = [];
    return;
  }
  const res = await api.get(`/messages/${encodeURIComponent(taskId.value)}`);
  messages.value = res.data;
}
</script>

