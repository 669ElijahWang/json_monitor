<template>
  <div style="padding: 16px">
    <el-alert
      v-if="error"
      type="warning"
      show-icon
      :closable="false"
      title="链路追踪不可用"
      :description="error"
      style="margin-bottom: 12px"
    />
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
        <el-timeline-item v-for="m in messages" :key="m.id" :timestamp="formatLocal(m.createdAt)">
          <div style="display: flex; gap: 12px; flex-wrap: wrap">
            <el-tag>{{ m.systemNo || "unknown" }}</el-tag>
            <el-tag type="info">{{ m.nodeName || "unknown" }}</el-tag>
            <el-tag :type="m.result === 'SUCCESS' ? 'success' : (m.result === 'FAIL' ? 'danger' : 'warning')">
              {{ m.result }}
            </el-tag>
            <el-popover placement="bottom" width="520" trigger="click">
              <template #reference>
                <el-button size="small">Labels</el-button>
              </template>
              <pre style="max-height: 480px; overflow: auto; margin: 0">{{ JSON.stringify(m.labels || {}, null, 2) }}</pre>
            </el-popover>
          </div>
        </el-timeline-item>
      </el-timeline>
    </el-card>
  </div>
</template>

<script setup>
import { onMounted, ref, watch } from "vue";
import { api } from "../api/client";
import { useRoute } from "vue-router";

const route = useRoute();
const taskId = ref("");
const messages = ref([]);
const error = ref("");

function pad2(n) {
  return String(n).padStart(2, "0");
}

function formatLocal(iso) {
  if (!iso) return "-";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())} ${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(
    d.getSeconds()
  )}`;
}

async function load() {
  error.value = "";
  if (!taskId.value) {
    messages.value = [];
    return;
  }
  try {
    const res = await api.get(`/messages/${encodeURIComponent(taskId.value)}`);
    messages.value = res.data || [];
  } catch (e) {
    messages.value = [];
    const msg = e?.response?.data?.message || e?.message || String(e);
    error.value = msg;
  }
}

onMounted(() => {
  const q = route.query?.taskId;
  if (q) {
    taskId.value = String(q);
    load();
  }
});

watch(
  () => route.query?.taskId,
  (v) => {
    if (!v) return;
    const next = String(v);
    if (next === taskId.value) return;
    taskId.value = next;
    load();
  }
);
</script>
