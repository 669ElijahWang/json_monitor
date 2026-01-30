<template>
  <div style="padding: 16px">
    <el-alert
      v-if="error"
      type="error"
      show-icon
      :closable="false"
      title="挂起任务加载失败"
      :description="error"
      style="margin-bottom: 12px"
    />

    <el-card>
      <el-form :inline="true" @submit.prevent>
        <el-form-item label="expected(s)">
          <el-input-number v-model="filters.expectedSeconds" :min="1" :max="86400" style="width: 160px" />
        </el-form-item>
        <el-form-item label="window(min)">
          <el-input-number v-model="filters.minutes" :min="1" :max="21600" style="width: 160px" />
        </el-form-item>
        <el-form-item label="size">
          <el-input-number v-model="filters.size" :min="1" :max="500" style="width: 140px" />
        </el-form-item>
        <el-form-item label="taskId contains">
          <el-input v-model="filters.q" placeholder="可选" style="width: 220px" />
        </el-form-item>
        <el-form-item>
          <el-button type="primary" @click="load">刷新</el-button>
        </el-form-item>
        <el-form-item>
          <div style="color: #666">当前挂起：{{ total }}</div>
        </el-form-item>
      </el-form>
    </el-card>

    <el-alert
      v-if="baseHealthError"
      type="warning"
      show-icon
      :closable="false"
      title="基座指标不可用"
      :description="baseHealthError"
      style="margin-top: 12px"
    />

    <el-card v-if="baseHealth" style="margin-top: 16px">
      <template #header>
        <div style="display: flex; align-items: center; justify-content: space-between; gap: 12px">
          <div>基座 Kafka 健康（Prometheus）</div>
          <el-tag :type="statusTagType">{{ statusText }}</el-tag>
        </div>
      </template>

      <el-alert
        v-if="topHint"
        type="error"
        show-icon
        :closable="false"
        title="自动根因建议"
        :description="topHint"
        style="margin-bottom: 12px"
      />

      <div style="display: flex; flex-wrap: wrap; gap: 10px; align-items: center">
        <el-tag>Produce 延迟 {{ formatMs(metrics.produceLatencyMsAvg5m) }}</el-tag>
        <el-tag>磁盘 I/O 等待 {{ formatPercent(metrics.diskIoBusyRatio) }}</el-tag>
        <el-tag>Network Idle {{ formatPercent(metrics.networkProcessorIdleRatio) }}</el-tag>
        <el-tag>URP {{ formatInt(metrics.underReplicatedPartitions) }}</el-tag>
        <el-tag>Controller Switch(1h) {{ formatInt(metrics.controllerSwitches1h) }}</el-tag>
        <el-tag>ISR Shrinks(1h) {{ formatInt(metrics.isrShrinks1h) }}</el-tag>
      </div>

      <div v-if="baseHealth?.suggestions?.length" style="margin-top: 10px; color: #666; line-height: 1.6">
        {{ baseHealth.suggestions.join("；") }}
      </div>

      <div v-if="baseHealth?.missing?.length" style="margin-top: 8px; color: #999">
        未接入：{{ baseHealth.missing.join(", ") }}
      </div>
    </el-card>

    <el-card style="margin-top: 16px">
      <template #header>只有开始节点且超时未进入后续节点</template>
      <el-table :data="rows" style="width: 100%">
        <el-table-column prop="waitSeconds" label="wait" width="120" :formatter="formatWaitCell" />
        <el-table-column prop="startSeenAt" label="startSeenAt" width="210" :formatter="formatIsoCell" />
        <el-table-column prop="tenant" label="tenant" width="120" />
        <el-table-column prop="systemNo" label="system" width="120" />
        <el-table-column prop="startNodeName" label="startNode" width="140" />
        <el-table-column prop="adviseKey" label="eventType" width="160" />
        <el-table-column prop="busId" label="busId" width="140" />
        <el-table-column prop="taskId" label="taskId" min-width="260">
          <template #default="{ row }">
            <div style="display: flex; gap: 8px; align-items: center">
              <el-button size="small" @click="goTrace(row.taskId)">追踪</el-button>
              <span style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace">
                {{ row.taskId }}
              </span>
            </div>
          </template>
        </el-table-column>
      </el-table>
    </el-card>
  </div>
</template>

<script setup>
import { computed, onBeforeUnmount, onMounted, reactive, ref } from "vue";
import { api } from "../api/client";
import { useRouter } from "vue-router";

const router = useRouter();
const error = ref("");
const rows = ref([]);
const total = ref(0);
const filters = reactive({ expectedSeconds: 30, minutes: 60, size: 200, q: "" });
const baseHealth = ref(null);
const baseHealthError = ref("");
const metrics = computed(() => baseHealth.value?.metrics || {});
let timer;
let lastBaseHealthAt = 0;

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

function formatIsoCell(row, column, cellValue) {
  return formatLocal(cellValue);
}

function formatWaitSeconds(sec) {
  if (sec == null) return "-";
  const v = Number(sec);
  if (!Number.isFinite(v) || v <= 0) return "0s";
  const s = Math.floor(v);
  const ss = s % 60;
  const mmTotal = Math.floor(s / 60);
  const mm = mmTotal % 60;
  const hh = Math.floor(mmTotal / 60);
  if (hh > 0) return `${hh}h${mm}m${ss}s`;
  if (mm > 0) return `${mm}m${ss}s`;
  return `${ss}s`;
}

function formatWaitCell(row, column, cellValue) {
  return formatWaitSeconds(cellValue);
}

function goTrace(taskId) {
  if (!taskId) return;
  router.push({ path: "/trace", query: { taskId } });
}

function formatMs(v) {
  if (v == null) return "-";
  const n = Number(v);
  if (!Number.isFinite(n)) return String(v);
  return `${n.toFixed(n >= 100 ? 0 : 1)}ms`;
}

function formatPercent(v) {
  if (v == null) return "-";
  const n = Number(v);
  if (!Number.isFinite(n)) return String(v);
  return `${(n * 100).toFixed(0)}%`;
}

function formatInt(v) {
  if (v == null) return "-";
  const n = Number(v);
  if (!Number.isFinite(n)) return String(v);
  return String(Math.round(n));
}

const statusText = computed(() => {
  const s = baseHealth.value?.status;
  if (s === "CRITICAL") return "亚健康";
  if (s === "WARN") return "关注";
  if (s === "OK") return "正常";
  return "未知";
});

const statusTagType = computed(() => {
  const s = baseHealth.value?.status;
  if (s === "CRITICAL") return "danger";
  if (s === "WARN") return "warning";
  if (s === "OK") return "success";
  return "info";
});

const topHint = computed(() => {
  const disk = Number(metrics.value?.diskIoBusyRatio);
  if (total.value > 50 && Number.isFinite(disk) && disk > 0.8) {
    return `当前挂起任务 > 50 且磁盘 I/O 等待 > 80%，建议检查大报文或硬件负载（磁盘 I/O 等待 ${formatPercent(disk)}）`;
  }
  return "";
});

async function loadBaseHealth() {
  const now = Date.now();
  if (now - lastBaseHealthAt < 15000) return;
  lastBaseHealthAt = now;
  baseHealthError.value = "";
  try {
    const res = await api.get("/base/health");
    baseHealth.value = res.data || null;
  } catch (e) {
    baseHealth.value = null;
    baseHealthError.value = e?.response?.data?.message || e?.message || String(e);
  }
}

async function load() {
  error.value = "";
  try {
    const res = await api.get("/tasks/pending", { params: { ...filters } });
    rows.value = res.data?.content || [];
    total.value = Number(res.data?.totalElements || rows.value.length || 0);
    await loadBaseHealth();
  } catch (e) {
    rows.value = [];
    total.value = 0;
    error.value = e?.response?.data?.message || e?.message || String(e);
  }
}

onMounted(async () => {
  await load();
  timer = window.setInterval(load, 5000);
});

onBeforeUnmount(() => {
  if (timer) window.clearInterval(timer);
});
</script>
