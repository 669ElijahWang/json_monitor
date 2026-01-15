<template>
  <div style="padding: 16px">
    <el-alert
      v-if="error"
      type="error"
      show-icon
      :closable="false"
      title="仪表盘数据加载失败"
      :description="error"
      style="margin-bottom: 12px"
    />

    <el-row :gutter="16">
      <el-col :span="8">
        <el-card>
          <template #header>近 {{ data.window }} 消息数</template>
          <div style="font-size: 28px; font-weight: 700">{{ data.messages }}</div>
          <div style="color: #666; margin-top: 8px">{{ data.from }} ~ {{ data.to }}</div>
        </el-card>
      </el-col>
      <el-col :span="8">
        <el-card>
          <template #header>成功 / 失败</template>
          <div style="display: flex; gap: 12px; align-items: baseline">
            <div style="font-size: 22px; font-weight: 700; color: #2a7">成功 {{ data.success }}</div>
            <div style="font-size: 22px; font-weight: 700; color: #d33">失败 {{ data.fail }}</div>
          </div>
        </el-card>
      </el-col>
      <el-col :span="8">
        <el-card>
          <template #header>延迟 (ms)</template>
          <div style="display: flex; gap: 12px; flex-wrap: wrap">
            <el-tag type="info">P50 {{ latency.p50Ms ?? "-" }}</el-tag>
            <el-tag type="warning">P95 {{ latency.p95Ms ?? "-" }}</el-tag>
            <el-tag type="danger">P99 {{ latency.p99Ms ?? "-" }}</el-tag>
            <el-tag type="success">AVG {{ latency.avgMs ?? "-" }}</el-tag>
          </div>
        </el-card>
      </el-col>
    </el-row>

    <el-row :gutter="16" style="margin-top: 16px">
      <el-col :span="16">
        <el-card>
          <template #header>吞吐趋势（每分钟）</template>
          <div ref="trendEl" style="height: 360px" />
        </el-card>
      </el-col>
      <el-col :span="8">
        <el-card>
          <template #header>租户 Top</template>
          <div ref="tenantEl" style="height: 360px" />
        </el-card>
      </el-col>
    </el-row>
  </div>
</template>

<script setup>
import { nextTick, onBeforeUnmount, onMounted, reactive, ref } from "vue";
import { api } from "../api/client";
import * as echarts from "echarts";

const data = reactive({ window: "60m", messages: 0, success: 0, fail: 0, from: "-", to: "-" });
const latency = reactive({ window: "60m", count: 0, p50Ms: null, p95Ms: null, p99Ms: null, avgMs: null });
const error = ref("");

const trendEl = ref(null);
const tenantEl = ref(null);
let trendChart;
let tenantChart;
let timer;
let initRetry;

function renderTrend(rows) {
  if (!trendChart) return;
  const x = rows.map((r) => (r.ts || "").slice(11, 16));
  const total = rows.map((r) => r.total || 0);
  const success = rows.map((r) => r.success || 0);
  const fail = rows.map((r) => r.fail || 0);
  const timeout = rows.map((r) => r.timeout || 0);

  trendChart.setOption({
    tooltip: { trigger: "axis" },
    legend: { data: ["总数", "成功", "失败", "超时"] },
    grid: { left: 40, right: 20, top: 30, bottom: 30 },
    xAxis: { type: "category", data: x },
    yAxis: { type: "value" },
    series: [
      { name: "总数", type: "line", smooth: true, data: total },
      { name: "成功", type: "line", smooth: true, data: success },
      { name: "失败", type: "line", smooth: true, data: fail },
      { name: "超时", type: "line", smooth: true, data: timeout }
    ]
  });
}

function renderTenant(rows) {
  if (!tenantChart) return;
  const names = rows.map((r) => r.name);
  const values = rows.map((r) => r.count);

  tenantChart.setOption({
    tooltip: { trigger: "axis" },
    grid: { left: 80, right: 20, top: 20, bottom: 30 },
    xAxis: { type: "value" },
    yAxis: { type: "category", data: names, inverse: true },
    series: [{ type: "bar", data: values }]
  });
}

async function ensureCharts() {
  await nextTick();
  if (!trendEl.value || !tenantEl.value) return;

  if (!trendChart) {
    if (trendEl.value.clientWidth === 0 || trendEl.value.clientHeight === 0) {
      initRetry = window.setTimeout(ensureCharts, 50);
      return;
    }
    trendChart = echarts.init(trendEl.value);
  }
  if (!tenantChart) {
    if (tenantEl.value.clientWidth === 0 || tenantEl.value.clientHeight === 0) {
      initRetry = window.setTimeout(ensureCharts, 50);
      return;
    }
    tenantChart = echarts.init(tenantEl.value);
  }
}

async function loadAll() {
  try {
    await ensureCharts();
    const errors = [];

    try {
      const o = await api.get("/stats/overview", { params: { minutes: 60 } });
      Object.assign(data, o.data);
    } catch (e) {
      errors.push(`overview: ${e?.message || String(e)}`);
    }

    try {
      const l = await api.get("/stats/latency", { params: { minutes: 60 } });
      Object.assign(latency, l.data);
    } catch (e) {
      errors.push(`latency: ${e?.message || String(e)}`);
    }

    try {
      const t = await api.get("/stats/timeseries", { params: { minutes: 60 } });
      renderTrend(t.data || []);
    } catch (e) {
      errors.push(`timeseries: ${e?.message || String(e)}`);
    }

    try {
      const b = await api.get("/stats/breakdown", { params: { minutes: 60, by: "tenant" } });
      renderTenant(b.data || []);
    } catch (e) {
      errors.push(`breakdown: ${e?.message || String(e)}`);
    }

    error.value = errors.length ? errors.join(" | ") : "";
  } catch (e) {
    error.value = e?.message || String(e);
  }
}

onMounted(async () => {
  await loadAll();
  timer = window.setInterval(loadAll, 5000);
  window.addEventListener("resize", onResize);
});

function onResize() {
  trendChart?.resize();
  tenantChart?.resize();
}

onBeforeUnmount(() => {
  if (initRetry) window.clearTimeout(initRetry);
  if (timer) window.clearInterval(timer);
  window.removeEventListener("resize", onResize);
  trendChart?.dispose();
  tenantChart?.dispose();
});
</script>
