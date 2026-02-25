<template>
  <div class="dashboard-container">
    <el-alert
      v-if="error"
      type="error"
      show-icon
      :closable="false"
      title="仪表盘数据加载失败"
      :description="error"
      class="error-alert"
    />

    <el-row :gutter="20">
      <el-col :span="8">
        <el-card class="hover-card stats-card" shadow="never">
          <div class="stats-header">
            <div class="stats-header-left">
              <span>近</span>
              <el-select
                v-model="selectedMinutes"
                size="small"
                class="time-select"
                @change="onTimeRangeChange"
              >
                <el-option
                  v-for="opt in timeOptions"
                  :key="opt.value"
                  :label="opt.label"
                  :value="opt.value"
                />
              </el-select>
              <span>消息数</span>
            </div>
            <el-icon class="stats-icon text-primary"><Message /></el-icon>
          </div>
          <div class="stats-value text-primary">{{ data.messages }}</div>
          <div class="stats-footer">{{ formatLocal(data.from) }} ~ {{ formatLocal(data.to) }}</div>
        </el-card>
      </el-col>
      <el-col :span="8">
        <el-card class="hover-card stats-card" shadow="never">
           <div class="stats-header">
            <span>消息分布</span>
            <el-icon class="stats-icon text-success"><Check /></el-icon>
          </div>
          <div class="stats-content-row" style="gap: 16px; align-items: flex-start">
            <div class="stats-item">
              <div class="stats-label text-primary">节点状态</div>
              <div class="stats-sub-value text-primary">{{ data.state }}</div>
            </div>
             <div class="stats-item">
              <div class="stats-label text-danger">异常节点</div>
              <div class="stats-sub-value text-danger">{{ data.competence }}</div>
            </div>
             <div class="stats-item">
              <div class="stats-label text-warning">租户数据</div>
              <div class="stats-sub-value text-warning">{{ data.tenant }}</div>
            </div>
          </div>
        </el-card>
      </el-col>
      <el-col :span="8">
        <el-card class="hover-card stats-card" shadow="never">
           <div class="stats-header">
             <span>积压延迟监控</span>
            <el-icon class="stats-icon text-warning"><Timer /></el-icon>
          </div>
           <div class="latency-desc-list">
             <div class="latency-desc-item">
               <span class="dot blue"></span> 50% 的消息积压在 <span class="highlight">{{ latency.lag?.p50S ?? "-" }}</span> 秒之内
             </div>
             <div class="latency-desc-item">
               <span class="dot orange"></span> 95% 的消息积压在 <span class="highlight">{{ latency.lag?.p95S ?? "-" }}</span> 秒之内
             </div>
             <div class="latency-desc-item">
               <span class="dot red"></span> 99% 的消息积压在 <span class="highlight">{{ latency.lag?.p99S ?? "-" }}</span> 秒之内
             </div>
           </div>
        </el-card>
      </el-col>
    </el-row>

    <el-row :gutter="20" style="margin-top: 20px">
      <el-col :span="16">
        <el-card class="hover-card" shadow="never">
          <template #header>
            <div class="card-header">
              <div class="card-header-left">
                <span>吞吐趋势（每分钟）</span>
                <el-tag v-if="lastUpdateTime" size="small" type="info" effect="plain" class="update-tag">
                  更新于 {{ lastUpdateTime }}
                </el-tag>
              </div>
              <el-icon><TrendCharts /></el-icon>
            </div>
          </template>
          <div ref="trendEl" style="height: 380px" />
        </el-card>
      </el-col>
      <el-col :span="8">
        <el-card class="hover-card" shadow="never">
          <template #header>
             <div class="card-header">
              <span>租户 Top</span>
              <el-icon><User /></el-icon>
            </div>
          </template>
          <div ref="tenantEl" style="height: 380px" />
        </el-card>
      </el-col>
    </el-row>

    <!-- 状态监控行 -->
    <el-row :gutter="20" style="margin-top: 20px">
      <el-col :span="12">
        <el-card class="hover-card" shadow="never">
          <template #header>
            <div class="card-header">
              <span>Kafka接收状态分布（watchState）</span>
              <el-icon><DataAnalysis /></el-icon>
            </div>
          </template>
          <div ref="watchStateEl" style="height: 320px" />
        </el-card>
      </el-col>
      <el-col :span="12">
        <el-card class="hover-card" shadow="never">
          <template #header>
            <div class="card-header">
              <span>消息类型分布</span>
              <el-icon><Tickets /></el-icon>
            </div>
          </template>
          <div ref="messageTypeEl" style="height: 320px" />
        </el-card>
      </el-col>
    </el-row>

    <el-row :gutter="20" style="margin-top: 20px">
      <el-col :span="12">
        <el-card class="hover-card" shadow="never">
          <template #header>
            <div class="card-header">
              <span>消息大小分布</span>
              <el-icon><PieChart /></el-icon>
            </div>
          </template>
          <div ref="sizeEl" style="height: 380px" />
        </el-card>
      </el-col>
      <el-col :span="12">
        <el-card class="hover-card" shadow="never">
          <template #header>
            <div class="card-header">
              <span>大报文轨迹</span>
              <el-icon><Warning /></el-icon>
            </div>
          </template>
          <el-table :data="bigMessages" style="width: 100%" size="small" :header-cell-style="{background: '#f5f7fa'}">
            <el-table-column prop="observedAt" label="Time" width="160" :formatter="formatIsoCell" show-overflow-tooltip />
            <el-table-column prop="sizeBytes" label="Size" width="100" :formatter="formatBytesCell" />
            <el-table-column prop="topic" label="Topic" show-overflow-tooltip />
             <el-table-column label="Action" width="80" align="center">
              <template #default="{ row }">
                 <el-button link type="primary" size="small" @click="goTrace(row.taskId)">追踪</el-button>
              </template>
            </el-table-column>
          </el-table>
        </el-card>
      </el-col>
    </el-row>
  </div>
</template>

<script setup>
import { nextTick, onBeforeUnmount, onMounted, reactive, ref } from "vue";
import { api } from "../api/client";
import * as echarts from "echarts";
import { useRouter } from "vue-router";
import { Message, Check, Timer, TrendCharts, User, PieChart, Warning, DataAnalysis, Tickets } from "@element-plus/icons-vue";

const router = useRouter();

// 时间选择选项
const timeOptions = [
  { label: "15 分钟", value: 15 },
  { label: "30 分钟", value: 30 },
  { label: "60 分钟", value: 60 },
  { label: "3 小时", value: 180 },
  { label: "6 小时", value: 360 },
  { label: "12 小时", value: 720 },
  { label: "24 小时", value: 1440 }
];

const selectedMinutes = ref(60);

const data = reactive({ window: "60m", messages: 0, state: 0, competence: 0, tenant: 0, from: "-", to: "-" });
const latency = reactive({ window: "60m", lag: null, internal: null, e2e: null });
const error = ref("");

const trendEl = ref(null);
const tenantEl = ref(null);
const sizeEl = ref(null);
const watchStateEl = ref(null);
const messageTypeEl = ref(null);
const bigMessages = ref([]);
const lastUpdateTime = ref("");
let trendChart;
let tenantChart;
let sizeChart;
let watchStateChart;
let messageTypeChart;
let timer;
let initRetry;

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

function formatHHmm(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return (iso || "").slice(11, 16);
  return `${pad2(d.getHours())}:${pad2(d.getMinutes())}`;
}

function formatHHmmss(date) {
  if (!date) return "";
  const d = date instanceof Date ? date : new Date(date);
  if (Number.isNaN(d.getTime())) return "";
  return `${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())}`;
}

function formatBytes(n) {
  const v = Number(n);
  if (!Number.isFinite(v) || v <= 0) return "0B";
  const units = ["B", "KB", "MB", "GB"];
  let x = v;
  let i = 0;
  while (x >= 1024 && i < units.length - 1) {
    x /= 1024;
    i++;
  }
  const digits = i === 0 ? 0 : x >= 100 ? 0 : x >= 10 ? 1 : 2;
  return `${x.toFixed(digits)}${units[i]}`;
}

function formatBytesCell(row, column, cellValue) {
  return formatBytes(cellValue);
}

function renderSize(resp) {
  if (!sizeChart) return;
  const buckets = resp?.buckets || [];
  const x = [];
  const y = [];
  let prevLe = null;
  for (const b of buckets) {
    const le = String(b.le ?? "");
    const cnt = Number(b.count ?? 0);
    if (le === "+Inf") {
      const lb = prevLe == null ? "0B" : formatBytes(prevLe);
      x.push(`>${lb}`);
    } else {
      const upper = Number(le);
      if (Number.isFinite(upper)) {
        // 第一个桶用 "≤X" 表示，后续桶用 "prevLe~X" 区间表示
        if (prevLe == null) {
          x.push(`≤${formatBytes(upper)}`);
        } else {
          x.push(`${formatBytes(prevLe)}~${formatBytes(upper)}`);
        }
        prevLe = upper;
      } else {
        x.push(le || "-");
      }
    }
    y.push(Number.isFinite(cnt) ? cnt : 0);
  }

  sizeChart.setOption({
    tooltip: { trigger: "axis" },
    grid: { left: 50, right: 20, top: 20, bottom: 80 },
    xAxis: { type: "category", data: x, axisLabel: { interval: 0, rotate: 35, color: "#666", fontSize: 11 } },
    yAxis: { type: "value", splitLine: { lineStyle: { type: "dashed", color: "#eee" } } },
    series: [
      {
        type: "bar",
        data: y,
        itemStyle: { borderRadius: [4, 4, 0, 0], color: "#409eff" },
        barMaxWidth: 40
      }
    ]
  });
}

function goTrace(taskId) {
  if (!taskId) return;
  router.push({ path: "/trace", query: { taskId } });
}

function renderTrend(rows) {
  if (!trendChart) return;
  const x = rows.map((r) => formatHHmm(r.ts));
  
  // 固定显示三个系列：State、Competence、Tenant
  const series = [
    {
      name: '节点状态',
      type: 'line',
      smooth: true,
      showSymbol: true,
      symbolSize: 4,
      itemStyle: { color: '#409eff' }, // 蓝色
      areaStyle: { color: 'rgba(64, 158, 255, 0.1)' },
      data: rows.map(r => r.state || 0)
    },
    {
      name: '异常节点',
      type: 'line',
      smooth: true,
      showSymbol: true,
      symbolSize: 4,
      itemStyle: { color: '#f56c6c' }, // 红色
      areaStyle: { color: 'rgba(245, 108, 108, 0.1)' },
      data: rows.map(r => r.competence || 0)
    },
    {
      name: '租户数据',
      type: 'line',
      smooth: true,
      showSymbol: true,
      symbolSize: 4,
      itemStyle: { color: '#e6a23c' }, // 橙色
      areaStyle: { color: 'rgba(230, 162, 60, 0.1)' },
      data: rows.map(r => r.tenant || 0)
    }
  ];

  trendChart.setOption({
    tooltip: { 
      trigger: 'axis',
      formatter: (params) => {
        let result = `<div style="font-weight:600;margin-bottom:4px">${params[0].axisValue}</div>`;
        let total = 0;
        params.forEach(p => {
          total += p.value || 0;
          result += `<div style="display:flex;justify-content:space-between;gap:12px">
            <span>${p.marker} ${p.seriesName}</span>
            <span style="font-weight:600">${p.value || 0}</span>
          </div>`;
        });
        result += `<div style="border-top:1px solid #eee;margin-top:4px;padding-top:4px;display:flex;justify-content:space-between">
          <span>总计</span><span style="font-weight:600">${total}</span>
        </div>`;
        return result;
      }
    },
    legend: { data: ['节点状态', '异常节点', '租户数据'], top: 0 },
    grid: { left: 50, right: 20, top: 40, bottom: 30 },
    xAxis: { 
      type: 'category', 
      data: x, 
      axisLine: { lineStyle: { color: '#ccc' } },
      axisLabel: { color: '#666' }
    },
    yAxis: { 
      type: 'value', 
      minInterval: 1,
      splitLine: { lineStyle: { type: 'dashed', color: '#eee' } }
    },
    series: series
  }, true);
}

function renderTenant(rows) {
  if (!tenantChart) return;
  const names = rows.map((r) => r.name);
  const values = rows.map((r) => r.count);

  tenantChart.setOption({
    tooltip: { trigger: "axis" },
    grid: { left: 100, right: 30, top: 10, bottom: 20 },
    xAxis: { type: "value", splitLine: { show: false } },
    yAxis: { type: "category", data: names, inverse: true, axisTick: { show: false }, axisLine: { show: false } },
    series: [
      {
        type: "bar",
        data: values,
        itemStyle: { borderRadius: [0, 4, 4, 0], color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [{ offset: 0, color: "#83bff6" }, { offset: 1, color: "#188df0" }]) },
        label: { show: true, position: "right" }
      }
    ]
  });
}

async function ensureCharts() {
  await nextTick();
  if (!trendEl.value || !tenantEl.value || !sizeEl.value) return;

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
  if (!sizeChart) {
    if (sizeEl.value.clientWidth === 0 || sizeEl.value.clientHeight === 0) {
      initRetry = window.setTimeout(ensureCharts, 50);
      return;
    }
    sizeChart = echarts.init(sizeEl.value);
  }
  // 状态图表初始化
  if (!watchStateChart && watchStateEl.value?.clientWidth > 0 && watchStateEl.value?.clientHeight > 0) {
    watchStateChart = echarts.init(watchStateEl.value);
  }
  if (!messageTypeChart && messageTypeEl.value?.clientWidth > 0 && messageTypeEl.value?.clientHeight > 0) {
    messageTypeChart = echarts.init(messageTypeEl.value);
  }
}

// watchState状态颜色映射
const watchStateColors = {
  '0': '#909399',  // 初始节点 - 灰色
  '1': '#409eff',  // 获取 - 蓝色
  '2': '#e6a23c',  // 处理中 - 橙色
  '4': '#f56c6c',  // 处理失败 - 红色
  '5': '#67c23a',  // 处理完成 - 绿色
};

function renderPieChart(chart, data, colorMap, title) {
  if (!chart) return;
  const pieData = data.map(item => ({
    name: item.desc || item.name,
    value: item.count,
    itemStyle: { color: colorMap[item.name] || '#409eff' }
  }));
  
  chart.setOption({
    tooltip: {
      trigger: 'item',
      formatter: '{b}: {c} ({d}%)'
    },
    legend: {
      orient: 'vertical',
      right: 10,
      top: 'center',
      formatter: (name) => {
        const item = pieData.find(d => d.name === name);
        return item ? `${name}: ${item.value}` : name;
      }
    },
    series: [{
      type: 'pie',
      radius: ['40%', '70%'],
      center: ['40%', '50%'],
      avoidLabelOverlap: true,
      itemStyle: {
        borderRadius: 8,
        borderColor: '#fff',
        borderWidth: 2
      },
      label: {
        show: false,
        position: 'center'
      },
      emphasis: {
        label: {
          show: true,
          fontSize: 16,
          fontWeight: 'bold'
        }
      },
      labelLine: {
        show: false
      },
      data: pieData
    }]
  });
}

const typeNameMap = {
  'STATE': '节点状态',
  'state': '节点状态',
  'TENANT_MESSAGE': '租户数据',
  'tenant_message': '租户数据',
  'COMPETENCE': '异常节点',
  'competence': '异常节点'
};

function renderBarChart(chart, data) {
  if (!chart) return;
  const names = data.map(r => typeNameMap[r.name] || r.name);
  const values = data.map(r => r.count);
  const colors = ['#409eff', '#67c23a', '#e6a23c', '#f56c6c', '#909399'];
  
  chart.setOption({
    tooltip: { trigger: 'axis' },
    grid: { left: 100, right: 30, top: 20, bottom: 30 },
    xAxis: { type: 'value', splitLine: { lineStyle: { type: 'dashed', color: '#eee' } } },
    yAxis: { type: 'category', data: names, inverse: true, axisTick: { show: false }, axisLine: { show: false } },
    series: [{
      type: 'bar',
      data: values.map((v, i) => ({
        value: v,
        itemStyle: { color: colors[i % colors.length], borderRadius: [0, 4, 4, 0] }
      })),
      label: { show: true, position: 'right' }
    }]
  });
}

async function loadAll() {
  try {
    await ensureCharts();
    const errors = [];
    const mins = selectedMinutes.value;
    
    // 使用实时统计接口（内存数据，无 Prometheus 延迟）
    try {
      const o = await api.get("/stats/realtime/overview", { params: { minutes: mins } });
      Object.assign(data, o.data);
    } catch (e) {
      errors.push(`overview: ${e?.message || String(e)}`);
    }

    try {
      const l = await api.get("/stats/realtime/latency", { params: { minutes: mins } });
      Object.assign(latency, l.data);
    } catch (e) {
      errors.push(`latency: ${e?.message || String(e)}`);
    }

    // 使用实时吞吐趋势接口
    try {
      const t = await api.get("/stats/realtime/timeseries", { params: { minutes: mins } });
      renderTrend(t.data || []);
      // 更新最后刷新时间
      lastUpdateTime.value = formatHHmmss(new Date());
    } catch (e) {
      errors.push(`timeseries: ${e?.message || String(e)}`);
    }

    try {
      const b = await api.get("/stats/realtime/tenants", { params: { minutes: mins } });
      renderTenant(b.data || []);
    } catch (e) {
      errors.push(`breakdown: ${e?.message || String(e)}`);
    }

    try {
      const s = await api.get("/stats/message-size", { params: { minutes: mins } });
      renderSize(s.data || {});
    } catch (e) {
      errors.push(`messageSize: ${e?.message || String(e)}`);
    }

    try {
      const bm = await api.get("/stats/big-messages", { params: { limit: 50 } });
      bigMessages.value = bm.data?.content || [];
    } catch (e) {
      bigMessages.value = [];
      errors.push(`bigMessages: ${e?.message || String(e)}`);
    }

    error.value = errors.length ? errors.join(" | ") : "";

    // 加载实时状态统计数据（使用实时接口，数据稳定不浮动）
    try {
      const ws = await api.get("/stats/realtime/watch-states");
      if (ws.data?.length > 0) {
        renderPieChart(watchStateChart, ws.data, watchStateColors, 'Kafka接收状态');
      }
    } catch (e) {
      console.warn('watchStates:', e?.message || String(e));
    }

    try {
      const mt = await api.get("/stats/realtime/message-types");
      if (mt.data?.length > 0) {
        renderBarChart(messageTypeChart, mt.data);
      }
    } catch (e) {
      console.warn('messageTypes:', e?.message || String(e));
    }
  } catch (e) {
    error.value = e?.message || String(e);
  }
}

// 时间范围变化处理
function onTimeRangeChange() {
  loadAll();
}

onMounted(async () => {
  await loadAll();
  // 实时刷新：每2秒刷新一次，确保有消息时立即显示
  timer = window.setInterval(loadAll, 2000);
  window.addEventListener("resize", onResize);
});

function onResize() {
  trendChart?.resize();
  tenantChart?.resize();
  sizeChart?.resize();
  watchStateChart?.resize();
  messageTypeChart?.resize();
}

onBeforeUnmount(() => {
  if (initRetry) window.clearTimeout(initRetry);
  if (timer) window.clearInterval(timer);
  window.removeEventListener("resize", onResize);
  trendChart?.dispose();
  tenantChart?.dispose();
  sizeChart?.dispose();
  watchStateChart?.dispose();
  messageTypeChart?.dispose();
});
</script>

<style scoped>
.dashboard-container {
  padding: 0;
}
.error-alert {
  margin-bottom: 20px;
}
.stats-card {
  height: 100%;
  border: none;
  border-radius: var(--border-radius, 12px);
}
.stats-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-size: 14px;
  color: var(--text-secondary, #909399);
  margin-bottom: 12px;
}
.stats-header-left {
  display: flex;
  align-items: center;
  gap: 6px;
}
.time-select {
  width: 100px;
}
.time-select :deep(.el-input__wrapper) {
  background: rgba(64, 158, 255, 0.08);
  border: 1px solid rgba(64, 158, 255, 0.2);
  border-radius: 6px;
  box-shadow: none;
}
.time-select :deep(.el-input__inner) {
  color: var(--primary-color, #409eff);
  font-weight: 500;
}
.stats-icon {
  font-size: 20px;
  background: rgba(0,0,0,0.03);
  padding: 8px;
  border-radius: 8px;
}
.stats-value {
  font-size: 32px;
  font-weight: 700;
  line-height: 1.2;
}
.stats-sub-value {
  font-size: 24px;
  font-weight: 700;
}
.stats-footer {
  margin-top: 12px;
  font-size: 12px;
  color: var(--text-secondary, #909399);
}
.stats-content-row {
  display: flex;
  align-items: center;
  gap: 24px;
}
.stats-item {
  display: flex;
  flex-direction: column;
}
.stats-label {
  font-size: 12px;
  margin-bottom: 4px;
}
.text-primary { color: var(--primary-color, #409eff); }
.text-success { color: #67c23a; }
.text-danger { color: #f56c6c; }
.text-warning { color: #e6a23c; }

.latency-desc-list {
  display: flex;
  flex-direction: column;
  gap: 16px;
  padding: 8px 0;
}
.latency-desc-item {
  font-size: 14px;
  color: #606266;
  display: flex;
  align-items: center;
  gap: 10px;
}
.latency-desc-item .highlight {
  font-family: Monaco, Consolas, monospace;
  font-weight: 700;
  color: #303133;
  font-size: 16px;
  margin: 0 4px;
}
.dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  display: inline-block;
}
.dot.blue { background: #409eff; }
.dot.orange { background: #e6a23c; }
.dot.red { background: #f56c6c; }
.card-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  font-weight: 600;
}
.card-header-left {
  display: flex;
  align-items: center;
  gap: 8px;
}
.update-tag {
  font-size: 11px;
  padding: 2px 6px;
  font-weight: normal;
}
</style>
