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
        <el-form-item label="任务ID">
          <el-input v-model="taskId" placeholder="输入 任务ID" style="width: 320px" />
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
          <div style="display: flex; gap: 8px; flex-wrap: wrap; align-items: center">
            <!-- 种类标签 -->
            <el-tag :type="getCategoryTagType(m.category)" v-if="m.category && m.category !== 'unknown'">
              {{ translateCategory(m.category) }}
            </el-tag>
            
            <!-- 系统编号标签 -->
            <el-tag effect="plain">
              {{ (m.systemNo && m.systemNo !== 'unknown') ? m.systemNo : '-' }}
            </el-tag>

            <el-tag type="info" v-if="m.transNo">流水号:{{ m.transNo }}</el-tag>
            <el-tag type="info" v-if="m.workitemId">工作项ID:{{ m.workitemId }}</el-tag>
            <el-tag type="info" effect="dark">{{ m.nodeName || "unknown" }}</el-tag>
            
            <el-tag :type="m.result === 'SUCCESS' ? 'success' : (m.result === 'FAIL' ? 'danger' : 'warning')">
              {{ m.result === 'UNKNOWN' || !m.result ? '-' : (m.result === 'PENDING' ? 'START' : m.result) }}
            </el-tag>
            <el-popover placement="bottom" width="600" trigger="click">
              <template #reference>
                <el-button size="small">详情</el-button>
              </template>
              <div style="max-height: 500px; overflow: auto">
                <template v-if="m.rawJson">
                  <pre style="margin: 0; font-family: monospace; font-size: 12px; background: #f8f9fa; padding: 12px; border-radius: 4px">{{ formatJson(m.rawJson) }}</pre>
                </template>
                <template v-else>
                  <div style="font-weight: bold; margin-bottom: 8px; font-size: 13px; color: #e6a23c">标签数据 (历史消息):</div>
                  <pre style="margin: 0; font-family: monospace; font-size: 12px; background: #f8f9fa; padding: 12px; border-radius: 4px">{{ JSON.stringify(getFilteredLabels(m.labels), null, 2) }}</pre>
                </template>
              </div>
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

function pad3(n) {
  return String(n).padStart(3, "0");
}

function formatLocal(iso) {
  if (!iso) return "-";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())} ${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(
    d.getSeconds()
  )}.${pad3(d.getMilliseconds())}`;
}

/**
 * 格式化 JSON
 */
function formatJson(val) {
  if (!val) return "";
  
  // 处理带 Key 的复合格式：KAFKA_KEY[...]KAFKA_BODY[...]
  if (val.startsWith("KAFKA_KEY[")) {
    const keyEnd = val.indexOf("]KAFKA_BODY[");
    if (keyEnd !== -1) {
      const key = val.substring(10, keyEnd);
      const body = val.substring(keyEnd + 12, val.length - 1);
      try {
        const obj = JSON.parse(body);
        return `${key}\n\n${JSON.stringify(obj, null, 2)}`;
      } catch (e) {
        return `${key}\n\n${body}`;
      }
    }
  }

  try {
    const obj = typeof val === "string" ? JSON.parse(val) : val;
    return JSON.stringify(obj, null, 2);
  } catch (e) {
    return val;
  }
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

/**
 * 翻译种类为中文
 */
function translateCategory(category) {
  const map = {
    'state': '节点状态',
    'competence': '异常节点',
    'tenant_message': '租户数据',
    'tenant-message': '租户数据',
    'MESSAGE': '租户数据',
    'AGENT': 'AGENT租户',
    'SUNYARD': 'SUNYARD租户'
  };
  return map[category] || map[category.toLowerCase()] || category;
}

/**
 * 根据消息种类返回对应的标签颜色类型
 */
function getCategoryTagType(category) {
  if (!category) return "info";
  const cat = category.toLowerCase();
  if (cat === "state") return "";          // 默认蓝色
  if (cat === "competence") return "danger"; // 红色 - 异常
  if (cat === "sunyard") return "success";   // 绿色 - SUNYARD租户
  if (cat === "agent") return "warning";     // 橙色 - AGENT租户
  return "info"; // 灰色 - 其他
}

/**
 * 过滤 labels，移除 busId
 */
function getFilteredLabels(labels) {
  if (!labels) return {};
  const { busId, ...rest } = labels;
  return rest;
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
