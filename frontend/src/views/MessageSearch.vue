<template>
  <div style="padding: 16px">
    <el-alert
      v-if="error"
      type="warning"
      show-icon
      :closable="false"
      title="消息查询不可用"
      :description="error"
      style="margin-bottom: 12px"
    />
    <el-card>
      <el-form :inline="true" @submit.prevent>
        <el-form-item label="任务ID">
          <el-input v-model="filters.q" placeholder="输入 任务ID" style="width: 240px" />
        </el-form-item>
        <el-form-item label="种类">
          <el-select v-model="filters.category" placeholder="请选择种类" clearable filterable style="width: 160px">
            <el-option label="节点状态" value="state" />
            <el-option label="异常节点" value="competence" />
            <el-option label="租户数据" value="__ALL_TENANTS__" />
            <el-option label="SUNYARD租户" value="SUNYARD" />
            <el-option label="AGENT租户" value="AGENT" />
          </el-select>
        </el-form-item>
        <el-form-item label="节点">
          <el-input v-model="filters.nodeName" placeholder="AT1" style="width: 160px" />
        </el-form-item>
        <el-form-item label="窗口(min)">
          <el-input-number v-model="filters.minutes" :min="1" :max="21600" style="width: 140px" />
        </el-form-item>
        <el-form-item>
          <el-button type="primary" @click="load">查询</el-button>
        </el-form-item>
      </el-form>
    </el-card>

    <el-card style="margin-top: 16px">
      <el-table :data="rows" row-key="id" style="width: 100%">
        <el-table-column prop="createdAt" label="时间" width="210" :formatter="formatLocalCell" />
        <el-table-column prop="taskId" label="任务ID" width="200" />
        <el-table-column label="系统编号" width="120">
          <template #default="{ row }">
            {{ (row.systemNo === 'unknown' || !row.systemNo) ? '-' : row.systemNo }}
          </template>
        </el-table-column>
        <el-table-column prop="transNo" label="流水号" width="130" />
        <el-table-column prop="workitemId" label="工作项ID" width="130" />
        <el-table-column label="种类" width="120">
          <template #default="{ row }">
            <el-tag :type="getCategoryTagType(row.category)" size="small">
              {{ translateCategory(row.category) }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="nodeName" label="节点" width="140" />
        <el-table-column label="结果" width="140">
          <template #default="{ row }">
            <span v-if="row.category && row.category.toLowerCase() === 'state'">
              {{ row.result === 'PENDING' ? 'START' : (row.result === 'UNKNOWN' ? '-' : row.result) }}
            </span>
            <span v-else>{{ (row.result === 'UNKNOWN' || !row.result) ? '-' : row.result }}</span>
          </template>
        </el-table-column>

        <el-table-column label="详情">
          <template #default="{ row }">
            <el-popover placement="left" width="600" trigger="click">
              <template #reference>
                <el-button size="small">详情</el-button>
              </template>
              <div style="max-height: 500px; overflow: auto">
                <template v-if="row.rawJson">
                  <div style="font-weight: bold; margin-bottom: 8px; font-size: 13px; color: #409eff">原始 JSON 数据:</div>
                  <pre style="margin: 0; font-family: monospace; font-size: 12px; background: #f8f9fa; padding: 12px; border-radius: 4px">{{ formatJson(row.rawJson) }}</pre>
                </template>
                <template v-else>
                  <div style="font-weight: bold; margin-bottom: 8px; font-size: 13px; color: #e6a23c">标签数据 (历史消息):</div>
                  <pre style="margin: 0; font-family: monospace; font-size: 12px; background: #f8f9fa; padding: 12px; border-radius: 4px">{{ JSON.stringify(getFilteredLabels(row.labels), null, 2) }}</pre>
                </template>
              </div>
            </el-popover>
          </template>
        </el-table-column>
      </el-table>

      <div style="display: flex; justify-content: flex-end; margin-top: 12px">
        <el-pagination
          layout="prev, pager, next, sizes, total"
          :pager-count="11"
          :page-size="page.size"
          :page-sizes="[10, 20, 50]"
          :total="page.total"
          :current-page="page.page + 1"
          @update:page-size="onSize"
          @update:current-page="onPage"
        />
      </div>
    </el-card>
  </div>
</template>

<script setup>
import { reactive, ref } from "vue";
import { api } from "../api/client";

const filters = reactive({ q: "", category: "", nodeName: "", minutes: 60 });
const rows = ref([]);
const page = reactive({ page: 0, size: 20, total: 0 });
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

function formatLocalCell(row, column, cellValue) {
  return formatLocal(cellValue);
}

/**
 * 翻译种类为中文
 */
function translateCategory(category) {
  if (!category) return "未知";
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
 * @param category 消息种类 (state/competence/SUNYARD/AGENT等)
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
 * 格式化 JSON
 */
function formatJson(val) {
  if (!val) return "";
  
  // 处理带 Key 的复合格式：KAFKA_KEY[...]KAFKA_BODY[...]
  if (val.startsWith("KAFKA_KEY[")) {
    const keyEnd = val.indexOf("]KAFKA_BODY[");
    if (keyEnd !== -1) {
      const key = val.substring(10, keyEnd); // "KAFKA_KEY[".length
      const body = val.substring(keyEnd + 12, val.length - 1); // "]KAFKA_BODY[".length
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

/**
 * 过滤 labels，移除 busId
 */
function getFilteredLabels(labels) {
  if (!labels) return {};
  const { busId, ...rest } = labels;
  return rest;
}

async function load() {
  error.value = "";
  try {
    const res = await api.get("/messages/search", {
      params: { ...filters, page: page.page, size: page.size }
    });
    rows.value = res.data.content || [];
    page.total = res.data.totalElements || 0;
  } catch (e) {
    rows.value = [];
    page.total = 0;
    const msg = e?.response?.data?.message || e?.message || String(e);
    error.value = msg;
  }
}

function onPage(p) {
  page.page = p - 1;
  load();
}

function onSize(s) {
  page.size = s;
  page.page = 0;
  load();
}

load();
</script>
