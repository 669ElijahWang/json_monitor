<template>
  <div style="padding: 16px">
    <el-alert
      v-if="error"
      type="error"
      show-icon
      :closable="false"
      title="积压查询失败"
      :description="error"
      style="margin-bottom: 12px"
    />

    <el-card>
      <el-form :inline="true" @submit.prevent>
        <el-form-item label="topic">
          <el-input v-model="filters.topic" placeholder="monitor-topic" style="width: 220px" />
        </el-form-item>
        <el-form-item label="groupId">
          <el-input v-model="filters.groupId" placeholder="monitor-observer-group" style="width: 220px" />
        </el-form-item>
        <el-form-item label="limit">
          <el-input-number v-model="filters.limit" :min="1" :max="200" style="width: 140px" />
        </el-form-item>
        <el-form-item>
          <el-button type="primary" @click="refreshAll">刷新</el-button>
        </el-form-item>
        <el-form-item>
          <div style="color: #666">总积压：{{ snapshot.totalLag ?? "-" }}</div>
        </el-form-item>
      </el-form>
    </el-card>

    <el-row :gutter="16" style="margin-top: 16px">
      <el-col :span="10">
        <el-card>
          <template #header>分区积压</template>
          <el-table :data="snapshot.partitions || []" style="width: 100%">
            <el-table-column prop="partition" label="partition" width="110" />
            <el-table-column prop="committedOffset" label="committed" width="140" />
            <el-table-column prop="endOffset" label="end" width="140" />
            <el-table-column prop="lag" label="lag" />
            <el-table-column prop="oldestAgeMs" label="Oldest Message Age" width="170" :formatter="formatAgeCell" />
          </el-table>
        </el-card>
      </el-col>

      <el-col :span="14">
        <el-card>
          <template #header>积压消息样例</template>
          <el-table :data="snapshot.records || []" style="width: 100%">
            <el-table-column prop="timestamp" label="time" width="210" :formatter="formatTsCell" />
            <el-table-column prop="partition" label="p" width="60" />
            <el-table-column prop="offset" label="offset" width="110" />
            <el-table-column prop="key" label="key" width="200" />
            <el-table-column label="value">
              <template #default="{ row }">
                <el-popover placement="left" width="520" trigger="click">
                  <template #reference>
                    <el-button size="small">查看</el-button>
                  </template>
                  <pre style="max-height: 520px; overflow: auto; margin: 0">{{ row.value }}</pre>
                </el-popover>
              </template>
            </el-table-column>
          </el-table>
        </el-card>

        <el-card style="margin-top: 16px">
          <template #header>全部积压浏览（分页）</template>

          <el-alert
            v-if="browseError"
            type="error"
            show-icon
            :closable="false"
            title="分页查询失败"
            :description="browseError"
            style="margin-bottom: 12px"
          />

          <el-form :inline="true" @submit.prevent style="margin-bottom: 8px">
            <el-form-item label="partition">
              <el-select v-model="browser.partition" placeholder="选择分区" style="width: 140px" @change="onPartitionChange">
                <el-option
                  v-for="p in snapshot.partitions || []"
                  :key="p.partition"
                  :label="String(p.partition)"
                  :value="p.partition"
                />
              </el-select>
            </el-form-item>

            <el-form-item label="startOffset">
              <el-input-number v-model="browser.startOffset" :min="0" :step="1" style="width: 190px" />
            </el-form-item>

            <el-form-item label="pageSize">
              <el-input-number v-model="browser.limit" :min="1" :max="1000" style="width: 150px" />
            </el-form-item>

            <el-form-item label="maxValueLen">
              <el-input-number v-model="browser.maxValueLen" :min="0" :max="20000" style="width: 170px" />
            </el-form-item>

            <el-form-item>
              <el-button @click="loadPage">查询</el-button>
            </el-form-item>

            <el-form-item>
              <el-button @click="jumpToCommitted">从committed开始</el-button>
            </el-form-item>

            <el-form-item>
              <el-button :disabled="!canPrev" @click="prevPage">上一页</el-button>
            </el-form-item>

            <el-form-item>
              <el-button type="primary" :disabled="!page.hasMore" @click="nextPage">下一页</el-button>
            </el-form-item>

            <el-form-item>
              <div style="color: #666">
                committed={{ page.committedOffset ?? "-" }}，end={{ page.endOffset ?? "-" }}，next={{ page.nextOffset ?? "-" }}
              </div>
            </el-form-item>
          </el-form>

          <el-table :data="page.records || []" style="width: 100%">
            <el-table-column prop="timestamp" label="time" width="210" :formatter="formatTsCell" />
            <el-table-column prop="partition" label="p" width="60" />
            <el-table-column prop="offset" label="offset" width="110" />
            <el-table-column prop="key" label="key" width="200" />
            <el-table-column label="value">
              <template #default="{ row }">
                <el-popover placement="left" width="520" trigger="click">
                  <template #reference>
                    <el-button size="small">查看</el-button>
                  </template>
                  <pre style="max-height: 520px; overflow: auto; margin: 0">{{ row.value }}</pre>
                </el-popover>
              </template>
            </el-table-column>
          </el-table>
        </el-card>
      </el-col>
    </el-row>
  </div>
</template>

<script setup>
import { computed, onBeforeUnmount, onMounted, reactive, ref } from "vue";
import { api } from "../api/client";

const filters = reactive({ topic: "monitor-topic", groupId: "monitor-observer-group", limit: 50 });
const snapshot = reactive({ totalLag: 0, partitions: [], records: [] });
const error = ref("");
const browseError = ref("");
const browser = reactive({ partition: null, startOffset: 0, limit: 200, maxValueLen: 2000 });
const page = reactive({
  committedOffset: null,
  beginningOffset: null,
  endOffset: null,
  startOffset: null,
  nextOffset: null,
  hasMore: false,
  records: [],
});
let timer;
const lastKey = ref("");

function pad2(n) {
  return String(n).padStart(2, "0");
}

function formatTs(ms) {
  if (ms == null) return "-";
  const d = new Date(Number(ms));
  if (Number.isNaN(d.getTime())) return String(ms);
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())} ${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(
    d.getSeconds()
  )}`;
}

function formatTsCell(row, column, cellValue) {
  return formatTs(cellValue);
}

function formatAgeMs(ms) {
  if (ms == null) return "-";
  const v = Number(ms);
  if (!Number.isFinite(v) || v <= 0) return "0s";
  const totalSec = Math.floor(v / 1000);
  const sec = totalSec % 60;
  const totalMin = Math.floor(totalSec / 60);
  const min = totalMin % 60;
  const hour = Math.floor(totalMin / 60);
  if (hour > 0) return `${hour}h${min}m${sec}s`;
  if (min > 0) return `${min}m${sec}s`;
  return `${sec}s`;
}

function formatAgeCell(row, column, cellValue) {
  return formatAgeMs(cellValue);
}

function findPartitionRow(partition) {
  return (snapshot.partitions || []).find((p) => Number(p.partition) === Number(partition));
}

function canBrowse() {
  return browser.partition != null && filters.topic && filters.groupId;
}

function syncBrowserFromSnapshot() {
  if (browser.partition == null) {
    const first = snapshot.partitions?.[0]?.partition;
    if (first != null) {
      browser.partition = first;
    }
  }
  const pr = findPartitionRow(browser.partition);
  if (pr?.committedOffset != null && (browser.startOffset == null || browser.startOffset < pr.committedOffset)) {
    browser.startOffset = pr.committedOffset;
  }
}

async function loadPage() {
  browseError.value = "";
  if (!canBrowse()) return;
  try {
    const res = await api.get("/kafka/backlog/records", {
      params: {
        topic: filters.topic,
        groupId: filters.groupId,
        partition: browser.partition,
        startOffset: browser.startOffset,
        limit: browser.limit,
        maxValueLen: browser.maxValueLen,
      },
    });
    Object.assign(page, res.data || {});
    if (res.data?.startOffset != null) {
      browser.startOffset = res.data.startOffset;
    }
    if (res.data?.error) {
      browseError.value = res.data.error;
    }
  } catch (e) {
    const msg = e?.response?.data?.message || e?.message || String(e);
    browseError.value = msg;
  }
}

function onPartitionChange() {
  const pr = findPartitionRow(browser.partition);
  if (pr?.committedOffset != null) {
    browser.startOffset = pr.committedOffset;
  }
  loadPage();
}

function jumpToCommitted() {
  if (page.committedOffset != null) {
    browser.startOffset = page.committedOffset;
  }
  loadPage();
}

function prevPage() {
  const committed = Number(page.committedOffset ?? 0);
  const start = Number(browser.startOffset ?? committed);
  browser.startOffset = Math.max(committed, Math.max(0, start - Number(browser.limit ?? 200)));
  loadPage();
}

function nextPage() {
  if (!page.hasMore) return;
  if (page.nextOffset != null) {
    browser.startOffset = page.nextOffset;
  }
  loadPage();
}

const canPrev = computed(() => {
  const committed = Number(page.committedOffset ?? 0);
  const start = Number(browser.startOffset ?? committed);
  return start > committed;
});

async function loadSnapshot(resetPage) {
  error.value = "";
  try {
    const res = await api.get("/kafka/backlog", { params: { ...filters } });
    Object.assign(snapshot, res.data || {});
    if (res.data?.error) {
      error.value = res.data.error;
    }
    const key = `${filters.topic}|${filters.groupId}`;
    if (resetPage || key !== lastKey.value) {
      lastKey.value = key;
      syncBrowserFromSnapshot();
      await loadPage();
    } else {
      syncBrowserFromSnapshot();
    }
  } catch (e) {
    const msg = e?.response?.data?.message || e?.message || String(e);
    error.value = msg;
  }
}

async function refreshAll() {
  await loadSnapshot(true);
}

onMounted(async () => {
  await loadSnapshot(true);
  timer = window.setInterval(() => loadSnapshot(false), 5000);
});

onBeforeUnmount(() => {
  if (timer) window.clearInterval(timer);
});
</script>
