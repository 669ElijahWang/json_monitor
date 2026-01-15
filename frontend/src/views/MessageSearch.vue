<template>
  <div style="padding: 16px">
    <el-card>
      <el-form :inline="true" @submit.prevent>
        <el-form-item label="关键词">
          <el-input v-model="filters.q" placeholder="taskId / nodeName / adviseKey ..." style="width: 240px" />
        </el-form-item>
        <el-form-item label="租户">
          <el-input v-model="filters.tenant" placeholder="BIOM" style="width: 160px" />
        </el-form-item>
        <el-form-item label="系统">
          <el-input v-model="filters.systemNo" placeholder="SYS-A" style="width: 160px" />
        </el-form-item>
        <el-form-item label="busId">
          <el-input v-model="filters.busId" placeholder="B-01" style="width: 160px" />
        </el-form-item>
        <el-form-item label="taskId">
          <el-input v-model="filters.taskId" placeholder="t-001" style="width: 200px" />
        </el-form-item>
        <el-form-item>
          <el-button type="primary" @click="load">查询</el-button>
        </el-form-item>
      </el-form>
    </el-card>

    <el-card style="margin-top: 16px">
      <el-table :data="rows" row-key="id" style="width: 100%">
        <el-table-column prop="createdAt" label="时间" width="210" />
        <el-table-column prop="taskId" label="taskId" width="200" />
        <el-table-column prop="tenant" label="租户" width="120" />
        <el-table-column prop="systemNo" label="系统" width="120" />
        <el-table-column prop="nodeName" label="节点" width="160" />
        <el-table-column prop="result" label="结果" width="140" />
        <el-table-column label="详情">
          <template #default="{ row }">
            <el-popover placement="left" width="520" trigger="click">
              <template #reference>
                <el-button size="small">JSON</el-button>
              </template>
              <pre style="max-height: 480px; overflow: auto; margin: 0">{{ row.rawJson }}</pre>
            </el-popover>
          </template>
        </el-table-column>
      </el-table>

      <div style="display: flex; justify-content: flex-end; margin-top: 12px">
        <el-pagination
          layout="prev, pager, next, sizes, total"
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

const filters = reactive({ q: "", tenant: "", systemNo: "", busId: "", taskId: "" });
const rows = ref([]);
const page = reactive({ page: 0, size: 20, total: 0 });

async function load() {
  const res = await api.get("/messages/search", {
    params: { ...filters, page: page.page, size: page.size }
  });
  rows.value = res.data.content;
  page.total = res.data.totalElements;
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

