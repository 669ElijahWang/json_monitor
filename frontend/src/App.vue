<template>
  <el-container style="height: 100vh">
    <el-aside width="220px" style="border-right: 1px solid #eee">
      <div style="padding: 16px; font-weight: 600">Kafka 流程监控</div>
      <el-menu router :default-active="$route.path">
        <el-menu-item index="/dashboard">仪表盘</el-menu-item>
        <el-menu-item index="/messages">消息搜索</el-menu-item>
        <el-menu-item index="/trace">链路追踪</el-menu-item>
        <el-menu-item index="/grafana">Grafana</el-menu-item>
        <el-menu-item index="/alerts">告警规则</el-menu-item>
      </el-menu>
    </el-aside>
    <el-container>
      <el-header style="border-bottom: 1px solid #eee; display: flex; align-items: center">
        <div style="font-weight: 600">{{ title }}</div>
      </el-header>
      <el-main style="background: #fafafa">
        <router-view />
      </el-main>
    </el-container>
  </el-container>
</template>

<script setup>
import { computed } from "vue";
import { useRoute } from "vue-router";

const route = useRoute();
const title = computed(() => {
  if (route.path === "/dashboard") return "仪表盘";
  if (route.path === "/messages") return "消息搜索";
  if (route.path === "/trace") return "链路追踪";
  if (route.path === "/grafana") return "Grafana";
  if (route.path === "/alerts") return "告警规则";
  return "Kafka 流程监控";
});
</script>

