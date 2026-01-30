<template>
  <el-container class="app-container">
    <el-aside width="240px" class="app-aside">
      <div class="logo-container">
        <el-icon :size="24" color="#409eff" style="margin-right: 8px"><Monitor /></el-icon>
        <span class="logo-text">Kafka Monitor</span>
      </div>
      <el-menu
        router
        :default-active="$route.path"
        class="app-menu"
        background-color="transparent"
        text-color="#b0b0b0"
        active-text-color="#fff"
      >
        <el-menu-item index="/dashboard">
          <el-icon><Odometer /></el-icon>
          <span>仪表盘</span>
        </el-menu-item>
        <el-menu-item index="/messages">
          <el-icon><Search /></el-icon>
          <span>消息搜索</span>
        </el-menu-item>
        <el-menu-item index="/backlog">
          <el-icon><Box /></el-icon>
          <span>积压消息</span>
        </el-menu-item>
        <el-menu-item index="/pending">
          <el-icon><Timer /></el-icon>
          <span>挂起任务</span>
        </el-menu-item>
        <el-menu-item index="/trace">
          <el-icon><Connection /></el-icon>
          <span>链路追踪</span>
        </el-menu-item>
        <el-menu-item index="/grafana">
          <el-icon><DataAnalysis /></el-icon>
          <span>Grafana</span>
        </el-menu-item>
        <el-menu-item index="/alerts">
          <el-icon><Bell /></el-icon>
          <span>告警规则</span>
        </el-menu-item>
      </el-menu>
    </el-aside>
    <el-container class="main-container">
      <el-header class="app-header">
        <div class="header-title">{{ title }}</div>
        <div class="header-actions">
           <!-- Placeholder for user actions or theme toggle -->
        </div>
      </el-header>
      <el-main class="app-main">
        <router-view v-slot="{ Component }">
          <transition name="fade" mode="out-in">
            <component :is="Component" />
          </transition>
        </router-view>
      </el-main>
    </el-container>
  </el-container>
</template>

<script setup>
import { computed } from "vue";
import { useRoute } from "vue-router";
import {
  Monitor,
  Odometer,
  Search,
  Box,
  Timer,
  Connection,
  DataAnalysis,
  Bell
} from "@element-plus/icons-vue";

const route = useRoute();
const title = computed(() => {
  const map = {
    "/dashboard": "仪表盘",
    "/messages": "消息搜索",
    "/backlog": "积压消息",
    "/pending": "挂起任务",
    "/trace": "链路追踪",
    "/grafana": "Grafana",
    "/alerts": "告警规则"
  };
  return map[route.path] || "Kafka 流程监控";
});
</script>

<style scoped>
.app-container {
  height: 100vh;
  background-color: var(--bg-color);
}

.app-aside {
  background: #1a1c22;
  border-right: none;
  display: flex;
  flex-direction: column;
  transition: width 0.3s;
}

.logo-container {
  height: 60px;
  display: flex;
  align-items: center;
  justify-content: center;
  color: #fff;
  font-size: 18px;
  font-weight: 700;
  border-bottom: 1px solid rgba(255, 255, 255, 0.05);
}

.logo-text {
  background: linear-gradient(90deg, #409eff, #36cfc9);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
}

.app-menu {
  border-right: none;
  flex: 1;
  padding-top: 10px;
}

.app-menu .el-menu-item {
  margin: 4px 12px;
  border-radius: 8px;
  height: 50px;
}

.app-menu .el-menu-item:hover {
  background-color: rgba(255, 255, 255, 0.08);
}

.app-menu .el-menu-item.is-active {
  background: linear-gradient(90deg, #409eff 0%, #3a8ee6 100%);
  color: #fff;
  box-shadow: 0 4px 12px rgba(64, 158, 255, 0.3);
}

.main-container {
  display: flex;
  flex-direction: column;
}

.app-header {
  height: 60px;
  background-color: #fff;
  border-bottom: 1px solid #f0f0f0;
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0 24px;
  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.02);
}

.header-title {
  font-size: 20px;
  font-weight: 600;
  color: #1f2f3d;
}

.app-main {
  padding: 24px;
  overflow-y: auto;
}
</style>
