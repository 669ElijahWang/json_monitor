import { createRouter, createWebHistory } from "vue-router";
import Dashboard from "../views/Dashboard.vue";
import MessageSearch from "../views/MessageSearch.vue";
import TraceView from "../views/TraceView.vue";
import GrafanaEmbed from "../views/GrafanaEmbed.vue";
import AlertRules from "../views/AlertRules.vue";

export default createRouter({
  history: createWebHistory(),
  routes: [
    { path: "/", redirect: "/dashboard" },
    { path: "/dashboard", component: Dashboard },
    { path: "/messages", component: MessageSearch },
    { path: "/trace", component: TraceView },
    { path: "/grafana", component: GrafanaEmbed },
    { path: "/alerts", component: AlertRules }
  ]
});

