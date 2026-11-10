import {defineConfig} from '@playwright/test';
export default defineConfig({
 testDir:'./e2e',testMatch:'timing-production.spec.js',timeout:240000,workers:1,
 use:{baseURL:'http://127.0.0.1:8310',viewport:{width:1440,height:1000},screenshot:'only-on-failure',trace:'retain-on-failure'},
 reporter:[['list'],['json',{outputFile:'../docs/gpu-timing-20261110/browser-production-report.json'}]]
});
