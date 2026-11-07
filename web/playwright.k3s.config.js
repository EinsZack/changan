import {defineConfig} from '@playwright/test';
export default defineConfig({
 testDir:'./e2e',testMatch:'k3s.spec.js',timeout:240000,workers:1,
 use:{baseURL:process.env.CHANGAN_BROWSER_BASE||'http://127.0.0.1:8410',viewport:{width:1440,height:1000},screenshot:'only-on-failure',trace:'retain-on-failure'},
 reporter:[['list'],['json',{outputFile:'../docs/k3s-browser-test-report.json'}]]
});
