import {defineConfig} from '@playwright/test';
export default defineConfig({
 testDir:'./e2e',testMatch:'timing-real.spec.js',timeout:60000,workers:1,
 use:{baseURL:'http://127.0.0.1:18313',viewport:{width:1440,height:1000},screenshot:'only-on-failure',trace:'retain-on-failure'},
 reporter:[['list'],['json',{outputFile:'../docs/gpu-timing-20261110/browser-real-report.json'}]],
 webServer:{command:'npx vite preview --config vite.timing.config.js --host 127.0.0.1 --port 18313 --strictPort --outDir ../tmp/task-metrics-web-dist',url:'http://127.0.0.1:18313',reuseExistingServer:false}
});
