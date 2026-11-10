import {defineConfig} from '@playwright/test';
export default defineConfig({
 testDir:'./e2e',testMatch:'timing.spec.js',timeout:30000,workers:1,
 use:{baseURL:'http://127.0.0.1:18312',viewport:{width:1440,height:1000},screenshot:'only-on-failure',trace:'retain-on-failure'},
 reporter:[['list'],['json',{outputFile:'../docs/gpu-timing-20261110/browser-fixture-report.json'}]],
 webServer:{command:'npx vite preview --host 127.0.0.1 --port 18312 --strictPort --outDir ../tmp/timing-web-dist',url:'http://127.0.0.1:18312',reuseExistingServer:false}
});
