import {defineConfig} from '@playwright/test';
export default defineConfig({testDir:'./e2e',testIgnore:['alerts.spec.js','metrics.spec.js'],timeout:90000,use:{baseURL:'http://127.0.0.1:8310',viewport:{width:1440,height:1000},screenshot:'only-on-failure',video:'on'},workers:1,reporter:[['list'],['json',{outputFile:'../docs/browser-test-report.json'}]]});
