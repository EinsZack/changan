import {defineConfig} from '@playwright/test';
export default defineConfig({
    testDir: './e2e',
    testMatch: 'metrics.spec.js',
    timeout: 30000,
    workers: 1,
    use: {baseURL: 'http://127.0.0.1:18311', viewport: {width: 1440, height: 1000}, screenshot: 'only-on-failure', trace: 'retain-on-failure'},
    reporter: [['list'], ['json', {outputFile: '../docs/task-metrics-browser-test-report.json'}]],
    webServer: {
        command: 'npx vite preview --host 127.0.0.1 --port 18311 --strictPort --outDir ../tmp/task-metrics-web-dist',
        url: 'http://127.0.0.1:18311',
        timeout: 30000,
        reuseExistingServer: false,
    },
});
