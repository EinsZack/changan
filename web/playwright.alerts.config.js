import {defineConfig} from '@playwright/test';
export default defineConfig({
    testDir: './e2e',
    testMatch: 'alerts.spec.js',
    timeout: 60000,
    workers: 1,
    use: {baseURL: 'http://127.0.0.1:18310', viewport: {width: 1440, height: 1000}, screenshot: 'only-on-failure', trace: 'retain-on-failure'},
    reporter: [['list'], ['json', {outputFile: '../docs/alerts-browser-test-report.json'}]],
    webServer: {
        command: 'python ../scripts/serve_alert_tests.py',
        url: 'http://127.0.0.1:18310/health',
        timeout: 30000,
        reuseExistingServer: false,
    },
});
