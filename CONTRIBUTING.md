# 项目开发与版本管理

业务开发统一在服务器 `/root/projects/changan-orchestrator` 进行，使用 `/root/miniforge3/envs/changan-orchestrator`。本地资料目录保留需求、计划和交付说明。

## 开始工作

1. 检查 `git status --short --branch`，保留其他人的未提交修改。
2. 执行 `git pull --ff-only origin main`。发生分叉时先核对改动，不强制覆盖。
3. 阅读 `AGENTS.md` 和本次涉及的接口、测试及开发计划。

## 提交与验证

- 每个功能提交包括实现、必要测试、使用或接口说明。
- 先运行对应测试，再执行项目回归；前端变更执行 `npm run build` 和相应浏览器验证。
- 业务测试可能创建任务、重启服务，优先使用隔离环境；操作运行环境前核对在途任务。
- 提交前检查 `git diff --check`、`git diff --stat` 及待提交内容。
- 使用明确的提交说明，例如 `feat: add in-app task alerts` 或 `fix: deduplicate task alerts`。
- 验证通过后执行 `git push origin main`，核对远端提交。避免强制推送和无关文件混入。

## 文件边界

源码、依赖清单、部署脚本、接口说明及必要测试由 Git 管理。运行数据库、日志、上传视频、模型二进制、访问凭据、依赖目录和构建产物按 `.gitignore` 排除。

测试证据应脱敏并注明日期、版本、环境及输入。历史证据不能作为新版本的通过结论，服务器进程不能计为真实车端节点。

## 开发记录
