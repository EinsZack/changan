# 车群任务编排调度与协同执行

面向中期展示的可运行视频任务原型。管理服务接收视频、切分任务、读取资源快照并调度，独立执行器完成真实ONNX目标检测，回传事件并聚合结果。网页可查看工作流、调度依据、执行时间轴、识别结果、触发策略和历史对比。

0.2.0增加站内任务告警：超时或最终失败时提示，可查看未读数量、告警原因并标记已读或确认。详见[站内告警使用说明](docs/站内告警使用说明.md)和[本次交付说明](docs/站内告警交付说明_20261031.md)。

当前默认环境是同一开发服务器上的4个执行器进程，明确标为server_process；真实车端和正式资源池联调尚未完成。

## 快速进入

代码主目录：`/root/projects/changan-orchestrator`，SSH：`ssh -p 22031 root@10.13.0.9`。

```bash
cd /root/projects/changan-orchestrator
source /root/miniforge3/etc/profile.d/conda.sh
conda activate changan-orchestrator
python scripts/manage.py start
```

电脑端建立隧道：

```bash
ssh -N -L 127.0.0.1:8310:127.0.0.1:8310 -p 22031 root@10.13.0.9
```

访问 <http://127.0.0.1:8310>。API文档：<http://127.0.0.1:8310/docs>。

## 文档

- [开发计划与实施记录](docs/开发计划.md)
- [交付物说明](docs/交付物说明.md)
- [使用说明](docs/使用说明.md)
- [接口与联调说明](docs/接口与联调说明.md)
- [测试与验证](docs/测试与验证.md)
- [演示脚本](docs/演示脚本.md)
- [第三方组件与素材](docs/第三方组件与素材.md)

## 代码与验证

| 目录 | 内容 |
|---|---|
| app/ | API、持久化、编排调度、资源池适配、连接器与视频切分 |
| workers/ | HTTP执行器、状态持久化、ONNX推理及预览产物 |
| web/ | React界面、构建及浏览器测试 |
| scripts/ | 启停、资产准备、环境复现、集成测试和基线测试 |
| tests/ | 状态机、接口、幂等、调度与契约测试 |
| configs/ | systemd及环境配置示例 |
| docs/ | 开发计划、交付文档、原始测试记录和截图 |

```bash
python -m pytest -q
python scripts/integration.py
python scripts/benchmark.py
cd web && npm run build && npx playwright test
```

集成测试会真实创建任务和重启本项目进程，运行前应确保无其他在途工作。请勿将一次成功演示或服务器基线结果写为真实车端验收通过。

本仓库后续代码开发均在服务器完成；进展记录在开发计划，不再维护接手文档作为开发日志。

## 代码仓库与协作

GitHub 仓库：<https://github.com/EinsZack/changan>。服务器上的本目录是开发工作区，`main` 分支保存已验证版本。

开始工作前检查 `git status` 并执行 `git pull --ff-only`；按功能提交源码、必要测试及使用文档，验证后推送。提交说明和具体流程见 [CONTRIBUTING.md](CONTRIBUTING.md)。

模型与视频按 `assets/manifest.json` 和校验值准备；运行数据库、上传文件、凭据、依赖目录和构建产物不入库。仓库中的旧测试报告保留各自生成日期，不代表当前版本已复测。

## 任务详情统计展示

2026年10月31日补充“任务详情 → 传输与吞吐”：展示已有片段输入量、下载耗时与速率、处理吞吐，以及调度时保存的网络观测。详见[使用与验证说明](docs/任务传输统计使用说明_20261031.md)。
