# K3s 容器执行部署与使用说明（0.4.0）

## 当前架构

管理 API、任务数据库、FFmpeg 视频切分和业务调度仍在服务器项目专用环境中运行。视频推理已改为每次分片尝试创建一个 Kubernetes Job，由选定节点上的 Pod 执行。Docker 用于构建和推送镜像；K3s 节点中的 containerd 拉取并运行镜像。开发服务器用 k3d 创建真实 K3s 集群，k3d 的节点容器只是本机开发拓扑，不代表独立车端硬件。

当前开发集群为 changan-dev：1 个控制节点、4 个带业务标签的执行节点 lab-1～lab-4。同一物理服务器 lab-31，节点容器各设置 8 GiB 内存上限；业务层每节点预算 3072 MiB、一个执行槽。较大的容器上限用于容纳嵌套环境继承的大页预留及节点服务，不代表四份独立物理内存。业务容器默认请求 1 CPU / 512 MiB、限制 2 CPU / 1024 MiB；更高的任务内存参数同步提高请求和上限。

管理页面仍通过 SSH 隧道访问 127.0.0.1:8310。内部容器网关仅绑定开发集群网桥地址的 8311 端口，只有分片下载、执行认领、事件及产物上传接口。管理 API 不需要对容器网段开放。registry 仅发布到服务器环回地址 5500，同时连接集群内部网络。

## 已部署服务器的日常操作

```bash
ssh -p 22031 root@10.13.0.9
cd /root/projects/changan-orchestrator
source /root/miniforge3/etc/profile.d/conda.sh
conda activate changan-orchestrator
python scripts/cluster.py status
python scripts/manage.py status
```

服务器重启后，先运行 `python scripts/cluster.py start`，待节点 Ready 后运行 `python scripts/manage.py start`。当前外层服务器容器的 PID 1 是 sshd，没有使用 systemd 自动拉起业务服务；因此不能承诺外层服务器重启后自动恢复。Docker 节点容器本身有重启策略。

电脑端建立原有 SSH 隧道后打开 http://127.0.0.1:8310：

```bash
ssh -N -L 127.0.0.1:8310:127.0.0.1:8310 -p 22031 root@10.13.0.9
```

查看实际集群对象：

```bash
python scripts/cluster.py kubectl get nodes -o wide
python scripts/cluster.py kubectl get jobs,pods -n changan -o wide
python scripts/cluster.py kubectl get events -n changan --sort-by=.lastTimestamp
```

任务结束后 Job、Pod 和该次尝试的 Secret 会自动清理。因此没有在途任务时看不到视频 Pod 是正常现象。任务详情中的“容器与镜像”保留已清理对象的 UID、镜像摘要、实际节点及失败日志。

## 从新环境准备开发集群

只在项目目录和专用 Python 环境操作。先运行原有 `scripts/bootstrap.sh` 准备管理端、模型、视频和前端依赖，核对模型资产校验。安装 iptables、conntrack 后，运行 bash scripts/install_container_tools.sh 安装项目内 Docker CLI/daemon、k3d；本次使用 Docker 28.5.2、k3d 5.9.0、K3s v1.35.5+k3s1。工具置于 `vendor/docker`、`vendor/bin/k3d`，不上传二进制和运行数据。

本服务器是嵌套容器。第一次创建开发集群前如内存控制器尚未委托，在 Docker 未启动、没有其他子 cgroup 的情况下运行 `python scripts/prepare_nested_cgroup.py`。脚本仅接受 PID 1 为 sshd 且当前 cgroup 名字空间根为 / 的开发容器；普通 Linux / THOR 设备不运行此脚本。它将当前容器进程归入无额外限制的叶 cgroup，再开放当前容器内的资源委托，不修改物理宿主机资源上限。

```bash
python scripts/cluster.py create
python scripts/configure_dev_cluster.py
python scripts/preload_system_images.py
python scripts/build_video_image.py
python scripts/manage.py start
```

下载受限时可在同一交互式 Bash 中执行已授权的 `magic on`，再启动项目 Docker daemon。不要打印、复制或提交代理环境。预装脚本通过项目 Docker daemon 下载并导入 pause、CoreDNS 等基础镜像，避免 K3s 节点依赖外网；业务镜像依然通过内部 registry 真正拉取。

`configure_dev_cluster.py` 写入命名空间、RBAC、节点标签、资源配额和网络策略，保存受限 kubeconfig，合并更新 .env。它用于本机开发集群；不要拿它重标真实板卡。build 脚本构建模型与环境镜像、推送并将不可变摘要写入 .env。模型随镜像交付，任务提交时冻结镜像和模型 SHA256；改变镜像配置只影响新任务。

## 任务使用与状态

新建视频任务、上传视频、选择节点、设置采样率和调度算法的入口保持一致。“容器与镜像”新增每次尝试的目标/实际节点、Job、Pod、镜像摘要、模型摘要及清理状态。

阶段依次包含：Job 已提交、Pod 等待/镜像准备、容器启动、模型加载、真实推理、产物上传、结果核验。Pod Running 本身不等于模型就绪；应用回传决定模型和推理阶段。成功要求输入与模型身份一致、采样结果完整、所有预览产物上传并通过摘要检查，且实际执行 Pod 成功退出。K3s Job 完成不能替代业务结果校验。

每个 Pod 通过本次尝试的令牌和 Downward API Pod UID 认领执行权。管理端核对 Job owner UID、实际节点及活动尝试，一个尝试只允许一个 Pod 认领。容器没有 Kubernetes 管理凭据、不启动常驻 HTTP worker。模型和输入均校验摘要，工作目录为临时 emptyDir，产物回传至管理端持久化存储。

业务调度选定节点后，Job 使用节点标签和必须满足的节点亲和性交由 Kubernetes 调度，不绕过 Kubernetes scheduler。每次尝试的 restartPolicy=Never、backoffLimit=0，业务重试由编排器统一控制。管理服务重启后按持久化 Job/Pod 身份核对，不重新盲发。

取消/超时先请求停止并清理 Job，直到旧 Pod 确认清理、节点观测有效后才释放槽位、触发业务重试。节点或控制面失联时保持资源占用并标记待核对，不能把“没有收到回执”当作“已经停止”。长期失联需要基础设施侧确认设备隔离/恢复，平台不会强制删除未知执行再伪装成清理完成。

## 配置与权限

.env 和 data/k3s/*.yaml 均含运行配置或凭据，权限为 0600，不入库。

| 配置 | 作用 |
|---|---|
| CHANGAN_BACKEND=k3s | 当前默认执行模式；process 只用于旧版回归 |
| CHANGAN_KUBECONFIG | 受限服务账号 kubeconfig |
| CHANGAN_NAMESPACE / CHANGAN_CLUSTER_ID | 命名空间、业务集群标识 |
| CHANGAN_VIDEO_IMAGE | 必须包含 @sha256: 的已审核视频镜像 |
| CHANGAN_IMAGE_ARCH | 当前镜像支持的架构，本次为 amd64 |
| CHANGAN_IMAGE_PULL_SECRET | 正式私有仓库的预建拉取 Secret 名称，可省略 |
| CHANGAN_CONTAINER_HOST / CHANGAN_CONTAINER_BASE | 容器网关绑定地址及 Pod 可达地址 |
| CHANGAN_ENVIRONMENT_NOTE | 页面明确展示的环境说明 |
| CHANGAN_RESOURCE_PROVIDER_URL / CHANGAN_RESOURCE_PROVIDER_TOKEN | 继续使用资源池适配契约；见接口与联调说明 |

受限账号只读节点、读本命名空间 Pod/日志/事件，以及创建/读取/删除本命名空间 Job 和任务 Secret；不使用集群管理员凭据执行业务。当前原型采用专用命名空间内的持久服务账号令牌，需按基础设施管理规范轮换；重新生成 kubeconfig 后重启 API 和内部网关。正式部署建议接入组织现有的凭据轮换机制。

节点注册由集群标签完成，例如管理员给已核验设备配置 `changan.io/node-id`、`changan.io/kind`、`changan.io/physical-host`、`changan.io/slots`、`changan.io/memory-mib`。业务节点 ID 必须唯一，重复标签会阻止调度。前端的停用操作只停止新任务分配；硬件维护和驱逐由基础设施管理。

## THOR 接入边界

4 块 NVIDIA THOR 尚未到位。本次验证为 amd64 + CPU ONNX，不包含 THOR、ARM64、GPU、真实车载网络或跨车性能验收。当前 amd64 镜像不会被调度到 arm64 节点。

板卡到位后与顾强强共同核对官方系统、驱动和容器运行时，接入真实 K3s agent、仓库和 TLS 网络，注册唯一节点及真实资源预算。随后构建并验证 ARM64 镜像；如果需要 GPU，再增加 NVIDIA runtime/device plugin 和实际 GPU 推理后端。仅申请 nvidia.com/gpu 不能让当前 CPUExecutionProvider 自动获得加速。

当前应用模板仍是视频分析。已有镜像化交付、Job 执行和结果协议可以复用；其他任务需要独立应用镜像与输入/结果契约，不接受任意用户镜像或任意 shell 直接执行。

## 验证与回退

`python -m pytest -q` 运行管理端回归，`web/playwright.k3s.config.js` 运行真实容器浏览器验收。完整故障验收脚本 `scripts/integration_k3s.py --faults` 仅允许 data/k3s-validation、8410/8411 的隔离实例，包含管理服务重启、Pod 中断和缺失镜像；不要对正常业务实例运行故障测试。报告中区分真实 K3s 运行与模拟 API 单元测试。

旧数据库采用增量列迁移，历史任务仍可查看，旧记录 backend=process，不补造 Job/Pod。升级前的数据库和前端备份位于 data/backups/20261107-k3s。需要回退时先停止新任务并确认容器全部清理，再切换 CHANGAN_BACKEND=process、重启管理服务和旧执行器。旧进程后端作为维护兼容路径保留，日常平台不启动它。
