#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"
test "$(uname -m)" = x86_64 || { echo '此脚本仅用于 amd64 开发服务器；THOR 使用原生适配流程。'; exit 1; }
mkdir -p vendor/bin data/k3s
if [ ! -x vendor/docker/dockerd ]; then
  curl -fsSL --retry 3 https://download.docker.com/linux/static/stable/x86_64/docker-28.5.2.tgz -o data/k3s/docker.tgz
  printf '%s  %s\n' ea90cfd12e1eeb12aa1c971741adb8bd4ed88e2a574eaac13f5029a1dbc6300d data/k3s/docker.tgz | sha256sum -c -
  tar -xzf data/k3s/docker.tgz -C vendor
fi
if [ ! -x vendor/bin/k3d ]; then
  curl -fsSL --retry 3 https://github.com/k3d-io/k3d/releases/download/v5.9.0/k3d-linux-amd64 -o data/k3s/k3d-linux-amd64
  printf '%s  %s\n' 06d8f25bc3a971c4eb29e0ff08429b180402db0f4dec838c9eac427e296800a0 data/k3s/k3d-linux-amd64 | sha256sum -c -
  install -m 0755 data/k3s/k3d-linux-amd64 vendor/bin/k3d
fi
echo '项目内 Docker/k3d 工具就绪；请确认 iptables、conntrack、cgroup 及专用网络配置。'
