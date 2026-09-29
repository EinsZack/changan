#!/usr/bin/env bash
set -euo pipefail
: "${NODE_ID:?必须设置唯一NODE_ID}"
: "${CHANGAN_INTERNAL_TOKEN:?必须配置与管理侧一致的内部令牌}"
cd "$(dirname "$0")/.."
exec "${CHANGAN_PYTHON:-/root/miniforge3/envs/changan-orchestrator/bin/python}" -m uvicorn workers.service:app --host "${WORKER_HOST:-127.0.0.1}" --port "${WORKER_PORT:-8321}" --no-access-log
