#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"
CONDA_ROOT="${CHANGAN_CONDA_ROOT:-/root/miniforge3}"
if ! "$CONDA_ROOT/bin/conda" run -n changan-orchestrator python --version >/dev/null 2>&1; then
  "$CONDA_ROOT/bin/conda" env create -f environment.yml
fi
PY="$CONDA_ROOT/envs/changan-orchestrator/bin/python"
"$PY" -m pip install -r requirements.lock.txt
mkdir -p vendor/bin assets/models assets/samples logs
if [ ! -x vendor/bin/redis-server ]; then
  curl -L --fail --retry 2 -o vendor/redis-7.2.7.tar.gz https://download.redis.io/releases/redis-7.2.7.tar.gz
  printf '%s  %s\n' '72c081e3b8cfae7144273d26d76736f08319000af46c01515cad5d29765cead5' 'vendor/redis-7.2.7.tar.gz' | sha256sum -c -
  tar -xzf vendor/redis-7.2.7.tar.gz -C vendor
  make -C vendor/redis-7.2.7 -j2 MALLOC=libc redis-server
  cp vendor/redis-7.2.7/src/redis-server vendor/bin/
fi
if [ ! -f assets/models/yolox_tiny.onnx ]; then
  curl -L --fail --retry 2 -o assets/models/yolox_tiny.onnx https://github.com/Megvii-BaseDetection/YOLOX/releases/download/0.1.1rc0/yolox_tiny.onnx
fi
if [ ! -f assets/samples/vtest.avi ]; then
  curl -L --fail --retry 2 -o assets/samples/vtest.avi https://raw.githubusercontent.com/opencv/opencv/master/samples/data/vtest.avi
fi
"$PY" scripts/verify_assets.py
"$PY" scripts/prepare_demo.py
(cd web && npm ci --no-audit --no-fund && npm run build)
printf '依赖与界面准备完成。启动：%s scripts/manage.py start\n' "$PY"
