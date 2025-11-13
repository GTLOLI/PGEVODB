#!/usr/bin/env bash
set -euo pipefail

# 在项目根（脚本所在目录）执行
cd "$(dirname "$0")"

VENV=".venv"
REQ="requirements.txt"
SRC_DIR="src"

# 如果没有 venv，自动创建并安装
if [ ! -d "$VENV" ]; then
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install --upgrade pip
  "$VENV/bin/pip" install -r "$REQ"
fi

# 把 src 添加到 PYTHONPATH，这样 python -m pgmigrate.cli 能找到包
export PYTHONPATH="${PWD}/${SRC_DIR}:${PYTHONPATH:-}"

# 用 venv 的 python 运行 pgmigrate
"$VENV/bin/python" -m pgmigrate.cli "$@"
