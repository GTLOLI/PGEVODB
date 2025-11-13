#!/usr/bin/env bash
set -euo pipefail

VENV=".venv"
REQ="requirements.txt"

# 如果没有 venv，自动创建并安装
if [ ! -d "$VENV" ]; then
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install --upgrade pip
  "$VENV/bin/pip" install -r "$REQ"
fi

# 用 venv 的 python 运行 pgmigrate
"$VENV/bin/python" -m pgmigrate.cli "$@"
