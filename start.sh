#!/usr/bin/env bash
# SubtitleForge 启动脚本（NAS 直跑模式）
cd "$(dirname "$0")"
set -a
source .env
set +a
mkdir -p media config data models logs
LOG="logs/subtitle-forge.log"
nohup .venv/bin/python -m app.main >> "$LOG" 2>&1 &
echo $! > logs/subtitle-forge.pid
echo "started pid=$(cat logs/subtitle-forge.pid) log=$PWD/$LOG"
