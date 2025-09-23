#!/usr/bin/env bash

set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
PIDFILE="$DIR/.pipeline.pid"
LOGFILE="$DIR/pipeline.out"

if [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
  echo "Pipeline already running with PID $(cat "$PIDFILE")."
  exit 0
fi

echo "Starting pipeline in background... (logs: $LOGFILE)"
nohup "$DIR/run_pipeline.sh" > "$LOGFILE" 2>&1 &
echo $! > "$PIDFILE"
disown || true
echo "Started. PID $(cat "$PIDFILE")."


