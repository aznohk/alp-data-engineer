#!/usr/bin/env bash

set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
PIDFILE="$DIR/.pipeline.pid"

if [[ ! -f "$PIDFILE" ]]; then
  echo "No PID file found. Trying best-effort stop..."
  pkill -f "$DIR/run_pipeline.sh" || true
  pkill -f "$DIR/generateTransactionsBronze.py" || true
  pkill -f "$DIR/main.py" || true
  exit 0
fi

PID=$(cat "$PIDFILE")
if kill -0 "$PID" 2>/dev/null; then
  echo "Stopping pipeline PID $PID ..."
  kill -TERM "$PID" || true
  sleep 1
  if kill -0 "$PID" 2>/dev/null; then
    echo "Force killing pipeline PID $PID ..."
    kill -9 "$PID" || true
  fi
else
  echo "PID $PID not running. Cleaning up PID file."
fi

rm -f "$PIDFILE"
echo "Stopped."


