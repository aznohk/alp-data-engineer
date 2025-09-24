#!/usr/bin/env bash

set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
PIDFILE="$DIR/.pipeline.pid"
LOGFILE="$DIR/pipeline.out"

echo "🛑 Stopping Enhanced Data Pipeline"
echo "=================================="

# Check if PID file exists
if [[ ! -f "$PIDFILE" ]]; then
  echo "⚠️  No PID file found. Trying best-effort stop..."
  echo "   Killing any running pipeline processes..."
  
  # Kill pipeline processes
  pkill -f "$DIR/run_pipeline.sh" 2>/dev/null || true
  pkill -f "$DIR/run_pipeline_complete.py" 2>/dev/null || true
  pkill -f "$DIR/generateTransactionsBronze.py" 2>/dev/null || true
  pkill -f "$DIR/main.py" 2>/dev/null || true
  
  # Remove log and out files
  echo "🧹 Removing log files (*.log, *.out)..."
  rm -f "$DIR"/*.log "$DIR"/*.out 2>/dev/null || true

  echo "✅ Cleanup completed"
  exit 0
fi

PID=$(cat "$PIDFILE")
echo "📋 Found PID file with PID: $PID"

# Check if process is running
if kill -0 "$PID" 2>/dev/null; then
  echo "🔄 Stopping pipeline PID $PID..."
  
  # Try graceful shutdown first
  kill -TERM "$PID" 2>/dev/null || true
  sleep 2
  
  # Check if still running
  if kill -0 "$PID" 2>/dev/null; then
    echo "⚠️  Graceful shutdown failed. Force killing PID $PID..."
    kill -9 "$PID" 2>/dev/null || true
    sleep 1
  fi
  
  # Final check
  if kill -0 "$PID" 2>/dev/null; then
    echo "❌ Failed to stop process $PID"
  else
    echo "✅ Pipeline stopped successfully"
  fi
else
  echo "⚠️  PID $PID not running. Cleaning up PID file."
fi

# Clean up PID file
rm -f "$PIDFILE"

echo "🧹 Removing log files (*.log, *.out)..."
rm -f "$DIR"/*.log "$DIR"/*.out 2>/dev/null || true

echo "✅ Pipeline stop completed and logs removed."


