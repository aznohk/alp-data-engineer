#!/usr/bin/env bash

set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
PIDFILE="$DIR/.pipeline.pid"
LOGFILE="$DIR/pipeline.out"
ENV_FILE="$DIR/.env"

# Default configuration
PIPELINE_MODE=${PIPELINE_MODE:-complete}
PIPELINE_INTERVAL_SEC=${PIPELINE_INTERVAL_SEC:-60}
RATE_PER_MINUTE=${RATE_PER_MINUTE:-120}
FRAUD_RATIO=${FRAUD_RATIO:-0.25}
REPEAT_PROB=${REPEAT_PROB:-0.15}
MAX_TRANSACTIONS=${MAX_TRANSACTIONS:-}

echo "🚀 Starting Enhanced Data Pipeline"
echo "=================================="

# Check if pipeline is already running
if [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
  echo "⚠️  Pipeline already running with PID $(cat "$PIDFILE")."
  echo "   Use './stop_pipeline.sh' to stop it first."
  exit 1
fi

# Load environment variables
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
  echo "📄 Environment loaded from $ENV_FILE"
else
  echo "⚠️  No .env file found at $ENV_FILE"
fi

# Validate configuration
echo "🔍 Validating pipeline configuration..."
if ! python "$DIR/run_pipeline_complete.py" --dry-run >/dev/null 2>&1; then
  echo "❌ Pipeline validation failed!"
  echo "   Please check your database configuration and try again."
  exit 1
fi
echo "✅ Pipeline validation passed"

# Show configuration
echo ""
echo "⚙️  Pipeline Configuration:"
echo "   Mode: $PIPELINE_MODE"
echo "   Interval: ${PIPELINE_INTERVAL_SEC}s"
echo "   Bronze Rate: ${RATE_PER_MINUTE} transactions/minute"
echo "   Fraud Ratio: ${FRAUD_RATIO}"
echo "   Repeat Probability: ${REPEAT_PROB}"
if [[ -n "$MAX_TRANSACTIONS" ]]; then
  echo "   Max Transactions: $MAX_TRANSACTIONS"
fi

echo ""
echo "🔄 Starting pipeline in background..."
echo "   Logs: $LOGFILE"
echo "   PID file: $PIDFILE"

# Start the pipeline
nohup "$DIR/run_pipeline.sh" > "$LOGFILE" 2>&1 &
PIPELINE_PID=$!
echo $PIPELINE_PID > "$PIDFILE"
disown || true

echo ""
echo "✅ Pipeline started successfully!"
echo "   PID: $PIPELINE_PID"
echo "   Logs: tail -f $LOGFILE"
echo "   Status: python $DIR/run_pipeline_complete.py --status"
echo "   Stop: ./stop_pipeline.sh"
echo ""
echo "🎯 Pipeline is now processing data through Bronze → Silver → Gold layers"


