#!/usr/bin/env bash

set -euo pipefail

# Configuration
BRONZE_SCRIPT="$(dirname "$0")/generateTransactionsBronze.py"
PIPELINE_RUNNER="$(dirname "$0")/run_pipeline_complete.py"
ENV_FILE="$(dirname "$0")/.env"

# Default parameters (can be overridden with env vars)
RATE_PER_MINUTE=${RATE_PER_MINUTE:-120}
FRAUD_RATIO=${FRAUD_RATIO:-0.25}
REPEAT_PROB=${REPEAT_PROB:-0.15}
MAX_TRANSACTIONS=${MAX_TRANSACTIONS:-}
PIPELINE_INTERVAL_SEC=${PIPELINE_INTERVAL_SEC:-60}
PIPELINE_MODE=${PIPELINE_MODE:-complete}

echo "🚀 Starting Enhanced Pipeline: Bronze Generator + Complete Data Pipeline (Bronze->Silver->Gold)"

# Source .env if present so both Python scripts get the same settings
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
  echo "📄 .env loaded from $ENV_FILE"
else
  echo "⚠️  No .env found at $ENV_FILE (continuing)"
fi

# Validate pipeline configuration first
echo "🔍 Validating pipeline configuration..."
python "$PIPELINE_RUNNER" --dry-run || {
  echo "❌ Pipeline validation failed. Exiting."
  exit 1
}
echo "✅ Pipeline validation passed"

# Start bronze generator in background
echo "🟤 Starting Bronze Data Generator..."
if [[ -n "${MAX_TRANSACTIONS}" ]]; then
  python "$BRONZE_SCRIPT" --rate "$RATE_PER_MINUTE" --fraud-ratio "$FRAUD_RATIO" --repeat-prob "$REPEAT_PROB" --max-transactions "$MAX_TRANSACTIONS" &
else
  python "$BRONZE_SCRIPT" --rate "$RATE_PER_MINUTE" --fraud-ratio "$FRAUD_RATIO" --repeat-prob "$REPEAT_PROB" &
fi
BRONZE_PID=$!
echo "🟤 Bronze generator PID: $BRONZE_PID"

# Function to cleanup on exit
cleanup() {
  echo "🛑 Stopping pipeline..."
  kill -INT $BRONZE_PID 2>/dev/null || true
  echo "✅ Pipeline stopped"
  exit 0
}

trap cleanup INT TERM

# Run complete pipeline periodically until bronze stops
echo "🔄 Starting periodic pipeline execution (mode: $PIPELINE_MODE, interval: ${PIPELINE_INTERVAL_SEC}s)"
while kill -0 "$BRONZE_PID" 2>/dev/null; do
  echo "🔄 Running complete data pipeline..."
  python "$PIPELINE_RUNNER" --mode "$PIPELINE_MODE" || echo "⚠️  Pipeline run failed (continuing)"
  echo "⏰ Sleeping ${PIPELINE_INTERVAL_SEC}s before next pipeline run..."
  sleep "$PIPELINE_INTERVAL_SEC"
done

echo "🟤 Bronze generator exited. Running final pipeline..."
python "$PIPELINE_RUNNER" --mode "$PIPELINE_MODE" || true

echo "🎉 Enhanced pipeline complete!"
echo "📊 Showing final pipeline status..."
python "$PIPELINE_RUNNER" --status


