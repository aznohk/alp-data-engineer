#!/usr/bin/env bash

set -euo pipefail

# Configuration
BRONZE_SCRIPT="$(dirname "$0")/generateTransactionsBronze.py"
SILVER_MAIN="$(dirname "$0")/main.py"
ENV_FILE="$(dirname "$0")/.env"

# Default parameters (can be overridden with env vars)
RATE_PER_MINUTE=${RATE_PER_MINUTE:-120}
FRAUD_RATIO=${FRAUD_RATIO:-0.25}
REPEAT_PROB=${REPEAT_PROB:-0.15}
MAX_TRANSACTIONS=${MAX_TRANSACTIONS:-}
SILVER_INTERVAL_SEC=${SILVER_INTERVAL_SEC:-30}

echo "Starting pipeline: bronze generator + periodic silver transformation"

# Source .env if present so both Python scripts get the same settings
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
  echo ".env loaded from $ENV_FILE"
else
  echo "No .env found at $ENV_FILE (continuing)"
fi

# Start bronze generator in background
if [[ -n "${MAX_TRANSACTIONS}" ]]; then
  python "$BRONZE_SCRIPT" --rate "$RATE_PER_MINUTE" --fraud-ratio "$FRAUD_RATIO" --repeat-prob "$REPEAT_PROB" --max-transactions "$MAX_TRANSACTIONS" &
else
  python "$BRONZE_SCRIPT" --rate "$RATE_PER_MINUTE" --fraud-ratio "$FRAUD_RATIO" --repeat-prob "$REPEAT_PROB" &
fi
BRONZE_PID=$!
echo "Bronze generator PID: $BRONZE_PID"

trap 'echo "Stopping..."; kill -INT $BRONZE_PID 2>/dev/null || true; exit 0' INT TERM

# Run silver job periodically until bronze stops (or indefinitely if bronze is running)
while kill -0 "$BRONZE_PID" 2>/dev/null; do
  echo "Running silver transformation..."
  python "$SILVER_MAIN" || echo "Silver job failed (continuing)"
  echo "Sleeping ${SILVER_INTERVAL_SEC}s before next silver run..."
  sleep "$SILVER_INTERVAL_SEC"
done

echo "Bronze generator exited. Final silver run..."
python "$SILVER_MAIN" || true
echo "Pipeline complete."


