#!/usr/bin/env bash

set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
PIDFILE="$DIR/.pipeline.pid"
LOGFILE="$DIR/pipeline.out"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

show_help() {
    echo -e "${BLUE}🚀 Enhanced Data Pipeline Manager${NC}"
    echo "=================================="
    echo ""
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "  start     Start the pipeline in background"
    echo "  stop      Stop the running pipeline"
    echo "  restart   Restart the pipeline"
    echo "  status    Show pipeline status and metrics"
    echo "  logs      Show pipeline logs"
    echo "  config    Show pipeline configuration"
    echo "  validate  Validate pipeline configuration"
    echo "  demo      Run pipeline demo"
    echo "  help      Show this help message"
    echo ""
    echo "Options:"
    echo "  --mode MODE           Pipeline mode (complete, gold-only, legacy)"
    echo "  --interval SECONDS    Pipeline execution interval (default: 60)"
    echo "  --rate RATE           Bronze generator rate per minute (default: 120)"
    echo "  --fraud-ratio RATIO   Fraud ratio (default: 0.25)"
    echo "  --max-transactions N  Maximum transactions to generate"
    echo ""
    echo "Examples:"
    echo "  $0 start"
    echo "  $0 start --mode gold-only --interval 30"
    echo "  $0 status"
    echo "  $0 logs"
    echo "  $0 stop"
}

is_running() {
    [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null
}

start_pipeline() {
    if is_running; then
        echo -e "${YELLOW}⚠️  Pipeline is already running with PID $(cat "$PIDFILE")${NC}"
        echo "   Use '$0 stop' to stop it first, or '$0 restart' to restart"
        return 1
    fi
    
    echo -e "${GREEN}🚀 Starting Enhanced Data Pipeline...${NC}"
    "$DIR/start_pipeline.sh"
}

stop_pipeline() {
    if ! is_running; then
        echo -e "${YELLOW}⚠️  No pipeline is currently running${NC}"
        return 0
    fi
    
    echo -e "${RED}🛑 Stopping Enhanced Data Pipeline...${NC}"
    "$DIR/stop_pipeline.sh"
}

restart_pipeline() {
    echo -e "${BLUE}🔄 Restarting Enhanced Data Pipeline...${NC}"
    stop_pipeline
    sleep 2
    start_pipeline
}

show_status() {
    echo -e "${BLUE}📊 Pipeline Status${NC}"
    echo "=================="
    
    if is_running; then
        echo -e "${GREEN}✅ Pipeline is running${NC}"
        echo "   PID: $(cat "$PIDFILE")"
        echo "   Started: $(stat -f "%Sm" "$PIDFILE" 2>/dev/null || stat -c "%y" "$PIDFILE" 2>/dev/null || echo "Unknown")"
    else
        echo -e "${RED}❌ Pipeline is not running${NC}"
    fi
    
    echo ""
    echo "📈 Pipeline Metrics:"
    python "$DIR/run_pipeline_complete.py" --status
}

show_logs() {
    echo -e "${BLUE}📝 Pipeline Logs${NC}"
    echo "================"
    
    if [[ -f "$LOGFILE" ]]; then
        echo "Showing last 50 lines of $LOGFILE:"
        echo "----------------------------------------"
        tail -50 "$LOGFILE"
        echo "----------------------------------------"
        echo ""
        echo "To follow logs in real-time: tail -f $LOGFILE"
    else
        echo -e "${YELLOW}⚠️  No log file found at $LOGFILE${NC}"
    fi
}

show_config() {
    echo -e "${BLUE}⚙️  Pipeline Configuration${NC}"
    echo "=========================="
    python "$DIR/run_pipeline_complete.py" --config
}

validate_config() {
    echo -e "${BLUE}🔍 Validating Pipeline Configuration${NC}"
    echo "====================================="
    python "$DIR/run_pipeline_complete.py" --dry-run
}

run_demo() {
    echo -e "${BLUE}🎬 Running Pipeline Demo${NC}"
    echo "======================="
    python "$DIR/demo_pipeline.py"
}

# Parse command line arguments
COMMAND=""
MODE="complete"
INTERVAL="60"
RATE="120"
FRAUD_RATIO="0.25"
REPEAT_PROB="0.15"
MAX_TRANSACTIONS=""

while [[ $# -gt 0 ]]; do
    case $1 in
        start|stop|restart|status|logs|config|validate|demo|help)
            COMMAND="$1"
            shift
            ;;
        --mode)
            MODE="$2"
            shift 2
            ;;
        --interval)
            INTERVAL="$2"
            shift 2
            ;;
        --rate)
            RATE="$2"
            shift 2
            ;;
        --fraud-ratio)
            FRAUD_RATIO="$2"
            shift 2
            ;;
        --repeat-prob)
            REPEAT_PROB="$2"
            shift 2
            ;;
        --max-transactions)
            MAX_TRANSACTIONS="$2"
            shift 2
            ;;
        *)
            echo -e "${RED}❌ Unknown option: $1${NC}"
            show_help
            exit 1
            ;;
    esac
done

# Set environment variables for pipeline
export PIPELINE_MODE="$MODE"
export PIPELINE_INTERVAL_SEC="$INTERVAL"
export RATE_PER_MINUTE="$RATE"
export FRAUD_RATIO="$FRAUD_RATIO"
export REPEAT_PROB="$REPEAT_PROB"
if [[ -n "$MAX_TRANSACTIONS" ]]; then
    export MAX_TRANSACTIONS="$MAX_TRANSACTIONS"
fi

# Execute command
case "$COMMAND" in
    start)
        start_pipeline
        ;;
    stop)
        stop_pipeline
        ;;
    restart)
        restart_pipeline
        ;;
    status)
        show_status
        ;;
    logs)
        show_logs
        ;;
    config)
        show_config
        ;;
    validate)
        validate_config
        ;;
    demo)
        run_demo
        ;;
    help|"")
        show_help
        ;;
    *)
        echo -e "${RED}❌ Unknown command: $COMMAND${NC}"
        show_help
        exit 1
        ;;
esac
