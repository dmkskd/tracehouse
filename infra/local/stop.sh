#!/bin/bash
# Stop ClickHouse server
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$SCRIPT_DIR/clickhouse.pid"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo "Stopping ClickHouse (PID: $PID)..."
        kill "$PID"
        
        # Wait for graceful shutdown
        for i in {1..10}; do
            if ! kill -0 "$PID" 2>/dev/null; then
                rm -f "$PID_FILE"
                echo -e "${GREEN}✓ ClickHouse stopped${NC}"
                exit 0
            fi
            sleep 1
        done
        
        # Force kill if still running
        echo -e "${YELLOW}Force killing...${NC}"
        kill -9 "$PID" 2>/dev/null || true
        rm -f "$PID_FILE"
        echo -e "${GREEN}✓ ClickHouse stopped${NC}"
    else
        echo -e "${YELLOW}ClickHouse not running (stale PID file)${NC}"
        rm -f "$PID_FILE"
    fi
else
    # Try to find and kill any clickhouse process in this directory
    PIDS=$(pgrep -f "clickhouse.*$SCRIPT_DIR" 2>/dev/null || true)
    if [ -n "$PIDS" ]; then
        echo "Found ClickHouse processes: $PIDS"
        kill $PIDS 2>/dev/null || true
        echo -e "${GREEN}✓ ClickHouse stopped${NC}"
    else
        echo -e "${YELLOW}ClickHouse is not running${NC}"
    fi
fi
