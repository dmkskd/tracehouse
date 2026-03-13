#!/bin/bash
# Setup and start ClickHouse single binary with project configuration
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$SCRIPT_DIR/data"
LOG_DIR="$SCRIPT_DIR/logs"
CONFIG_DIR="$SCRIPT_DIR/config"
PID_FILE="$SCRIPT_DIR/clickhouse.pid"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}ClickHouse Local Setup${NC}"
echo "========================"

# Check if ClickHouse is installed
if ! command -v clickhouse &> /dev/null; then
    echo -e "${YELLOW}ClickHouse not found. Installing...${NC}"
    
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        if command -v brew &> /dev/null; then
            brew install clickhouse
        else
            echo "Installing via curl..."
            curl https://clickhouse.com/ | sh
            sudo mv clickhouse /usr/local/bin/
        fi
    else
        # Linux
        echo "Installing via curl..."
        curl https://clickhouse.com/ | sh
        sudo mv clickhouse /usr/local/bin/
    fi
fi

# Verify installation
CLICKHOUSE_VERSION=$(clickhouse --version 2>/dev/null | head -1 || echo "unknown")
echo -e "ClickHouse version: ${GREEN}$CLICKHOUSE_VERSION${NC}"

# Create directories
mkdir -p "$DATA_DIR"
mkdir -p "$LOG_DIR"
mkdir -p "$DATA_DIR/tmp"
mkdir -p "$DATA_DIR/user_files"
mkdir -p "$DATA_DIR/format_schemas"

echo "Data directory: $DATA_DIR"
echo "Log directory: $LOG_DIR"
echo "Config directory: $CONFIG_DIR"

# Check if already running
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo -e "${YELLOW}ClickHouse is already running (PID: $OLD_PID)${NC}"
        echo "Use ./stop.sh to stop it first, or connect with: clickhouse client"
        exit 0
    else
        rm -f "$PID_FILE"
    fi
fi

# Start ClickHouse server
echo ""
echo -e "${GREEN}Starting ClickHouse server...${NC}"

clickhouse server \
    --config-file="$CONFIG_DIR/config.xml" \
    --pid-file="$PID_FILE" \
    -- \
    --path="$DATA_DIR" \
    --tmp_path="$DATA_DIR/tmp/" \
    --user_files_path="$DATA_DIR/user_files/" \
    --format_schema_path="$DATA_DIR/format_schemas/" \
    --logger.log="$LOG_DIR/clickhouse-server.log" \
    --logger.errorlog="$LOG_DIR/clickhouse-server.err.log" \
    &

# Wait for server to start
echo "Waiting for server to start..."
for i in {1..30}; do
    if clickhouse client --query "SELECT 1" &>/dev/null; then
        echo ""
        echo -e "${GREEN}✓ ClickHouse is running!${NC}"
        echo ""
        
        # Create read-only user (idempotent)
        if [ -f "$SCRIPT_DIR/../scripts/setup_read_only_user.sql" ]; then
            echo "Creating read_only user..."
            clickhouse client < "$SCRIPT_DIR/../scripts/setup_read_only_user.sql" 2>/dev/null && \
                echo -e "${GREEN}✓ read_only user created${NC}" || \
                echo -e "${YELLOW}⚠ read_only user setup skipped (may already exist)${NC}"
            echo ""
        fi
        
        echo "Ports:"
        echo "  - Native:     9000"
        echo "  - HTTP:       8123"
        echo "  - Prometheus: 9363"
        echo ""
        echo "Connect with: clickhouse client"
        echo "Stop with:    ./stop.sh"
        echo ""
        echo "Logs: $LOG_DIR/clickhouse-server.log"
        exit 0
    fi
    sleep 1
    echo -n "."
done

echo ""
echo -e "${RED}Failed to start ClickHouse. Check logs:${NC}"
echo "  tail -f $LOG_DIR/clickhouse-server.err.log"
exit 1
