#!/bin/bash
# Stop dfdb-go server

PID_FILE="/tmp/dfdb-server.pid"

# Kill by PID file
if [ -f "$PID_FILE" ]; then
    PID=$(cat $PID_FILE)
    if ps -p $PID > /dev/null 2>&1; then
        echo "Stopping server (PID: $PID)..."
        kill $PID 2>/dev/null
        sleep 1
        # Force kill if still running
        if ps -p $PID > /dev/null 2>&1; then
            kill -9 $PID 2>/dev/null
        fi
        rm -f $PID_FILE
        echo "✓ Server stopped"
    else
        echo "Server not running (stale PID file)"
        rm -f $PID_FILE
    fi
else
    # Fallback: kill by name
    if pkill -f dfdb-server 2>/dev/null; then
        echo "✓ Server stopped (by name)"
    else
        echo "No server process found"
    fi
fi

# Clean up port
lsof -ti:8081 | xargs kill -9 2>/dev/null
