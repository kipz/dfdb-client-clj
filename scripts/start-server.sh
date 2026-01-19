#!/bin/bash
# Start dfdb-go server on port 8081

SERVER_BIN="/tmp/dfdb-server"
PORT="${1:-8081}"
LOG_FILE="/tmp/dfdb-server.log"
PID_FILE="/tmp/dfdb-server.pid"

# Kill any existing server
pkill -f dfdb-server 2>/dev/null
lsof -ti:$PORT | xargs kill -9 2>/dev/null
sleep 1

# Start server
echo "Starting dfdb-go server on port $PORT..."
$SERVER_BIN -a :$PORT > $LOG_FILE 2>&1 &
SERVER_PID=$!
echo $SERVER_PID > $PID_FILE

# Wait for server to be ready
sleep 2

# Check if server is running
if curl -s http://localhost:$PORT/api/health > /dev/null 2>&1; then
    echo "✓ Server started successfully (PID: $SERVER_PID)"
    echo "  Logs: $LOG_FILE"
    echo "  Health: http://localhost:$PORT/api/health"
else
    echo "✗ Server failed to start. Check logs: $LOG_FILE"
    exit 1
fi
