#!/bin/bash
# Restart dfdb-go server with fresh database

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Restarting dfdb-go server..."
$SCRIPT_DIR/stop-server.sh
sleep 1
$SCRIPT_DIR/start-server.sh $@
