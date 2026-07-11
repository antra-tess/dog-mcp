#!/bin/bash
# Start Go2 control system
# Usage: ./start_all.sh [robot_ip]

cd "$(dirname "$0")"

PYTHON=./venv/bin/python
if [ ! -x "$PYTHON" ]; then
    echo "❌ venv not found. Create it with:"
    echo "   python3.12 -m venv venv && ./venv/bin/pip install websockets pydantic mcp ./unitree_webrtc_connect"
    exit 1
fi

# Robot IP: arg > env > default. Multicast discovery doesn't work across
# wired/wireless on this network, so an explicit IP is required.
export GO2_ROBOT_IP="${1:-${GO2_ROBOT_IP:-192.168.1.179}}"

echo "🐕 Starting Go2 Control System"
echo "================================"
echo "   Robot IP: $GO2_ROBOT_IP"

# Start the WebSocket server in background
echo "Starting WebSocket server (port 8765)..."
$PYTHON go2_server_v2.py &
WS_PID=$!

# Wait for it to start
sleep 3

# Start HTTP server for web UI
echo "Starting Web UI server (port 8000)..."
(cd web_ui && exec ../venv/bin/python -m http.server 8000) &
HTTP_PID=$!

echo ""
echo "✅ Servers started!"
echo "   WebSocket: ws://localhost:8765"
echo "   Web UI:    http://localhost:8000"
echo ""
echo "Press Ctrl+C to stop all servers"

# Handle shutdown
trap "kill $WS_PID $HTTP_PID 2>/dev/null; exit" INT TERM

# Wait
wait
